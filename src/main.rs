//! Godworks OS data-plane (Rust) — the authoritative store + op-broker + zone handoff
//! + WAL durability + lease/heartbeat failover. A drop-in, FULL-PARITY equivalent of
//!   the reference server (same ~17-op contract, same length-prefixed-JSON wire in
//!   phase 1, so the Godot bridge and reference wire-tests run against it UNCHANGED).
//!
//! design-parity vs the reference (no function lost, none downgraded):
//!   hot path     : WorkerConnect, Interest/QBI, CreateEntity, DeleteEntity,
//!                  UpdateComponent (single-writer belt -> apply -> hysteresis handoff
//!                  -> propagate), UpdateRejected, AddEntity/RemoveEntity/ComponentUpdate/
//!                  AuthorityChange/CriticalSection.
//!   durability   : append-only WAL (JSONL, fsync via sync_data) with register/write/
//!                  transfer events; recover() rebuilds the EXACT store from the log alone
//!                  (the durable-recovery fix) — GW_WAL=<path> enables it; restart recovers.
//!   liveness     : per-region lease + Heartbeat/write renew + a monitor that fails a
//!                  lapsed region over to a STANDBY (reclaim-to-standby), orphaned_regions
//!                  as the honest no-spare dead-end; metrics{handoffs,applies,failovers}.
//! the reference is the conformance oracle; this is the runtime (no GC, real threads, ARM).

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use std::sync::atomic::AtomicBool;

use godworks_broker::wal::{
    crc32_ieee, read_wal_events, wal_v1_envelope_line, wal_v1_header_line, WalReadReport,
};
use godworks_core::{
    AuthorityMode, PartitionMapSpec, PartitionSchema, RegionSplitSpec, SpatialSchema,
    VersionedPartitionMap, COORDINATE_CODEC_VERSION, SPATIAL_SCHEMA_VERSION,
    STANDARD_COMPONENT_REGISTRY_VERSION,
};
use godworks_protocol::{
    operation_semantics, partition_map_contract_value, DEFAULT_MAX_FRAME_BYTES,
    SNAPSHOT_MANIFEST_VERSION, SNAPSHOT_SCHEMA_VERSION,
};
use serde_json::{json, Map, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc::{self, Sender, UnboundedSender};
use tokio::sync::Mutex;

mod replay_tape;
use replay_tape::ReplayTape;

const BOUNDARY: f64 = 0.0;
const H: f64 = 0.5;
const INTEREST_MARGIN: f64 = 1.0;
// G4 backpressure: over this per-worker egress backlog, DEGRADABLE ops (ComponentUpdate / InspectorFrame)
// are dropped to bound a slow consumer's memory; CRITICAL ops (AuthorityChange / Add+RemoveEntity /
// CommandResponse / WAL-visible) always send -- but only INTO the bounded channel below.
const EGRESS_SOFT_CAP: u64 = 4096;
// T1 HARD CAP (the structural egress bound). The per-consumer egress is a BOUNDED tokio channel of this
// capacity -- a slow/malicious consumer that stops draining can hold AT MOST this many unsent frames, then
// the bound is hit and the consumer is force-DISCONNECTED (it reconnects + re-checks-out via checkout_all,
// so NO state is silently lost). The cap is a PROPERTY OF THE CHANNEL TYPE (mpsc::channel(CHANNEL_CAP) cannot
// physically buffer more), not a flag anyone can forget to enable -- remove it and it no longer compiles as
// bounded. It sits ABOVE EGRESS_SOFT_CAP so the order is: healthy -> degradable-shed at the soft cap ->
// (only a genuinely stuck consumer that won't drain even critical-only traffic reaches) hard cap -> kick.
// This is the hard RAM ceiling the G4 comment promised + the convergent Interest/Security T1 finding's fix.
// VALUE = 4x EGRESS_SOFT_CAP (16384). Rationale: the degradable soft-drop already sheds at 4096, so a consumer
// holding 4x that many UNSENT frames -- and still not draining even critical-only traffic -- is unambiguously
// stuck (no legitimate healthy consumer backs up 16k critical frames). At ~150 B/frame that is a ~2.5 MB hard
// ceiling PER consumer: tight, principled, not a test-tuned knob. Above it, the critical-send-failure kicks it.
const CHANNEL_CAP: usize = EGRESS_SOFT_CAP as usize * 4;
// Security v0 ingress bound. The frame-size cap blocks one oversized body; this token bucket blocks the
// other public-TCP failure mode: many valid small frames. Defaults are intentionally generous for local game
// traffic and tests; operators can lower them with GW_INGRESS_RATE_PER_SEC / GW_INGRESS_BURST_FRAMES.
const DEFAULT_INGRESS_RATE_PER_SEC: f64 = 2000.0;
const DEFAULT_INGRESS_BURST_FRAMES: f64 = 4000.0;
// Sustained large-but-valid JSON frames are the second half of the ingress-cost surface after MAX_FRAME:
// one frame is not one unit of broker work. Charge at least one token per 8 KiB of parsed payload.
const INGRESS_BYTES_PER_TOKEN: f64 = 8192.0;
const PROTOCOL_VERSION: u64 = 1; // L4: this broker's wire-format version
const MIN_PROTO: u64 = 1; // L4: oldest peer wire-version still understood
struct InboundFrame {
    value: Value,
    byte_len: usize,
}

fn parse_nonnegative_f64_env(name: &str, default: f64) -> f64 {
    std::env::var(name)
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
        .filter(|f| f.is_finite() && *f >= 0.0)
        .unwrap_or(default)
}

fn parse_nonempty_env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|s| !s.is_empty())
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PeerClaims {
    region: String,
    attributes: HashSet<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PeerRole {
    Worker,
    Client,
    Observer,
    Mesh,
}

impl PeerRole {
    fn as_str(self) -> &'static str {
        match self {
            PeerRole::Worker => "worker",
            PeerRole::Client => "client",
            PeerRole::Observer => "observer",
            PeerRole::Mesh => "mesh",
        }
    }
}

fn peer_role_for(region: &str, attributes: &HashSet<String>) -> PeerRole {
    if region == "MESH" || attributes.contains("role.mesh") {
        PeerRole::Mesh
    } else if region == "OBS" || attributes.contains("role.observer") {
        PeerRole::Observer
    } else if region == "CLIENT" || attributes.contains("role.client") {
        PeerRole::Client
    } else {
        PeerRole::Worker
    }
}

fn parse_connect_auth_claims(spec: &str) -> HashMap<String, PeerClaims> {
    let mut claims = HashMap::new();
    for entry in spec.split(',') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        let mut parts = entry.splitn(3, ':');
        let Some(token) = parts.next().map(str::trim).filter(|s| !s.is_empty()) else {
            continue;
        };
        let Some(region) = parts.next().map(str::trim).filter(|s| !s.is_empty()) else {
            continue;
        };
        let attributes = parts
            .next()
            .unwrap_or("")
            .split('|')
            .filter_map(|attr| {
                let attr = attr.trim();
                (!attr.is_empty()).then(|| attr.to_string())
            })
            .collect();
        claims.insert(
            token.to_string(),
            PeerClaims {
                region: region.to_string(),
                attributes,
            },
        );
    }
    claims
}

fn parse_connect_auth_claims_env() -> HashMap<String, PeerClaims> {
    parse_nonempty_env("GW_AUTH_CLAIMS")
        .map(|spec| parse_connect_auth_claims(&spec))
        .unwrap_or_default()
}

fn is_broker_owned_attribute(attr: &str) -> bool {
    matches!(
        attr,
        "kernel_admin"
            | "acl_admin"
            | "inspector"
            | "snapshot"
            | "ops"
            | "debug"
            | "observer"
            | "role.mesh"
            | "role.observer"
            | "role.client"
    )
}

fn strip_peer_declared_broker_owned_attributes(attributes: &mut HashSet<String>) {
    attributes.retain(|attr| !is_broker_owned_attribute(attr));
}

fn broker_owned_connect_region(region: &str) -> bool {
    matches!(region, "MESH" | "OBS" | "CLIENT" | "STANDBY")
}

// ── DYNAMIC SPLITTING (load-based capacity-add, not just the W|E boundary-shift) ──
// A coarse region under sustained high load SPLITS into sub-bands, each owned by a fresh
// standby worker -- this ADDS capacity (rebalance() only shifts the W|E line between two
// fixed workers). `splits[region]` = sorted sub-boundaries within that region's x-range;
// sub-band 0 keeps the region's own name (its original worker), bands 1.. are "<region>#i"
// (new workers). Absent/empty => no split => the coarse region is returned unchanged, so
// the existing W/E + mesh paths are untouched until a split actually happens.
fn refine_region(
    coarse: &str,
    x: f64,
    splits: &std::collections::HashMap<String, Vec<f64>>,
) -> String {
    match splits.get(coarse) {
        Some(subs) if !subs.is_empty() => {
            for (i, b) in subs.iter().enumerate() {
                if x < *b {
                    return if i == 0 {
                        coarse.to_string()
                    } else {
                        format!("{coarse}#{i}")
                    };
                }
            }
            format!("{coarse}#{}", subs.len())
        }
        _ => coarse.to_string(),
    }
}

// ── N-ZONE 1D-STRIP PARTITION (the generalization of the W|E split) ──────────────────────────────
// The partition model is a 1D STRIP of N zones along x, cut by a SORTED list of boundaries:
//   `bounds = []`        -> ONE zone "E" (the all-positive default; the original const-BOUNDARY=0 behaviour)
//   `bounds = [b0]`      -> TWO zones W (x<b0) | E (x>=b0)  -- BYTE-FOR-BYTE the proven 2-zone path
//   `bounds = [b0,b1,..]`-> N zones  Z0 (x<b0) | Z1 [b0,b1) | .. | Z{k} (x>=b_{k-1})
// A unit crossing ANY strip edge auto-handoffs to the neighbour that owns the strip (the SAME position-driven
// handoff proven for W|E in S3 -- mesh routing/conservation/fencing are already N-neighbour generic). The
// W|E names are KEPT for the 0/1-boundary case so every existing 2-zone gate + the W/E worker pass unchanged;
// >=2 boundaries switch to Z-indexed strip names. `boundary: f64` (the DYNAMIC load-balance line) is the
// 1-boundary special case -- a single-element `boundaries` carries it; rebalance() still shifts that one line.

// Parse the partition cut points from the environment. GW_BOUNDARIES="50,100,150" (any whitespace tolerated)
// => a SORTED, de-duplicated list of N-1 cuts for N strips. Absent => the single GW_BOUNDARY (or BOUNDARY=0.0)
// as a 1-element list (the proven W|E split). Empty/garbage entries are skipped; an empty result falls back to
// [BOUNDARY] so region-assignment always has at least the one all-positive-x => "E" cut (today's default).
fn parse_boundaries() -> Vec<f64> {
    if let Ok(spec) = std::env::var("GW_BOUNDARIES") {
        let mut v: Vec<f64> = spec
            .split(',')
            .filter_map(|s| s.trim().parse::<f64>().ok())
            .filter(|f| f.is_finite())
            .collect();
        v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        v.dedup();
        if !v.is_empty() {
            return v;
        }
    }
    let b = std::env::var("GW_BOUNDARY")
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
        .filter(|f| f.is_finite())
        .unwrap_or(BOUNDARY);
    vec![b]
}

// ── 2D GRID PARTITION (D1: square zones instead of 1D strips) ──────────────────────────────────────
// GW_GRID2D="<cols>x<rows>" (+ GW_ARENA="<w>[,<h>]", default 5000) => a COLSxROWS grid of square-ish
// zones named "Z<col>_<row>", derived from BOTH x AND y (the 1D strip path is along x only). When set,
// the broker auto-hands-off by 2D cell (per-axis hysteresis, mirroring region_after's W|E band). Absent
// => None => the 1D-strip path is byte-for-byte unchanged. Note: CONFIG (a partition MODEL choice, like
// GW_BOUNDARIES), not a per-call flag -- the handoff stays automatic + structural.
fn parse_grid2d_values(spec: &str, arena: Option<&str>) -> Option<(usize, usize, f64, f64)> {
    let (cs, rs) = spec.split_once('x')?;
    let cols: usize = cs.trim().parse().ok()?;
    let rows: usize = rs.trim().parse().ok()?;
    if cols == 0 || rows == 0 {
        return None;
    }
    let mut it = arena
        .unwrap_or_default()
        .split(',')
        .filter_map(|s| s.trim().parse::<f64>().ok());
    let aw = it.next().unwrap_or(5000.0);
    let ah = it.next().unwrap_or(aw);
    if !aw.is_finite() || !ah.is_finite() || aw <= 0.0 || ah <= 0.0 {
        return None;
    }
    let cw = aw / cols as f64;
    let ch = ah / rows as f64;
    if !cw.is_finite() || !ch.is_finite() || cw <= 0.0 || ch <= 0.0 {
        return None;
    }
    Some((cols, rows, cw, ch))
}

fn parse_grid2d() -> Option<(usize, usize, f64, f64)> {
    let spec = std::env::var("GW_GRID2D").ok()?;
    let arena = std::env::var("GW_ARENA").ok();
    parse_grid2d_values(&spec, arena.as_deref())
}

// The 2D cell name "Z<col>_<row>" for a position.
fn region_2d(pos: [f64; 2], cols: usize, rows: usize, cw: f64, ch: f64) -> String {
    let cx = ((pos[0] / cw).floor() as i64).clamp(0, cols as i64 - 1);
    let cy = ((pos[1] / ch).floor() as i64).clamp(0, rows as i64 - 1);
    format!("Z{cx}_{cy}")
}

// Parse a 2D cell name "Z<cx>_<cy>" back to indices (None if not a 2D cell name, e.g. a strip "Z3"/"W").
fn parse_grid_cell(name: &str) -> Option<(i64, i64)> {
    let s = name.strip_prefix('Z')?;
    let (a, b) = s.split_once('_')?;
    Some((a.parse().ok()?, b.parse().ok()?))
}

// Per-axis hysteretic next cell index: commit to the neighbour cell only once past the shared edge by
// +/- H (so a unit straddling a cell seam doesn't ping-pong) -- the 2D analogue of region_after's band.
fn hysteretic_cell(p: f64, cur: i64, cell: f64, n: usize) -> i64 {
    let upper = (cur + 1) as f64 * cell;
    let lower = cur as f64 * cell;
    if p >= upper + H || p < lower - H {
        ((p / cell).floor() as i64).clamp(0, n as i64 - 1)
    } else {
        cur.clamp(0, n as i64 - 1)
    }
}

// Where a unit in `current` (a "Z<cx>_<cy>" cell) moves to after reaching pos, with per-axis hysteresis.
// If `current` is not a 2D cell name (fresh spawn / cross-broker adopt), snap to the geometric cell.
fn region_2d_after(
    pos: [f64; 2],
    current: &str,
    cols: usize,
    rows: usize,
    cw: f64,
    ch: f64,
) -> String {
    match parse_grid_cell(current) {
        Some((cx, cy)) => format!(
            "Z{}_{}",
            hysteretic_cell(pos[0], cx, cw, cols),
            hysteretic_cell(pos[1], cy, ch, rows)
        ),
        None => region_2d(pos, cols, rows, cw, ch),
    }
}

// The strip INDEX x falls into, given the sorted boundary list (0..=bounds.len()).
fn strip_index(x: f64, bounds: &[f64]) -> usize {
    let mut i = 0;
    while i < bounds.len() && x >= bounds[i] {
        i += 1;
    }
    i
}

// The zone NAME for a strip index. 0/1-boundary => W|E (back-compat); >=2 boundaries => Z<i>.
fn strip_name(idx: usize, bounds: &[f64]) -> String {
    if bounds.len() <= 1 {
        // 0 boundaries => single zone "E"; 1 boundary => W (idx 0) | E (idx 1). The original W|E partition.
        if idx == 0 && !bounds.is_empty() {
            "W".to_string()
        } else {
            "E".to_string()
        }
    } else {
        format!("Z{idx}")
    }
}

// The strip index a strip NAME denotes (inverse of strip_name), or None if it is not a continuous-partition
// zone of THIS topology. W=>0, E=>last; Z<i> parses i. Used to apply edge-hysteresis on the ACTIVE strip.
fn strip_index_of_name(name: &str, bounds: &[f64]) -> Option<usize> {
    match name {
        "W" => Some(0),
        "E" => Some(bounds.len()), // the last strip (1 boundary => idx 1; 0 boundaries => idx 0)
        other => other
            .strip_prefix('Z')
            .and_then(|n| n.parse::<usize>().ok())
            .filter(|i| *i <= bounds.len()),
    }
}

fn initial_region(x: f64, bounds: &[f64]) -> String {
    strip_name(strip_index(x, bounds), bounds)
}

// Where `current` moves to after reaching x, with HYSTERESIS on the active strip's two edges (so a unit
// straddling a seam doesn't ping-pong). Generalizes the W|E [b-H,b+H] band to per-edge bands on N strips:
// the unit only commits to the next strip up once x >= upper_edge + H, or down once x < lower_edge - H.
fn region_after(x: f64, current: &str, bounds: &[f64]) -> String {
    let cur_idx = match strip_index_of_name(current, bounds) {
        Some(i) => i,
        None => return current.to_string(),
    };
    // commit UP across the strip's upper edge (bounds[cur_idx]) only past the +H hysteresis band
    if cur_idx < bounds.len() && x >= bounds[cur_idx] + H {
        // jump as many strips as the position warrants (a big teleport crosses several edges at once)
        return strip_name(strip_index(x, bounds), bounds);
    }
    // commit DOWN across the strip's lower edge (bounds[cur_idx-1]) only past the -H hysteresis band
    if cur_idx > 0 && x < bounds[cur_idx - 1] - H {
        return strip_name(strip_index(x, bounds), bounds);
    }
    current.to_string()
}

fn coarse_region(region: &str) -> &str {
    region.split('#').next().unwrap_or(region)
}

// A region label that names a 1D-strip partition zone (W/E for 0-1 boundaries, Z<i> for N) -- as opposed to
// a NAMED region (a planet/portal zone honored verbatim, never position-re-derived). The topology is implied
// by the broker's boundary list; without it (a worker that never set GW_BOUNDAR*) only W|E count, so a plain
// "Z3" on a single-zone broker is treated as a named region (unchanged behaviour).
fn is_strip_region_name(region: &str, bounds: &[f64]) -> bool {
    let coarse = coarse_region(region);
    if bounds.len() <= 1 {
        return strip_index_of_name(coarse, bounds).is_some();
    }
    coarse
        .strip_prefix('Z')
        .filter(|n| !n.is_empty() && n.chars().all(|c| c.is_ascii_digit()))
        .and_then(|n| n.parse::<usize>().ok())
        .map(|i| i <= bounds.len())
        .unwrap_or(false)
}

fn movement_region_after(
    x: f64,
    current: &str,
    bounds: &[f64],
    splits: &std::collections::HashMap<String, Vec<f64>>,
) -> String {
    let coarse = coarse_region(current);
    let next = if is_strip_region_name(coarse, bounds) {
        region_after(x, coarse, bounds)
    } else {
        coarse.to_string()
    };
    refine_region(&next, x, splits)
}

fn spawn_region(
    pos: [f64; 2],
    requested: Option<&str>,
    bounds: &[f64],
    splits: &std::collections::HashMap<String, Vec<f64>>,
) -> String {
    match requested.map(str::trim).filter(|r| !r.is_empty()) {
        // a NAMED region (not a strip zone of this topology) is honored verbatim (planet/portal zones).
        Some(region) if !is_strip_region_name(region, bounds) => {
            refine_region(coarse_region(region), pos[0], splits)
        }
        // a strip zone request (W/E/Z<i>) OR none: the position picks the strip (the source of truth).
        _ => refine_region(&initial_region(pos[0], bounds), pos[0], splits),
    }
}

// CROSS-BROKER ADOPT region label (the cross-broker fold A->B region-mislabel fix). When this broker ADOPTS an
// entity handed across the process seam, the entity now belongs to one of THIS broker's OWNED zones -- its
// `region` must name that zone, NOT be re-derived from position against this broker's local W|E boundary
// (the old `spawn_in_region(..., None, ...)` path did the latter -> a unit Folded into ZB at x<0 got
// mislabeled "W" instead of "ZB"). The receiving region, by most-authoritative owned source available:
//   1. in 2D-grid mode, the addressed grid cell if THIS broker owns it; otherwise reject without ACK
//      (a parseable cell name is not enough -- the receiver must actually own that cell).
//   2. else `my_region` (this broker's GW_ADVERTISE'd owned region) -- set in the registry/advertise topology
//      (the advertise-topology tests): the broker IS that zone, so adopt -> it.
//   3. else the `target` the sender addressed, IF this broker actually owns it (a local worker leases it:
//      region_worker has it) -- the static-GW_MESH topology (mesh_soak owns "E", routed target "E") where
//      my_region is empty but the broker still owns the target zone.
//   4. else fall back to the position-derived region (preserves the prior behavior for a topology that
//      advertises nothing AND doesn't own the target as a named zone -- e.g. nzone's "EARTH" target landing
//      on a broker whose only owned zone is "E": there is no better label than the geometric one).
fn receiving_region_for_adopt(
    state: &ServerState,
    pos: [f64; 2],
    target: Option<&str>,
) -> Option<String> {
    if let Some((c, r, cw, ch)) = state.grid2d {
        if let Some(t) = target.map(str::trim).filter(|t| !t.is_empty()) {
            if let Some((col, row)) = parse_grid_cell(t) {
                let in_bounds = col >= 0 && row >= 0 && col < c as i64 && row < r as i64;
                if state.region_worker.contains_key(t) || state.my_region == t {
                    return in_bounds.then(|| t.to_string());
                }
                return None;
            }
        }
        let geometric = region_2d(pos, c, r, cw, ch);
        if state.region_worker.contains_key(&geometric) || state.my_region == geometric {
            return Some(geometric);
        }
        return None;
    }
    if !state.my_region.is_empty() {
        return Some(state.my_region.clone());
    }
    if let Some(t) = target.map(str::trim).filter(|t| !t.is_empty()) {
        if state.region_worker.contains_key(t) {
            return Some(refine_region(coarse_region(t), pos[0], &state.splits));
        }
    }
    Some(spawn_region(pos, None, &state.boundaries, &state.splits))
}

fn existing_mesh_adopt_matches(
    state: &ServerState,
    eid: &str,
    adopt_region: &str,
    authority_epoch: Option<u64>,
) -> bool {
    let Some(e) = state.entities.get(eid) else {
        return false;
    };
    if e.region != adopt_region {
        return false;
    }
    if let Some(inbound_epoch) = authority_epoch {
        let current_epoch = physics_island_component_names(&e.components, &e.authority)
            .iter()
            .map(|comp| component_authority_epoch(e, comp))
            .max()
            .unwrap_or_else(|| component_authority_epoch(e, "pos"));
        if current_epoch != inbound_epoch {
            return false;
        }
    }
    true
}

fn is_platform_reserved_component(comp: &str) -> bool {
    comp == "kernel"
        || comp.starts_with("kernel.")
        || comp == "ownership"
        || comp.starts_with("ownership.")
        || comp == "zone.law"
        || comp == "threshold.tx"
        || comp == "authority.mode"
}

#[derive(Clone, Debug)]
struct ComponentAuthority {
    owner: Option<String>,
    epoch: u64,
    mode: AuthorityMode,
}

#[derive(Clone)]
struct Entity {
    pos: [f64; 2],
    vel: [f64; 2],
    components: Map<String, Value>,
    region: String,
    version: u64,
    authority: HashMap<String, ComponentAuthority>,
    last_broadcast_cell: Option<(i64, i64)>, // Interest: the cell at the last propagate -> notify viewers it LEAVES (no stale ghost). Init to the create cell so the FIRST move is covered.
}

// ── CROSS-BROKER SEAM-INTEREST: a read-only GHOST of a neighbour broker's near-seam entity ──────────────
// The missing half of the N-zone tiling (handoff ✅; interest was the gap): a unit at the border of zone A
// must SEE + TARGET a unit in the adjacent zone B WITHOUT either crossing, so the contiguous-world illusion
// holds. Each broker PUSHES its OWN entities within a BORDER BAND of width `interest_band` around every seam
// to its meshed neighbour(s) over the EXISTING mesh channel (the same `state.mesh` Sender used for handoff,
// already retry-resilient); the neighbour holds them HERE -- in `ghosts`, NOT in `entities`.
//
// THE KEY INVARIANT (why this can NEVER re-introduce split-brain / packet-loss): a ghost lives in a SEPARATE
// map from `entities`. Authority is granted ONLY over `entities` (grant_authority/region leases operate on
// `entities`), and a component WRITE is rejected for any eid not in `entities` (validate_and_apply_nosync's
// first gate). So a ghost is STRUCTURALLY non-authoritative -- it cannot be leased, cannot be granted, and a
// forged UpdateComponent/claim on it is rejected. Reading a ghost grants nothing. The ACTUAL cross-seam kill
// reuses the PROVEN S3 path (a projectile crosses the seam via MeshHandoff -> the owning broker B applies the
// AoE under its OWN sole authority). Authority STAYS with B. No authority transfer => no split-brain. The
// ghost is transient (NOT WAL'd -- is_persistent_op is false for MeshGhost): a ghost vanishing on restart is
// correct (B re-pushes it), so there is no durability fork either. The read-only-ness is the STRUCTURE (a
// distinct map), not a flag; `interest_band` is CONFIG (GW_INTEREST_BAND), default 0 => no push => byte-for-
// byte the current behaviour for every existing gate.
struct GhostEntity {
    pos: [f64; 2],
    vel: [f64; 2],
    components: Map<String, Value>,
    owner_region: String, // the SOURCE zone (the neighbour broker's owned region) that holds authority -- for the read-only reject message + the observer tag
    last_seen: Instant, // refreshed on each push; a ghost not refreshed within GHOST_TTL is reaped (it left the band / its source went away)
}

struct WorkerHandle {
    region: String,
    role: PeerRole,
    attributes: HashSet<String>, // worker-type attributes (ACL / LB constraints)
    view: HashSet<String>,
    authority_epochs: HashMap<String, u64>,
    aoi_center: Option<[f64; 2]>,
    aoi_radius: Option<f64>,
    fidelity_full_radius: Option<f64>,
    fidelity_coarse_rate: u64,
    fidelity_coarse_grid: f64,
    fidelity_seq: HashMap<String, u64>,
    tx: Sender<Vec<u8>>, // T1: BOUNDED (capacity CHANNEL_CAP) -- the structural hard cap on this consumer's egress RAM
    out_queue: Arc<AtomicU64>, // G4 egress backlog depth (enqueued-by-emit minus dequeued-by-writer) -- backpressure visibility
    dropped: Arc<AtomicU64>, // G4 count of degradable frames dropped under backpressure (slow consumer)
    disconnect: Arc<AtomicBool>, // T1: set when this consumer hit the hard egress cap on a CRITICAL frame -> reap_disconnecting tears it down (it reconnects + re-checks-out, no silent loss)
    grid_cells: Vec<(i64, i64)>, // Interest: the spatial-hash cells this worker's AOI occupies (tracked for removal on re-interest / disconnect)
    ingress_tokens: f64,         // Security v0: per-peer token bucket for valid inbound frames
    ingress_last_refill: Instant,
    ingress_rejected: u64,
}

impl WorkerHandle {
    fn has_global_observer_claim(&self) -> bool {
        if self.role != PeerRole::Observer {
            return false;
        }
        self.attributes
            .iter()
            .any(|a| a == "observer" || a == "debug" || a == "inspector")
    }

    fn default_region_interest(&self, pos: [f64; 2]) -> bool {
        match self.role {
            PeerRole::Observer => self.has_global_observer_claim(),
            PeerRole::Mesh => false, // a cross-broker mesh link is a conduit, not an interest-holder
            PeerRole::Client => false, // clients opt into AOI explicitly; they never own a default region view
            PeerRole::Worker => match self.region.as_str() {
                "W" => pos[0] < H + INTEREST_MARGIN,
                _ => pos[0] >= -H - INTEREST_MARGIN,
            },
        }
    }

    fn should_be_global_interest_holder(&self) -> bool {
        self.aoi_center.is_some() || self.default_region_interest([0.0, 0.0])
    }

    fn interested_in(&self, pos: [f64; 2]) -> bool {
        if let Some(c) = self.aoi_center {
            let r = self.aoi_radius.unwrap_or(0.0);
            let dx = pos[0] - c[0];
            let dy = pos[1] - c[1];
            return dx * dx + dy * dy <= r * r;
        }
        self.default_region_interest(pos)
    }

    fn full_fidelity_for(&self, pos: [f64; 2]) -> bool {
        match (self.aoi_center, self.fidelity_full_radius) {
            (Some(c), Some(r)) => {
                let dx = pos[0] - c[0];
                let dy = pos[1] - c[1];
                dx * dx + dy * dy <= r * r
            }
            _ => true,
        }
    }
}

// the worker-attribute required to WRITE `comp` on entity `e`, or None (unrestricted)
fn acl_write_attr(e: &Entity, comp: &str) -> Option<String> {
    e.components
        .get("acl")
        .and_then(|a| a.get("write"))
        .and_then(|w| w.get(comp))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

// the attribute that grants CLIENT-authoritative write to `comp` (player-authoritative: the holder
// writes WITHOUT owning the region -- a client driving its own avatar), or None. Declared in the
// entity's `acl`: {"client_write": {comp: attr}} -- a client-authoritative-components partition.
fn acl_client_write_attr_from_components(comps: &Map<String, Value>, comp: &str) -> Option<String> {
    comps
        .get("acl")
        .and_then(|a| a.get("client_write"))
        .and_then(|w| w.get(comp))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

fn acl_client_write_attr(e: &Entity, comp: &str) -> Option<String> {
    acl_client_write_attr_from_components(&e.components, comp)
}

fn is_kernel_locked_component(comp: &str) -> bool {
    comp == "kernel"
        || comp.starts_with("kernel.")
        || comp == "ownership"
        || comp.starts_with("ownership.")
        || comp == "zone.law"
}

fn is_spatial_component(comp: &str) -> bool {
    matches!(
        comp,
        "pos"
            | "vel"
            | "phys"
            | "physics"
            | "rot"
            | "lin"
            | "ang"
            | "at_rest"
            | "gen"
            | "t_server"
            | "sim_time"
    ) || comp.starts_with("phys.")
        || comp.starts_with("physics.")
}

fn authority_mode_from_value(v: &Value) -> Option<AuthorityMode> {
    if let Some(s) = v.as_str() {
        AuthorityMode::from_wire_str(s)
    } else {
        v.get("mode")
            .and_then(|m| m.as_str())
            .and_then(AuthorityMode::from_wire_str)
    }
}

fn authority_owner_from_value(v: &Value) -> Option<String> {
    if let Some(o) = v.get("owner").and_then(|m| m.as_str()) {
        if !o.is_empty() {
            return Some(o.to_string());
        }
    }
    None
}

fn authority_epoch_from_value(v: &Value) -> Option<u64> {
    v.get("authority_epoch")
        .or_else(|| v.get("epoch"))
        .and_then(|m| m.as_u64())
}

fn authority_spec_for<'a>(comps: &'a Map<String, Value>, comp: &str) -> Option<&'a Value> {
    comps
        .get("authority.mode")
        .and_then(|m| m.as_object())
        .and_then(|m| m.get(comp))
}

fn default_authority_mode(comps: &Map<String, Value>, comp: &str) -> AuthorityMode {
    if let Some(v) = authority_spec_for(comps, comp) {
        if let Some(mode) = authority_mode_from_value(v) {
            return mode;
        }
    }
    if is_kernel_locked_component(comp) {
        AuthorityMode::PersistentKernelLock
    } else if comp == "threshold.tx" {
        AuthorityMode::ThresholdOverlap
    } else if acl_client_write_attr_from_components(comps, comp).is_some() {
        AuthorityMode::ClientForwardSparse
    } else if is_spatial_component(comp) {
        AuthorityMode::ServerPhysicsIsland
    } else {
        AuthorityMode::ServerArbitrated
    }
}

fn initial_authority_map(
    comps: &Map<String, Value>,
    epoch: u64,
) -> HashMap<String, ComponentAuthority> {
    let mut names: HashSet<String> = comps.keys().cloned().collect();
    names.insert("pos".to_string());
    names.insert("vel".to_string());
    if let Some(spec) = comps.get("authority.mode").and_then(|m| m.as_object()) {
        for comp in spec.keys() {
            names.insert(comp.clone());
        }
    }
    let mut authority = HashMap::new();
    for comp in names {
        let spec = authority_spec_for(comps, &comp);
        authority.insert(
            comp.clone(),
            ComponentAuthority {
                owner: spec.and_then(authority_owner_from_value),
                epoch: spec.and_then(authority_epoch_from_value).unwrap_or(epoch),
                mode: default_authority_mode(comps, &comp),
            },
        );
    }
    authority
}

fn apply_authority_snapshot(e: &mut Entity, snapshot: &Value) {
    let Some(map) = snapshot.as_object() else {
        return;
    };
    for (comp, spec) in map {
        let mode = authority_mode_from_value(spec)
            .unwrap_or_else(|| default_authority_mode(&e.components, comp));
        let owner = authority_owner_from_value(spec);
        let epoch =
            authority_epoch_from_value(spec).unwrap_or_else(|| component_authority_epoch(e, comp));
        e.authority
            .insert(comp.clone(), ComponentAuthority { owner, epoch, mode });
    }
}

fn authority_to_json(authority: &HashMap<String, ComponentAuthority>) -> Value {
    let mut out = Map::new();
    for (comp, ca) in authority {
        out.insert(
            comp.clone(),
            json!({
                "owner": ca.owner.clone(),
                "authority_epoch": ca.epoch,
                "mode": ca.mode.as_wire_str()
            }),
        );
    }
    Value::Object(out)
}

fn ensure_component_authority(e: &mut Entity, comp: &str) {
    if e.authority.contains_key(comp) {
        return;
    }
    let epoch = e.authority.get("pos").map(|ca| ca.epoch).unwrap_or(1);
    e.authority.insert(
        comp.to_string(),
        ComponentAuthority {
            owner: authority_spec_for(&e.components, comp).and_then(authority_owner_from_value),
            epoch,
            mode: default_authority_mode(&e.components, comp),
        },
    );
}

fn component_authority_epoch(e: &Entity, comp: &str) -> u64 {
    e.authority
        .get(comp)
        .or_else(|| {
            if matches!(comp, "delete" | "threshold.tx" | "fold") {
                e.authority.get("pos")
            } else {
                None
            }
        })
        .map(|ca| ca.epoch)
        .unwrap_or(1)
}

fn set_component_authority_epoch(e: &mut Entity, comp: &str, epoch: u64) {
    ensure_component_authority(e, comp);
    if let Some(ca) = e.authority.get_mut(comp) {
        ca.epoch = epoch;
    }
}

fn snapshot_authority_hash(entities: &HashMap<String, Entity>) -> u64 {
    let mut ids: Vec<String> = entities.keys().cloned().collect();
    ids.sort();
    let mut authority_hash: u64 = 0xcbf29ce484222325;
    for eid in &ids {
        for b in eid.bytes() {
            authority_hash = (authority_hash ^ b as u64).wrapping_mul(0x100000001b3);
        }
        if let Some(e) = entities.get(eid) {
            if let Some(ca) = e.authority.get("pos") {
                authority_hash = authority_hash.wrapping_add(ca.epoch);
                if let Some(o) = &ca.owner {
                    for b in o.bytes() {
                        authority_hash = (authority_hash ^ b as u64).wrapping_mul(0x100000001b3);
                    }
                }
            }
        }
    }
    authority_hash
}

fn bump_component_authority_epoch(e: &mut Entity, comp: &str) -> u64 {
    ensure_component_authority(e, comp);
    let ca = e.authority.get_mut(comp).unwrap();
    ca.epoch = ca.epoch.saturating_add(1);
    ca.epoch
}

fn physics_island_component_names(
    comps: &Map<String, Value>,
    authority: &HashMap<String, ComponentAuthority>,
) -> Vec<String> {
    let mut names: HashSet<String> = authority.keys().cloned().collect();
    for comp in comps.keys() {
        names.insert(comp.clone());
    }
    names.insert("pos".to_string());
    names.insert("vel".to_string());
    let mut out: Vec<String> = names
        .into_iter()
        .filter(|comp| {
            authority
                .get(comp)
                .map(|ca| ca.mode == AuthorityMode::ServerPhysicsIsland)
                .unwrap_or_else(|| {
                    default_authority_mode(comps, comp) == AuthorityMode::ServerPhysicsIsland
                })
        })
        .collect();
    out.sort();
    out
}

fn set_physics_island_authority_epoch(e: &mut Entity, epoch: u64) -> Vec<String> {
    let comps = physics_island_component_names(&e.components, &e.authority);
    for comp in &comps {
        set_component_authority_epoch(e, comp, epoch);
    }
    comps
}

fn advance_physics_island_authority(
    e: &mut Entity,
    old_owner: Option<&str>,
    new_owner: Option<&str>,
) -> (u64, Vec<String>) {
    let comps = physics_island_component_names(&e.components, &e.authority);
    let epoch = comps
        .iter()
        .map(|comp| component_authority_epoch(e, comp))
        .max()
        .unwrap_or_else(|| component_authority_epoch(e, "pos"))
        .saturating_add(1);
    let mut moved = Vec::new();
    for comp in comps {
        ensure_component_authority(e, &comp);
        let ca = e.authority.get_mut(&comp).unwrap();
        if ca.mode != AuthorityMode::ServerPhysicsIsland {
            continue;
        }
        let owner_matches = match old_owner {
            Some(old) => ca.owner.as_deref() == Some(old) || ca.owner.is_none(),
            None => ca.owner.is_none(),
        };
        if owner_matches {
            ca.epoch = epoch;
            ca.owner = new_owner.map(str::to_string);
            moved.push(comp);
        }
    }
    (epoch, moved)
}

fn bump_spatial_authority_epoch(e: &mut Entity) -> u64 {
    let epoch = bump_component_authority_epoch(e, "pos");
    set_physics_island_authority_epoch(e, epoch);
    epoch
}

fn component_authority_mode(e: &Entity, comp: &str) -> AuthorityMode {
    e.authority
        .get(comp)
        .or_else(|| {
            if matches!(comp, "delete" | "threshold.tx" | "fold") {
                e.authority.get("pos")
            } else {
                None
            }
        })
        .map(|ca| ca.mode.clone())
        .unwrap_or_else(|| default_authority_mode(&e.components, comp))
}

fn component_authority_owner(e: &Entity, comp: &str) -> Option<String> {
    e.authority
        .get(comp)
        .or_else(|| {
            if matches!(comp, "delete" | "threshold.tx" | "fold") {
                e.authority.get("pos")
            } else {
                None
            }
        })
        .and_then(|ca| ca.owner.clone())
}

// whether a worker with `attrs` may READ a component bag.
fn acl_read_ok_components(attrs: &HashSet<String>, components: &Map<String, Value>) -> bool {
    if let Some(read) = components
        .get("acl")
        .and_then(|a| a.get("read"))
        .and_then(|r| r.as_array())
    {
        if !read.is_empty() {
            return read
                .iter()
                .filter_map(|v| v.as_str())
                .any(|a| attrs.contains(a));
        }
    }
    true
}

// whether a worker with `attrs` may READ (be checked out) entity `e`
fn acl_read_ok(attrs: &HashSet<String>, e: &Entity) -> bool {
    acl_read_ok_components(attrs, &e.components)
}

fn worker_has_attr(state: &ServerState, wid: &str, attr: &str) -> bool {
    state
        .workers
        .get(wid)
        .map(|w| w.attributes.contains(attr))
        .unwrap_or(false)
}

fn reject_kernel_reserved_write(state: &mut ServerState, wid: &str, eid: &str, comp: &str) -> bool {
    if is_platform_reserved_component(comp) && !worker_has_attr(state, wid, "kernel_admin") {
        emit(
            state,
            wid,
            json!({"op":"UpdateRejected","entity":eid,"comp":comp,
            "reason":"platform-reserved components require the 'kernel_admin' attribute"}),
        );
        true
    } else {
        false
    }
}

fn authoritative_entity_owner(state: &ServerState, wid: &str, eid: &str) -> Result<(), String> {
    let region = state
        .entities
        .get(eid)
        .map(|e| e.region.clone())
        .ok_or_else(|| "entity not found".to_string())?;
    let owner = state.region_worker.get(&region).cloned();
    if owner.as_deref() == Some(wid) || worker_has_attr(state, wid, "kernel_admin") {
        Ok(())
    } else {
        Err(format!(
            "not authoritative; owner={}",
            owner.unwrap_or_default()
        ))
    }
}

fn authoritative_component_writer(
    state: &ServerState,
    wid: &str,
    eid: &str,
    comp: &str,
) -> Result<(), String> {
    if worker_has_attr(state, wid, "kernel_admin") {
        return Ok(());
    }
    let e = state
        .entities
        .get(eid)
        .ok_or_else(|| "entity not found".to_string())?;
    if component_authority_mode(e, comp) == AuthorityMode::ClientForwardSparse {
        if let Some(attr) = acl_client_write_attr(e, comp) {
            if state
                .workers
                .get(wid)
                .map(|w| w.attributes.contains(&attr))
                .unwrap_or(false)
            {
                return Ok(());
            }
        }
    }
    if let Some(owner) = component_authority_owner(e, comp) {
        if owner == wid {
            return Ok(());
        }
        return Err(format!("not authoritative; owner={owner}"));
    }
    authoritative_entity_owner(state, wid, eid)
}

fn authority_key(eid: &str, comp: &str) -> String {
    format!("{eid}:{comp}")
}

fn grant_authority(state: &mut ServerState, wid: &str, eid: &str, comp: &str) {
    let (epoch, mode) = match state.entities.get_mut(eid) {
        Some(e) => {
            ensure_component_authority(e, comp);
            let ca = e.authority.get_mut(comp).unwrap();
            ca.owner = Some(wid.to_string());
            (ca.epoch, ca.mode.as_wire_str())
        }
        None => return,
    };
    if let Some(w) = state.workers.get_mut(wid) {
        w.authority_epochs.insert(authority_key(eid, comp), epoch);
    }
    emit(
        state,
        wid,
        json!({"op":"AuthorityChange","entity":eid,"comp":comp,
        "authoritative":true,"authority_epoch":epoch,"mode":mode}),
    );
}

fn revoke_authority(state: &mut ServerState, wid: &str, eid: &str, comp: &str) {
    let (epoch, mode) = match state.entities.get_mut(eid) {
        Some(e) => {
            ensure_component_authority(e, comp);
            let ca = e.authority.get_mut(comp).unwrap();
            if ca.owner.as_deref() == Some(wid) {
                ca.owner = None;
            }
            (ca.epoch, ca.mode.as_wire_str())
        }
        None => (0, "server_arbitrated"),
    };
    if let Some(w) = state.workers.get_mut(wid) {
        w.authority_epochs.remove(&authority_key(eid, comp));
    }
    emit(
        state,
        wid,
        json!({"op":"AuthorityChange","entity":eid,"comp":comp,
        "authoritative":false,"authority_epoch":epoch,"mode":mode}),
    );
}

fn region_physics_island_grants(state: &ServerState, wid: &str, eid: &str) -> Vec<String> {
    let Some(e) = state.entities.get(eid) else {
        return Vec::new();
    };
    if state.region_worker.get(&e.region).map(|s| s.as_str()) != Some(wid) {
        return Vec::new();
    }
    physics_island_component_names(&e.components, &e.authority)
        .into_iter()
        .filter(|comp| {
            component_authority_mode(e, comp) == AuthorityMode::ServerPhysicsIsland
                && acl_client_write_attr(e, comp).is_none()
                && component_authority_owner(e, comp)
                    .map(|owner| owner == wid)
                    .unwrap_or(true)
        })
        .collect()
}

fn grant_region_physics_island_authority(state: &mut ServerState, wid: &str, eid: &str) {
    let comps = region_physics_island_grants(state, wid, eid);
    for comp in comps {
        grant_authority(state, wid, eid, &comp);
    }
}

fn frame_authority_epoch(f: &Value) -> Option<u64> {
    f.get("authority_epoch")
        .or_else(|| f.get("epoch"))
        .and_then(|v| v.as_u64())
}

fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_millis() as u64
}

fn value_u64(v: &Value) -> Option<u64> {
    v.as_u64()
        .or_else(|| v.as_i64().and_then(|n| u64::try_from(n).ok()))
}

fn stored_component_u64(e: &Entity, comp: &str) -> Option<u64> {
    e.components.get(comp).and_then(value_u64)
}

fn stored_physics_field_u64(e: &Entity, field: &str) -> Option<u64> {
    e.components
        .get("physics")
        .and_then(|v| v.get(field))
        .and_then(value_u64)
}

fn next_gen_value(current: Option<u64>, supplied: &Value) -> u64 {
    match current {
        Some(v) => v.saturating_add(1),
        None => value_u64(supplied).unwrap_or(1),
    }
}

fn next_server_time_value(current: Option<u64>) -> u64 {
    let floor = current.map(|v| v.saturating_add(1)).unwrap_or(0);
    floor.max(unix_time_ms())
}

// G1 cross-broker time-interpolation: sim_time is a WALL-CLOCK-INDEPENDENT logical clock. Unlike t_server
// (unix_time_ms-based -> skews across brokers with different wall-clocks -> seam teleport/rubberbanding),
// sim_time accumulates a nominal sim step per accepted sample, so a client can interpolate by it ACROSS a
// seam without cross-broker clock skew. A worker may supply a larger value (its real dt * zone_time_scale);
// the broker takes the max to stay monotonic and continuous on handoff (the gen-twin for interpolation TIME).
const SIM_DT_MS: u64 = 16;

fn next_sim_time_value(current: Option<u64>, supplied: &Value) -> u64 {
    let supplied_v = value_u64(supplied).unwrap_or(0);
    match current {
        Some(v) => v.saturating_add(SIM_DT_MS).max(supplied_v),
        None => supplied_v.max(SIM_DT_MS),
    }
}

fn normalize_physics_clock_write(e: &Entity, comp: &str, value: Value) -> Value {
    match comp {
        "gen" => json!(next_gen_value(stored_component_u64(e, "gen"), &value)),
        "t_server" => json!(next_server_time_value(stored_component_u64(e, "t_server"))),
        "sim_time" => json!(next_sim_time_value(
            stored_component_u64(e, "sim_time"),
            &value
        )),
        "physics" => {
            let Some(obj) = value.as_object() else {
                return value;
            };
            let mut out = obj.clone();
            let supplied_gen = out.get("gen").unwrap_or(&Value::Null);
            out.insert(
                "gen".to_string(),
                json!(next_gen_value(
                    stored_physics_field_u64(e, "gen"),
                    supplied_gen
                )),
            );
            out.insert(
                "t_server".to_string(),
                json!(next_server_time_value(stored_physics_field_u64(
                    e, "t_server"
                ))),
            );
            let supplied_sim = out.get("sim_time").cloned().unwrap_or(Value::Null);
            out.insert(
                "sim_time".to_string(),
                json!(next_sim_time_value(
                    stored_physics_field_u64(e, "sim_time"),
                    &supplied_sim
                )),
            );
            Value::Object(out)
        }
        _ => value,
    }
}

fn authority_bound_f64(e: &Entity, comp: &str, keys: &[&str]) -> Option<f64> {
    let spec = authority_spec_for(&e.components, comp)?;
    for key in keys {
        if let Some(v) = spec
            .get(*key)
            .or_else(|| spec.get("bounds").and_then(|b| b.get(*key)))
        {
            if let Some(n) = v.as_f64() {
                return Some(n.max(0.0));
            }
        }
    }
    None
}

fn authority_spec_string(e: &Entity, comp: &str, keys: &[&str]) -> Option<String> {
    let spec = authority_spec_for(&e.components, comp)?;
    for key in keys {
        if let Some(v) = spec
            .get(*key)
            .or_else(|| spec.get("bounds").and_then(|b| b.get(*key)))
        {
            if let Some(s) = v.as_str() {
                if !s.is_empty() {
                    return Some(s.to_string());
                }
            }
        }
    }
    None
}

fn reject_client_forward_sparse_envelope(
    state: &mut ServerState,
    wid: &str,
    eid: &str,
    comp: &str,
    value: &Value,
) -> bool {
    let reason = {
        let Some(e) = state.entities.get(eid) else {
            return false;
        };
        if component_authority_mode(e, comp) != AuthorityMode::ClientForwardSparse {
            None
        } else if comp == "pos" {
            authority_bound_f64(
                e,
                comp,
                &["max_position_delta", "max_pos_delta", "max_delta", "maxPositionDelta"],
            )
            .and_then(|max_delta| {
                let next = arr2(Some(value));
                let dx = next[0] - e.pos[0];
                let dy = next[1] - e.pos[1];
                let distance = (dx * dx + dy * dy).sqrt();
                if distance > max_delta {
                    Some(format!(
                        "client_forward_sparse envelope: pos delta {:.3} exceeds max_position_delta {:.3}",
                        distance, max_delta
                    ))
                } else {
                    None
                }
            })
        } else {
            None
        }
    };
    if let Some(reason) = reason {
        emit(
            state,
            wid,
            json!({"op":"UpdateRejected","entity":eid,"comp":comp,
            "reason":reason,"mode":"client_forward_sparse"}),
        );
        true
    } else {
        false
    }
}

fn contact_arbitration_owner(state: &ServerState, a: &str, b: &str, comp: &str) -> Option<String> {
    state
        .entities
        .get(a)
        .and_then(|e| {
            authority_spec_string(
                e,
                comp,
                &[
                    "arbitration_owner",
                    "contact_owner",
                    "contactArbiter",
                    "arbiter",
                ],
            )
        })
        .or_else(|| {
            state.entities.get(b).and_then(|e| {
                authority_spec_string(
                    e,
                    comp,
                    &[
                        "arbitration_owner",
                        "contact_owner",
                        "contactArbiter",
                        "arbiter",
                    ],
                )
            })
        })
        .or_else(|| {
            state
                .entities
                .get(a)
                .and_then(|e| state.region_worker.get(&e.region).cloned())
        })
}

fn escalate_component_to_server_arbitrated(
    state: &mut ServerState,
    eid: &str,
    comp: &str,
    owner: Option<String>,
    reason: &str,
) {
    let (old_owner, authority_epoch, version, authority) = {
        let Some(e) = state.entities.get_mut(eid) else {
            return;
        };
        ensure_component_authority(e, comp);
        let ca = e.authority.get_mut(comp).unwrap();
        let old_owner = ca.owner.clone();
        ca.mode = AuthorityMode::ServerArbitrated;
        ca.owner = owner.clone();
        ca.epoch = ca.epoch.saturating_add(1);
        let authority_epoch = ca.epoch;
        e.version += 1;
        (
            old_owner,
            authority_epoch,
            e.version,
            authority_to_json(&e.authority),
        )
    };
    let _ = state.wal_append(&json!({
        "kind":"component_authority","entity":eid,"version":version,
        "writer":"broker.contact","comp":comp,"owner":owner.clone(),
        "authority_epoch":authority_epoch,"mode":"server_arbitrated",
        "reason":reason,"authority":authority
    }));
    if let Some(old) = old_owner {
        if owner.as_deref() != Some(old.as_str()) {
            if let Some(w) = state.workers.get_mut(&old) {
                w.authority_epochs.remove(&authority_key(eid, comp));
            }
            emit(
                state,
                &old,
                json!({"op":"AuthorityChange","entity":eid,"comp":comp,
                "authoritative":false,"authority_epoch":authority_epoch,
                "mode":"server_arbitrated","reason":reason}),
            );
        }
    }
    if let Some(owner_wid) = owner.as_deref() {
        if state.workers.contains_key(owner_wid) {
            grant_authority(state, owner_wid, eid, comp);
        }
    }
}

fn reject_and_escalate_contact_risk(
    state: &mut ServerState,
    wid: &str,
    eid: &str,
    comp: &str,
    value: &Value,
) -> bool {
    if comp != "pos" {
        return false;
    }
    let (next, radius) = {
        let Some(e) = state.entities.get(eid) else {
            return false;
        };
        if component_authority_mode(e, comp) != AuthorityMode::ClientForwardSparse {
            return false;
        }
        let Some(radius) = authority_bound_f64(
            e,
            comp,
            &[
                "contact_radius",
                "contactRadius",
                "contact_margin",
                "contactMargin",
            ],
        ) else {
            return false;
        };
        (arr2(Some(value)), radius)
    };
    let contact = state.entities.iter().find_map(|(other_id, other)| {
        if other_id == eid
            || component_authority_mode(other, comp) != AuthorityMode::ClientForwardSparse
        {
            return None;
        }
        let other_radius = authority_bound_f64(
            other,
            comp,
            &[
                "contact_radius",
                "contactRadius",
                "contact_margin",
                "contactMargin",
            ],
        )
        .unwrap_or(0.0);
        let threshold = radius + other_radius;
        if threshold <= 0.0 {
            return None;
        }
        let dx = next[0] - other.pos[0];
        let dy = next[1] - other.pos[1];
        let distance = (dx * dx + dy * dy).sqrt();
        if distance <= threshold {
            Some((other_id.clone(), distance, threshold))
        } else {
            None
        }
    });
    let Some((other_id, distance, threshold)) = contact else {
        return false;
    };
    let owner = contact_arbitration_owner(state, eid, &other_id, comp);
    let reason = format!(
        "contact-risk: candidate distance {:.3} <= contact threshold {:.3}; escalated to server_arbitrated",
        distance, threshold
    );
    escalate_component_to_server_arbitrated(state, eid, comp, owner.clone(), &reason);
    escalate_component_to_server_arbitrated(state, &other_id, comp, owner, &reason);
    emit(
        state,
        wid,
        json!({"op":"UpdateRejected","entity":eid,"comp":comp,
        "reason":reason,"mode":"server_arbitrated","contact_entity":other_id}),
    );
    true
}

fn reject_stale_authority_epoch(
    state: &mut ServerState,
    wid: &str,
    eid: &str,
    comp: &str,
    f: &Value,
) -> bool {
    reject_stale_authority_epoch_val(state, wid, eid, comp, frame_authority_epoch(f))
}

// Same staleness check, but the supplied epoch is passed directly (so a BatchUpdate entry can carry its
// OWN authority_epoch, or None to fall back to the per-(eid,comp) cached epoch). The frame-based wrapper
// above just extracts f["authority_epoch"] and delegates here -> ONE epoch-fencing path, no divergence.
fn reject_stale_authority_epoch_val(
    state: &mut ServerState,
    wid: &str,
    eid: &str,
    comp: &str,
    supplied: Option<u64>,
) -> bool {
    let current = match state.entities.get(eid) {
        Some(e) => component_authority_epoch(e, comp),
        None => return false,
    };
    let cached = state
        .workers
        .get(wid)
        .and_then(|w| w.authority_epochs.get(&authority_key(eid, comp)).copied());
    let candidate = supplied.or(cached);
    if let Some(epoch) = candidate {
        if epoch != current {
            emit(
                state,
                wid,
                json!({"op":"UpdateRejected","entity":eid,"comp":comp,
                "reason":format!("stale authority epoch {}; current={}", epoch, current),
                "authority_epoch":current}),
            );
            return true;
        }
    }
    false
}

// visibility = geometric interest AND read-ACL
fn visible(w: &WorkerHandle, e: &Entity) -> bool {
    w.interested_in(e.pos) && acl_read_ok(&w.attributes, e)
}

#[derive(Default)]
struct Metrics {
    handoffs: u64, // LOCAL same-broker region->region transfers (the in-process handoff() path)
    mesh_handoffs: u64, // CROSS-BROKER handoffs: an entity FORWARDED across the process seam to a neighbour broker (mesh_forward, counted on the SENDING broker at the delivered-departure linearization point). Local handoffs counted ONLY same-broker -> cross-server handoffs were invisible in the Inspector before this.
    applies: u64,
    failovers: u64,
    wal_compactions: u64, // R0.3: how many times the WAL was compacted (bounded-disk metric)
    fenced_stale_handoffs: u64, // L6: inbound MeshHandoffs REJECTED because the sender carried a stale lease_epoch (a fenced returned-from-partition incarnation)
}

// C2: pre-handoff intent (the overlay's I:<target>). One per entity (our handoff is per-region);
// set when the entity enters the hysteresis overlap band, cleared at commit / on leaving the band.
#[derive(Clone)]
struct HandoffIntent {
    source_region: String,
    target_region: String,
    source_worker: String,
    target_worker: String,
    epoch: u64,
}

// Hardening #1 Step 1: a budgeted INCREMENTAL split. maybe_split enqueues this job (the movers to hand off
// to the new sub-region) instead of handing off ALL of them in one locked pass; process_rebalance_jobs
// applies a small batch per monitor tick under a time budget, so the split spreads over many ticks and
// NEVER freezes the broker (Step 0 measured 2.3s @ 20k in one pass). should_still_move re-checks each mover.
struct RebalanceJob {
    eids: Vec<String>, // candidate eids to RE-ROUTE (snapshotted at enqueue); the drain recomputes each
    // entity's correct region NOW (movement_region_after) + hands it off if it changed,
    // so the job is idempotent with the move-path and a shifted boundary. BOTH maybe_split
    // and rebalance feed this one queue, so neither does an unbudgeted O(N) handoff loop.
    cursor: usize,
}

// L1 event-storm: a buffered EntityEvent awaiting the coalescing flush. Events buffer per-broker, then
// flush_events() coalesces them by class (critical=all delivered, visual=one-per-coalesce_key with a count,
// debug=dropped over budget) so a 1000-event burst can't flood the egress or the client frame.
struct BufferedEvent {
    target_wids: Vec<String>, // the interested workers (computed at buffer time, before the flush)
    eid: String,
    event: Value,
    payload: Value,
    class: String, // "critical" (never coalesced/dropped) | "visual" (coalesced by key) | "debug" (dropped over budget)
    coalesce_key: String, // visual events sharing this key collapse to one (with a count)
    sim_time: Value,
    gen: Value,
}

#[derive(Clone, Debug)]
struct PendingCommand {
    caller: String,
    entity: String,
    owner: String,
    authority_comp: String,
    authority_epoch: Option<u64>,
}

type CoalescedEventsByWorker = HashMap<String, (Vec<Value>, HashMap<String, (Value, u64)>)>;

struct ServerState {
    entities: HashMap<String, Entity>,
    workers: HashMap<String, WorkerHandle>,
    region_worker: HashMap<String, String>,
    // ── durability ──
    wal: Option<File>,
    wal_bytes: u64, // G2 snapshot: cumulative WAL bytes written = the point-in-time restore offset (consistent cut)
    wal_path: String, // R0.3 compaction: the GW_WAL path (so the tick can rewrite/rename it). Empty = no WAL.
    wal_compact_bytes: u64, // R0.3: compact when wal_bytes exceeds this (GW_WAL_COMPACT_BYTES, default 64 MB; 0 = disabled)
    snapshot_seen: bool, // R0.3: a coordinated G2 SnapshotMarker was taken against THIS broker -> disable compaction (an external GW_RESTORE_OFFSET cut must stay valid; rewriting the file would invalidate it)
    wal_degraded: bool, // R0.1: a WAL write/sync failed -> fail-closed (reject persistent ops, do not publish)
    wal_fail_inject: bool, // R0.1: GW_WAL_FAIL test hook (force a WAL failure for the recovery-correctness gate)
    durable_gen: u64, // DurableTransition watermark: observers only see transitions applied at/below this generation
    pending_gen: u64, // highest WAL-appended-but-not-yet-fsynced transition generation
    pending_updates: Vec<PreparedUpdate>, // staged component writes waiting for the group fsync barrier
    pending_handoffs: Vec<PreparedHandoff>, // staged same-broker authority transfers waiting for the same durability law
    pending_remote_handoffs: Vec<PendingRemoteHandoff>, // non-durable cross-broker seam intents; flushed after the current durable batch finishes applying
    pending_failovers: Vec<PreparedFailover>, // staged grant-only lease failovers waiting for the durable authority watermark
    pending_block_migrations: Vec<PreparedBlockMigration>, // staged 2D rebalance block moves; one block == one atomic durable group
    zone_topology_rev: u64, // R0.2: bumped on every boundary/split change; the latest partition_config restores on recovery
    mesh_ack_drop: bool, // G2.1c test hook (GW_MESH_ACK_DROP): adopt an inbound MeshHandoff but DROP the MeshAck -> a stable in-flight state for the consistent-cut test
    mesh_adopt_drop: bool, // G2.1d test hook (GW_MESH_ADOPT_DROP): DROP an inbound MeshHandoff (no adopt, no ack) -> the wire-transit state for the resolver test
    interest_grid: HashMap<(i64, i64), HashSet<String>>, // Interest: spatial-hash cell -> AOI-worker ids covering it (broadphase to prune the O(E×V) propagate fan-out)
    global_workers: HashSet<String>, // Interest: workers NOT grid-indexed (no AOI, or an AOI too large to index) -> propagate always checks these
    // ── liveness / failover ──
    region_expires: HashMap<String, Instant>,
    lease_ttl: f64,
    standbys: Vec<String>,
    orphaned_regions: Vec<String>,
    metrics: Metrics,
    rejected: Vec<Value>,
    replay_tape: Option<ReplayTape>, // Model Plane v0: optional bounded/redacted JSONL observer, never runtime authority.
    // ── documented SpatialOS contract ops ──
    pending_commands: HashMap<String, PendingCommand>, // request_id -> typed routed-command contract
    entity_id_reservations: u64,                       // monotonic ReserveEntityIds counter
    flags: Map<String, Value>,                         // runtime config flags (FlagUpdate)
    worker_load: HashMap<String, f64>,                 // wid -> last reported load (Metrics)
    boundary: f64, // DYNAMIC partition split (load balancing) -- the 1-boundary W|E line; == boundaries[0] when present
    boundaries: Vec<f64>, // N-ZONE 1D-strip cut points (sorted). 0/1 elems => W|E names; >=2 => Z<i> strips. Source of truth for region-assignment (the W|E `boundary` above is the 1-element special case rebalance() still shifts).
    grid2d: Option<(usize, usize, f64, f64)>, // D1: 2D-grid partition (cols, rows, cell_w, cell_h) from GW_GRID2D; Some => square "Z<col>_<row>" zones derived from (x,y); None => the 1D-strip path above (unchanged)
    splits: HashMap<String, Vec<f64>>, // region -> sorted sub-boundaries (dynamic load-split capacity-add)
    rebalance_jobs: Vec<RebalanceJob>, // Hardening #1: pending budgeted-incremental split jobs (front = active)
    event_outbox: Vec<BufferedEvent>, // L1: EntityEvents buffered for the coalescing flush (storm-bounded delivery)
    load_level: u8, // L3 graceful-degradation: 0 normal / 1 stressed / 2 overloaded (derived from the egress backlog); degradation keys off this; recovers when load clears
    ingress_rate_per_sec: f64, // Security v0: frame token refill per worker/peer
    ingress_burst_frames: f64, // Security v0: max accumulated inbound frame tokens per worker/peer
    connect_auth_token: Option<String>, // Security v0: optional shared secret required on WorkerConnect before registration
    connect_auth_claims: HashMap<String, PeerClaims>, // Security v0.1: token -> broker-owned region/attrs; peer JSON cannot self-assign them
    mesh: HashMap<String, UnboundedSender<Vec<u8>>>, // region -> link to the neighbour BROKER owning it (N-neighbour cross-broker mesh)
    // B1 cross-broker seam: entities handed EAST but not yet ACK'd by the neighbour. The forward removes
    // the entity from `entities` but parks it here (the MeshHandoff frame + when-sent) until a MeshAck
    // confirms it landed; a periodic re-send covers a dropped handoff (the receive is idempotent). The
    // invariant: an entity crossing the seam is in EXACTLY ONE of {here.entities, pending_mesh, neighbour}
    // -- it never vanishes (the old path removed it before any confirmation).
    pending_mesh: HashMap<String, (Value, Instant, String)>, // eid -> (MeshHandoff frame, when-sent, TARGET region) -- target picks the re-send link
    mesh_forwarded_epoch: HashMap<String, u64>, // eid -> latest durable mesh_out epoch; fences old-source re-sends after this broker forwarded the entity onward
    // B1 fix: this broker is CONFIGURED to mesh east (GW_MESH_EAST set), independent of whether the link
    // is momentarily up (mesh_east Some). Routing must NOT depend on the link being up at the instant an
    // entity crosses -- else a cross while the neighbour is down fell through to a LOCAL handoff and
    // ORPHANED the entity (region E, no owner, frozen) instead of keeping it local + retrying each move.
    mesh_regions: HashSet<String>, // regions this broker meshes OUT to (remote zones), independent of a link being momentarily up
    mesh_link_spawned: HashSet<String>, // regions with a (forever-retrying) dynamic mesh-link TASK spawned THIS process lifetime. NOT persisted/restored: a WAL recovery repopulates mesh_regions (routing) but the OLD link task died with the crash, so spawning must gate on THIS set -- else a recovered broker thinks it is linked (mesh_regions.contains) yet has no live link, and recovered in-flight handoffs never resend (the churn pending-leak).
    // L6 LEASE-FENCED REGISTRY (broker-incarnation fencing across the mesh): the per-region MONOTONIC lease
    // epoch this broker KNOWS (its OWN region's claimed epoch + every peer region's epoch learned from the
    // registry). A region taken over by a new broker incarnation gets a STRICTLY HIGHER epoch (registry
    // current +1, or an explicit GW_ADVERTISE "@N"). Ownership-bearing mesh traffic carries the SENDER's
    // (src_region, lease_epoch); a receiver REJECTS a frame whose epoch is STALE (< the epoch it knows for
    // that region) -> a returned-from-partition OLD incarnation is fenced out (its handoffs refused, never
    // adopted = no split-brain). ADDITIVE: a missing epoch (legacy single-incarnation frames/tests) reads as
    // None -> accepted, so the existing nzone/mesh_seam/discovery/soak gates are unaffected.
    region_lease_epoch: HashMap<String, u64>,
    my_region: String, // this broker's OWN advertised region (from GW_ADVERTISE "E=host:port[@epoch]"); empty if none. Source-stamp for outbound MeshHandoffs.
    superseded_regions: HashSet<String>, // L6 self-fence: my_region for which the registry now shows a STRICTLY HIGHER epoch held by a DIFFERENT addr -> I am the stale incarnation; suppress my outbound ownership traffic for it.
    deleted_entities: HashSet<String>, // durable tombstones: delete dominates late threshold/mesh/recreate attempts
    // C2: pre-handoff intent per entity (the overlay's I:<target>); cleared at commit / on leaving the band
    pending_handoff_intent: HashMap<String, HandoffIntent>,
    threshold_ttl: Duration,
    // Hardening #1 Step 0: lock-hold instrumentation -- the max time the monitor tick held the global lock,
    // by path, exposed via InspectorFrame. Measure the zone-split freeze before applying budgeted rebalance.
    lock_max_hold_ms: f64,
    lock_max_hold_path: String,
    lock_last_hold_ms: f64,
    // ── CROSS-BROKER SEAM-INTEREST ──
    ghosts: HashMap<String, GhostEntity>, // read-only mirrors of NEIGHBOUR-broker near-seam entities (NEVER in `entities` -> structurally non-authoritative). Keyed by the SAME eid the source broker uses.
    interest_band: f64, // GW_INTEREST_BAND: the border-band half-width around each seam whose entities a broker pushes to its neighbour(s) as ghosts. 0 (default) => no push => the current behaviour, byte-for-byte.
    ghost_seq: u64, // monotonic counter so a ghost-push batch can be cheap to throttle (push every Nth tick is unnecessary; the monitor tick is 300ms which is already a fine ghost-refresh rate)
    // ── #3 OPS: GRACEFUL-DRAIN (rolling-deploy without kicking players) ─────────────────────────────────────
    // A broker told to DRAIN (the `Drain` op, or GW_DRAIN_ON_START for a test) (1) STOPS accepting new entity
    // CreateEntity (rejected with reason "draining" so the caller re-creates on a live broker), and (2) hands EVERY
    // entity it owns across the mesh to the neighbour that owns where the entity IS -- via the SAME proven 2-phase
    // mesh_forward path (pending_mesh -> MeshAck -> exactly-once, conservation-EXACT). When entities AND pending_mesh
    // both empty, the drain is COMPLETE and (if `drain_exit`) the process exits 0 -- a clean rolling-deploy shutdown.
    // Note: `draining` is STATE set by a COMMAND, not a config flag on a shelf; the reject-new + hand-off-all + exit-when-
    // empty IS the structural behaviour of that state. Players are uninterrupted: the neighbour adopts each entity
    // (AddEntity + the component stream) and a client re-checks-out from the neighbour (checkout_all), so no view is lost.
    draining: bool,
    drain_exit: bool, // when a completed drain should std::process::exit(0) (a real rolling deploy); a test can drain-without-exit to assert conservation against the still-running neighbour.
    monitor_last_tick_ms: u64, // last completed monitor tick wall-clock; health uses age so one old tick cannot fake liveness.
    tick_lag_ms: f64, // #3 metrics: how late the 300ms monitor tick actually fired vs schedule (the broker's saturation signal -- a healthy broker is ~0, a CPU-starved one lags). Measured in the monitor loop.
}

impl ServerState {
    fn new(lease_ttl: f64) -> Self {
        ServerState {
            entities: HashMap::new(),
            workers: HashMap::new(),
            region_worker: HashMap::new(),
            wal: None,
            wal_bytes: 0,
            wal_path: String::new(),
            // R0.3: compact past 64 MB by default; GW_WAL_COMPACT_BYTES overrides; 0 disables compaction.
            wal_compact_bytes: std::env::var("GW_WAL_COMPACT_BYTES")
                .ok()
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(64 * 1024 * 1024),
            snapshot_seen: false,
            wal_degraded: std::env::var("GW_WAL_FAIL").is_ok(), // R0.1: a configured-broken WAL starts fail-closed
            wal_fail_inject: std::env::var("GW_WAL_FAIL").is_ok(),
            durable_gen: 0,
            pending_gen: 0,
            pending_updates: Vec::new(),
            pending_handoffs: Vec::new(),
            pending_remote_handoffs: Vec::new(),
            pending_failovers: Vec::new(),
            pending_block_migrations: Vec::new(),
            zone_topology_rev: 0,
            mesh_ack_drop: std::env::var("GW_MESH_ACK_DROP").is_ok(),
            mesh_adopt_drop: std::env::var("GW_MESH_ADOPT_DROP").is_ok(),
            interest_grid: HashMap::new(),
            global_workers: HashSet::new(),
            region_expires: HashMap::new(),
            lease_ttl,
            standbys: Vec::new(),
            orphaned_regions: Vec::new(),
            metrics: Metrics::default(),
            rejected: Vec::new(),
            replay_tape: ReplayTape::from_env(),
            pending_commands: HashMap::new(),
            entity_id_reservations: 0,
            flags: Map::new(),
            worker_load: HashMap::new(),
            // CONFIGURABLE W|E partition line. BOUNDARY (0.0) was the only value -> on a map with all-positive-x
            // coordinates (the godot-open-rts map, any real game map) EVERY entity is region "E" from the start
            // (initial_region: x<boundary -> W) and region_after never returns "W" (needs x < boundary-H = -0.5),
            // so a position-driven cross-zone handoff NEVER fired (mesh_handoffs stayed 0). GW_BOUNDARY=<f64> sets
            // the split line so e.g. GW_BOUNDARY=80 makes an entity crossing x=80 auto-handoff W<->E. Parsed here
            // (mirrors wal_compact_bytes just below); default = BOUNDARY (0.0) so the old behavior is byte-for-byte
            // preserved when unset. Note: this is CONFIG, the handoff stays automatic + structural (no per-call flag);
            // state.boundary remains DYNAMIC afterward (rebalance() load-shifts it; WAL recovery restores it).
            // N-ZONE: GW_BOUNDARIES="b0,b1,.." (sorted) cuts the map into N strips (Z0..Zn). Back-compat:
            // absent => fall back to the single GW_BOUNDARY (or BOUNDARY=0.0) -> a 1-element list == today's
            // W|E split, byte-for-byte. `boundary` carries the first cut (the dynamic W|E line rebalance shifts).
            boundary: parse_boundaries().first().copied().unwrap_or(BOUNDARY),
            boundaries: parse_boundaries(),
            grid2d: parse_grid2d(),
            splits: HashMap::new(),
            rebalance_jobs: Vec::new(),
            event_outbox: Vec::new(),
            load_level: 0,
            ingress_rate_per_sec: parse_nonnegative_f64_env(
                "GW_INGRESS_RATE_PER_SEC",
                DEFAULT_INGRESS_RATE_PER_SEC,
            ),
            ingress_burst_frames: parse_nonnegative_f64_env(
                "GW_INGRESS_BURST_FRAMES",
                DEFAULT_INGRESS_BURST_FRAMES,
            )
            .max(1.0),
            connect_auth_token: parse_nonempty_env("GW_AUTH_TOKEN"),
            connect_auth_claims: parse_connect_auth_claims_env(),
            mesh: HashMap::new(),
            pending_mesh: HashMap::new(),
            mesh_forwarded_epoch: HashMap::new(),
            mesh_regions: HashSet::new(),
            mesh_link_spawned: HashSet::new(),
            region_lease_epoch: HashMap::new(),
            my_region: String::new(),
            superseded_regions: HashSet::new(),
            deleted_entities: HashSet::new(),
            pending_handoff_intent: HashMap::new(),
            threshold_ttl: Duration::from_secs(30),
            lock_max_hold_ms: 0.0,
            lock_max_hold_path: String::new(),
            lock_last_hold_ms: 0.0,
            ghosts: HashMap::new(),
            // GW_INTEREST_BAND=<f64>: the seam border-band half-width. Default 0.0 => seam-interest OFF (no
            // ghost push) => every existing gate is unaffected. A real game sets e.g. 12 (>= weapon range) so a
            // unit within 12 of a seam sees + can target the neighbour zone's units within 12 of it.
            interest_band: std::env::var("GW_INTEREST_BAND")
                .ok()
                .and_then(|s| s.parse::<f64>().ok())
                .filter(|f| f.is_finite() && *f >= 0.0)
                .unwrap_or(0.0),
            ghost_seq: 0,
            // #3 OPS: GW_DRAIN_ON_START=1 brings the broker up already draining (a test hook to start a node in the
            // drain state without racing the Drain op); default off. drain_exit defaults true (a real rolling deploy
            // exits when drained); GW_DRAIN_NO_EXIT=1 keeps it alive after draining so a test can read the post-drain
            // metrics + assert the neighbour absorbed every entity. Both ADDITIVE: unset => no drain => unchanged.
            draining: std::env::var("GW_DRAIN_ON_START").is_ok(),
            drain_exit: std::env::var("GW_DRAIN_NO_EXIT").is_err(),
            monitor_last_tick_ms: 0,
            tick_lag_ms: 0.0,
        }
    }

    // ── WAL: append-only durable journal (== persistence.py WAL.append + fsync) ──
    // R0.1: durable-before-publish. Returns Err if the WAL write/sync fails (a swallowed error is NOT
    // commercial-grade); the broker then goes fail-closed (wal_degraded) so persistent ops are rejected and
    // nothing is published as success. GW_WAL_FAIL injects a failure for the recovery-correctness acceptance.
    fn wal_append(&mut self, ev: &Value) -> Result<(), ()> {
        // single-record durable append = write the line THEN fsync (behaviour unchanged: durable-before-return).
        self.wal_append_nosync(ev)?;
        self.wal_sync()
    }

    // GROUP-COMMIT primitive: write the record's line WITHOUT fsync. A caller applying MANY records in one
    // tick (BatchUpdate) writes all their lines via this, then calls wal_sync() ONCE -- collapsing N fsyncs
    // into 1 (the per-tick durability wall at scale) while preserving the SAME on-disk format + the SAME
    // durable-before-publish invariant (every line is on disk + fsync'd before ANY of the group is published).
    fn wal_append_nosync(&mut self, ev: &Value) -> Result<(), ()> {
        if self.wal_fail_inject {
            self.wal_degraded = true;
            return Err(());
        }
        if let Some(f) = self.wal.as_mut() {
            // #2: write each record as a v1 integrity envelope ({"_c":crc32,"_d":payload}) so a half-written
            // last line is DETECTED on recovery (CRC), not silently skipped. wal_bytes counts the on-disk
            // envelope line so GW_RESTORE_OFFSET / recovery byte-accounting stay consistent.
            let mut line = wal_v1_envelope_line(ev);
            line.push('\n');
            if f.write_all(line.as_bytes()).is_err() {
                self.wal_degraded = true;
                return Err(());
            }
            self.wal_bytes += line.len() as u64; // G2: the offset for point-in-time snapshots / consistent cuts
        }
        Ok(())
    }

    // flush the OS buffer + fsync to disk (one durability barrier). Pairs with wal_append_nosync for group
    // commit; called once after a batch's lines are all written. A failure -> fail-closed (wal_degraded).
    fn wal_sync(&mut self) -> Result<(), ()> {
        if self.wal_fail_inject {
            self.wal_degraded = true;
            return Err(());
        }
        if let Some(f) = self.wal.as_mut() {
            if f.flush().is_err() || f.sync_data().is_err() {
                self.wal_degraded = true;
                return Err(());
            }
        }
        Ok(())
    }

    // #2: stamp the v1 version header as the FIRST line of a FRESH WAL (file currently empty). On an existing
    // WAL (wal_bytes>0) this is a no-op, so re-opening a populated log never double-writes the header. Counts
    // the header into wal_bytes so the offset accounting includes it. Called right after the WAL is opened.
    fn ensure_wal_header(&mut self) {
        if self.wal_bytes != 0 || self.wal_fail_inject {
            return; // existing WAL (header already there) or fail-closed -> nothing to stamp
        }
        if let Some(f) = self.wal.as_mut() {
            let mut line = wal_v1_header_line();
            line.push('\n');
            if f.write_all(line.as_bytes()).is_err() || f.flush().is_err() || f.sync_data().is_err()
            {
                self.wal_degraded = true;
                return;
            }
            self.wal_bytes += line.len() as u64;
        }
    }

    // ── R0.3 WAL COMPACTION: bound the append-only journal on disk ──
    // Without this the WAL grows forever (every write/transfer/epoch is an appended fsync'd line), so a
    // broker running for days fills the disk -- the disk analog of the unbounded `state.rejected` RAM leak.
    // Compaction rewrites the log as the CURRENT state expressed in the EXISTING `register` events (one per
    // LIVE entity) + the latest `partition_config`, which `recover_from_wal` already replays verbatim, so the
    // recovery FORMAT does not change. Live-entity snapshot inherently drops tombstones (correct: a deleted
    // entity must not resurrect) and departed/mesh-handed entities (correct: they live on the neighbour now).
    // Called ONLY from the single-threaded broker tick under the Arc<Mutex>, so it never races a concurrent
    // write -> no torn-write window (the rename is atomic; the reopened handle then appends fresh writes).
    fn maybe_compact_wal(&mut self) {
        // gates: WAL configured, not fail-closed, threshold enabled (>0) and exceeded.
        if self.wal_path.is_empty()
            || self.wal.is_none()
            || self.wal_degraded
            || self.wal_compact_bytes == 0
            || self.wal_bytes <= self.wal_compact_bytes
        {
            return;
        }
        // G2 SAFETY: if a coordinated point-in-time snapshot was taken against this broker, an external
        // coordinator may restart us with GW_RESTORE_OFFSET=<bytes>. Rewriting the file would invalidate that
        // byte offset (the consistent cut), so compaction stays OFF for the lifetime of a snapshot-using
        // broker. The disk-fill case (no coordinated snapshots) compacts normally. Structural guard, not a flag.
        if self.snapshot_seen {
            return;
        }
        // Compaction is a durable-cut operation: it rewrites RAM state into a new WAL. Staged transitions
        // still live only in the current WAL + pending_* queues until their group fsync/apply pass, so
        // compacting here would snapshot pre-transition RAM and delete the only file carrying the staged line.
        if !self.pending_updates.is_empty()
            || !self.pending_handoffs.is_empty()
            || !self.pending_failovers.is_empty()
            || !self.pending_block_migrations.is_empty()
        {
            return;
        }
        let tmp = format!("{}.tmp", self.wal_path);
        // Build the snapshot as register lines (== spawn_in_region's WAL event, == recover_from_wal's reader)
        // + the latest partition_config (== wal_partition_config), into <wal>.tmp.
        let mut out = match OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)
        {
            Ok(f) => f,
            Err(_) => {
                self.wal_degraded = true; // can't write the snapshot -> fail-closed (do not pretend success)
                return;
            }
        };
        let mut bytes: u64 = 0;
        let mut buf = String::new();
        // #2: the compacted WAL is a fresh v1 log -> stamp the version header first so recovery reads it as v1.
        buf.clear();
        buf.push_str(&wal_v1_header_line());
        buf.push('\n');
        if out.write_all(buf.as_bytes()).is_err() {
            self.wal_degraded = true;
            return;
        }
        bytes += buf.len() as u64;
        for (eid, e) in self.entities.iter() {
            // authority_epoch scalar = the spatial (pos) epoch seed; the full `authority` map below carries
            // every component's exact owner/epoch/mode (apply_authority_snapshot overlays it on recovery).
            let pos_epoch = e.authority.get("pos").map(|ca| ca.epoch).unwrap_or(1);
            let ev = json!({
                "kind": "register",
                "entity": eid,
                "version": e.version,
                "authority_epoch": pos_epoch,
                "pos": [e.pos[0], e.pos[1]],
                "vel": [e.vel[0], e.vel[1]],
                "components": Value::Object(e.components.clone()),
                "region": e.region,
                "authority": authority_to_json(&e.authority),
            });
            buf.clear();
            buf.push_str(&wal_v1_envelope_line(&ev)); // #2: integrity envelope, same format as wal_append
            buf.push('\n');
            if out.write_all(buf.as_bytes()).is_err() {
                self.wal_degraded = true;
                return;
            }
            bytes += buf.len() as u64;
        }
        // Preserve permanent delete tombstones across compaction. The broker rejects a CreateEntity for any
        // tombstoned id; dropping these records during compaction would let a restart recreate a deleted id.
        let mut tombstones: Vec<String> = self.deleted_entities.iter().cloned().collect();
        tombstones.sort();
        for eid in tombstones {
            let ev = json!({
                "kind": "delete_tombstone",
                "entity": eid,
                "version": 0,
                "writer": "wal_compaction",
            });
            buf.clear();
            buf.push_str(&wal_v1_envelope_line(&ev));
            buf.push('\n');
            if out.write_all(buf.as_bytes()).is_err() {
                self.wal_degraded = true;
                return;
            }
            bytes += buf.len() as u64;
        }
        // Preserve completed onward mesh departures across compaction. Without this durable fence, an old
        // upstream MeshHandoff resend after a restart could re-adopt an eid this broker already forwarded.
        let mut forwarded: Vec<(String, u64)> = self
            .mesh_forwarded_epoch
            .iter()
            .map(|(eid, epoch)| (eid.clone(), *epoch))
            .collect();
        forwarded.sort_by(|a, b| a.0.cmp(&b.0));
        for (eid, authority_epoch) in forwarded {
            if self.entities.contains_key(&eid) || self.deleted_entities.contains(&eid) {
                continue;
            }
            let ev = json!({
                "kind": "mesh_forwarded_fence",
                "entity": eid,
                "authority_epoch": authority_epoch,
                "writer": "wal_compaction",
            });
            buf.clear();
            buf.push_str(&wal_v1_envelope_line(&ev));
            buf.push('\n');
            if out.write_all(buf.as_bytes()).is_err() {
                self.wal_degraded = true;
                return;
            }
            bytes += buf.len() as u64;
        }
        // latest partition_config so the router restores the SAME placement function (do NOT bump the rev --
        // this is a faithful re-encoding of the current topology, not a new topology change).
        let splits: Map<String, Value> = self
            .splits
            .iter()
            .map(|(r, v)| (r.clone(), json!(v)))
            .collect();
        let mesh: Vec<String> = self.mesh_regions.iter().cloned().collect();
        let pc = json!({
            "kind": "partition_config",
            "version": self.zone_topology_rev,
            "boundary": self.boundary,
            "boundaries": self.boundaries.clone(),
            "splits": Value::Object(splits),
            "mesh_regions": mesh,
        });
        buf.clear();
        buf.push_str(&wal_v1_envelope_line(&pc)); // #2: integrity envelope
        buf.push('\n');
        if out.write_all(buf.as_bytes()).is_err() {
            self.wal_degraded = true;
            return;
        }
        bytes += buf.len() as u64;
        // durably land the snapshot, then atomically swap it in.
        if out.flush().is_err() || out.sync_all().is_err() {
            self.wal_degraded = true;
            return;
        }
        drop(out); // close the tmp handle before the rename (Windows: can't rename an open-for-write file onto target)
                   // Drop the OLD wal handle so the rename can replace the file on Windows.
        self.wal = None;
        if std::fs::rename(&tmp, &self.wal_path).is_err() {
            // rename failed -> the original WAL is still intact on disk; reopen it (append) and keep serving.
            if let Ok(f) = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.wal_path)
            {
                self.wal = Some(f);
            } else {
                self.wal_degraded = true;
            }
            return;
        }
        // reopen the freshly-compacted WAL in append mode and reset the offset to the snapshot size.
        match OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.wal_path)
        {
            Ok(f) => {
                self.wal = Some(f);
                let before = self.wal_bytes;
                self.wal_bytes = bytes;
                self.metrics.wal_compactions += 1;
                println!(
                    "[rust-broker] R0.3 WAL compacted: {} entities, {} -> {} bytes ({} compaction(s))",
                    self.entities.len(),
                    before,
                    bytes,
                    self.metrics.wal_compactions
                );
            }
            Err(_) => {
                self.wal_degraded = true; // compacted file exists but we can't reopen it -> fail-closed
            }
        }
    }
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// #3 OPS metric: this process's resident-set size in bytes, for the health endpoint (a runtime prober watches RSS
// to catch a leaking broker before OOM). Platform-native with NO new crate dependency (the Cargo.toml stays
// tokio+serde_json only): Windows reads GetProcessMemoryInfo.WorkingSetSize via a tiny psapi FFI; Linux reads
// /proc/self/statm (pages * page_size). Where unavailable we return 0 HONESTLY rather than fabricate a number --
// a 0 RSS in the frame means "not measured on this platform", not "0 bytes used".
#[cfg(windows)]
fn process_rss_bytes() -> u64 {
    // PROCESS_MEMORY_COUNTERS layout (psapi.h); we only read WorkingSetSize. cb must be the struct size.
    #[repr(C)]
    struct ProcessMemoryCounters {
        cb: u32,
        page_fault_count: u32,
        peak_working_set_size: usize,
        working_set_size: usize,
        quota_peak_paged_pool_usage: usize,
        quota_paged_pool_usage: usize,
        quota_peak_non_paged_pool_usage: usize,
        quota_non_paged_pool_usage: usize,
        pagefile_usage: usize,
        peak_pagefile_usage: usize,
    }
    extern "system" {
        fn GetCurrentProcess() -> isize;
        fn K32GetProcessMemoryInfo(
            process: isize,
            ppsmemcounters: *mut ProcessMemoryCounters,
            cb: u32,
        ) -> i32;
    }
    unsafe {
        let mut pmc: ProcessMemoryCounters = std::mem::zeroed();
        pmc.cb = std::mem::size_of::<ProcessMemoryCounters>() as u32;
        if K32GetProcessMemoryInfo(GetCurrentProcess(), &mut pmc, pmc.cb) != 0 {
            pmc.working_set_size as u64
        } else {
            0
        }
    }
}

#[cfg(target_os = "linux")]
fn process_rss_bytes() -> u64 {
    // /proc/self/statm field 2 = resident pages; * the page size = RSS bytes.
    if let Ok(s) = std::fs::read_to_string("/proc/self/statm") {
        if let Some(rss_pages) = s
            .split_whitespace()
            .nth(1)
            .and_then(|p| p.parse::<u64>().ok())
        {
            let page = 4096u64; // the near-universal default; a small skew here is irrelevant for a leak-watch metric
            return rss_pages * page;
        }
    }
    0
}

#[cfg(not(any(windows, target_os = "linux")))]
fn process_rss_bytes() -> u64 {
    0
}

// #3 OPS: the live broker health/metrics snapshot, as a plain JSON object over the EXISTING length-prefixed wire
// (no HTTP stack pulled in, no second port to secure). Carries the runtime fields needed for liveness checks:
// entity_count, mesh_handoffs, tick-lag, RSS, WAL size, ghost count -- plus the liveness-relevant rest. Built from
// the SAME state.metrics the Inspector reads, so the two never diverge. Returned by the `Health` op (ungated: a
// liveness prober holds no inspector attribute) and folded into the InspectorFrame too.
fn health_snapshot(state: &ServerState) -> Value {
    let now = now_millis();
    let monitor_tick_age_ms = if state.monitor_last_tick_ms == 0 {
        u64::MAX
    } else {
        now.saturating_sub(state.monitor_last_tick_ms)
    };
    json!({
        "status": if state.draining { "draining" } else if state.wal_degraded { "degraded" } else { "ok" },
        "draining": state.draining,
        "wal_degraded": state.wal_degraded,
        "entity_count": state.entities.len(),
        "ghost_count": state.ghosts.len(),
        "worker_count": state.workers.len(),
        "mesh_handoffs": state.metrics.mesh_handoffs,
        "handoffs": state.metrics.handoffs,
        "applies": state.metrics.applies,
        "failovers": state.metrics.failovers,
        "fenced_stale_handoffs": state.metrics.fenced_stale_handoffs,
        "wal_compactions": state.metrics.wal_compactions,
        "pending_mesh": state.pending_mesh.len(),
        "monitor_last_tick_ms": state.monitor_last_tick_ms,
        "monitor_tick_age_ms": monitor_tick_age_ms,
        "tick_lag_ms": state.tick_lag_ms,
        "lock_max_hold_ms": state.lock_max_hold_ms,
        "rss_bytes": process_rss_bytes(),
        "wal_bytes": state.wal_bytes,
        "load_level": state.load_level,
        "my_region": state.my_region,
        "mesh_regions": state.mesh_regions.iter().cloned().collect::<Vec<String>>(),
        "boundaries": state.boundaries.clone(),
        "t_server": now,
    })
}

// #3 OPS: GRACEFUL-DRAIN core -- pick the mesh-neighbour region each owned entity should be handed to, then hand
// EVERY owned entity across via the proven 2-phase mesh_forward (pending_mesh -> MeshAck -> exactly-once). Routing:
// if this broker runs a strip topology (W/E/Z<i>) and a position-derived neighbour strip is a live mesh region,
// the entity goes to the strip it would cross into (the natural adjacency); otherwise it goes to ANY live mesh
// neighbour (a named-zone topology like nzone's planets -- still conservation-exact, just not position-adjacent).
// Returns how many it dispatched THIS pass. Idempotent across ticks: an entity already moved into pending_mesh is
// gone from `entities`, so a re-run only dispatches what remains. Conservation is the mesh path's exactly-once.
fn drain_handoff_owned(state: &mut ServerState) -> usize {
    if state.mesh.is_empty() {
        return 0; // nowhere to drain to (a single-broker topology has no neighbour) -- honest dead-end, caller reports it
    }
    let live_targets: Vec<String> = state.mesh.keys().cloned().collect();
    // a stable, deterministic neighbour to fall back to (sorted so the choice is reproducible across runs/ticks)
    let mut sorted_targets = live_targets.clone();
    sorted_targets.sort();
    let fallback = sorted_targets[0].clone();
    let bounds = state.boundaries.clone();
    let eids: Vec<String> = state.entities.keys().cloned().collect();
    let mut dispatched = 0usize;
    for eid in eids {
        let (pos, region) = match state.entities.get(&eid) {
            Some(e) => (e.pos, e.region.clone()),
            None => continue,
        };
        // position-adjacent strip neighbour, if this broker is a strip owner and that neighbour is a live mesh link
        let target = {
            let mut chosen: Option<String> = None;
            if is_strip_region_name(coarse_region(&region), &bounds) {
                let idx = strip_index(pos[0], &bounds);
                let neighbour = strip_name(idx, &bounds);
                if neighbour != region && state.mesh.contains_key(&neighbour) {
                    chosen = Some(neighbour);
                }
            }
            chosen.unwrap_or_else(|| fallback.clone())
        };
        mesh_forward(state, &eid, &target);
        dispatched += 1;
    }
    dispatched
}

fn threshold_tx_value(tx_id: &str, phase: &str, from: Value, to: Value, ts_ms: u64) -> Value {
    json!({
        "tx_id": tx_id,
        "phase": phase,
        "from": from,
        "to": to,
        "ts_ms": ts_ms
    })
}

// ══ #2 WAL HYGIENE: version header + per-record CRC + corrupt-tail truncate / mid-corruption refuse ══
// The WAL is JSONL (one event per line). v1 wraps each event line in a 2-field integrity ENVELOPE:
//   {"_c":<crc32 of the inner payload string>,"_d":"<the event JSON, serde-escaped as a string>"}
// `_d` is the EXACT serialized payload string; serde un-escapes it back byte-for-byte on read, so the CRC
// is computed over a reproducible canonical string (no key-order / re-serialization ambiguity). A fresh WAL
// also gets a header line {"kind":"wal_header","wal_version":1}. v0 WALs (no header, bare event lines) stay
// readable via the lenient legacy path so existing logs + the L5 drill never regress.
// What recovery learned about the WAL's integrity (for the dry-run report + the startup fail-closed gate).
struct RecoverReport {
    wal_version: u64,
    selected_event_count: u64,
    decoded_record_count: u64,
    corrupt_tail_record_count: u64,
    truncated_tail_bytes: u64,
    recoverable_prefix_bytes: u64,
    unknown_kind_count: u64,
    kind_counts: BTreeMap<String, u64>,
    unknown_kinds: BTreeMap<String, u64>,
    // Some(msg) => REFUSE: mid-stream corruption or an unknown version. Startup must NOT serve; print the msg.
    error: Option<String>,
}

type RecoveredStore = (
    HashMap<String, Entity>,
    HashSet<String>,
    Option<Value>,
    HashMap<String, Value>,
    HashMap<String, u64>,
    u64,
    RecoverReport,
);

type ReplayStore = (
    HashMap<String, Entity>,
    HashSet<String>,
    Option<Value>,
    HashMap<String, Value>,
    HashMap<String, u64>,
    u64,
);

impl From<WalReadReport> for RecoverReport {
    fn from(report: WalReadReport) -> Self {
        Self {
            wal_version: report.wal_version,
            selected_event_count: report.selected_event_count,
            decoded_record_count: report.decoded_record_count,
            corrupt_tail_record_count: report.corrupt_tail_record_count,
            truncated_tail_bytes: report.truncated_tail_bytes,
            recoverable_prefix_bytes: report.recoverable_prefix_bytes,
            unknown_kind_count: report.unknown_kind_count,
            kind_counts: report.kind_counts,
            unknown_kinds: report.unknown_kinds,
            error: report.error,
        }
    }
}

// A stable content hash of the recovered store (order-independent: per-entity hashes XOR-folded) so the
// dry-run can fingerprint a recovered world without serving it. Covers id/pos/vel/region/version/components.
fn store_content_hash(store: &HashMap<String, Entity>) -> u64 {
    let mut acc: u64 = 0;
    for (eid, e) in store.iter() {
        let canon = json!({
            "id": eid,
            "pos": [e.pos[0], e.pos[1]],
            "vel": [e.vel[0], e.vel[1]],
            "region": e.region,
            "version": e.version,
            "components": Value::Object(e.components.clone()),
        });
        let s = serde_json::to_string(&canon).unwrap();
        // fold a 32-bit CRC into 64-bit, XOR across entities (commutative -> order-independent)
        acc ^= ((crc32_ieee(s.as_bytes()) as u64) << 1) | 1;
    }
    acc
}

// ── recover the EXACT store from the WAL alone (== the reference server recover / apply_event) ──
// #2: now ALSO returns a RecoverReport (version / truncated-tail bytes / refuse-error). A v1 file with a
// corrupt TRAILING run truncates it cleanly; a corrupt record with a VALID record AFTER it (mid-stream) or an
// unknown wal_version REFUSES (report.error set, empty store) so the caller fails closed instead of serving
// partial state. v0 files keep the lenient legacy behavior (skip unparseable lines) — no regression.
fn recover_from_wal_report(path: &str, up_to_offset: Option<u64>) -> RecoveredStore {
    // First pass: shared WAL scanner/decoder. This is the same integrity truth the wal_inspect CLI uses; the
    // broker-specific reducer below intentionally stays here until the Entity/authority model is extracted.
    let read = read_wal_events(path, up_to_offset);
    let report = RecoverReport::from(read.report);
    if report.error.is_some() {
        return (
            HashMap::new(),
            HashSet::new(),
            None,
            HashMap::new(),
            HashMap::new(),
            0,
            report,
        );
    }

    let good_events: Vec<&Value> = read.events.iter().collect();
    let (
        store,
        tombstones,
        last_partition,
        recovered_pending,
        mesh_forwarded_epoch,
        recovered_id_hwm,
    ) = apply_wal_events(&good_events);
    if report.truncated_tail_bytes > 0 {
        println!(
            "[rust-broker] #2 WAL: truncated {} corrupt trailing byte(s) ({} record(s)) — the in-progress write from the crash",
            report.truncated_tail_bytes, report.corrupt_tail_record_count
        );
    }
    (
        store,
        tombstones,
        last_partition,
        recovered_pending,
        mesh_forwarded_epoch,
        recovered_id_hwm,
        report,
    )
}

fn truncate_wal_tail_to_recoverable_prefix(
    path: &str,
    report: &RecoverReport,
) -> Result<(), String> {
    if report.truncated_tail_bytes == 0 {
        return Ok(());
    }
    let current_len = std::fs::metadata(path)
        .map_err(|e| format!("metadata failed for WAL tail truncate: {e}"))?
        .len();
    let prefix = report.recoverable_prefix_bytes;
    if prefix > current_len {
        return Err(format!(
            "recoverable WAL prefix {prefix} exceeds on-disk length {current_len}"
        ));
    }
    if prefix == current_len {
        return Ok(());
    }
    let f = OpenOptions::new()
        .write(true)
        .open(path)
        .map_err(|e| format!("open failed for WAL tail truncate: {e}"))?;
    f.set_len(prefix)
        .map_err(|e| format!("set_len({prefix}) failed for WAL tail truncate: {e}"))?;
    f.sync_data()
        .map_err(|e| format!("sync_data failed after WAL tail truncate: {e}"))?;
    println!(
        "[rust-broker] #2 WAL: physically truncated corrupt tail to recoverable prefix {prefix} byte(s) (was {current_len})"
    );
    Ok(())
}

// Apply already-decoded (CRC-verified) WAL events to a fresh store. Split out of recover_from_wal_report so
// the integrity/version gate runs FIRST, then this pure replay runs on the good (CRC-passing) prefix only.
fn apply_wal_events(events: &[&Value]) -> ReplayStore {
    let mut store: HashMap<String, Entity> = HashMap::new();
    let mut tombstones: HashSet<String> = HashSet::new();
    let mut last_partition: Option<Value> = None; // R0.2: the latest partition_config (boundary/splits/mesh) to restore
    let mut recovered_pending: HashMap<String, Value> = HashMap::new(); // G2.1d: mesh_out not acked by the cut -> resend on restore
    let mut mesh_forwarded_epoch: HashMap<String, u64> = HashMap::new(); // latest durable departure epoch per eid; fences late old-source resends after onward forwarding
    let mut recovered_id_hwm: u64 = 0; // ReserveEntityIds high-water mark: never reissue a block after restart
    let g2d_off = std::env::var("GW_G2D_OFF").is_ok(); // G2.1d test toggle: OFF reverts to pre-resolver (proves the wire-transit LOSS)
    for ev in events {
        let ev = (*ev).clone();
        match ev.get("kind").and_then(|v| v.as_str()).unwrap_or("") {
            "register" => {
                let eid = ev["entity"].as_str().unwrap_or("").to_string();
                if tombstones.contains(&eid) {
                    continue;
                }
                mesh_forwarded_epoch.remove(&eid);
                let pos = arr2(ev.get("pos"));
                let vel = arr2(ev.get("vel"));
                let components = ev
                    .get("components")
                    .and_then(|v| v.as_object())
                    .cloned()
                    .unwrap_or_default();
                let region = ev["region"].as_str().unwrap_or("E").to_string();
                let version = ev["version"].as_u64().unwrap_or(0);
                let authority_epoch = ev["authority_epoch"].as_u64().unwrap_or(1);
                store.insert(
                    eid,
                    Entity {
                        pos,
                        vel,
                        authority: initial_authority_map(&components, authority_epoch),
                        components,
                        region,
                        version,
                        last_broadcast_cell: Some(interest_cell_of(pos)),
                    },
                );
                if let (Some(e), Some(snapshot)) = (
                    store.get_mut(ev["entity"].as_str().unwrap_or("")),
                    ev.get("authority"),
                ) {
                    apply_authority_snapshot(e, snapshot);
                }
            }
            "write" => {
                let eid = ev["entity"].as_str().unwrap_or("");
                if tombstones.contains(eid) {
                    continue;
                }
                if let Some(e) = store.get_mut(eid) {
                    let comp = ev["comp"].as_str().unwrap_or("");
                    let val = ev.get("value").cloned().unwrap_or(Value::Null);
                    if comp == "pos" {
                        e.pos = arr2(Some(&val));
                    } else if comp == "vel" {
                        e.vel = arr2(Some(&val));
                    } else {
                        e.components.insert(comp.to_string(), val);
                    }
                    ensure_component_authority(e, comp);
                    e.version = ev["version"].as_u64().unwrap_or(e.version);
                }
            }
            "component_add" => {
                let eid = ev["entity"].as_str().unwrap_or("");
                if tombstones.contains(eid) {
                    continue;
                }
                if let Some(e) = store.get_mut(eid) {
                    let comp = ev["comp"].as_str().unwrap_or("");
                    let val = ev.get("value").cloned().unwrap_or(Value::Null);
                    e.components.insert(comp.to_string(), val);
                    ensure_component_authority(e, comp);
                    e.version = ev["version"].as_u64().unwrap_or(e.version);
                }
            }
            "component_remove" => {
                let eid = ev["entity"].as_str().unwrap_or("");
                if tombstones.contains(eid) {
                    continue;
                }
                if let Some(e) = store.get_mut(eid) {
                    let comp = ev["comp"].as_str().unwrap_or("");
                    e.components.remove(comp);
                    e.version = ev["version"].as_u64().unwrap_or(e.version);
                }
            }
            "delete_tombstone" => {
                let eid = ev["entity"].as_str().unwrap_or("");
                tombstones.insert(eid.to_string());
                store.remove(eid);
                mesh_forwarded_epoch.remove(eid);
            }
            "mesh_forwarded_fence" => {
                let eid = ev["entity"].as_str().unwrap_or("");
                if tombstones.contains(eid) {
                    continue;
                }
                let forwarded_epoch = ev["authority_epoch"].as_u64().unwrap_or(0);
                store.remove(eid);
                recovered_pending.remove(eid);
                mesh_forwarded_epoch
                    .entry(eid.to_string())
                    .and_modify(|epoch| *epoch = (*epoch).max(forwarded_epoch))
                    .or_insert(forwarded_epoch);
            }
            "transfer" => {
                let eid = ev["entity"].as_str().unwrap_or("");
                if tombstones.contains(eid) {
                    continue;
                }
                if let Some(e) = store.get_mut(eid) {
                    e.region = ev["to"].as_str().unwrap_or(&e.region).to_string();
                    if ev.get("pos").is_some() {
                        e.pos = arr2(ev.get("pos"));
                    }
                    if ev.get("vel").is_some() {
                        e.vel = arr2(ev.get("vel"));
                    }
                    e.version = ev["version"].as_u64().unwrap_or(e.version);
                    if let Some(authority) = ev.get("authority") {
                        apply_authority_snapshot(e, authority);
                    } else {
                        let authority_epoch = ev["authority_epoch"]
                            .as_u64()
                            .unwrap_or_else(|| bump_spatial_authority_epoch(e));
                        set_physics_island_authority_epoch(e, authority_epoch);
                    }
                }
            }
            "authority_epoch" => {
                let eid = ev["entity"].as_str().unwrap_or("");
                if tombstones.contains(eid) {
                    continue;
                }
                if let Some(e) = store.get_mut(eid) {
                    let comp = ev["comp"].as_str().unwrap_or("pos");
                    let authority_epoch = ev["authority_epoch"]
                        .as_u64()
                        .unwrap_or_else(|| component_authority_epoch(e, comp));
                    if let Some(authority) = ev.get("authority") {
                        apply_authority_snapshot(e, authority);
                    } else if comp == "pos" || comp == "physics_island" {
                        set_physics_island_authority_epoch(e, authority_epoch);
                    } else {
                        set_component_authority_epoch(e, comp, authority_epoch);
                    }
                    e.version = ev["version"].as_u64().unwrap_or(e.version);
                }
            }
            "failover_grant" | "block_migration" => {
                if let Some(grants) = ev.get("grants").and_then(|v| v.as_array()) {
                    for grant in grants {
                        let eid = grant["entity"].as_str().unwrap_or("");
                        if tombstones.contains(eid) {
                            continue;
                        }
                        if let Some(e) = store.get_mut(eid) {
                            if let Some(authority) = grant.get("authority") {
                                apply_authority_snapshot(e, authority);
                            } else {
                                let authority_epoch = grant["authority_epoch"]
                                    .as_u64()
                                    .unwrap_or_else(|| component_authority_epoch(e, "pos"));
                                set_physics_island_authority_epoch(e, authority_epoch);
                            }
                            e.version = grant["version"].as_u64().unwrap_or(e.version);
                        }
                    }
                }
            }
            "component_authority" => {
                let eid = ev["entity"].as_str().unwrap_or("");
                if tombstones.contains(eid) {
                    continue;
                }
                if let Some(e) = store.get_mut(eid) {
                    let comp = ev["comp"].as_str().unwrap_or("");
                    ensure_component_authority(e, comp);
                    if let Some(ca) = e.authority.get_mut(comp) {
                        ca.owner = ev
                            .get("owner")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string());
                        ca.epoch = ev["authority_epoch"].as_u64().unwrap_or(ca.epoch);
                        if let Some(mode) =
                            ev["mode"].as_str().and_then(AuthorityMode::from_wire_str)
                        {
                            ca.mode = mode;
                        }
                    }
                    e.version = ev["version"].as_u64().unwrap_or(e.version);
                }
            }
            "threshold_prepare" | "threshold_preload_ready" => {
                let eid = ev["entity"].as_str().unwrap_or("");
                if tombstones.contains(eid) {
                    continue;
                }
                if let Some(e) = store.get_mut(eid) {
                    let kind = ev["kind"].as_str().unwrap_or("");
                    let phase = kind.strip_prefix("threshold_").unwrap_or(kind);
                    e.components.insert(
                        "threshold.tx".to_string(),
                        threshold_tx_value(
                            ev.get("tx_id").and_then(|v| v.as_str()).unwrap_or(""),
                            phase,
                            ev.get("from").cloned().unwrap_or(Value::Null),
                            ev.get("to").cloned().unwrap_or(Value::Null),
                            ev.get("ts_ms").and_then(|v| v.as_u64()).unwrap_or(0),
                        ),
                    );
                    e.version = ev["version"].as_u64().unwrap_or(e.version);
                }
            }
            "threshold_commit" => {
                let eid = ev["entity"].as_str().unwrap_or("");
                if tombstones.contains(eid) {
                    continue;
                }
                if let Some(e) = store.get_mut(eid) {
                    if let Some(to) = ev.get("to").and_then(|v| v.as_str()) {
                        if !to.is_empty() {
                            e.region = to.to_string();
                        }
                    }
                    if let Some(authority) = ev.get("authority") {
                        apply_authority_snapshot(e, authority);
                    } else {
                        let authority_epoch = ev["authority_epoch"]
                            .as_u64()
                            .unwrap_or_else(|| bump_spatial_authority_epoch(e));
                        set_physics_island_authority_epoch(e, authority_epoch);
                    }
                    e.components.insert(
                        "threshold.tx".to_string(),
                        threshold_tx_value(
                            ev.get("tx_id").and_then(|v| v.as_str()).unwrap_or(""),
                            "commit",
                            ev.get("from").cloned().unwrap_or(Value::Null),
                            ev.get("to").cloned().unwrap_or(Value::Null),
                            ev.get("ts_ms").and_then(|v| v.as_u64()).unwrap_or(0),
                        ),
                    );
                    e.version = ev["version"].as_u64().unwrap_or(e.version);
                }
            }
            "threshold_adopt" | "threshold_abort" => {
                let eid = ev["entity"].as_str().unwrap_or("");
                if tombstones.contains(eid) {
                    continue;
                }
                if let Some(e) = store.get_mut(eid) {
                    e.components.remove("threshold.tx");
                    e.version = ev["version"].as_u64().unwrap_or(e.version);
                }
            }
            "mesh_out" => {
                // The source WAL says a departure crossed the process seam: remove the local copy (the
                // receiver's register WAL is the target-side copy). G2.1d: ALSO park the full payload as
                // in-flight channel state -- if no mesh_acked follows by the cut, the handoff is "in the
                // channel", so restore recreates pending_mesh from this record and resends (exactly-once).
                let eid = ev["entity"].as_str().unwrap_or("");
                store.remove(eid);
                let forwarded_epoch = ev["authority_epoch"]
                    .as_u64()
                    .or_else(|| ev["source_durable_gen"].as_u64())
                    .unwrap_or(0);
                mesh_forwarded_epoch
                    .entry(eid.to_string())
                    .and_modify(|epoch| *epoch = (*epoch).max(forwarded_epoch))
                    .or_insert(forwarded_epoch);
                if !g2d_off {
                    recovered_pending.insert(eid.to_string(), ev.clone());
                }
            }
            "mesh_acked" => {
                // The entity crossed the seam AND the neighbour ACK'd it -> it lives THERE now. Drop the
                // local copy AND clear the in-flight channel state (the handoff completed -> do NOT resend).
                let eid = ev["entity"].as_str().unwrap_or("");
                store.remove(eid);
                recovered_pending.remove(eid);
            }
            "partition_config" => {
                last_partition = Some(ev.clone()); // R0.2: keep the LATEST -> restored before serving routing
            }
            "reserve_entity_ids" => {
                recovered_id_hwm = recovered_id_hwm.max(ev["next_id"].as_u64().unwrap_or(0));
            }
            _ => {}
        }
    }
    // Crash before threshold_commit -> recover as ABORT, not as a half-open threshold.
    for e in store.values_mut() {
        let committed = e
            .components
            .get("threshold.tx")
            .and_then(|v| v.get("phase"))
            .and_then(|v| v.as_str())
            == Some("commit");
        if !committed {
            e.components.remove("threshold.tx");
        }
    }
    (
        store,
        tombstones,
        last_partition,
        recovered_pending,
        mesh_forwarded_epoch,
        recovered_id_hwm,
    )
}

fn arr2(v: Option<&Value>) -> [f64; 2] {
    if let Some(a) = v.and_then(|x| x.as_array()) {
        [
            a.first().and_then(|x| x.as_f64()).unwrap_or(0.0),
            a.get(1).and_then(|x| x.as_f64()).unwrap_or(0.0),
        ]
    } else {
        [0.0, 0.0]
    }
}

fn frame(v: &Value) -> Vec<u8> {
    let body = serde_json::to_vec(v).unwrap();
    let n = body.len() as u32;
    let mut out = Vec::with_capacity(4 + body.len());
    out.extend_from_slice(&n.to_be_bytes());
    out.extend_from_slice(&body);
    out
}

fn worker_connect_frame(worker_id: &str, region: &str, auth_token: Option<&str>) -> Vec<u8> {
    let mut v = json!({
        "op": "WorkerConnect",
        "worker_id": worker_id,
        "region": region,
        "proto": PROTOCOL_VERSION
    });
    if let Some(token) = auth_token {
        v["auth_token"] = json!(token);
    }
    frame(&v)
}

fn replay_tape_value_bytes(f: &Value, key: &str) -> Option<usize> {
    f.get(key)
        .and_then(|value| serde_json::to_vec(value).ok())
        .map(|body| body.len())
}

fn replay_tape_op_summary(f: &Value, byte_len: usize) -> Value {
    let mut summary = Map::new();
    let op = f.get("op").and_then(|v| v.as_str()).unwrap_or("");
    summary.insert("op".to_string(), json!(op));
    summary.insert("wire_bytes".to_string(), json!(byte_len));
    if let Some(semantics) = operation_semantics(op) {
        summary.insert(
            "persistence".to_string(),
            json!(semantics.persistence.as_str()),
        );
        summary.insert("category".to_string(), json!(semantics.category.as_str()));
        if let Some(response_op) = semantics.response_op {
            summary.insert("response_op".to_string(), json!(response_op));
        }
    }
    for key in [
        "request_id",
        "entity",
        "comp",
        "region",
        "target",
        "src_region",
    ] {
        if let Some(value) = f.get(key).and_then(|v| v.as_str()) {
            summary.insert(key.to_string(), json!(value));
        }
    }
    if let Some(epoch) = f.get("authority_epoch").and_then(|v| v.as_u64()) {
        summary.insert("authority_epoch".to_string(), json!(epoch));
    }
    if f.get("auth_token").is_some() {
        summary.insert("credential_present".to_string(), json!(true));
    }
    if let Some(bytes) = replay_tape_value_bytes(f, "value") {
        summary.insert("value_bytes".to_string(), json!(bytes));
    }
    if let Some(bytes) = replay_tape_value_bytes(f, "payload") {
        summary.insert("payload_bytes".to_string(), json!(bytes));
    }
    if let Some(bytes) = replay_tape_value_bytes(f, "components") {
        summary.insert("components_bytes".to_string(), json!(bytes));
    }
    if let Some(bytes) = replay_tape_value_bytes(f, "updates") {
        summary.insert("updates_bytes".to_string(), json!(bytes));
    }
    if let Some(count) = f.get("updates").and_then(|v| v.as_array()).map(Vec::len) {
        summary.insert("update_count".to_string(), json!(count));
    }
    if let Some(count) = f
        .get("components")
        .and_then(|v| v.as_object())
        .map(Map::len)
    {
        summary.insert("component_count".to_string(), json!(count));
    }
    Value::Object(summary)
}

fn partition_schema_for_state(state: &ServerState) -> PartitionSchema {
    if let Some((cols, rows, _cell_w, _cell_h)) = state.grid2d {
        // parse_grid2d already rejects zero dimensions; keep the core constructor as the contract gate.
        PartitionSchema::grid2d(cols as u64, rows as u64)
            .expect("runtime grid2d dimensions must satisfy the spatial contract")
    } else {
        PartitionSchema::strip1d(state.boundaries.len() as u64)
    }
}

fn partition_schema_contract_value(schema: PartitionSchema) -> Value {
    match schema {
        PartitionSchema::Grid2D { cols, rows } => {
            json!({
                "kind": "grid2d",
                "cols": cols,
                "rows": rows
            })
        }
        PartitionSchema::Strip1D { boundary_count } => {
            json!({
                "kind": "strip1d",
                "boundary_count": boundary_count
            })
        }
    }
}

fn spatial_schema_for_state(state: &ServerState) -> SpatialSchema {
    SpatialSchema::current_2d(partition_schema_for_state(state))
}

fn partition_map_for_state(state: &ServerState) -> VersionedPartitionMap {
    let spec = if let Some((cols, rows, cell_w, cell_h)) = state.grid2d {
        PartitionMapSpec::grid2d(cols as u64, rows as u64, cell_w, cell_h, [0.0, 0.0])
            .expect("runtime grid2d partition map must be reproducible")
    } else {
        let splits: Vec<RegionSplitSpec> = state
            .splits
            .iter()
            .map(|(region, boundaries)| {
                RegionSplitSpec::new(region.clone(), boundaries.clone())
                    .expect("runtime strip splits must be reproducible")
            })
            .collect();
        PartitionMapSpec::strip1d(state.boundaries.clone(), splits)
            .expect("runtime strip partition map must be reproducible")
    };
    VersionedPartitionMap::new(state.zone_topology_rev, spec)
}

fn spatial_schema_contract(state: &ServerState) -> Value {
    let schema = spatial_schema_for_state(state);
    json!({
        "spatial_dim": schema.spatial_dim.as_wire_str(),
        "coordinate_codec": schema.coordinate_codec.as_wire_str(),
        "partition_schema": partition_schema_contract_value(schema.partition_schema)
    })
}

fn partition_map_contract(state: &ServerState) -> Value {
    partition_map_contract_value(&partition_map_for_state(state))
}

fn record_replay_tape_spatial_contract(state: &ServerState, event: &mut Map<String, Value>) {
    let schema = spatial_schema_for_state(state);
    event.insert(
        "spatial_dim".to_string(),
        json!(schema.spatial_dim.as_wire_str()),
    );
    event.insert(
        "coordinate_codec".to_string(),
        json!(schema.coordinate_codec.as_wire_str()),
    );
    event.insert(
        "component_registry_version".to_string(),
        json!(STANDARD_COMPONENT_REGISTRY_VERSION),
    );
    event.insert(
        "partition_schema".to_string(),
        partition_schema_contract_value(schema.partition_schema),
    );
}

fn record_replay_tape_ingress(
    state: &ServerState,
    wid: &str,
    f: &Value,
    byte_len: usize,
    outcome: &str,
    reason: Option<&str>,
) {
    let Some(tape) = state.replay_tape.as_ref() else {
        return;
    };
    let (role, region) = state
        .workers
        .get(wid)
        .map(|w| (w.role.as_str(), w.region.as_str()))
        .unwrap_or(("unknown", ""));
    let mut event = Map::new();
    event.insert("kind".to_string(), json!("broker_ingress"));
    event.insert("t_ms".to_string(), json!(now_millis()));
    event.insert("peer".to_string(), json!(wid));
    event.insert("role".to_string(), json!(role));
    event.insert("region".to_string(), json!(region));
    event.insert("outcome".to_string(), json!(outcome));
    event.insert("durable_gen".to_string(), json!(state.durable_gen));
    event.insert("pending_gen".to_string(), json!(state.pending_gen));
    record_replay_tape_spatial_contract(state, &mut event);
    event.insert(
        "op_summary".to_string(),
        replay_tape_op_summary(f, byte_len),
    );
    if let Some(reason) = reason {
        event.insert("reason".to_string(), json!(reason));
    }
    tape.record(Value::Object(event));
}

struct ReplayConnectRecord<'a> {
    wid: &'a str,
    frame: &'a Value,
    byte_len: usize,
    region: &'a str,
    attributes: &'a HashSet<String>,
    outcome: &'a str,
    reason: Option<&'a str>,
}

fn record_replay_tape_connect(state: &ServerState, rec: ReplayConnectRecord<'_>) {
    let Some(tape) = state.replay_tape.as_ref() else {
        return;
    };
    let role = peer_role_for(rec.region, rec.attributes);
    let mut event = Map::new();
    event.insert("kind".to_string(), json!("broker_connect"));
    event.insert("t_ms".to_string(), json!(now_millis()));
    event.insert("peer".to_string(), json!(rec.wid));
    event.insert("role".to_string(), json!(role.as_str()));
    event.insert("region".to_string(), json!(rec.region));
    event.insert("outcome".to_string(), json!(rec.outcome));
    event.insert("wire_bytes".to_string(), json!(rec.byte_len));
    event.insert(
        "requested_region".to_string(),
        rec.frame.get("region").cloned().unwrap_or(Value::Null),
    );
    event.insert(
        "proto".to_string(),
        rec.frame.get("proto").cloned().unwrap_or(Value::Null),
    );
    event.insert(
        "credential_present".to_string(),
        json!(rec.frame.get("auth_token").is_some()),
    );
    event.insert("attribute_count".to_string(), json!(rec.attributes.len()));
    record_replay_tape_spatial_contract(state, &mut event);
    if let Some(reason) = rec.reason {
        event.insert("reason".to_string(), json!(reason));
    }
    tape.record(Value::Object(event));
}

struct ReplayHandoffRecord<'a> {
    path: &'a str,
    eid: &'a str,
    from: Option<&'a str>,
    to: Option<&'a str>,
    authority_epoch: Option<u64>,
    source_durable_gen: Option<u64>,
    lease_epoch: Option<u64>,
}

fn record_replay_tape_handoff(state: &ServerState, rec: ReplayHandoffRecord<'_>) {
    let Some(tape) = state.replay_tape.as_ref() else {
        return;
    };
    let mut event = Map::new();
    event.insert("kind".to_string(), json!("broker_handoff"));
    event.insert("t_ms".to_string(), json!(now_millis()));
    event.insert("path".to_string(), json!(rec.path));
    event.insert("entity".to_string(), json!(rec.eid));
    event.insert("durable_gen".to_string(), json!(state.durable_gen));
    record_replay_tape_spatial_contract(state, &mut event);
    if let Some(from) = rec.from {
        event.insert("from".to_string(), json!(from));
    }
    if let Some(to) = rec.to {
        event.insert("to".to_string(), json!(to));
    }
    if let Some(authority_epoch) = rec.authority_epoch {
        event.insert("authority_epoch".to_string(), json!(authority_epoch));
    }
    if let Some(source_durable_gen) = rec.source_durable_gen {
        event.insert("source_durable_gen".to_string(), json!(source_durable_gen));
    }
    if let Some(lease_epoch) = rec.lease_epoch {
        event.insert("lease_epoch".to_string(), json!(lease_epoch));
    }
    tape.record(Value::Object(event));
}

fn record_replay_tape_emit(state: &ServerState, wid: &str, v: &Value) {
    let Some(tape) = state.replay_tape.as_ref() else {
        return;
    };
    let op = v.get("op").and_then(|o| o.as_str()).unwrap_or("");
    if op != "UpdateRejected" && op != "AuthorityChange" {
        return;
    }
    let (role, region) = state
        .workers
        .get(wid)
        .map(|w| (w.role.as_str(), w.region.as_str()))
        .unwrap_or(("unknown", ""));
    let mut event = Map::new();
    event.insert("kind".to_string(), json!("broker_outbound"));
    event.insert("t_ms".to_string(), json!(now_millis()));
    event.insert("peer".to_string(), json!(wid));
    event.insert("role".to_string(), json!(role));
    event.insert("region".to_string(), json!(region));
    event.insert("op".to_string(), json!(op));
    event.insert("durable_gen".to_string(), json!(state.durable_gen));
    record_replay_tape_spatial_contract(state, &mut event);
    if let Some(value) = v.get("request_id").and_then(|value| value.as_str()) {
        event.insert("request_id".to_string(), json!(value));
    }
    if let Some(value) = v.get("entity").and_then(|value| value.as_str()) {
        event.insert("entity".to_string(), json!(value));
    }
    if let Some(value) = v.get("comp").and_then(|value| value.as_str()) {
        event.insert("comp".to_string(), json!(value));
    }
    if let Some(value) = v.get("reason").and_then(|value| value.as_str()) {
        event.insert("reason".to_string(), json!(value));
    }
    if let Some(value) = v.get("error").and_then(|value| value.as_str()) {
        event.insert("error".to_string(), json!(value));
    }
    if let Some(value) = v.get("rejected_op").and_then(|value| value.as_str()) {
        event.insert("rejected_op".to_string(), json!(value));
    }
    if let Some(value) = v.get("peer_role").and_then(|value| value.as_str()) {
        event.insert("peer_role".to_string(), json!(value));
    }
    if let Some(value) = v.get("authoritative").and_then(|value| value.as_bool()) {
        event.insert("authoritative".to_string(), json!(value));
    }
    if let Some(value) = v.get("authority_epoch").and_then(|value| value.as_u64()) {
        event.insert("authority_epoch".to_string(), json!(value));
    }
    if let Some(value) = v.get("mode").and_then(|value| value.as_str()) {
        event.insert("mode".to_string(), json!(value));
    }
    tape.record(Value::Object(event));
}

fn emit(state: &ServerState, wid: &str, v: Value) {
    if let Some(w) = state.workers.get(wid) {
        // G4 bounding (graceful first line): over the soft cap, a slow consumer's DEGRADABLE flood is
        // dropped to bound memory; CRITICAL ops (AuthorityChange / Add+RemoveEntity / CommandResponse /
        // WAL-visible) are never soft-dropped. L3: the degradable-drop cap LOWERS as load_level rises
        // (4096 -> 2048 -> 1024) so a stressed broker sheds degradable floods sooner. CRITICAL goes on to
        // the bounded channel below.
        let op = v.get("op").and_then(|o| o.as_str()).unwrap_or("");
        record_replay_tape_emit(state, wid, &v);
        let degradable = op == "ComponentUpdate" || op == "InspectorFrame";
        let cap = EGRESS_SOFT_CAP >> state.load_level;
        if degradable && w.out_queue.load(Ordering::Relaxed) > cap {
            w.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }
        // T1 STRUCTURAL HARD CAP: the channel is BOUNDED (CHANNEL_CAP). try_send returns Full ONLY when the
        // consumer has not drained CHANNEL_CAP frames -- i.e. it is stuck even on critical-only traffic (the
        // degradable shedding above already kept a healthy-but-bursty consumer well under this). A degradable
        // Full is just dropped (it's degradable, already past the soft cap). A CRITICAL Full means "this
        // consumer is genuinely not draining" -> we MUST NOT grow RAM (unbounded) and MUST NOT silently drop
        // the critical frame; the structural answer is to FORCE-DISCONNECT it (flag here, reaped at the end
        // of dispatch). The client reconnects and re-checks-out via checkout_all, rebuilding its full view --
        // no state is silently lost. This is the bound the G4 comment promised, enforced by the channel TYPE.
        match w.tx.try_send(frame(&v)) {
            Ok(()) => {
                w.out_queue.fetch_add(1, Ordering::Relaxed); // enqueued -> egress backlog (G4 visibility)
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                if degradable {
                    w.dropped.fetch_add(1, Ordering::Relaxed);
                } else {
                    // hard-cap hit on a CRITICAL frame -> mark for force-disconnect (reaped post-dispatch)
                    w.disconnect.store(true, Ordering::Relaxed);
                }
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {} // receiver gone (already disconnecting)
        }
    }
}

// T1: reap any worker flagged for force-disconnect (it hit the hard egress cap on a CRITICAL frame). Removing
// it from `workers` DROPS its bounded Sender, which makes the writer task's rx.recv() return None -> the
// writer exits + closes the socket -> the slow consumer's read loop gets EOF and tears its connection down.
// The client then reconnects and re-checks-out (checkout_all) -> NO silent state loss. Called at the end of
// dispatch (which holds &mut state), so the slow consumer is torn down within ONE frame of the overflow --
// bounding broker RAM at CHANNEL_CAP frames per consumer, structurally (not via any flag).
fn reap_disconnecting(state: &mut ServerState) {
    let kick: Vec<String> = state
        .workers
        .iter()
        .filter(|(_, w)| w.disconnect.load(Ordering::Relaxed))
        .map(|(id, _)| id.clone())
        .collect();
    for wid in kick {
        remove_from_interest_grid(state, &wid);
        state.workers.remove(&wid);
        println!("[T1] worker '{wid}' force-disconnected -- hard egress cap ({CHANNEL_CAP}) hit on a critical frame (slow/stuck consumer); it will reconnect + re-checkout");
    }
}

fn refill_ingress_tokens(w: &mut WorkerHandle, rate_per_sec: f64, burst_frames: f64, now: Instant) {
    if rate_per_sec > 0.0 {
        let elapsed = now
            .checked_duration_since(w.ingress_last_refill)
            .unwrap_or_default()
            .as_secs_f64();
        w.ingress_tokens = (w.ingress_tokens + elapsed * rate_per_sec).min(burst_frames);
    }
    w.ingress_last_refill = now;
}

fn ingress_frame_cost_units(f: &Value, byte_len: usize) -> f64 {
    let op = f.get("op").and_then(|v| v.as_str()).unwrap_or("");
    let op_cost: f64 = match op {
        // Cheap keepalive / telemetry frames are still at least one ingress unit: a peer can otherwise
        // spin these forever as a scheduler/lock tax.
        "Heartbeat" | "Metrics" | "LogMessage" | "Disconnect" => 1.0,
        // Read fan-out, ownership, identity, and component-bulk paths cost more than one scalar update.
        "CreateEntity"
        | "DeleteEntity"
        | "EntityQuery"
        | "ReserveEntityIds"
        | "SetComponentAuthority"
        | "BatchUpdate"
        | "Fold"
        | "CriticalSection" => 4.0,
        // Cross-broker/control frames are trusted-authenticated but still expensive under a bad peer.
        "MeshHandoff" | "MeshGhost" | "MeshAck" | "CommandRequest" | "CommandResponse" => 2.0,
        _ => 1.0,
    };
    let byte_cost = byte_len as f64 / INGRESS_BYTES_PER_TOKEN;
    op_cost.max(byte_cost).max(1.0)
}

fn reject_ingress_rate_limit(
    state: &mut ServerState,
    wid: &str,
    f: &Value,
    byte_len: usize,
) -> bool {
    let rate = state.ingress_rate_per_sec;
    let burst = state.ingress_burst_frames.max(1.0);
    let now = Instant::now();
    let Some(w) = state.workers.get_mut(wid) else {
        return false;
    };

    refill_ingress_tokens(w, rate, burst, now);
    let cost = ingress_frame_cost_units(f, byte_len);
    if w.ingress_tokens >= cost {
        w.ingress_tokens -= cost;
        return false;
    }

    w.ingress_rejected = w.ingress_rejected.saturating_add(1);
    let remaining_tokens = w.ingress_tokens;
    let op = f.get("op").and_then(|v| v.as_str()).unwrap_or("");
    emit(
        state,
        wid,
        json!({
            "op": "UpdateRejected",
            "request_id": f.get("request_id").cloned().unwrap_or(Value::Null),
            "entity": f.get("entity").cloned().unwrap_or(Value::Null),
            "comp": f.get("comp").cloned().unwrap_or(Value::Null),
            "error": "rate_limit_error",
            "rate_limited": true,
            "limited_op": op,
            "rate_limit_cost": cost,
            "rate_limit_tokens": remaining_tokens,
            "reason": "rate_limit_error: ingress cost budget exceeded"
        }),
    );
    true
}

fn role_policy_allows(role: PeerRole, attributes: &HashSet<String>, op: &str) -> bool {
    if op == "Health" {
        return true;
    }
    if op == "SetComponentAuthority" && attributes.contains("kernel_admin") {
        return true;
    }
    if op == "SnapshotMarker"
        && attributes
            .iter()
            .any(|a| a == "snapshot" || a == "inspector" || a == "kernel_admin")
    {
        return true;
    }
    if op == "Drain"
        && attributes
            .iter()
            .any(|a| a == "kernel_admin" || a == "inspector" || a == "ops")
    {
        return true;
    }
    if op == "InspectorQuery"
        && attributes
            .iter()
            .any(|a| a == "inspector" || a == "debug" || a == "kernel_admin")
    {
        return true;
    }
    match role {
        PeerRole::Worker => !matches!(
            op,
            "MeshHandoff" | "MeshAck" | "MeshGhost" | "MeshGhostRemove"
        ),
        PeerRole::Client => matches!(
            op,
            "Heartbeat"
                | "LogMessage"
                | "Metrics"
                | "Disconnect"
                | "Interest"
                | "EntityQuery"
                | "CommandRequest"
                | "UpdateComponent"
        ),
        PeerRole::Observer => {
            matches!(
                op,
                "Heartbeat" | "LogMessage" | "Metrics" | "Disconnect" | "Interest" | "EntityQuery"
            ) || (op == "InspectorQuery"
                && attributes.iter().any(|a| a == "inspector" || a == "debug"))
        }
        PeerRole::Mesh => matches!(
            op,
            "Heartbeat"
                | "LogMessage"
                | "Metrics"
                | "Disconnect"
                | "MeshHandoff"
                | "MeshAck"
                | "MeshGhost"
                | "MeshGhostRemove"
        ),
    }
}

fn reject_role_policy(state: &mut ServerState, wid: &str, f: &Value) -> bool {
    let op = f.get("op").and_then(|v| v.as_str()).unwrap_or("");
    let Some((role, attributes)) = state
        .workers
        .get(wid)
        .map(|w| (w.role, w.attributes.clone()))
    else {
        return false;
    };
    if role_policy_allows(role, &attributes, op) {
        return false;
    }
    emit(
        state,
        wid,
        json!({
            "op": "UpdateRejected",
            "request_id": f.get("request_id").cloned().unwrap_or(Value::Null),
            "entity": f.get("entity").cloned().unwrap_or(Value::Null),
            "comp": "role_policy",
            "error": "role_policy_error",
            "rejected_op": op,
            "peer_role": role.as_str(),
            "reason": format!("role {} cannot send {}", role.as_str(), op)
        }),
    );
    true
}

// L1 event-storm: flush the buffered EntityEvents to interested workers, COALESCED by class so a 1000-event
// burst delivers BOUNDED output -- visual: one per coalesce_key carrying a count (1000 tracers from one
// source -> 1 with count=1000); debug: dropped under the storm. The client still orders by sim_time/gen;
// this caps egress + the client frame (the deepest L1 product hole). NOTE: CRITICAL events no longer reach
// this buffer -- they deliver INLINE at ingress (see the EntityEvent handler) so their "never dropped/exact/
// timely" guarantee is load-independent (not behind this lock-contending 20Hz tick). The `_ =>` critical arm
// below is kept only as a defensive fallback (delivers ALL, exact) in case any path ever buffers a critical.
fn flush_events(state: &mut ServerState) {
    if state.event_outbox.is_empty() {
        return;
    }
    // L3 graceful-degradation: the visual-event budget SHRINKS as load_level rises (64 -> 32 -> 16) so a
    // stressed broker coalesces harder; critical events are unaffected (always delivered, exact count).
    let max_visual: usize = 64usize >> state.load_level;
    let buffered = std::mem::take(&mut state.event_outbox);
    // per worker -> (critical events [all], visual coalesce_key -> (representative, count))
    let mut per_worker: CoalescedEventsByWorker = HashMap::new();
    for ev in buffered {
        let base = json!({"op":"EntityEvent","entity":ev.eid,"event":ev.event,"payload":ev.payload,
            "sim_time":ev.sim_time,"gen":ev.gen,"class":ev.class});
        for wid in &ev.target_wids {
            let entry = per_worker
                .entry(wid.clone())
                .or_insert_with(|| (Vec::new(), HashMap::new()));
            match ev.class.as_str() {
                "visual" => {
                    let v = entry
                        .1
                        .entry(ev.coalesce_key.clone())
                        .or_insert_with(|| (base.clone(), 0));
                    v.0 = base.clone(); // newest representative (last buffered = latest)
                    v.1 += 1;
                }
                "debug" => {} // dropped under the storm (lowest priority)
                _ => entry.0.push(base.clone()), // critical (default): keep ALL, exact count
            }
        }
    }
    for (wid, (critical, visual)) in per_worker {
        for c in critical {
            emit(state, &wid, c);
        }
        for (_, (mut rep, count)) in visual.into_iter().take(max_visual) {
            rep["count"] = json!(count); // one per key, carrying how many coalesced
            emit(state, &wid, rep);
        }
    }
}

fn quantize_f64(v: f64, grid: f64) -> f64 {
    if grid <= 0.0 {
        v
    } else {
        (v / grid).round() * grid
    }
}

fn quantize_array(value: &Value, grid: f64) -> Value {
    match value.as_array() {
        Some(items) => Value::Array(
            items
                .iter()
                .map(|item| {
                    item.as_f64()
                        .map(|v| json!(quantize_f64(v, grid)))
                        .unwrap_or_else(|| item.clone())
                })
                .collect(),
        ),
        None => value.clone(),
    }
}

fn coarsen_component_value(comp: &str, value: &Value, grid: f64) -> Value {
    if grid <= 0.0 {
        return value.clone();
    }
    if comp == "pos" || comp == "vel" {
        return quantize_array(value, grid);
    }
    if comp == "physics" {
        if let Some(obj) = value.as_object() {
            let mut out = obj.clone();
            for key in ["pos", "vel", "lin", "ang"] {
                if let Some(v) = out.get(key).cloned() {
                    out.insert(key.to_string(), quantize_array(&v, grid));
                }
            }
            return Value::Object(out);
        }
    }
    value.clone()
}

fn fidelity_update_for_worker(
    state: &mut ServerState,
    wid: &str,
    eid: &str,
    comp: &str,
    value: &Value,
) -> Option<(Value, bool)> {
    let pos = state.entities.get(eid)?.pos;
    let w = state.workers.get_mut(wid)?;
    if w.full_fidelity_for(pos) {
        return Some((value.clone(), false));
    }

    let rate = w.fidelity_coarse_rate.max(1);
    let key = format!("{eid}:{comp}");
    let seq = w.fidelity_seq.entry(key).or_insert(0);
    *seq += 1;
    if rate > 1 && (*seq - 1) % rate != 0 {
        return None;
    }

    Some((
        coarsen_component_value(comp, value, w.fidelity_coarse_grid),
        true,
    ))
}

fn send_full(state: &mut ServerState, wid: &str, eid: &str) {
    let (pos, vel, comps) = match state.entities.get(eid) {
        Some(e) => (e.pos, e.vel, e.components.clone()),
        None => return,
    };
    emit(
        state,
        wid,
        json!({"op":"CriticalSection","phase":"begin","entity":eid}),
    );
    emit(state, wid, json!({"op":"AddEntity","entity":eid}));
    emit(
        state,
        wid,
        json!({"op":"ComponentUpdate","entity":eid,"comp":"pos","value":[pos[0],pos[1]]}),
    );
    emit(
        state,
        wid,
        json!({"op":"ComponentUpdate","entity":eid,"comp":"vel","value":[vel[0],vel[1]]}),
    );
    for (k, val) in comps.iter() {
        emit(
            state,
            wid,
            json!({"op":"ComponentUpdate","entity":eid,"comp":k,"value":val}),
        );
    }
    emit(
        state,
        wid,
        json!({"op":"CriticalSection","phase":"end","entity":eid}),
    );
    if let Some(w) = state.workers.get_mut(wid) {
        w.view.insert(eid.to_string());
    }
    grant_client_authority(state, wid, eid);
}

fn grant_client_authority(state: &mut ServerState, wid: &str, eid: &str) {
    // on checkout, grant `wid` client-authority over each comp the entity's ACL client-writes to one
    // of its attributes -- emit AuthorityChange(true) so the client learns it owns its avatar's comp.
    let grants: Vec<String> = match state.entities.get(eid) {
        Some(e) => match e
            .components
            .get("acl")
            .and_then(|a| a.get("client_write"))
            .and_then(|m| m.as_object())
        {
            Some(cw) => {
                let attrs = state
                    .workers
                    .get(wid)
                    .map(|w| w.attributes.clone())
                    .unwrap_or_default();
                cw.iter()
                    .filter(|(_, v)| v.as_str().map(|a| attrs.contains(a)).unwrap_or(false))
                    .map(|(comp, _)| comp.clone())
                    .collect()
            }
            None => vec![],
        },
        None => vec![],
    };
    for comp in grants {
        grant_authority(state, wid, eid, &comp);
    }
}

fn checkout_all(state: &mut ServerState, wid: &str) {
    let eids: Vec<String> = state.entities.keys().cloned().collect();
    for eid in eids {
        let (pos, region) = {
            let e = &state.entities[&eid];
            (e.pos, e.region.clone())
        };
        let interested = match (state.workers.get(wid), state.entities.get(&eid)) {
            (Some(w), Some(e)) => visible(w, e),
            _ => false,
        };
        let _ = pos;
        if interested {
            send_full(state, wid, &eid);
            let _ = region;
            grant_region_physics_island_authority(state, wid, &eid);
        }
    }
}

fn renew(state: &mut ServerState, wid: &str) {
    let until = Instant::now() + Duration::from_secs_f64(state.lease_ttl);
    let regions: Vec<String> = state
        .region_worker
        .iter()
        .filter(|(_, owner)| owner.as_str() == wid)
        .map(|(r, _)| r.clone())
        .collect();
    for r in regions {
        state.region_expires.insert(r, until);
    }
}

#[derive(Default)]
struct SpawnAuthoritySeed {
    epoch: Option<u64>,
    snapshot: Option<Value>,
}

fn spawn_in_region(
    state: &mut ServerState,
    eid: &str,
    pos: [f64; 2],
    vel: [f64; 2],
    comps: Map<String, Value>,
    requested_region: Option<&str>,
    authority: SpawnAuthoritySeed,
) -> bool {
    if state.deleted_entities.contains(eid) {
        return false;
    }
    let region = match state.grid2d {
        // D1: 2D-grid mode derives the cell from (x,y); single-broker so requested_region is moot here
        Some((c, r, cw, ch)) => region_2d(pos, c, r, cw, ch),
        None => spawn_region(pos, requested_region, &state.boundaries, &state.splits),
    };
    spawn_committed_region(state, eid, pos, vel, comps, region, authority)
}

fn spawn_committed_region(
    state: &mut ServerState,
    eid: &str,
    pos: [f64; 2],
    vel: [f64; 2],
    comps: Map<String, Value>,
    region: String,
    authority: SpawnAuthoritySeed,
) -> bool {
    if state.deleted_entities.contains(eid) {
        return false;
    }
    state.mesh_forwarded_epoch.remove(eid);
    let epoch = authority.epoch.unwrap_or(1);
    state.entities.insert(
        eid.to_string(),
        Entity {
            pos,
            vel,
            authority: initial_authority_map(&comps, epoch),
            components: comps.clone(),
            region: region.clone(),
            version: 1,
            last_broadcast_cell: Some(interest_cell_of(pos)),
        },
    );
    if let (Some(e), Some(snapshot)) = (state.entities.get_mut(eid), authority.snapshot.as_ref()) {
        apply_authority_snapshot(e, snapshot);
    }
    let authority = state
        .entities
        .get(eid)
        .map(|e| authority_to_json(&e.authority))
        .unwrap_or_else(|| json!({}));
    // #2 WAL-THEN-PUBLISH: persist the spawn BEFORE announcing the entity. If the WAL append fails (disk full /
    // fail-closed) the entity did NOT durably persist -> roll the in-memory insert back and do NOT publish it,
    // so a CreateEntity that didn't survive is never broadcast to observers (it would vanish on restart).
    if state
        .wal_append(&json!({
            "kind":"register","entity":eid,"version":1,"authority_epoch":epoch,
            "pos":[pos[0],pos[1]],"vel":[vel[0],vel[1]],
            "components":Value::Object(comps),"region":region.clone(),"authority":authority
        }))
        .is_err()
    {
        state.entities.remove(eid); // not persisted -> not live; never announce it
        return false;
    }
    let wids: Vec<String> = state.workers.keys().cloned().collect();
    for wid in wids {
        let interested = match state.entities.get(eid) {
            Some(e) => visible(&state.workers[&wid], e),
            None => false,
        };
        if interested {
            send_full(state, &wid, eid);
            let _ = &region;
            grant_region_physics_island_authority(state, &wid, eid);
        }
    }
    true
}

// CROSS-BROKER MESH: hand an entity across the PROCESS seam to the neighbour broker owning `target`.
fn mesh_forward(state: &mut ServerState, eid: &str, target: &str) {
    let (pos, vel, comps, authority_epoch, authority) = match state.entities.get(eid) {
        Some(e) => {
            let mut authority = e.authority.clone();
            let comps_to_move = physics_island_component_names(&e.components, &authority);
            let authority_epoch = comps_to_move
                .iter()
                .map(|comp| component_authority_epoch(e, comp))
                .max()
                .unwrap_or_else(|| component_authority_epoch(e, "pos"))
                .saturating_add(1);
            for comp in comps_to_move {
                let mode = authority
                    .get(&comp)
                    .map(|ca| ca.mode.clone())
                    .unwrap_or_else(|| default_authority_mode(&e.components, &comp));
                if mode == AuthorityMode::ServerPhysicsIsland {
                    authority
                        .entry(comp.clone())
                        .and_modify(|ca| {
                            ca.epoch = authority_epoch;
                            ca.owner = None;
                        })
                        .or_insert(ComponentAuthority {
                            owner: None,
                            epoch: authority_epoch,
                            mode: AuthorityMode::ServerPhysicsIsland,
                        });
                }
            }
            (
                e.pos,
                e.vel,
                e.components.clone(),
                authority_epoch,
                authority_to_json(&authority),
            )
        }
        None => return,
    };
    // L6 SELF-FENCE: if a NEWER incarnation has taken over the region I own (the registry shows my_region at
    // a strictly higher lease_epoch held by a different addr), I am the STALE owner -- do NOT keep emitting
    // ownership traffic. Hold the entity local + park-and-refuse rather than push a stale handoff the peer
    // would (rightly) reject. This is the sender-side half of the fence; the receiver-side reject is the
    // authoritative one. ADDITIVE: superseded_regions is empty unless the registry actually superseded me.
    if !state.my_region.is_empty() && state.superseded_regions.contains(&state.my_region) {
        eprintln!(
            "[fence] SELF-FENCED: region '{}' superseded by a higher lease_epoch -- holding {eid} local, NOT forwarding (stale incarnation)",
            state.my_region
        );
        return;
    }
    // carry velocity across the process seam too -- adopting with [0,0] stalls the body at the boundary
    // then re-accelerates (discontinuous cross-broker flight). pos+vel both ride. Build the frame ONCE so
    // a dropped send can be re-tried verbatim until the neighbour ACKs. `target` rides too so the receiver
    // lands the entity in the right one of ITS regions (N-neighbour, not just an east seam).
    // L6 lease fence: stamp the SENDER's owned region + the lease_epoch it holds for it, so the receiver can
    // fence a stale incarnation (a returned-from-partition old owner carries its OLD, lower epoch).
    let src_region = state.my_region.clone();
    let src_lease_epoch = state.region_lease_epoch.get(&src_region).copied();
    let source_gen = state.pending_gen.saturating_add(1);
    let handoff = json!({"op":"MeshHandoff","entity":eid,"target":target,
        "pos":[pos[0],pos[1]],"vel":[vel[0],vel[1]],
        "authority_epoch":authority_epoch,
        "authority":authority.clone(),
        "src_region":src_region,
        "lease_epoch":src_lease_epoch,
        "source_durable_gen":source_gen,
        "components":Value::Object(comps)});
    let tx = match state.mesh.get(target).cloned() {
        Some(tx) => tx,
        None => {
            // the link is down RIGHT NOW -- keep the entity LOCAL (do not park+remove into a dead seam). A
            // stalled body at the boundary beats a vanished one; the next move re-handoffs when the link returns.
            eprintln!("[mesh] link to {target} down -- keeping {eid} local (not vanishing it into a dead seam)");
            return;
        }
    };
    let mesh_out = json!({"kind":"mesh_out","entity":eid,"target":target,
        "authority_epoch":authority_epoch,"authority":authority,
        "src_region":src_region,
        "lease_epoch":src_lease_epoch,
        "source_durable_gen":source_gen,
        "gen":source_gen,
        "pos":[pos[0],pos[1]],"vel":[vel[0],vel[1]],
        "components":handoff.get("components").cloned().unwrap_or(Value::Null)});
    // WAL + fsync the cross-seam departure BEFORE a neighbour can observe/adopt the handoff. The durable
    // watermark is the source-side visibility gate: crash before it -> source keeps the entity; crash after
    // it -> recovery rebuilds pending_mesh and resends. Sending above source durable_gen would let the
    // neighbour adopt a transition this broker could not reproduce.
    if state.wal_append_nosync(&mesh_out).is_err() {
        eprintln!("[mesh] WAL failed before forwarding {eid} -> {target}; keeping entity local and failing closed");
        return;
    }
    state.pending_gen = source_gen;
    if state.wal_sync().is_err() {
        eprintln!(
            "[mesh] WAL sync failed before forwarding {eid} -> {target}; keeping entity local and failing closed"
        );
        return;
    }
    state.durable_gen = state.durable_gen.max(source_gen);
    state
        .mesh_forwarded_epoch
        .entry(eid.to_string())
        .and_modify(|epoch| *epoch = (*epoch).max(authority_epoch))
        .or_insert(authority_epoch);
    record_replay_tape_handoff(
        state,
        ReplayHandoffRecord {
            path: "mesh_out",
            eid,
            from: Some(src_region.as_str()),
            to: Some(target),
            authority_epoch: Some(authority_epoch),
            source_durable_gen: Some(source_gen),
            lease_epoch: src_lease_epoch,
        },
    );
    // Park it pending the neighbour's MeshAck BEFORE attempting the send; if this process crashes after the WAL
    // but before/while sending, recovery reconstructs the same pending handoff from mesh_out.
    state.pending_mesh.insert(
        eid.to_string(),
        (handoff.clone(), Instant::now(), target.to_string()),
    );
    state.entities.remove(eid);
    let delivered = tx.send(frame(&handoff)).is_ok();
    if !delivered {
        // the link is down RIGHT NOW -- keep the entity LOCAL (do not park+remove into a dead seam). A
        // WAL already linearized the departure, so do NOT resurrect locally; pending_mesh will resend once
        // the mesh task reconnects.
        eprintln!("[mesh] link to {target} dropped after WAL -- parked {eid} pending resend");
    }
    // CROSS-BROKER handoff counted HERE -- the delivered-departure linearization point (a live link accepted
    // the frame + the mesh_out is WAL'd). The mirror of `handoffs += 1` in the LOCAL handoff() path, but a
    // DISTINCT metric so the Inspector sees same-broker vs cross-server handoffs separately. Counted on the
    // SENDING broker (the one initiating the authority transfer across the seam); a dropped/re-sent frame goes
    // through resend_pending_mesh (a different path), so this fires exactly once per logical cross-broker move.
    state.metrics.mesh_handoffs += 1;
    let wids: Vec<String> = state.workers.keys().cloned().collect();
    for wid in wids {
        if state.workers[&wid].view.contains(eid) {
            emit(state, &wid, json!({"op":"RemoveEntity","entity":eid}));
            if let Some(w) = state.workers.get_mut(&wid) {
                w.view.remove(eid);
            }
        }
    }
    eprintln!("[mesh] forwarded {eid} -> {target} (pending the neighbour's MeshAck, re-sent until confirmed)");
}

// B1: re-send any cross-broker handoff the neighbour hasn't ACK'd yet (a dropped MeshHandoff OR a dropped
// MeshAck). The receive is idempotent (re-sends are re-ACK'd, never double-adopted); entries clear on
// MeshAck. Only re-send entries older than the ACK round-trip so the happy path doesn't double-send.
fn resend_pending_mesh(state: &mut ServerState) {
    if state.pending_mesh.is_empty() {
        return;
    }
    let now = Instant::now();
    let stale: Vec<(Value, String)> = state
        .pending_mesh
        .values()
        .filter(|(_, t, _)| now.duration_since(*t) > Duration::from_secs(1))
        .map(|(fr, _, tgt)| (fr.clone(), tgt.clone()))
        .collect();
    for (fr, tgt) in &stale {
        if let Some(tx) = state.mesh.get(tgt) {
            let _ = tx.send(frame(fr));
        }
    }
}

// ── CROSS-BROKER SEAM-INTEREST: push this broker's near-seam entities to the meshed neighbour as GHOSTS ──
// A ghost is a READ-ONLY mirror so the neighbour's worker can SEE + TARGET across the seam without anything
// crossing. For each LOCAL entity (one this broker owns, i.e. in `entities`) that sits within `interest_band`
// of a boundary which separates its strip from a NEIGHBOUR strip we MESH to, send a `MeshGhost` frame over the
// existing mesh channel to that neighbour. The neighbour stores it in `ghosts` (NOT `entities`) -> structurally
// non-authoritative. Authority NEVER leaves this broker: we keep simulating + owning the entity; the ghost is a
// projection. A ghost that LEAVES the band (or whose source disappears) is reaped by the receiver's TTL.
//
// Cost is bounded to the band, not the world: only entities within `interest_band` of a meshed seam are pushed,
// and only on the monitor tick (300ms) -- a fine ghost-refresh rate (the actual cross-seam DAMAGE is the S3
// projectile path, which is realtime; the ghost only needs to keep the AIM-point fresh). GW_INTEREST_BAND=0
// (default) short-circuits the whole pass -> zero work + zero wire for every gate that doesn't opt in.
fn push_border_ghosts(state: &mut ServerState) {
    if state.interest_band <= 0.0 || state.mesh.is_empty() || state.entities.is_empty() {
        return;
    }
    let band = state.interest_band;
    let bounds = state.boundaries.clone();
    state.ghost_seq = state.ghost_seq.wrapping_add(1);
    // Group the frames to send by TARGET neighbour region, so we serialize each entity once per neighbour it
    // borders (an entity near a seam borders exactly one neighbour strip on that side).
    let mut to_send: Vec<(String, Value)> = Vec::new(); // (target_region, MeshGhost frame)
    for (eid, e) in state.entities.iter() {
        // Only project PERSISTENT entities (units/buildings/etc.) as ghosts -- a PROJECTILE is transient and
        // crosses via the proven MeshHandoff path when it actually enters the neighbour zone; ghosting it would
        // be churn + a confusing "enemy" for a targeter. (A wreck/economy is likewise not a target.) Everything
        // else IS projected so the neighbour can see + target it across the seam.
        match e.components.get("kind").and_then(|v| v.as_str()) {
            Some("projectile") | Some("wreck") | Some("economy") => continue,
            _ => {}
        }
        // Only POSITION-SHARDED strip entities have a meaningful seam; a NAMED region (planet/portal) has no
        // 1D-strip neighbour to push along (its "seam" is a portal, handled by Fold, not border-interest).
        let cur_coarse = coarse_region(&e.region);
        let cur_idx = match strip_index_of_name(cur_coarse, &bounds) {
            Some(i) => i,
            None => continue,
        };
        let x = e.pos[0];
        // Each strip i has up to two seams: its UPPER edge bounds[i] (neighbour strip i+1) and its LOWER edge
        // bounds[i-1] (neighbour strip i-1). Push to whichever neighbour seam the entity is within `band` of AND
        // which this broker actually meshes to (a remote zone).
        // UPPER seam -> neighbour strip i+1
        if cur_idx < bounds.len() && (bounds[cur_idx] - x).abs() <= band {
            let nbr = strip_name(cur_idx + 1, &bounds);
            if state.mesh_regions.contains(&nbr) && !state.region_worker.contains_key(&nbr) {
                to_send.push((nbr, ghost_frame(eid, e, &state.my_region)));
            }
        }
        // LOWER seam -> neighbour strip i-1
        if cur_idx > 0 && (x - bounds[cur_idx - 1]).abs() <= band {
            let nbr = strip_name(cur_idx - 1, &bounds);
            if state.mesh_regions.contains(&nbr) && !state.region_worker.contains_key(&nbr) {
                to_send.push((nbr, ghost_frame(eid, e, &state.my_region)));
            }
        }
    }
    for (target, fr) in to_send {
        if let Some(tx) = state.mesh.get(&target) {
            let _ = tx.send(frame(&fr));
        }
    }
}

// Build a MeshGhost frame: the read-only projection of a local entity (pos/vel/components + the owning region).
fn ghost_frame(eid: &str, e: &Entity, owner_region: &str) -> Value {
    json!({"op":"MeshGhost","entity":eid,
        "pos":[e.pos[0],e.pos[1]],"vel":[e.vel[0],e.vel[1]],
        "components":Value::Object(e.components.clone()),
        "owner_region":owner_region})
}

// ── reap ghosts whose source stopped refreshing them (left the band, or the source broker went away) ──
// A ghost is refreshed every monitor tick while its source entity stays in the band. If it isn't refreshed
// within GHOST_TTL the source has moved out of the band (or crossed -- in which case the REAL entity arrives
// via MeshHandoff and the worker adopts it) or the link dropped; either way the stale ghost is removed and a
// RemoveEntity-style notice goes to the workers viewing it so no stale enemy lingers on screen / as a target.
const GHOST_TTL: Duration = Duration::from_millis(1500);
fn reap_stale_ghosts(state: &mut ServerState) {
    if state.ghosts.is_empty() {
        return;
    }
    let now = Instant::now();
    let stale: Vec<String> = state
        .ghosts
        .iter()
        .filter(|(_, g)| now.duration_since(g.last_seen) > GHOST_TTL)
        .map(|(eid, _)| eid.clone())
        .collect();
    for eid in stale {
        remove_ghost(state, &eid);
    }
}

// Remove a ghost + tell every worker viewing it to drop it (RemoveEntity). Used by the TTL reaper and by an
// explicit MeshGhostRemove from the source (the source can proactively retract a ghost on the entity's delete).
fn remove_ghost(state: &mut ServerState, eid: &str) {
    if state.ghosts.remove(eid).is_none() {
        return;
    }
    let wids: Vec<String> = state.workers.keys().cloned().collect();
    for wid in wids {
        if state.workers[&wid].view.contains(eid) {
            emit(state, &wid, json!({"op":"RemoveEntity","entity":eid}));
            if let Some(w) = state.workers.get_mut(&wid) {
                w.view.remove(eid);
            }
        }
    }
}

// Is this worker interested in and allowed to read a ghost? Ghosts carry the source component bag,
// so read ACL must apply before a ghost row can feed queries, manifests, or QBI.
fn ghost_visible(w: &WorkerHandle, g: &GhostEntity) -> bool {
    w.interested_in(g.pos) && acl_read_ok_components(&w.attributes, &g.components)
}

// Push a ghost's current state to the interested workers as the SAME AddEntity/ComponentUpdate stream a real
// entity uses -- so the worker + observer render/track it with NO new client op (a ghost looks like any other
// viewed entity on the wire; it just never carries an AuthorityChange(true), so the worker never tries to own
// it). The `ghost:true` component + `owner_region` let a client that cares distinguish it (the observer can
// tint it); a client that doesn't care just sees an entity it can aim at. Sends only the DELTA-relevant comps.
fn propagate_ghost(state: &mut ServerState, eid: &str, first_time: bool) {
    let (pos, vel, comps, owner_region) = match state.ghosts.get(eid) {
        Some(g) => (g.pos, g.vel, g.components.clone(), g.owner_region.clone()),
        None => return,
    };
    let wids: Vec<String> = state.workers.keys().cloned().collect();
    for wid in wids {
        let inside = match state.ghosts.get(eid) {
            Some(g) => ghost_visible(&state.workers[&wid], g),
            None => false,
        };
        let has = state.workers[&wid].view.contains(eid);
        if inside && !has {
            // first appearance to THIS worker: the full checkout stream (so it renders + becomes targetable).
            emit(
                state,
                &wid,
                json!({"op":"CriticalSection","phase":"begin","entity":eid}),
            );
            emit(state, &wid, json!({"op":"AddEntity","entity":eid}));
            // The read-only marker goes FIRST (right after AddEntity), BEFORE the components -- so the receiver
            // migrates the entity into its read-only ghost store before any `kind` arrives. Otherwise a ghost of
            // a PROJECTILE would have its `kind:projectile` mistaken for a local adopt + promoted to a real
            // shell. A ghost NEVER gets an AuthorityChange(true) (the worker's adopt path keys on that), so it
            // is held read-only forever -- exactly the intent. `owner_region` rides alongside (the zone that
            // holds authority over it) so a client can label the cross-seam mirror.
            emit(
                state,
                &wid,
                json!({"op":"ComponentUpdate","entity":eid,"comp":"ghost","value":true}),
            );
            emit(
                state,
                &wid,
                json!({"op":"ComponentUpdate","entity":eid,"comp":"owner_region","value":owner_region}),
            );
            emit(
                state,
                &wid,
                json!({"op":"ComponentUpdate","entity":eid,"comp":"pos","value":[pos[0],pos[1]]}),
            );
            emit(
                state,
                &wid,
                json!({"op":"ComponentUpdate","entity":eid,"comp":"vel","value":[vel[0],vel[1]]}),
            );
            for (k, val) in comps.iter() {
                emit(
                    state,
                    &wid,
                    json!({"op":"ComponentUpdate","entity":eid,"comp":k,"value":val}),
                );
            }
            emit(
                state,
                &wid,
                json!({"op":"CriticalSection","phase":"end","entity":eid}),
            );
            if let Some(w) = state.workers.get_mut(&wid) {
                w.view.insert(eid.to_string());
            }
        } else if !inside && has {
            emit(state, &wid, json!({"op":"RemoveEntity","entity":eid}));
            if let Some(w) = state.workers.get_mut(&wid) {
                w.view.remove(eid);
            }
        } else if inside && !first_time {
            // a refresh: just the moving fields (pos/vel) -> keep the aim-point + render fresh.
            emit(
                state,
                &wid,
                json!({"op":"ComponentUpdate","entity":eid,"comp":"pos","value":[pos[0],pos[1]]}),
            );
            emit(
                state,
                &wid,
                json!({"op":"ComponentUpdate","entity":eid,"comp":"vel","value":[vel[0],vel[1]]}),
            );
        }
    }
}

fn gc_threshold_timeouts(state: &mut ServerState) {
    let ttl_ms = state.threshold_ttl.as_millis() as u64;
    if ttl_ms == 0 {
        return;
    }
    let now = now_millis();
    let stale: Vec<(String, String, Value, Value)> = state
        .entities
        .iter()
        .filter_map(|(eid, e)| {
            let tx = e.components.get("threshold.tx")?;
            let phase = tx.get("phase").and_then(|v| v.as_str()).unwrap_or("");
            if !matches!(phase, "prepare" | "preload_ready") {
                return None;
            }
            let ts = tx.get("ts_ms").and_then(|v| v.as_u64()).unwrap_or(now);
            if now.saturating_sub(ts) < ttl_ms {
                return None;
            }
            Some((
                eid.clone(),
                tx.get("tx_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                tx.get("from").cloned().unwrap_or(Value::Null),
                tx.get("to").cloned().unwrap_or(Value::Null),
            ))
        })
        .collect();

    for (eid, tx_id, from, to) in stale {
        let version = if let Some(e) = state.entities.get(&eid) {
            if !e.components.contains_key("threshold.tx") {
                continue;
            }
            e.version.saturating_add(1)
        } else {
            continue;
        };
        if state
            .wal_append(&json!({
            "kind":"threshold_abort","entity":&eid,"version":version,
            "writer":"threshold-gc","tx_id":tx_id,"from":from,"to":to,
            "reason":"preload timeout"
            }))
            .is_err()
        {
            state.rejected.push(json!({
                "entity":eid,
                "comp":"threshold.tx",
                "reason":"wal_persist_failed: threshold timeout abort not durably recorded"
            }));
            continue;
        }
        if let Some(e) = state.entities.get_mut(&eid) {
            if e.components.remove("threshold.tx").is_none() {
                continue;
            }
            e.version = version;
        } else {
            continue;
        }
        let wids: Vec<String> = state.workers.keys().cloned().collect();
        for wid in wids {
            if state.workers[&wid].view.contains(&eid) {
                emit(
                    state,
                    &wid,
                    json!({"op":"RemoveComponent","entity":eid,"comp":"threshold.tx"}),
                );
            }
        }
    }
}

fn handoff(state: &mut ServerState, eid: &str, old: &str, new: &str) {
    let _ = handoff_with_position(state, eid, old, new, None);
}

fn handoff_with_position(
    state: &mut ServerState,
    eid: &str,
    old: &str,
    new: &str,
    pos_override: Option<[f64; 2]>,
) -> bool {
    // Remote process seam: defer the actual mesh_out until the current durable batch has applied all
    // same-entity writes. mesh_forward still WALs+fsyncs before send; the queue only prevents an
    // inline remove from tearing the batch and dropping later component writes.
    if state.mesh_regions.contains(new) && !state.region_worker.contains_key(new) {
        return queue_remote_handoff(state, eid, new);
    }

    queue_local_handoff(state, eid, old, new, pos_override, "handoff")
}

fn queue_remote_handoff(state: &mut ServerState, eid: &str, target: &str) -> bool {
    if !state.entities.contains_key(eid) {
        return false;
    }
    if let Some(existing) = state
        .pending_remote_handoffs
        .iter_mut()
        .find(|h| h.eid == eid)
    {
        existing.target = target.to_string();
        return true;
    }
    state.pending_remote_handoffs.push(PendingRemoteHandoff {
        eid: eid.to_string(),
        target: target.to_string(),
    });
    true
}

fn flush_pending_remote_handoffs(state: &mut ServerState) {
    if state.pending_remote_handoffs.is_empty() {
        return;
    }
    let pending = std::mem::take(&mut state.pending_remote_handoffs);
    for h in pending {
        if remote_handoff_target_still_current(state, &h.eid, &h.target) {
            mesh_forward(state, &h.eid, &h.target);
        }
    }
}

fn remote_handoff_target_still_current(state: &ServerState, eid: &str, target: &str) -> bool {
    let Some(e) = state.entities.get(eid) else {
        return false;
    };
    let next = if let Some((c, r, cw, ch)) = state.grid2d {
        region_2d_after(e.pos, &e.region, c, r, cw, ch)
    } else {
        movement_region_after(e.pos[0], &e.region, &state.boundaries, &state.splits)
    };
    next == target && state.mesh_regions.contains(&next) && !state.region_worker.contains_key(&next)
}

fn queue_local_handoff(
    state: &mut ServerState,
    eid: &str,
    old: &str,
    new: &str,
    pos_override: Option<[f64; 2]>,
    reason: &str,
) -> bool {
    if state.pending_handoffs.iter().any(|h| h.eid == eid) {
        return true;
    }
    let old_wid = state.region_worker.get(old).cloned();
    let new_wid = state.region_worker.get(new).cloned();
    let (ver, authority_epoch, pos, vel, moved_comps, authority) = {
        let Some(e) = state.entities.get(eid) else {
            return false;
        };
        let mut next = e.clone();
        if let Some(pos) = pos_override {
            next.pos = pos;
            next.last_broadcast_cell = Some(interest_cell_of(pos));
        }
        next.region = new.to_string();
        let pending_entity_handoffs = state
            .pending_handoffs
            .iter()
            .filter(|h| h.eid == eid)
            .count() as u64;
        next.version = next
            .version
            .saturating_add(pending_entity_handoffs)
            .saturating_add(1);
        let (authority_epoch, moved_comps) =
            advance_physics_island_authority(&mut next, old_wid.as_deref(), new_wid.as_deref());
        (
            next.version,
            authority_epoch,
            next.pos,
            next.vel,
            moved_comps,
            authority_to_json(&next.authority),
        )
    };
    let gen = state.pending_gen.saturating_add(1);
    let lease_epoch = state.region_lease_epoch.get(old).copied();
    let prepared = PreparedHandoff {
        gen,
        eid: eid.to_string(),
        from: old.to_string(),
        to: new.to_string(),
        pos,
        vel,
        version: ver,
        authority_epoch,
        authority,
        moved_comps,
        old_wid,
        new_wid,
        lease_epoch,
        reason: reason.to_string(),
    };
    if state
        .wal_append_nosync(&prepared_handoff_wal_event(&prepared))
        .is_err()
    {
        return false;
    }
    state.pending_gen = gen;
    state.pending_handoffs.push(prepared);

    // Assembly handoff: queue children into the SAME pending handoff group so the root and parts cross
    // the same durable barrier. They apply together after wal_sync, not as a visible parent/child tear.
    let children: Vec<String> = state
        .entities
        .iter()
        .filter(|(cid, e)| {
            cid.as_str() != eid && e.components.get("parent").and_then(|p| p.as_str()) == Some(eid)
        })
        .map(|(cid, _)| cid.clone())
        .collect();
    for child in children {
        if let Some(co) = state.entities.get(&child).map(|e| e.region.clone()) {
            if co != new {
                queue_local_handoff(state, &child, &co, new, None, "assembly");
            }
        }
    }
    true
}

fn apply_prepared_handoff(state: &mut ServerState, h: &PreparedHandoff) {
    if let Some(e) = state.entities.get_mut(&h.eid) {
        e.pos = h.pos;
        e.vel = h.vel;
        e.region = h.to.clone();
        e.version = h.version;
        apply_authority_snapshot(e, &h.authority);
        e.last_broadcast_cell = Some(interest_cell_of(h.pos));
    } else {
        state.rejected.push(json!({
            "entity": h.eid,
            "from": h.from,
            "to": h.to,
            "reason": "prepared handoff target entity missing or deleted before durable apply"
        }));
        return;
    }
    state.metrics.handoffs += 1;
    record_replay_tape_handoff(
        state,
        ReplayHandoffRecord {
            path: "local",
            eid: &h.eid,
            from: Some(h.from.as_str()),
            to: Some(h.to.as_str()),
            authority_epoch: Some(h.authority_epoch),
            source_durable_gen: Some(h.gen),
            lease_epoch: h.lease_epoch,
        },
    );

    if let Some(ow) = h.old_wid.clone() {
        if state.workers.contains_key(&ow) {
            for comp in &h.moved_comps {
                revoke_authority(state, &ow, &h.eid, comp);
            }
        }
    }
    if let Some(nw) = h.new_wid.clone() {
        if state.workers.contains_key(&nw) {
            if !state.workers[&nw].view.contains(&h.eid) {
                send_full(state, &nw, &h.eid);
            }
            for comp in &h.moved_comps {
                grant_authority(state, &nw, &h.eid, comp);
            }
        }
    }
}

// C2: pre-handoff AUTHORITY_LOSS_IMMINENT (the overlay's I:<target>).
fn seam_intent_target(x: f64, current: &str, bounds: &[f64]) -> Option<String> {
    let cur_idx = strip_index_of_name(coarse_region(current), bounds)?;
    // entered the UPPER-edge band [bounds[cur_idx], bounds[cur_idx]+H) -> intends the next strip up
    if cur_idx < bounds.len() && x >= bounds[cur_idx] && x < bounds[cur_idx] + H {
        return Some(strip_name(cur_idx + 1, bounds));
    }
    // entered the LOWER-edge band (bounds[cur_idx-1]-H, bounds[cur_idx-1]) -> intends the strip down
    if cur_idx > 0 && x < bounds[cur_idx - 1] && x > bounds[cur_idx - 1] - H {
        return Some(strip_name(cur_idx - 1, bounds));
    }
    None
}

fn handoff_intent_to_json(i: &HandoffIntent) -> Value {
    json!({
        "source": i.source_worker, "target": i.target_worker,
        "source_region": i.source_region, "target_region": i.target_region,
        "epoch": i.epoch, "state": "AUTHORITY_LOSS_IMMINENT"
    })
}

// Emit LOSS_IMMINENT to the current owner, once per (entity,target,epoch); dedup via pending_handoff_intent.
fn maybe_emit_loss_imminent(
    state: &mut ServerState,
    eid: &str,
    source_region: &str,
    target_region: &str,
) {
    let source_worker = match state
        .entities
        .get(eid)
        .and_then(|e| component_authority_owner(e, "pos"))
    {
        Some(w) => w,
        None => return,
    };
    let target_worker = match state.region_worker.get(target_region).cloned() {
        Some(w) => w,
        None => return,
    };
    let epoch = state
        .entities
        .get(eid)
        .map(|e| component_authority_epoch(e, "pos"))
        .unwrap_or(0);
    let already = state
        .pending_handoff_intent
        .get(eid)
        .map(|i| i.target_region == target_region && i.epoch == epoch)
        .unwrap_or(false);
    if already {
        return;
    }
    state.pending_handoff_intent.insert(
        eid.to_string(),
        HandoffIntent {
            source_region: source_region.to_string(),
            target_region: target_region.to_string(),
            source_worker: source_worker.clone(),
            target_worker: target_worker.clone(),
            epoch,
        },
    );
    emit(
        state,
        &source_worker,
        json!({
            "op": "AuthorityChange", "entity": eid, "comp": "pos",
            "authoritative": true, "state": "AUTHORITY_LOSS_IMMINENT",
            "authority_epoch": epoch, "mode": "threshold_overlap",
            "handoff_target": target_worker, "handoff_target_region": target_region
        }),
    );
}

// Interest spatial hash: a fixed-cell grid mapping cell -> the AOI-workers whose interest covers it.
// propagate() looks up an updated entity's cell and fans out only to those workers (broadphase) + the exact
// interested_in() narrowphase -- bounding the O(entities × viewers) broadcast for spread-out viewers.
const INTEREST_CELL: f64 = 4.0;

#[allow(dead_code)] // Interest: used by the propagate-prune (next sub-step); the grid is maintained now
fn interest_cell_of(pos: [f64; 2]) -> (i64, i64) {
    (
        (pos[0] / INTEREST_CELL).floor() as i64,
        (pos[1] / INTEREST_CELL).floor() as i64,
    )
}

// the cells a worker's AOI (center, radius) bounding-box overlaps -- a SUPERSET of the circle (the exact
// circle test is the narrowphase), so the grid never misses a genuinely-interested worker. An AOI too large
// to index (> MAX_GRID_CELLS) returns empty -> the caller treats the worker as global (always checked).
fn interest_cells_for(center: [f64; 2], radius: f64) -> Vec<(i64, i64)> {
    const MAX_GRID_CELLS: i64 = 256;
    let x0 = ((center[0] - radius) / INTEREST_CELL).floor() as i64;
    let x1 = ((center[0] + radius) / INTEREST_CELL).floor() as i64;
    let y0 = ((center[1] - radius) / INTEREST_CELL).floor() as i64;
    let y1 = ((center[1] + radius) / INTEREST_CELL).floor() as i64;
    if (x1 - x0 + 1).saturating_mul(y1 - y0 + 1) > MAX_GRID_CELLS {
        return Vec::new();
    }
    let mut cells = Vec::new();
    let mut cx = x0;
    while cx <= x1 {
        let mut cy = y0;
        while cy <= y1 {
            cells.push((cx, cy));
            cy += 1;
        }
        cx += 1;
    }
    cells
}

fn remove_from_interest_grid(state: &mut ServerState, wid: &str) {
    state.global_workers.remove(wid);
    let old: Vec<(i64, i64)> = match state.workers.get_mut(wid) {
        Some(w) => std::mem::take(&mut w.grid_cells),
        None => return,
    };
    for cell in old {
        if let Some(set) = state.interest_grid.get_mut(&cell) {
            set.remove(wid);
            if set.is_empty() {
                state.interest_grid.remove(&cell);
            }
        }
    }
}

// recompute a worker's grid cells from its current AOI. A worker with no aoi_center only lands in
// global_workers if its broker-owned role has default regional/global interest (W/E-style region owners,
// or OBS with an explicit observer/debug/inspector claim). A concrete AOI too large to index also lands in
// global_workers, but interested_in() remains the exact narrowphase. Called on connect and on each Interest op.
fn update_interest_grid(state: &mut ServerState, wid: &str) {
    remove_from_interest_grid(state, wid);
    let (cells, should_be_global) = match state.workers.get(wid) {
        Some(w) => match (w.aoi_center, w.aoi_radius) {
            (Some(c), Some(r)) => {
                let cells = interest_cells_for(c, r);
                let too_large_for_grid = cells.is_empty();
                (cells, too_large_for_grid)
            }
            _ => (Vec::new(), w.should_be_global_interest_holder()),
        },
        None => return,
    };
    if cells.is_empty() && should_be_global {
        state.global_workers.insert(wid.to_string());
    } else if !cells.is_empty() {
        for cell in &cells {
            state
                .interest_grid
                .entry(*cell)
                .or_default()
                .insert(wid.to_string());
        }
    }
    if let Some(w) = state.workers.get_mut(wid) {
        w.grid_cells = cells;
    }
}

fn propagate(state: &mut ServerState, eid: &str, comp: &str, value: &Value) {
    let (pos, last_cell) = match state.entities.get(eid) {
        Some(e) => (e.pos, e.last_broadcast_cell),
        None => return,
    };
    // Interest broadphase: the AOI-workers whose grid cell covers this entity's pos, PLUS the cell it occupied
    // at the last broadcast (so a viewer it just LEFT still gets the RemoveEntity -- no stale ghost), PLUS the
    // global (no-AOI / too-large) workers. interested_in() below is the exact narrowphase (a superset is OK).
    let cell = interest_cell_of(pos);
    let mut wid_set: HashSet<String> = state.global_workers.clone();
    if let Some(set) = state.interest_grid.get(&cell) {
        wid_set.extend(set.iter().cloned());
    }
    if let Some(lc) = last_cell {
        if lc != cell {
            if let Some(set) = state.interest_grid.get(&lc) {
                wid_set.extend(set.iter().cloned());
            }
        }
    }
    let wids: Vec<String> = wid_set.into_iter().collect();
    for wid in wids {
        let inside = match state.entities.get(eid) {
            Some(e) => visible(&state.workers[&wid], e),
            None => false,
        };
        let has = state.workers[&wid].view.contains(eid);
        if inside && !has {
            send_full(state, &wid, eid);
        } else if !inside && has {
            emit(state, &wid, json!({"op":"RemoveEntity","entity":eid}));
            if let Some(w) = state.workers.get_mut(&wid) {
                w.view.remove(eid);
            }
        } else if inside {
            if let Some((out_value, coarse)) =
                fidelity_update_for_worker(state, &wid, eid, comp, value)
            {
                let mut msg =
                    json!({"op":"ComponentUpdate","entity":eid,"comp":comp,"value":out_value});
                if coarse {
                    msg["fidelity"] = json!("coarse");
                }
                emit(state, &wid, msg);
            }
        }
    }
    if let Some(e) = state.entities.get_mut(eid) {
        e.last_broadcast_cell = Some(cell); // remember the cell so the next move notifies the viewers it leaves
    }
}

// ── failover monitor (== the reference server check_leases): reclaim a lapsed region to a STANDBY ──
// dynamic LOAD-BASED load balancing: shift the partition boundary toward the heavier
// region-worker and shed the entities that flip across the new split via the handoff.
// Hardening #1 Step 0: record the monitor-tick lock-hold by path -> the InspectorFrame exposes max + path,
// so the zone-split freeze is MEASURED (expect 50-100ms under load) before the budgeted-rebalance fix.
fn record_lock_hold(state: &mut ServerState, path: &str, dur: std::time::Duration) {
    let ms = dur.as_secs_f64() * 1000.0;
    state.lock_last_hold_ms = ms;
    if ms > state.lock_max_hold_ms {
        state.lock_max_hold_ms = ms;
        state.lock_max_hold_path = path.to_string();
    }
}

fn rebalance(state: &mut ServerState) {
    if state.grid2d.is_some() {
        return; // D1: 2D-grid mode has its own block->worker rebalance (D3); the 1D W|E line-shift does not apply
    }
    if !state.rebalance_jobs.is_empty() {
        return; // a budgeted migration is draining -> don't shift the boundary or pile on another O(N) pass
    }
    // N-ZONE: the W|E load-shift is the classic 2-fixed-worker rebalance (shed across ONE line between W & E).
    // It does not generalize to an N-strip topology (>1 cut, Z<i> workers) -> leave the strip cuts FIXED there
    // (a multi-strip deployment rebalances by adding/moving zone-servers, not by sliding one of N interior lines
    // -- which would silently desync boundaries[i]). Only the 1-boundary W|E topology slides its single line.
    if state.boundaries.len() != 1 {
        return;
    }
    let (threshold, step, lo, hi) = (0.25f64, 0.5f64, -7.0f64, 7.0f64);
    let w_load = state
        .region_worker
        .get("W")
        .and_then(|w| state.worker_load.get(w))
        .copied()
        .unwrap_or(0.0);
    let e_load = state
        .region_worker
        .get("E")
        .and_then(|w| state.worker_load.get(w))
        .copied()
        .unwrap_or(0.0);
    let imbalance = w_load - e_load;
    if imbalance > threshold && state.boundary > lo {
        state.boundary = (state.boundary - step).max(lo); // shrink W -> shed to E
    } else if imbalance < -threshold && state.boundary < hi {
        state.boundary = (state.boundary + step).min(hi); // shrink E -> shed to W
    } else {
        return;
    }
    state.boundaries[0] = state.boundary; // keep the 1-element strip list in lockstep with the slid W|E line
    wal_partition_config(state); // R0.2: persist the new boundary BEFORE re-routing entities across it
                                 // Hardening #1: ENQUEUE the crossers as a budgeted job (re-routed incrementally) instead of handing off
                                 // ALL of them in this one locked pass -- the same O(N) freeze maybe_split had (Step 0: 2.3s @ 20k; the
                                 // freeze MOVED here once a split set splits[region]). The drain re-routes each via movement_region_after.
    let bounds = &state.boundaries;
    let splits = &state.splits;
    let crossers: Vec<String> = state
        .entities
        .iter()
        .filter(|(_, e)| {
            is_strip_region_name(&e.region, bounds)
                && movement_region_after(e.pos[0], &e.region, bounds, splits) != e.region
        })
        .map(|(k, _)| k.clone())
        .collect();
    if !crossers.is_empty() {
        state.rebalance_jobs.push(RebalanceJob {
            eids: crossers,
            cursor: 0,
        });
    }
}

// R0.2: persist the partition topology (boundary / splits / mesh_regions) so a restart restores the SAME
// routing function -- else boundary resets to 0.0 and the router disagrees with recovered entity placement.
fn wal_partition_config(state: &mut ServerState) {
    state.zone_topology_rev += 1;
    let version = state.zone_topology_rev;
    let boundary = state.boundary;
    let boundaries = state.boundaries.clone();
    let splits: Map<String, Value> = state
        .splits
        .iter()
        .map(|(r, v)| (r.clone(), json!(v)))
        .collect();
    let mesh: Vec<String> = state.mesh_regions.iter().cloned().collect();
    let _ = state.wal_append(&json!({
        "kind": "partition_config", "version": version, "boundary": boundary,
        "boundaries": boundaries,
        "splits": Value::Object(splits), "mesh_regions": mesh
    }));
}

// load-SPLIT: a coarse region over SPLIT_HI with a free standby splits at its entities' median x into
// a NEW sub-band + worker -- ADDS capacity for a crowd-in-one-spot (rebalance() only shifts the W|E
// line between two FIXED workers; this gives the hot region a SECOND worker). First version splits each
// coarse region at most once (subs empty); recursive/multi-level split is a later refinement.
fn maybe_split(state: &mut ServerState) {
    if state.grid2d.is_some() {
        return; // D1: 2D-grid mode does not use the 1D median-x split (capacity moves via block reassignment)
    }
    const SPLIT_HI: f64 = 0.85;
    if state.standbys.is_empty() || !state.rebalance_jobs.is_empty() {
        return; // one budgeted migration at a time -> don't pile a split on an in-flight rebalance/split
    }
    let hot = state
        .region_worker
        .iter()
        .filter(|(r, _)| {
            !r.contains('#')
                && state
                    .splits
                    .get(r.as_str())
                    .map(|s| s.is_empty())
                    .unwrap_or(true)
        })
        .filter_map(|(r, w)| state.worker_load.get(w).map(|&l| (r.clone(), l)))
        .filter(|(_, l)| l.is_finite() && *l > SPLIT_HI)
        .max_by(|a, b| a.1.total_cmp(&b.1));
    let region = match hot {
        Some((r, _)) => r,
        None => return,
    };
    let mut xs: Vec<f64> = state
        .entities
        .values()
        .filter(|e| e.region == region)
        .map(|e| e.pos[0])
        .filter(|x| x.is_finite())
        .collect();
    if xs.len() < 2 {
        return;
    }
    xs.sort_by(|a, b| a.total_cmp(b));
    let median = xs[xs.len() / 2];
    state.splits.entry(region.clone()).or_default().push(median);
    wal_partition_config(state); // R0.2: persist the new split BEFORE the new sub-region routing is used
    let new_region = format!("{region}#1");
    let new_wid = state.standbys.remove(0);
    state
        .region_worker
        .insert(new_region.clone(), new_wid.clone());
    state.region_expires.insert(
        new_region.clone(),
        Instant::now() + Duration::from_secs_f64(state.lease_ttl),
    );
    if let Some(w) = state.workers.get_mut(&new_wid) {
        w.region = new_region.clone();
    }
    eprintln!("[rust-broker] SPLIT region {region} at x={median} -> {new_region} ({new_wid})");
    let movers: Vec<String> = state
        .entities
        .iter()
        .filter(|(_, e)| e.region == region && e.pos[0] >= median)
        .map(|(k, _)| k.clone())
        .collect();
    // Hardening #1: ENQUEUE the movers as a budgeted-incremental job instead of handing off ALL of them in
    // this one locked pass (Step 0 measured 2.3s @ 20k). process_rebalance_jobs drains it a small batch per
    // monitor tick, re-routing each via movement_region_after (the split boundary is persisted above).
    let _ = new_region; // the drain recomputes the target region per entity from the persisted split
    state.rebalance_jobs.push(RebalanceJob {
        eids: movers,
        cursor: 0,
    });
}

// Hardening #1: RE-ROUTE one candidate -> compute its correct region NOW (movement_region_after with the
// current pos/boundary/splits) and hand it off if it changed. Idempotent: if the move-path already migrated
// it (nr == region) or it was deleted (None), this is a no-op. The single migration primitive BOTH
// maybe_split and rebalance feed, so neither does an unbudgeted O(N) handoff loop.
fn re_route_one(state: &mut ServerState, eid: &str) {
    let (pos, region) = match state.entities.get(eid) {
        Some(e) => (e.pos, e.region.clone()),
        None => return,
    };
    // D1: 2D-grid mode -- re-route by 2D cell (a "Z3_2" name is NOT a strip-name, so the 1D guard below
    // would skip it; dispatch BEFORE that guard).
    if let Some((c, r, cw, ch)) = state.grid2d {
        let nr = region_2d_after(pos, &region, c, r, cw, ch);
        if nr != region {
            handoff(state, eid, &region, &nr);
        }
        return;
    }
    let x = pos[0];
    if !is_strip_region_name(&region, &state.boundaries) {
        return;
    }
    let nr = movement_region_after(x, &region, &state.boundaries, &state.splits);
    if nr != region {
        handoff(state, eid, &region, &nr);
    }
}

// Hardening #1: drain the active RebalanceJob a SMALL batch per monitor tick under a time budget, so the
// O(N) migration (the 2.3s freeze in one pass) spreads over many ticks and the lock-hold stays tiny.
fn process_rebalance_jobs(state: &mut ServerState) {
    const BATCH: usize = 8; // candidates per inner batch (fully applied, THEN the budget is re-checked)
    const BUDGET_MS: u128 = 2; // never hold the global lock longer than this for rebalance work
    let start = Instant::now();
    loop {
        if start.elapsed().as_millis() >= BUDGET_MS {
            break;
        }
        // pull the next batch + advance the cursor by EXACTLY that many (it never runs ahead of the work),
        // then release the job borrow so re_route_one can take &mut state.
        let (batch, drained) = {
            let job = match state.rebalance_jobs.first_mut() {
                Some(j) => j,
                None => break,
            };
            let end = (job.cursor + BATCH).min(job.eids.len());
            let batch: Vec<String> = job.eids[job.cursor..end].to_vec();
            job.cursor = end;
            (batch, job.cursor >= job.eids.len())
        };
        if batch.is_empty() {
            if drained {
                state.rebalance_jobs.remove(0);
            }
            break;
        }
        for eid in &batch {
            re_route_one(state, eid);
        }
        if drained {
            state.rebalance_jobs.remove(0);
            break;
        }
    }
}

// L3 graceful-degradation governor: derive a global load_level (0 normal / 1 stressed / 2 overloaded) from
// the WORST per-worker egress backlog. The degradation actions key off this -- emit() drops degradable ops
// at a cap that LOWERS with load, and flush_events coalesces visual events HARDER -- bounding memory under a
// storm while CRITICAL ops always pass; it recovers to 0 when the backlog clears (no manual flag = organic).
// Hysteresis: rise immediately, but hold in the mid-band so the level doesn't flap.
fn update_load_governor(state: &mut ServerState) {
    let max_oq = state
        .workers
        .values()
        .map(|w| w.out_queue.load(Ordering::Relaxed))
        .max()
        .unwrap_or(0);
    let new_level = if max_oq >= EGRESS_SOFT_CAP {
        2
    } else if max_oq >= EGRESS_SOFT_CAP / 2 {
        1
    } else if max_oq < EGRESS_SOFT_CAP / 4 {
        0
    } else {
        state.load_level.min(1) // 1/4..1/2 band: hold (don't flap), never above 1
    };
    if new_level != state.load_level {
        eprintln!(
            "[L3] load_level {} -> {new_level} (max out_queue {max_oq})",
            state.load_level
        );
        state.load_level = new_level;
    }
}

// D3: 2D-grid LOAD rebalance -- when one worker's blocks hold far more entities than another's, move ONE
// block from the hottest worker to the coldest by reassigning region_worker + re-authoritying the block's
// entities (the check_leases failover handover, load-triggered between two LIVE workers). Hysteretic (only
// on a real imbalance, so it doesn't churn); 2D-grid mode ONLY (the 1D path uses rebalance()/maybe_split()).
fn rebalance_2d(state: &mut ServerState) {
    if state.grid2d.is_none() || !state.rebalance_jobs.is_empty() {
        return;
    }
    // per-worker entity load (over the regions each worker owns)
    let mut load: HashMap<String, usize> = HashMap::new();
    for wid in state.region_worker.values() {
        load.entry(wid.clone()).or_insert(0);
    }
    for e in state.entities.values() {
        if let Some(wid) = state.region_worker.get(&e.region) {
            if let Some(n) = load.get_mut(wid) {
                *n += 1;
            }
        }
    }
    if load.len() < 2 {
        return;
    }
    let (hot, hot_n) = load
        .iter()
        .max_by_key(|(_, n)| **n)
        .map(|(w, n)| (w.clone(), *n))
        .unwrap();
    let (cold, cold_n) = load
        .iter()
        .min_by_key(|(_, n)| **n)
        .map(|(w, n)| (w.clone(), *n))
        .unwrap();
    if hot == cold || (hot_n as f64) < (cold_n as f64) * 1.5 + 4.0 {
        return; // hysteresis: only shed on a real imbalance
    }
    let hot_blocks: Vec<String> = state
        .region_worker
        .iter()
        .filter(|(_, w)| **w == hot)
        .map(|(r, _)| r.clone())
        .collect();
    if hot_blocks.len() < 2 {
        return; // leave the hot worker at least one block
    }
    // move the hot block whose load is CLOSEST to half the gap (converges without overshoot/ping-pong)
    let target = ((hot_n - cold_n) / 2) as i64;
    let mut bl: HashMap<String, i64> = HashMap::new();
    for b in &hot_blocks {
        bl.insert(b.clone(), 0);
    }
    for e in state.entities.values() {
        if let Some(n) = bl.get_mut(&e.region) {
            *n += 1;
        }
    }
    let block = bl
        .iter()
        .min_by_key(|(_, n)| (**n - target).abs())
        .map(|(b, _)| b.clone())
        .unwrap();
    // Stage the WHOLE block as one durable transition. A block is a partition unit, so moving its
    // entities one-by-one creates a recoverable-but-wrong split block after a crash.
    if queue_block_migration(state, &block, &hot, &cold) {
        eprintln!("[rust-broker] REBALANCE-2D queued block {block} {hot}->{cold} (hot_load={hot_n} cold_load={cold_n})");
    } else {
        state.rejected.push(json!({
            "block":block,
            "old_owner":hot,
            "new_owner":cold,
            "reason":"wal_persist_failed: block migration not durably staged"
        }));
    }
}

fn check_leases(state: &mut ServerState) {
    let now = Instant::now();
    let regions: Vec<String> = state.region_worker.keys().cloned().collect();
    for region in regions {
        let expired = match state.region_expires.get(&region) {
            Some(exp) => now >= *exp,
            None => false,
        };
        if !expired {
            continue;
        }
        if state.pending_failovers.iter().any(|f| f.region == region) {
            continue;
        }
        if state.standbys.is_empty() {
            if !state.orphaned_regions.contains(&region) {
                state.orphaned_regions.push(region.clone());
                eprintln!("[rust-broker] region {region} ORPHANED (lease lapsed, no standby)");
            }
            continue;
        }
        let old_wid = state.region_worker.get(&region).cloned();
        let new_wid = state.standbys.remove(0);
        let until = now + Duration::from_secs_f64(state.lease_ttl);
        if queue_failover_grant(state, &region, old_wid, &new_wid, until) {
            eprintln!(
                "[rust-broker] FAILOVER queued region {region} -> {new_wid} (grant-only, pending durable watermark)"
            );
        } else {
            state.standbys.insert(0, new_wid);
        }
    }
}

// R0.1: persistent ops mutate durable state (must be WAL'd before publish); transient ops do not.
fn is_persistent_op(op: &str) -> bool {
    matches!(
        op,
        "CreateEntity"
            | "DeleteEntity"
            | "AddComponent"
            | "RemoveComponent"
            | "UpdateComponent"
            | "BatchUpdate"
            | "SetComponentAuthority"
            | "MeshHandoff"
            | "MeshAck"
            | "ThresholdTx"
            | "Fold"
            | "SnapshotMarker"
            | "ReserveEntityIds"
    )
}

#[derive(Clone)]
struct PreparedUpdate {
    gen: u64,
    eid: String,
    comp: String,
    value: Value,
    version: u64,
    writer: String,
}

#[derive(Clone)]
struct PreparedHandoff {
    gen: u64,
    eid: String,
    from: String,
    to: String,
    pos: [f64; 2],
    vel: [f64; 2],
    version: u64,
    authority_epoch: u64,
    authority: Value,
    moved_comps: Vec<String>,
    old_wid: Option<String>,
    new_wid: Option<String>,
    lease_epoch: Option<u64>,
    reason: String,
}

#[derive(Clone)]
struct PendingRemoteHandoff {
    eid: String,
    target: String,
}

#[derive(Clone)]
struct PreparedFailoverGrant {
    eid: String,
    version: u64,
    authority_epoch: u64,
    authority: Value,
    moved_comps: Vec<String>,
}

#[derive(Clone)]
struct PreparedFailover {
    gen: u64,
    region: String,
    old_wid: Option<String>,
    new_wid: String,
    until: Instant,
    grants: Vec<PreparedFailoverGrant>,
}

#[derive(Clone)]
struct PreparedBlockMigration {
    gen: u64,
    block: String,
    old_wid: String,
    new_wid: String,
    grants: Vec<PreparedFailoverGrant>,
}

fn prepared_update_wal_event(u: &PreparedUpdate) -> Value {
    json!({
        "kind":"write","entity":u.eid,"version":u.version,
        "writer":u.writer,"comp":u.comp,"value":u.value,
        "gen":u.gen
    })
}

fn prepared_handoff_wal_event(h: &PreparedHandoff) -> Value {
    json!({
        "kind":"transfer","entity":h.eid,"version":h.version,
        "authority_epoch":h.authority_epoch,
        "authority":h.authority,
        "authority_snapshot":h.authority,
        "components":h.moved_comps,
        "from":h.from,"to":h.to,
        "source":h.from,"target":h.to,
        "x":h.pos[0],
        "pos":[h.pos[0],h.pos[1]],"vel":[h.vel[0],h.vel[1]],
        "lease_epoch":h.lease_epoch,
        "reason":h.reason,
        "gen":h.gen
    })
}

fn prepared_failover_wal_event(f: &PreparedFailover) -> Value {
    let grants: Vec<Value> = f
        .grants
        .iter()
        .map(|g| {
            json!({
                "entity":g.eid,
                "version":g.version,
                "authority_epoch":g.authority_epoch,
                "authority":g.authority,
                "components":g.moved_comps,
            })
        })
        .collect();
    json!({
        "kind":"failover_grant",
        "region":f.region,
        "old_owner":f.old_wid.clone(),
        "new_owner":f.new_wid.clone(),
        "grants":grants,
        "gen":f.gen,
        "reason":"lease expired"
    })
}

fn prepared_block_migration_wal_event(m: &PreparedBlockMigration) -> Value {
    let grants: Vec<Value> = m
        .grants
        .iter()
        .map(|g| {
            json!({
                "entity":g.eid,
                "version":g.version,
                "authority_epoch":g.authority_epoch,
                "authority":g.authority,
                "components":g.moved_comps,
            })
        })
        .collect();
    json!({
        "kind":"block_migration",
        "block":m.block,
        "old_owner":m.old_wid,
        "new_owner":m.new_wid,
        "grants":grants,
        "gen":m.gen,
        "reason":"rebalance_2d"
    })
}

fn apply_prepared_update(state: &mut ServerState, u: &PreparedUpdate) {
    let Some(e) = state.entities.get_mut(&u.eid) else {
        return;
    };
    ensure_component_authority(e, &u.comp);
    if u.comp == "pos" {
        e.pos = arr2(Some(&u.value));
    } else if u.comp == "vel" {
        e.vel = arr2(Some(&u.value));
    } else {
        e.components.insert(u.comp.clone(), u.value.clone());
    }
    e.version = u.version;
    state.metrics.applies += 1;
}

// THE component-write CORE (validate + prepare durable event, NO canonical RAM mutation).
// This is the first DurableTransition spine:
// validate -> WAL append/sync -> apply RAM -> publish. If WAL fails, no later Interest/query/reconnect can
// observe a value recovery cannot reproduce.
fn prepare_update(
    state: &mut ServerState,
    wid: &str,
    eid: &str,
    comp: &str,
    raw_value: Value,
    supplied_epoch: Option<u64>,
) -> Option<PreparedUpdate> {
    if !state.entities.contains_key(eid) {
        // SEAM-INTEREST read-only enforcement: a ghost is a non-authoritative mirror of a NEIGHBOUR broker's
        // entity. It is NOT in `entities`, so it can never be leased or granted authority -- and a WRITE to it
        // (a forged authority-grab: "I'll just UpdateComponent the cross-seam enemy's pos/hp") is rejected
        // HERE, structurally (the eid simply isn't an owned entity). We emit an explicit UpdateRejected naming
        // the real owner so the attempt is VISIBLE (and the truth-gate can assert FAIL-without / PASS-with).
        // This is the structural guarantee in code: reading a ghost grants nothing; authority stays with the owner zone.
        if let Some(g) = state.ghosts.get(eid) {
            let owner = g.owner_region.clone();
            emit(
                state,
                wid,
                json!({"op":"UpdateRejected","entity":eid,"comp":comp,
                "reason":format!("ghost is read-only (non-authoritative; owned by zone '{}'); cannot claim authority over a cross-seam mirror", owner),
                "ghost":true,"owner_region":owner}),
            );
        } else {
            emit(
                state,
                wid,
                json!({"op":"UpdateRejected","entity":eid,"comp":comp,
                "reason":"entity not found"}),
            );
        }
        return None;
    }
    // 5a: authority is a COMPONENT property. Region ownership remains the fallback for legacy
    // server-auth components, but a component lease/ACL can move a single component to a physics
    // worker, logic worker, or sparse client without moving the whole entity.
    if let Err(reason) = authoritative_component_writer(state, wid, eid, comp) {
        state
            .rejected
            .push(json!({"entity":eid,"writer":wid,"owner":reason,"comp":comp}));
        if state.rejected.len() > 512 {
            // SOAK FIX: the rejected log grew unbounded (498K entries = ~550MB over a 90s soak); the
            // Inspector only ever reads the last 20 (recent_rejected). Keep ~256 most-recent.
            let drop_n = state.rejected.len() - 256;
            state.rejected.drain(0..drop_n);
        }
        emit(
            state,
            wid,
            json!({"op":"UpdateRejected","entity":eid,"comp":comp,"reason":reason}),
        );
        return None;
    }
    // WRITE ACL: even an authority holder needs the declared attribute to write this component.
    let need = state
        .entities
        .get(eid)
        .and_then(|e| acl_write_attr(e, comp));
    if let Some(attr) = need {
        let has = state
            .workers
            .get(wid)
            .map(|w| w.attributes.contains(&attr))
            .unwrap_or(false);
        if !has {
            emit(
                state,
                wid,
                json!({"op":"UpdateRejected","entity":eid,"comp":comp,
                "reason":format!("acl: write requires attribute '{}'", attr)}),
            );
            return None;
        }
    }
    // A1 thin-fill: the "acl" component governs read/write authority -- rewriting it is a
    // privilege-escalation. Gate it behind a dedicated 'acl_admin' attribute (the ACL is set at
    // CreateEntity by the trusted creator; normal gamemode workers cannot mutate it after).
    if comp == "acl"
        && !state
            .workers
            .get(wid)
            .map(|w| w.attributes.iter().any(|a| a == "acl_admin"))
            .unwrap_or(false)
    {
        emit(
            state,
            wid,
            json!({"op":"UpdateRejected","entity":eid,"comp":comp,
            "reason":"acl is immutable post-creation (requires the 'acl_admin' attribute)"}),
        );
        return None;
    }
    if reject_kernel_reserved_write(state, wid, eid, comp) {
        return None;
    }
    if reject_stale_authority_epoch_val(state, wid, eid, comp, supplied_epoch) {
        return None;
    }
    let value = {
        let e = state.entities.get(eid).unwrap();
        normalize_physics_clock_write(e, comp, raw_value)
    };
    if reject_client_forward_sparse_envelope(state, wid, eid, comp, &value) {
        return None;
    }
    if reject_and_escalate_contact_risk(state, wid, eid, comp, &value) {
        return None;
    }
    let pending_entity_writes = state
        .pending_updates
        .iter()
        .filter(|u| u.eid == eid)
        .count() as u64;
    let version = state
        .entities
        .get(eid)
        .map(|e| {
            e.version
                .saturating_add(pending_entity_writes)
                .saturating_add(1)
        })
        .unwrap_or(1);
    let gen = state.pending_gen.saturating_add(1);
    let prepared = PreparedUpdate {
        gen,
        eid: eid.to_string(),
        comp: comp.to_string(),
        value,
        version,
        writer: wid.to_string(),
    };
    // #2 WAL-THEN-PUBLISH: persist the line durably (write now, fsync by the caller) BEFORE broadcasting. If
    // the WAL WRITE fails (disk full / fail-closed) the write did NOT persist -> reject it + do NOT publish.
    // (The fsync barrier is the caller's wal_sync; on a single write that's apply_one_update, on a batch it's
    // one wal_sync for the whole group -- either way every published value is on disk + fsync'd first.)
    if state
        .wal_append_nosync(&prepared_update_wal_event(&prepared))
        .is_err()
    {
        emit(
            state,
            wid,
            json!({"op":"UpdateRejected","entity":eid,"comp":comp,
            "reason":"wal_persist_failed: write not durably persisted (disk full / WAL degraded) — not published"}),
        );
        return None;
    }
    state.pending_gen = gen;
    Some(prepared)
}

// pos-zoning (seam handoff / loss-imminent / intent-clear) for a just-applied pos write, THEN propagate the
// value to interested workers. Runs AFTER the durable fsync (publish-after-durable). Factored out so the
// single + batch paths share the identical post-commit publish step.
fn zone_and_propagate(state: &mut ServerState, eid: &str, comp: &str, value: &Value) {
    if comp == "pos" {
        // a CHILD (has a `parent` component) follows its root's ASSEMBLY handoff and never
        // self-zones -- else a ship's part could cross the seam a tick before/after the root
        // and tear the assembly. Only roots / standalone entities zone on their own pos.
        let is_child = state
            .entities
            .get(eid)
            .map(|e| e.components.contains_key("parent"))
            .unwrap_or(false);
        if !is_child {
            let (pos, cur) = {
                let Some(e) = state.entities.get(eid) else {
                    return;
                };
                (e.pos, e.region.clone())
            };
            if let Some((c, r, cw, ch)) = state.grid2d {
                // D1: 2D-grid mode -- handoff by 2D cell (the 1D seam-intent overlap band is x-only and
                // does not apply; a 2D loss-imminent band is a later refinement).
                let nr = region_2d_after(pos, &cur, c, r, cw, ch);
                if nr != cur {
                    state.pending_handoff_intent.remove(eid);
                    handoff(state, eid, &cur, &nr);
                }
            } else {
                let x = pos[0];
                let nr = movement_region_after(x, &cur, &state.boundaries, &state.splits);
                if nr != cur {
                    state.pending_handoff_intent.remove(eid); // commit consumes the intent
                    handoff(state, eid, &cur, &nr);
                } else if let Some(tgt) = seam_intent_target(x, &cur, &state.boundaries) {
                    maybe_emit_loss_imminent(state, eid, &cur, &tgt); // C2: in the overlap band
                } else {
                    state.pending_handoff_intent.remove(eid); // left the band -> clear
                }
            }
        }
    }
    propagate(state, eid, comp, value);
}

fn queue_prepared_update(state: &mut ServerState, u: PreparedUpdate) {
    state.pending_updates.push(u);
}

fn flush_pending_handoffs(state: &mut ServerState) {
    if state.pending_handoffs.is_empty() {
        return;
    }
    let pending = std::mem::take(&mut state.pending_handoffs);
    let max_gen = pending
        .iter()
        .map(|h| h.gen)
        .max()
        .unwrap_or(state.durable_gen);
    if state.wal_sync().is_err() {
        for h in &pending {
            state.rejected.push(json!({
                "entity":h.eid,
                "from":h.from,
                "to":h.to,
                "reason":"wal_sync_failed: staged handoff did not cross durable watermark; authority not transferred"
            }));
        }
        state.pending_handoffs = pending;
        return;
    }
    state.durable_gen = state.durable_gen.max(max_gen);
    for h in &pending {
        apply_prepared_handoff(state, h);
    }
}

fn queue_failover_grant(
    state: &mut ServerState,
    region: &str,
    old_wid: Option<String>,
    new_wid: &str,
    until: Instant,
) -> bool {
    if state.pending_failovers.iter().any(|f| f.region == region) {
        return true;
    }
    let eids: Vec<String> = state
        .entities
        .iter()
        .filter(|(_, e)| e.region == region)
        .map(|(k, _)| k.clone())
        .collect();
    let mut grants = Vec::new();
    for eid in eids {
        let Some(e) = state.entities.get(&eid) else {
            continue;
        };
        let mut next = e.clone();
        let (authority_epoch, moved_comps) =
            advance_physics_island_authority(&mut next, old_wid.as_deref(), Some(new_wid));
        if moved_comps.is_empty() {
            continue;
        }
        next.version = next.version.saturating_add(1);
        grants.push(PreparedFailoverGrant {
            eid,
            version: next.version,
            authority_epoch,
            authority: authority_to_json(&next.authority),
            moved_comps,
        });
    }
    let gen = state.pending_gen.saturating_add(1);
    let prepared = PreparedFailover {
        gen,
        region: region.to_string(),
        old_wid,
        new_wid: new_wid.to_string(),
        until,
        grants,
    };
    if state
        .wal_append_nosync(&prepared_failover_wal_event(&prepared))
        .is_err()
    {
        return false;
    }
    state.pending_gen = gen;
    state.pending_failovers.push(prepared);
    true
}

fn apply_prepared_failover(state: &mut ServerState, f: &PreparedFailover) {
    state
        .region_worker
        .insert(f.region.clone(), f.new_wid.clone());
    state.region_expires.insert(f.region.clone(), f.until);
    state.metrics.failovers += 1;
    if let Some(w) = state.workers.get_mut(&f.new_wid) {
        w.region = f.region.clone();
    }
    for grant in &f.grants {
        if let Some(e) = state.entities.get_mut(&grant.eid) {
            e.version = grant.version;
            apply_authority_snapshot(e, &grant.authority);
        } else {
            continue;
        }
        if state.workers.contains_key(&f.new_wid) {
            if !state.workers[&f.new_wid].view.contains(&grant.eid) {
                send_full(state, &f.new_wid, &grant.eid);
            }
            for comp in &grant.moved_comps {
                grant_authority(state, &f.new_wid, &grant.eid, comp);
            }
        }
    }
}

fn flush_pending_failovers(state: &mut ServerState) {
    if state.pending_failovers.is_empty() {
        return;
    }
    let pending = std::mem::take(&mut state.pending_failovers);
    let max_gen = pending
        .iter()
        .map(|f| f.gen)
        .max()
        .unwrap_or(state.durable_gen);
    if state.wal_sync().is_err() {
        for f in &pending {
            state.rejected.push(json!({
                "region":f.region,
                "new_owner":f.new_wid,
                "reason":"wal_sync_failed: staged failover grant did not cross durable watermark"
            }));
        }
        return;
    }
    state.durable_gen = state.durable_gen.max(max_gen);
    for f in &pending {
        apply_prepared_failover(state, f);
    }
}

fn queue_block_migration(
    state: &mut ServerState,
    block: &str,
    old_wid: &str,
    new_wid: &str,
) -> bool {
    if state
        .pending_block_migrations
        .iter()
        .any(|m| m.block == block)
    {
        return true;
    }
    let eids: Vec<String> = state
        .entities
        .iter()
        .filter(|(_, e)| e.region == block)
        .map(|(k, _)| k.clone())
        .collect();
    let mut grants = Vec::new();
    for eid in eids {
        let Some(e) = state.entities.get(&eid) else {
            continue;
        };
        let mut next = e.clone();
        let (authority_epoch, moved_comps) =
            advance_physics_island_authority(&mut next, Some(old_wid), Some(new_wid));
        if moved_comps.is_empty() {
            continue;
        }
        next.version = next.version.saturating_add(1);
        grants.push(PreparedFailoverGrant {
            eid,
            version: next.version,
            authority_epoch,
            authority: authority_to_json(&next.authority),
            moved_comps,
        });
    }
    let gen = state.pending_gen.saturating_add(1);
    let prepared = PreparedBlockMigration {
        gen,
        block: block.to_string(),
        old_wid: old_wid.to_string(),
        new_wid: new_wid.to_string(),
        grants,
    };
    if state
        .wal_append_nosync(&prepared_block_migration_wal_event(&prepared))
        .is_err()
    {
        return false;
    }
    state.pending_gen = gen;
    state.pending_block_migrations.push(prepared);
    true
}

fn apply_prepared_block_migration(state: &mut ServerState, m: &PreparedBlockMigration) {
    state
        .region_worker
        .insert(m.block.clone(), m.new_wid.clone());
    if let Some(w) = state.workers.get_mut(&m.new_wid) {
        w.region = m.block.clone();
    }
    for grant in &m.grants {
        if let Some(e) = state.entities.get_mut(&grant.eid) {
            e.version = grant.version;
            apply_authority_snapshot(e, &grant.authority);
        } else {
            continue;
        }
        for comp in &grant.moved_comps {
            revoke_authority(state, &m.old_wid, &grant.eid, comp);
        }
        if state.workers.contains_key(&m.new_wid) {
            if !state.workers[&m.new_wid].view.contains(&grant.eid) {
                send_full(state, &m.new_wid, &grant.eid);
            }
            for comp in &grant.moved_comps {
                grant_authority(state, &m.new_wid, &grant.eid, comp);
            }
        }
    }
}

fn flush_pending_block_migrations(state: &mut ServerState) {
    if state.pending_block_migrations.is_empty() {
        return;
    }
    let pending = std::mem::take(&mut state.pending_block_migrations);
    let max_gen = pending
        .iter()
        .map(|m| m.gen)
        .max()
        .unwrap_or(state.durable_gen);
    if state.wal_sync().is_err() {
        for m in &pending {
            state.rejected.push(json!({
                "block":m.block,
                "old_owner":m.old_wid,
                "new_owner":m.new_wid,
                "reason":"wal_sync_failed: staged block migration did not cross durable watermark"
            }));
        }
        return;
    }
    state.durable_gen = state.durable_gen.max(max_gen);
    for m in &pending {
        apply_prepared_block_migration(state, m);
    }
}

// DurableTransition group barrier. Component writes are WAL-appended nosync and staged, then this
// fsyncs the whole group and advances durable_gen. Only after that do we mutate canonical RAM and
// publish, so every existing read/projection path is naturally clamped to <= durable_gen.
fn flush_pending_updates(state: &mut ServerState) {
    if state.pending_updates.is_empty() {
        flush_pending_handoffs(state);
        flush_pending_failovers(state);
        flush_pending_block_migrations(state);
        flush_pending_remote_handoffs(state);
        return;
    }
    let pending = std::mem::take(&mut state.pending_updates);
    let max_gen = pending
        .iter()
        .map(|u| u.gen)
        .max()
        .unwrap_or(state.durable_gen);
    if state.wal_sync().is_err() {
        for u in &pending {
            emit(
                state,
                &u.writer,
                json!({"op":"UpdateRejected","entity":u.eid,"comp":u.comp,
                "reason":"wal_sync_failed: staged write did not cross durable watermark; not published"}),
            );
        }
        return;
    }
    state.durable_gen = state.durable_gen.max(max_gen);
    for u in &pending {
        apply_prepared_update(state, u);
        renew(state, &u.writer);
        zone_and_propagate(state, &u.eid, &u.comp, &u.value);
    }
    flush_pending_handoffs(state);
    flush_pending_failovers(state);
    flush_pending_block_migrations(state);
    flush_pending_remote_handoffs(state);
}

fn record_mesh_ack(state: &mut ServerState, eid: &str) -> bool {
    if state
        .wal_append(&json!({"kind":"mesh_acked","entity":eid}))
        .is_ok()
    {
        state.pending_mesh.remove(eid);
        true
    } else {
        state.rejected.push(json!({
            "entity":eid,
            "reason":"wal_persist_failed: mesh ack not durably recorded; keeping pending handoff for resend"
        }));
        false
    }
}

// Single-write path (UpdateComponent): validate+WAL-write(no fsync) -> queue. A durability tick performs
// the group fsync and publishes. Returns true if accepted into the pending durable generation.
fn apply_one_update(
    state: &mut ServerState,
    wid: &str,
    eid: &str,
    comp: &str,
    raw_value: Value,
    supplied_epoch: Option<u64>,
) -> bool {
    let prepared = match prepare_update(state, wid, eid, comp, raw_value, supplied_epoch) {
        Some(u) => u,
        None => return false,
    };
    queue_prepared_update(state, prepared);
    true
}

fn dispatch_frame(state: &mut ServerState, wid: &str, f: &Value, byte_len: usize) {
    if reject_ingress_rate_limit(state, wid, f, byte_len) {
        record_replay_tape_ingress(
            state,
            wid,
            f,
            byte_len,
            "rejected",
            Some("rate_limit_error"),
        );
        reap_disconnecting(state);
        return;
    }
    if reject_role_policy(state, wid, f) {
        record_replay_tape_ingress(
            state,
            wid,
            f,
            byte_len,
            "rejected",
            Some("role_policy_error"),
        );
        reap_disconnecting(state);
        return;
    }
    dispatch_inner(state, wid, f);
    record_replay_tape_ingress(state, wid, f, byte_len, "dispatched", None);
    // T1: a single frame's fan-out (e.g. a critical EntityEvent to N observers) may have driven a stuck
    // consumer past the hard egress cap; reap it NOW (within this frame) so its bounded channel can hold at
    // most CHANNEL_CAP frames before the socket is torn down -- the structural RAM bound. Runs on EVERY
    // dispatch path (the inner fn has many early returns) by wrapping, so no overflow path can skip the reap.
    reap_disconnecting(state);
}

#[cfg(test)]
fn dispatch_test_frame(state: &mut ServerState, wid: &str, f: &Value) {
    let byte_len = serde_json::to_vec(f).map(|body| body.len()).unwrap_or(0);
    dispatch_frame(state, wid, f, byte_len);
}

fn dispatch_inner(state: &mut ServerState, wid: &str, f: &Value) {
    let op = f.get("op").and_then(|v| v.as_str()).unwrap_or("");
    // R0.1 fail-closed: once the WAL is degraded (a durable write failed), reject every PERSISTENT op so
    // nothing is published-as-success that recovery cannot reproduce; transient ops (events/metrics/queries) pass.
    if state.wal_degraded && is_persistent_op(op) {
        let req_id = f.get("request_id").and_then(|v| v.as_str()).unwrap_or("");
        emit(
            state,
            wid,
            json!({"op":"UpdateRejected","request_id":req_id,
            "reason":"broker WAL-degraded (fail-closed): persistent op rejected"}),
        );
        return;
    }
    match op {
        "UpdateComponent" => {
            let eid = match f.get("entity").and_then(|v| v.as_str()) {
                Some(s) => s.to_string(),
                None => return,
            };
            let comp = f
                .get("comp")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let raw_value = f.get("value").cloned().unwrap_or(Value::Null);
            apply_one_update(state, wid, &eid, &comp, raw_value, frame_authority_epoch(f));
        }
        // WIRE-BATCH (the scale fix): ONE frame carrying MANY of a worker's per-tick position writes, so N
        // separate UpdateComponent frames collapse to 1 (framing + JSON-envelope overhead amortized N->1, and
        // the broker does 1 frame-read + 1 JSON parse + 1 dispatch-lock acquisition instead of N). The batch is
        // NOT a fast-path that bypasses anything: every entry runs through apply_one_update -- the SAME
        // per-entity authority / cheat-line / epoch-fence / ACL / WAL-then-publish / pos-zoning path as a single
        // UpdateComponent. A non-owner's entry is rejected per-entity (the rest of the batch still applies); a
        // stale epoch is fenced per-entry. Shape: {"op":"BatchUpdate","comp":"pos","updates":[["eid",[x,y]],...]}
        //   - `comp` is the shared component for the whole batch (homogeneous; pos is the dominant per-tick push).
        //   - each `updates` entry is a compact 2- or 3-array [entity, value] or [entity, value, authority_epoch]
        //     (the array form drops the per-entry {"entity":..,"comp":..,"value":..} key verbosity = the byte win).
        //   - an OPTIONAL top-level "values" object form is also accepted: {"comp":..,"values":{eid:val,...}}.
        // Back-compat: single UpdateComponent above is untouched; BatchUpdate is purely additive.
        "BatchUpdate" => {
            let comp = f
                .get("comp")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            if comp.is_empty() {
                return;
            }
            // collect (eid, value, optional per-entry epoch) without holding any borrow on `f` during apply
            let mut items: Vec<(String, Value, Option<u64>)> = Vec::new();
            if let Some(arr) = f.get("updates").and_then(|v| v.as_array()) {
                for u in arr {
                    if let Some(entry) = u.as_array() {
                        // ["eid", value] or ["eid", value, epoch]
                        let eid = match entry.first().and_then(|v| v.as_str()) {
                            Some(s) => s.to_string(),
                            None => continue,
                        };
                        let val = entry.get(1).cloned().unwrap_or(Value::Null);
                        let ep = entry.get(2).and_then(|v| v.as_u64());
                        items.push((eid, val, ep));
                    } else if let Some(obj) = u.as_object() {
                        // {"entity":..,"value":..[,"authority_epoch":..]} (verbose entry form, also accepted)
                        if let Some(eid) = obj.get("entity").and_then(|v| v.as_str()) {
                            let val = obj.get("value").cloned().unwrap_or(Value::Null);
                            let ep = obj.get("authority_epoch").and_then(|v| v.as_u64());
                            items.push((eid.to_string(), val, ep));
                        }
                    }
                }
            } else if let Some(map) = f.get("values").and_then(|v| v.as_object()) {
                let shared_ep = frame_authority_epoch(f);
                for (eid, val) in map {
                    items.push((eid.clone(), val.clone(), shared_ep));
                }
            }
            // WATERMARK-COMMIT: validate+prepare each entry + WAL-WRITE (no fsync); queue all accepted
            // entries into the broker-wide durable generation. The durability tick performs ONE wal_sync
            // for every pending single/batch write and only then publishes.
            // A rejected entry already emitted its UpdateRejected and is simply not in `prepared`.
            let mut accepted = 0usize;
            for (eid, val, ep) in items {
                if let Some(u) = prepare_update(state, wid, &eid, &comp, val, ep) {
                    queue_prepared_update(state, u);
                    accepted += 1;
                }
            }
            if accepted == 0 {
                // nothing applied (all rejected / unknown) -> no fsync, no publish
            }
        }
        "Fold" => {
            // PORTAL fold (folds = portals to other dimensions): teleport an entity to a FAR region at
            // pos P -- NOT an adjacent seam-cross. Reuses the proven authority-handoff; the trigger is a
            // portal-request from the owning worker (which already validated the portal-entry), not
            // region_after. So a fold = reposition-to-P + authority(cur -> target).
            let eid = match f.get("entity").and_then(|v| v.as_str()) {
                Some(s) => s.to_string(),
                None => return,
            };
            let target = f
                .get("region")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let cur = match state.entities.get(&eid) {
                Some(e) => e.region.clone(),
                None => return,
            };
            // CHEAT-LINE: only the entity's current owner-worker may fold it (no teleporting others).
            if state.region_worker.get(&cur).map(|s| s.as_str()) != Some(wid) {
                emit(
                    state,
                    wid,
                    json!({"op":"UpdateRejected","entity":eid,"comp":"fold",
                    "reason":"not authoritative; cannot fold an entity you do not own"}),
                );
                return;
            }
            if target.is_empty() || target == cur {
                return;
            }
            // The fold destination is part of the handoff transaction: WAL first, then pos+region+authority.
            let pos = f.get("pos").map(|p| arr2(Some(p)));
            if !handoff_with_position(state, &eid, &cur, &target, pos) {
                emit(
                    state,
                    wid,
                    json!({"op":"UpdateRejected","entity":eid,"comp":"fold",
                    "reason":"fold handoff failed before durable commit"}),
                );
                return;
            }
            flush_pending_handoffs(state);
            flush_pending_remote_handoffs(state);
            if let Some(np) = state.entities.get(&eid).map(|e| e.pos) {
                propagate(state, &eid, "pos", &json!(np));
            }
        }
        "Heartbeat" => {
            renew(state, wid); // liveness with no writes (idle owner stays alive)
        }
        "Interest" => {
            let center = f.get("center").and_then(|v| v.as_array()).map(|a| {
                [
                    a.first().and_then(|x| x.as_f64()).unwrap_or(0.0),
                    a.get(1).and_then(|x| x.as_f64()).unwrap_or(0.0),
                ]
            });
            // A4 (interest-clamp): cap the self-declared AOI radius. An unbounded radius (e.g. 1e9) is both
            // a fan-out cost (the broker would test/ship a world-sized interest-set) and part of the info-
            // scrape vector. MAX_AOI is generous vs any real view. NOTE: the OTHER half of the interest-
            // scrape -- the OBS region's GLOBAL interest -- would be gated by requiring an 'observer'
            // attribute; that's DEFERRED (it ripples to every trusted relay + dev tool, all of which connect
            // OBS, and the scrape needs a network-exposed broker that isn't live yet). See the BLUEPRINT.
            const MAX_AOI: f64 = 5_000.0;
            let radius = f
                .get("radius")
                .and_then(|v| v.as_f64())
                .map(|r| r.clamp(0.0, MAX_AOI));
            let full_radius = f
                .get("full_radius")
                .or_else(|| f.get("fullRadius"))
                .and_then(|v| v.as_f64())
                .map(|r| r.clamp(0.0, MAX_AOI));
            let coarse_rate = f
                .get("coarse_rate")
                .or_else(|| f.get("coarseRate"))
                .and_then(|v| v.as_u64())
                .unwrap_or(1)
                .clamp(1, 1_000);
            let coarse_grid = f
                .get("coarse_grid")
                .or_else(|| f.get("coarseGrid"))
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0)
                .clamp(0.0, 1_000_000.0);
            let global_obs_denied = center.is_none()
                && state
                    .workers
                    .get(wid)
                    .is_some_and(|w| w.region == "OBS" && !w.has_global_observer_claim());
            if let Some(w) = state.workers.get_mut(wid) {
                w.aoi_center = center;
                w.aoi_radius = radius;
                w.fidelity_full_radius = full_radius;
                w.fidelity_coarse_rate = coarse_rate;
                w.fidelity_coarse_grid = coarse_grid;
                w.fidelity_seq.clear();
            }
            update_interest_grid(state, wid); // Interest: re-index this worker's AOI into the spatial grid
            if global_obs_denied {
                emit(
                    state,
                    wid,
                    json!({"op":"UpdateRejected","comp":"interest",
                    "reason":"global OBS interest requires the observer/debug/inspector attribute"}),
                );
            }
            let eids: Vec<String> = state.entities.keys().cloned().collect();
            for eid in eids {
                let inside = visible(&state.workers[wid], &state.entities[&eid]);
                let has = state.workers[wid].view.contains(&eid);
                if inside && !has {
                    send_full(state, wid, &eid);
                } else if !inside && has {
                    emit(state, wid, json!({"op":"RemoveEntity","entity":eid}));
                    if let Some(w) = state.workers.get_mut(wid) {
                        w.view.remove(&eid);
                    }
                }
            }
        }
        "LogMessage" => {}
        // EVENT semantics (gap-C): a TRANSIENT entity event (tree-cut / hit / interact / door-open) that
        // rides the interest channel to every interested worker but is NEVER stored -- not a component,
        // no WAL, no checkout-replay (a late joiner must not re-see a one-shot event). Cheat-line: only
        // the entity's authoritative owner may fire its events. Mirrors WA's "events ride the update
        // channel" without turning events into state.
        "EntityEvent" => {
            let eid = f
                .get("entity")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            if let Err(reason) = authoritative_entity_owner(state, wid, &eid) {
                emit(
                    state,
                    wid,
                    json!({"op":"UpdateRejected","entity":eid,"comp":"event","reason":reason}),
                );
                return;
            }
            let event = f.get("event").cloned().unwrap_or(Value::Null);
            let payload = f.get("payload").cloned().unwrap_or(Value::Null);
            // Cross-seam event ordering: stamp the event with the entity's CURRENT
            // sim_time + gen so the client orders it in the SAME SimTimeInterpBuffer as the state stream --
            // the event renders at the moment it belongs to (e.g. TakeDamage AFTER the bullet reaches the
            // target by sim_time), not whenever the packet happens to arrive. Falls back to null (arrival
            // order) for an entity with no sim_time. The event stays TRANSIENT (not stored); only the stamp
            // rides, reusing the G1 logical clock + the G4-critical tier (EntityEvent is not degradable).
            let (ev_sim, ev_gen) = state
                .entities
                .get(&eid)
                .map(|e| {
                    (
                        e.components.get("sim_time").cloned().unwrap_or(Value::Null),
                        e.components.get("gen").cloned().unwrap_or(Value::Null),
                    )
                })
                .unwrap_or((Value::Null, Value::Null));
            // L1 event-storm: CLASSIFY (default "critical" -- a creator that doesn't classify is never
            // silently dropped/coalesced) + a coalesce_key (default eid:event so same-name visual events from
            // one entity collapse).
            let class = f
                .get("class")
                .and_then(|v| v.as_str())
                .unwrap_or("critical")
                .to_string();
            let coalesce_key = f
                .get("coalesce_key")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| format!("{}:{}", eid, event.as_str().unwrap_or("")));
            let target_wids: Vec<String> = state
                .workers
                .iter()
                .filter(|(_, w)| {
                    state
                        .entities
                        .get(&eid)
                        .map(|e| visible(w, e))
                        .unwrap_or(false)
                })
                .map(|(id, _)| id.clone())
                .collect();
            // CRITICAL events deliver INLINE (in THIS lock-hold), NOT via the buffered 20Hz flush tick.
            // WHY: flush_events runs on a separate 20Hz task that must re-acquire this same global lock, and
            // tokio's Mutex is not fair -- under a sustained ComponentUpdate flood (many connections each
            // re-locking per frame) the flush tick can be LOCK-STARVED for seconds, delaying buffered events
            // unboundedly. For VISUAL events that only costs latency on a coalesced stream (fine). But a
            // CRITICAL event is the "never dropped, exact, timely" class -- making its delivery depend on a
            // starvable tick was the defect a commercial fault-soak surfaced (a 600-critical storm under load
            // delivered only ~390 within 45s, the rest stuck buffered behind the starved flush). Delivering
            // critical events here, while we already hold the lock that received them, makes the L1 guarantee
            // load-INDEPENDENT (they ride the same per-frame lock as every other op) -- and it is also the
            // correct semantics: "critical" means immediate, only "visual" needs coalescing. Visual/debug
            // still BUFFER for the coalescing flush (storm-bounded egress). Critical is never coalesced anyway,
            // so emitting it now (vs after a <=50ms flush) changes nothing but the starvation exposure.
            if class == "critical" {
                let base = json!({"op":"EntityEvent","entity":eid,"event":event,"payload":payload,
                    "sim_time":ev_sim,"gen":ev_gen,"class":class});
                for wid2 in &target_wids {
                    emit(state, wid2, base.clone());
                }
            } else {
                state.event_outbox.push(BufferedEvent {
                    target_wids,
                    eid,
                    event,
                    payload,
                    class,
                    coalesce_key,
                    sim_time: ev_sim,
                    gen: ev_gen,
                });
            }
        }
        "CreateEntity" => {
            let eid = match f.get("entity").and_then(|v| v.as_str()) {
                Some(s) => s.to_string(),
                None => return,
            };
            // #3 GRACEFUL-DRAIN: a draining broker accepts NO new entities -- it is being decommissioned, so a new
            // spawn must land on a live broker, not on one about to shut down (which would either lose it or force
            // an immediate re-handoff). Reject cleanly with reason "draining" so the worker re-creates elsewhere.
            // The hand-off of EXISTING entities (mesh_forward) continues; only NEW arrivals are refused.
            if state.draining {
                if let Some(req) = f.get("request_id") {
                    emit(
                        state,
                        wid,
                        json!({"op":"CreateEntityResponse","request_id":req,
                        "entity":eid,"success":false,"reason":"draining"}),
                    );
                } else {
                    emit(
                        state,
                        wid,
                        json!({"op":"UpdateRejected","entity":eid,"comp":"create",
                        "reason":"draining"}),
                    );
                }
                return;
            }
            if state.deleted_entities.contains(&eid) {
                if let Some(req) = f.get("request_id") {
                    emit(
                        state,
                        wid,
                        json!({"op":"CreateEntityResponse","request_id":req,
                        "entity":eid,"success":false,"reason":"entity tombstoned"}),
                    );
                } else {
                    emit(
                        state,
                        wid,
                        json!({"op":"UpdateRejected","entity":eid,"comp":"create",
                        "reason":"entity tombstoned"}),
                    );
                }
                return;
            }
            let mut comps: Map<String, Value> = f
                .get("components")
                .and_then(|v| v.as_object())
                .cloned()
                .unwrap_or_default();
            let p: Vec<f64> = comps
                .get("pos")
                .and_then(|v| v.as_array())
                .map(|a| a.iter().filter_map(|x| x.as_f64()).collect())
                .unwrap_or_else(|| vec![0.0, 0.0, 0.0]);
            let pos2 = if p.len() >= 3 {
                [p[0], p[2]]
            } else if p.len() == 2 {
                [p[0], p[1]]
            } else if p.len() == 1 {
                [p[0], 0.0]
            } else {
                [0.0, 0.0]
            };
            // honor an initial "vel" component too (symmetric with pos): a body created already
            // moving keeps that velocity. e.vel feeds the handoff continuity audit + the mesh carry,
            // so a hardcoded [0,0] here silently zeroed the velocity of every freshly-spawned body.
            let pv: Vec<f64> = comps
                .get("vel")
                .and_then(|v| v.as_array())
                .map(|a| a.iter().filter_map(|x| x.as_f64()).collect())
                .unwrap_or_else(|| vec![0.0, 0.0]);
            let vel2 = if pv.len() >= 3 {
                [pv[0], pv[2]]
            } else if pv.len() == 2 {
                [pv[0], pv[1]]
            } else if pv.len() == 1 {
                [pv[0], 0.0]
            } else {
                [0.0, 0.0]
            };
            // pos/vel live authoritative TOP-LEVEL (e.pos / e.vel); drop the duplicates from the
            // component map, else the checkout re-sends the STALE spawn-time pos AFTER the live one
            // and overwrites every later UpdateComponent("pos") move in a viewer's view (entities
            // never appear to move). Top-level is the single source of truth for pos/vel.
            comps.remove("pos");
            comps.remove("vel");
            if comps.keys().any(|k| is_platform_reserved_component(k))
                && !worker_has_attr(state, wid, "kernel_admin")
            {
                if let Some(req) = f.get("request_id") {
                    emit(
                        state,
                        wid,
                        json!({"op":"CreateEntityResponse","request_id":req,
                        "entity":eid,"success":false,
                        "reason":"platform-reserved components require the 'kernel_admin' attribute"}),
                    );
                } else {
                    emit(
                        state,
                        wid,
                        json!({"op":"UpdateRejected","entity":eid,"comp":"kernel",
                        "reason":"platform-reserved components require the 'kernel_admin' attribute"}),
                    );
                }
                return;
            }
            let created = !state.entities.contains_key(&eid);
            if created {
                let requested_region = f.get("region").and_then(|v| v.as_str());
                if !spawn_in_region(
                    state,
                    &eid,
                    pos2,
                    vel2,
                    comps,
                    requested_region,
                    SpawnAuthoritySeed::default(),
                ) {
                    if let Some(req) = f.get("request_id") {
                        emit(
                            state,
                            wid,
                            json!({"op":"CreateEntityResponse","request_id":req,
                            "entity":eid,"success":false,
                            "reason":"wal_persist_failed: create not durably persisted"}),
                        );
                    } else {
                        emit(
                            state,
                            wid,
                            json!({"op":"UpdateRejected","entity":eid,"comp":"create",
                            "reason":"wal_persist_failed: create not durably persisted"}),
                        );
                    }
                    return;
                }
            }
            if let Some(req) = f.get("request_id") {
                emit(
                    state,
                    wid,
                    json!({"op":"CreateEntityResponse","request_id":req,
                    "entity":eid,"success":created}),
                );
            }
        }
        "DeleteEntity" => {
            let eid = match f.get("entity").and_then(|v| v.as_str()) {
                Some(s) => s.to_string(),
                None => return,
            };
            let request_id = f.get("request_id").cloned();
            if state.deleted_entities.contains(&eid) {
                if let Some(req) = request_id {
                    emit(
                        state,
                        wid,
                        json!({"op":"DeleteEntityResponse","request_id":req,
                        "entity":eid,"success":true,"idempotent":true}),
                    );
                }
                return;
            }
            if let Err(reason) = authoritative_entity_owner(state, wid, &eid) {
                if let Some(req) = request_id {
                    emit(
                        state,
                        wid,
                        json!({"op":"DeleteEntityResponse","request_id":req,
                        "entity":eid,"success":false,"reason":reason}),
                    );
                } else {
                    emit(
                        state,
                        wid,
                        json!({"op":"UpdateRejected","entity":eid,"comp":"delete","reason":reason}),
                    );
                }
                return;
            }
            if reject_stale_authority_epoch(state, wid, &eid, "delete", f) {
                return;
            }
            let delete_version = state
                .entities
                .get(&eid)
                .map(|e| e.version.saturating_add(1))
                .unwrap_or(0);
            if state
                .wal_append(&json!({
                    "kind":"delete_tombstone","entity":&eid,"version":delete_version,"writer":wid
                }))
                .is_err()
            {
                if let Some(req) = request_id {
                    emit(
                        state,
                        wid,
                        json!({"op":"DeleteEntityResponse","request_id":req,
                        "entity":eid,"success":false,"reason":"wal_persist_failed: tombstone not durable"}),
                    );
                } else {
                    emit(
                        state,
                        wid,
                        json!({"op":"UpdateRejected","entity":eid,"comp":"delete",
                        "reason":"wal_persist_failed: tombstone not durable"}),
                    );
                }
                return;
            }
            state.deleted_entities.insert(eid.clone());
            state.pending_handoffs.retain(|h| h.eid != eid);
            state.pending_handoff_intent.remove(&eid);
            let removed = state.entities.remove(&eid).is_some();
            if removed {
                let wids: Vec<String> = state.workers.keys().cloned().collect();
                for wid2 in wids {
                    if state.workers[&wid2].view.contains(&eid) {
                        emit(state, &wid2, json!({"op":"RemoveEntity","entity":eid}));
                        if let Some(w) = state.workers.get_mut(&wid2) {
                            w.view.remove(&eid);
                        }
                    }
                }
            }
            if let Some(req) = request_id {
                emit(
                    state,
                    wid,
                    json!({"op":"DeleteEntityResponse","request_id":req,
                    "entity":eid,"success":removed}),
                );
            }
        }
        // ── Commands / RPC: caller -> authority holder -> response -> caller ──
        "CommandRequest" => {
            let eid = f
                .get("entity")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let req_id = f
                .get("request_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            if req_id.is_empty() {
                emit(
                    state,
                    wid,
                    json!({"op":"CommandResponse","request_id":req_id,
                    "success":false,"reason":"missing command request_id"}),
                );
                return;
            }
            if state.pending_commands.contains_key(&req_id) {
                emit(
                    state,
                    wid,
                    json!({"op":"CommandResponse","request_id":req_id,
                    "success":false,"reason":"duplicate pending command request_id"}),
                );
                return;
            }
            let route = state.entities.get(&eid).and_then(|e| {
                // Route to the entity's CURRENT pos-authority owner so a command follows the
                // auto-handoff across zones (fixes a commanded player losing input after crossing a
                // seam). Fall back to the region's worker when no pos-owner is set.
                if let Some(ca) = e.authority.get("pos") {
                    if let Some(owner) = ca.owner.clone() {
                        return Some((owner, "pos".to_string(), Some(ca.epoch)));
                    }
                }
                state
                    .region_worker
                    .get(&e.region)
                    .cloned()
                    .map(|owner| (owner, "pos".to_string(), None))
            });
            match route {
                Some((ow, authority_comp, authority_epoch)) if state.workers.contains_key(&ow) => {
                    state.pending_commands.insert(
                        req_id.clone(),
                        PendingCommand {
                            caller: wid.to_string(),
                            entity: eid.clone(),
                            owner: ow.clone(),
                            authority_comp: authority_comp.clone(),
                            authority_epoch,
                        },
                    );
                    emit(
                        state,
                        &ow,
                        json!({"op":"CommandRequest","entity":eid,
                        "command":f.get("command").cloned().unwrap_or(Value::Null),
                        "payload":f.get("payload").cloned().unwrap_or(Value::Null),
                        "request_id":req_id,"caller":wid,
                        "authority_comp":authority_comp,"authority_epoch":authority_epoch}),
                    );
                }
                _ => {
                    emit(
                        state,
                        wid,
                        json!({"op":"CommandResponse","request_id":req_id,
                        "success":false,"reason":"no authoritative worker for entity"}),
                    );
                }
            }
        }
        "CommandResponse" => {
            let req_id = f
                .get("request_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            if let Some(pending) = state.pending_commands.get(&req_id).cloned() {
                if pending.owner != wid {
                    return;
                }
                let response_entity = f.get("entity").and_then(|v| v.as_str()).or_else(|| {
                    f.get("payload")
                        .and_then(|p| p.get("entity"))
                        .and_then(|v| v.as_str())
                });
                if response_entity != Some(pending.entity.as_str()) {
                    return;
                }
                let current_authority_ok = match state.entities.get(&pending.entity) {
                    Some(e) => match pending.authority_epoch {
                        Some(epoch) => e
                            .authority
                            .get(&pending.authority_comp)
                            .map(|ca| {
                                ca.owner.as_deref() == Some(pending.owner.as_str())
                                    && ca.epoch == epoch
                            })
                            .unwrap_or(false),
                        None => state
                            .region_worker
                            .get(&e.region)
                            .map(|owner| owner == &pending.owner)
                            .unwrap_or(false),
                    },
                    None => false,
                };
                if !current_authority_ok {
                    state.pending_commands.remove(&req_id);
                    if state.workers.contains_key(&pending.caller) {
                        emit(
                            state,
                            &pending.caller,
                            json!({"op":"CommandResponse","request_id":req_id,
                            "success":false,"reason":"stale command authority"}),
                        );
                    }
                    return;
                }
                state.pending_commands.remove(&req_id);
                if state.workers.contains_key(&pending.caller) {
                    emit(
                        state,
                        &pending.caller,
                        json!({"op":"CommandResponse","request_id":req_id,
                        "entity":pending.entity,
                        "routed_owner":pending.owner,
                        "authority_comp":pending.authority_comp,
                        "authority_epoch":pending.authority_epoch,
                        "success":f.get("success").cloned().unwrap_or(Value::Bool(true)),
                        "reason":f.get("reason").cloned().unwrap_or(Value::Null),
                        "payload":f.get("payload").cloned().unwrap_or(Value::Null)}),
                    );
                }
            }
        }
        // ── EntityQuery: constraint -> matching entities ──
        "EntityQuery" => {
            let req_id = f
                .get("request_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let q = f
                .get("query")
                .cloned()
                .unwrap_or_else(|| json!({"type":"all"}));
            let mut hits: Vec<Value> = Vec::new();
            let worker = match state.workers.get(wid) {
                Some(w) => w,
                None => return,
            };
            let want_intent = f
                .get("include_handoff_intent")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            let debug_ok = worker
                .attributes
                .iter()
                .any(|a| a == "observer" || a == "debug");
            for (eid, e) in state.entities.iter() {
                if matches_query(e, &q) && visible(worker, e) {
                    let mut row = json!({"entity":eid,"pos":[e.pos[0],e.pos[1]],
                        "components":Value::Object(e.components.clone()),"region":e.region,
                        "authority":authority_to_json(&e.authority)});
                    if want_intent && debug_ok {
                        if let Some(i) = state.pending_handoff_intent.get(eid) {
                            row["handoff_intent"] = handoff_intent_to_json(i);
                        }
                    }
                    hits.push(row);
                }
            }
            // SEAM-INTEREST: ghosts (read-only neighbour-zone mirrors) are visible to a worker whose AOI covers
            // them too, so the cross-seam battle renders (the observer's EntityQuery{all} picks them up) + a
            // query-based targeter finds them. They carry a `ghost:true` + `owner_region` marker and NO authority
            // (the row's `authority` is empty), so nothing can mistake a ghost for an authoritative entity.
            for (eid, g) in state.ghosts.iter() {
                if ghost_query_match(g, &q) && ghost_visible(worker, g) {
                    let mut comps = g.components.clone();
                    comps.insert("ghost".to_string(), json!(true));
                    hits.push(json!({"entity":eid,"pos":[g.pos[0],g.pos[1]],
                        "components":Value::Object(comps),"region":g.owner_region,
                        "ghost":true,"owner_region":g.owner_region,"authority":json!({})}));
                }
            }
            emit(
                state,
                wid,
                json!({"op":"EntityQueryResponse","request_id":req_id,
                "count":hits.len(),"entities":hits}),
            );
        }
        // ── InspectorQuery: read-only whole-cluster truth frame. Gated by the
        // "inspector" attr; POLLED (the Godot inspector polls at rate_hz). Reuses authority_to_json +
        // handoff_intent_to_json; sources zones/workers/metrics/pending from live state. Entities capped so
        // the Inspector never becomes the fan-out killer. ──
        "InspectorQuery" => {
            let req_id = f
                .get("request_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let worker = match state.workers.get(wid) {
                Some(w) => w,
                None => return,
            };
            if !worker.attributes.iter().any(|a| a == "inspector") {
                emit(
                    state,
                    wid,
                    json!({"op":"UpdateRejected","request_id":req_id,
                    "reason":"InspectorQuery requires the inspector attribute"}),
                );
                return;
            }
            let cap = f
                .get("max_entities")
                .and_then(|v| v.as_u64())
                .unwrap_or(1000) as usize;
            // zones: region -> owning worker + last-reported load + dynamic split sub-boundaries
            let zones: Vec<Value> = state
                .region_worker
                .iter()
                .map(|(region, w)| {
                    json!({"region": region, "worker": w,
                    "load": state.worker_load.get(w).copied().unwrap_or(0.0),
                    "splits": state.splits.get(region)})
                })
                .collect();
            // workers: id -> region, view_count (interest-set size), attributes, load, out_queue.
            // G4 backpressure: out_queue = the per-worker egress backlog depth (enqueued-by-emit minus
            // dequeued-by-the-writer). The channel is still UNBOUNDED -- this makes the backlog VISIBLE
            // (the next step bounds it: a bounded channel + drop-on-full once a real slow-consumer is seen).
            let workers: Vec<Value> = state
                .workers
                .iter()
                .map(|(id, w)| {
                    json!({"worker_id": id, "region": w.region, "view_count": w.view.len(),
                    "attributes": w.attributes.iter().cloned().collect::<Vec<String>>(),
                    "load": state.worker_load.get(id).copied().unwrap_or(0.0),
                    "out_queue": w.out_queue.load(Ordering::Relaxed),
                    "dropped": w.dropped.load(Ordering::Relaxed),
                    "ingress_tokens": w.ingress_tokens,
                    "ingress_rejected": w.ingress_rejected,
                    "slow_consumer": w.out_queue.load(Ordering::Relaxed) > EGRESS_SOFT_CAP})
                })
                .collect();
            // entities: capped, each with per-component authority + handoff intent + the gen clock
            let mut ents: Vec<Value> = Vec::new();
            for (eid, e) in state.entities.iter() {
                if ents.len() >= cap {
                    break;
                }
                let mut row = json!({"entity": eid, "region": e.region,
                    "pos": [e.pos[0], e.pos[1]], "authority": authority_to_json(&e.authority)});
                if let Some(g) = e.components.get("gen") {
                    row["gen"] = g.clone();
                }
                if let Some(st) = e.components.get("sim_time") {
                    row["sim_time"] = st.clone();
                }
                if let Some(i) = state.pending_handoff_intent.get(eid) {
                    row["handoff_intent"] = handoff_intent_to_json(i);
                }
                ents.push(row);
            }
            // SEAM-INTEREST: include the read-only ghosts (tagged) so the Inspector shows the cross-seam mirrors
            // a broker is holding. They carry `ghost:true` + `owner_region` and an EMPTY authority map (a ghost
            // owns nothing) -- visibly distinct from an authoritative entity in the same frame.
            for (eid, g) in state.ghosts.iter() {
                if ents.len() >= cap {
                    break;
                }
                ents.push(json!({"entity": eid, "region": g.owner_region,
                    "pos": [g.pos[0], g.pos[1]], "authority": json!({}),
                    "ghost": true, "owner_region": g.owner_region}));
            }
            let frame = json!({
                "op": "InspectorFrame",
                "request_id": req_id,
                "t_server": now_millis(),
                "broker": {
                    "entity_count": state.entities.len(),
                    "handoffs": state.metrics.handoffs,
                    "mesh_handoffs": state.metrics.mesh_handoffs, // CROSS-BROKER handoffs (entities forwarded across the process seam) -- previously invisible; `handoffs` counts only LOCAL same-broker transfers
                    "applies": state.metrics.applies,
                    "failovers": state.metrics.failovers,
                    "boundary": state.boundary,
                    "boundaries": state.boundaries.clone(), // N-ZONE: the full strip cut list (the inspector/gate reads the partition shape)
                    "lock_max_hold_ms": state.lock_max_hold_ms,
                    "lock_max_hold_path": state.lock_max_hold_path,
                    "lock_last_hold_ms": state.lock_last_hold_ms,
                    "load_level": state.load_level,
                    // L6 lease-fenced registry: this broker's own region + the lease epoch it holds, the
                    // epochs it knows for every region (registry-learned), whether it has been superseded
                    // (self-fenced), and how many stale-incarnation handoffs it has rejected.
                    "my_region": state.my_region,
                    "region_lease_epoch": state.region_lease_epoch.iter().map(|(k,v)| (k.clone(), json!(v))).collect::<Map<String,Value>>(),
                    "superseded_regions": state.superseded_regions.iter().cloned().collect::<Vec<String>>(),
                    "fenced_stale_handoffs": state.metrics.fenced_stale_handoffs,
                    // SEAM-INTEREST: how many cross-seam read-only ghosts this broker currently holds + the band.
                    "ghosts": state.ghosts.len(),
                    "interest_band": state.interest_band,
                    // #3 OPS: the new health/metrics fields, also surfaced here so the Inspector and the Health
                    // endpoint never diverge (both read the same state). tick_lag = monitor-cadence slip;
                    // rss_bytes = this process's working set; draining = the graceful-drain state.
                    "tick_lag_ms": state.tick_lag_ms,
                    "rss_bytes": process_rss_bytes(),
                    "wal_bytes": state.wal_bytes,
                    "draining": state.draining,
                    "pending_mesh": state.pending_mesh.len()
                },
                "zones": zones,
                "workers": workers,
                "entities": ents,
                "entities_capped": state.entities.len() > cap,
                "pending": {
                    "mesh": state.pending_mesh.len(),
                    "handoff_intent": state.pending_handoff_intent.len()
                },
                "recent_rejected": state.rejected.iter().rev().take(20).cloned().collect::<Vec<Value>>()
            });
            emit(state, wid, frame);
        }
        // ── #3 OPS: HEALTH/METRICS endpoint (a scrapable live-metrics surface). UNGATED:
        // a liveness/readiness prober (k8s, load-balancer, or curl-equivalent) holds NO inspector
        // attribute, so health must be readable by any connected worker. Returns ONE HealthFrame over the existing
        // length-prefixed-JSON wire -- no HTTP dep, no second port. The fields are the live state.metrics + RSS +
        // tick-lag + WAL/ghost/pending sizes (health_snapshot), so this never diverges from the Inspector view. ──
        "Health" => {
            let req_id = f
                .get("request_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let mut snap = health_snapshot(state);
            snap["op"] = json!("HealthFrame");
            snap["request_id"] = json!(req_id);
            emit(state, wid, snap);
        }
        // ── #3 OPS: GRACEFUL-DRAIN (rolling-deploy without kicking players). A `Drain` op flips this broker into
        // the draining state: it (1) STOPS accepting new CreateEntity (rejected "draining" so the spawn lands on a
        // live broker), and (2) hands EVERY entity it owns across the mesh to the neighbour owning where the entity
        // is, via the PROVEN 2-phase mesh_forward (pending_mesh -> MeshAck -> exactly-once, conservation-EXACT). The
        // monitor tick (drain progress) re-runs the hand-off until entities+pending_mesh are empty, then -- if
        // drain_exit -- exits 0 (a clean deploy shutdown). GATED to an admin attribute (drain is a control action):
        // kernel_admin OR inspector OR a worker carrying "ops". `exit` field overrides drain_exit per-call. ──
        "Drain" => {
            let req_id = f
                .get("request_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            if !worker_has_attr(state, wid, "kernel_admin")
                && !worker_has_attr(state, wid, "inspector")
                && !worker_has_attr(state, wid, "ops")
            {
                emit(
                    state,
                    wid,
                    json!({"op":"UpdateRejected","request_id":req_id,
                    "reason":"Drain requires the kernel_admin/inspector/ops attribute"}),
                );
                return;
            }
            if let Some(ex) = f.get("exit").and_then(|v| v.as_bool()) {
                state.drain_exit = ex;
            }
            state.draining = true;
            let dispatched = drain_handoff_owned(state); // kick off the hand-off immediately; the tick continues it
            let remaining = state.entities.len();
            let no_neighbour = state.mesh.is_empty();
            eprintln!("[drain] DRAIN requested -- draining=true, dispatched {dispatched} entit(y/ies) to neighbour(s), {remaining} still local, pending_mesh={}{}",
                state.pending_mesh.len(), if no_neighbour { " (NO mesh neighbour -- cannot drain, dead-end)" } else { "" });
            emit(
                state,
                wid,
                json!({"op":"DrainAck","request_id":req_id,
                "draining": true, "dispatched": dispatched, "remaining_local": remaining,
                "pending_mesh": state.pending_mesh.len(), "drain_exit": state.drain_exit,
                "no_neighbour": no_neighbour}),
            );
        }
        // ── G2 SnapshotMarker: a coordinator records a consistent point-in-time cut (WAL offset + manifest) ──
        "SnapshotMarker" => {
            let req_id = f
                .get("request_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let snapshot_id = f
                .get("snapshot_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            // gate: a snapshot reads the whole world -- require the snapshot/inspector/kernel_admin attribute
            {
                let worker = match state.workers.get(wid) {
                    Some(w) => w,
                    None => return,
                };
                if !worker
                    .attributes
                    .iter()
                    .any(|a| a == "snapshot" || a == "inspector" || a == "kernel_admin")
                {
                    emit(
                        state,
                        wid,
                        json!({"op":"UpdateRejected","request_id":req_id,
                        "reason":"SnapshotMarker requires the snapshot/inspector/kernel_admin attribute"}),
                    );
                    return;
                }
            }
            // A snapshot cut must describe the canonical RAM state that the returned
            // wal_offset can replay. Drain staged DurableTransitions first so the
            // marker cannot name queued WAL records the live world has not published.
            flush_pending_updates(state);
            if state.wal_degraded {
                emit(
                    state,
                    wid,
                    json!({"op":"UpdateRejected","request_id":req_id,
                    "reason":"wal_degraded: snapshot cut cannot be made while durable queues are unflushed"}),
                );
                return;
            }
            // Record the marker in the WAL FIRST -> the restore replays UP TO this offset (the consistent cut).
            // If the marker is not durable, do not disable compaction or return a manifest for a cut recovery
            // cannot name.
            if state
                .wal_append(
                    &json!({"kind":"snapshot_marker","snapshot_id":snapshot_id,"t":now_millis()}),
                )
                .is_err()
            {
                emit(
                    state,
                    wid,
                    json!({"op":"UpdateRejected","request_id":req_id,
                    "reason":"wal_persist_failed: snapshot marker not durably recorded"}),
                );
                return;
            }
            // R0.3: a coordinated point-in-time snapshot was taken -> disable WAL compaction for this broker's
            // lifetime, so a later GW_RESTORE_OFFSET cut against this WAL stays byte-valid (compaction would
            // rewrite the file and invalidate the offset). The disk-fill scenario uses no SnapshotMarker.
            state.snapshot_seen = true;
            let wal_offset = state.wal_bytes;
            // authority_hash (FNV-1a fold over (entity, pos-owner, pos-epoch)) -> a restore detects a divergent cut
            let authority_hash = snapshot_authority_hash(&state.entities);
            let in_flight: Vec<Value> = state
                .pending_mesh
                .iter()
                .map(|(eid, (_frame, _when, target))| json!({"entity": eid, "target": target, "type": "MeshHandoff"}))
                .collect();
            let broker_id = std::env::var("GW_BROKER_ID").unwrap_or_else(|_| "broker".to_string());
            emit(
                state,
                wid,
                json!({
                    "op": "SnapshotManifest", "request_id": req_id, "snapshot_id": snapshot_id,
                    "snapshot_manifest_version": SNAPSHOT_MANIFEST_VERSION,
                    "snapshot_schema_version": SNAPSHOT_SCHEMA_VERSION,
                    "spatial_schema_version": SPATIAL_SCHEMA_VERSION,
                    "coordinate_codec_version": COORDINATE_CODEC_VERSION,
                    "component_registry_version": STANDARD_COMPONENT_REGISTRY_VERSION,
                    "partition_map_version": state.zone_topology_rev,
                    "spatial_schema": spatial_schema_contract(state),
                    "partition_map": partition_map_contract(state),
                    "broker_id": broker_id, "wal_offset": wal_offset, "entity_count": state.entities.len(),
                    "authority_hash": authority_hash.to_string(), "pending_mesh": state.pending_mesh.len(),
                    "in_flight": in_flight, "t_server": now_millis()
                }),
            );
        }
        // ── ReserveEntityIds: atomic id block ──
        "ReserveEntityIds" => {
            let req_id = f
                .get("request_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let n = f.get("count").and_then(|v| v.as_u64()).unwrap_or(1);
            let first = state.entity_id_reservations;
            let next = state.entity_id_reservations.saturating_add(n);
            if state
                .wal_append(
                    &json!({"kind":"reserve_entity_ids","first_id":first,"count":n,"next_id":next}),
                )
                .is_err()
            {
                emit(
                    state,
                    wid,
                    json!({"op":"UpdateRejected","request_id":req_id,
                    "reason":"wal_persist_failed: id reservation not durably recorded"}),
                );
                return;
            }
            state.entity_id_reservations = next;
            emit(
                state,
                wid,
                json!({"op":"ReserveEntityIdsResponse","request_id":req_id,
                "first_id":first,"count":n}),
            );
        }
        // ── dynamic components ──
        "SetComponentAuthority" => {
            let eid = f
                .get("entity")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let comp = f
                .get("comp")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let request_id = f.get("request_id").cloned();
            if !worker_has_attr(state, wid, "kernel_admin") {
                let reason = "component authority changes require the 'kernel_admin' attribute";
                if let Some(req) = request_id {
                    emit(
                        state,
                        wid,
                        json!({"op":"SetComponentAuthorityResponse","request_id":req,
                        "entity":eid,"comp":comp,"success":false,"reason":reason}),
                    );
                } else {
                    emit(
                        state,
                        wid,
                        json!({"op":"UpdateRejected","entity":eid,"comp":comp,"reason":reason}),
                    );
                }
                return;
            }
            let owner = f
                .get("owner")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string());
            let mode = f
                .get("mode")
                .and_then(|v| v.as_str())
                .and_then(AuthorityMode::from_wire_str);
            let requested_epoch = frame_authority_epoch(f);
            let old_owner = state
                .entities
                .get(&eid)
                .and_then(|e| component_authority_owner(e, &comp));
            let (version, authority_epoch, mode_s) = if let Some(e) = state.entities.get(&eid) {
                let mut next = e.clone();
                ensure_component_authority(&mut next, &comp);
                let ca = next.authority.get_mut(&comp).unwrap();
                let current_epoch = ca.epoch;
                if let Some(expected_epoch) = requested_epoch {
                    if expected_epoch != current_epoch {
                        let reason = format!(
                            "authority_epoch fence mismatch: expected current {current_epoch}, got {expected_epoch}"
                        );
                        if let Some(req) = request_id.clone() {
                            emit(
                                state,
                                wid,
                                json!({"op":"SetComponentAuthorityResponse","request_id":req,
                                "entity":eid,"comp":comp,"success":false,
                                "reason":reason,"current_authority_epoch":current_epoch}),
                            );
                        } else {
                            emit(
                                state,
                                wid,
                                json!({"op":"UpdateRejected","entity":eid,"comp":comp,
                                "reason":reason,"current_authority_epoch":current_epoch}),
                            );
                        }
                        return;
                    }
                }
                if let Some(mode) = mode.clone() {
                    ca.mode = mode;
                }
                ca.owner = owner.clone();
                ca.epoch = current_epoch.saturating_add(1);
                next.version = next.version.saturating_add(1);
                (next.version, ca.epoch, ca.mode.as_wire_str().to_string())
            } else {
                return;
            };
            if state
                .wal_append(&json!({
                "kind":"component_authority","entity":&eid,"version":version,
                "writer":wid,"comp":&comp,"owner":owner.clone(),
                "authority_epoch":authority_epoch,"mode":mode_s.clone()
                }))
                .is_err()
            {
                let reason = "wal_persist_failed: component authority change not durably recorded";
                if let Some(req) = request_id.clone() {
                    emit(
                        state,
                        wid,
                        json!({"op":"SetComponentAuthorityResponse","request_id":req,
                        "entity":eid,"comp":comp,"success":false,"reason":reason}),
                    );
                } else {
                    emit(
                        state,
                        wid,
                        json!({"op":"UpdateRejected","entity":eid,"comp":comp,"reason":reason}),
                    );
                }
                return;
            }
            if let Some(e) = state.entities.get_mut(&eid) {
                ensure_component_authority(e, &comp);
                let ca = e.authority.get_mut(&comp).unwrap();
                if let Some(mode) = mode {
                    ca.mode = mode;
                }
                ca.owner = owner.clone();
                ca.epoch = authority_epoch;
                e.version = version;
            } else {
                return;
            }
            if let Some(old_wid) = old_owner.as_deref() {
                if Some(old_wid.to_string()) != owner && state.workers.contains_key(old_wid) {
                    revoke_authority(state, old_wid, &eid, &comp);
                }
            }
            if let Some(owner_wid) = owner.as_deref() {
                if state.workers.contains_key(owner_wid) {
                    grant_authority(state, owner_wid, &eid, &comp);
                }
            }
            if let Some(req) = request_id {
                emit(
                    state,
                    wid,
                    json!({"op":"SetComponentAuthorityResponse","request_id":req,
                    "entity":eid,"comp":comp,"success":true,
                    "owner":owner.clone(),"authority_epoch":authority_epoch,"mode":mode_s}),
                );
            }
        }
        "AddComponent" => {
            let eid = f
                .get("entity")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let comp = f
                .get("comp")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let value = f.get("value").cloned().unwrap_or(Value::Null);
            // A1 + 5a: only this component's authority holder may add it. Region owner is the fallback
            // for legacy components; explicit component leases can route add/remove to physics/logic.
            if !state.entities.contains_key(&eid) {
                return;
            }
            if let Err(reason) = authoritative_component_writer(state, wid, &eid, &comp) {
                emit(
                    state,
                    wid,
                    json!({"op":"UpdateRejected","entity":eid,"comp":comp,"reason":reason}),
                );
                return;
            }
            if comp == "acl"
                && !state
                    .workers
                    .get(wid)
                    .map(|w| w.attributes.iter().any(|a| a == "acl_admin"))
                    .unwrap_or(false)
            {
                emit(
                    state,
                    wid,
                    json!({"op":"UpdateRejected","entity":eid,"comp":comp,
                    "reason":"acl requires the 'acl_admin' attribute"}),
                );
                return;
            }
            if reject_kernel_reserved_write(state, wid, &eid, &comp) {
                return;
            }
            if reject_stale_authority_epoch(state, wid, &eid, &comp, f) {
                return;
            }
            let exists = state
                .entities
                .get(&eid)
                .map(|e| !e.components.contains_key(&comp))
                .unwrap_or(false);
            if exists {
                let version = state
                    .entities
                    .get(&eid)
                    .map(|e| e.version.saturating_add(1))
                    .unwrap_or(1);
                if state
                    .wal_append(&json!({
                    "kind":"component_add","entity":&eid,"version":version,
                    "writer":wid,"comp":&comp,"value":value.clone()
                    }))
                    .is_err()
                {
                    emit(
                        state,
                        wid,
                        json!({"op":"UpdateRejected","entity":eid,"comp":comp,
                        "reason":"wal_persist_failed: component add not durably recorded"}),
                    );
                    return;
                }
                if let Some(e) = state.entities.get_mut(&eid) {
                    e.components.insert(comp.clone(), value.clone());
                    ensure_component_authority(e, &comp);
                    e.version = version;
                } else {
                    return;
                }
                let wids: Vec<String> = state.workers.keys().cloned().collect();
                for w2 in wids {
                    if state.workers[&w2].view.contains(&eid) {
                        emit(
                            state,
                            &w2,
                            json!({"op":"AddComponent","entity":eid,"comp":comp,"value":value.clone()}),
                        );
                    }
                }
            }
        }
        "RemoveComponent" => {
            let eid = f
                .get("entity")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let comp = f
                .get("comp")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            // A1 + 5a: only this component's authority holder may remove it.
            if !state.entities.contains_key(&eid) {
                return;
            }
            if let Err(reason) = authoritative_component_writer(state, wid, &eid, &comp) {
                emit(
                    state,
                    wid,
                    json!({"op":"UpdateRejected","entity":eid,"comp":comp,"reason":reason}),
                );
                return;
            }
            if comp == "acl"
                && !state
                    .workers
                    .get(wid)
                    .map(|w| w.attributes.iter().any(|a| a == "acl_admin"))
                    .unwrap_or(false)
            {
                emit(
                    state,
                    wid,
                    json!({"op":"UpdateRejected","entity":eid,"comp":comp,
                    "reason":"acl requires the 'acl_admin' attribute"}),
                );
                return;
            }
            if reject_kernel_reserved_write(state, wid, &eid, &comp) {
                return;
            }
            if reject_stale_authority_epoch(state, wid, &eid, &comp, f) {
                return;
            }
            let had = state
                .entities
                .get(&eid)
                .map(|e| e.components.contains_key(&comp))
                .unwrap_or(false);
            if had {
                let version = state
                    .entities
                    .get(&eid)
                    .map(|e| e.version.saturating_add(1))
                    .unwrap_or(1);
                if state
                    .wal_append(&json!({
                    "kind":"component_remove","entity":&eid,"version":version,
                    "writer":wid,"comp":&comp
                    }))
                    .is_err()
                {
                    emit(
                        state,
                        wid,
                        json!({"op":"UpdateRejected","entity":eid,"comp":comp,
                        "reason":"wal_persist_failed: component remove not durably recorded"}),
                    );
                    return;
                }
                if let Some(e) = state.entities.get_mut(&eid) {
                    e.components.remove(&comp);
                    e.version = version;
                } else {
                    return;
                }
                let wids: Vec<String> = state.workers.keys().cloned().collect();
                for w2 in wids {
                    if state.workers[&w2].view.contains(&eid) {
                        emit(
                            state,
                            &w2,
                            json!({"op":"RemoveComponent","entity":eid,"comp":comp}),
                        );
                    }
                }
            }
        }
        // ── threshold transactions (prepare/preload/commit/adopt/abort) ──
        "ThresholdTx" => {
            let eid = f
                .get("entity")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let tx_id = f
                .get("tx_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let phase = f
                .get("phase")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let request_id = f.get("request_id").cloned();
            let allowed = matches!(
                phase.as_str(),
                "prepare" | "preload_ready" | "commit" | "adopt" | "abort"
            );
            if eid.is_empty() || tx_id.is_empty() || !allowed {
                if let Some(req) = request_id {
                    emit(
                        state,
                        wid,
                        json!({"op":"ThresholdTxResponse","request_id":req,
                        "entity":eid,"tx_id":tx_id,"success":false,
                        "reason":"invalid threshold transaction"}),
                    );
                }
                return;
            }
            if state.deleted_entities.contains(&eid) {
                if let Some(req) = request_id {
                    emit(
                        state,
                        wid,
                        json!({"op":"ThresholdTxResponse","request_id":req,
                        "entity":eid,"tx_id":tx_id,"phase":phase,
                        "success":false,"reason":"entity tombstoned"}),
                    );
                } else {
                    emit(
                        state,
                        wid,
                        json!({"op":"UpdateRejected","entity":eid,
                        "comp":"threshold.tx","reason":"entity tombstoned"}),
                    );
                }
                return;
            }
            if let Err(reason) = authoritative_entity_owner(state, wid, &eid) {
                if let Some(req) = request_id {
                    emit(
                        state,
                        wid,
                        json!({"op":"ThresholdTxResponse","request_id":req,
                        "entity":eid,"tx_id":tx_id,"success":false,"reason":reason}),
                    );
                } else {
                    emit(
                        state,
                        wid,
                        json!({"op":"UpdateRejected","entity":eid,"comp":"threshold.tx","reason":reason}),
                    );
                }
                return;
            }
            if reject_stale_authority_epoch(state, wid, &eid, "threshold.tx", f) {
                return;
            }
            let from_value = f.get("from").cloned().unwrap_or(Value::Null);
            let to_value = f.get("to").cloned().unwrap_or(Value::Null);
            let ts_ms = now_millis();
            let tx_value =
                threshold_tx_value(&tx_id, &phase, from_value.clone(), to_value.clone(), ts_ms);
            let commit_phase = phase == "commit";
            let final_phase = matches!(phase.as_str(), "adopt" | "abort");
            let old_owner = state
                .entities
                .get(&eid)
                .and_then(|e| state.region_worker.get(&e.region).cloned());
            let commit_target = if commit_phase {
                to_value
                    .as_str()
                    .filter(|s| !s.is_empty())
                    .map(str::to_string)
            } else {
                None
            };
            let new_owner = commit_target
                .as_deref()
                .and_then(|region| state.region_worker.get(region).cloned());
            let (version, existed, authority_epoch, old_region, new_region, moved_comps, authority) =
                if let Some(e) = state.entities.get(&eid) {
                    let mut next = e.clone();
                    let existed = next.components.contains_key("threshold.tx");
                    let old_region = next.region.clone();
                    if final_phase {
                        next.components.remove("threshold.tx");
                    } else {
                        next.components
                            .insert("threshold.tx".to_string(), tx_value.clone());
                    }
                    if commit_phase {
                        if let Some(to) = to_value.as_str().filter(|s| !s.is_empty()) {
                            next.region = to.to_string();
                        }
                    }
                    let (authority_epoch, moved_comps) = if commit_phase {
                        advance_physics_island_authority(
                            &mut next,
                            old_owner.as_deref(),
                            new_owner.as_deref(),
                        )
                    } else {
                        (component_authority_epoch(&next, "pos"), Vec::new())
                    };
                    let new_region = next.region.clone();
                    next.version = next.version.saturating_add(1);
                    (
                        next.version,
                        existed,
                        authority_epoch,
                        old_region,
                        new_region,
                        moved_comps,
                        authority_to_json(&next.authority),
                    )
                } else {
                    return;
                };
            if state
                .wal_append(&json!({
                "kind":format!("threshold_{}", phase),
                "entity":&eid,"version":version,"writer":wid,
                "tx_id":&tx_id,"authority_epoch":authority_epoch,
                "authority":authority.clone(),
                "components":moved_comps.clone(),
                "from": from_value.clone(),
                "to": to_value.clone(),
                "ts_ms": ts_ms
                }))
                .is_err()
            {
                if let Some(req) = request_id.clone() {
                    emit(
                        state,
                        wid,
                        json!({"op":"ThresholdTxResponse","request_id":req,
                        "entity":eid,"tx_id":tx_id,"phase":phase,
                        "success":false,
                        "reason":"wal_persist_failed: threshold transaction not durably recorded"}),
                    );
                } else {
                    emit(
                        state,
                        wid,
                        json!({"op":"UpdateRejected","entity":eid,"comp":"threshold.tx",
                        "reason":"wal_persist_failed: threshold transaction not durably recorded"}),
                    );
                }
                return;
            }
            if let Some(e) = state.entities.get_mut(&eid) {
                if final_phase {
                    e.components.remove("threshold.tx");
                } else {
                    e.components
                        .insert("threshold.tx".to_string(), tx_value.clone());
                }
                if commit_phase {
                    if let Some(to) = to_value.as_str().filter(|s| !s.is_empty()) {
                        e.region = to.to_string();
                    }
                    apply_authority_snapshot(e, &authority);
                }
                e.version = version;
            } else {
                return;
            }
            if commit_phase && old_region != new_region {
                let old_wid = state.region_worker.get(&old_region).cloned();
                let new_wid = state.region_worker.get(&new_region).cloned();
                if let Some(ow) = old_wid {
                    if state.workers.contains_key(&ow) {
                        for comp in &moved_comps {
                            revoke_authority(state, &ow, &eid, comp);
                        }
                    }
                }
                if let Some(nw) = new_wid {
                    if state.workers.contains_key(&nw) {
                        if !state.workers[&nw].view.contains(&eid) {
                            send_full(state, &nw, &eid);
                        }
                        for comp in &moved_comps {
                            grant_authority(state, &nw, &eid, comp);
                        }
                    }
                }
            }
            let wids: Vec<String> = state.workers.keys().cloned().collect();
            for w2 in wids {
                if state.workers[&w2].view.contains(&eid) {
                    if final_phase {
                        emit(
                            state,
                            &w2,
                            json!({"op":"RemoveComponent","entity":eid,"comp":"threshold.tx"}),
                        );
                    } else if existed {
                        emit(
                            state,
                            &w2,
                            json!({"op":"ComponentUpdate","entity":eid,"comp":"threshold.tx","value":tx_value}),
                        );
                    } else {
                        emit(
                            state,
                            &w2,
                            json!({"op":"AddComponent","entity":eid,"comp":"threshold.tx","value":tx_value}),
                        );
                    }
                }
            }
            if let Some(req) = request_id {
                emit(
                    state,
                    wid,
                    json!({"op":"ThresholdTxResponse","request_id":req,
                    "entity":eid,"tx_id":tx_id,"phase":phase,"success":true}),
                );
            }
        }
        // ── runtime flags (broadcast) ──
        "FlagUpdate" => {
            let flag = f
                .get("flag")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let value = f.get("value").cloned().unwrap_or(Value::Null);
            state.flags.insert(flag.clone(), value.clone());
            let wids: Vec<String> = state.workers.keys().cloned().collect();
            for w2 in wids {
                emit(
                    state,
                    &w2,
                    json!({"op":"FlagUpdate","flag":flag,"value":value}),
                );
            }
        }
        // ── metrics (load-aware LB input) ──
        "Metrics" => {
            let load = f.get("load").and_then(|v| v.as_f64()).unwrap_or(0.0);
            state.worker_load.insert(wid.to_string(), load);
        }
        // CROSS-BROKER MESH inbound: a neighbour broker handed us an entity -> adopt it locally
        "MeshHandoff" => {
            let eid = f
                .get("entity")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            if eid.is_empty() {
                return;
            }
            // L6 LEASE FENCE (the authoritative split-brain reject): a MeshHandoff carries the SENDER's owned
            // (src_region, lease_epoch). If the sender's epoch is STRICTLY LOWER than the epoch THIS broker
            // knows for that region (learned from the registry: a newer incarnation took it over with a higher
            // epoch), the sender is a STALE incarnation returned from a partition -> REFUSE it: do NOT adopt,
            // do NOT MeshAck (so it stays parked in the stale owner's pending_mesh, never double-owned here).
            // ADDITIVE/back-compat: a frame with NO lease_epoch (legacy single-incarnation tests) or one whose
            // epoch is >= what we know is accepted unchanged.
            if let (Some(src_region), Some(sender_epoch)) = (
                f.get("src_region").and_then(|v| v.as_str()),
                f.get("lease_epoch").and_then(|v| v.as_u64()),
            ) {
                if let Some(&known_epoch) = state.region_lease_epoch.get(src_region) {
                    if sender_epoch < known_epoch {
                        state.metrics.fenced_stale_handoffs += 1;
                        let rec = json!({"reason":"stale lease epoch (fenced incarnation)",
                            "entity":eid,"src_region":src_region,
                            "sender_lease_epoch":sender_epoch,"known_lease_epoch":known_epoch});
                        state.rejected.push(rec);
                        if state.rejected.len() > 256 {
                            let drop = state.rejected.len() - 256;
                            state.rejected.drain(0..drop);
                        }
                        eprintln!(
                            "[fence] REJECTED stale-epoch MeshHandoff for {eid} from region '{src_region}' lease_epoch={sender_epoch} < known {known_epoch} (returned-from-partition incarnation fenced; not adopted, not ACK'd)"
                        );
                        return;
                    }
                }
            }
            // G2.1d test: DROP the inbound handoff (no adopt, no ack) -> the wire-transit cut (A mesh_out'd,
            // B never adopted). On restore the resolver rebuilds A.pending_mesh and resends -> exactly-once.
            if state.mesh_adopt_drop {
                return;
            }
            if state.deleted_entities.contains(&eid) {
                emit(state, wid, json!({"op":"MeshAck","entity":eid}));
                return;
            }
            // SEAM-INTEREST -> CROSS transition: this entity was being PROJECTED to us as a ghost (it was near
            // the seam in the neighbour zone) and has now genuinely CROSSED. The real, authoritative entity is
            // about to be adopted into `entities`; drop the read-only ghost first so the two never coexist (the
            // ghost was the aim-point; now WE own the real thing). remove_ghost also clears it from worker views,
            // and spawn_in_region below re-checks-it-out as a real (authoritative) entity.
            if state.ghosts.contains_key(&eid) {
                remove_ghost(state, &eid);
            }
            // adopt it if NEW; if we already have it this is a RE-SEND (our earlier MeshAck was lost) --
            // don't double-spawn, but DO re-ACK below so the sender can release it. ACK in BOTH cases.
            let pos = arr2(f.get("pos"));
            let authority_epoch = f.get("authority_epoch").and_then(|v| v.as_u64());
            if let Some(forwarded_epoch) = state.mesh_forwarded_epoch.get(&eid).copied() {
                let stale_or_legacy = authority_epoch
                    .map(|epoch| epoch <= forwarded_epoch)
                    .unwrap_or(true);
                if stale_or_legacy {
                    state.rejected.push(json!({
                        "reason":"mesh handoff stale after onward forward",
                        "entity":eid,
                        "authority_epoch":authority_epoch,
                        "forwarded_authority_epoch":forwarded_epoch
                    }));
                    if state.rejected.len() > 256 {
                        let drop = state.rejected.len() - 256;
                        state.rejected.drain(0..drop);
                    }
                    emit(state, wid, json!({"op":"MeshAck","entity":eid}));
                    return;
                }
                if state.pending_mesh.contains_key(&eid) && !record_mesh_ack(state, &eid) {
                    return;
                }
            } else if state.pending_mesh.contains_key(&eid) {
                state.rejected.push(json!({
                    "reason":"mesh handoff stale while onward handoff pending",
                    "entity":eid,
                    "authority_epoch":authority_epoch
                }));
                if state.rejected.len() > 256 {
                    let drop = state.rejected.len() - 256;
                    state.rejected.drain(0..drop);
                }
                emit(state, wid, json!({"op":"MeshAck","entity":eid}));
                return;
            }
            let target = f.get("target").and_then(|v| v.as_str());
            let Some(adopt_region) = receiving_region_for_adopt(state, pos, target) else {
                state.rejected.push(json!({
                    "reason":"mesh handoff target region not owned",
                    "entity":eid,
                    "target":target,
                    "pos":[pos[0],pos[1]]
                }));
                if state.rejected.len() > 256 {
                    let drop = state.rejected.len() - 256;
                    state.rejected.drain(0..drop);
                }
                return;
            };
            if !state.entities.contains_key(&eid) {
                let vel = arr2(f.get("vel")); // adopt the inbound velocity (hardcoded [0,0] -> C1 seam break)
                let authority_snapshot = f.get("authority").cloned();
                let comps: Map<String, Value> = f
                    .get("components")
                    .and_then(|v| v.as_object())
                    .cloned()
                    .unwrap_or_default();
                // the zone this adopted entity now BELONGS to = one of THIS broker's owned regions (NOT a
                // position-derive against our local W|E boundary, which mislabeled a Fold-into-ZB as "W").
                if !spawn_committed_region(
                    state,
                    &eid,
                    pos,
                    vel,
                    comps,
                    adopt_region.clone(),
                    SpawnAuthoritySeed {
                        epoch: authority_epoch,
                        snapshot: authority_snapshot,
                    },
                ) {
                    // (broker owned it, no worker simulated it) -> frozen at the seam.
                    return;
                }
                eprintln!(
                    "[mesh] adopted {eid} (region '{adopt_region}'; vel {vel:?} carried; now ours)"
                );
            } else if !existing_mesh_adopt_matches(state, &eid, &adopt_region, authority_epoch) {
                state.rejected.push(json!({
                    "reason":"mesh handoff existing-entity mismatch",
                    "entity":eid,
                    "adopt_region":adopt_region,
                    "authority_epoch":authority_epoch
                }));
                if state.rejected.len() > 256 {
                    let drop = state.rejected.len() - 256;
                    state.rejected.drain(0..drop);
                }
                return;
            }
            record_replay_tape_handoff(
                state,
                ReplayHandoffRecord {
                    path: "mesh_in",
                    eid: &eid,
                    from: f.get("src_region").and_then(|v| v.as_str()),
                    to: Some(adopt_region.as_str()),
                    authority_epoch,
                    source_durable_gen: f.get("source_durable_gen").and_then(|v| v.as_u64()),
                    lease_epoch: f.get("lease_epoch").and_then(|v| v.as_u64()),
                },
            );
            // confirm receipt to the sender (the mesh-link worker that delivered this) so it releases the
            // parked entity from pending_mesh. Idempotent: re-sends are re-ACK'd, never double-adopted.
            // G2.1c: GW_MESH_ACK_DROP holds the ack so the entity stays in-flight (A.pending + B.adopted).
            if !state.mesh_ack_drop {
                emit(state, wid, json!({"op":"MeshAck","entity":eid}));
            }
        }
        // ── CROSS-BROKER SEAM-INTEREST inbound: a neighbour broker is projecting one of ITS near-seam entities
        // to us as a READ-ONLY ghost (so OUR worker can see + target it across the seam). We store it in
        // `ghosts` -- NEVER in `entities` -- so it is structurally non-authoritative: it can't be leased, can't
        // be granted authority, and a write to it is rejected (validate_and_apply_nosync's not-in-`entities`
        // gate). It is transient (no WAL): the source re-pushes it every tick; if the source stops, our TTL
        // reaper drops it. The SAME eid the source uses is the key, so if the entity later genuinely CROSSES
        // (a MeshHandoff for the same eid), the adopt removes the ghost and the real entity takes over (handled
        // in the MeshHandoff arm above via the ghost-supersede below). ──
        "MeshGhost" => {
            let eid = match f.get("entity").and_then(|v| v.as_str()) {
                Some(s) if !s.is_empty() => s.to_string(),
                _ => return,
            };
            // GUARD: if we ALREADY OWN this eid as a real entity (it lives in `entities` here -- e.g. it crossed
            // to us, or the source is mislabeled), do NOT also ghost it. A real entity dominates its ghost: we
            // are its authority now; a stale projection from the old owner must not shadow it.
            if state.entities.contains_key(&eid) {
                return;
            }
            let pos = arr2(f.get("pos"));
            let vel = arr2(f.get("vel"));
            let components: Map<String, Value> = f
                .get("components")
                .and_then(|v| v.as_object())
                .cloned()
                .unwrap_or_default();
            let owner_region = f
                .get("owner_region")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let first_time = !state.ghosts.contains_key(&eid);
            state.ghosts.insert(
                eid.clone(),
                GhostEntity {
                    pos,
                    vel,
                    components,
                    owner_region,
                    last_seen: Instant::now(),
                },
            );
            // mirror it to the interested workers (AddEntity/ComponentUpdate stream; NEVER an AuthorityChange).
            propagate_ghost(state, &eid, first_time);
        }
        // source proactively retracts a ghost (e.g. the underlying entity was deleted). Idempotent.
        "MeshGhostRemove" => {
            if let Some(eid) = f.get("entity").and_then(|v| v.as_str()) {
                remove_ghost(state, eid);
            }
        }
        _ => {}
    }
}

// the GhostEntity analogue of matches_query (a ghost has pos + components + owner_region, no ACL/authority).
fn ghost_query_match(g: &GhostEntity, q: &Value) -> bool {
    match q.get("type").and_then(|v| v.as_str()).unwrap_or("all") {
        "all" => true,
        "sphere" => {
            let c = q.get("center").map(|v| arr2(Some(v))).unwrap_or([0.0, 0.0]);
            let r = q.get("radius").and_then(|v| v.as_f64()).unwrap_or(0.0);
            let dx = g.pos[0] - c[0];
            let dy = g.pos[1] - c[1];
            dx * dx + dy * dy <= r * r
        }
        "box" => {
            let lo = q.get("min").map(|v| arr2(Some(v))).unwrap_or([0.0, 0.0]);
            let hi = q.get("max").map(|v| arr2(Some(v))).unwrap_or([0.0, 0.0]);
            g.pos[0] >= lo[0] && g.pos[0] <= hi[0] && g.pos[1] >= lo[1] && g.pos[1] <= hi[1]
        }
        "component" => {
            let comp = q.get("comp").and_then(|v| v.as_str()).unwrap_or("");
            g.components.contains_key(comp) || comp == "pos" || comp == "vel"
        }
        // a ghost's "region" is its OWNER region (the source zone); a region query matches on that.
        "region" => q.get("region").and_then(|v| v.as_str()) == Some(g.owner_region.as_str()),
        _ => false,
    }
}

fn matches_query(e: &Entity, q: &Value) -> bool {
    match q.get("type").and_then(|v| v.as_str()).unwrap_or("all") {
        "all" => true,
        "sphere" => {
            let c = q.get("center").map(|v| arr2(Some(v))).unwrap_or([0.0, 0.0]);
            let r = q.get("radius").and_then(|v| v.as_f64()).unwrap_or(0.0);
            let dx = e.pos[0] - c[0];
            let dy = e.pos[1] - c[1];
            dx * dx + dy * dy <= r * r
        }
        "box" => {
            let lo = q.get("min").map(|v| arr2(Some(v))).unwrap_or([0.0, 0.0]);
            let hi = q.get("max").map(|v| arr2(Some(v))).unwrap_or([0.0, 0.0]);
            e.pos[0] >= lo[0] && e.pos[0] <= hi[0] && e.pos[1] >= lo[1] && e.pos[1] <= hi[1]
        }
        "component" => {
            let comp = q.get("comp").and_then(|v| v.as_str()).unwrap_or("");
            e.components.contains_key(comp) || comp == "pos" || comp == "vel"
        }
        "region" => q.get("region").and_then(|v| v.as_str()) == Some(e.region.as_str()),
        _ => false,
    }
}

// A2 thin-fill (2026-06-20): the frame length `n` is peer-controlled; `vec![0u8; n]` would alloc up
// to 4 GiB on a single crafted frame = a one-packet remote OOM-DoS. Clamp before allocating.
// Keep the broker bound to the protocol crate's public v1 contract so SDKs, docs, and runtime agree.
const MAX_FRAME: usize = DEFAULT_MAX_FRAME_BYTES;

async fn read_frame<R: AsyncReadExt + Unpin>(rd: &mut R) -> Option<InboundFrame> {
    let mut hdr = [0u8; 4];
    rd.read_exact(&mut hdr).await.ok()?;
    let n = u32::from_be_bytes(hdr) as usize;
    if n > MAX_FRAME {
        return None; // oversized frame -> drop the connection (the caller breaks on None)
    }
    let mut body = vec![0u8; n];
    rd.read_exact(&mut body).await.ok()?;
    Some(InboundFrame {
        value: serde_json::from_slice(&body).ok()?,
        byte_len: n,
    })
}

fn auth_token_matches(required: Option<&str>, provided: Option<&str>) -> bool {
    required.is_none_or(|expected| provided == Some(expected))
}

fn resolve_connect_claims(
    required: Option<&str>,
    configured_claims: &HashMap<String, PeerClaims>,
    provided: Option<&str>,
    requested_region: Option<&str>,
    requested_attributes: &HashSet<String>,
) -> Result<Option<PeerClaims>, &'static str> {
    if !configured_claims.is_empty() {
        let Some(token) = provided else {
            return Err("authentication required");
        };
        let Some(claims) = configured_claims.get(token) else {
            return Err("authentication required");
        };
        if requested_region.is_some_and(|region| region != claims.region) {
            return Err("auth token is not valid for requested region");
        }
        if !requested_attributes.is_subset(&claims.attributes) {
            return Err("auth token is not valid for requested attributes");
        }
        return Ok(Some(claims.clone()));
    }

    if auth_token_matches(required, provided) {
        Ok(None)
    } else {
        Err("authentication required")
    }
}

fn auth_token_for_region(state: &ServerState, region: &str) -> Option<String> {
    state
        .connect_auth_claims
        .iter()
        .find(|(_, claims)| claims.region == region)
        .map(|(token, _)| token.clone())
        .or_else(|| state.connect_auth_token.clone())
}

async fn handle_conn(sock: tokio::net::TcpStream, state: Arc<Mutex<ServerState>>) {
    sock.set_nodelay(true).ok();
    let (mut rd, mut wr) = sock.into_split();

    let first_frame = match read_frame(&mut rd).await {
        Some(f) => f,
        None => return,
    };
    let first_byte_len = first_frame.byte_len;
    let first = first_frame.value;
    if first.get("op").and_then(|v| v.as_str()) != Some("WorkerConnect") {
        return;
    }
    let wid = match first.get("worker_id").and_then(|v| v.as_str()) {
        Some(s) => s.to_string(),
        None => return,
    };
    let requested_region = first.get("region").and_then(|v| v.as_str());
    let mut region = requested_region.unwrap_or("OBS").to_string();
    let mut attributes: HashSet<String> = first
        .get("attributes")
        .and_then(|v| v.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|x| x.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();

    // L4 protocol-versioning: a peer MAY declare its wire-format version via `proto` (u64).
    // ABSENT => legacy v0 (accepted, back-compat). PRESENT but outside [MIN_PROTO, PROTOCOL_VERSION]
    // => reject with a clean VersionReject frame + close the connection BEFORE the worker registers,
    // so a rolling broker upgrade can't let an incompatible peer corrupt the mesh.
    if let Some(proto) = first.get("proto").and_then(|v| v.as_u64()) {
        if proto < MIN_PROTO || proto > PROTOCOL_VERSION {
            {
                let s = state.lock().await;
                record_replay_tape_connect(
                    &s,
                    ReplayConnectRecord {
                        wid: &wid,
                        frame: &first,
                        byte_len: first_byte_len,
                        region: &region,
                        attributes: &attributes,
                        outcome: "rejected",
                        reason: Some("version_reject"),
                    },
                );
            }
            let rej = frame(&json!({
                "op": "VersionReject",
                "reason": "incompatible protocol version",
                "peer_proto": proto,
                "broker_proto": PROTOCOL_VERSION,
                "min_proto": MIN_PROTO,
            }));
            let _ = wr.write_all(&rej).await;
            println!("[broker] worker '{wid}' REJECTED -- incompatible proto {proto} (broker {PROTOCOL_VERSION}, min {MIN_PROTO})");
            return; // do NOT register; the connection drops as wr/rd go out of scope
        }
    }

    // Security v0/v0.1: optional connect gate. This is intentionally BEFORE worker registration,
    // before region lease, before egress channel allocation, and before the peer can claim privileged
    // attributes. Unset auth config preserves the dev/test wire shape. GW_AUTH_TOKEN keeps the old dev
    // shared-secret behavior; GW_AUTH_CLAIMS is the stricter production shape where the token maps to the
    // broker-owned region/attributes and peer JSON cannot self-assign them. The token is never logged.
    let (required_auth, configured_claims) = {
        let s = state.lock().await;
        (s.connect_auth_token.clone(), s.connect_auth_claims.clone())
    };
    let provided_auth = first.get("auth_token").and_then(|v| v.as_str());
    let mut used_broker_claims = false;
    match resolve_connect_claims(
        required_auth.as_deref(),
        &configured_claims,
        provided_auth,
        requested_region,
        &attributes,
    ) {
        Ok(Some(claims)) => {
            region = claims.region;
            attributes = claims.attributes;
            used_broker_claims = true;
        }
        Ok(None) => {}
        Err(reason) => {
            {
                let s = state.lock().await;
                record_replay_tape_connect(
                    &s,
                    ReplayConnectRecord {
                        wid: &wid,
                        frame: &first,
                        byte_len: first_byte_len,
                        region: &region,
                        attributes: &attributes,
                        outcome: "rejected",
                        reason: Some(reason),
                    },
                );
            }
            let rej = frame(&json!({
                "op": "AuthReject",
                "worker_id": wid,
                "error": "auth_error",
                "reason": reason
            }));
            let _ = wr.write_all(&rej).await;
            println!("[broker] worker '{wid}' REJECTED -- auth failed");
            return; // do NOT register; the connection drops as wr/rd go out of scope
        }
    }
    if !used_broker_claims {
        if requested_region.is_some_and(broker_owned_connect_region) {
            let reason = "broker-owned connect region requires token-bound claim";
            {
                let s = state.lock().await;
                record_replay_tape_connect(
                    &s,
                    ReplayConnectRecord {
                        wid: &wid,
                        frame: &first,
                        byte_len: first_byte_len,
                        region: &region,
                        attributes: &attributes,
                        outcome: "rejected",
                        reason: Some(reason),
                    },
                );
            }
            let rej = frame(&json!({
                "op": "AuthReject",
                "worker_id": wid,
                "error": "auth_error",
                "reason": reason
            }));
            let _ = wr.write_all(&rej).await;
            println!("[broker] worker '{wid}' REJECTED -- broker-owned connect region requires token-bound claim");
            return;
        }
        strip_peer_declared_broker_owned_attributes(&mut attributes);
    }
    let role = peer_role_for(&region, &attributes);

    // T1: BOUNDED egress channel (capacity CHANNEL_CAP). A stuck consumer can hold AT MOST CHANNEL_CAP unsent
    // frames; emit()'s try_send then fails Full and (for a critical frame) flags the worker for force-disconnect.
    // The bound is the channel TYPE, not a flag -- the structural RAM cap on this consumer's egress.
    let (tx, mut rx) = mpsc::channel::<Vec<u8>>(CHANNEL_CAP);
    let out_queue = Arc::new(AtomicU64::new(0)); // G4 backpressure: per-worker egress backlog depth
    let dropped = Arc::new(AtomicU64::new(0)); // G4 backpressure: degradable frames dropped under backpressure
    let disconnect = Arc::new(AtomicBool::new(false)); // T1: set when the hard egress cap was hit on a critical frame
    let disconnect_rd = disconnect.clone(); // T1: the read loop watches this to tear the conn down even if the client never sends/closes
    let oq_writer = out_queue.clone();
    let writer = tokio::spawn(async move {
        while let Some(buf) = rx.recv().await {
            oq_writer.fetch_sub(1, Ordering::Relaxed); // dequeued -> about to write to the socket
            if wr.write_all(&buf).await.is_err() {
                break;
            }
        }
        // rx closed (the WorkerHandle's tx was dropped -- normal disconnect OR a T1 force-disconnect via
        // reap_disconnecting). Dropping `wr` here closes the socket's write half so the peer reads EOF.
    });

    {
        let mut s = state.lock().await;
        let ingress_tokens = s.ingress_burst_frames.max(1.0);
        let connect_attributes_for_tape = attributes.clone();
        s.workers.insert(
            wid.clone(),
            WorkerHandle {
                region: region.clone(),
                role,
                attributes,
                view: HashSet::new(),
                authority_epochs: HashMap::new(),
                aoi_center: None,
                aoi_radius: None,
                fidelity_full_radius: None,
                fidelity_coarse_rate: 1,
                fidelity_coarse_grid: 0.0,
                fidelity_seq: HashMap::new(),
                tx,
                out_queue,
                dropped,
                disconnect,
                grid_cells: Vec::new(),
                ingress_tokens,
                ingress_last_refill: Instant::now(),
                ingress_rejected: 0,
            },
        );
        update_interest_grid(&mut s, &wid); // Interest: default a fresh worker to global (no AOI yet); an Interest op re-indexes it
        println!(
            "[broker] worker '{wid}' connected as region '{region}' role '{}'",
            role.as_str()
        );
        record_replay_tape_connect(
            &s,
            ReplayConnectRecord {
                wid: &wid,
                frame: &first,
                byte_len: first_byte_len,
                region: &region,
                attributes: &connect_attributes_for_tape,
                outcome: "accepted",
                reason: None,
            },
        );
        if role == PeerRole::Worker && region == "STANDBY" {
            s.standbys.push(wid.clone()); // a hot spare for failover
        } else if role == PeerRole::Worker {
            // ANY real zone-region leases authority: W/E for the position-sharded seam, AND arbitrary
            // open-world regions (planets / fold-targets like "MARS") so a PORTAL FOLD can hand authority
            // to a non-adjacent zone. OBS (observer) + MESH (cross-broker link) are control-regions.
            // A3 FENCING (resilience-#1, 2026-06-20): the insert WAS unconditional, so a reconnecting or
            // attacker worker claiming an already-leased region would STEAL it (split-brain -- and would
            // then PASS the authority check, defeating A1). Grant ONLY if the region is FREE or its lease
            // has EXPIRED; if a DIFFERENT worker still holds a LIVE lease, refuse (this worker observes
            // the region but owns nothing). A failover (expired lease) or a same-worker reconnect still grants.
            let now = Instant::now();
            let live_owner = s
                .region_worker
                .get(&region)
                .cloned()
                .filter(|owner| owner != &wid)
                .filter(|_| {
                    s.region_expires
                        .get(&region)
                        .map(|exp| *exp > now)
                        .unwrap_or(false)
                });
            if let Some(held) = live_owner {
                println!("[broker] worker '{wid}' REFUSED region '{region}' -- still held by a live lease ('{held}')");
            } else {
                let ttl = s.lease_ttl;
                s.region_worker.insert(region.clone(), wid.clone());
                s.region_expires
                    .insert(region.clone(), now + Duration::from_secs_f64(ttl));
                // start/renew the lease
            }
        }
        checkout_all(&mut s, &wid);
    }

    loop {
        // T1: race the next inbound frame against a short poll of the force-disconnect flag, so a consumer
        // that hit the hard egress cap (set by emit/reaped in dispatch) is torn down within ~100ms EVEN IF it
        // never sends or closes its own socket (a malicious "connect, never read, never write" client). The
        // bounded channel already caps its RAM at CHANNEL_CAP; this guarantees the parked read task + socket
        // are released too. Structural: the flag IS set by hitting the channel's capacity, not by any config.
        let frame = tokio::select! {
            f = read_frame(&mut rd) => f,
            _ = tokio::time::sleep(Duration::from_millis(100)) => {
                if disconnect_rd.load(Ordering::Relaxed) { break; }
                continue;
            }
        };
        match frame {
            None => break,
            Some(f) => {
                if f.value.get("op").and_then(|v| v.as_str()) == Some("Disconnect") {
                    break;
                }
                {
                    let mut s = state.lock().await;
                    // T1: measure the PER-FRAME dispatch lock-hold (the egress fan-out of a critical
                    // EntityEvent to N observers happens here, inside this hold) so lock_max_hold_ms in the
                    // InspectorFrame reflects the storm -- the monitor-tick instrumentation does NOT cover
                    // this path. record_lock_hold keeps only a max (cheap), so it's safe at high op rates.
                    let _t = Instant::now();
                    dispatch_frame(&mut s, &wid, &f.value, f.byte_len);
                    record_lock_hold(&mut s, "dispatch", _t.elapsed());
                }
                if disconnect_rd.load(Ordering::Relaxed) {
                    break; // this consumer was force-disconnected during its own frame's fan-out
                }
            }
        }
    }

    {
        let mut s = state.lock().await;
        remove_from_interest_grid(&mut s, &wid); // Interest: drop the worker's grid cells + global membership first
        s.workers.remove(&wid);
    }
    writer.abort();
}

// L2 service-discovery: a SMALL registry (GW_REGISTRY=dir) -- each broker writes its OWN per-region file
// {addr, hb_ms} (so two brokers never race on one file). registry_read merges all of them. The mesh link
// resolves the neighbour addr FROM the registry each retry, so a replaced node (B -> B' at a NEW addr) is
// followed automatically -- the L2 gap was the STATIC GW_MESH addr. NOT a k8s control-plane.
fn registry_read(reg_dir: &str) -> Map<String, Value> {
    let mut out = Map::new();
    if let Ok(rd) = std::fs::read_dir(reg_dir) {
        for ent in rd.flatten() {
            let p = ent.path();
            if p.extension().and_then(|e| e.to_str()) == Some("json") {
                if let (Some(region), Ok(txt)) = (
                    p.file_stem().and_then(|s| s.to_str()),
                    std::fs::read_to_string(&p),
                ) {
                    if let Ok(v) = serde_json::from_str::<Value>(&txt) {
                        out.insert(region.to_string(), v);
                    }
                }
            }
        }
    }
    out
}

// L6 lease-fenced registry: the heartbeat also stamps the lease_epoch this incarnation holds for the region,
// so a peer reading the registry learns the CURRENT owner's epoch (and a superseded incarnation is detectable
// by a strictly higher epoch under a different addr). lease_epoch is additive: an old record without it reads
// as None on the peer side -> legacy-accepted.
//
// REPLACE-IF-NEWER: when two
// incarnations both own region E (the split-brain window), a STALE incarnation's heartbeat must NOT clobber a
// newer one's registry record. Only write if our lease_epoch is >= the epoch currently recorded -- so old-B@10
// can never overwrite B'@11, and the highest-epoch holder wins the registry. A legacy record without an epoch
// (None) is treated as 0 -> still overwritable by an epoch'd writer (back-compat upgrade path).
fn registry_heartbeat(reg_dir: &str, region: &str, addr: &str, now_ms: u64, lease_epoch: u64) {
    let path = std::path::Path::new(reg_dir).join(format!("{region}.json"));
    let current = registry_lease_epoch(reg_dir, region).unwrap_or(0);
    if lease_epoch < current {
        return; // a newer incarnation owns this region in the registry -- do not clobber it (replace-if-newer)
    }
    if let Ok(s) =
        serde_json::to_string(&json!({"addr": addr, "hb_ms": now_ms, "lease_epoch": lease_epoch}))
    {
        let _ = std::fs::write(path, s); // best-effort; replace-if-newer guards the cross-incarnation race
    }
}

// L6: the lease_epoch currently recorded in the registry for `region` (0 / None if absent or legacy).
fn registry_lease_epoch(reg_dir: &str, region: &str) -> Option<u64> {
    registry_read(reg_dir)
        .get(region)
        .and_then(|e| e.get("lease_epoch"))
        .and_then(|v| v.as_u64())
}

// L2: a mesh connect-loop that resolves the neighbour addr from the registry each retry (follows B -> B').
// Mirrors the static GW_MESH connect-loop body but with dynamic addr resolution. The 2-phase handoff +
// pending_mesh resend (unchanged) deliver across the gap; this just keeps the LINK pointed at the live node.
fn spawn_mesh_link_dynamic(st: Arc<Mutex<ServerState>>, region: String, reg_dir: String) {
    tokio::spawn(async move {
        loop {
            let addr = registry_read(&reg_dir)
                .get(&region)
                .and_then(|e| e.get("addr"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            if let Some(addr) = addr {
                if let Ok(stream) = tokio::net::TcpStream::connect(&addr).await {
                    stream.set_nodelay(true).ok();
                    let (mut rd, mut wr) = stream.into_split();
                    let token = {
                        let s = st.lock().await;
                        auth_token_for_region(&s, "MESH")
                    };
                    let hs = worker_connect_frame(
                        &format!("mesh-link-{region}"),
                        "MESH",
                        token.as_deref(),
                    );
                    if wr.write_all(&hs).await.is_ok() {
                        let (tx, mut rx) = mpsc::unbounded_channel::<Vec<u8>>();
                        st.lock().await.mesh.insert(region.clone(), tx);
                        println!(
                            "[mesh] DISCOVERED + linked region {region} @ {addr} (from registry)"
                        );
                        let st_ack = st.clone();
                        tokio::spawn(async move {
                            while let Some(f) = read_frame(&mut rd).await {
                                if f.value.get("op").and_then(|v| v.as_str()) == Some("MeshAck") {
                                    if let Some(eid) =
                                        f.value.get("entity").and_then(|v| v.as_str())
                                    {
                                        let mut s = st_ack.lock().await;
                                        record_mesh_ack(&mut s, eid);
                                    }
                                }
                            }
                        });
                        while let Some(buf) = rx.recv().await {
                            if wr.write_all(&buf).await.is_err() {
                                break; // link dropped -> fall through, re-resolve addr (follows a moved node)
                            }
                        }
                        st.lock().await.mesh.remove(&region);
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    });
}

#[tokio::main]
async fn main() {
    let port: u16 = std::env::var("GW_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(7777);
    let lease_ttl: f64 = std::env::var("GW_LEASE_TTL")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(30.0);
    let threshold_ttl_ms: u64 = std::env::var("GW_THRESHOLD_TTL_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(30_000);

    let mut state = ServerState::new(lease_ttl);
    state.threshold_ttl = Duration::from_millis(threshold_ttl_ms);

    // durability: recover the EXACT pre-crash store from the WAL alone, then continue logging
    if let Ok(path) = std::env::var("GW_WAL") {
        // G2: GW_RESTORE_OFFSET=<bytes> rolls the world back to a snapshot cut (else full recovery)
        let restore_offset = std::env::var("GW_RESTORE_OFFSET")
            .ok()
            .and_then(|s| s.parse::<u64>().ok());
        let (
            store,
            tombstones,
            topology,
            recovered_pending,
            recovered_forwarded,
            recovered_id_hwm,
            report,
        ) = recover_from_wal_report(&path, restore_offset);

        // #2 RESTORE DRY-RUN: GW_RESTORE_DRYRUN=1 validates a WAL (version / corrupt-tail / content hash /
        // entity_count) and EXITS WITHOUT serving, so automation can vet a WAL before booting the broker on it.
        if std::env::var("GW_RESTORE_DRYRUN").is_ok() {
            let dry = json!({
                "dry_run": true,
                "wal": path,
                "entity_count": store.len(),
                "store_hash": format!("{:016x}", store_content_hash(&store)),
                "wal_version": report.wal_version,
                "selected_event_count": report.selected_event_count,
                "decoded_record_count": report.decoded_record_count,
                "corrupt_tail_record_count": report.corrupt_tail_record_count,
                "truncated_tail_bytes": report.truncated_tail_bytes,
                "recoverable_prefix_bytes": report.recoverable_prefix_bytes,
                "unknown_kind_count": report.unknown_kind_count,
                "kind_counts": report.kind_counts,
                "unknown_kinds": report.unknown_kinds,
                "error": report.error,
            });
            println!("{}", serde_json::to_string(&dry).unwrap());
            // refuse => non-zero exit so a script can gate on it; clean => 0.
            std::process::exit(if report.error.is_some() { 2 } else { 0 });
        }

        // not a dry-run: a refuse (mid-corruption / unknown version) must NOT serve partial state -> fail closed.
        if let Some(err) = report.error {
            eprintln!("[rust-broker] WAL recovery REFUSED — not serving: {}", err);
            std::process::exit(2);
        }
        if let Err(err) = truncate_wal_tail_to_recoverable_prefix(&path, &report) {
            eprintln!("[rust-broker] WAL recovery REFUSED — not serving: {}", err);
            std::process::exit(2);
        }
        let n = store.len();
        state.entities = store;
        state.deleted_entities = tombstones;
        state.mesh_forwarded_epoch = recovered_forwarded;
        state.entity_id_reservations = recovered_id_hwm;
        // R0.2: restore the partition topology (boundary/splits/mesh) so the router matches recovered placement.
        if let Some(pc) = topology {
            if let Some(b) = pc.get("boundary").and_then(|v| v.as_f64()) {
                state.boundary = b;
            }
            // N-ZONE: restore the full strip cut list so the router rebuilds the SAME N-way assignment. A WAL
            // predating N-zone has no `boundaries` field -> fall back to the single restored `boundary` (the
            // 1-element W|E list), so old WALs recover byte-for-byte.
            if let Some(bs) = pc.get("boundaries").and_then(|v| v.as_array()) {
                let v: Vec<f64> = bs.iter().filter_map(|x| x.as_f64()).collect();
                if !v.is_empty() {
                    state.boundaries = v;
                }
            } else {
                state.boundaries = vec![state.boundary];
            }
            if let Some(splits) = pc.get("splits").and_then(|v| v.as_object()) {
                state.splits = splits
                    .iter()
                    .filter_map(|(r, v)| {
                        v.as_array()
                            .map(|a| (r.clone(), a.iter().filter_map(|x| x.as_f64()).collect()))
                    })
                    .collect();
            }
            if let Some(mesh) = pc.get("mesh_regions").and_then(|v| v.as_array()) {
                state.mesh_regions = mesh
                    .iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect();
            }
            state.zone_topology_rev = pc.get("version").and_then(|v| v.as_u64()).unwrap_or(0);
            println!("[rust-broker] R0.2: restored partition_config boundary={} boundaries={:?} splits={} mesh={}",
                state.boundary, state.boundaries, state.splits.len(), state.mesh_regions.len());
        }
        // G2.1d: an in-flight handoff (mesh_out without mesh_acked by the cut) is "in the channel" -> rebuild
        // pending_mesh from the full mesh_out payload so startup/link-ready resends it and the target adopts
        // it exactly once (the receiver adopt is idempotent). Closes the wire-transit snapshot-loss window.
        for (eid, payload) in recovered_pending {
            let target = payload
                .get("target")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let handoff = json!({"op":"MeshHandoff","entity":eid.clone(),"target":target.clone(),
                "pos":payload.get("pos").cloned().unwrap_or(Value::Null),
                "vel":payload.get("vel").cloned().unwrap_or(Value::Null),
                "authority_epoch":payload.get("authority_epoch").cloned().unwrap_or(Value::Null),
                "authority":payload.get("authority").cloned().unwrap_or(Value::Null),
                "src_region":payload.get("src_region").cloned().unwrap_or(Value::Null),
                "lease_epoch":payload.get("lease_epoch").cloned().unwrap_or(Value::Null),
                "components":payload.get("components").cloned().unwrap_or(Value::Null)});
            state
                .pending_mesh
                .insert(eid, (handoff, Instant::now(), target));
        }
        if !state.pending_mesh.is_empty() {
            println!("[rust-broker] G2.1d: restored {} in-flight handoff(s) to pending_mesh -> will resend",
                state.pending_mesh.len());
        }
        let f = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .expect("WAL open failed");
        // #2: seed wal_bytes from the ACTUAL on-disk size so the compaction threshold + GW_RESTORE_OFFSET stay
        // consistent AND ensure_wal_header correctly no-ops on a populated WAL (only a truly-empty file is
        // header-stamped). Previously wal_bytes started at 0 even on a recovered (non-empty) WAL.
        state.wal_bytes = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
        state.wal = Some(f);
        state.wal_path = path.clone(); // R0.3: the tick uses this to rewrite/rename the WAL on compaction
        state.ensure_wal_header(); // stamp the v1 version header iff this is a brand-new (empty) WAL
        println!(
            "[rust-broker] WAL {path}: recovered {n} entities from the log (durable-recovery fix); wal_version={}, on-disk={} bytes; compaction threshold={} bytes (0=off)",
            report.wal_version, state.wal_bytes, state.wal_compact_bytes
        );
    }

    // #2 CROSS-MACHINE: the bind ADDRESS is config-driven (GW_BIND), default "127.0.0.1" so every existing
    // localhost gate is byte-for-byte unchanged. The DIAL side was ALREADY host-agnostic -- GW_MESH="E=host:port"
    // and the registry `addr` both feed TcpStream::connect(&addr), which takes any host:port (incl. a remote IP /
    // DNS name). The one thing pinned to loopback was the LISTENER: bind(("127.0.0.1", port)) means a broker on
    // machine A literally cannot accept a connection from machine B, so a cross-host mesh dial would refuse. Set
    // GW_BIND=0.0.0.0 (all interfaces) -- or a specific NIC IP -- to let real remote peers reach this listener.
    // Note: this is a config VALUE consumed exactly once at bind (no flag in any hot path, no second code branch);
    // GW_MESH stays THE neighbour-address config (region=host:port), so there is no duplicate "GW_NEIGHBORS" knob
    // -- the cross-host mesh is "dial GW_MESH host:port (already works) + listen on GW_BIND so the peer can dial back".
    let bind_host = std::env::var("GW_BIND").unwrap_or_else(|_| "127.0.0.1".to_string());
    let listener = TcpListener::bind((bind_host.as_str(), port))
        .await
        .unwrap_or_else(|e| panic!("bind {bind_host}:{port} failed: {e}"));
    println!(
        "[rust-broker] Godworks OS data-plane live @ {bind_host}:{port} (Rust/tokio; full reference-parity; lease_ttl={lease_ttl})"
    );

    let state = Arc::new(Mutex::new(state));

    // DurableTransition watermark tick: all single/batch component writes append WAL lines without fsync,
    // then this one barrier advances durable_gen and publishes the group. This is the general law behind
    // BatchUpdate's old special-case group commit, without per-op disk stalls under game-rate writes.
    {
        let st = state.clone();
        let flush_ms = std::env::var("GW_DURABLE_FLUSH_MS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .filter(|ms| *ms > 0)
            .unwrap_or(16);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_millis(flush_ms));
            loop {
                tick.tick().await;
                let mut s = st.lock().await;
                let _t = Instant::now();
                flush_pending_updates(&mut s);
                record_lock_hold(&mut s, "durable_flush", _t.elapsed());
                reap_disconnecting(&mut s);
            }
        });
    }

    // liveness monitor (failover) + dynamic load-balancing monitor on the broker's clock
    {
        let st = state.clone();
        tokio::spawn(async move {
            let nominal = Duration::from_millis(300);
            let mut tick = tokio::time::interval(nominal);
            let mut last_wake = Instant::now();
            loop {
                tick.tick().await;
                // #3 metrics: tick-lag = how much LATER this tick fired than the 300ms schedule (the broker's
                // saturation signal -- a starved/locked-up broker can't keep its monitor cadence). >= 0; ~0 healthy.
                let elapsed = last_wake.elapsed();
                last_wake = Instant::now();
                let lag = elapsed.as_secs_f64() * 1000.0 - nominal.as_secs_f64() * 1000.0;
                let mut s = st.lock().await;
                s.monitor_last_tick_ms = now_millis();
                s.tick_lag_ms = if lag > 0.0 { lag } else { 0.0 };
                let _t = Instant::now();
                check_leases(&mut s);
                record_lock_hold(&mut s, "check_leases", _t.elapsed());
                let _t = Instant::now();
                rebalance(&mut s);
                record_lock_hold(&mut s, "rebalance", _t.elapsed());
                let _t = Instant::now();
                rebalance_2d(&mut s);
                record_lock_hold(&mut s, "rebalance_2d", _t.elapsed());
                let _t = Instant::now();
                maybe_split(&mut s);
                record_lock_hold(&mut s, "maybe_split", _t.elapsed());
                let _t = Instant::now();
                process_rebalance_jobs(&mut s);
                record_lock_hold(&mut s, "rebalance_jobs", _t.elapsed());
                resend_pending_mesh(&mut s); // re-deliver any cross-broker handoff the neighbour hasn't ACK'd
                push_border_ghosts(&mut s); // CROSS-BROKER SEAM-INTEREST: project this broker's near-seam entities to the meshed neighbour(s) as read-only ghosts
                reap_stale_ghosts(&mut s); // drop ghosts whose source stopped refreshing (left the band / link dropped)
                gc_threshold_timeouts(&mut s);
                update_load_governor(&mut s); // L3: derive load_level from the egress backlog -> degradation keys off it
                                              // #3 GRACEFUL-DRAIN progress: while draining, keep handing owned entities to neighbours (a body that
                                              // arrived/was-created-pre-drain or whose first hand-off raced a momentarily-down link gets swept up
                                              // here). When BOTH entities and pending_mesh are empty, every owned entity has been ACK'd onto a
                                              // neighbour (conservation-exact via the mesh path) -> the drain is COMPLETE; exit 0 if drain_exit so a
                                              // rolling deploy proceeds. drain_exit=false (a test) leaves it alive to be asserted against.
                if s.draining {
                    if !s.entities.is_empty() {
                        let _ = drain_handoff_owned(&mut s);
                    }
                    if s.entities.is_empty()
                        && s.pending_mesh.is_empty()
                        && !s.mesh.is_empty()
                        && s.drain_exit
                    {
                        println!("[drain] COMPLETE -- all entities handed off + ACK'd by neighbour(s); exiting 0 (clean rolling-deploy shutdown)");
                        std::process::exit(0);
                    }
                }
                // R0.3: bound the WAL on disk. Runs under THIS lock (no concurrent writer -> no torn-write race);
                // a no-op until wal_bytes exceeds the threshold, so the per-tick cost is one comparison.
                let _t = Instant::now();
                s.maybe_compact_wal();
                record_lock_hold(&mut s, "wal_compact", _t.elapsed());
            }
        });
    }

    // L1 event-storm: a fast event-flush tick (20Hz) coalesces the buffered EntityEvents (critical=all,
    // visual=one-per-coalesce_key with a count, debug=dropped) so a 1000-event burst delivers bounded output.
    {
        let st = state.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_millis(50));
            loop {
                tick.tick().await;
                let mut s = st.lock().await;
                flush_events(&mut s);
            }
        });
    }

    // OWNED-REGION IDENTITY from GW_ADVERTISE="REGION=host:port[@epoch]", parsed UNCONDITIONALLY at startup
    // (NOT only inside the GW_REGISTRY discovery block below). A broker's advertised region IS the zone it
    // owns, registry or no registry -- the static-GW_MESH topology (the advertise-topology's static
    // peer) advertises a region but configures no registry, so my_region stayed EMPTY there and an inbound
    // mesh adopt fell back to a POSITION-derive against the local W|E boundary -> the fold-into-ZB
    // region mislabel ("W"/"E" instead of the owned zone). Setting my_region here makes receiving_region_for_adopt
    // label an adopted entity with this broker's OWN zone. If GW_REGISTRY is also set, the block below re-sets
    // my_region (identical value) + seeds the lease_epoch; ADDITIVE: no GW_ADVERTISE => my_region stays empty
    // (mesh_soak / nzone) => adopt falls to the target-owned / position path exactly as before.
    if state.lock().await.my_region.is_empty() {
        if let Ok(advertise) = std::env::var("GW_ADVERTISE") {
            if let Some((region, _rest)) = advertise.split_once('=') {
                let region = region.trim().to_string();
                if !region.is_empty() {
                    state.lock().await.my_region = region;
                }
            }
        }
    }

    // CROSS-BROKER MESH: connect (with retry) to each configured NEIGHBOUR broker, so entities crossing
    // into a remote zone are handed across the PROCESS boundary to the broker owning it. Config:
    // GW_MESH="E=host:port,MARS=host:port,..." (region=addr pairs); GW_MESH_EAST=addr is the legacy
    // shorthand for "E=addr". N-neighbour = the open-universe-of-planets backbone (planet = zone-broker).
    let mut mesh_cfg: Vec<(String, String)> = Vec::new();
    if let Ok(addr) = std::env::var("GW_MESH_EAST") {
        mesh_cfg.push(("E".to_string(), addr));
    }
    if let Ok(spec) = std::env::var("GW_MESH") {
        for pair in spec.split(',') {
            if let Some((r, a)) = pair.split_once('=') {
                let (r, a) = (r.trim().to_string(), a.trim().to_string());
                if !r.is_empty() && !a.is_empty() {
                    mesh_cfg.push((r, a));
                }
            }
        }
    }
    for (region, addr) in mesh_cfg {
        state.lock().await.mesh_regions.insert(region.clone()); // route region-bound entities to the mesh even while the link reconnects
        let st = state.clone();
        tokio::spawn(async move {
            loop {
                if let Ok(stream) = tokio::net::TcpStream::connect(&addr).await {
                    stream.set_nodelay(true).ok();
                    let (mut rd, mut wr) = stream.into_split();
                    let token = {
                        let s = st.lock().await;
                        auth_token_for_region(&s, "MESH")
                    };
                    let hs = worker_connect_frame(
                        &format!("mesh-link-{region}"),
                        "MESH",
                        token.as_deref(),
                    );
                    if wr.write_all(&hs).await.is_ok() {
                        let (tx, mut rx) = mpsc::unbounded_channel::<Vec<u8>>();
                        {
                            st.lock().await.mesh.insert(region.clone(), tx);
                        }
                        println!("[mesh] linked to neighbour broker for region {region} @ {addr}");
                        let st_ack = st.clone();
                        tokio::spawn(async move {
                            // the only inbound on our OUTBOUND mesh link is the neighbour's MeshAck ->
                            // release the parked entity from pending_mesh (the 2-phase handoff confirmed).
                            while let Some(f) = read_frame(&mut rd).await {
                                if f.value.get("op").and_then(|v| v.as_str()) == Some("MeshAck") {
                                    if let Some(eid) =
                                        f.value.get("entity").and_then(|v| v.as_str())
                                    {
                                        let mut s = st_ack.lock().await;
                                        // B2 recovery: record the confirmed departure so a crash-restart
                                        // does not resurrect an entity that already landed on the neighbour.
                                        record_mesh_ack(&mut s, eid);
                                    }
                                }
                            }
                        });
                        while let Some(buf) = rx.recv().await {
                            if wr.write_all(&buf).await.is_err() {
                                break;
                            }
                        }
                        st.lock().await.mesh.remove(&region);
                    }
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        });
    }

    // L2 service-discovery: if GW_REGISTRY (a dir) is set, self-register + heartbeat this broker's advertised
    // region (GW_ADVERTISE="E=host:port") on a 1s tick, and DISCOVER neighbour brokers from the registry --
    // spawning a dynamic-addr mesh link for each FRESH region != self. Replaces the static GW_MESH at scale;
    // a replaced node is followed automatically. The static GW_MESH path above still works (back-compat).
    if let Ok(reg_dir) = std::env::var("GW_REGISTRY") {
        let advertise = std::env::var("GW_ADVERTISE").unwrap_or_default();
        // L6 partition proxy (test hook): while GW_REG_FREEZE_FILE exists, this broker performs NO registry I/O
        // (no read, no write) -- a faithful "partitioned from the shared registry store" model: it keeps
        // SERVING with its last-known lease_epoch, cannot see it has been superseded, and stops advertising.
        // Removing the file = the partition HEALS -> it resumes registry I/O and self-fences on seeing a higher
        // epoch. Unset in production (no freeze) -> a plain no-op.
        let freeze_file = std::env::var("GW_REG_FREEZE_FILE").unwrap_or_default();
        let _ = std::fs::create_dir_all(&reg_dir);
        // Parse "REGION=host:port[@epoch]". The optional @epoch lets a test pin the spec's 10 / 11; absent,
        // the own epoch is the registry's CURRENT epoch for this region + 1 (so a takeover incarnation
        // strictly out-epochs the last one) or 1 if the region is unclaimed.
        let (my_region, my_addr, explicit_epoch) = {
            if let Some((region, rest)) = advertise.split_once('=') {
                let region = region.trim().to_string();
                let (addr, epoch) = match rest.rsplit_once('@') {
                    Some((a, ep)) => (a.trim().to_string(), ep.trim().parse::<u64>().ok()),
                    None => (rest.trim().to_string(), None),
                };
                (region, addr, epoch)
            } else {
                (String::new(), String::new(), None)
            }
        };
        // Establish THIS incarnation's lease_epoch for its own region, monotonically above whatever the
        // registry currently shows, then publish it into state (mesh_forward stamps outbound handoffs with it).
        let my_epoch = if my_region.is_empty() {
            0
        } else {
            explicit_epoch.unwrap_or_else(|| {
                registry_lease_epoch(&reg_dir, &my_region)
                    .map(|e| e + 1)
                    .unwrap_or(1)
            })
        };
        if !my_region.is_empty() {
            let mut s = state.lock().await;
            s.my_region = my_region.clone();
            s.region_lease_epoch.insert(my_region.clone(), my_epoch);
        }
        let st = state.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_millis(1000));
            loop {
                tick.tick().await;
                // partition proxy: while frozen, do NO registry I/O at all (isolated from the store)
                if !freeze_file.is_empty() && std::path::Path::new(&freeze_file).exists() {
                    continue;
                }
                let now = now_millis();
                if !my_region.is_empty() && !my_addr.is_empty() {
                    registry_heartbeat(&reg_dir, &my_region, &my_addr, now, my_epoch);
                    // heartbeat self + own epoch
                }
                for (region, entry) in registry_read(&reg_dir) {
                    let hb = entry.get("hb_ms").and_then(|v| v.as_u64()).unwrap_or(0);
                    let entry_epoch = entry.get("lease_epoch").and_then(|v| v.as_u64());
                    let entry_addr = entry
                        .get("addr")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string());
                    if region == my_region {
                        // L6 SELF-FENCE: a STRICTLY HIGHER epoch for MY region held by a DIFFERENT addr means a
                        // newer incarnation took over -> I am the stale owner. Mark superseded so mesh_forward
                        // stops emitting ownership traffic for it. (Same addr at a higher epoch = me restarted,
                        // not a supersession.)
                        if let Some(ep) = entry_epoch {
                            let other_addr =
                                entry_addr.as_deref().map(|a| a != my_addr).unwrap_or(false);
                            if ep > my_epoch && other_addr {
                                let mut s = st.lock().await;
                                if s.superseded_regions.insert(my_region.clone()) {
                                    eprintln!(
                                        "[fence] region '{my_region}' SUPERSEDED: registry shows lease_epoch={ep} (> mine {my_epoch}) at a different addr -- self-fencing this incarnation"
                                    );
                                }
                            }
                        }
                        continue;
                    }
                    if now.saturating_sub(hb) < 5000 {
                        // L6: learn the peer region's CURRENT lease_epoch (monotonic max) so an inbound stale
                        // handoff from a fenced incarnation of THAT region can be rejected on receipt here.
                        if let Some(ep) = entry_epoch {
                            let mut s = st.lock().await;
                            let cur = s.region_lease_epoch.get(&region).copied().unwrap_or(0);
                            if ep > cur {
                                s.region_lease_epoch.insert(region.clone(), ep);
                            }
                        }
                        // Spawn the dynamic mesh-link iff no link TASK is already running for this region THIS
                        // lifetime (mesh_link_spawned), NOT iff mesh_regions lacks it. A WAL recovery restores
                        // mesh_regions (so routing knows W is remote) but the OLD link task died in the crash --
                        // gating on mesh_regions made a recovered broker believe it was linked while it had no
                        // live link, so its recovered in-flight handoffs never resent (the churn pending-leak).
                        // mesh_link_spawned starts empty every boot -> a recovered broker re-spawns the link once;
                        // the task itself loops forever (reconnect-on-drop), so one spawn per region is enough.
                        let need_spawn = {
                            let mut s = st.lock().await;
                            s.mesh_regions.insert(region.clone()); // routing: W is a remote meshed region
                            if s.mesh_link_spawned.contains(&region) {
                                false
                            } else {
                                s.mesh_link_spawned.insert(region.clone());
                                true
                            }
                        };
                        if need_spawn {
                            spawn_mesh_link_dynamic(st.clone(), region.clone(), reg_dir.clone());
                        }
                    }
                }
            }
        });
    }

    loop {
        match listener.accept().await {
            Ok((sock, _)) => {
                let st = state.clone();
                tokio::spawn(handle_conn(sock, st));
            }
            Err(_) => continue,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_entity(pos: [f64; 2], region: &str) -> Entity {
        let components = Map::new();
        Entity {
            pos,
            vel: [1.0, 0.0],
            authority: initial_authority_map(&components, 1),
            components,
            region: region.to_string(),
            version: 1,
            last_broadcast_cell: Some(interest_cell_of(pos)),
        }
    }

    fn expected_snapshot_authority_hash_for_test(entities: &HashMap<String, Entity>) -> u64 {
        let mut ids: Vec<String> = entities.keys().cloned().collect();
        ids.sort();
        let mut h: u64 = 0xcbf29ce484222325;
        for eid in &ids {
            for b in eid.bytes() {
                h = (h ^ b as u64).wrapping_mul(0x100000001b3);
            }
            let Some(entity) = entities.get(eid) else {
                continue;
            };
            let Some(ca) = entity.authority.get("pos") else {
                continue;
            };
            h = h.wrapping_add(ca.epoch);
            if let Some(owner) = &ca.owner {
                for b in owner.bytes() {
                    h = (h ^ b as u64).wrapping_mul(0x100000001b3);
                }
            }
        }
        h
    }

    fn test_physics_island_entity(pos: [f64; 2], region: &str) -> Entity {
        let mut components = Map::new();
        components.insert("rot".to_string(), json!(0.25));
        components.insert("lin".to_string(), json!([1.0, 0.0]));
        components.insert("ang".to_string(), json!(0.125));
        components.insert("at_rest".to_string(), json!(false));
        Entity {
            pos,
            vel: [1.0, 0.0],
            authority: initial_authority_map(&components, 1),
            components,
            region: region.to_string(),
            version: 1,
            last_broadcast_cell: Some(interest_cell_of(pos)),
        }
    }

    fn expanded_physics_island_components() -> Vec<&'static str> {
        vec!["ang", "at_rest", "lin", "pos", "rot", "vel"]
    }

    fn stamp_expanded_physics_island_epochs(e: &mut Entity) -> HashMap<String, u64> {
        let epochs = [
            ("ang", 3),
            ("at_rest", 5),
            ("lin", 7),
            ("pos", 11),
            ("rot", 13),
            ("vel", 17),
        ];
        let mut out = HashMap::new();
        for (comp, epoch) in epochs {
            set_component_authority_epoch(e, comp, epoch);
            out.insert(comp.to_string(), epoch);
        }
        out
    }

    fn physics_island_update_value(comp: &str) -> Value {
        match comp {
            "ang" => json!(0.75),
            "at_rest" => json!(true),
            "lin" => json!([2.0, 0.0]),
            "pos" => json!([3.0, 0.0]),
            "rot" => json!(0.5),
            "vel" => json!([0.0, 2.0]),
            other => panic!("unexpected physics-island comp {other}"),
        }
    }

    fn add_test_worker(state: &mut ServerState, wid: &str, region: &str) {
        let (tx, _rx) = mpsc::channel(CHANNEL_CAP);
        let ingress_tokens = state.ingress_burst_frames.max(1.0);
        state.workers.insert(
            wid.to_string(),
            WorkerHandle {
                region: region.to_string(),
                role: peer_role_for(region, &HashSet::new()),
                attributes: HashSet::new(),
                view: HashSet::new(),
                authority_epochs: HashMap::new(),
                aoi_center: None,
                aoi_radius: None,
                fidelity_full_radius: None,
                fidelity_coarse_rate: 1,
                fidelity_coarse_grid: 0.0,
                fidelity_seq: HashMap::new(),
                tx,
                out_queue: Arc::new(AtomicU64::new(0)),
                dropped: Arc::new(AtomicU64::new(0)),
                disconnect: Arc::new(AtomicBool::new(false)),
                grid_cells: Vec::new(),
                ingress_tokens,
                ingress_last_refill: Instant::now(),
                ingress_rejected: 0,
            },
        );
        state
            .region_worker
            .insert(region.to_string(), wid.to_string());
    }

    fn add_test_worker_with_rx(
        state: &mut ServerState,
        wid: &str,
        region: &str,
    ) -> mpsc::Receiver<Vec<u8>> {
        let (tx, rx) = mpsc::channel(CHANNEL_CAP);
        let ingress_tokens = state.ingress_burst_frames.max(1.0);
        state.workers.insert(
            wid.to_string(),
            WorkerHandle {
                region: region.to_string(),
                role: peer_role_for(region, &HashSet::new()),
                attributes: HashSet::new(),
                view: HashSet::new(),
                authority_epochs: HashMap::new(),
                aoi_center: None,
                aoi_radius: None,
                fidelity_full_radius: None,
                fidelity_coarse_rate: 1,
                fidelity_coarse_grid: 0.0,
                fidelity_seq: HashMap::new(),
                tx,
                out_queue: Arc::new(AtomicU64::new(0)),
                dropped: Arc::new(AtomicU64::new(0)),
                disconnect: Arc::new(AtomicBool::new(false)),
                grid_cells: Vec::new(),
                ingress_tokens,
                ingress_last_refill: Instant::now(),
                ingress_rejected: 0,
            },
        );
        state
            .region_worker
            .insert(region.to_string(), wid.to_string());
        rx
    }

    fn add_test_peer_with_rx(
        state: &mut ServerState,
        wid: &str,
        region: &str,
        attributes: HashSet<String>,
    ) -> mpsc::Receiver<Vec<u8>> {
        let (tx, rx) = mpsc::channel(CHANNEL_CAP);
        let ingress_tokens = state.ingress_burst_frames.max(1.0);
        let role = peer_role_for(region, &attributes);
        state.workers.insert(
            wid.to_string(),
            WorkerHandle {
                region: region.to_string(),
                role,
                attributes,
                view: HashSet::new(),
                authority_epochs: HashMap::new(),
                aoi_center: None,
                aoi_radius: None,
                fidelity_full_radius: None,
                fidelity_coarse_rate: 1,
                fidelity_coarse_grid: 0.0,
                fidelity_seq: HashMap::new(),
                tx,
                out_queue: Arc::new(AtomicU64::new(0)),
                dropped: Arc::new(AtomicU64::new(0)),
                disconnect: Arc::new(AtomicBool::new(false)),
                grid_cells: Vec::new(),
                ingress_tokens,
                ingress_last_refill: Instant::now(),
                ingress_rejected: 0,
            },
        );
        if role == PeerRole::Worker {
            state
                .region_worker
                .insert(region.to_string(), wid.to_string());
        }
        update_interest_grid(state, wid);
        rx
    }

    fn replay_tape_path(name: &str) -> String {
        let unique = format!("{}_{}_{}.jsonl", name, std::process::id(), now_millis());
        std::env::temp_dir()
            .join(unique)
            .to_string_lossy()
            .to_string()
    }

    fn install_replay_tape(state: &mut ServerState, path: &str) {
        state.replay_tape = Some(ReplayTape::open_path(path, 128).unwrap());
    }

    fn wait_for_tape(path: &str, min_lines: usize) -> Vec<Value> {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let content = std::fs::read_to_string(path).unwrap_or_default();
            let lines: Vec<Value> = content
                .lines()
                .filter_map(|line| serde_json::from_str::<Value>(line).ok())
                .collect();
            if lines.len() >= min_lines || Instant::now() >= deadline {
                return lines;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn seed_2d_rebalance_state(state: &mut ServerState) -> Vec<String> {
        state.grid2d = Some((2, 2, 10.0, 10.0));
        add_test_worker(state, "hot", "Z0_0");
        add_test_worker(state, "cold", "Z1_0");
        state
            .region_worker
            .insert("Z0_1".to_string(), "hot".to_string());

        let mut eids = Vec::new();
        for i in 0..4 {
            let eid = format!("a{i}");
            assert!(spawn_in_region(
                state,
                &eid,
                [1.0, 1.0 + i as f64],
                [0.0, 0.0],
                Map::new(),
                None,
                SpawnAuthoritySeed::default(),
            ));
            grant_region_physics_island_authority(state, "hot", &eid);
            eids.push(eid);
        }
        for i in 0..4 {
            let eid = format!("b{i}");
            assert!(spawn_in_region(
                state,
                &eid,
                [1.0, 11.0 + i as f64],
                [0.0, 0.0],
                Map::new(),
                None,
                SpawnAuthoritySeed::default(),
            ));
            grant_region_physics_island_authority(state, "hot", &eid);
            eids.push(eid);
        }
        eids
    }

    #[test]
    fn maybe_split_ignores_non_finite_positions_without_panicking() {
        let mut state = ServerState::new(30.0);
        add_test_worker(&mut state, "hot", "W");
        add_test_worker(&mut state, "standby", "SPARE");
        state.region_worker.remove("SPARE");
        state.standbys.push("standby".to_string());
        state.worker_load.insert("hot".to_string(), 1.0);
        state
            .entities
            .insert("nan".to_string(), test_entity([f64::NAN, 0.0], "W"));
        state
            .entities
            .insert("left".to_string(), test_entity([-4.0, 0.0], "W"));
        state
            .entities
            .insert("right".to_string(), test_entity([4.0, 0.0], "W"));

        maybe_split(&mut state);

        let splits = state.splits.get("W").expect("hot region should split");
        assert_eq!(splits.len(), 1);
        assert!(
            splits[0].is_finite(),
            "split boundary must come from finite entity positions"
        );
        assert_eq!(
            state.rebalance_jobs.len(),
            1,
            "finite entities still produce the budgeted split migration"
        );
    }

    #[test]
    fn grid2d_rejects_non_positive_or_nan_arena() {
        assert!(parse_grid2d_values("2x2", Some("0,100")).is_none());
        assert!(parse_grid2d_values("2x2", Some("NaN,100")).is_none());
        assert_eq!(
            parse_grid2d_values("2x2", Some("200,100")),
            Some((2, 2, 100.0, 50.0))
        );
    }

    #[test]
    fn replay_tape_disabled_by_default() {
        let old = std::env::var_os("GW_REPLAY_TAPE");
        std::env::remove_var("GW_REPLAY_TAPE");
        let state = ServerState::new(30.0);
        assert!(state.replay_tape.is_none());
        if let Some(old) = old {
            std::env::set_var("GW_REPLAY_TAPE", old);
        }
    }

    #[test]
    fn replay_tape_never_records_auth_token_or_component_body() {
        let mut state = ServerState::new(30.0);
        let path = replay_tape_path("gw_replay_redaction");
        install_replay_tape(&mut state, &path);
        let _rx = add_test_peer_with_rx(
            &mut state,
            "client-1",
            "CLIENT",
            HashSet::from(["role.client".to_string(), "player.alice".to_string()]),
        );
        let sentinel = "SECRET_COMPONENT_BODY_SHOULD_NOT_APPEAR";
        let frame = json!({
            "op":"CreateEntity",
            "request_id":"create-secret",
            "entity":"client-forged",
            "auth_token":"super-secret-token",
            "components":{"profile": sentinel, "pos":[1.0,2.0]}
        });

        dispatch_test_frame(&mut state, "client-1", &frame);

        let _lines = wait_for_tape(&path, 2);
        let content = std::fs::read_to_string(&path).unwrap_or_default();
        assert!(!content.contains("super-secret-token"));
        assert!(!content.contains("auth_token"));
        assert!(!content.contains(sentinel));
        assert!(content.contains("credential_present"));
        assert!(content.contains("components_bytes"));
    }

    #[test]
    fn replay_tape_large_payload_records_size_not_payload() {
        let mut state = ServerState::new(30.0);
        let path = replay_tape_path("gw_replay_large_payload");
        install_replay_tape(&mut state, &path);
        let _rx = add_test_peer_with_rx(
            &mut state,
            "client-1",
            "CLIENT",
            HashSet::from(["role.client".to_string(), "player.alice".to_string()]),
        );
        let sentinel = format!("LARGE_PAYLOAD_SENTINEL_{}", "x".repeat(2048));
        let frame = json!({
            "op":"UpdateComponent",
            "entity":"missing-entity",
            "comp":"bio",
            "value": sentinel
        });

        dispatch_test_frame(&mut state, "client-1", &frame);

        let _lines = wait_for_tape(&path, 2);
        let content = std::fs::read_to_string(&path).unwrap_or_default();
        assert!(!content.contains("LARGE_PAYLOAD_SENTINEL"));
        assert!(content.contains("value_bytes"));
        assert!(content.contains("\"outcome\":\"dispatched\""));
    }

    #[test]
    fn replay_tape_role_policy_reject_matches_frame_reason() {
        let mut state = ServerState::new(30.0);
        let path = replay_tape_path("gw_replay_role_policy");
        install_replay_tape(&mut state, &path);
        let _rx = add_test_peer_with_rx(
            &mut state,
            "client-1",
            "CLIENT",
            HashSet::from(["role.client".to_string(), "player.alice".to_string()]),
        );

        dispatch_test_frame(
            &mut state,
            "client-1",
            &json!({"op":"CreateEntity","request_id":"c1","entity":"bad"}),
        );

        let lines = wait_for_tape(&path, 2);
        assert!(lines.iter().any(|event| {
            event.get("kind").and_then(Value::as_str) == Some("broker_ingress")
                && event.get("outcome").and_then(Value::as_str) == Some("rejected")
                && event.get("reason").and_then(Value::as_str) == Some("role_policy_error")
                && event
                    .get("op_summary")
                    .and_then(|summary| summary.get("op"))
                    .and_then(Value::as_str)
                    == Some("CreateEntity")
                && event
                    .get("op_summary")
                    .and_then(|summary| summary.get("persistence"))
                    .and_then(Value::as_str)
                    == Some("persistent")
                && event
                    .get("op_summary")
                    .and_then(|summary| summary.get("category"))
                    .and_then(Value::as_str)
                    == Some("entity_lifecycle")
                && event
                    .get("op_summary")
                    .and_then(|summary| summary.get("response_op"))
                    .and_then(Value::as_str)
                    == Some("CreateEntityResponse")
        }));
        assert!(lines.iter().any(|event| {
            event.get("kind").and_then(Value::as_str) == Some("broker_outbound")
                && event.get("op").and_then(Value::as_str) == Some("UpdateRejected")
                && event.get("error").and_then(Value::as_str) == Some("role_policy_error")
                && event.get("rejected_op").and_then(Value::as_str) == Some("CreateEntity")
                && event.get("peer_role").and_then(Value::as_str) == Some("client")
        }));
    }

    #[test]
    fn replay_tape_handoff_authority_change_records_epoch() {
        let mut state = ServerState::new(30.0);
        let _old_rx = add_test_worker_with_rx(&mut state, "zw-W", "W");
        let _new_rx = add_test_worker_with_rx(&mut state, "zw-E", "E");
        state.entities.insert(
            "ship".to_string(),
            test_physics_island_entity([-1.0, 0.0], "W"),
        );
        grant_region_physics_island_authority(&mut state, "zw-W", "ship");
        let path = replay_tape_path("gw_replay_handoff_epoch");
        install_replay_tape(&mut state, &path);

        assert!(handoff_with_position(
            &mut state,
            "ship",
            "W",
            "E",
            Some([1.0, 0.0])
        ));
        flush_pending_handoffs(&mut state);

        let lines = wait_for_tape(&path, 2);
        assert!(lines.iter().any(|event| {
            event.get("kind").and_then(Value::as_str) == Some("broker_handoff")
                && event.get("path").and_then(Value::as_str) == Some("local")
                && event.get("entity").and_then(Value::as_str) == Some("ship")
                && event.get("from").and_then(Value::as_str) == Some("W")
                && event.get("to").and_then(Value::as_str) == Some("E")
                && event.get("spatial_dim").and_then(Value::as_str) == Some("D2")
                && event.get("coordinate_codec").and_then(Value::as_str) == Some("debug_f64_2")
                && event
                    .get("component_registry_version")
                    .and_then(Value::as_u64)
                    == Some(STANDARD_COMPONENT_REGISTRY_VERSION)
                && event
                    .get("partition_schema")
                    .and_then(|schema| schema.get("kind"))
                    .and_then(Value::as_str)
                    == Some("strip1d")
                && event
                    .get("authority_epoch")
                    .and_then(Value::as_u64)
                    .is_some_and(|epoch| epoch > 1)
        }));
    }

    #[test]
    fn n_zone_strip_names_do_not_accept_legacy_we_labels() {
        assert!(is_strip_region_name("W", &[0.0]));
        assert!(is_strip_region_name("E", &[0.0]));
        assert!(!is_strip_region_name("W", &[0.0, 10.0]));
        assert!(!is_strip_region_name("E", &[0.0, 10.0]));
        assert!(is_strip_region_name("Z2", &[0.0, 10.0]));
        assert!(!is_strip_region_name("Z2_0", &[0.0, 10.0]));
    }

    #[test]
    fn spawn_in_region_reports_failure_when_wal_fails() {
        let mut state = ServerState::new(30.0);
        state.wal_fail_inject = true;
        state.wal_degraded = false;

        let ok = spawn_in_region(
            &mut state,
            "e-spawn",
            [1.0, 2.0],
            [0.0, 0.0],
            Map::new(),
            None,
            SpawnAuthoritySeed::default(),
        );

        assert!(!ok);
        assert!(!state.entities.contains_key("e-spawn"));
        assert!(state.wal_degraded);
    }

    #[test]
    fn mesh_forward_does_not_send_or_remove_when_mesh_out_wal_fails() {
        let mut state = ServerState::new(30.0);
        state.wal_fail_inject = true;
        state.wal_degraded = false;
        state.my_region = "W".to_string();
        state.region_lease_epoch.insert("W".to_string(), 7);
        state
            .entities
            .insert("ship".to_string(), test_entity([1.0, 0.0], "W"));

        let (tx, mut rx) = mpsc::unbounded_channel();
        state.mesh.insert("E".to_string(), tx);

        mesh_forward(&mut state, "ship", "E");

        assert!(state.entities.contains_key("ship"));
        assert!(state.pending_mesh.is_empty());
        assert!(rx.try_recv().is_err());
        assert!(state.wal_degraded);
    }

    #[test]
    fn cross_broker_handoff_flushes_after_same_batch_writes() {
        let mut state = ServerState::new(30.0);
        state.boundaries = vec![0.0];
        state.grid2d = None;
        state.my_region = "W".to_string();
        state.region_lease_epoch.insert("W".to_string(), 1);
        add_test_worker(&mut state, "zw-W", "W");
        state.mesh_regions.insert("E".to_string());
        let (tx, mut rx) = mpsc::unbounded_channel();
        state.mesh.insert("E".to_string(), tx);

        let mut ship = test_entity([-1.0, 0.0], "W");
        ship.components.insert("hp".to_string(), json!(1));
        state.entities.insert("ship".to_string(), ship);
        state.pending_updates.push(PreparedUpdate {
            gen: 1,
            eid: "ship".to_string(),
            comp: "pos".to_string(),
            value: json!([1.0, 0.0]),
            version: 2,
            writer: "zw-W".to_string(),
        });
        state.pending_updates.push(PreparedUpdate {
            gen: 2,
            eid: "ship".to_string(),
            comp: "hp".to_string(),
            value: json!(2),
            version: 3,
            writer: "zw-W".to_string(),
        });
        state.pending_gen = 2;

        flush_pending_updates(&mut state);

        assert!(
            !state.entities.contains_key("ship"),
            "entity should leave only after the whole durable update batch applies"
        );
        assert!(state.pending_mesh.contains_key("ship"));
        let handoff = decode_test_frame(
            &rx.try_recv()
                .expect("cross-broker handoff should be sent after batch flush"),
        );
        assert_eq!(handoff["op"], "MeshHandoff");
        assert_eq!(handoff["entity"], "ship");
        assert_eq!(handoff["components"]["hp"], json!(2));
        assert!(
            state.pending_remote_handoffs.is_empty(),
            "remote intent queue should drain after sending mesh_out"
        );
    }

    #[test]
    fn cross_broker_handoff_intent_is_rechecked_after_same_batch_return() {
        let mut state = ServerState::new(30.0);
        state.boundaries = vec![0.0];
        state.grid2d = None;
        state.my_region = "W".to_string();
        state.region_lease_epoch.insert("W".to_string(), 1);
        add_test_worker(&mut state, "zw-W", "W");
        state.mesh_regions.insert("E".to_string());
        let (tx, mut rx) = mpsc::unbounded_channel();
        state.mesh.insert("E".to_string(), tx);

        state
            .entities
            .insert("ship".to_string(), test_entity([-1.0, 0.0], "W"));
        state.pending_updates.push(PreparedUpdate {
            gen: 1,
            eid: "ship".to_string(),
            comp: "pos".to_string(),
            value: json!([1.0, 0.0]),
            version: 2,
            writer: "zw-W".to_string(),
        });
        state.pending_updates.push(PreparedUpdate {
            gen: 2,
            eid: "ship".to_string(),
            comp: "pos".to_string(),
            value: json!([-1.0, 0.0]),
            version: 3,
            writer: "zw-W".to_string(),
        });
        state.pending_gen = 2;

        flush_pending_updates(&mut state);

        let ship = state
            .entities
            .get("ship")
            .expect("same-batch return to W must keep the entity local");
        assert_eq!(ship.region, "W");
        assert_eq!(ship.pos, [-1.0, 0.0]);
        assert!(state.pending_mesh.is_empty());
        assert!(state.pending_remote_handoffs.is_empty());
        assert!(
            rx.try_recv().is_err(),
            "stale remote handoff intent must not send after final batch position returns local"
        );
    }

    fn decode_test_frame(buf: &[u8]) -> Value {
        let n = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        serde_json::from_slice(&buf[4..4 + n]).unwrap()
    }

    #[test]
    fn command_response_wrong_worker_does_not_satisfy_pending() {
        let mut state = ServerState::new(30.0);
        let mut caller_rx = add_test_peer_with_rx(&mut state, "client", "CLIENT", HashSet::new());
        let mut owner_rx = add_test_worker_with_rx(&mut state, "w1", "W");
        add_test_worker(&mut state, "w2", "E");
        state
            .entities
            .insert("ship".to_string(), test_entity([-1.0, 0.0], "W"));
        grant_region_physics_island_authority(&mut state, "w1", "ship");
        while owner_rx.try_recv().is_ok() {}

        dispatch_test_frame(
            &mut state,
            "client",
            &json!({"op":"CommandRequest","request_id":"cmd-1","entity":"ship","command":"move"}),
        );
        let forwarded =
            decode_test_frame(&owner_rx.try_recv().expect("command must route to owner"));
        assert_eq!(forwarded["op"], "CommandRequest");
        assert_eq!(forwarded["entity"], "ship");
        assert_eq!(forwarded["authority_comp"], "pos");
        let routed_epoch = forwarded["authority_epoch"]
            .as_u64()
            .expect("command must carry the routed authority epoch");

        dispatch_test_frame(
            &mut state,
            "w2",
            &json!({"op":"CommandResponse","request_id":"cmd-1","entity":"ship",
            "success":true,"payload":{"entity":"ship","handled_by":"w2"}}),
        );
        assert!(
            caller_rx.try_recv().is_err(),
            "wrong worker must not satisfy or consume the command"
        );
        assert!(state.pending_commands.contains_key("cmd-1"));

        dispatch_test_frame(
            &mut state,
            "w1",
            &json!({"op":"CommandResponse","request_id":"cmd-1","entity":"ship",
            "success":true,"payload":{"entity":"ship","handled_by":"w1"}}),
        );
        let response =
            decode_test_frame(&caller_rx.try_recv().expect("owner response must forward"));
        assert_eq!(response["op"], "CommandResponse");
        assert_eq!(response["entity"], "ship");
        assert_eq!(response["routed_owner"], "w1");
        assert_eq!(response["authority_comp"], "pos");
        assert_eq!(response["authority_epoch"].as_u64(), Some(routed_epoch));
        assert_eq!(response["success"], true);
        assert!(!state.pending_commands.contains_key("cmd-1"));
    }

    #[test]
    fn command_response_wrong_entity_does_not_satisfy_pending() {
        let mut state = ServerState::new(30.0);
        let mut caller_rx = add_test_peer_with_rx(&mut state, "client", "CLIENT", HashSet::new());
        let mut owner_rx = add_test_worker_with_rx(&mut state, "w1", "W");
        state
            .entities
            .insert("ship".to_string(), test_entity([-1.0, 0.0], "W"));
        grant_region_physics_island_authority(&mut state, "w1", "ship");
        while owner_rx.try_recv().is_ok() {}

        dispatch_test_frame(
            &mut state,
            "client",
            &json!({"op":"CommandRequest","request_id":"cmd-entity","entity":"ship","command":"move"}),
        );
        let _ = owner_rx.try_recv().expect("command must route to owner");

        dispatch_test_frame(
            &mut state,
            "w1",
            &json!({"op":"CommandResponse","request_id":"cmd-entity","entity":"other",
            "success":true,"payload":{"entity":"other","handled_by":"w1"}}),
        );
        assert!(
            caller_rx.try_recv().is_err(),
            "wrong entity must not complete the pending command"
        );
        assert!(state.pending_commands.contains_key("cmd-entity"));

        dispatch_test_frame(
            &mut state,
            "w1",
            &json!({"op":"CommandResponse","request_id":"cmd-entity","payload":{"entity":"ship"},
            "success":true}),
        );
        let response =
            decode_test_frame(&caller_rx.try_recv().expect("matching entity must forward"));
        assert_eq!(response["op"], "CommandResponse");
        assert_eq!(response["entity"], "ship");
        assert_eq!(response["routed_owner"], "w1");
        assert_eq!(response["authority_comp"], "pos");
        assert!(response["authority_epoch"].as_u64().is_some());
        assert_eq!(response["success"], true);
        assert!(!state.pending_commands.contains_key("cmd-entity"));
    }

    #[test]
    fn command_response_stale_owner_after_handoff_fails() {
        let mut state = ServerState::new(30.0);
        let mut caller_rx = add_test_peer_with_rx(&mut state, "client", "CLIENT", HashSet::new());
        let mut old_owner_rx = add_test_worker_with_rx(&mut state, "w1", "W");
        add_test_worker(&mut state, "w2", "E");
        state
            .entities
            .insert("ship".to_string(), test_entity([-1.0, 0.0], "W"));
        grant_region_physics_island_authority(&mut state, "w1", "ship");
        while old_owner_rx.try_recv().is_ok() {}

        dispatch_test_frame(
            &mut state,
            "client",
            &json!({"op":"CommandRequest","request_id":"cmd-stale","entity":"ship","command":"move"}),
        );
        let _ = old_owner_rx
            .try_recv()
            .expect("command must route to the old owner before handoff");

        assert!(handoff_with_position(
            &mut state,
            "ship",
            "W",
            "E",
            Some([2.0, 0.0])
        ));
        flush_pending_handoffs(&mut state);
        assert_eq!(
            component_authority_owner(&state.entities["ship"], "pos"),
            Some("w2".to_string())
        );

        dispatch_test_frame(
            &mut state,
            "w1",
            &json!({"op":"CommandResponse","request_id":"cmd-stale","entity":"ship",
            "success":true,"payload":{"entity":"ship","handled_by":"w1"}}),
        );
        let response = decode_test_frame(
            &caller_rx
                .try_recv()
                .expect("stale owner response must produce a caller-visible failure"),
        );
        assert_eq!(response["op"], "CommandResponse");
        assert_eq!(response["request_id"], "cmd-stale");
        assert_eq!(response["success"], false);
        assert_eq!(response["reason"], "stale command authority");
        assert!(!state.pending_commands.contains_key("cmd-stale"));
    }

    #[test]
    fn entity_query_filters_acl_protected_ghosts_before_rows_and_qbi() {
        let mut state = ServerState::new(30.0);
        let mut rx_public = add_test_worker_with_rx(&mut state, "public-observer", "OBS");
        let mut rx_secret = add_test_worker_with_rx(&mut state, "secret-observer", "OBS");
        for wid in ["public-observer", "secret-observer"] {
            let worker = state.workers.get_mut(wid).unwrap();
            worker.aoi_center = Some([0.0, 0.0]);
            worker.aoi_radius = Some(10.0);
        }
        state
            .workers
            .get_mut("secret-observer")
            .unwrap()
            .attributes
            .insert("secret_reader".to_string());

        let mut components = Map::new();
        components.insert("acl".to_string(), json!({"read": ["secret_reader"]}));
        components.insert("hidden_logic".to_string(), json!({"server_only": true}));
        state.ghosts.insert(
            "secret-ghost".to_string(),
            GhostEntity {
                pos: [1.0, 0.0],
                vel: [0.0, 0.0],
                components,
                owner_region: "E".to_string(),
                last_seen: Instant::now(),
            },
        );

        dispatch_inner(
            &mut state,
            "public-observer",
            &json!({"op":"EntityQuery","request_id":"public-all","query":{"type":"all"}}),
        );
        let public_response =
            decode_test_frame(&rx_public.try_recv().expect("public query response"));
        assert_eq!(
            public_response.get("count").and_then(Value::as_u64),
            Some(0),
            "public observer saw an ACL-protected ghost row: {public_response}"
        );

        dispatch_inner(
            &mut state,
            "public-observer",
            &json!({"op":"EntityQuery","request_id":"public-qbi","query":{"type":"component","comp":"hidden_logic"}}),
        );
        let public_qbi = decode_test_frame(&rx_public.try_recv().expect("public qbi response"));
        assert_eq!(
            public_qbi.get("count").and_then(Value::as_u64),
            Some(0),
            "public QBI matched an ACL-protected ghost component: {public_qbi}"
        );

        dispatch_inner(
            &mut state,
            "secret-observer",
            &json!({"op":"EntityQuery","request_id":"secret-all","query":{"type":"all"}}),
        );
        let secret_response =
            decode_test_frame(&rx_secret.try_recv().expect("secret query response"));
        assert_eq!(
            secret_response.get("count").and_then(Value::as_u64),
            Some(1),
            "authorized observer should still see the ghost: {secret_response}"
        );
    }

    #[test]
    fn health_snapshot_reports_stale_and_fresh_monitor_tick_age() {
        let mut state = ServerState::new(30.0);
        let stale = health_snapshot(&state);
        assert_eq!(
            stale.get("monitor_tick_age_ms").and_then(Value::as_u64),
            Some(u64::MAX),
            "a never-ticked monitor loop must not look fresh: {stale}"
        );

        state.monitor_last_tick_ms = now_millis().saturating_sub(25);
        let fresh = health_snapshot(&state);
        assert!(
            fresh
                .get("monitor_tick_age_ms")
                .and_then(Value::as_u64)
                .is_some_and(|age| age <= 1_000),
            "recent monitor tick should expose a fresh age: {fresh}"
        );
    }

    #[test]
    fn batch_update_missing_entity_emits_update_rejected() {
        let mut state = ServerState::new(30.0);
        let mut rx = add_test_worker_with_rx(&mut state, "owner-W", "W");

        dispatch_inner(
            &mut state,
            "owner-W",
            &json!({"op":"BatchUpdate","comp":"pos","updates":[["missing-body",[1.0,0.0]]]}),
        );

        let rejected = decode_test_frame(&rx.try_recv().expect("missing entity rejection"));
        assert_eq!(
            rejected.get("op").and_then(Value::as_str),
            Some("UpdateRejected")
        );
        assert_eq!(
            rejected.get("entity").and_then(Value::as_str),
            Some("missing-body")
        );
        assert_eq!(rejected.get("comp").and_then(Value::as_str), Some("pos"));
        assert_eq!(
            rejected.get("reason").and_then(Value::as_str),
            Some("entity not found")
        );
    }

    #[test]
    fn obs_without_observer_attr_cannot_query_global_entities() {
        let mut state = ServerState::new(30.0);
        assert!(spawn_in_region(
            &mut state,
            "visible-if-authorized",
            [0.0, 0.0],
            [0.0, 0.0],
            Map::new(),
            Some("W"),
            SpawnAuthoritySeed::default(),
        ));
        let mut rx = add_test_worker_with_rx(&mut state, "viewer", "OBS");

        dispatch_test_frame(
            &mut state,
            "viewer",
            &json!({"op":"EntityQuery","request_id":"q1","query":{"type":"all"}}),
        );

        let response = decode_test_frame(&rx.try_recv().expect("query must respond"));
        assert_eq!(response["op"], "EntityQueryResponse");
        assert_eq!(response["count"], 0);
        assert_eq!(response["entities"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn obs_with_observer_attr_can_query_global_entities() {
        let mut state = ServerState::new(30.0);
        assert!(spawn_in_region(
            &mut state,
            "visible-to-observer",
            [0.0, 0.0],
            [0.0, 0.0],
            Map::new(),
            Some("W"),
            SpawnAuthoritySeed::default(),
        ));
        let mut rx = add_test_worker_with_rx(&mut state, "observer", "OBS");
        state
            .workers
            .get_mut("observer")
            .unwrap()
            .attributes
            .insert("observer".to_string());
        update_interest_grid(&mut state, "observer");
        assert!(state.global_workers.contains("observer"));

        dispatch_test_frame(
            &mut state,
            "observer",
            &json!({"op":"EntityQuery","request_id":"q1","query":{"type":"all"}}),
        );

        let response = decode_test_frame(&rx.try_recv().expect("query must respond"));
        assert_eq!(response["op"], "EntityQueryResponse");
        assert_eq!(response["count"], 1);
        assert_eq!(response["entities"][0]["entity"], "visible-to-observer");
    }

    #[test]
    fn obs_without_observer_attr_no_center_interest_is_rejected_and_not_global() {
        let mut state = ServerState::new(30.0);
        let mut rx = add_test_worker_with_rx(&mut state, "viewer", "OBS");
        update_interest_grid(&mut state, "viewer");
        assert!(
            !state.global_workers.contains("viewer"),
            "plain OBS peer must not start as a global observer"
        );

        dispatch_test_frame(&mut state, "viewer", &json!({"op":"Interest"}));

        let rejected = decode_test_frame(&rx.try_recv().expect("global OBS interest must reject"));
        assert_eq!(rejected["op"], "UpdateRejected");
        assert_eq!(rejected["comp"], "interest");
        assert!(rejected["reason"]
            .as_str()
            .unwrap()
            .contains("global OBS interest requires"));
        assert!(
            !state.global_workers.contains("viewer"),
            "rejected plain OBS interest must not enter global_workers"
        );
    }

    #[tokio::test]
    async fn read_frame_rejects_oversized_header_before_body() {
        let (mut client, mut server) = tokio::io::duplex(64);
        let oversized = (DEFAULT_MAX_FRAME_BYTES as u32 + 1).to_be_bytes();
        client.write_all(&oversized).await.unwrap();

        let result = tokio::time::timeout(Duration::from_millis(50), read_frame(&mut server))
            .await
            .expect("oversized header should return before reading a body");

        assert!(result.is_none());
    }

    #[test]
    fn worker_connect_auth_token_matcher_is_fail_closed_when_configured() {
        assert!(auth_token_matches(None, None));
        assert!(auth_token_matches(None, Some("anything")));
        assert!(auth_token_matches(Some("secret"), Some("secret")));
        assert!(!auth_token_matches(Some("secret"), None));
        assert!(!auth_token_matches(Some("secret"), Some("wrong")));
    }

    #[test]
    fn worker_connect_claims_bind_token_to_region_and_attributes() {
        let claims = parse_connect_auth_claims("w-secret:W:physics|sim,mesh-secret:MESH:mesh");
        assert_eq!(claims.len(), 2);

        let requested_attrs = HashSet::from(["physics".to_string()]);
        let resolved =
            resolve_connect_claims(None, &claims, Some("w-secret"), Some("W"), &requested_attrs)
                .unwrap()
                .unwrap();
        assert_eq!(resolved.region, "W");
        assert!(resolved.attributes.contains("physics"));
        assert!(resolved.attributes.contains("sim"));

        assert_eq!(
            resolve_connect_claims(None, &claims, Some("w-secret"), Some("E"), &requested_attrs)
                .unwrap_err(),
            "auth token is not valid for requested region"
        );

        let escalated_attrs = HashSet::from(["physics".to_string(), "inspector".to_string()]);
        assert_eq!(
            resolve_connect_claims(None, &claims, Some("w-secret"), Some("W"), &escalated_attrs)
                .unwrap_err(),
            "auth token is not valid for requested attributes"
        );

        let derived =
            resolve_connect_claims(None, &claims, Some("w-secret"), None, &HashSet::new())
                .unwrap()
                .unwrap();
        assert_eq!(derived.region, "W");
    }

    #[test]
    fn peer_declared_broker_owned_attributes_are_stripped() {
        let mut attributes = HashSet::from([
            "physics".to_string(),
            "kernel_admin".to_string(),
            "acl_admin".to_string(),
            "inspector".to_string(),
            "role.client".to_string(),
        ]);

        strip_peer_declared_broker_owned_attributes(&mut attributes);

        assert!(attributes.contains("physics"));
        assert!(!attributes.contains("kernel_admin"));
        assert!(!attributes.contains("acl_admin"));
        assert!(!attributes.contains("inspector"));
        assert!(!attributes.contains("role.client"));
    }

    #[tokio::test]
    async fn worker_connect_peer_declared_kernel_admin_is_not_registered() {
        let state = Arc::new(Mutex::new(ServerState::new(30.0)));
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_state = state.clone();
        let server = tokio::spawn(async move {
            let (sock, _) = listener.accept().await.unwrap();
            handle_conn(sock, server_state).await;
        });

        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
        client
            .write_all(&frame(&json!({
                "op":"WorkerConnect",
                "worker_id":"fake-admin",
                "region":"W",
                "attributes":["physics","kernel_admin"]
            })))
            .await
            .unwrap();

        let mut registered_without_privilege = false;
        for _ in 0..20 {
            {
                let s = state.lock().await;
                registered_without_privilege = s.workers.get("fake-admin").is_some_and(|w| {
                    w.attributes.contains("physics") && !w.attributes.contains("kernel_admin")
                });
            }
            if registered_without_privilege {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            registered_without_privilege,
            "peer-declared attributes may carry ordinary project claims, but broker-owned privileges must not register from JSON"
        );

        client
            .write_all(&frame(&json!({"op":"Disconnect"})))
            .await
            .unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn worker_connect_shared_token_does_not_grant_kernel_admin() {
        let state = Arc::new(Mutex::new(ServerState::new(30.0)));
        {
            let mut s = state.lock().await;
            s.connect_auth_token = Some("shared".to_string());
        }
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_state = state.clone();
        let server = tokio::spawn(async move {
            let (sock, _) = listener.accept().await.unwrap();
            handle_conn(sock, server_state).await;
        });

        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
        client
            .write_all(&frame(&json!({
                "op":"WorkerConnect",
                "worker_id":"shared-token-peer",
                "region":"W",
                "attributes":["physics","kernel_admin"],
                "auth_token":"shared"
            })))
            .await
            .unwrap();

        let mut registered_without_privilege = false;
        for _ in 0..20 {
            {
                let s = state.lock().await;
                registered_without_privilege =
                    s.workers.get("shared-token-peer").is_some_and(|w| {
                        w.attributes.contains("physics") && !w.attributes.contains("kernel_admin")
                    });
            }
            if registered_without_privilege {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            registered_without_privilege,
            "GW_AUTH_TOKEN authenticates membership only; broker-owned privileges require GW_AUTH_CLAIMS"
        );

        client
            .write_all(&frame(&json!({"op":"Disconnect"})))
            .await
            .unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn worker_connect_peer_declared_mesh_region_requires_claim() {
        let state = Arc::new(Mutex::new(ServerState::new(30.0)));
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_state = state.clone();
        let server = tokio::spawn(async move {
            let (sock, _) = listener.accept().await.unwrap();
            handle_conn(sock, server_state).await;
        });

        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
        client
            .write_all(&frame(&json!({
                "op":"WorkerConnect",
                "worker_id":"fake-mesh",
                "region":"MESH"
            })))
            .await
            .unwrap();

        let mut hdr = [0u8; 4];
        client.read_exact(&mut hdr).await.unwrap();
        let n = u32::from_be_bytes(hdr) as usize;
        let mut body = vec![0u8; n];
        client.read_exact(&mut body).await.unwrap();
        let reject: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(reject["op"], "AuthReject");
        assert_eq!(reject["worker_id"], "fake-mesh");
        assert_eq!(
            reject["reason"],
            "broker-owned connect region requires token-bound claim"
        );
        server.await.unwrap();

        let s = state.lock().await;
        assert!(
            !s.workers.contains_key("fake-mesh"),
            "peer-declared MESH must not register as a mesh role"
        );
    }

    #[tokio::test]
    async fn worker_connect_shared_token_cannot_claim_mesh_region() {
        let state = Arc::new(Mutex::new(ServerState::new(30.0)));
        {
            let mut s = state.lock().await;
            s.connect_auth_token = Some("shared".to_string());
        }
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_state = state.clone();
        let server = tokio::spawn(async move {
            let (sock, _) = listener.accept().await.unwrap();
            handle_conn(sock, server_state).await;
        });

        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
        client
            .write_all(&frame(&json!({
                "op":"WorkerConnect",
                "worker_id":"shared-token-mesh",
                "region":"MESH",
                "auth_token":"shared"
            })))
            .await
            .unwrap();

        let mut hdr = [0u8; 4];
        client.read_exact(&mut hdr).await.unwrap();
        let n = u32::from_be_bytes(hdr) as usize;
        let mut body = vec![0u8; n];
        client.read_exact(&mut body).await.unwrap();
        let reject: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(reject["op"], "AuthReject");
        assert_eq!(
            reject["reason"],
            "broker-owned connect region requires token-bound claim"
        );
        server.await.unwrap();

        let s = state.lock().await;
        assert!(!s.workers.contains_key("shared-token-mesh"));
    }

    #[tokio::test]
    async fn worker_connect_claim_token_can_register_mesh_region() {
        let state = Arc::new(Mutex::new(ServerState::new(30.0)));
        {
            let mut s = state.lock().await;
            s.connect_auth_claims.insert(
                "mesh-secret".to_string(),
                PeerClaims {
                    region: "MESH".to_string(),
                    attributes: HashSet::from(["role.mesh".to_string()]),
                },
            );
        }
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_state = state.clone();
        let server = tokio::spawn(async move {
            let (sock, _) = listener.accept().await.unwrap();
            handle_conn(sock, server_state).await;
        });

        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
        client
            .write_all(&frame(&json!({
                "op":"WorkerConnect",
                "worker_id":"mesh-link",
                "auth_token":"mesh-secret"
            })))
            .await
            .unwrap();

        let mut registered_as_mesh = false;
        for _ in 0..20 {
            {
                let s = state.lock().await;
                registered_as_mesh = s.workers.get("mesh-link").is_some_and(|w| {
                    w.region == "MESH"
                        && w.role == PeerRole::Mesh
                        && w.attributes.contains("role.mesh")
                });
            }
            if registered_as_mesh {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            registered_as_mesh,
            "token-bound claims, not peer JSON, grant the mesh role"
        );

        client
            .write_all(&frame(&json!({"op":"Disconnect"})))
            .await
            .unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn worker_connect_claim_token_can_register_kernel_admin() {
        let state = Arc::new(Mutex::new(ServerState::new(30.0)));
        {
            let mut s = state.lock().await;
            s.connect_auth_claims.insert(
                "admin-secret".to_string(),
                PeerClaims {
                    region: "OBS".to_string(),
                    attributes: HashSet::from(["kernel_admin".to_string()]),
                },
            );
        }
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_state = state.clone();
        let server = tokio::spawn(async move {
            let (sock, _) = listener.accept().await.unwrap();
            handle_conn(sock, server_state).await;
        });

        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
        client
            .write_all(&frame(&json!({
                "op":"WorkerConnect",
                "worker_id":"admin",
                "auth_token":"admin-secret"
            })))
            .await
            .unwrap();

        let mut registered_with_privilege = false;
        for _ in 0..20 {
            {
                let s = state.lock().await;
                registered_with_privilege = s
                    .workers
                    .get("admin")
                    .is_some_and(|w| w.region == "OBS" && w.attributes.contains("kernel_admin"));
            }
            if registered_with_privilege {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            registered_with_privilege,
            "broker-owned claims, not peer JSON, are allowed to grant kernel_admin"
        );

        client
            .write_all(&frame(&json!({"op":"Disconnect"})))
            .await
            .unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn worker_connect_claim_token_rejects_wrong_region_before_registration() {
        let state = Arc::new(Mutex::new(ServerState::new(30.0)));
        {
            let mut s = state.lock().await;
            s.connect_auth_claims.insert(
                "w-secret".to_string(),
                PeerClaims {
                    region: "W".to_string(),
                    attributes: HashSet::from(["physics".to_string()]),
                },
            );
        }

        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_state = state.clone();
        let server = tokio::spawn(async move {
            let (sock, _) = listener.accept().await.unwrap();
            handle_conn(sock, server_state).await;
        });

        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
        client
            .write_all(&frame(&json!({
                "op":"WorkerConnect",
                "worker_id":"region-thief",
                "region":"E",
                "attributes":["physics"],
                "auth_token":"w-secret"
            })))
            .await
            .unwrap();

        let mut hdr = [0u8; 4];
        client.read_exact(&mut hdr).await.unwrap();
        let n = u32::from_be_bytes(hdr) as usize;
        let mut body = vec![0u8; n];
        client.read_exact(&mut body).await.unwrap();
        let reject: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(reject["op"], "AuthReject");
        assert_eq!(reject["error"], "auth_error");
        assert_eq!(
            reject["reason"],
            "auth token is not valid for requested region"
        );
        server.await.unwrap();

        let s = state.lock().await;
        assert!(
            !s.workers.contains_key("region-thief"),
            "wrong-region claim token must not register a worker"
        );
        assert!(
            !matches!(s.region_worker.get("E"), Some(w) if w == "region-thief"),
            "wrong-region claim token must not claim region ownership"
        );
    }

    #[tokio::test]
    async fn replay_tape_worker_connect_redacts_auth_token_on_reject() {
        let path = replay_tape_path("gw_replay_connect_redaction");
        let state = Arc::new(Mutex::new(ServerState::new(30.0)));
        {
            let mut s = state.lock().await;
            install_replay_tape(&mut s, &path);
            s.connect_auth_claims.insert(
                "w-secret".to_string(),
                PeerClaims {
                    region: "W".to_string(),
                    attributes: HashSet::from(["physics".to_string()]),
                },
            );
        }

        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_state = state.clone();
        let server = tokio::spawn(async move {
            let (sock, _) = listener.accept().await.unwrap();
            handle_conn(sock, server_state).await;
        });

        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
        client
            .write_all(&frame(&json!({
                "op":"WorkerConnect",
                "worker_id":"region-thief",
                "region":"E",
                "attributes":["physics"],
                "auth_token":"w-secret"
            })))
            .await
            .unwrap();

        let mut hdr = [0u8; 4];
        client.read_exact(&mut hdr).await.unwrap();
        let n = u32::from_be_bytes(hdr) as usize;
        let mut body = vec![0u8; n];
        client.read_exact(&mut body).await.unwrap();
        let reject: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(reject["op"], "AuthReject");
        server.await.unwrap();

        let _lines = wait_for_tape(&path, 1);
        let content = std::fs::read_to_string(&path).unwrap_or_default();
        assert!(content.contains("\"kind\":\"broker_connect\""));
        assert!(content.contains("\"credential_present\":true"));
        assert!(content.contains("\"outcome\":\"rejected\""));
        assert!(!content.contains("w-secret"));
        assert!(!content.contains("auth_token"));
    }

    #[tokio::test]
    async fn worker_connect_claim_token_registers_from_broker_owned_claims() {
        let state = Arc::new(Mutex::new(ServerState::new(30.0)));
        {
            let mut s = state.lock().await;
            s.connect_auth_claims.insert(
                "w-secret".to_string(),
                PeerClaims {
                    region: "W".to_string(),
                    attributes: HashSet::from(["physics".to_string()]),
                },
            );
        }

        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_state = state.clone();
        let server = tokio::spawn(async move {
            let (sock, _) = listener.accept().await.unwrap();
            handle_conn(sock, server_state).await;
        });

        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
        client
            .write_all(&frame(&json!({
                "op":"WorkerConnect",
                "worker_id":"worker-ok",
                "region":"W",
                "auth_token":"w-secret"
            })))
            .await
            .unwrap();

        let mut registered = false;
        for _ in 0..20 {
            {
                let s = state.lock().await;
                registered = s.workers.contains_key("worker-ok")
                    && matches!(s.region_worker.get("W"), Some(w) if w == "worker-ok")
                    && s.workers
                        .get("worker-ok")
                        .is_some_and(|w| w.attributes.contains("physics"));
            }
            if registered {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            registered,
            "claim token must register with broker-owned region and attributes"
        );

        client
            .write_all(&frame(&json!({"op":"Disconnect"})))
            .await
            .unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn client_role_claim_cannot_lease_worker_region() {
        let state = Arc::new(Mutex::new(ServerState::new(30.0)));
        {
            let mut s = state.lock().await;
            s.connect_auth_claims.insert(
                "client-secret".to_string(),
                PeerClaims {
                    region: "W".to_string(),
                    attributes: HashSet::from([
                        "role.client".to_string(),
                        "player.alice".to_string(),
                    ]),
                },
            );
        }

        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_state = state.clone();
        let server = tokio::spawn(async move {
            let (sock, _) = listener.accept().await.unwrap();
            handle_conn(sock, server_state).await;
        });

        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
        client
            .write_all(&frame(&json!({
                "op":"WorkerConnect",
                "worker_id":"client-alice",
                "region":"W",
                "auth_token":"client-secret"
            })))
            .await
            .unwrap();

        let mut registered = false;
        for _ in 0..20 {
            {
                let s = state.lock().await;
                registered = s.workers.get("client-alice").is_some_and(|w| {
                    w.role == PeerRole::Client
                        && w.region == "W"
                        && w.attributes.contains("player.alice")
                }) && !matches!(s.region_worker.get("W"), Some(w) if w == "client-alice");
            }
            if registered {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            registered,
            "a client claim may connect with region context but must not lease the worker region"
        );

        client
            .write_all(&frame(&json!({"op":"Disconnect"})))
            .await
            .unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn worker_connect_auth_rejects_before_registration() {
        let state = Arc::new(Mutex::new(ServerState::new(30.0)));
        {
            let mut s = state.lock().await;
            s.connect_auth_token = Some("secret".to_string());
        }

        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_state = state.clone();
        let server = tokio::spawn(async move {
            let (sock, _) = listener.accept().await.unwrap();
            handle_conn(sock, server_state).await;
        });

        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
        client
            .write_all(&frame(&json!({
                "op":"WorkerConnect",
                "worker_id":"intruder",
                "region":"W"
            })))
            .await
            .unwrap();

        let mut hdr = [0u8; 4];
        client.read_exact(&mut hdr).await.unwrap();
        let n = u32::from_be_bytes(hdr) as usize;
        let mut body = vec![0u8; n];
        client.read_exact(&mut body).await.unwrap();
        let reject: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(reject["op"], "AuthReject");
        assert_eq!(reject["error"], "auth_error");
        assert_eq!(reject["worker_id"], "intruder");
        server.await.unwrap();

        let s = state.lock().await;
        assert!(
            !s.workers.contains_key("intruder"),
            "failed auth must not register a worker"
        );
        assert!(
            !matches!(s.region_worker.get("W"), Some(w) if w == "intruder"),
            "failed auth must not claim region ownership"
        );
    }

    #[tokio::test]
    async fn worker_connect_auth_accepts_matching_token_before_disconnect() {
        let state = Arc::new(Mutex::new(ServerState::new(30.0)));
        {
            let mut s = state.lock().await;
            s.connect_auth_token = Some("secret".to_string());
        }

        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_state = state.clone();
        let server = tokio::spawn(async move {
            let (sock, _) = listener.accept().await.unwrap();
            handle_conn(sock, server_state).await;
        });

        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
        client
            .write_all(&frame(&json!({
                "op":"WorkerConnect",
                "worker_id":"worker-ok",
                "region":"W",
                "auth_token":"secret"
            })))
            .await
            .unwrap();

        let mut registered = false;
        for _ in 0..20 {
            {
                let s = state.lock().await;
                registered = s.workers.contains_key("worker-ok")
                    && matches!(s.region_worker.get("W"), Some(w) if w == "worker-ok");
            }
            if registered {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(registered, "matching auth token must register the worker");

        client
            .write_all(&frame(&json!({"op":"Disconnect"})))
            .await
            .unwrap();
        server.await.unwrap();
    }

    #[test]
    fn client_role_rejects_create_entity_before_dispatch_or_wal() {
        let mut state = ServerState::new(30.0);
        let mut rx = add_test_peer_with_rx(
            &mut state,
            "client-alice",
            "W",
            HashSet::from(["role.client".to_string(), "player.alice".to_string()]),
        );

        dispatch_test_frame(
            &mut state,
            "client-alice",
            &json!({
                "op":"CreateEntity",
                "request_id":"create-client",
                "entity":"client-spawn",
                "components":{"pos":[0.0,0.0],"vel":[0.0,0.0]}
            }),
        );

        assert!(!state.entities.contains_key("client-spawn"));
        assert_eq!(state.wal_bytes, 0);
        let reject = decode_test_frame(&rx.try_recv().expect("client create must reject"));
        assert_eq!(reject["op"], "UpdateRejected");
        assert_eq!(reject["comp"], "role_policy");
        assert_eq!(reject["rejected_op"], "CreateEntity");
        assert_eq!(reject["peer_role"], "client");
    }

    #[test]
    fn client_role_update_component_still_requires_component_authority() {
        let mut state = ServerState::new(30.0);
        add_test_worker(&mut state, "owner", "W");
        assert!(spawn_in_region(
            &mut state,
            "ship",
            [-1.0, 0.0],
            [0.0, 0.0],
            Map::new(),
            Some("W"),
            SpawnAuthoritySeed::default(),
        ));
        let mut rx = add_test_peer_with_rx(
            &mut state,
            "client-alice",
            "W",
            HashSet::from(["role.client".to_string(), "player.alice".to_string()]),
        );

        dispatch_test_frame(
            &mut state,
            "client-alice",
            &json!({
                "op":"UpdateComponent",
                "entity":"ship",
                "comp":"pos",
                "value":[2.0,0.0],
                "authority_epoch":0
            }),
        );

        let reject = decode_test_frame(
            &rx.try_recv()
                .expect("client update without authority must reject"),
        );
        assert_eq!(reject["op"], "UpdateRejected");
        assert_eq!(reject["comp"], "pos");
        assert_ne!(reject["comp"], "role_policy");
        assert_eq!(state.entities["ship"].pos, [-1.0, 0.0]);
    }

    #[test]
    fn mesh_role_rejects_non_mesh_lifecycle_ops_but_allows_mesh_ack() {
        let mut state = ServerState::new(30.0);
        let mut rx = add_test_peer_with_rx(
            &mut state,
            "mesh-peer",
            "MESH",
            HashSet::from(["role.mesh".to_string()]),
        );

        dispatch_test_frame(
            &mut state,
            "mesh-peer",
            &json!({
                "op":"CreateEntity",
                "request_id":"mesh-create",
                "entity":"bad-mesh-spawn",
                "components":{"pos":[0.0,0.0]}
            }),
        );
        let reject = decode_test_frame(&rx.try_recv().expect("mesh create must reject"));
        assert_eq!(reject["op"], "UpdateRejected");
        assert_eq!(reject["comp"], "role_policy");
        assert_eq!(reject["peer_role"], "mesh");
        assert!(!state.entities.contains_key("bad-mesh-spawn"));

        dispatch_test_frame(
            &mut state,
            "mesh-peer",
            &json!({"op":"MeshAck","entity":"not-pending"}),
        );
        assert!(
            rx.try_recv().is_err(),
            "mesh role gate must allow mesh-family ops through without role rejection"
        );
    }

    #[test]
    fn observer_role_rejects_write_ops_and_requires_inspector_for_inspector_query() {
        let mut state = ServerState::new(30.0);
        let mut rx = add_test_peer_with_rx(
            &mut state,
            "observer",
            "OBS",
            HashSet::from(["observer".to_string()]),
        );

        dispatch_test_frame(
            &mut state,
            "observer",
            &json!({
                "op":"CreateEntity",
                "request_id":"obs-create",
                "entity":"observer-spawn",
                "components":{"pos":[0.0,0.0]}
            }),
        );
        let create_reject = decode_test_frame(&rx.try_recv().expect("observer create must reject"));
        assert_eq!(create_reject["op"], "UpdateRejected");
        assert_eq!(create_reject["comp"], "role_policy");
        assert_eq!(create_reject["peer_role"], "observer");
        assert!(!state.entities.contains_key("observer-spawn"));

        dispatch_test_frame(
            &mut state,
            "observer",
            &json!({"op":"InspectorQuery","request_id":"inspect-denied"}),
        );
        let inspector_reject =
            decode_test_frame(&rx.try_recv().expect("plain observer inspector must reject"));
        assert_eq!(inspector_reject["op"], "UpdateRejected");
        assert_eq!(inspector_reject["comp"], "role_policy");

        state
            .workers
            .get_mut("observer")
            .unwrap()
            .attributes
            .insert("inspector".to_string());
        dispatch_test_frame(
            &mut state,
            "observer",
            &json!({"op":"InspectorQuery","request_id":"inspect-ok","max_entities":0}),
        );
        let frame = decode_test_frame(&rx.try_recv().expect("inspector query must pass"));
        assert_eq!(frame["op"], "InspectorFrame");
        assert_eq!(frame["request_id"], "inspect-ok");
    }

    #[test]
    fn ingress_rate_limit_rejects_before_dispatch_or_wal() {
        let mut state = ServerState::new(30.0);
        state.ingress_rate_per_sec = 0.0;
        state.ingress_burst_frames = 2.0;
        let mut rx = add_test_worker_with_rx(&mut state, "spam", "W");

        dispatch_test_frame(
            &mut state,
            "spam",
            &json!({"op":"LogMessage","msg":"budget-1"}),
        );
        dispatch_test_frame(
            &mut state,
            "spam",
            &json!({"op":"LogMessage","msg":"budget-2"}),
        );
        dispatch_test_frame(
            &mut state,
            "spam",
            &json!({
                "op":"CreateEntity",
                "request_id":"create-after-budget",
                "entity":"should-not-exist",
                "components":{"pos":[-1.0,0.0],"vel":[0.0,0.0]}
            }),
        );

        assert!(
            !state.entities.contains_key("should-not-exist"),
            "rate-limited CreateEntity must not reach dispatch_inner"
        );
        assert_eq!(state.workers["spam"].ingress_rejected, 1);
        let reject = decode_test_frame(
            &rx.try_recv()
                .expect("rate-limited peer must receive a structured rejection"),
        );
        assert_eq!(reject["op"], "UpdateRejected");
        assert_eq!(reject["error"], "rate_limit_error");
        assert_eq!(reject["rate_limited"], true);
        assert_eq!(reject["limited_op"], "CreateEntity");
        assert_eq!(reject["request_id"], "create-after-budget");
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn ingress_rate_limit_refills_by_elapsed_time() {
        let mut state = ServerState::new(30.0);
        state.ingress_rate_per_sec = 10.0;
        state.ingress_burst_frames = 1.0;
        let mut rx = add_test_worker_with_rx(&mut state, "refill", "W");
        {
            let w = state.workers.get_mut("refill").unwrap();
            w.ingress_tokens = 0.0;
            w.ingress_last_refill = Instant::now() - Duration::from_millis(250);
        }

        dispatch_test_frame(
            &mut state,
            "refill",
            &json!({"op":"LogMessage","msg":"after-refill"}),
        );

        assert_eq!(state.workers["refill"].ingress_rejected, 0);
        assert!(rx.try_recv().is_err());
        assert!(
            state.workers["refill"].ingress_tokens < 1.0,
            "the refilled token should be consumed by the accepted frame"
        );
    }

    #[test]
    fn ingress_rate_limit_charges_expensive_ops_above_one_frame() {
        let mut state = ServerState::new(30.0);
        state.ingress_rate_per_sec = 0.0;
        state.ingress_burst_frames = 3.0;
        let mut rx = add_test_worker_with_rx(&mut state, "expensive", "W");

        dispatch_test_frame(
            &mut state,
            "expensive",
            &json!({
                "op":"CreateEntity",
                "request_id":"expensive-create",
                "entity":"must-not-create",
                "components":{"pos":[-1.0,0.0],"vel":[0.0,0.0]}
            }),
        );

        assert!(
            !state.entities.contains_key("must-not-create"),
            "an expensive persistent op must not bypass the ingress cost budget as a single cheap frame"
        );
        let reject = decode_test_frame(
            &rx.try_recv()
                .expect("expensive op must receive a structured rate-limit rejection"),
        );
        assert_eq!(reject["limited_op"], "CreateEntity");
        assert!(reject["rate_limit_cost"].as_f64().unwrap() >= 4.0);
        assert_eq!(reject["request_id"], "expensive-create");
    }

    #[test]
    fn ingress_rate_limit_charges_large_valid_payload_by_bytes() {
        let mut state = ServerState::new(30.0);
        state.ingress_rate_per_sec = 0.0;
        state.ingress_burst_frames = 1.0;
        let mut rx = add_test_worker_with_rx(&mut state, "large", "W");
        let payload = "x".repeat(INGRESS_BYTES_PER_TOKEN as usize + 512);

        dispatch_test_frame(
            &mut state,
            "large",
            &json!({"op":"LogMessage","request_id":"large-log","msg":payload}),
        );

        let reject = decode_test_frame(
            &rx.try_recv()
                .expect("large-but-valid payload must be charged above a one-frame unit"),
        );
        assert_eq!(reject["limited_op"], "LogMessage");
        assert!(reject["rate_limit_cost"].as_f64().unwrap() > 1.0);
        assert_eq!(reject["request_id"], "large-log");
    }

    #[test]
    fn cross_broker_adopt_not_visible_above_source_durable_gen() {
        let mut state = ServerState::new(30.0);
        state.my_region = "W".to_string();
        state.region_lease_epoch.insert("W".to_string(), 7);
        state
            .entities
            .insert("ship".to_string(), test_entity([1.0, 0.0], "W"));
        let before = state.durable_gen;

        let (tx, mut rx) = mpsc::unbounded_channel();
        state.mesh.insert("E".to_string(), tx);

        mesh_forward(&mut state, "ship", "E");

        let fr = rx
            .try_recv()
            .expect("mesh frame must be sent only after source durable watermark covers it");
        let handoff = decode_test_frame(&fr);
        let source_gen = handoff
            .get("source_durable_gen")
            .and_then(|v| v.as_u64())
            .expect("MeshHandoff must carry the source durable generation it is covered by");

        assert!(source_gen > before);
        assert!(
            source_gen <= state.durable_gen,
            "neighbour saw MeshHandoff gen {source_gen} above source durable_gen {}",
            state.durable_gen
        );
        assert!(!state.entities.contains_key("ship"));
        assert!(state.pending_mesh.contains_key("ship"));
    }

    #[test]
    fn mesh_forward_carries_full_physics_island_authority_snapshot() {
        let mut state = ServerState::new(30.0);
        state.my_region = "W".to_string();
        state.region_lease_epoch.insert("W".to_string(), 7);
        add_test_worker(&mut state, "w1", "W");
        let mut ship = test_physics_island_entity([1.0, 0.0], "W");
        let old_epochs = stamp_expanded_physics_island_epochs(&mut ship);
        let expected_epoch = old_epochs.values().copied().max().unwrap() + 1;
        state.entities.insert("ship".to_string(), ship);
        grant_region_physics_island_authority(&mut state, "w1", "ship");

        let (tx, mut rx) = mpsc::unbounded_channel();
        state.mesh.insert("E".to_string(), tx);

        mesh_forward(&mut state, "ship", "E");

        let fr = rx
            .try_recv()
            .expect("mesh forward must emit the durable covered handoff");
        let handoff = decode_test_frame(&fr);
        assert_eq!(handoff["op"], "MeshHandoff");
        assert_eq!(handoff["authority_epoch"], expected_epoch);

        let authority = handoff["authority"]
            .as_object()
            .expect("MeshHandoff must carry an authority snapshot");
        for comp in expanded_physics_island_components() {
            let spec = authority
                .get(comp)
                .unwrap_or_else(|| panic!("missing authority for {comp}"));
            assert!(
                spec["owner"].is_null(),
                "mesh source must drop local ownership for {comp}"
            );
            assert_eq!(spec["authority_epoch"], expected_epoch, "{comp}");
            assert_eq!(spec["mode"], "server_physics_island", "{comp}");
        }

        let components = handoff["components"]
            .as_object()
            .expect("MeshHandoff must carry the component payload");
        for comp in ["ang", "at_rest", "lin", "rot"] {
            assert!(
                components.contains_key(comp),
                "expanded physics payload must carry {comp} across the broker seam"
            );
        }
    }

    #[test]
    fn mesh_adopt_persists_committed_region_for_strip_target() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "gw_mesh_adopt_region_{}_{}",
            std::process::id(),
            unique
        ));
        let _ = std::fs::create_dir_all(&dir);
        let wal_path = dir.join("test.wal").to_string_lossy().to_string();

        let mut state = ServerState::new(30.0);
        state.wal_path = wal_path.clone();
        state.wal = Some(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&wal_path)
                .unwrap(),
        );
        add_test_worker(&mut state, "east-worker", "E");

        dispatch_inner(
            &mut state,
            "mesh-link",
            &json!({
                "op":"MeshHandoff",
                "entity":"ship",
                "source_region":"W",
                "target":"E",
                "source_durable_gen":7,
                "lease_epoch":1,
                "authority_epoch":9,
                "pos":[-5.0,0.0],
                "vel":[0.0,0.0],
                "components":{"pos":[-5.0,0.0],"vel":[0.0,0.0]}
            }),
        );

        assert_eq!(state.entities["ship"].region, "E");
        drop(state.wal.take());
        let (store, _, _, _, _, _, report) = recover_from_wal_report(&wal_path, None);
        assert!(report.error.is_none(), "{:?}", report.error);
        assert_eq!(store["ship"].region, "E");
    }

    #[test]
    fn mesh_adopt_commits_grid_cell_before_advertised_strip_region() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "gw_mesh_adopt_grid_region_{}_{}",
            std::process::id(),
            unique
        ));
        let _ = std::fs::create_dir_all(&dir);
        let wal_path = dir.join("test.wal").to_string_lossy().to_string();

        let mut state = ServerState::new(30.0);
        state.grid2d = Some((4, 4, 30.0, 30.0));
        state.my_region = "E".to_string();
        state.wal_path = wal_path.clone();
        state.wal = Some(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&wal_path)
                .unwrap(),
        );
        add_test_worker(&mut state, "cell-worker", "Z2_3");

        dispatch_inner(
            &mut state,
            "mesh-link",
            &json!({
                "op":"MeshHandoff",
                "entity":"ship",
                "source_region":"Z2_2",
                "target":"Z2_3",
                "source_durable_gen":7,
                "lease_epoch":1,
                "authority_epoch":9,
                "pos":[75.0,105.0],
                "vel":[0.0,0.0],
                "components":{"pos":[75.0,105.0],"vel":[0.0,0.0]}
            }),
        );

        assert_eq!(state.entities["ship"].region, "Z2_3");
        assert_eq!(
            state.region_worker.get(&state.entities["ship"].region),
            Some(&"cell-worker".to_string())
        );
        drop(state.wal.take());
        let (store, _, _, _, _, _, report) = recover_from_wal_report(&wal_path, None);
        assert!(report.error.is_none(), "{:?}", report.error);
        assert_eq!(store["ship"].region, "Z2_3");
    }

    #[test]
    fn mesh_adopt_grid_target_overrides_geometric_cell() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "gw_mesh_adopt_grid_target_{}_{}",
            std::process::id(),
            unique
        ));
        let _ = std::fs::create_dir_all(&dir);
        let wal_path = dir.join("test.wal").to_string_lossy().to_string();

        let mut state = ServerState::new(30.0);
        state.grid2d = Some((4, 4, 30.0, 30.0));
        state.wal_path = wal_path.clone();
        state.wal = Some(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&wal_path)
                .unwrap(),
        );
        add_test_worker(&mut state, "cell-worker", "Z2_3");

        dispatch_inner(
            &mut state,
            "mesh-link",
            &json!({
                "op":"MeshHandoff",
                "entity":"ship",
                "source_region":"Z2_2",
                "target":"Z2_3",
                "source_durable_gen":7,
                "lease_epoch":1,
                "authority_epoch":9,
                "pos":[15.0,15.0],
                "vel":[0.0,0.0],
                "components":{"pos":[15.0,15.0],"vel":[0.0,0.0]}
            }),
        );

        assert_eq!(region_2d([15.0, 15.0], 4, 4, 30.0, 30.0), "Z0_0");
        assert_eq!(state.entities["ship"].region, "Z2_3");
        assert_eq!(
            state.region_worker.get(&state.entities["ship"].region),
            Some(&"cell-worker".to_string())
        );
        drop(state.wal.take());
        let (store, _, _, _, _, _, report) = recover_from_wal_report(&wal_path, None);
        assert!(report.error.is_none(), "{:?}", report.error);
        assert_eq!(store["ship"].region, "Z2_3");
    }

    #[test]
    fn mesh_adopt_grid_target_must_be_receiver_owned() {
        let mut state = ServerState::new(30.0);
        state.grid2d = Some((4, 4, 30.0, 30.0));
        add_test_worker(&mut state, "cell-worker", "Z2_3");
        let mut rx = add_test_worker_with_rx(&mut state, "mesh-link", "MESH");

        dispatch_inner(
            &mut state,
            "mesh-link",
            &json!({
                "op":"MeshHandoff",
                "entity":"ship",
                "source_region":"Z2_2",
                "target":"Z3_3",
                "source_durable_gen":7,
                "lease_epoch":1,
                "authority_epoch":9,
                "pos":[75.0,105.0],
                "vel":[0.0,0.0],
                "components":{"pos":[75.0,105.0],"vel":[0.0,0.0]}
            }),
        );

        assert!(
            !state.entities.contains_key("ship"),
            "receiver must not adopt a MeshHandoff into an unowned grid cell"
        );
        assert!(
            rx.try_recv().is_err(),
            "unowned target must not be ACKed as adopted"
        );
        assert!(state.rejected.iter().any(|r| {
            r.get("reason").and_then(|v| v.as_str()) == Some("mesh handoff target region not owned")
                && r.get("target").and_then(|v| v.as_str()) == Some("Z3_3")
        }));
    }

    #[test]
    fn mesh_handoff_existing_entity_exact_duplicate_is_reacked() {
        let mut state = ServerState::new(30.0);
        add_test_worker(&mut state, "east-worker", "E");
        let mut rx = add_test_worker_with_rx(&mut state, "mesh-link", "MESH");
        let mut ship = test_entity([5.0, 0.0], "E");
        set_component_authority_epoch(&mut ship, "pos", 9);
        set_component_authority_epoch(&mut ship, "vel", 9);
        state.entities.insert("ship".to_string(), ship);

        dispatch_inner(
            &mut state,
            "mesh-link",
            &json!({
                "op":"MeshHandoff",
                "entity":"ship",
                "source_region":"W",
                "target":"E",
                "source_durable_gen":7,
                "lease_epoch":1,
                "authority_epoch":9,
                "pos":[-5.0,0.0],
                "vel":[0.0,0.0],
                "components":{"pos":[-5.0,0.0],"vel":[0.0,0.0]}
            }),
        );

        let ack = decode_test_frame(&rx.try_recv().expect("exact duplicate should be re-ACKed"));
        assert_eq!(ack.get("op").and_then(|v| v.as_str()), Some("MeshAck"));
        assert_eq!(ack.get("entity").and_then(|v| v.as_str()), Some("ship"));
        assert!(state.rejected.is_empty());
    }

    #[test]
    fn mesh_handoff_existing_entity_mismatch_must_not_ack_as_idempotent() {
        let mut state = ServerState::new(30.0);
        add_test_worker(&mut state, "east-worker", "E");
        let mut rx = add_test_worker_with_rx(&mut state, "mesh-link", "MESH");
        let mut ship = test_entity([5.0, 0.0], "E");
        set_component_authority_epoch(&mut ship, "pos", 3);
        set_component_authority_epoch(&mut ship, "vel", 3);
        state.entities.insert("ship".to_string(), ship);

        dispatch_inner(
            &mut state,
            "mesh-link",
            &json!({
                "op":"MeshHandoff",
                "entity":"ship",
                "source_region":"W",
                "target":"E",
                "source_durable_gen":7,
                "lease_epoch":1,
                "authority_epoch":9,
                "pos":[-5.0,0.0],
                "vel":[0.0,0.0],
                "components":{"pos":[-5.0,0.0],"vel":[0.0,0.0]}
            }),
        );

        assert!(
            rx.try_recv().is_err(),
            "mismatched existing entity must not be ACKed"
        );
        assert!(state.rejected.iter().any(|r| {
            r.get("reason").and_then(|v| v.as_str())
                == Some("mesh handoff existing-entity mismatch")
        }));
        assert_eq!(component_authority_epoch(&state.entities["ship"], "pos"), 3);
    }

    #[test]
    fn mesh_handoff_old_upstream_resend_after_onward_forward_is_reacked_without_readopt() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "gw_mesh_forwarded_fence_{}_{}",
            std::process::id(),
            unique
        ));
        let _ = std::fs::create_dir_all(&dir);
        let wal_path = dir.join("test.wal").to_string_lossy().to_string();

        let mut state = ServerState::new(30.0);
        state.wal_path = wal_path.clone();
        state.wal = Some(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&wal_path)
                .unwrap(),
        );
        state.ensure_wal_header();
        add_test_worker(&mut state, "east-worker", "E");
        let mut upstream_rx = add_test_worker_with_rx(&mut state, "mesh-link-W", "MESH");

        let upstream_handoff = json!({
            "op":"MeshHandoff",
            "entity":"ship",
            "source_region":"W",
            "target":"E",
            "source_durable_gen":7,
            "lease_epoch":1,
            "authority_epoch":9,
            "pos":[5.0,0.0],
            "vel":[1.0,0.0],
            "components":{"pos":[5.0,0.0],"vel":[1.0,0.0]}
        });

        state.mesh_ack_drop = true;
        dispatch_inner(&mut state, "mesh-link-W", &upstream_handoff);
        assert!(
            upstream_rx.try_recv().is_err(),
            "first ACK is intentionally lost"
        );
        assert!(state.entities.contains_key("ship"));

        state.mesh_ack_drop = false;
        let (tx_f, mut rx_f) = mpsc::unbounded_channel();
        state.mesh.insert("F".to_string(), tx_f);
        mesh_forward(&mut state, "ship", "F");
        let onward = decode_test_frame(
            &rx_f
                .try_recv()
                .expect("onward MeshHandoff to F must be emitted"),
        );
        assert_eq!(onward["op"], "MeshHandoff");
        let forwarded_epoch = state
            .mesh_forwarded_epoch
            .get("ship")
            .copied()
            .expect("onward mesh_forward must record the departure fence");
        assert!(forwarded_epoch > 9);
        assert!(record_mesh_ack(&mut state, "ship"));
        assert!(!state.entities.contains_key("ship"));
        assert!(!state.pending_mesh.contains_key("ship"));
        state.snapshot_seen = false;
        state.wal_compact_bytes = 1;
        state.wal_bytes = state.wal_bytes.max(4096);
        state.maybe_compact_wal();
        assert_eq!(
            state.metrics.wal_compactions, 1,
            "compaction must preserve the forwarded fence, not force WAL growth forever"
        );

        drop(state.wal.take());
        let (_store, _deleted, _cfg, recovered_pending, recovered_forwarded, _id_hwm, report) =
            recover_from_wal_report(&wal_path, None);
        assert!(report.error.is_none(), "{:?}", report.error);
        assert!(
            recovered_pending.is_empty(),
            "the onward handoff was ACKed, so restart must not resend E->F"
        );
        assert_eq!(
            recovered_forwarded.get("ship").copied(),
            Some(forwarded_epoch),
            "restart must preserve the fact that E already forwarded this lineage onward"
        );

        let mut recovered = ServerState::new(30.0);
        recovered.mesh_forwarded_epoch = recovered_forwarded;
        add_test_worker(&mut recovered, "east-worker", "E");
        let mut recovered_upstream_rx =
            add_test_worker_with_rx(&mut recovered, "mesh-link-W", "MESH");

        dispatch_inner(&mut recovered, "mesh-link-W", &upstream_handoff);

        let ack = decode_test_frame(
            &recovered_upstream_rx
                .try_recv()
                .expect("old upstream resend should be ACKed so W clears pending_mesh"),
        );
        assert_eq!(ack["op"], "MeshAck");
        assert_eq!(ack["entity"], "ship");
        assert!(
            !recovered.entities.contains_key("ship"),
            "old W->E resend must not recreate ship on E after E already forwarded it to F"
        );
        assert!(recovered.rejected.iter().any(|r| {
            r.get("reason").and_then(|v| v.as_str())
                == Some("mesh handoff stale after onward forward")
        }));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn update_component_wal_fail_does_not_advance_ram() {
        let mut state = ServerState::new(30.0);
        state.wal_fail_inject = true;
        state.wal_degraded = false;
        add_test_worker(&mut state, "w1", "W");
        state
            .entities
            .insert("avatar".to_string(), test_entity([-1.0, 2.0], "W"));

        let ok = apply_one_update(
            &mut state,
            "w1",
            "avatar",
            "pos",
            json!([-3.0, 100.0]),
            None,
        );

        assert!(!ok);
        let e = state.entities.get("avatar").unwrap();
        assert_eq!(e.pos, [-1.0, 2.0]);
        assert_eq!(e.version, 1);
        assert!(state.wal_degraded);
    }

    #[test]
    fn batch_update_wal_fail_does_not_advance_ram() {
        let mut state = ServerState::new(30.0);
        state.wal_fail_inject = true;
        state.wal_degraded = false;
        add_test_worker(&mut state, "w1", "W");
        state
            .entities
            .insert("a".to_string(), test_entity([-1.0, 0.0], "W"));
        state
            .entities
            .insert("b".to_string(), test_entity([-2.0, 0.0], "W"));

        dispatch_test_frame(
            &mut state,
            "w1",
            &json!({"op":"BatchUpdate","comp":"pos","updates":[["a",[10.0,0.0]],["b",[11.0,0.0]]]}),
        );

        assert_eq!(state.entities["a"].pos, [-1.0, 0.0]);
        assert_eq!(state.entities["b"].pos, [-2.0, 0.0]);
        assert_eq!(state.entities["a"].version, 1);
        assert_eq!(state.entities["b"].version, 1);
        assert!(state.wal_degraded);
    }

    #[test]
    fn update_component_waits_for_durable_watermark_flush() {
        let mut state = ServerState::new(30.0);
        add_test_worker(&mut state, "w1", "W");
        state
            .entities
            .insert("avatar".to_string(), test_entity([-1.0, 2.0], "W"));

        let ok = apply_one_update(
            &mut state,
            "w1",
            "avatar",
            "pos",
            json!([-3.0, 100.0]),
            None,
        );

        assert!(ok);
        assert_eq!(state.pending_updates.len(), 1);
        assert_eq!(state.pending_gen, 1);
        assert_eq!(state.durable_gen, 0);
        assert_eq!(state.entities["avatar"].pos, [-1.0, 2.0]);
        assert_eq!(state.entities["avatar"].version, 1);

        flush_pending_updates(&mut state);

        assert!(state.pending_updates.is_empty());
        assert_eq!(state.durable_gen, 1);
        assert_eq!(state.entities["avatar"].pos, [-3.0, 100.0]);
        assert_eq!(state.entities["avatar"].version, 2);
    }

    #[test]
    fn single_updates_group_until_one_watermark_flush() {
        let mut state = ServerState::new(30.0);
        add_test_worker(&mut state, "w1", "W");
        state
            .entities
            .insert("avatar".to_string(), test_entity([-1.0, 2.0], "W"));

        assert!(apply_one_update(
            &mut state,
            "w1",
            "avatar",
            "pos",
            json!([-3.0, 20.0]),
            None,
        ));
        assert!(apply_one_update(
            &mut state,
            "w1",
            "avatar",
            "pos",
            json!([-4.0, 40.0]),
            None,
        ));

        assert_eq!(state.pending_updates.len(), 2);
        assert_eq!(state.pending_gen, 2);
        assert_eq!(state.durable_gen, 0);
        assert_eq!(state.entities["avatar"].pos, [-1.0, 2.0]);

        flush_pending_updates(&mut state);

        assert_eq!(state.entities["avatar"].pos, [-4.0, 40.0]);
        assert_eq!(state.entities["avatar"].version, 3);
        assert_eq!(state.durable_gen, 2);
    }

    #[test]
    fn watermark_sync_fail_does_not_advance_ram() {
        let mut state = ServerState::new(30.0);
        add_test_worker(&mut state, "w1", "W");
        state
            .entities
            .insert("avatar".to_string(), test_entity([-1.0, 2.0], "W"));

        assert!(apply_one_update(
            &mut state,
            "w1",
            "avatar",
            "pos",
            json!([-3.0, 100.0]),
            None,
        ));
        state.wal_fail_inject = true;

        flush_pending_updates(&mut state);

        assert!(state.pending_updates.is_empty());
        assert_eq!(state.durable_gen, 0);
        assert_eq!(state.entities["avatar"].pos, [-1.0, 2.0]);
        assert_eq!(state.entities["avatar"].version, 1);
        assert!(state.wal_degraded);
    }

    #[test]
    fn delete_entity_wal_fail_does_not_tombstone_or_remove() {
        let mut state = ServerState::new(30.0);
        state.wal_fail_inject = true;
        state.wal_degraded = false;
        add_test_worker(&mut state, "w1", "W");
        state
            .entities
            .insert("victim".to_string(), test_entity([-1.0, 0.0], "W"));

        dispatch_test_frame(
            &mut state,
            "w1",
            &json!({"op":"DeleteEntity","entity":"victim"}),
        );

        assert!(state.entities.contains_key("victim"));
        assert!(!state.deleted_entities.contains("victim"));
        assert!(state.wal_degraded);
    }

    #[test]
    fn handoff_atomic_under_watermark() {
        let mut state = ServerState::new(30.0);
        add_test_worker(&mut state, "w1", "W");
        add_test_worker(&mut state, "w2", "E");
        state
            .entities
            .insert("ship".to_string(), test_entity([-1.0, 0.0], "W"));
        grant_region_physics_island_authority(&mut state, "w1", "ship");

        assert!(handoff_with_position(
            &mut state,
            "ship",
            "W",
            "E",
            Some([2.0, 0.0])
        ));

        assert_eq!(state.pending_handoffs.len(), 1);
        assert_eq!(state.durable_gen, 0);
        assert_eq!(state.entities["ship"].region, "W");
        assert_eq!(state.entities["ship"].pos, [-1.0, 0.0]);
        assert_eq!(
            component_authority_owner(&state.entities["ship"], "pos"),
            Some("w1".to_string())
        );

        flush_pending_handoffs(&mut state);

        assert!(state.pending_handoffs.is_empty());
        assert_eq!(state.durable_gen, 1);
        assert_eq!(state.entities["ship"].region, "E");
        assert_eq!(state.entities["ship"].pos, [2.0, 0.0]);
        assert_eq!(
            component_authority_owner(&state.entities["ship"], "pos"),
            Some("w2".to_string())
        );
        assert!(!state.workers["w1"]
            .authority_epochs
            .contains_key(&authority_key("ship", "pos")));
        assert!(state.workers["w2"]
            .authority_epochs
            .contains_key(&authority_key("ship", "pos")));
    }

    #[test]
    fn physics_island_handoff_fences_all_components() {
        let mut state = ServerState::new(30.0);
        add_test_worker(&mut state, "w1", "W");
        add_test_worker(&mut state, "w2", "E");
        let mut ship = test_physics_island_entity([-1.0, 0.0], "W");
        let old_epochs = stamp_expanded_physics_island_epochs(&mut ship);
        let expected_epoch = old_epochs.values().copied().max().unwrap() + 1;
        state.entities.insert("ship".to_string(), ship);
        grant_region_physics_island_authority(&mut state, "w1", "ship");

        for comp in expanded_physics_island_components() {
            assert_eq!(
                component_authority_owner(&state.entities["ship"], comp),
                Some("w1".to_string()),
                "{comp}"
            );
            assert_eq!(
                state.workers["w1"]
                    .authority_epochs
                    .get(&authority_key("ship", comp))
                    .copied(),
                Some(old_epochs[comp]),
                "{comp}"
            );
        }

        assert!(handoff_with_position(
            &mut state,
            "ship",
            "W",
            "E",
            Some([2.0, 0.0])
        ));

        let expected_comps: Vec<String> = expanded_physics_island_components()
            .into_iter()
            .map(|s| s.to_string())
            .collect();
        assert_eq!(state.pending_handoffs.len(), 1);
        assert_eq!(state.pending_handoffs[0].moved_comps, expected_comps);
        assert_eq!(state.pending_handoffs[0].authority_epoch, expected_epoch);
        let pending_authority = state.pending_handoffs[0]
            .authority
            .as_object()
            .expect("pending handoff must carry an authority snapshot");
        for comp in expanded_physics_island_components() {
            let spec = pending_authority
                .get(comp)
                .unwrap_or_else(|| panic!("missing pending authority for {comp}"));
            assert_eq!(spec["owner"], "w2", "{comp}");
            assert_eq!(spec["authority_epoch"], expected_epoch, "{comp}");
        }

        flush_pending_handoffs(&mut state);

        for comp in expanded_physics_island_components() {
            assert_eq!(
                component_authority_owner(&state.entities["ship"], comp),
                Some("w2".to_string()),
                "{comp}"
            );
            assert_eq!(
                component_authority_epoch(&state.entities["ship"], comp),
                expected_epoch,
                "{comp}"
            );
            assert!(
                !state.workers["w1"]
                    .authority_epochs
                    .contains_key(&authority_key("ship", comp)),
                "old owner still has cached authority for {comp}"
            );
            assert_eq!(
                state.workers["w2"]
                    .authority_epochs
                    .get(&authority_key("ship", comp))
                    .copied(),
                Some(expected_epoch),
                "{comp}"
            );
            assert!(
                !apply_one_update(
                    &mut state,
                    "w1",
                    "ship",
                    comp,
                    physics_island_update_value(comp),
                    Some(old_epochs[comp]),
                ),
                "old owner write must be fenced for {comp}"
            );
            assert!(
                apply_one_update(
                    &mut state,
                    "w2",
                    "ship",
                    comp,
                    physics_island_update_value(comp),
                    Some(expected_epoch),
                ),
                "new owner write must apply for {comp}"
            );
        }
        flush_pending_updates(&mut state);

        assert_eq!(state.entities["ship"].pos, [3.0, 0.0]);
        assert_eq!(state.entities["ship"].vel, [0.0, 2.0]);
        assert_eq!(state.entities["ship"].components["rot"], json!(0.5));
        assert_eq!(state.entities["ship"].components["lin"], json!([2.0, 0.0]));
        assert_eq!(state.entities["ship"].components["ang"], json!(0.75));
        assert_eq!(state.entities["ship"].components["at_rest"], json!(true));
    }

    #[test]
    fn physics_handoff_preserves_gameplay_authority() {
        let mut state = ServerState::new(30.0);
        add_test_worker(&mut state, "w1", "W");
        add_test_worker(&mut state, "w2", "E");
        add_test_worker(&mut state, "gameplay", "GAME");
        let mut ship = test_physics_island_entity([-1.0, 0.0], "W");
        ship.components.insert(
            "inventory".to_string(),
            json!({"iron": 3, "atlas_shards": 1}),
        );
        ship.components
            .insert("health".to_string(), json!({"hp": 100}));
        ensure_component_authority(&mut ship, "inventory");
        ensure_component_authority(&mut ship, "health");
        set_component_authority_epoch(&mut ship, "inventory", 29);
        set_component_authority_epoch(&mut ship, "health", 31);
        state.entities.insert("ship".to_string(), ship);
        grant_region_physics_island_authority(&mut state, "w1", "ship");
        grant_authority(&mut state, "gameplay", "ship", "inventory");
        grant_authority(&mut state, "gameplay", "ship", "health");
        let inventory_epoch = component_authority_epoch(&state.entities["ship"], "inventory");
        let health_epoch = component_authority_epoch(&state.entities["ship"], "health");
        let old_physics_epochs = stamp_expanded_physics_island_epochs(
            state.entities.get_mut("ship").expect("seeded ship"),
        );
        grant_region_physics_island_authority(&mut state, "w1", "ship");

        assert_eq!(inventory_epoch, 29);
        assert_eq!(health_epoch, 31);
        assert_eq!(
            component_authority_owner(&state.entities["ship"], "inventory"),
            Some("gameplay".to_string())
        );
        assert_eq!(
            component_authority_owner(&state.entities["ship"], "health"),
            Some("gameplay".to_string())
        );
        assert_eq!(
            state.workers["gameplay"]
                .authority_epochs
                .get(&authority_key("ship", "inventory"))
                .copied(),
            Some(inventory_epoch)
        );
        assert_eq!(
            state.workers["gameplay"]
                .authority_epochs
                .get(&authority_key("ship", "health"))
                .copied(),
            Some(health_epoch)
        );

        assert!(handoff_with_position(
            &mut state,
            "ship",
            "W",
            "E",
            Some([2.0, 0.0])
        ));

        assert_eq!(state.pending_handoffs.len(), 1);
        assert!(!state.pending_handoffs[0]
            .moved_comps
            .contains(&"inventory".to_string()));
        assert!(!state.pending_handoffs[0]
            .moved_comps
            .contains(&"health".to_string()));
        let pending_authority = state.pending_handoffs[0]
            .authority
            .as_object()
            .expect("pending handoff must carry an authority snapshot");
        assert_eq!(pending_authority["inventory"]["owner"], "gameplay");
        assert_eq!(
            pending_authority["inventory"]["authority_epoch"],
            inventory_epoch
        );
        assert_eq!(pending_authority["inventory"]["mode"], "server_arbitrated");
        assert_eq!(pending_authority["health"]["owner"], "gameplay");
        assert_eq!(pending_authority["health"]["authority_epoch"], health_epoch);
        assert_eq!(pending_authority["health"]["mode"], "server_arbitrated");

        flush_pending_handoffs(&mut state);

        for comp in expanded_physics_island_components() {
            assert_eq!(
                component_authority_owner(&state.entities["ship"], comp),
                Some("w2".to_string()),
                "{comp}"
            );
            assert!(
                !state.workers["w1"]
                    .authority_epochs
                    .contains_key(&authority_key("ship", comp)),
                "old physics owner still has cached authority for {comp}"
            );
            assert!(state.workers["w2"]
                .authority_epochs
                .contains_key(&authority_key("ship", comp)));
        }
        assert_eq!(
            component_authority_owner(&state.entities["ship"], "inventory"),
            Some("gameplay".to_string())
        );
        assert_eq!(
            component_authority_epoch(&state.entities["ship"], "inventory"),
            inventory_epoch
        );
        assert_eq!(
            component_authority_mode(&state.entities["ship"], "inventory"),
            AuthorityMode::ServerArbitrated
        );
        assert_eq!(
            component_authority_owner(&state.entities["ship"], "health"),
            Some("gameplay".to_string())
        );
        assert_eq!(
            component_authority_epoch(&state.entities["ship"], "health"),
            health_epoch
        );
        assert_eq!(
            component_authority_mode(&state.entities["ship"], "health"),
            AuthorityMode::ServerArbitrated
        );
        assert!(state.workers["gameplay"]
            .authority_epochs
            .contains_key(&authority_key("ship", "inventory")));
        assert!(state.workers["gameplay"]
            .authority_epochs
            .contains_key(&authority_key("ship", "health")));
        assert!(!state.workers["w2"]
            .authority_epochs
            .contains_key(&authority_key("ship", "inventory")));
        assert!(!state.workers["w2"]
            .authority_epochs
            .contains_key(&authority_key("ship", "health")));
        assert!(!apply_one_update(
            &mut state,
            "w1",
            "ship",
            "pos",
            json!([4.0, 0.0]),
            Some(old_physics_epochs["pos"]),
        ));
        assert!(!apply_one_update(
            &mut state,
            "w2",
            "ship",
            "health",
            json!({"hp": 1}),
            Some(health_epoch),
        ));
        assert!(apply_one_update(
            &mut state,
            "gameplay",
            "ship",
            "inventory",
            json!({"iron": 4, "atlas_shards": 1}),
            Some(inventory_epoch),
        ));
        assert!(apply_one_update(
            &mut state,
            "gameplay",
            "ship",
            "health",
            json!({"hp": 90}),
            Some(health_epoch),
        ));
        flush_pending_updates(&mut state);
        assert_eq!(
            state.entities["ship"].components["inventory"],
            json!({"iron": 4, "atlas_shards": 1})
        );
        assert_eq!(
            state.entities["ship"].components["health"],
            json!({"hp": 90})
        );
    }

    #[test]
    fn handoff_sync_fail_keeps_old_owner_and_pending_retry() {
        let mut state = ServerState::new(30.0);
        add_test_worker(&mut state, "w1", "W");
        add_test_worker(&mut state, "w2", "E");
        state
            .entities
            .insert("ship".to_string(), test_entity([-1.0, 0.0], "W"));
        grant_region_physics_island_authority(&mut state, "w1", "ship");

        assert!(handoff_with_position(
            &mut state,
            "ship",
            "W",
            "E",
            Some([2.0, 0.0])
        ));
        state.wal_fail_inject = true;
        flush_pending_handoffs(&mut state);

        assert_eq!(state.pending_handoffs.len(), 1);
        assert_eq!(state.durable_gen, 0);
        assert_eq!(state.entities["ship"].region, "W");
        assert_eq!(state.entities["ship"].pos, [-1.0, 0.0]);
        assert_eq!(
            component_authority_owner(&state.entities["ship"], "pos"),
            Some("w1".to_string())
        );
        assert!(state.wal_degraded);

        state.wal_fail_inject = false;
        state.wal_degraded = false;
        flush_pending_handoffs(&mut state);

        assert!(state.pending_handoffs.is_empty());
        assert_eq!(state.entities["ship"].region, "E");
        assert_eq!(state.entities["ship"].pos, [2.0, 0.0]);
        assert_eq!(
            component_authority_owner(&state.entities["ship"], "pos"),
            Some("w2".to_string())
        );
    }

    #[test]
    fn delete_tombstone_cancels_pending_local_handoff() {
        let mut state = ServerState::new(30.0);
        add_test_worker(&mut state, "w1", "W");
        let mut new_owner_rx = add_test_worker_with_rx(&mut state, "w2", "E");
        state
            .entities
            .insert("ship".to_string(), test_entity([-1.0, 0.0], "W"));
        grant_region_physics_island_authority(&mut state, "w1", "ship");

        assert!(handoff_with_position(
            &mut state,
            "ship",
            "W",
            "E",
            Some([2.0, 0.0])
        ));
        assert_eq!(state.pending_handoffs.len(), 1);

        dispatch_inner(
            &mut state,
            "w1",
            &json!({"op":"DeleteEntity","entity":"ship"}),
        );

        assert!(state.deleted_entities.contains("ship"));
        assert!(!state.entities.contains_key("ship"));
        assert!(state.pending_handoffs.is_empty());

        flush_pending_handoffs(&mut state);

        assert_eq!(state.metrics.handoffs, 0);
        assert!(
            new_owner_rx.try_recv().is_err(),
            "cancelled pending handoff must not grant authority to the new owner after delete"
        );
    }

    #[test]
    fn delete_tombstone_dominates_queued_handoff_after_recovery() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "gw_delete_handoff_recovery_{}_{}",
            std::process::id(),
            unique
        ));
        let _ = std::fs::create_dir_all(&dir);
        let wal_path = dir.join("test.wal").to_string_lossy().to_string();

        let mut state = ServerState::new(30.0);
        state.wal_path = wal_path.clone();
        state.wal = Some(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&wal_path)
                .unwrap(),
        );
        add_test_worker(&mut state, "w1", "W");
        add_test_worker(&mut state, "w2", "E");
        assert!(spawn_in_region(
            &mut state,
            "ship",
            [-1.0, 0.0],
            [1.0, 0.0],
            Map::new(),
            Some("W"),
            SpawnAuthoritySeed::default(),
        ));
        grant_region_physics_island_authority(&mut state, "w1", "ship");

        assert!(handoff_with_position(
            &mut state,
            "ship",
            "W",
            "E",
            Some([2.0, 0.0])
        ));
        assert_eq!(state.pending_handoffs.len(), 1);

        dispatch_inner(
            &mut state,
            "w1",
            &json!({"op":"DeleteEntity","entity":"ship"}),
        );
        assert!(state.deleted_entities.contains("ship"));
        assert!(!state.entities.contains_key("ship"));
        assert!(state.pending_handoffs.is_empty());
        drop(state.wal.take());

        let read = read_wal_events(&wal_path, None);
        assert!(read.report.error.is_none(), "{:?}", read.report.error);
        let transfer_idx = read
            .events
            .iter()
            .position(|ev| ev.get("kind").and_then(|v| v.as_str()) == Some("transfer"))
            .expect("queued handoff must be present in the WAL");
        let delete_idx = read
            .events
            .iter()
            .position(|ev| ev.get("kind").and_then(|v| v.as_str()) == Some("delete_tombstone"))
            .expect("delete tombstone must be present in the WAL");
        assert!(
            transfer_idx < delete_idx,
            "the tombstone must be the later durable transition and dominate replay"
        );

        let (entities, tombstones, _cfg, pending_mesh, forwarded, _id_hwm, report) =
            recover_from_wal_report(&wal_path, None);
        assert!(
            report.error.is_none(),
            "recovery must not error: {:?}",
            report.error
        );
        assert!(
            tombstones.contains("ship"),
            "delete tombstone must survive replay"
        );
        assert!(
            !entities.contains_key("ship"),
            "recovery must not resurrect a queued handoff after a later delete"
        );
        assert!(
            !pending_mesh.contains_key("ship"),
            "local handoff/delete recovery must not produce cross-mesh resend state"
        );
        assert!(
            !forwarded.contains_key("ship"),
            "local handoff/delete recovery must not create a mesh-forward fence"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn prepared_handoff_missing_entity_records_rejection() {
        let mut state = ServerState::new(30.0);
        add_test_worker(&mut state, "w1", "W");
        add_test_worker(&mut state, "w2", "E");
        state
            .entities
            .insert("ship".to_string(), test_entity([-1.0, 0.0], "W"));
        grant_region_physics_island_authority(&mut state, "w1", "ship");

        assert!(handoff_with_position(
            &mut state,
            "ship",
            "W",
            "E",
            Some([2.0, 0.0])
        ));
        state.entities.remove("ship");
        state.deleted_entities.insert("ship".to_string());

        flush_pending_handoffs(&mut state);

        assert!(state.pending_handoffs.is_empty());
        assert_eq!(state.metrics.handoffs, 0);
        assert!(state.rejected.iter().any(|r| {
            r.get("reason").and_then(|v| v.as_str())
                == Some("prepared handoff target entity missing or deleted before durable apply")
                && r.get("entity").and_then(|v| v.as_str()) == Some("ship")
        }));
    }

    #[test]
    fn stale_old_owner_write_rejected_by_epoch_after_recovered_handoff() {
        let dir = std::env::temp_dir().join(format!("gw_handoff_epoch_{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let wal_path = dir.join("test.wal").to_string_lossy().to_string();

        let mut state = ServerState::new(30.0);
        state.wal_path = wal_path.clone();
        state.wal = Some(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&wal_path)
                .unwrap(),
        );
        add_test_worker(&mut state, "w1", "W");
        add_test_worker(&mut state, "w2", "E");
        assert!(spawn_in_region(
            &mut state,
            "ship",
            [-1.0, 0.0],
            [1.0, 0.0],
            Map::new(),
            Some("W"),
            SpawnAuthoritySeed::default(),
        ));
        grant_region_physics_island_authority(&mut state, "w1", "ship");
        let old_epoch = component_authority_epoch(&state.entities["ship"], "pos");

        assert!(handoff_with_position(
            &mut state,
            "ship",
            "W",
            "E",
            Some([2.0, 0.0])
        ));
        flush_pending_handoffs(&mut state);
        let new_epoch = component_authority_epoch(&state.entities["ship"], "pos");
        assert!(new_epoch > old_epoch);
        drop(state.wal.take());

        let (entities, _deleted, _cfg, _pending, _forwarded, _id_hwm, report) =
            recover_from_wal_report(&wal_path, None);
        assert!(
            report.error.is_none(),
            "recovery must not error: {:?}",
            report.error
        );

        let mut recovered = ServerState::new(30.0);
        recovered.entities = entities;
        add_test_worker(&mut recovered, "w1", "W");
        add_test_worker(&mut recovered, "w2", "E");

        assert!(!apply_one_update(
            &mut recovered,
            "w1",
            "ship",
            "pos",
            json!([9.0, 0.0]),
            Some(old_epoch),
        ));
        assert_eq!(recovered.entities["ship"].pos, [2.0, 0.0]);

        assert!(apply_one_update(
            &mut recovered,
            "w2",
            "ship",
            "pos",
            json!([3.0, 0.0]),
            Some(new_epoch),
        ));
        flush_pending_updates(&mut recovered);
        assert_eq!(recovered.entities["ship"].pos, [3.0, 0.0]);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn failover_grant_only_bumps_epoch_and_old_owner_rejected_on_reconnect() {
        let dir = std::env::temp_dir().join(format!("gw_failover_epoch_{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let wal_path = dir.join("test.wal").to_string_lossy().to_string();

        let mut state = ServerState::new(30.0);
        state.wal_path = wal_path.clone();
        state.wal = Some(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&wal_path)
                .unwrap(),
        );
        add_test_worker(&mut state, "w1", "W");
        add_test_worker(&mut state, "w2", "SPARE");
        state.standbys.push("w2".to_string());
        assert!(spawn_in_region(
            &mut state,
            "ship",
            [-1.0, 0.0],
            [1.0, 0.0],
            Map::new(),
            Some("W"),
            SpawnAuthoritySeed::default(),
        ));
        grant_region_physics_island_authority(&mut state, "w1", "ship");
        let old_epoch = component_authority_epoch(&state.entities["ship"], "pos");
        state
            .region_expires
            .insert("W".to_string(), Instant::now() - Duration::from_millis(1));

        check_leases(&mut state);

        assert_eq!(state.pending_failovers.len(), 1);
        assert_eq!(state.region_worker.get("W").map(|s| s.as_str()), Some("w1"));
        assert_eq!(
            component_authority_owner(&state.entities["ship"], "pos"),
            Some("w1".to_string())
        );
        assert_eq!(state.metrics.failovers, 0);

        flush_pending_failovers(&mut state);

        assert!(state.pending_failovers.is_empty());
        assert_eq!(state.region_worker.get("W").map(|s| s.as_str()), Some("w2"));
        assert_eq!(state.workers["w2"].region, "W");
        assert_eq!(state.metrics.failovers, 1);
        let new_epoch = component_authority_epoch(&state.entities["ship"], "pos");
        assert!(new_epoch > old_epoch);
        assert_eq!(
            component_authority_owner(&state.entities["ship"], "pos"),
            Some("w2".to_string())
        );
        drop(state.wal.take());

        let (entities, _deleted, _cfg, _pending, _forwarded, _id_hwm, report) =
            recover_from_wal_report(&wal_path, None);
        assert!(
            report.error.is_none(),
            "recovery must not error: {:?}",
            report.error
        );
        let recovered_ship = entities.get("ship").unwrap();
        assert_eq!(component_authority_epoch(recovered_ship, "pos"), new_epoch);
        assert_eq!(
            component_authority_owner(recovered_ship, "pos"),
            Some("w2".to_string())
        );

        let mut state = ServerState::new(30.0);
        state.entities = entities;
        add_test_worker(&mut state, "w1", "W");
        add_test_worker(&mut state, "w2", "SPARE");

        assert!(!apply_one_update(
            &mut state,
            "w1",
            "ship",
            "pos",
            json!([9.0, 0.0]),
            Some(old_epoch),
        ));
        assert_eq!(state.entities["ship"].pos, [-1.0, 0.0]);

        assert!(apply_one_update(
            &mut state,
            "w2",
            "ship",
            "pos",
            json!([3.0, 0.0]),
            Some(new_epoch),
        ));
        flush_pending_updates(&mut state);
        assert_eq!(state.entities["ship"].pos, [3.0, 0.0]);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn rebalance_2d_block_migration_atomic_all_or_nothing() {
        let mut failed = ServerState::new(30.0);
        let _ = seed_2d_rebalance_state(&mut failed);
        rebalance_2d(&mut failed);
        assert_eq!(failed.pending_block_migrations.len(), 1);
        let fail_block = failed.pending_block_migrations[0].block.clone();
        let fail_eids: Vec<String> = failed
            .entities
            .iter()
            .filter(|(_, e)| e.region == fail_block)
            .map(|(eid, _)| eid.clone())
            .collect();
        assert_eq!(fail_eids.len(), 4);
        failed.wal_fail_inject = true;
        flush_pending_block_migrations(&mut failed);

        assert!(failed.pending_block_migrations.is_empty());
        assert_eq!(failed.durable_gen, 0);
        assert_eq!(
            failed.region_worker.get(&fail_block).map(|s| s.as_str()),
            Some("hot")
        );
        for eid in &fail_eids {
            assert_eq!(
                component_authority_owner(&failed.entities[eid], "pos"),
                Some("hot".to_string())
            );
        }
        assert!(failed.wal_degraded);

        let dir = std::env::temp_dir().join(format!("gw_rebalance_block_{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let wal_path = dir.join("test.wal").to_string_lossy().to_string();

        let mut state = ServerState::new(30.0);
        state.wal_path = wal_path.clone();
        state.wal = Some(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&wal_path)
                .unwrap(),
        );
        let _ = seed_2d_rebalance_state(&mut state);

        rebalance_2d(&mut state);

        assert_eq!(state.pending_block_migrations.len(), 1);
        assert_eq!(state.durable_gen, 0);
        let block = state.pending_block_migrations[0].block.clone();
        let block_eids: Vec<String> = state
            .entities
            .iter()
            .filter(|(_, e)| e.region == block)
            .map(|(eid, _)| eid.clone())
            .collect();
        assert_eq!(block_eids.len(), 4);
        assert_eq!(
            state.region_worker.get(&block).map(|s| s.as_str()),
            Some("hot")
        );
        let old_epochs: HashMap<String, u64> = block_eids
            .iter()
            .map(|eid| {
                (
                    eid.clone(),
                    component_authority_epoch(&state.entities[eid], "pos"),
                )
            })
            .collect();
        for eid in &block_eids {
            assert_eq!(
                component_authority_owner(&state.entities[eid], "pos"),
                Some("hot".to_string())
            );
        }

        flush_pending_block_migrations(&mut state);

        assert!(state.pending_block_migrations.is_empty());
        assert_eq!(state.durable_gen, 1);
        assert_eq!(
            state.region_worker.get(&block).map(|s| s.as_str()),
            Some("cold")
        );
        assert_eq!(state.workers["cold"].region, block);
        let new_epochs: HashMap<String, u64> = block_eids
            .iter()
            .map(|eid| {
                let epoch = component_authority_epoch(&state.entities[eid], "pos");
                assert!(epoch > old_epochs[eid]);
                assert_eq!(
                    component_authority_owner(&state.entities[eid], "pos"),
                    Some("cold".to_string())
                );
                assert!(!state.workers["hot"]
                    .authority_epochs
                    .contains_key(&authority_key(eid, "pos")));
                assert!(state.workers["cold"]
                    .authority_epochs
                    .contains_key(&authority_key(eid, "pos")));
                (eid.clone(), epoch)
            })
            .collect();
        drop(state.wal.take());

        let (entities, _deleted, _cfg, _pending, _forwarded, _id_hwm, report) =
            recover_from_wal_report(&wal_path, None);
        assert!(
            report.error.is_none(),
            "recovery must not error: {:?}",
            report.error
        );
        for eid in &block_eids {
            let recovered = entities.get(eid).unwrap();
            assert_eq!(recovered.region, block);
            assert_eq!(component_authority_epoch(recovered, "pos"), new_epochs[eid]);
            assert_eq!(
                component_authority_owner(recovered, "pos"),
                Some("cold".to_string())
            );
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn reserve_entity_ids_persists_high_watermark_before_response() {
        let dir = std::env::temp_dir().join(format!("gw_reserve_ids_{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let wal_path = dir.join("test.wal").to_string_lossy().to_string();

        let mut state = ServerState::new(30.0);
        state.wal_path = wal_path.clone();
        state.wal = Some(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&wal_path)
                .unwrap(),
        );
        add_test_worker(&mut state, "w1", "W");

        dispatch_test_frame(
            &mut state,
            "w1",
            &json!({"op":"ReserveEntityIds","request_id":"r1","count":7}),
        );
        assert_eq!(state.entity_id_reservations, 7);
        drop(state.wal.take());

        let (_entities, _deleted, _cfg, _pending, _forwarded, id_hwm, report) =
            recover_from_wal_report(&wal_path, None);
        assert!(report.error.is_none());
        assert_eq!(id_hwm, 7);

        let mut failed = ServerState::new(30.0);
        failed.wal_fail_inject = true;
        failed.wal_degraded = false;
        add_test_worker(&mut failed, "w1", "W");
        dispatch_test_frame(
            &mut failed,
            "w1",
            &json!({"op":"ReserveEntityIds","request_id":"r1","count":7}),
        );
        assert_eq!(failed.entity_id_reservations, 0);
        assert!(failed.wal_degraded);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn persistent_gate_rejects_reserve_ids_when_wal_already_degraded() {
        let mut state = ServerState::new(30.0);
        state.wal_degraded = true;
        let mut rx = add_test_worker_with_rx(&mut state, "w1", "W");

        dispatch_test_frame(
            &mut state,
            "w1",
            &json!({"op":"ReserveEntityIds","request_id":"r1","count":7}),
        );

        assert_eq!(state.entity_id_reservations, 0);
        let rejected =
            decode_test_frame(&rx.try_recv().expect("ReserveEntityIds must fail closed"));
        assert_eq!(
            rejected.get("op").and_then(|v| v.as_str()),
            Some("UpdateRejected")
        );
        assert_eq!(
            rejected.get("request_id").and_then(|v| v.as_str()),
            Some("r1")
        );
        assert!(rejected
            .get("reason")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .contains("WAL-degraded"));
    }

    #[test]
    fn snapshot_marker_wal_fail_does_not_disable_compaction_or_emit_cut() {
        let mut state = ServerState::new(30.0);
        state.wal_fail_inject = true;
        state.wal_degraded = false;
        add_test_worker(&mut state, "snap", "OBS");
        state
            .workers
            .get_mut("snap")
            .unwrap()
            .attributes
            .insert("snapshot".to_string());

        dispatch_test_frame(
            &mut state,
            "snap",
            &json!({"op":"SnapshotMarker","request_id":"s1","snapshot_id":"cut-1"}),
        );

        assert!(!state.snapshot_seen);
        assert!(state.wal_degraded);
    }

    #[test]
    fn snapshot_marker_restore_offset_rolls_back_post_cut_entities() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "gw_snapshot_restore_cut_{}_{}",
            std::process::id(),
            unique
        ));
        let _ = std::fs::create_dir_all(&dir);
        let wal_path = dir.join("test.wal").to_string_lossy().to_string();

        let mut state = ServerState::new(30.0);
        state.wal_path = wal_path.clone();
        state.wal = Some(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&wal_path)
                .unwrap(),
        );
        state.ensure_wal_header();

        assert!(spawn_in_region(
            &mut state,
            "pre-cut",
            [-1.0, 0.0],
            [0.0, 0.0],
            Map::new(),
            Some("W"),
            SpawnAuthoritySeed::default(),
        ));

        let mut rx = add_test_worker_with_rx(&mut state, "snap", "OBS");
        state
            .workers
            .get_mut("snap")
            .unwrap()
            .attributes
            .insert("snapshot".to_string());

        dispatch_test_frame(
            &mut state,
            "snap",
            &json!({"op":"SnapshotMarker","request_id":"s1","snapshot_id":"cut-1"}),
        );

        let manifest = decode_test_frame(&rx.try_recv().expect("snapshot manifest must emit"));
        assert_eq!(manifest["op"], "SnapshotManifest");
        assert_eq!(manifest["snapshot_id"], "cut-1");
        let cut_offset = manifest["wal_offset"]
            .as_u64()
            .expect("manifest must expose a WAL cut offset");

        assert!(spawn_in_region(
            &mut state,
            "post-cut",
            [1.0, 0.0],
            [0.0, 0.0],
            Map::new(),
            Some("E"),
            SpawnAuthoritySeed::default(),
        ));
        drop(state.wal.take());

        let (cut_store, _deleted, _cfg, cut_pending, _forwarded, _id_hwm, cut_report) =
            recover_from_wal_report(&wal_path, Some(cut_offset));
        assert!(cut_report.error.is_none(), "{:?}", cut_report.error);
        assert_eq!(
            cut_report.kind_counts.get("snapshot_marker").copied(),
            Some(1)
        );
        assert!(cut_store.contains_key("pre-cut"));
        assert!(
            !cut_store.contains_key("post-cut"),
            "restore-to-cut must not leak entities created after the snapshot marker"
        );
        assert!(cut_pending.is_empty());

        let (full_store, _deleted, _cfg, _pending, _forwarded, _id_hwm, full_report) =
            recover_from_wal_report(&wal_path, None);
        assert!(full_report.error.is_none(), "{:?}", full_report.error);
        assert!(full_store.contains_key("pre-cut"));
        assert!(full_store.contains_key("post-cut"));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn snapshot_marker_flushes_pending_update_before_cut() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "gw_snapshot_flush_pending_{}_{}",
            std::process::id(),
            unique
        ));
        let _ = std::fs::create_dir_all(&dir);
        let wal_path = dir.join("test.wal").to_string_lossy().to_string();

        let mut state = ServerState::new(30.0);
        state.wal_path = wal_path.clone();
        state.wal = Some(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&wal_path)
                .unwrap(),
        );
        state.ensure_wal_header();
        add_test_worker(&mut state, "w1", "W");
        assert!(spawn_in_region(
            &mut state,
            "snapshot-ship",
            [-1.0, 0.0],
            [0.0, 0.0],
            Map::new(),
            Some("W"),
            SpawnAuthoritySeed::default(),
        ));

        assert!(apply_one_update(
            &mut state,
            "w1",
            "snapshot-ship",
            "pos",
            json!([4.0, 0.0]),
            None,
        ));
        assert_eq!(state.pending_updates.len(), 1);
        assert_eq!(state.entities["snapshot-ship"].pos, [-1.0, 0.0]);

        let mut rx = add_test_worker_with_rx(&mut state, "snap", "OBS");
        state
            .workers
            .get_mut("snap")
            .unwrap()
            .attributes
            .insert("snapshot".to_string());

        dispatch_test_frame(
            &mut state,
            "snap",
            &json!({"op":"SnapshotMarker","request_id":"s1","snapshot_id":"flush-cut"}),
        );

        let manifest = decode_test_frame(&rx.try_recv().expect("snapshot manifest must emit"));
        assert_eq!(manifest["op"], "SnapshotManifest");
        let cut_offset = manifest["wal_offset"]
            .as_u64()
            .expect("manifest must expose a WAL cut offset");
        assert!(state.pending_updates.is_empty());
        assert_eq!(state.entities["snapshot-ship"].pos, [4.0, 0.0]);
        drop(state.wal.take());

        let (cut_store, _deleted, _cfg, cut_pending, _forwarded, _id_hwm, cut_report) =
            recover_from_wal_report(&wal_path, Some(cut_offset));
        assert!(cut_report.error.is_none(), "{:?}", cut_report.error);
        let recovered = cut_store
            .get("snapshot-ship")
            .expect("snapshot cut must recover the updated entity");
        assert_eq!(
            recovered.pos,
            [4.0, 0.0],
            "restore from SnapshotManifest offset did not reproduce marker-time live state"
        );
        assert!(cut_pending.is_empty());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn snapshot_manifest_carries_spatial_schema_contract() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "gw_snapshot_spatial_contract_{}_{}",
            std::process::id(),
            unique
        ));
        let _ = std::fs::create_dir_all(&dir);
        let wal_path = dir.join("test.wal").to_string_lossy().to_string();

        let mut state = ServerState::new(30.0);
        state.grid2d = Some((3, 2, 10.0, 20.0));
        state.zone_topology_rev = 7;
        state.wal_path = wal_path.clone();
        state.wal = Some(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&wal_path)
                .unwrap(),
        );
        state.ensure_wal_header();

        let mut rx = add_test_worker_with_rx(&mut state, "snap", "OBS");
        state
            .workers
            .get_mut("snap")
            .unwrap()
            .attributes
            .insert("snapshot".to_string());

        dispatch_test_frame(
            &mut state,
            "snap",
            &json!({"op":"SnapshotMarker","request_id":"s1","snapshot_id":"cut-1"}),
        );

        let manifest = decode_test_frame(&rx.try_recv().expect("snapshot manifest must emit"));
        assert_eq!(manifest["op"], "SnapshotManifest");
        assert_eq!(manifest["snapshot_manifest_version"], 1);
        assert_eq!(manifest["snapshot_schema_version"], 1);
        assert_eq!(manifest["spatial_schema_version"], 1);
        assert_eq!(manifest["coordinate_codec_version"], 1);
        assert_eq!(
            manifest["component_registry_version"],
            STANDARD_COMPONENT_REGISTRY_VERSION
        );
        assert_eq!(manifest["partition_map_version"], 7);
        assert_eq!(manifest["spatial_schema"]["spatial_dim"], "D2");
        assert_eq!(
            manifest["spatial_schema"]["coordinate_codec"],
            "debug_f64_2"
        );
        assert_eq!(
            manifest["spatial_schema"]["partition_schema"],
            json!({"kind":"grid2d","cols":3,"rows":2})
        );
        assert_eq!(
            manifest["partition_map"],
            json!({
                "version": 7,
                "kind": "grid2d",
                "cols": 3,
                "rows": 2,
                "cell_w": 10.0,
                "cell_h": 20.0,
                "origin": [0.0, 0.0]
            })
        );

        let typed =
            godworks_protocol::json::decode_json_value(&manifest).expect("manifest must decode");
        let godworks_protocol::Op::SnapshotManifest(typed_manifest) = typed else {
            panic!("expected typed SnapshotManifest");
        };
        assert!(typed_manifest.has_current_versions());
        assert_eq!(typed_manifest.snapshot_id(), Some("cut-1"));
        assert_eq!(typed_manifest.partition_map_version(), Some(7));
        assert_eq!(
            typed_manifest.spatial_schema(),
            Some(SpatialSchema::current_2d(PartitionSchema::Grid2D {
                cols: 3,
                rows: 2
            }))
        );
        assert_eq!(
            typed_manifest.partition_map(),
            Some(VersionedPartitionMap::new(
                7,
                PartitionMapSpec::grid2d(3, 2, 10.0, 20.0, [0.0, 0.0]).unwrap()
            ))
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn snapshot_manifest_carries_strip_partition_map_contract() {
        let mut state = ServerState::new(30.0);
        state.grid2d = None;
        state.boundary = 0.0;
        state.boundaries = vec![-10.0, 0.0, 10.0];
        state.splits.insert("Z1".to_string(), vec![3.0, 6.0]);
        state.zone_topology_rev = 42;

        let mut rx = add_test_worker_with_rx(&mut state, "snap", "OBS");
        state
            .workers
            .get_mut("snap")
            .unwrap()
            .attributes
            .insert("snapshot".to_string());

        dispatch_test_frame(
            &mut state,
            "snap",
            &json!({"op":"SnapshotMarker","request_id":"s1","snapshot_id":"strip-cut"}),
        );

        let manifest = decode_test_frame(&rx.try_recv().expect("snapshot manifest must emit"));
        assert_eq!(manifest["op"], "SnapshotManifest");
        assert_eq!(manifest["partition_map_version"], 42);
        assert_eq!(
            manifest["partition_map"],
            json!({
                "version": 42,
                "kind": "strip1d",
                "boundaries": [-10.0, 0.0, 10.0],
                "splits": [
                    { "region": "Z1", "boundaries": [3.0, 6.0] }
                ]
            })
        );

        let typed =
            godworks_protocol::json::decode_json_value(&manifest).expect("manifest must decode");
        let godworks_protocol::Op::SnapshotManifest(typed_manifest) = typed else {
            panic!("expected typed SnapshotManifest");
        };
        assert_eq!(
            typed_manifest.partition_map(),
            Some(VersionedPartitionMap::new(
                42,
                PartitionMapSpec::strip1d(
                    vec![-10.0, 0.0, 10.0],
                    vec![RegionSplitSpec::new("Z1", vec![3.0, 6.0]).unwrap()]
                )
                .unwrap()
            ))
        );
    }

    #[test]
    fn snapshot_manifest_authority_hash_matches_pos_owner_epoch_cut() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "gw_snapshot_authority_hash_{}_{}",
            std::process::id(),
            unique
        ));
        let _ = std::fs::create_dir_all(&dir);
        let wal_path = dir.join("test.wal").to_string_lossy().to_string();

        let mut state = ServerState::new(30.0);
        state.wal_path = wal_path.clone();
        state.wal = Some(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&wal_path)
                .unwrap(),
        );
        state.ensure_wal_header();
        add_test_worker(&mut state, "w1", "W");
        add_test_worker(&mut state, "w2", "E");

        assert!(spawn_in_region(
            &mut state,
            "zeta",
            [-1.0, 0.0],
            [0.0, 0.0],
            Map::new(),
            Some("W"),
            SpawnAuthoritySeed::default(),
        ));
        assert!(spawn_in_region(
            &mut state,
            "alpha",
            [1.0, 0.0],
            [0.0, 0.0],
            Map::new(),
            Some("E"),
            SpawnAuthoritySeed::default(),
        ));
        set_component_authority_epoch(state.entities.get_mut("zeta").unwrap(), "pos", 11);
        set_component_authority_epoch(state.entities.get_mut("alpha").unwrap(), "pos", 4);
        grant_authority(&mut state, "w1", "zeta", "pos");
        grant_authority(&mut state, "w2", "alpha", "pos");

        let expected = expected_snapshot_authority_hash_for_test(&state.entities);
        let mut rx = add_test_worker_with_rx(&mut state, "snap", "OBS");
        state
            .workers
            .get_mut("snap")
            .unwrap()
            .attributes
            .insert("snapshot".to_string());

        dispatch_test_frame(
            &mut state,
            "snap",
            &json!({"op":"SnapshotMarker","request_id":"s1","snapshot_id":"hash-cut"}),
        );

        let manifest = decode_test_frame(&rx.try_recv().expect("snapshot manifest must emit"));
        assert_eq!(manifest["op"], "SnapshotManifest");
        assert_eq!(manifest["snapshot_id"], "hash-cut");
        let expected = expected.to_string();
        assert_eq!(
            manifest["authority_hash"].as_str(),
            Some(expected.as_str()),
            "SnapshotManifest authority_hash must be an exact, stable hash over sorted entity id plus pos owner/epoch"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn snapshot_vector_restores_in_flight_mesh_handoff_exactly_once() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "gw_snapshot_mesh_pending_{}_{}",
            std::process::id(),
            unique
        ));
        let _ = std::fs::create_dir_all(&dir);
        let wal_path = dir.join("test.wal").to_string_lossy().to_string();

        let mut state = ServerState::new(30.0);
        state.my_region = "W".to_string();
        state.region_lease_epoch.insert("W".to_string(), 7);
        state.wal_path = wal_path.clone();
        state.wal = Some(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&wal_path)
                .unwrap(),
        );
        state.ensure_wal_header();
        add_test_worker(&mut state, "w1", "W");

        let mut ship = test_physics_island_entity([-1.0, 2.0], "W");
        stamp_expanded_physics_island_epochs(&mut ship);
        state.entities.insert("ship".to_string(), ship);
        grant_region_physics_island_authority(&mut state, "w1", "ship");

        let (tx, mut mesh_rx) = mpsc::unbounded_channel();
        state.mesh.insert("E".to_string(), tx);
        mesh_forward(&mut state, "ship", "E");
        let handoff = decode_test_frame(
            &mesh_rx
                .try_recv()
                .expect("mesh handoff must be emitted after durable mesh_out"),
        );
        assert_eq!(handoff["op"], "MeshHandoff");
        assert!(!state.entities.contains_key("ship"));
        assert!(state.pending_mesh.contains_key("ship"));

        let mut rx = add_test_worker_with_rx(&mut state, "snap", "OBS");
        state
            .workers
            .get_mut("snap")
            .unwrap()
            .attributes
            .insert("snapshot".to_string());
        dispatch_test_frame(
            &mut state,
            "snap",
            &json!({"op":"SnapshotMarker","request_id":"s1","snapshot_id":"mesh-cut"}),
        );
        let manifest = decode_test_frame(&rx.try_recv().expect("snapshot manifest must emit"));
        assert_eq!(manifest["op"], "SnapshotManifest");
        assert_eq!(manifest["pending_mesh"], 1);
        assert_eq!(
            manifest["in_flight"]
                .as_array()
                .expect("manifest must list in-flight mesh handoffs")
                .len(),
            1
        );
        let cut_offset = manifest["wal_offset"]
            .as_u64()
            .expect("manifest must expose a WAL cut offset");

        let (cut_store, _deleted, _cfg, cut_pending, _forwarded, _id_hwm, cut_report) =
            recover_from_wal_report(&wal_path, Some(cut_offset));
        assert!(cut_report.error.is_none(), "{:?}", cut_report.error);
        assert!(
            !cut_store.contains_key("ship"),
            "source recovery at the cut must not resurrect a departed mesh entity"
        );
        let recovered_handoff = cut_pending
            .get("ship")
            .expect("mesh_out before the cut must recover as an in-flight handoff");
        assert_eq!(recovered_handoff["kind"], "mesh_out");
        assert_eq!(recovered_handoff["target"], "E");
        assert_eq!(recovered_handoff["pos"], json!([-1.0, 2.0]));
        let recovered_components = recovered_handoff["components"]
            .as_object()
            .expect("mesh_out must retain component payload");
        for comp in ["rot", "lin", "ang", "at_rest"] {
            assert!(
                recovered_components.contains_key(comp),
                "mesh_out must retain physics payload component {comp}"
            );
        }

        assert!(record_mesh_ack(&mut state, "ship"));
        drop(state.wal.take());

        let (full_store, _deleted, _cfg, full_pending, _forwarded, _id_hwm, full_report) =
            recover_from_wal_report(&wal_path, None);
        assert!(full_report.error.is_none(), "{:?}", full_report.error);
        assert!(
            !full_store.contains_key("ship"),
            "source full recovery must not regain an acked mesh entity"
        );
        assert!(
            !full_pending.contains_key("ship"),
            "mesh_acked after the cut must clear the recovered pending handoff"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn component_add_remove_wal_fail_does_not_mutate_ram() {
        let mut add_state = ServerState::new(30.0);
        add_state.wal_fail_inject = true;
        add_state.wal_degraded = false;
        add_test_worker(&mut add_state, "w1", "W");
        add_state
            .entities
            .insert("crate".to_string(), test_entity([-1.0, 0.0], "W"));
        grant_region_physics_island_authority(&mut add_state, "w1", "crate");

        dispatch_test_frame(
            &mut add_state,
            "w1",
            &json!({"op":"AddComponent","entity":"crate","comp":"loot","value":3}),
        );

        assert!(!add_state.entities["crate"].components.contains_key("loot"));
        assert_eq!(add_state.entities["crate"].version, 1);
        assert!(add_state.wal_degraded);

        let mut remove_state = ServerState::new(30.0);
        remove_state.wal_fail_inject = true;
        remove_state.wal_degraded = false;
        add_test_worker(&mut remove_state, "w1", "W");
        remove_state
            .entities
            .insert("crate".to_string(), test_entity([-1.0, 0.0], "W"));
        remove_state
            .entities
            .get_mut("crate")
            .unwrap()
            .components
            .insert("loot".to_string(), json!(3));
        ensure_component_authority(remove_state.entities.get_mut("crate").unwrap(), "loot");

        dispatch_test_frame(
            &mut remove_state,
            "w1",
            &json!({"op":"RemoveComponent","entity":"crate","comp":"loot"}),
        );

        assert!(remove_state.entities["crate"]
            .components
            .contains_key("loot"));
        assert_eq!(remove_state.entities["crate"].version, 1);
        assert!(remove_state.wal_degraded);
    }

    #[test]
    fn set_component_authority_rejects_authority_epoch_rewind() {
        let mut state = ServerState::new(30.0);
        add_test_worker(&mut state, "w1", "W");
        add_test_worker(&mut state, "w2", "E");
        let mut admin_rx = add_test_worker_with_rx(&mut state, "admin", "OBS");
        state
            .workers
            .get_mut("admin")
            .unwrap()
            .attributes
            .insert("kernel_admin".to_string());
        state
            .entities
            .insert("ship".to_string(), test_entity([-1.0, 0.0], "W"));
        set_component_authority_epoch(state.entities.get_mut("ship").unwrap(), "pos", 9);
        grant_authority(&mut state, "w1", "ship", "pos");

        dispatch_test_frame(
            &mut state,
            "admin",
            &json!({"op":"SetComponentAuthority","request_id":"rewind",
                "entity":"ship","comp":"pos","owner":"w2","authority_epoch":3}),
        );

        let response = decode_test_frame(&admin_rx.try_recv().expect("admin response"));
        assert_eq!(response["op"], "SetComponentAuthorityResponse");
        assert_eq!(response["success"], false);
        assert_eq!(response["current_authority_epoch"], 9);
        assert_eq!(
            component_authority_owner(&state.entities["ship"], "pos"),
            Some("w1".to_string())
        );
        assert_eq!(component_authority_epoch(&state.entities["ship"], "pos"), 9);
        assert!(state.workers["w1"]
            .authority_epochs
            .contains_key(&authority_key("ship", "pos")));
        assert!(!state.workers["w2"]
            .authority_epochs
            .contains_key(&authority_key("ship", "pos")));
    }

    #[test]
    fn set_component_authority_current_epoch_bumps_epoch() {
        let mut state = ServerState::new(30.0);
        add_test_worker(&mut state, "w1", "W");
        add_test_worker(&mut state, "w2", "E");
        let mut admin_rx = add_test_worker_with_rx(&mut state, "admin", "OBS");
        state
            .workers
            .get_mut("admin")
            .unwrap()
            .attributes
            .insert("kernel_admin".to_string());
        state
            .entities
            .insert("ship".to_string(), test_entity([-1.0, 0.0], "W"));
        set_component_authority_epoch(state.entities.get_mut("ship").unwrap(), "pos", 9);
        grant_authority(&mut state, "w1", "ship", "pos");

        dispatch_test_frame(
            &mut state,
            "admin",
            &json!({"op":"SetComponentAuthority","request_id":"cas",
                "entity":"ship","comp":"pos","owner":"w2","authority_epoch":9}),
        );

        let response = decode_test_frame(&admin_rx.try_recv().expect("admin response"));
        assert_eq!(response["op"], "SetComponentAuthorityResponse");
        assert_eq!(response["success"], true);
        assert_eq!(response["authority_epoch"], 10);
        assert_eq!(
            component_authority_owner(&state.entities["ship"], "pos"),
            Some("w2".to_string())
        );
        assert_eq!(
            component_authority_epoch(&state.entities["ship"], "pos"),
            10
        );
        assert!(!state.workers["w1"]
            .authority_epochs
            .contains_key(&authority_key("ship", "pos")));
        assert_eq!(
            state.workers["w2"]
                .authority_epochs
                .get(&authority_key("ship", "pos")),
            Some(&10)
        );
    }

    #[test]
    fn set_component_authority_omitted_epoch_bumps_from_current_epoch() {
        let mut state = ServerState::new(30.0);
        add_test_worker(&mut state, "w1", "W");
        add_test_worker(&mut state, "w2", "E");
        let mut admin_rx = add_test_worker_with_rx(&mut state, "admin", "OBS");
        state
            .workers
            .get_mut("admin")
            .unwrap()
            .attributes
            .insert("kernel_admin".to_string());
        state
            .entities
            .insert("ship".to_string(), test_entity([-1.0, 0.0], "W"));
        set_component_authority_epoch(state.entities.get_mut("ship").unwrap(), "pos", 12);
        grant_authority(&mut state, "w1", "ship", "pos");

        dispatch_test_frame(
            &mut state,
            "admin",
            &json!({"op":"SetComponentAuthority","request_id":"omitted",
                "entity":"ship","comp":"pos","owner":"w2"}),
        );

        let response = decode_test_frame(&admin_rx.try_recv().expect("admin response"));
        assert_eq!(response["op"], "SetComponentAuthorityResponse");
        assert_eq!(response["success"], true);
        assert_eq!(response["authority_epoch"], 13);
        assert_eq!(
            component_authority_owner(&state.entities["ship"], "pos"),
            Some("w2".to_string())
        );
        assert_eq!(
            component_authority_epoch(&state.entities["ship"], "pos"),
            13
        );
        assert_eq!(
            state.workers["w2"]
                .authority_epochs
                .get(&authority_key("ship", "pos")),
            Some(&13)
        );
    }

    #[test]
    fn set_component_authority_is_durable_and_revokes_old_cache() {
        let mut failed = ServerState::new(30.0);
        failed.wal_fail_inject = true;
        failed.wal_degraded = false;
        add_test_worker(&mut failed, "w1", "W");
        add_test_worker(&mut failed, "w2", "E");
        add_test_worker(&mut failed, "admin", "OBS");
        failed
            .workers
            .get_mut("admin")
            .unwrap()
            .attributes
            .insert("kernel_admin".to_string());
        failed
            .entities
            .insert("ship".to_string(), test_entity([-1.0, 0.0], "W"));
        grant_region_physics_island_authority(&mut failed, "w1", "ship");

        dispatch_test_frame(
            &mut failed,
            "admin",
            &json!({"op":"SetComponentAuthority","entity":"ship","comp":"pos","owner":"w2"}),
        );

        assert_eq!(
            component_authority_owner(&failed.entities["ship"], "pos"),
            Some("w1".to_string())
        );
        assert!(failed.workers["w1"]
            .authority_epochs
            .contains_key(&authority_key("ship", "pos")));
        assert!(!failed.workers["w2"]
            .authority_epochs
            .contains_key(&authority_key("ship", "pos")));

        let mut state = ServerState::new(30.0);
        add_test_worker(&mut state, "w1", "W");
        add_test_worker(&mut state, "w2", "E");
        add_test_worker(&mut state, "admin", "OBS");
        state
            .workers
            .get_mut("admin")
            .unwrap()
            .attributes
            .insert("kernel_admin".to_string());
        state
            .entities
            .insert("ship".to_string(), test_entity([-1.0, 0.0], "W"));
        grant_region_physics_island_authority(&mut state, "w1", "ship");

        dispatch_test_frame(
            &mut state,
            "admin",
            &json!({"op":"SetComponentAuthority","entity":"ship","comp":"pos","owner":"w2"}),
        );

        assert_eq!(
            component_authority_owner(&state.entities["ship"], "pos"),
            Some("w2".to_string())
        );
        assert!(!state.workers["w1"]
            .authority_epochs
            .contains_key(&authority_key("ship", "pos")));
        assert!(state.workers["w2"]
            .authority_epochs
            .contains_key(&authority_key("ship", "pos")));
    }

    #[test]
    fn threshold_commit_wal_fail_does_not_move_or_fence() {
        let mut state = ServerState::new(30.0);
        state.wal_fail_inject = true;
        state.wal_degraded = false;
        add_test_worker(&mut state, "w1", "W");
        add_test_worker(&mut state, "w2", "E");
        state
            .entities
            .insert("ship".to_string(), test_entity([-1.0, 0.0], "W"));
        grant_region_physics_island_authority(&mut state, "w1", "ship");
        let old_epoch = component_authority_epoch(&state.entities["ship"], "pos");

        dispatch_test_frame(
            &mut state,
            "w1",
            &json!({"op":"ThresholdTx","entity":"ship","tx_id":"tx1","phase":"commit","from":"W","to":"E"}),
        );

        assert_eq!(state.entities["ship"].region, "W");
        assert!(!state.entities["ship"]
            .components
            .contains_key("threshold.tx"));
        assert_eq!(
            component_authority_owner(&state.entities["ship"], "pos"),
            Some("w1".to_string())
        );
        assert_eq!(
            component_authority_epoch(&state.entities["ship"], "pos"),
            old_epoch
        );
        assert!(state.wal_degraded);
    }

    #[test]
    fn mesh_ack_wal_fail_keeps_pending_handoff() {
        let mut failed = ServerState::new(30.0);
        failed.wal_fail_inject = true;
        failed.wal_degraded = false;
        failed.pending_mesh.insert(
            "ship".to_string(),
            (
                json!({"op":"MeshHandoff","entity":"ship"}),
                Instant::now(),
                "E".to_string(),
            ),
        );

        assert!(!record_mesh_ack(&mut failed, "ship"));
        assert!(failed.pending_mesh.contains_key("ship"));
        assert!(failed.wal_degraded);

        let mut state = ServerState::new(30.0);
        state.pending_mesh.insert(
            "ship".to_string(),
            (
                json!({"op":"MeshHandoff","entity":"ship"}),
                Instant::now(),
                "E".to_string(),
            ),
        );

        assert!(record_mesh_ack(&mut state, "ship"));
        assert!(!state.pending_mesh.contains_key("ship"));
    }

    #[test]
    fn mesh_ack_is_classified_persistent_for_fail_closed_contract() {
        assert!(is_persistent_op("MeshAck"));
        assert!(is_persistent_op("ReserveEntityIds"));
    }

    #[test]
    fn fold_transfer_recovery_preserves_position() {
        let dir = std::env::temp_dir().join(format!("gw_fold_recover_{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let wal_path = dir.join("test.wal").to_string_lossy().to_string();

        let mut state = ServerState::new(30.0);
        state.wal_path = wal_path.clone();
        state.wal = Some(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&wal_path)
                .unwrap(),
        );
        add_test_worker(&mut state, "w1", "W");
        add_test_worker(&mut state, "m1", "MARS");
        assert!(spawn_in_region(
            &mut state,
            "foldy",
            [-1.0, 2.0],
            [0.5, 0.0],
            Map::new(),
            Some("W"),
            SpawnAuthoritySeed::default(),
        ));

        assert!(handoff_with_position(
            &mut state,
            "foldy",
            "W",
            "MARS",
            Some([100.0, 200.0])
        ));
        assert_eq!(state.entities["foldy"].region, "W");
        assert_eq!(state.pending_handoffs.len(), 1);
        flush_pending_handoffs(&mut state);
        assert_eq!(state.entities["foldy"].region, "MARS");
        assert_eq!(state.entities["foldy"].pos, [100.0, 200.0]);
        drop(state.wal.take());

        let (entities, _deleted, _cfg, _pending, _forwarded, _id_hwm, report) =
            recover_from_wal_report(&wal_path, None);
        assert!(
            report.error.is_none(),
            "recovery must not error: {:?}",
            report.error
        );
        let recovered = entities.get("foldy").unwrap();
        assert_eq!(recovered.region, "MARS");
        assert_eq!(recovered.pos, [100.0, 200.0]);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn recovery_physically_truncates_corrupt_tail_before_append() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "gw_wal_tail_truncate_{}_{}",
            std::process::id(),
            nonce
        ));
        let _ = std::fs::create_dir_all(&dir);
        let wal_path = dir.join("test.wal").to_string_lossy().to_string();
        let header = wal_v1_header_line();
        let register = wal_v1_envelope_line(&json!({
            "kind":"register",
            "entity":"ship",
            "region":"W",
            "pos":[1.0,0.0],
            "vel":[0.0,0.0],
            "components":{}
        }));
        let corrupt_tail = "{\"_c\":0,\"_d\":\"{\\\"kind\\\":\\\"write\\\"}\"}";
        std::fs::write(
            &wal_path,
            format!("{header}\n{register}\n{corrupt_tail}").as_bytes(),
        )
        .unwrap();
        let original_len = std::fs::metadata(&wal_path).unwrap().len();

        let (store, _, _, _, _, _, report) = recover_from_wal_report(&wal_path, None);
        assert!(report.error.is_none(), "{:?}", report.error);
        assert_eq!(store["ship"].pos, [1.0, 0.0]);
        assert!(report.truncated_tail_bytes > 0);
        assert!(report.recoverable_prefix_bytes < original_len);

        truncate_wal_tail_to_recoverable_prefix(&wal_path, &report).unwrap();
        assert_eq!(
            std::fs::metadata(&wal_path).unwrap().len(),
            report.recoverable_prefix_bytes,
            "recovery must make the replay prefix physical before appending"
        );

        {
            let mut f = OpenOptions::new().append(true).open(&wal_path).unwrap();
            writeln!(
                f,
                "{}",
                wal_v1_envelope_line(&json!({
                    "kind":"write",
                    "entity":"ship",
                    "comp":"pos",
                    "value":[2.0,0.0],
                    "version":2
                }))
            )
            .unwrap();
            f.sync_data().unwrap();
        }

        let (store2, _, _, _, _, _, report2) = recover_from_wal_report(&wal_path, None);
        assert!(
            report2.error.is_none(),
            "a second restart after append must not reinterpret the old tail as mid-corruption: {:?}",
            report2.error
        );
        assert_eq!(store2["ship"].pos, [2.0, 0.0]);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn compaction_preserves_delete_tombstones() {
        // Regression for the P1: WAL compaction dropped delete tombstones, so compact+restart could
        // recreate a deleted id. Round-trip through the REAL recovery path: the tombstone must survive.
        let dir = std::env::temp_dir().join(format!("gw_compact_tomb_{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let wal_path = dir.join("test.wal").to_string_lossy().to_string();

        let mut state = ServerState::new(30.0);
        state.wal_path = wal_path.clone();
        state.wal = Some(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&wal_path)
                .unwrap(),
        );
        state.wal_degraded = false;
        state.snapshot_seen = false; // compaction stays off under coordinated snapshots; this is the disk-fill case
        state.wal_compact_bytes = 1; // tiny threshold so the compaction gate fires
        state.wal_bytes = 4096; // > threshold
        state
            .entities
            .insert("alive-1".to_string(), test_entity([1.0, 0.0], "W"));
        state.deleted_entities.insert("ghost-1".to_string());

        state.maybe_compact_wal();
        assert!(!state.wal_degraded, "compaction must succeed");

        // Replay the compacted WAL exactly as a restart would.
        let (entities, deleted, _cfg, _comp, _forwarded, _id_hwm, report) =
            recover_from_wal_report(&wal_path, None);
        assert!(
            report.error.is_none(),
            "recovery must not error: {:?}",
            report.error
        );
        assert!(
            deleted.contains("ghost-1"),
            "delete tombstone must survive WAL compaction (else a restart recreates the deleted id)"
        );
        assert!(
            entities.contains_key("alive-1"),
            "live entity must survive compaction"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn compaction_waits_for_pending_durable_transition_cut() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "gw_compact_pending_{}_{}",
            std::process::id(),
            nonce
        ));
        let _ = std::fs::create_dir_all(&dir);
        let wal_path = dir.join("test.wal").to_string_lossy().to_string();
        let original = "pending-update-still-in-current-wal\n";
        std::fs::write(&wal_path, original).unwrap();

        let mut state = ServerState::new(30.0);
        state.wal_path = wal_path.clone();
        state.wal = Some(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&wal_path)
                .unwrap(),
        );
        state.wal_degraded = false;
        state.snapshot_seen = false;
        state.wal_compact_bytes = 1;
        state.wal_bytes = 4096;
        state
            .entities
            .insert("ship".to_string(), test_entity([1.0, 0.0], "W"));
        state.pending_updates.push(PreparedUpdate {
            gen: 2,
            eid: "ship".to_string(),
            comp: "pos".to_string(),
            value: json!([2.0, 0.0]),
            version: 2,
            writer: "w1".to_string(),
        });

        state.maybe_compact_wal();

        assert!(
            !state.wal_degraded,
            "a pending transition should postpone compaction, not degrade WAL"
        );
        assert_eq!(
            std::fs::read_to_string(&wal_path).unwrap(),
            original,
            "compaction must not rewrite the current WAL while staged transitions still need it"
        );
        assert_eq!(
            state.metrics.wal_compactions, 0,
            "pending transitions mean there is not yet a compactable durable cut"
        );
        assert_eq!(state.pending_updates.len(), 1);
        assert!(
            !std::path::Path::new(&format!("{wal_path}.tmp")).exists(),
            "postponed compaction should not leave a tmp snapshot"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }
}
