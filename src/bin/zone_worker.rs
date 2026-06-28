//! godworks zone_worker — a rapier2d PHYSICS zone-worker that speaks the Godworks op-wire.
//!
//! This is the genuine-new piece for the distributed-authority PHYSICS demo: every existing worker
//! (`the reference worker`, `the reference runner`, the Godot-headless one) does NAIVE velocity integration — no
//! collisions. This one holds ONE `rapier2d` world per process with REAL collision/contact solving, and
//! `loadgen.rs`'s identical wire-framing (4-byte BE length-prefix + JSON) so the broker is untouched.
//!
//! Authority drives the body lifecycle (the key nuance): a rapier rigid body is CREATED on
//! `AuthorityChange{authoritative=true}` from the entity's carried pos+vel, and DESTROYED on
//! `{false}` / `RemoveEntity`. Each tick we step rapier and write each OWNED body's `pos` (and `vel`)
//! back to the broker — which, in AUTO mode, triggers the broker's server-side position-driven
//! auto-handoff at a strip boundary (the worker is BLIND to the border). In FOLD mode (for a 2-D
//! square grid, which the broker's single-axis auto-handoff can't express) the worker computes the
//! 4-neighbour crossing and sends `Fold(entity, region, pos)`; the broker still does 100% of the
//! authority transfer (revoke/grant/epoch-bump/stale-fence/mesh-forward) server-side.
//!
//! Every UpdateComponent carries the worker's cached `authority_epoch` — the correct epoch-fence
//! design: after a handoff bumps the epoch, an in-flight write tagged with the old epoch is rejected.
//!
//! Structured stderr log lines (the demo's proof signals; parsed by gw_physics_demo.py):
//!   [zw R] connected id=.. port=.. hz=.. mode=auto|fold
//!   [zw R] spawn e=.. pos=[..] vel=[..]
//!   [zw R] AUTH-GAIN e=.. epoch=E adopt pos=[..] vel=[..]     (the new zone ADOPTS + simulates)
//!   [zw R] AUTH-LOSS e=.. epoch=E destroy
//!   [zw R] LOSS-IMMINENT e=.. target=..                       (broker C2 pre-handoff intent)
//!   [zw R] REJECTED e=.. comp=.. reason='..'                  (the stale/old-owner write fence)
//!   [zw R] fold e=.. -> region (exit ..)
//!   [zw R] tick=N owned=K view=V rejects=J hz=..
//!
//! Config (env):
//!   GW_ZW_PORT(7777) GW_ZW_HOST(127.0.0.1) GW_ZW_REGION(W) GW_ZW_ID(zw-<region>) GW_ZW_HZ(30)
//!   GW_ZW_SPAWN(0) GW_ZW_SPAWN_BOX("x0,x1,y0,y1") GW_ZW_SPAWN_SPEED(3) GW_ZW_RADIUS(0.5)
//!   GW_ZW_REST(0.9) GW_ZW_INTEREST(1e6) GW_ZW_DURATION(none=run until killed) GW_ZW_SEED(0)
//!   GW_ZW_WORLD("x0,x1,y0,y1" outer bounce-walls; empty=no walls)
//!   GW_ZW_CELL("x0,x1,y0,y1" this worker's authoritative cell -> presence selects FOLD mode)
//!   GW_ZW_NEIGHBORS("xlo:R,xhi:R,ylo:R,yhi:R" neighbour region by exit edge; for FOLD mode)

use std::collections::HashMap;
use std::env;
use std::time::{Duration, Instant};

use rapier2d::prelude::*;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;

// ── wire framing: identical to loadgen.rs (4-byte big-endian length prefix + JSON body) ──
fn frame(v: &Value) -> Vec<u8> {
    let b = serde_json::to_vec(v).unwrap();
    let n = b.len() as u32;
    let mut o = Vec::with_capacity(4 + b.len());
    o.extend_from_slice(&n.to_be_bytes());
    o.extend_from_slice(&b);
    o
}

async fn read_frame<R: AsyncReadExt + Unpin>(rd: &mut R) -> Option<Value> {
    let mut h = [0u8; 4];
    rd.read_exact(&mut h).await.ok()?;
    let n = u32::from_be_bytes(h) as usize;
    let mut b = vec![0u8; n];
    rd.read_exact(&mut b).await.ok()?;
    serde_json::from_slice(&b).ok()
}

// ── tiny xorshift RNG (avoid pulling in the `rand` crate; keep deps == broker's tokio+serde+rapier) ──
struct Rng(u64);
impl Rng {
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn unit(&mut self) -> f32 {
        (self.next_u64() >> 11) as f32 / (1u64 << 53) as f32
    }
    fn range(&mut self, lo: f32, hi: f32) -> f32 {
        lo + self.unit() * (hi - lo)
    }
}

fn env_f32(k: &str, d: f32) -> f32 {
    env::var(k).ok().and_then(|s| s.parse().ok()).unwrap_or(d)
}
fn env_u64(k: &str, d: u64) -> u64 {
    env::var(k).ok().and_then(|s| s.parse().ok()).unwrap_or(d)
}
fn env_str(k: &str, d: &str) -> String {
    env::var(k).ok().filter(|s| !s.is_empty()).unwrap_or_else(|| d.to_string())
}
// parse "x0,x1,y0,y1" -> [f32;4]
fn parse_box(s: &str) -> Option<[f32; 4]> {
    let v: Vec<f32> = s.split(',').filter_map(|t| t.trim().parse().ok()).collect();
    if v.len() == 4 {
        Some([v[0], v[1], v[2], v[3]])
    } else {
        None
    }
}
// parse "xlo:R,xhi:R,ylo:R,yhi:R" -> map edge->region
fn parse_neighbors(s: &str) -> HashMap<String, String> {
    let mut m = HashMap::new();
    for part in s.split(',') {
        let part = part.trim();
        if let Some((edge, region)) = part.split_once(':') {
            if !edge.is_empty() && !region.is_empty() {
                m.insert(edge.trim().to_string(), region.trim().to_string());
            }
        }
    }
    m
}

struct Bot {
    handle: RigidBodyHandle,
    epoch: u64,
}

// agar.io cell sizing: a cell's AREA scales with mass, so radius = base * sqrt(mass). Used by the body
// collider on adopt and by the eat-check. Bigger cells also move slower (applied in the input handler).
fn radius_for(mass: f32, base: f32) -> f32 {
    base * mass.max(1.0).sqrt()
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let host = env_str("GW_ZW_HOST", "127.0.0.1");
    let port: u16 = env_u64("GW_ZW_PORT", 7777) as u16;
    let region = env_str("GW_ZW_REGION", "W");
    let wid = env_str("GW_ZW_ID", &format!("zw-{region}"));
    let hz = env_f32("GW_ZW_HZ", 30.0).max(1.0);
    let dt = 1.0 / hz;
    let spawn_n = env_u64("GW_ZW_SPAWN", 0);
    let spawn_box = env::var("GW_ZW_SPAWN_BOX").ok().and_then(|s| parse_box(&s));
    let spawn_speed = env_f32("GW_ZW_SPAWN_SPEED", 3.0);
    // optional FIXED initial velocity "vx,vy" — if set, every spawned bot gets exactly this (used by the
    // FLOOR for a deterministic +x crossing). Unset => random chaotic direction at spawn_speed (the grid).
    let spawn_vel = env::var("GW_ZW_SPAWN_VEL").ok().and_then(|s| {
        let v: Vec<f32> = s.split(',').filter_map(|t| t.trim().parse().ok()).collect();
        if v.len() == 2 {
            Some([v[0], v[1]])
        } else {
            None
        }
    });
    let radius = env_f32("GW_ZW_RADIUS", 0.5);
    let rest = env_f32("GW_ZW_REST", 0.9);
    let interest = env_f32("GW_ZW_INTEREST", 1.0e6);
    let duration = env::var("GW_ZW_DURATION").ok().and_then(|s| s.parse::<f64>().ok());
    let seed = env_u64("GW_ZW_SEED", 0);
    let world = env::var("GW_ZW_WORLD").ok().and_then(|s| parse_box(&s));
    let cell = env::var("GW_ZW_CELL").ok().and_then(|s| parse_box(&s));
    let neighbors = env::var("GW_ZW_NEIGHBORS").ok().map(|s| parse_neighbors(&s)).unwrap_or_default();
    // FOLD mode is selected by the PRESENCE of a cell + neighbour geometry (a topology config, like the
    // broker's GW_BOUNDARIES selecting strip mode) — not a behaviour flag. No cell => pure AUTO (blind).
    let fold_mode = cell.is_some() && !neighbors.is_empty();

    let mut rng = Rng(if seed == 0 {
        // distinct per region+pid so independent workers don't spawn identically
        0x9E3779B97F4A7C15u64
            ^ (std::process::id() as u64).wrapping_mul(0x100000001B3)
            ^ region.bytes().fold(1469598103934665603u64, |h, b| (h ^ b as u64).wrapping_mul(0x100000001B3))
    } else {
        seed
    });

    // ── connect + handshake ──
    let stream = match TcpStream::connect((host.as_str(), port)).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("[zw {region}] FATAL connect {host}:{port}: {e}");
            std::process::exit(2);
        }
    };
    stream.set_nodelay(true).ok();
    let (mut rd, mut wr) = stream.into_split();
    let (tx, mut rx) = mpsc::unbounded_channel::<Value>();
    tokio::spawn(async move {
        while let Some(f) = read_frame(&mut rd).await {
            if tx.send(f).is_err() {
                break;
            }
        }
    });

    wr.write_all(&frame(&json!({"op":"WorkerConnect","worker_id":wid,"region":region})))
        .await
        .ok();
    // a WIDE interest so we (a) are granted authority over entities we create in our region, and
    // (b) see neighbours' entities approaching the seam -> can adopt them on the authority grant.
    wr.write_all(&frame(&json!({"op":"Interest","center":[0.0,0.0],"radius":interest})))
        .await
        .ok();
    eprintln!(
        "[zw {region}] connected id={wid} port={port} hz={hz} mode={} cell={:?} neighbors={:?}",
        if fold_mode { "fold" } else { "auto" },
        cell,
        neighbors
    );

    // ── rapier world (one per process) ──
    let gravity = vector![0.0, 0.0];
    let mut ip = IntegrationParameters::default();
    ip.dt = dt;
    let mut pipeline = PhysicsPipeline::new();
    let mut islands = IslandManager::new();
    let mut bphase = DefaultBroadPhase::new();
    let mut nphase = NarrowPhase::new();
    let mut bodies = RigidBodySet::new();
    let mut colliders = ColliderSet::new();
    let mut ijoints = ImpulseJointSet::new();
    let mut mjoints = MultibodyJointSet::new();
    let mut ccd = CCDSolver::new();
    let mut query = QueryPipeline::new();
    let hooks = ();
    let events = ();

    // outer world bounce-walls (4 fixed thin cuboids); a bot bounces here, Folds at internal seams first.
    if let Some([x0, x1, y0, y1]) = world {
        let cx = 0.5 * (x0 + x1);
        let cy = 0.5 * (y0 + y1);
        let hx = 0.5 * (x1 - x0);
        let hy = 0.5 * (y1 - y0);
        let t = 1.0f32; // wall thickness
        let walls = [
            (cx, y0 - t, hx + t, t), // bottom
            (cx, y1 + t, hx + t, t), // top
            (x0 - t, cy, t, hy + t), // left
            (x1 + t, cy, t, hy + t), // right
        ];
        for (wx, wy, whx, why) in walls {
            colliders.insert(
                ColliderBuilder::cuboid(whx, why)
                    .translation(vector![wx, wy])
                    .restitution(1.0)
                    .build(),
            );
        }
    }

    // local mirrors built from the op-stream (for adoption: carried pos/vel)
    let mut view_pos: HashMap<String, [f32; 2]> = HashMap::new();
    let mut view_vel: HashMap<String, [f32; 2]> = HashMap::new();
    let mut view_mass: HashMap<String, f32> = HashMap::new();
    let mut bots: HashMap<String, Bot> = HashMap::new();
    let mut rejects: u64 = 0;

    // ── spawn our own bots (we declared Interest -> the broker grants us authority on create; the body
    //    is created when the AuthorityChange{true} echo arrives, from the pos/vel we stash here) ──
    for i in 0..spawn_n {
        let eid = format!("{region}-b{i}");
        let (x, y) = if let Some([x0, x1, y0, y1]) = spawn_box {
            (rng.range(x0, x1), rng.range(y0, y1))
        } else {
            (rng.range(-1.0, 1.0), rng.range(-1.0, 1.0))
        };
        let (vx, vy) = if let Some([fx, fy]) = spawn_vel {
            (fx, fy)
        } else {
            let ang = rng.range(0.0, std::f32::consts::TAU);
            (ang.cos() * spawn_speed, ang.sin() * spawn_speed)
        };
        let m0 = rng.range(1.0, 4.0);  // agar.io: spawn cells with small varied mass (size variety -> eat dynamics)
        view_pos.insert(eid.clone(), [x, y]);
        view_vel.insert(eid.clone(), [vx, vy]);
        view_mass.insert(eid.clone(), m0);
        wr.write_all(&frame(&json!({
            "op":"CreateEntity","entity":eid,"region":region,
            "components":{"pos":[x,y],"vel":[vx,vy],"mass":m0}
        })))
        .await
        .ok();
        eprintln!("[zw {region}] spawn e={eid} pos=[{x:.3},{y:.3}] vel=[{vx:.3},{vy:.3}]");
    }

    let mut ticker = tokio::time::interval(Duration::from_secs_f64(dt as f64));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let t_start = Instant::now();
    let mut tick: u64 = 0;
    let mut last_report = Instant::now();
    let mut acc_ticks: u64 = 0;

    loop {
        ticker.tick().await;

        // (1) drain + apply all pending ops at the tick boundary (game-loop discipline)
        while let Ok(f) = rx.try_recv() {
            apply_op(
                &f,
                &region,
                &mut view_pos,
                &mut view_vel,
                &mut bots,
                &mut bodies,
                &mut islands,
                &mut colliders,
                &mut ijoints,
                &mut mjoints,
                radius,
                rest,
                &mut rejects,
            );
        }

        // (2) step rapier (real collision/contact solve over the bodies we own)
        pipeline.step(
            &gravity,
            &ip,
            &mut islands,
            &mut bphase,
            &mut nphase,
            &mut bodies,
            &mut colliders,
            &mut ijoints,
            &mut mjoints,
            &mut ccd,
            Some(&mut query),
            &hooks,
            &events,
        );

        // (3a) AUTO mode: write every owned body's pos FIRST (this triggers the broker's server-side
        //      auto-handoff at the strip boundary), THEN vel. Writing pos-before-vel means that on the
        //      crossing tick the handoff fires on the pos write and the same-tick vel write arrives
        //      AFTER authority moved -> the broker fences it (the organic old-owner stale-write reject).
        // (3b) FOLD mode: a body that left this worker's cell is Fold()'d to the 4-neighbour region; the
        //      broker performs the authority transfer (mesh_forward -> adopt) server-side.
        let mut folded: Vec<String> = Vec::new();
        // pass A: positions / folds
        for (eid, bot) in bots.iter() {
            let (px, py, _vx, _vy) = body_state(&bodies, bot.handle);
            if fold_mode {
                if let Some(target) = fold_target(px, py, cell.as_ref().unwrap(), &neighbors) {
                    wr.write_all(&frame(&json!({
                        "op":"Fold","entity":eid,"region":target,"pos":[px,py]
                    })))
                    .await
                    .ok();
                    eprintln!("[zw {region}] fold e={eid} -> {target} (exit pos=[{px:.2},{py:.2}])");
                    folded.push(eid.clone());
                    continue;
                }
            }
            wr.write_all(&frame(&json!({
                "op":"UpdateComponent","entity":eid,"comp":"pos","value":[px,py],
                "authority_epoch":bot.epoch
            })))
            .await
            .ok();
        }
        // a folded body is now the neighbour's; drop it locally (the broker revokes our authority too)
        for eid in &folded {
            if let Some(bot) = bots.remove(eid) {
                destroy_body(bot.handle, &mut bodies, &mut islands, &mut colliders, &mut ijoints, &mut mjoints);
            }
        }
        // pass B: velocities (so the carried momentum is current for whoever adopts next)
        for (eid, bot) in bots.iter() {
            let (_px, _py, vx, vy) = body_state(&bodies, bot.handle);
            wr.write_all(&frame(&json!({
                "op":"UpdateComponent","entity":eid,"comp":"vel","value":[vx,vy],
                "authority_epoch":bot.epoch
            })))
            .await
            .ok();
        }

        // (4) heartbeat ~4x/s (renew the region lease)
        if tick % ((hz as u64 / 4).max(1)) == 0 {
            wr.write_all(&frame(&json!({"op":"Heartbeat","worker_id":wid}))).await.ok();
        }

        tick += 1;
        acc_ticks += 1;
        if last_report.elapsed().as_secs_f64() >= 1.0 {
            let ach = acc_ticks as f64 / last_report.elapsed().as_secs_f64();
            eprintln!(
                "[zw {region}] tick={tick} owned={} view={} rejects={rejects} hz={ach:.1}",
                bots.len(),
                view_pos.len()
            );
            last_report = Instant::now();
            acc_ticks = 0;
        }

        if let Some(d) = duration {
            if t_start.elapsed().as_secs_f64() >= d {
                break;
            }
        }
    }

    wr.write_all(&frame(&json!({"op":"Disconnect"}))).await.ok();
    eprintln!("[zw {region}] done tick={tick} owned={} rejects={rejects}", bots.len());
}

fn body_state(bodies: &RigidBodySet, h: RigidBodyHandle) -> (f32, f32, f32, f32) {
    if let Some(b) = bodies.get(h) {
        let t = b.translation();
        let v = b.linvel();
        (t.x, t.y, v.x, v.y)
    } else {
        (0.0, 0.0, 0.0, 0.0)
    }
}

// which neighbour region a body at (px,py) has crossed into, given this worker's cell [x0,x1,y0,y1].
// x-axis takes priority (matches the 1-D proven path); a corner exit folds on x first, then y next tick.
fn fold_target(
    px: f32,
    py: f32,
    cell: &[f32; 4],
    neighbors: &HashMap<String, String>,
) -> Option<String> {
    let [x0, x1, y0, y1] = *cell;
    if px < x0 {
        if let Some(r) = neighbors.get("xlo") {
            return Some(r.clone());
        }
    }
    if px > x1 {
        if let Some(r) = neighbors.get("xhi") {
            return Some(r.clone());
        }
    }
    if py < y0 {
        if let Some(r) = neighbors.get("ylo") {
            return Some(r.clone());
        }
    }
    if py > y1 {
        if let Some(r) = neighbors.get("yhi") {
            return Some(r.clone());
        }
    }
    None
}

fn destroy_body(
    h: RigidBodyHandle,
    bodies: &mut RigidBodySet,
    islands: &mut IslandManager,
    colliders: &mut ColliderSet,
    ijoints: &mut ImpulseJointSet,
    mjoints: &mut MultibodyJointSet,
) {
    bodies.remove(h, islands, colliders, ijoints, mjoints, true);
}

#[allow(clippy::too_many_arguments)]
fn apply_op(
    f: &Value,
    region: &str,
    view_pos: &mut HashMap<String, [f32; 2]>,
    view_vel: &mut HashMap<String, [f32; 2]>,
    bots: &mut HashMap<String, Bot>,
    bodies: &mut RigidBodySet,
    islands: &mut IslandManager,
    colliders: &mut ColliderSet,
    ijoints: &mut ImpulseJointSet,
    mjoints: &mut MultibodyJointSet,
    radius: f32,
    rest: f32,
    rejects: &mut u64,
) {
    let op = f.get("op").and_then(|v| v.as_str()).unwrap_or("");
    match op {
        "AddEntity" => {
            let eid = f.get("entity").and_then(|v| v.as_str()).unwrap_or("").to_string();
            if eid.is_empty() {
                return;
            }
            // carried components may ride on AddEntity (send_full) — stash pos/vel for adoption
            if let Some(c) = f.get("components") {
                if let Some(p) = arr2(c.get("pos")) {
                    view_pos.insert(eid.clone(), p);
                }
                if let Some(v) = arr2(c.get("vel")) {
                    view_vel.insert(eid.clone(), v);
                }
            }
            view_pos.entry(eid).or_insert([0.0, 0.0]);
        }
        "ComponentUpdate" => {
            let eid = f.get("entity").and_then(|v| v.as_str()).unwrap_or("");
            let comp = f.get("comp").and_then(|v| v.as_str()).unwrap_or("");
            if let Some(p) = arr2(f.get("value")) {
                if comp == "pos" {
                    view_pos.insert(eid.to_string(), p);
                } else if comp == "vel" {
                    view_vel.insert(eid.to_string(), p);
                }
            }
        }
        "RemoveEntity" => {
            let eid = f.get("entity").and_then(|v| v.as_str()).unwrap_or("");
            view_pos.remove(eid);
            view_vel.remove(eid);
            if let Some(bot) = bots.remove(eid) {
                destroy_body(bot.handle, bodies, islands, colliders, ijoints, mjoints);
            }
        }
        "AuthorityChange" => {
            let eid = f.get("entity").and_then(|v| v.as_str()).unwrap_or("").to_string();
            let comp = f.get("comp").and_then(|v| v.as_str()).unwrap_or("");
            // only the physics-island root component drives the body lifecycle (pos). vel rides with it.
            if comp != "pos" && !comp.is_empty() {
                return;
            }
            let epoch = f.get("authority_epoch").and_then(|v| v.as_u64()).unwrap_or(0);
            // C2 pre-handoff intent: NOT a real authority change — log + ignore for the body lifecycle
            if f.get("state").and_then(|v| v.as_str()) == Some("AUTHORITY_LOSS_IMMINENT") {
                let tgt = f
                    .get("handoff_target_region")
                    .and_then(|v| v.as_str())
                    .unwrap_or("?");
                eprintln!("[zw {region}] LOSS-IMMINENT e={eid} target={tgt}");
                return;
            }
            let authoritative = f.get("authoritative").and_then(|v| v.as_bool()).unwrap_or(false);
            if authoritative {
                // ADOPT: create a rapier body from the carried pos+vel (exactly-one-zone simulates it now)
                if let Some(bot) = bots.get_mut(&eid) {
                    bot.epoch = epoch; // re-grant: refresh the epoch we fence writes with
                } else {
                    let p = *view_pos.get(&eid).unwrap_or(&[0.0, 0.0]);
                    let v = *view_vel.get(&eid).unwrap_or(&[0.0, 0.0]);
                    let rb = RigidBodyBuilder::dynamic()
                        .translation(vector![p[0], p[1]])
                        .linvel(vector![v[0], v[1]])
                        .linear_damping(0.0)
                        .ccd_enabled(true)
                        .build();
                    let h = bodies.insert(rb);
                    colliders.insert_with_parent(
                        ColliderBuilder::ball(radius).restitution(rest).density(1.0).build(),
                        h,
                        bodies,
                    );
                    bots.insert(eid.clone(), Bot { handle: h, epoch });
                    eprintln!(
                        "[zw {region}] AUTH-GAIN e={eid} epoch={epoch} adopt pos=[{:.3},{:.3}] vel=[{:.3},{:.3}]",
                        p[0], p[1], v[0], v[1]
                    );
                }
            } else {
                // LOSE: destroy the local body (the other zone owns it now)
                if let Some(bot) = bots.remove(&eid) {
                    destroy_body(bot.handle, bodies, islands, colliders, ijoints, mjoints);
                    eprintln!("[zw {region}] AUTH-LOSS e={eid} epoch={epoch} destroy");
                } else {
                    eprintln!("[zw {region}] AUTH-LOSS e={eid} epoch={epoch} (no local body)");
                }
            }
        }
        "UpdateRejected" => {
            let eid = f.get("entity").and_then(|v| v.as_str()).unwrap_or("?");
            let comp = f.get("comp").and_then(|v| v.as_str()).unwrap_or("?");
            let reason = f.get("reason").and_then(|v| v.as_str()).unwrap_or("?");
            *rejects += 1;
            eprintln!("[zw {region}] REJECTED e={eid} comp={comp} reason='{reason}'");
        }
        _ => {}
    }
}

fn arr2(v: Option<&Value>) -> Option<[f32; 2]> {
    let a = v?.as_array()?;
    if a.len() < 2 {
        return None;
    }
    Some([a[0].as_f64()? as f32, a[1].as_f64()? as f32])
}
