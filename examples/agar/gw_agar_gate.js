// Reality gate for the Godworks agar.io demo.
//
// This is intentionally a live-game gate: it joins through the HTTP gateway,
// drives a player across partition seams, and asserts conservation/ownership
// properties from the live InspectorFrame-derived state.
const http = require("http");
const net = require("net");

const BASE = process.env.GW_AGAR_URL || `http://127.0.0.1:${process.env.GW_HTTP || "8091"}`;
const BROKER_HOST = process.env.GW_HOST || "127.0.0.1";
const BROKER_PORT = parseInt(process.env.GW_PORT || "7777", 10);
const DURATION_MS = parseInt(process.env.GW_GATE_MS || "22000", 10);
const SAMPLE_MS = parseInt(process.env.GW_GATE_SAMPLE_MS || "250", 10);
const MIN_ENTITIES = parseInt(process.env.GW_GATE_MIN_ENTITIES || "60", 10);
const MIN_OWNERS = parseInt(process.env.GW_GATE_MIN_OWNERS || "2", 10);
const REQUIRE_PLAYER_HANDOFF = process.env.GW_GATE_REQUIRE_HANDOFF !== "0";
const BROWSER_TOKEN = process.env.GW_BROWSER_TOKEN || "browser-token";
const SPAWN_TOKEN = process.env.GW_CLIENT_TOKEN || "spawn-token";

function req(method, path, body) {
  return new Promise((resolve, reject) => {
    const data = body == null ? null : Buffer.from(JSON.stringify(body));
    const u = new URL(path, BASE);
    const r = http.request({
      method,
      hostname: u.hostname,
      port: u.port,
      path: u.pathname + u.search,
      headers: data ? { "content-type": "application/json", "content-length": data.length } : {}
    }, res => {
      let out = "";
      res.on("data", d => out += d);
      res.on("end", () => {
        try { resolve(JSON.parse(out || "{}")); } catch (_) { resolve(out); }
      });
    });
    r.on("error", reject);
    if (data) r.write(data);
    r.end();
  });
}

function distance(a, b) {
  return !a || !b ? 0 : Math.hypot(a[0] - b[0], a[1] - b[1]);
}

function frame(obj) {
  const body = Buffer.from(JSON.stringify(obj), "utf8");
  const head = Buffer.alloc(4);
  head.writeUInt32BE(body.length, 0);
  return Buffer.concat([head, body]);
}

function brokerProbe(connect, ops = [], waitMs = 450) {
  return new Promise((resolve, reject) => {
    const frames = [];
    let buf = Buffer.alloc(0);
    const sock = net.connect(BROKER_PORT, BROKER_HOST, () => {
      sock.write(frame(connect));
      let delay = 80;
      for (const op of ops) {
        setTimeout(() => {
          if (!sock.destroyed) sock.write(frame(op));
        }, delay);
        delay += 80;
      }
      setTimeout(() => {
        sock.destroy();
        resolve(frames);
      }, waitMs + delay);
    });
    sock.on("data", d => {
      buf = Buffer.concat([buf, d]);
      while (buf.length >= 4) {
        const n = buf.readUInt32BE(0);
        if (buf.length < 4 + n) break;
        const body = buf.slice(4, 4 + n);
        buf = buf.slice(4 + n);
        try { frames.push(JSON.parse(body.toString("utf8"))); } catch (_) {}
      }
    });
    sock.on("error", reject);
  });
}

function hasReject(frames, requestId, rejectedOp) {
  return frames.some(f => f.op === "UpdateRejected"
    && (!requestId || f.request_id === requestId)
    && (!rejectedOp || f.rejected_op === rejectedOp || f.comp === "role_policy"));
}

async function securityNegativeGate() {
  const failures = [];
  const meshSelf = await brokerProbe({ op: "WorkerConnect", worker_id: "agar-neg-mesh-self", region: "MESH" });
  if (!meshSelf.some(f => f.op === "AuthReject")) failures.push("peer-declared MESH without claim was not AuthReject");

  const wrongClaim = await brokerProbe({ op: "WorkerConnect", worker_id: "agar-neg-mesh-wrong-token", region: "MESH", auth_token: SPAWN_TOKEN });
  if (!wrongClaim.some(f => f.op === "AuthReject")) failures.push("spawn token could claim MESH region");

  const clientCreate = await brokerProbe(
    { op: "WorkerConnect", worker_id: "agar-neg-client", region: "CLIENT", attributes: ["role.client"], auth_token: BROWSER_TOKEN },
    [{ op: "CreateEntity", request_id: "neg-client-create", entity: "neg-client-create", region: "Z0_0", components: { pos: [1, 1], vel: [0, 0] } }]
  );
  if (!hasReject(clientCreate, "neg-client-create", "CreateEntity")) failures.push("CLIENT claim could CreateEntity");

  const clientInspect = await brokerProbe(
    { op: "WorkerConnect", worker_id: "agar-neg-client-inspect", region: "CLIENT", attributes: ["role.client"], auth_token: BROWSER_TOKEN },
    [{ op: "InspectorQuery", request_id: "neg-client-inspect", max_entities: 1 }]
  );
  if (!hasReject(clientInspect, "neg-client-inspect", "InspectorQuery")) failures.push("CLIENT claim could InspectorQuery");

  const workerMesh = await brokerProbe(
    { op: "WorkerConnect", worker_id: "agar-neg-worker-mesh", region: "AGAR_SPAWNER", auth_token: SPAWN_TOKEN },
    [{ op: "MeshHandoff", request_id: "neg-worker-mesh", entity: "neg-worker-mesh", target: "Z0_0", pos: [1, 1], components: { pos: [1, 1], vel: [0, 0] }, authority_epoch: 1, lease_epoch: 1 }]
  );
  if (!hasReject(workerMesh, "neg-worker-mesh", "MeshHandoff")) failures.push("ordinary worker could send MeshHandoff");

  const authorityMode = await brokerProbe(
    { op: "WorkerConnect", worker_id: "agar-neg-authority-mode", region: "AGAR_SPAWNER", auth_token: SPAWN_TOKEN },
    [{ op: "CreateEntity", request_id: "neg-authority-mode", entity: "neg-authority-mode", region: "Z0_0", components: { pos: [2, 2], vel: [0, 0], "authority.mode": { pos: "kernel_admin" } } }]
  );
  if (!authorityMode.some(f => (f.request_id === "neg-authority-mode" && (f.success === false || f.op === "UpdateRejected")))) {
    failures.push("ordinary worker could create platform-reserved authority.mode");
  }

  if (failures.length) {
    const err = new Error(`security-negative gate failed: ${failures.join("; ")}`);
    err.failures = failures;
    throw err;
  }
  return {
    peer_declared_mesh_rejected: true,
    wrong_claim_mesh_rejected: true,
    client_create_rejected: true,
    client_inspector_rejected: true,
    worker_mesh_handoff_rejected: true,
    authority_mode_create_rejected: true
  };
}

async function main() {
  const security = await securityNegativeGate();
  const join = await req("POST", "/join", { pos: [12, 12] });
  const player = join.id;
  const world = join.world || [0, 120, 0, 120];
  if (!player) throw new Error("join did not return a player id");

  const start = Date.now();
  let samples = 0;
  let maxEntities = 0;
  let maxOwners = 0;
  let unknownOwner = 0;
  let duplicateFrames = 0;
  let playerSeen = false;
  let playerStart = null;
  let playerLast = null;
  let playerPath = 0;
  let playerMaxDisplacement = 0;
  let playerOwners = new Set();
  let observedOwnerChanges = 0;
  let lastPlayerOwner = null;
  let driveTarget = null;
  let initialCommand = null;
  let clientTruthMatches = 0;
  let clientTruthMismatches = 0;
  let playerMissingAfterSeen = 0;
  let playerAbsentStreak = 0;
  let playerMaxAbsentStreak = 0;
  let probeMissingBeforeHandoff = 0;
  let probeMissingBeforeHandoffStreak = 0;
  let probeMaxMissingBeforeHandoffStreak = 0;
  let commandAfterHandoff = null;
  let commandAfterHandoffOwnerAtSend = null;

  while (Date.now() - start < DURATION_MS) {
    const state = await req("GET", "/state");
    const clientState = await req("GET", "/client-state");
    if (Array.isArray(state)) {
      samples++;
      maxEntities = Math.max(maxEntities, state.length);
      const ids = new Set();
      const owners = new Set();
      for (const e of state) {
        if (ids.has(e.e)) duplicateFrames++;
        ids.add(e.e);
        if (!e.o || e.o === "?") unknownOwner++;
        else owners.add(e.o);
      }
      maxOwners = Math.max(maxOwners, owners.size);
      const p = state.find(e => e.e === player);
      if (p && Array.isArray(p.p)) {
        playerAbsentStreak = 0;
        probeMissingBeforeHandoffStreak = 0;
        playerSeen = true;
        if (!playerStart) playerStart = p.p.slice();
        if (!driveTarget) {
          const midX = (world[0] + world[1]) / 2;
          const midY = (world[2] + world[3]) / 2;
          driveTarget = [
            p.p[0] < midX ? world[1] - 2 : world[0] + 2,
            p.p[1] < midY ? world[3] - 2 : world[2] + 2
          ];
        }
        if (!initialCommand) {
          initialCommand = await req("POST", "/input?wait=1", { id: player, target: driveTarget });
        }
        if (samples % 4 === 0) {
          await req("POST", "/input", { id: player, target: driveTarget });
        }
        if (playerLast) playerPath += distance(playerLast, p.p);
        playerLast = p.p.slice();
        playerMaxDisplacement = Math.max(playerMaxDisplacement, distance(playerStart, playerLast));
        if (p.o && p.o !== "?") playerOwners.add(p.o);
        if (p.o && p.o !== "?") lastPlayerOwner = p.o;
        observedOwnerChanges = Math.max(observedOwnerChanges, Number(p.owner_changes || 0));
        if (Array.isArray(clientState)) {
          const cp = clientState.find(e => e.e === player);
          if (cp && Array.isArray(cp.p) && distance(cp.p, p.p) < 1.5) clientTruthMatches++;
          else if (cp) clientTruthMismatches++;
        }
        if (!commandAfterHandoff && (playerOwners.size > 1 || observedOwnerChanges > 0)) {
          commandAfterHandoffOwnerAtSend = p.o || null;
          commandAfterHandoff = await req("POST", "/input?wait=1", { id: player, target: driveTarget || p.p });
        }
      } else if (playerSeen) {
        playerMissingAfterSeen++;
        playerAbsentStreak++;
        playerMaxAbsentStreak = Math.max(playerMaxAbsentStreak, playerAbsentStreak);
        if (playerOwners.size <= 1 && observedOwnerChanges <= 0) {
          probeMissingBeforeHandoff++;
          probeMissingBeforeHandoffStreak++;
          probeMaxMissingBeforeHandoffStreak = Math.max(probeMaxMissingBeforeHandoffStreak, probeMissingBeforeHandoffStreak);
        }
      }
    }
    await new Promise(r => setTimeout(r, SAMPLE_MS));
  }

  const moved = distance(playerStart, playerLast);
  const handoffObserved = playerOwners.size > 1 || observedOwnerChanges > 0;
  const initialCommandOwner = initialCommand && initialCommand.response && initialCommand.response.payload && initialCommand.response.payload.owner;
  const initialCommandOk = !!(
    initialCommand &&
    initialCommand.ok &&
    initialCommand.response &&
    initialCommand.response.success === true &&
    typeof initialCommandOwner === "string" &&
    initialCommandOwner.startsWith("agar-Z")
  );
  const commandOwner = commandAfterHandoff && commandAfterHandoff.response && commandAfterHandoff.response.payload && commandAfterHandoff.response.payload.owner;
  const commandAfterHandoffOk = !!(
    commandAfterHandoff &&
    commandAfterHandoff.ok &&
    commandAfterHandoff.response &&
    commandAfterHandoff.response.success === true &&
    typeof commandOwner === "string" &&
    commandOwner.startsWith("agar-Z")
  );
  const report = {
    ok: true,
    base: BASE,
    security,
    samples,
    player,
    max_entities: maxEntities,
    max_owners: maxOwners,
    unknown_owner_frames: unknownOwner,
    duplicate_frames: duplicateFrames,
    player_seen: playerSeen,
    player_moved: moved,
    player_path: playerPath,
    player_max_displacement: playerMaxDisplacement,
    player_owner_count: playerOwners.size,
    observed_owner_changes: observedOwnerChanges,
    client_truth_matches: clientTruthMatches,
    client_truth_mismatches: clientTruthMismatches,
    player_missing_after_seen: playerMissingAfterSeen,
    player_max_absent_streak: playerMaxAbsentStreak,
    player_terminal_absent_streak: playerAbsentStreak,
    probe_missing_before_handoff: probeMissingBeforeHandoff,
    probe_max_missing_before_handoff_streak: probeMaxMissingBeforeHandoffStreak,
    initial_command_ok: initialCommandOk,
    initial_command_owner: initialCommandOwner || null,
    command_after_handoff_ok: commandAfterHandoffOk,
    command_after_handoff_owner: commandOwner || null,
    command_after_handoff_owner_at_send: commandAfterHandoffOwnerAtSend,
    last_player_owner: lastPlayerOwner,
    drive_target: driveTarget
  };

  const failures = [];
  if (samples < 5) failures.push("too few samples");
  if (maxEntities < MIN_ENTITIES) failures.push(`entity count too low: ${maxEntities} < ${MIN_ENTITIES}`);
  if (maxOwners < MIN_OWNERS) failures.push(`owner diversity too low: ${maxOwners} < ${MIN_OWNERS}`);
  if (unknownOwner > 0) failures.push(`unknown owners observed: ${unknownOwner}`);
  if (duplicateFrames > 0) failures.push(`duplicate entity ids observed: ${duplicateFrames}`);
  if (!playerSeen) failures.push("player never appeared in live state");
  if (probeMaxMissingBeforeHandoffStreak > 2) failures.push(`probe player disappeared before seam proof: ${probeMaxMissingBeforeHandoffStreak} consecutive samples`);
  if (!initialCommandOk) failures.push(`initial command was not acknowledged by an authoritative worker: ${JSON.stringify(initialCommand)}`);
  if (playerPath < 8) failures.push(`player path too short: ${playerPath.toFixed(2)}`);
  if (REQUIRE_PLAYER_HANDOFF && !handoffObserved) failures.push("player handoff was not observed");
  if (clientTruthMatches < 3) failures.push(`client stream did not match inspector often enough: ${clientTruthMatches}`);
  if (clientTruthMismatches > 0) failures.push(`client stream mismatched inspector: ${clientTruthMismatches}`);
  if (!commandAfterHandoffOk) failures.push(`command after handoff was not acknowledged by current owner: ${JSON.stringify(commandAfterHandoff)}`);

  if (failures.length) {
    report.ok = false;
    report.failures = failures;
    console.error(JSON.stringify(report, null, 2));
    process.exit(1);
  }
  console.log(JSON.stringify(report, null, 2));
}

main().catch(err => {
  console.error(err && err.stack || String(err));
  process.exit(1);
});
