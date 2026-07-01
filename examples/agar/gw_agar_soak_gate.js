// Multi-player soak gate for the Godworks agar.io demo.
//
// This is a live-game ruler, not a synthetic protocol fixture. It joins several
// player probes through the HTTP product path, repeatedly commands them across
// the arena, and checks that broker truth and the CLIENT stream keep agreeing
// while owners change under sustained input.
const http = require("http");

const BASE = process.env.GW_AGAR_URL || `http://127.0.0.1:${process.env.GW_HTTP || "8091"}`;
const DURATION_MS = parseInt(process.env.GW_SOAK_MS || "30000", 10);
const SAMPLE_MS = parseInt(process.env.GW_SOAK_SAMPLE_MS || "300", 10);
const COMMAND_MS = parseInt(process.env.GW_SOAK_COMMAND_MS || "1500", 10);
const PLAYERS = parseInt(process.env.GW_SOAK_PLAYERS || "4", 10);
const MIN_HANDOFF_PLAYERS = parseInt(process.env.GW_SOAK_MIN_HANDOFF_PLAYERS || `${Math.min(2, PLAYERS)}`, 10);
const MIN_PATH = parseFloat(process.env.GW_SOAK_MIN_PLAYER_PATH || "18");
const MAX_MISSING_STREAK = parseInt(process.env.GW_SOAK_MAX_MISSING_STREAK || "4", 10);
const CLIENT_EPSILON = parseFloat(process.env.GW_SOAK_CLIENT_EPSILON || "1.75");

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
    r.setTimeout(3500, () => {
      r.destroy(new Error(`request timeout: ${method} ${path}`));
    });
    r.on("error", reject);
    if (data) r.write(data);
    r.end();
  });
}

function distance(a, b) {
  return !Array.isArray(a) || !Array.isArray(b) ? 0 : Math.hypot(a[0] - b[0], a[1] - b[1]);
}

function clamp(v, min, max) {
  return Math.max(min, Math.min(max, v));
}

function planFor(world, index) {
  const [x0, x1, y0, y1] = world;
  const mx = (x0 + x1) / 2;
  const my = (y0 + y1) / 2;
  const margin = Math.max(8, Math.min(x1 - x0, y1 - y0) * 0.1);
  const starts = [
    [x0 + margin, y0 + margin],
    [x1 - margin, y0 + margin],
    [x0 + margin, y1 - margin],
    [x1 - margin, y1 - margin],
    [mx, y0 + margin],
    [mx, y1 - margin],
    [x0 + margin, my],
    [x1 - margin, my]
  ];
  const start = starts[index % starts.length];
  const opposite = [x0 + x1 - start[0], y0 + y1 - start[1]];
  const cross = index % 2 === 0 ? [opposite[0], my] : [mx, opposite[1]];
  return {
    start,
    targets: [
      [clamp(opposite[0], x0 + 2, x1 - 2), clamp(opposite[1], y0 + 2, y1 - 2)],
      [clamp(cross[0], x0 + 2, x1 - 2), clamp(cross[1], y0 + 2, y1 - 2)],
      [clamp(start[0], x0 + 2, x1 - 2), clamp(start[1], y0 + 2, y1 - 2)]
    ]
  };
}

function commandAcked(response) {
  return !!(
    response &&
    response.ok &&
    response.response &&
    response.response.success === true &&
    response.response.payload &&
    typeof response.response.payload.owner === "string" &&
    response.response.payload.owner.startsWith("agar-Z")
  );
}

async function commandPlayer(player, target) {
  const response = await req("POST", "/input?wait=1", { id: player.id, target });
  const acked = commandAcked(response);
  if (acked) {
    player.commandAcks++;
    player.lastCommandOwner = response.response.payload.owner;
    if (player.ownerSet.size > 1 || player.ownerChanges > 0) player.postHandoffAcks++;
  } else {
    player.commandFailures++;
    player.lastCommandFailure = response;
  }
}

async function samplePlayers(players) {
  const state = await req("GET", "/state");
  const clientState = await req("GET", "/client-state");
  if (!Array.isArray(state)) throw new Error(`broker state is not an array: ${JSON.stringify(state).slice(0, 256)}`);
  if (!Array.isArray(clientState)) throw new Error(`client state is not an array: ${JSON.stringify(clientState).slice(0, 256)}`);

  const ids = new Set();
  let duplicates = 0;
  let unknownOwners = 0;
  const owners = new Set();
  for (const row of state) {
    if (ids.has(row.e)) duplicates++;
    ids.add(row.e);
    if (!row.o || row.o === "?") unknownOwners++;
    else owners.add(row.o);
  }

  let clientMatches = 0;
  let clientMismatches = 0;
  for (const player of players) {
    const row = state.find(e => e.e === player.id);
    if (row && Array.isArray(row.p)) {
      player.seen = true;
      player.samplesSeen++;
      player.missingStreak = 0;
      player.missingBeforeHandoffStreak = 0;
      if (!player.startPos) player.startPos = row.p.slice();
      if (player.lastPos) player.path += distance(player.lastPos, row.p);
      player.lastPos = row.p.slice();
      player.maxDisplacement = Math.max(player.maxDisplacement, distance(player.startPos, player.lastPos));
      if (row.o && row.o !== "?") player.ownerSet.add(row.o);
      player.ownerChanges = Math.max(player.ownerChanges, Number(row.owner_changes || 0));

      const clientRow = clientState.find(e => e.e === player.id);
      if (clientRow && Array.isArray(clientRow.p) && distance(clientRow.p, row.p) <= CLIENT_EPSILON) {
        player.clientMatches++;
        clientMatches++;
      } else if (clientRow) {
        player.clientMismatches++;
        clientMismatches++;
      }
    } else if (player.seen) {
      player.missingStreak++;
      player.maxMissingStreak = Math.max(player.maxMissingStreak, player.missingStreak);
      if (player.ownerSet.size <= 1 && player.ownerChanges <= 0) {
        player.missingBeforeHandoffStreak++;
        player.maxMissingBeforeHandoffStreak = Math.max(
          player.maxMissingBeforeHandoffStreak,
          player.missingBeforeHandoffStreak
        );
      }
    }
  }

  return {
    entity_count: state.length,
    owner_count: owners.size,
    duplicate_frames: duplicates,
    unknown_owner_frames: unknownOwners,
    client_matches: clientMatches,
    client_mismatches: clientMismatches
  };
}

async function main() {
  if (PLAYERS < 2) throw new Error("GW_SOAK_PLAYERS must be >= 2");

  const firstPlan = planFor([0, 120, 0, 120], 0);
  const firstJoin = await req("POST", "/join", { pos: firstPlan.start });
  const world = Array.isArray(firstJoin.world) ? firstJoin.world : [0, 120, 0, 120];
  const players = [];

  for (let i = 0; i < PLAYERS; i++) {
    const plan = planFor(world, i);
    const join = i === 0 ? firstJoin : await req("POST", "/join", { pos: plan.start });
    if (!join.id) throw new Error(`join ${i} did not return a player id`);
    players.push({
      id: join.id,
      plan,
      seen: false,
      samplesSeen: 0,
      startPos: null,
      lastPos: null,
      path: 0,
      maxDisplacement: 0,
      ownerSet: new Set(),
      ownerChanges: 0,
      missingStreak: 0,
      maxMissingStreak: 0,
      missingBeforeHandoffStreak: 0,
      maxMissingBeforeHandoffStreak: 0,
      clientMatches: 0,
      clientMismatches: 0,
      commandAcks: 0,
      postHandoffAcks: 0,
      commandFailures: 0,
      lastCommandOwner: null,
      lastCommandFailure: null,
      targetIndex: 0
    });
  }

  await new Promise(resolve => setTimeout(resolve, 900));
  await Promise.all(players.map(player => commandPlayer(player, player.plan.targets[player.targetIndex])));

  const start = Date.now();
  let samples = 0;
  let maxEntities = 0;
  let minEntities = Infinity;
  let maxOwners = 0;
  let duplicateFrames = 0;
  let unknownOwnerFrames = 0;
  let clientTruthMatches = 0;
  let clientTruthMismatches = 0;
  let nextCommandAt = start + COMMAND_MS;
  let phase = 1;
  let herdSent = false;

  while (Date.now() - start < DURATION_MS) {
    const sample = await samplePlayers(players);
    samples++;
    maxEntities = Math.max(maxEntities, sample.entity_count);
    minEntities = Math.min(minEntities, sample.entity_count);
    maxOwners = Math.max(maxOwners, sample.owner_count);
    duplicateFrames += sample.duplicate_frames;
    unknownOwnerFrames += sample.unknown_owner_frames;
    clientTruthMatches += sample.client_matches;
    clientTruthMismatches += sample.client_mismatches;

    const now = Date.now();
    if (!herdSent && now - start > DURATION_MS / 2) {
      await req("POST", "/herd");
      herdSent = true;
    }
    if (now >= nextCommandAt) {
      await Promise.all(players.map((player) => {
        if (player.ownerSet.size > 1 || player.ownerChanges > 0) {
          player.targetIndex = (phase + player.targetIndex) % player.plan.targets.length;
        }
        const target = player.plan.targets[player.targetIndex];
        return commandPlayer(player, target);
      }));
      phase++;
      nextCommandAt = Date.now() + COMMAND_MS;
    }
    await new Promise(resolve => setTimeout(resolve, SAMPLE_MS));
  }

  const playersWithHandoff = players.filter(p => p.ownerSet.size > 1 || p.ownerChanges > 0).length;
  const playersWithPostHandoffAck = players.filter(p => p.postHandoffAcks > 0).length;
  const totalCommandAcks = players.reduce((sum, p) => sum + p.commandAcks, 0);
  const totalCommandFailures = players.reduce((sum, p) => sum + p.commandFailures, 0);
  const failures = [];

  if (samples < 5) failures.push("too few samples");
  if (maxEntities < PLAYERS) failures.push(`entity count never covered all players: max=${maxEntities} players=${PLAYERS}`);
  if (maxOwners < 2) failures.push(`owner diversity too low during soak: ${maxOwners} < 2`);
  if (duplicateFrames > 0) failures.push(`duplicate entity ids observed: ${duplicateFrames}`);
  if (unknownOwnerFrames > 0) failures.push(`unknown owners observed: ${unknownOwnerFrames}`);
  if (playersWithHandoff < MIN_HANDOFF_PLAYERS) failures.push(`too few players crossed ownership seams: ${playersWithHandoff} < ${MIN_HANDOFF_PLAYERS}`);
  if (playersWithPostHandoffAck < MIN_HANDOFF_PLAYERS) failures.push(`too few post-handoff command ACKs: ${playersWithPostHandoffAck} < ${MIN_HANDOFF_PLAYERS}`);
  if (clientTruthMatches < PLAYERS) failures.push(`client stream matched too few player samples: ${clientTruthMatches} < ${PLAYERS}`);
  if (clientTruthMismatches > 0) failures.push(`client stream mismatched broker truth: ${clientTruthMismatches}`);
  if (totalCommandAcks < PLAYERS * 2) failures.push(`too few command acknowledgements: ${totalCommandAcks} < ${PLAYERS * 2}`);
  if (totalCommandFailures > 0) failures.push(`command failures observed: ${totalCommandFailures}`);

  const playerReports = players.map(player => {
    if (!player.seen) failures.push(`${player.id} never appeared in broker state`);
    if (player.path < MIN_PATH) failures.push(`${player.id} path too short: ${player.path.toFixed(2)} < ${MIN_PATH}`);
    if (player.maxMissingBeforeHandoffStreak > 2) {
      failures.push(`${player.id} disappeared before seam proof: ${player.maxMissingBeforeHandoffStreak} consecutive samples`);
    }
    if (player.maxMissingStreak > MAX_MISSING_STREAK) failures.push(`${player.id} missing streak too long: ${player.maxMissingStreak} > ${MAX_MISSING_STREAK}`);
    if (player.clientMatches <= 0) failures.push(`${player.id} never matched CLIENT stream`);
    return {
      id: player.id,
      seen: player.seen,
      samples_seen: player.samplesSeen,
      path: Number(player.path.toFixed(3)),
      max_displacement: Number(player.maxDisplacement.toFixed(3)),
      owner_count: player.ownerSet.size,
      owner_changes: player.ownerChanges,
      max_missing_streak: player.maxMissingStreak,
      max_missing_before_handoff_streak: player.maxMissingBeforeHandoffStreak,
      client_matches: player.clientMatches,
      client_mismatches: player.clientMismatches,
      command_acks: player.commandAcks,
      post_handoff_acks: player.postHandoffAcks,
      command_failures: player.commandFailures,
      last_command_owner: player.lastCommandOwner
    };
  });

  const report = {
    ok: failures.length === 0,
    base: BASE,
    duration_ms: DURATION_MS,
    players: PLAYERS,
    samples,
    min_entities: Number.isFinite(minEntities) ? minEntities : 0,
    max_entities: maxEntities,
    max_owners: maxOwners,
    duplicate_frames: duplicateFrames,
    unknown_owner_frames: unknownOwnerFrames,
    client_truth_matches: clientTruthMatches,
    client_truth_mismatches: clientTruthMismatches,
    command_acks: totalCommandAcks,
    command_failures: totalCommandFailures,
    players_with_handoff: playersWithHandoff,
    players_with_post_handoff_ack: playersWithPostHandoffAck,
    herd_sent: herdSent,
    player_reports: playerReports
  };

  if (failures.length) {
    report.failures = failures;
    console.error(JSON.stringify(report, null, 2));
    process.exit(1);
  }
  console.log(JSON.stringify(report));
}

main().catch(err => {
  console.error(err && err.stack || String(err));
  process.exit(1);
});
