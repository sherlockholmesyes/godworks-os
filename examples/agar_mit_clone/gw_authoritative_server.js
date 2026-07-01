"use strict";

const express = require("express");
const fs = require("fs");
const http = require("http");
const ioFactory = require("socket.io");
const net = require("net");
const path = require("path");

let util = null;
try { util = require("./src/server/lib/util"); } catch (_) {}

const HOST = process.env.GW_HOST || "127.0.0.1";
const PORT = parseInt(process.env.GW_PORT || "7990", 10);
const HTTP_PORT = parseInt(process.env.PORT || process.env.GW_AUTH_HTTP || "3000", 10);
const OBS_TOKEN = process.env.GW_OBS_TOKEN || "obs-token";
const CLIENT_TOKEN = process.env.GW_CLIENT_TOKEN || "client-token";
const CONTROL_BASE = parseInt(process.env.GW_CONTROL_BASE || "8100", 10);
const COLS = parseInt(process.env.GW_COLS || "4", 10);
const ROWS = parseInt(process.env.GW_ROWS || "4", 10);
const WIDTH = parseFloat(process.env.GW_WIDTH || "5000");
const HEIGHT = parseFloat(process.env.GW_HEIGHT || "5000");
const UPDATE_HZ = parseFloat(process.env.GW_UPDATE_HZ || "25");
const STATE_ENTITY_LIMIT = parseInt(process.env.GW_AUTH_STATE_ENTITY_LIMIT || "20000", 10);
const COMMAND_ACK_TIMEOUT_MS = parseInt(process.env.GW_AUTH_COMMAND_ACK_TIMEOUT_MS || "1000", 10);
const COMMAND_MAX_ATTEMPTS = parseInt(process.env.GW_AUTH_COMMAND_MAX_ATTEMPTS || "4", 10);

const app = express();
const httpServer = http.Server(app);
const io = ioFactory(httpServer);
const builtClientRoot = path.join(__dirname, "bin", "client");
const rawClientRoot = path.join(__dirname, "src", "client");
const clientRoot = process.env.GW_CLIENT_ROOT
  || (fs.existsSync(path.join(builtClientRoot, "js", "app.js")) ? builtClientRoot : rawClientRoot);

const entities = new Map();
const players = new Map();
let obsSock = null;
let clientSock = null;
let obsBuf = Buffer.alloc(0);
let clientBuf = Buffer.alloc(0);
let requestSeq = 0;
let commandResponses = 0;
let commandRejects = 0;
let commandTransientRejects = 0;
let commandTimeouts = 0;
let commandTimeoutRetries = 0;
let ownerChangeResends = 0;
let despawnFallbacks = 0;
let despawnFailures = 0;
const rejectedCommands = [];
const transientRejectedCommands = [];
const timedOutCommands = [];
const ownerChangeResendEvents = [];
const failedDespawns = [];
const pendingCommands = new Map();

app.use(express.static(clientRoot));

function frame(obj) {
  const body = Buffer.from(JSON.stringify(obj), "utf8");
  const header = Buffer.alloc(4);
  header.writeUInt32BE(body.length, 0);
  return Buffer.concat([header, body]);
}

function send(sock, obj) {
  if (sock && !sock.destroyed) sock.write(frame(obj));
}

function connectValue(workerId, region, token, attributes) {
  const msg = { op: "WorkerConnect", worker_id: workerId, region, attributes: attributes || [] };
  if (token) msg.auth_token = token;
  return msg;
}

function connectBrokerStream(region, token, workerId, attributes, onFrame) {
  const sock = net.connect(PORT, HOST, () => {
    send(sock, connectValue(workerId, region, token, attributes));
    if (region === "OBS") {
      send(sock, { op: "Interest", center: [WIDTH / 2, HEIGHT / 2], radius: 1e9 });
      setInterval(() => {
        send(sock, { op: "InspectorQuery", request_id: `auth-${Date.now()}`, max_entities: 20000 });
      }, 100);
    }
    console.error(`[auth-server] connected ${region} ${HOST}:${PORT}`);
  });
  sock.on("data", d => onFrame(sock, d));
  sock.on("error", e => console.error(`[auth-server] ${region} broker error ${e.message}`));
  sock.on("close", () => {
    console.error(`[auth-server] ${region} broker closed`);
    setTimeout(() => {
      if (region === "OBS") obsSock = connectObs();
      else clientSock = connectClient();
    }, 1000);
  });
  return sock;
}

function parseFrames(buffer, data, cb) {
  buffer = Buffer.concat([buffer, data]);
  while (buffer.length >= 4) {
    const n = buffer.readUInt32BE(0);
    if (buffer.length < 4 + n) break;
    let msg = null;
    try { msg = JSON.parse(buffer.slice(4, 4 + n).toString("utf8")); } catch (_) {}
    buffer = buffer.slice(4 + n);
    if (msg) cb(msg);
  }
  return buffer;
}

function connectObs() {
  return connectBrokerStream("OBS", OBS_TOKEN, "gw-authoritative-server-obs", ["observer", "inspector"], (_sock, data) => {
    obsBuf = parseFrames(obsBuf, data, handleObsFrame);
  });
}

function connectClient() {
  return connectBrokerStream("CLIENT", CLIENT_TOKEN, "gw-authoritative-server-client", ["role.client"], (_sock, data) => {
    clientBuf = parseFrames(clientBuf, data, handleClientFrame);
  });
}

function handleObsFrame(msg) {
  if (msg.op === "AddEntity") {
    const c = msg.components || {};
    const entity = ensureEntity(msg.entity);
    if (Array.isArray(c.pos)) entity.pos = c.pos.slice();
    if (Array.isArray(c.vel)) entity.vel = c.vel.slice();
    if (c.mass != null) entity.mass = Number(c.mass) || entity.mass;
    if (c.kind != null) entity.kind = String(c.kind);
    if (c.name != null) entity.name = String(c.name);
    if (c.hue != null) entity.hue = Number(c.hue) || entity.hue;
  } else if (msg.op === "ComponentUpdate") {
    const entity = ensureEntity(msg.entity);
    if (msg.comp === "pos" && Array.isArray(msg.value)) entity.pos = msg.value.slice();
    else if (msg.comp === "vel" && Array.isArray(msg.value)) entity.vel = msg.value.slice();
    else if (msg.comp === "mass") entity.mass = Number(msg.value) || entity.mass;
    else if (msg.comp === "kind") entity.kind = String(msg.value);
    else if (msg.comp === "name") entity.name = String(msg.value || "");
    else if (msg.comp === "hue") entity.hue = Number(msg.value) || entity.hue;
  } else if (msg.op === "RemoveEntity") {
    entities.delete(msg.entity);
    for (const player of players.values()) {
      if (player.entity === msg.entity && player.socket.connected) {
        player.socket.emit("RIP");
      }
    }
  } else if (msg.op === "InspectorFrame") {
    for (const row of msg.entities || []) {
      const entity = ensureEntity(row.entity);
      if (Array.isArray(row.pos)) entity.pos = row.pos.slice();
      const oldOwner = entity.owner || entity.region || null;
      entity.region = row.region || entity.region;
      entity.owner = (((row.authority || {}).pos || {}).owner) || entity.owner || entity.region;
      maybeResendTargetAfterOwnerChange(row.entity, oldOwner, entity.owner);
    }
  }
}

function handleClientFrame(msg) {
  if (msg.op !== "CommandResponse") return;
  commandResponses++;
  const pending = pendingCommands.get(msg.request_id);
  pendingCommands.delete(msg.request_id);
  const player = pending ? players.get(pending.player) : null;
  if (player && player.commandInFlight === msg.request_id) {
    player.commandInFlight = null;
  }
  if (!msg.success) {
    const retryable = pending
      && isRetryableCommandReject(msg.reason)
      && (pending.attempt || 0) < 2
      && player
      && player.spawned
      && entities.has(player.entity);
    if (retryable) {
      commandTransientRejects++;
      recordCommandReject(transientRejectedCommands, msg, pending);
      player.queuedTarget = pending.target;
      setTimeout(() => flushQueuedTarget(player, (pending.attempt || 0) + 1), 25);
    } else {
      commandRejects++;
      recordCommandReject(rejectedCommands, msg, pending);
    }
  } else if (player && player.queuedTarget) {
    setTimeout(() => flushQueuedTarget(player, 0), 0);
  }
  while (pendingCommands.size > 1024) {
    const oldest = pendingCommands.keys().next().value;
    if (oldest === undefined) break;
    pendingCommands.delete(oldest);
  }
}

function isRetryableCommandReject(reason) {
  const text = String(reason || "").toLowerCase();
  return text.includes("stale command authority")
    || text.includes("no authoritative worker")
    || text.includes("entity not owned");
}

function recordCommandReject(list, msg, pending) {
  list.push({
    request_id: msg.request_id || null,
    entity: msg.entity || (pending && pending.entity) || null,
    player: (pending && pending.player) || null,
    reason: msg.reason || "unknown",
    target: (pending && pending.target) || null,
    attempt: pending ? (pending.attempt || 0) : null,
    age_ms: pending ? Math.max(0, Date.now() - pending.sentAt) : null,
  });
  while (list.length > 16) list.shift();
}

function recordCommandTimeout(pending, player, retried) {
  timedOutCommands.push({
    request_id: pending.request_id || null,
    entity: pending.entity || null,
    player: pending.player || null,
    target: pending.target || null,
    attempt: pending.attempt || 0,
    age_ms: Math.max(0, Date.now() - pending.sentAt),
    retried: !!retried,
    player_connected: !!(player && player.socket && player.socket.connected),
  });
  while (timedOutCommands.length > 16) timedOutCommands.shift();
}

function playerForEntity(entityId) {
  for (const player of players.values()) {
    if (player.entity === entityId) return player;
  }
  return null;
}

function recordOwnerChangeResend(player, oldOwner, newOwner, target) {
  ownerChangeResendEvents.push({
    entity: player.entity,
    player: player.id,
    old_owner: oldOwner || null,
    new_owner: newOwner || null,
    target,
    queued: !!player.commandInFlight,
    at: Date.now(),
  });
  while (ownerChangeResendEvents.length > 16) ownerChangeResendEvents.shift();
}

function maybeResendTargetAfterOwnerChange(entityId, oldOwner, newOwner) {
  if (!oldOwner || !newOwner || oldOwner === newOwner) return;
  const player = playerForEntity(entityId);
  if (!player || !player.spawned || !player.socket.connected) return;
  const target = player.lastWorldTarget || (player.lastTarget ? worldTargetFor(player, player.lastTarget) : null);
  if (!target) return;
  ownerChangeResends++;
  recordOwnerChangeResend(player, oldOwner, newOwner, target);
  if (player.commandInFlight) {
    player.queuedTarget = target;
    return;
  }
  setTimeout(() => sendTargetCommand(player, target, 0), 0);
}

function recordFailedDespawn(player, reason, region) {
  despawnFailures++;
  failedDespawns.push({
    player: player.id,
    entity: player.entity,
    reason: String(reason || "unknown"),
    region: region || null,
    ts: Date.now(),
  });
  while (failedDespawns.length > 16) failedDespawns.shift();
}

function ensureEntity(id) {
  if (!entities.has(id)) {
    entities.set(id, { id, pos: [WIDTH / 2, HEIGHT / 2], vel: [0, 0], mass: 1, kind: "cell", name: "", hue: 100, owner: null, region: null });
  }
  return entities.get(id);
}

function massToRadius(mass) {
  if (util && typeof util.massToRadius === "function") return util.massToRadius(mass);
  return 4 + Math.sqrt(Math.max(1, mass)) * 6;
}

function regionFor(pos) {
  const col = Math.max(0, Math.min(COLS - 1, Math.floor((pos[0] / WIDTH) * COLS)));
  const row = Math.max(0, Math.min(ROWS - 1, Math.floor((pos[1] / HEIGHT) * ROWS)));
  return { col, row, region: `Z${col}_${row}`, index: row * COLS + col };
}

function regionForOwner(owner) {
  const m = /^auth-Z(\d+)_(\d+)$/.exec(String(owner || ""));
  if (!m) return null;
  const col = Math.max(0, Math.min(COLS - 1, Number(m[1])));
  const row = Math.max(0, Math.min(ROWS - 1, Number(m[2])));
  return { col, row, region: `Z${col}_${row}`, index: row * COLS + col };
}

function spawnPoint() {
  return [
    WIDTH / 2 + (Math.random() - 0.5) * WIDTH * 0.1,
    HEIGHT / 2 + (Math.random() - 0.5) * HEIGHT * 0.1,
  ];
}

function clampWorld(pos) {
  return {
    x: Math.max(0, Math.min(WIDTH, Number(pos.x) || 0)),
    y: Math.max(0, Math.min(HEIGHT, Number(pos.y) || 0)),
  };
}

function worldTargetFor(player, target) {
  const raw = target || {};
  const entity = entities.get(player.entity);
  const base = entity && Array.isArray(entity.pos) ? entity.pos : [WIDTH / 2, HEIGHT / 2];
  return clampWorld({
    x: base[0] + (Number(raw.x) || 0),
    y: base[1] + (Number(raw.y) || 0),
  });
}

function flushQueuedTarget(player, attempt) {
  if (!player || !player.queuedTarget || player.commandInFlight) return;
  const target = player.queuedTarget;
  player.queuedTarget = null;
  sendTargetCommand(player, target, attempt || 0);
}

function sendTargetCommand(player, worldTarget, attempt) {
  if (!player.spawned || !entities.has(player.entity)) return;
  player.lastWorldTarget = worldTarget;
  if (player.commandInFlight) {
    player.queuedTarget = worldTarget;
    return;
  }
  const requestId = `auth-cmd-${requestSeq++}`;
  player.commandInFlight = requestId;
  pendingCommands.set(requestId, {
    request_id: requestId,
    entity: player.entity,
    player: player.id,
    target: worldTarget,
    attempt: attempt || 0,
    sentAt: Date.now(),
  });
  send(clientSock, {
    op: "CommandRequest",
    request_id: requestId,
    entity: player.entity,
    command: "set_target",
    payload: { target: worldTarget },
  });
}

function reapCommandTimeouts() {
  const now = Date.now();
  for (const [requestId, pending] of Array.from(pendingCommands.entries())) {
    const age = now - pending.sentAt;
    if (age < COMMAND_ACK_TIMEOUT_MS) continue;

    pendingCommands.delete(requestId);
    const player = players.get(pending.player);
    if (player && player.commandInFlight === requestId) {
      player.commandInFlight = null;
    }

    const retryable = player
      && player.socket
      && player.socket.connected
      && player.spawned
      && entities.has(player.entity)
      && (pending.attempt || 0) + 1 < COMMAND_MAX_ATTEMPTS;

    if (retryable) {
      commandTimeoutRetries++;
      recordCommandTimeout(pending, player, true);
      player.queuedTarget = player.queuedTarget || pending.target;
      setTimeout(() => flushQueuedTarget(player, (pending.attempt || 0) + 1), 0);
    } else {
      commandTimeouts++;
      recordCommandTimeout(pending, player, false);
    }
  }
}

function commandHealthSnapshot() {
  const now = Date.now();
  let inFlightPlayers = 0;
  let stuckPlayers = 0;
  let maxAge = 0;
  for (const player of players.values()) {
    if (!player.commandInFlight) continue;
    inFlightPlayers++;
    const pending = pendingCommands.get(player.commandInFlight);
    const age = pending ? Math.max(0, now - pending.sentAt) : COMMAND_ACK_TIMEOUT_MS;
    maxAge = Math.max(maxAge, age);
    if (age >= COMMAND_ACK_TIMEOUT_MS) stuckPlayers++;
  }
  return {
    pending: pendingCommands.size,
    inFlightPlayers,
    stuckPlayers,
    maxInFlightAgeMs: Math.round(maxAge),
    ackTimeoutMs: COMMAND_ACK_TIMEOUT_MS,
    maxAttempts: COMMAND_MAX_ATTEMPTS,
  };
}

function postJson(url, body) {
  return new Promise((resolve, reject) => {
    const parsed = new URL(url);
    const text = JSON.stringify(body);
    const req = http.request({
      hostname: parsed.hostname,
      port: parsed.port,
      path: parsed.pathname,
      method: "POST",
      headers: { "content-type": "application/json", "content-length": Buffer.byteLength(text) },
      timeout: 3000,
    }, res => {
      let out = "";
      res.setEncoding("utf8");
      res.on("data", chunk => { out += chunk; });
      res.on("end", () => {
        try { resolve(JSON.parse(out || "{}")); } catch (_) { resolve({ ok: false, reason: out }); }
      });
    });
    req.on("timeout", () => req.destroy(new Error(`timeout POST ${url}`)));
    req.on("error", reject);
    req.write(text);
    req.end();
  });
}

async function spawnPlayer(player) {
  const pos = spawnPoint();
  const region = regionFor(pos);
  const url = `http://127.0.0.1:${CONTROL_BASE + region.index}/spawn`;
  const body = {
    entity: player.entity,
    pos,
    kind: "player",
    mass: 10,
    name: player.name,
    hue: player.hue,
  };
  const reply = await postJson(url, body);
  if (!reply.ok) throw new Error(reply.reason || `spawn failed through ${url}`);
  return reply;
}

async function despawnPlayer(player) {
  const entity = entities.get(player.entity);
  const primary = regionForOwner(entity && entity.owner)
    || regionFor((entity && entity.pos) || player.spawnPos || [WIDTH / 2, HEIGHT / 2]);
  const tried = new Set();
  const regions = [primary];
  for (let row = 0; row < ROWS; row++) {
    for (let col = 0; col < COLS; col++) {
      regions.push({ col, row, region: `Z${col}_${row}`, index: row * COLS + col });
    }
  }
  let firstFailure = null;
  for (const region of regions) {
    if (!region || tried.has(region.index)) continue;
    tried.add(region.index);
    const url = `http://127.0.0.1:${CONTROL_BASE + region.index}/despawn`;
    try {
      const reply = await postJson(url, { entity: player.entity });
      if (reply.ok) {
        if (region.index !== primary.index) despawnFallbacks++;
        return true;
      }
      firstFailure = firstFailure || { region: region.region, reason: reply.reason || "unknown" };
    } catch (e) {
      firstFailure = firstFailure || { region: region.region, reason: e.message };
    }
  }
  recordFailedDespawn(player, firstFailure && firstFailure.reason, firstFailure && firstFailure.region);
  console.error(
    `[auth-server] despawn ${player.entity} failed after ${tried.size} workers; first=${(firstFailure && firstFailure.region) || "?"}: ${(firstFailure && firstFailure.reason) || "unknown"}`
  );
  return false;
}

function playerFrame(player) {
  const entity = entities.get(player.entity);
  const pos = entity ? entity.pos : player.spawnPos || [WIDTH / 2, HEIGHT / 2];
  const mass = entity ? entity.mass : 10;
  const cell = { x: pos[0], y: pos[1], mass, radius: massToRadius(mass) };
  return {
    x: pos[0],
    y: pos[1],
    cells: [cell],
    massTotal: Math.round(mass),
    hue: player.hue,
    id: player.id,
    name: player.name,
  };
}

function entityAsPlayer(entity) {
  const mass = entity.mass || 1;
  return {
    x: entity.pos[0],
    y: entity.pos[1],
    cells: [{ x: entity.pos[0], y: entity.pos[1], mass, radius: massToRadius(mass) }],
    massTotal: Math.round(mass),
    hue: entity.hue || 100,
    id: entity.id.replace(/:0$/, ""),
    name: entity.name || "",
  };
}

function entityAsFood(entity) {
  return {
    id: entity.id,
    x: entity.pos[0],
    y: entity.pos[1],
    mass: entity.mass || 1,
    radius: massToRadius(entity.mass || 1),
    hue: entity.hue || 100,
  };
}

function visibleFor(player) {
  const playerData = playerFrame(player);
  const users = [];
  const foods = [];
  for (const entity of entities.values()) {
    if (!entity.pos) continue;
    const dx = entity.pos[0] - playerData.x;
    const dy = entity.pos[1] - playerData.y;
    if (Math.hypot(dx, dy) > 1600) continue;
    if (entity.kind === "player") users.push(entityAsPlayer(entity));
    else if (entity.kind === "food") foods.push(entityAsFood(entity));
  }
  if (!users.some(user => user.id === player.id)) users.push(playerData);
  return { playerData, users, foods };
}

function leaderboard() {
  return Array.from(entities.values())
    .filter(entity => entity.kind === "player")
    .sort((a, b) => (b.mass || 0) - (a.mass || 0))
    .slice(0, 10)
    .map(entity => ({ id: entity.id.replace(/:0$/, ""), name: entity.name || "", mass: Math.round(entity.mass || 0) }));
}

io.on("connection", socket => {
  const type = socket.handshake.query.type;
  if (type !== "player" && type !== "spectator") return;

  const player = {
    id: socket.id,
    entity: `${socket.id}:0`,
    socket,
    name: "",
    hue: Math.round(Math.random() * 360),
    lastTarget: { x: WIDTH / 2, y: HEIGHT / 2 },
    spawned: false,
    commandInFlight: null,
    queuedTarget: null,
    lastWorldTarget: null,
  };
  players.set(socket.id, player);

  socket.on("respawn", () => {
    socket.emit("welcome", {
      id: socket.id,
      hue: player.hue,
      cells: [],
      massTotal: 10,
      x: WIDTH / 2,
      y: HEIGHT / 2,
    }, { width: WIDTH, height: HEIGHT });
  });

  socket.on("gotit", async data => {
    try {
      player.name = String((data && data.name) || "").replace(/(<([^>]+)>)/ig, "").slice(0, 24);
      await spawnPlayer(player);
      player.spawned = true;
      io.emit("playerJoin", { name: player.name });
      socket.emit("serverMSG", "Godworks authoritative mode: broker owns movement, food, mass, and handoff.");
    } catch (e) {
      socket.emit("kick", `Godworks spawn failed: ${e.message}`);
      socket.disconnect(true);
    }
  });

  socket.on("0", target => {
    player.lastTarget = target || player.lastTarget;
    if (!player.spawned || !entities.has(player.entity)) return;
    const worldTarget = worldTargetFor(player, player.lastTarget);
    sendTargetCommand(player, worldTarget, 0);
  });
  socket.on("1", () => socket.emit("serverMSG", "Split is not implemented in Godworks authoritative v0."));
  socket.on("2", () => socket.emit("serverMSG", "Mass eject is not implemented in Godworks authoritative v0."));
  socket.on("pingcheck", () => socket.emit("pongcheck"));
  socket.on("windowResized", () => {});
  socket.on("disconnect", () => {
    players.delete(socket.id);
    if (player.spawned) void despawnPlayer(player);
  });
});

setInterval(() => {
  for (const player of players.values()) {
    if (!player.spawned || !player.socket.connected) continue;
    const visible = visibleFor(player);
    player.socket.emit("serverTellPlayerMove", visible.playerData, visible.users, visible.foods, [], []);
    player.socket.emit("leaderboard", { players: players.size, leaderboard: leaderboard() });
  }
}, 1000 / UPDATE_HZ);

setInterval(reapCommandTimeouts, Math.max(50, Math.min(250, Math.floor(COMMAND_ACK_TIMEOUT_MS / 2))));

app.get("/state", (req, res) => {
  const includeEntities = req.query.entities === "1" || req.query.entities === "true";
  const commandHealth = commandHealthSnapshot();
  const body = {
    ok: true,
    godworksAuthoritative: true,
    ts: Date.now(),
    width: WIDTH,
    height: HEIGHT,
    grid: { cols: COLS, rows: ROWS },
    players: players.size,
    entities: entities.size,
    foods: Array.from(entities.values()).filter(entity => entity.kind === "food").length,
    playerEntities: Array.from(entities.values()).filter(entity => entity.kind === "player").length,
    commandResponses,
    commandRejects,
    commandTransientRejects,
    commandTimeouts,
    commandTimeoutRetries,
    ownerChangeResends,
    commandHealth,
    despawnFallbacks,
    despawnFailures,
    rejectedCommands,
    transientRejectedCommands,
    timedOutCommands,
    ownerChangeResendEvents,
    failedDespawns,
    owners: Array.from(entities.values()).reduce((acc, entity) => {
      const key = entity.owner || entity.region || "?";
      acc[key] = (acc[key] || 0) + 1;
      return acc;
    }, {}),
  };
  if (includeEntities) {
    body.entityRows = Array.from(entities.values())
      .slice(0, STATE_ENTITY_LIMIT)
      .map(entity => ({
        id: entity.id,
        x: Array.isArray(entity.pos) ? entity.pos[0] : null,
        y: Array.isArray(entity.pos) ? entity.pos[1] : null,
        mass: entity.mass || 1,
        kind: entity.kind || "cell",
        hue: entity.hue || 100,
        owner: entity.owner || null,
        region: entity.region || null,
      }));
    body.entityRowsTruncated = entities.size > body.entityRows.length;
  }
  res.json(body);
});

obsSock = connectObs();
clientSock = connectClient();
httpServer.listen(HTTP_PORT, "0.0.0.0", () => {
  console.error(`[auth-server] Godworks authoritative MIT client server on http://localhost:${HTTP_PORT}`);
  console.error(`[auth-server] serving client root ${clientRoot}`);
});
