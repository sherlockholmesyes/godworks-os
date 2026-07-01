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
      entity.region = row.region || entity.region;
      entity.owner = (((row.authority || {}).pos || {}).owner) || entity.owner || entity.region;
    }
  }
}

function handleClientFrame(msg) {
  if (msg.op !== "CommandResponse") return;
  commandResponses++;
  if (!msg.success) commandRejects++;
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
  const region = regionForOwner(entity && entity.owner)
    || regionFor((entity && entity.pos) || player.spawnPos || [WIDTH / 2, HEIGHT / 2]);
  const url = `http://127.0.0.1:${CONTROL_BASE + region.index}/despawn`;
  try {
    const reply = await postJson(url, { entity: player.entity });
    if (!reply.ok) console.error(`[auth-server] despawn ${player.entity} via ${region.region} failed: ${reply.reason || "unknown"}`);
  } catch (e) {
    console.error(`[auth-server] despawn ${player.entity} via ${region.region} error: ${e.message}`);
  }
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
    send(clientSock, {
      op: "CommandRequest",
      request_id: `auth-cmd-${requestSeq++}`,
      entity: player.entity,
      command: "set_target",
      payload: target,
    });
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

app.get("/state", (_req, res) => {
  res.json({
    ok: true,
    godworksAuthoritative: true,
    players: players.size,
    entities: entities.size,
    foods: Array.from(entities.values()).filter(entity => entity.kind === "food").length,
    playerEntities: Array.from(entities.values()).filter(entity => entity.kind === "player").length,
    commandResponses,
    commandRejects,
    owners: Array.from(entities.values()).reduce((acc, entity) => {
      const key = entity.owner || entity.region || "?";
      acc[key] = (acc[key] || 0) + 1;
      return acc;
    }, {}),
  });
});

obsSock = connectObs();
clientSock = connectClient();
httpServer.listen(HTTP_PORT, "0.0.0.0", () => {
  console.error(`[auth-server] Godworks authoritative MIT client server on http://localhost:${HTTP_PORT}`);
  console.error(`[auth-server] serving client root ${clientRoot}`);
});
