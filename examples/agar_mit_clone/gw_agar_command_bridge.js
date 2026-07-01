// Controlled-player bridge for the public-clean MIT agar.io clone adapter.
//
// This process owns one or more real stock-clone player sockets. It does not
// patch the clone and it does not talk to the Godworks broker. Godworks mirror
// workers call this bridge only after the broker routes a CommandRequest to the
// current authority holder. That keeps the command path explicit:
// broker CommandRequest -> current owner worker -> this bridge -> MIT socket.
//
// The MIT clone uses the same "0" socket event for target updates and heartbeat.
// To stay connected, the bridge periodically repeats the last broker-authorized
// target; it does not choose new movement targets on its own.
"use strict";

const http = require("http");
const io = require("socket.io-client");

const GAME_URL = process.env.GW_AGAR_GAME_URL || process.env.AGAR_URL || "http://127.0.0.1:3000";
const HOST = process.env.GW_AGAR_COMMAND_HOST || "127.0.0.1";
const PORT = parseInt(process.env.GW_AGAR_COMMAND_PORT || "8093", 10);
const BASE_NAME = process.env.GW_AGAR_COMMAND_NAME || `gw_cmd_${Date.now()}`;
const PLAYER_COUNT = parseInt(process.env.GW_AGAR_COMMAND_PLAYERS || "1", 10);
const SCREEN = parseInt(process.env.GW_AGAR_COMMAND_SCREEN || "900", 10);
const HEARTBEAT_MS = parseInt(process.env.GW_AGAR_COMMAND_HEARTBEAT_MS || "350", 10);

const players = [];

for (let i = 0; i < Math.max(1, PLAYER_COUNT); i++) {
  players.push(spawnControlledPlayer(i));
}

setInterval(() => {
  for (const player of players) {
    if (!player.socket.connected || !player.welcomed || player.kicked) continue;
    player.socket.emit("0", player.lastTarget);
  }
}, HEARTBEAT_MS);

function spawnControlledPlayer(index) {
  const name = PLAYER_COUNT === 1 ? BASE_NAME : `${BASE_NAME}_${String(index).padStart(2, "0")}`;
  const player = {
    index,
    name,
    socketId: null,
    welcomed: false,
    ripCount: 0,
    kicked: null,
    commandCount: 0,
    serverFrames: 0,
    lastTarget: { x: 2500, y: 2500 },
    lastCommand: null,
    socket: null,
  };

  const socket = io(GAME_URL, {
    query: { type: "player" },
    reconnection: true,
    reconnectionDelay: 500,
    reconnectionDelayMax: 2000,
  });
  player.socket = socket;

  socket.on("connect", () => {
    player.socketId = socket.id;
    player.welcomed = false;
    socket.emit("respawn");
  });

  socket.on("welcome", joined => {
    player.socketId = socket.id || (joined && joined.id) || player.socketId;
    socket.emit("gotit", {
      ...(joined || {}),
      name,
      screenWidth: SCREEN,
      screenHeight: SCREEN,
      target: player.lastTarget,
    });
    player.welcomed = true;
  });

  socket.on("serverTellPlayerMove", () => {
    player.serverFrames++;
  });

  socket.on("RIP", () => {
    player.ripCount++;
    player.welcomed = false;
    setTimeout(() => socket.emit("respawn"), 250);
  });

  socket.on("kick", reason => {
    player.kicked = reason || "unknown";
    player.welcomed = false;
  });

  socket.on("disconnect", () => {
    player.welcomed = false;
  });

  return player;
}

function entityFor(player) {
  return player.socketId ? `${player.socketId}:0` : null;
}

function playerState(player) {
  const entity = entityFor(player);
  return {
    ok: Boolean(player.socket.connected && player.welcomed && player.socketId && !player.kicked),
    game: GAME_URL,
    index: player.index,
    name: player.name,
    socketId: player.socketId,
    entity,
    connected: player.socket.connected,
    welcomed: player.welcomed,
    kicked: player.kicked,
    ripCount: player.ripCount,
    commandCount: player.commandCount,
    serverFrames: player.serverFrames,
    lastTarget: player.lastTarget,
    lastCommand: player.lastCommand,
  };
}

function aggregateState() {
  const all = players.map(playerState);
  const ready = all.filter(player => player.ok);
  const first = ready[0] || all[0] || {};
  return {
    ...first,
    ok: ready.length > 0,
    allReady: ready.length === players.length,
    playerCount: players.length,
    readyPlayers: ready.length,
    players: all,
  };
}

function findPlayerForMessage(msg) {
  const entity = msg && msg.entity;
  const ownerId = msg && msg.owner_id;
  if (entity) {
    const byEntity = players.find(player => entityFor(player) === entity);
    if (byEntity) return byEntity;
  }
  if (ownerId) {
    const byOwner = players.find(player => player.socketId === ownerId);
    if (byOwner) return byOwner;
  }
  if (!entity && !ownerId && players.length === 1) {
    return players[0];
  }
  return null;
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    let body = "";
    req.setEncoding("utf8");
    req.on("data", chunk => {
      body += chunk;
      if (body.length > 64 * 1024) {
        req.destroy(new Error("request body too large"));
      }
    });
    req.on("end", () => resolve(body));
    req.on("error", reject);
  });
}

function parseTarget(payload) {
  const raw = payload && typeof payload === "object" && Object.prototype.hasOwnProperty.call(payload, "target")
    ? payload.target
    : payload;
  const value = Array.isArray(raw) ? { x: raw[0], y: raw[1] } : raw;
  const x = Number(value && value.x);
  const y = Number(value && value.y);
  if (!Number.isFinite(x) || !Number.isFinite(y)) {
    throw new Error("payload.target must contain finite x/y");
  }
  return { x, y };
}

function sendJson(res, code, body) {
  const text = JSON.stringify(body, null, 2);
  res.writeHead(code, {
    "content-type": "application/json; charset=utf-8",
    "cache-control": "no-store",
  });
  res.end(text);
}

async function handleInput(req, res) {
  const raw = await readBody(req);
  const msg = raw ? JSON.parse(raw) : {};
  const player = findPlayerForMessage(msg);
  if (!player) {
    sendJson(res, 404, {
      ok: false,
      reason: "command entity does not match any controlled player",
      entity: msg.entity || null,
      owner_id: msg.owner_id || null,
      knownEntities: players.map(entityFor).filter(Boolean),
    });
    return;
  }
  if (!player.socket.connected || !player.welcomed || !player.socketId) {
    sendJson(res, 503, {
      ok: false,
      reason: "controlled player is not ready",
      state: playerState(player),
    });
    return;
  }
  if (player.kicked) {
    sendJson(res, 409, {
      ok: false,
      reason: "controlled player was kicked",
      state: playerState(player),
    });
    return;
  }
  const expectedEntity = entityFor(player);
  if (msg.entity && msg.entity !== expectedEntity) {
    sendJson(res, 404, {
      ok: false,
      reason: "command entity does not match controlled player",
      expectedEntity,
      socketId: player.socketId,
      entity: msg.entity,
    });
    return;
  }
  const target = parseTarget(msg.payload || {});
  player.lastTarget = target;
  player.lastCommand = {
    at: Date.now(),
    owner: msg.owner || null,
    request_id: msg.request_id || null,
    command: msg.command || null,
    entity: msg.entity || expectedEntity,
    target,
  };
  player.socket.emit("0", target);
  player.commandCount++;
  sendJson(res, 200, {
    ok: true,
    entity: expectedEntity,
    socketId: player.socketId,
    name: player.name,
    commandCount: player.commandCount,
    target,
  });
}

function queryPlayer(url) {
  const entity = url.searchParams.get("entity");
  const socketId = url.searchParams.get("socketId");
  const index = url.searchParams.get("index");
  if (entity) {
    return players.find(player => entityFor(player) === entity) || null;
  }
  if (socketId) {
    return players.find(player => player.socketId === socketId) || null;
  }
  if (index !== null) {
    return players.find(player => String(player.index) === String(index)) || null;
  }
  return null;
}

const server = http.createServer((req, res) => {
  Promise.resolve()
    .then(async () => {
      const url = new URL(req.url, `http://${HOST}:${PORT}`);
      if (req.method === "GET" && url.pathname === "/state") {
        const selected = queryPlayer(url);
        sendJson(res, 200, selected ? playerState(selected) : aggregateState());
      } else if (req.method === "GET" && url.pathname === "/players") {
        sendJson(res, 200, aggregateState());
      } else if (req.method === "POST" && url.pathname === "/input") {
        await handleInput(req, res);
      } else {
        sendJson(res, 404, { ok: false, reason: "not found" });
      }
    })
    .catch(err => {
      sendJson(res, 500, { ok: false, reason: err.message });
    });
});

server.listen(PORT, HOST, () => {
  console.error(`[agar-command-bridge] listening http://${HOST}:${PORT} game=${GAME_URL} players=${players.length} base=${BASE_NAME}`);
});
