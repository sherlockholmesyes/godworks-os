// Controlled-player bridge for the public-clean MIT agar.io clone adapter.
//
// This process owns one real stock-clone player socket. It does not patch the
// clone and it does not talk to the Godworks broker. Godworks mirror workers
// call this bridge only after the broker routes a CommandRequest to the current
// authority holder. That keeps the command path explicit:
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
const NAME = process.env.GW_AGAR_COMMAND_NAME || `gw_cmd_${Date.now()}`;
const SCREEN = parseInt(process.env.GW_AGAR_COMMAND_SCREEN || "900", 10);
const HEARTBEAT_MS = parseInt(process.env.GW_AGAR_COMMAND_HEARTBEAT_MS || "350", 10);

let socketId = null;
let welcomed = false;
let ripCount = 0;
let kicked = null;
let commandCount = 0;
let serverFrames = 0;
let lastTarget = { x: 2500, y: 2500 };
let lastCommand = null;

const socket = io(GAME_URL, {
  query: { type: "player" },
  reconnection: true,
  reconnectionDelay: 500,
  reconnectionDelayMax: 2000,
});

socket.on("connect", () => {
  socketId = socket.id;
  socket.emit("respawn");
});

socket.on("welcome", player => {
  socketId = socket.id || (player && player.id) || socketId;
  socket.emit("gotit", {
    ...(player || {}),
    name: NAME,
    screenWidth: SCREEN,
    screenHeight: SCREEN,
    target: lastTarget,
  });
  welcomed = true;
});

socket.on("serverTellPlayerMove", () => {
  serverFrames++;
});

socket.on("RIP", () => {
  ripCount++;
  setTimeout(() => socket.emit("respawn"), 250);
});

socket.on("kick", reason => {
  kicked = reason || "unknown";
});

setInterval(() => {
  if (!socket.connected || !welcomed || kicked) return;
  socket.emit("0", lastTarget);
}, HEARTBEAT_MS);

function state() {
  return {
    ok: Boolean(socket.connected && welcomed && socketId && !kicked),
    game: GAME_URL,
    name: NAME,
    socketId,
    entity: socketId ? `${socketId}:0` : null,
    connected: socket.connected,
    welcomed,
    kicked,
    ripCount,
    commandCount,
    serverFrames,
    lastTarget,
    lastCommand,
  };
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
  if (!socket.connected || !welcomed || !socketId) {
    sendJson(res, 503, { ok: false, reason: "controlled player is not ready", state: state() });
    return;
  }
  if (kicked) {
    sendJson(res, 409, { ok: false, reason: "controlled player was kicked", state: state() });
    return;
  }
  const expectedEntity = `${socketId}:0`;
  if (msg.entity && msg.entity !== expectedEntity && msg.owner_id !== socketId) {
    sendJson(res, 404, {
      ok: false,
      reason: "command entity does not match controlled player",
      expectedEntity,
      socketId,
      entity: msg.entity,
    });
    return;
  }
  const target = parseTarget(msg.payload || {});
  lastTarget = target;
  lastCommand = {
    at: Date.now(),
    owner: msg.owner || null,
    request_id: msg.request_id || null,
    command: msg.command || null,
    entity: msg.entity || expectedEntity,
    target,
  };
  socket.emit("0", target);
  commandCount++;
  sendJson(res, 200, {
    ok: true,
    entity: expectedEntity,
    socketId,
    commandCount,
    target,
  });
}

const server = http.createServer((req, res) => {
  Promise.resolve()
    .then(async () => {
      if (req.method === "GET" && req.url === "/state") {
        sendJson(res, 200, state());
      } else if (req.method === "POST" && req.url === "/input") {
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
  console.error(`[agar-command-bridge] listening http://${HOST}:${PORT} game=${GAME_URL} name=${NAME}`);
});
