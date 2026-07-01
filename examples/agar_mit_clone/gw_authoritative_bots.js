"use strict";

const http = require("http");
const io = require("socket.io-client");

const URL = process.env.GW_AGAR_GAME_URL || "http://127.0.0.1:3000";
const COUNT = parseInt(process.env.GW_AUTH_BOTS || "40", 10);
const SCREEN = parseInt(process.env.GW_AUTH_BOT_SCREEN || "900", 10);
const INTERVAL_MS = parseInt(process.env.GW_AUTH_BOT_INTERVAL_MS || "80", 10);
const CONTROL_PORT = parseInt(process.env.GW_AUTH_BOT_PORT || "8094", 10);
const HERD = process.env.GW_AUTH_BOT_HERD === "1";

const bots = [];
let startedAt = Date.now();

for (let i = 0; i < COUNT; i++) {
  bots.push(spawnBot(i));
}

setInterval(() => {
  const state = summarize();
  console.error(`[auth-bots] connected=${state.connected}/${COUNT} alive=${state.alive}/${COUNT} frames=${state.frames} commands=${state.commandsSent}`);
}, 5000);

function spawnBot(index) {
  const bot = {
    index,
    connected: false,
    alive: false,
    frames: 0,
    commandsSent: 0,
    rip: 0,
    kicked: 0,
    disconnects: 0,
    target: randomVector(),
    nextTargetAt: 0,
    socket: null,
  };

  const socket = io(URL, {
    query: { type: "player" },
    reconnection: true,
    reconnectionDelay: 500,
    reconnectionDelayMax: 2000,
  });
  bot.socket = socket;

  socket.on("connect", () => {
    bot.connected = true;
    bot.alive = false;
    socket.emit("respawn");
  });

  socket.on("welcome", player => {
    socket.emit("gotit", {
      ...(player || {}),
      name: `gwauth_${String(index).padStart(4, "0")}`,
      screenWidth: SCREEN,
      screenHeight: SCREEN,
      target: bot.target,
    });
    bot.alive = true;
  });

  socket.on("serverTellPlayerMove", () => {
    bot.frames++;
  });

  socket.on("RIP", () => {
    bot.rip++;
    bot.alive = false;
    setTimeout(() => {
      if (socket.connected) socket.emit("respawn");
    }, 500 + Math.random() * 1500);
  });

  socket.on("kick", reason => {
    bot.kicked++;
    bot.alive = false;
    console.error(`[auth-bots] kicked ${index}: ${reason || ""}`);
  });

  socket.on("disconnect", () => {
    bot.disconnects++;
    bot.connected = false;
    bot.alive = false;
  });

  setInterval(() => {
    if (!socket.connected || !bot.alive) return;
    const now = Date.now();
    if (HERD) {
      bot.target = {
        x: Math.sin(now / 900 + index) * SCREEN * 0.45,
        y: Math.cos(now / 1100 + index) * SCREEN * 0.45,
      };
    } else if (now >= bot.nextTargetAt) {
      bot.target = randomVector();
      bot.nextTargetAt = now + 800 + Math.random() * 2500;
    }
    socket.emit("0", bot.target);
    bot.commandsSent++;
  }, INTERVAL_MS);

  return bot;
}

function randomVector() {
  const angle = Math.random() * Math.PI * 2;
  const len = SCREEN * (0.25 + Math.random() * 0.45);
  return {
    x: Math.cos(angle) * len,
    y: Math.sin(angle) * len,
  };
}

function summarize() {
  return {
    ok: true,
    gameUrl: URL,
    configured: COUNT,
    uptimeMs: Date.now() - startedAt,
    connected: bots.filter(bot => bot.connected).length,
    alive: bots.filter(bot => bot.alive).length,
    frames: bots.reduce((sum, bot) => sum + bot.frames, 0),
    commandsSent: bots.reduce((sum, bot) => sum + bot.commandsSent, 0),
    rip: bots.reduce((sum, bot) => sum + bot.rip, 0),
    kicked: bots.reduce((sum, bot) => sum + bot.kicked, 0),
    disconnects: bots.reduce((sum, bot) => sum + bot.disconnects, 0),
  };
}

function sendJson(res, code, body) {
  res.writeHead(code, { "content-type": "application/json", "cache-control": "no-store" });
  res.end(JSON.stringify(body));
}

http.createServer((req, res) => {
  if (req.method === "GET" && req.url === "/state") {
    return sendJson(res, 200, summarize());
  }
  sendJson(res, 404, { ok: false, reason: "not found" });
}).listen(CONTROL_PORT, "127.0.0.1", () => {
  console.error(`[auth-bots] state http://127.0.0.1:${CONTROL_PORT}/state count=${COUNT}`);
});
