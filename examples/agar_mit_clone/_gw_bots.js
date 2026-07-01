// Bot load for the stock MIT agar.io clone. External only: connects as normal
// players and sends legal heartbeat targets. Used to make the shard monitor and
// broker bridge prove behavior under live moving gameplay.
"use strict";

const io = require("socket.io-client");

const URL = process.env.AGAR_URL || "http://127.0.0.1:3000";
const COUNT = parseInt(process.env.GW_BOTS || "40", 10);
const SCREEN = parseInt(process.env.GW_BOT_SCREEN || "900", 10);
const HERD = process.env.GW_BOT_HERD === "1";
const INTERVAL_MS = parseInt(process.env.GW_BOT_INTERVAL_MS || "80", 10);

const bots = [];

for (let i = 0; i < COUNT; i++) {
  bots.push(spawnBot(i));
}

setInterval(() => {
  const alive = bots.filter(b => b.alive).length;
  console.error(`[bots] alive=${alive}/${COUNT} herd=${HERD ? 1 : 0}`);
}, 5000);

function spawnBot(i) {
  const state = {
    id: i,
    alive: false,
    socket: null,
    target: randomTarget(),
    nextTargetAt: 0,
  };

  const socket = io(URL, {
    query: { type: "player" },
    reconnection: true,
    reconnectionDelay: 500,
    reconnectionDelayMax: 2000,
  });
  state.socket = socket;

  socket.on("connect", () => {
    state.alive = false;
    socket.emit("respawn");
  });

  socket.on("welcome", player => {
    const name = `gwbot_${String(i).padStart(2, "0")}`;
    socket.emit("gotit", {
      ...(player || {}),
      name,
      screenWidth: SCREEN,
      screenHeight: SCREEN,
      target: state.target,
    });
    state.alive = true;
  });

  socket.on("RIP", () => {
    state.alive = false;
    setTimeout(() => socket.emit("respawn"), 500 + Math.random() * 1500);
  });

  socket.on("kick", reason => {
    state.alive = false;
    console.error(`[bots] kicked ${i}: ${reason || ""}`);
  });

  socket.on("disconnect", () => {
    state.alive = false;
  });

  setInterval(() => {
    if (!socket.connected) return;
    const now = Date.now();
    if (HERD) {
      state.target = {
        x: SCREEN / 2 + Math.sin(now / 900 + i) * 230,
        y: SCREEN / 2 + Math.cos(now / 1100 + i) * 230,
      };
    } else if (now >= state.nextTargetAt) {
      state.target = randomTarget();
      state.nextTargetAt = now + 1200 + Math.random() * 3500;
    }
    socket.emit("0", state.target);
  }, INTERVAL_MS);

  return state;
}

function randomTarget() {
  const margin = SCREEN * 0.15;
  return {
    x: margin + Math.random() * (SCREEN - margin * 2),
    y: margin + Math.random() * (SCREEN - margin * 2),
  };
}
