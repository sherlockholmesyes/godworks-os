// Godworks spectator tap for the stock MIT agar.io clone.
// It does not patch the game. It connects as a spectator and normalizes the
// serverTellPlayerMove stream into stable entity records for shard monitors and
// Godworks bridge workers.
"use strict";

const io = require("socket.io-client");
const config = require("./config");

function connectSpectator(opts = {}) {
  const url = opts.url || process.env.AGAR_URL || "http://127.0.0.1:3000";
  const onFrame = opts.onFrame || (() => {});
  const onStatus = opts.onStatus || (() => {});
  const socket = io(url, {
    query: { type: "spectator" },
    reconnection: true,
    reconnectionDelay: 500,
    reconnectionDelayMax: 2000,
  });

  let lastFrame = {
    ts: Date.now(),
    width: config.gameWidth,
    height: config.gameHeight,
    players: [],
    food: [],
    mass: [],
    viruses: [],
    entities: [],
  };

  socket.on("connect", () => {
    onStatus(`[tap] connected ${url}`);
  });

  socket.on("welcome", (_player, sizes) => {
    if (sizes && sizes.width && sizes.height) {
      lastFrame.width = sizes.width;
      lastFrame.height = sizes.height;
    }
    socket.emit("gotit", { name: "gw-spectator" });
  });

  socket.on("serverTellPlayerMove", (_spectator, players, food, mass, viruses) => {
    lastFrame = normalizeFrame(players || [], food || [], mass || [], viruses || [], lastFrame.width, lastFrame.height);
    onFrame(lastFrame);
  });

  socket.on("disconnect", reason => onStatus(`[tap] disconnected ${reason}`));
  socket.on("connect_error", err => onStatus(`[tap] connect_error ${err.message}`));

  return {
    socket,
    getFrame: () => lastFrame,
    close: () => socket.close(),
  };
}

function normalizeFrame(players, food, mass, viruses, width = config.gameWidth, height = config.gameHeight) {
  const entities = [];

  for (const p of players) {
    const cells = Array.isArray(p.cells) && p.cells.length ? p.cells : [p];
    cells.forEach((cell, idx) => {
      const x = finite(cell.x, p.x);
      const y = finite(cell.y, p.y);
      if (x == null || y == null) return;
      entities.push({
        id: `${p.id || "player"}:${idx}`,
        owner_id: p.id || null,
        type: "player",
        name: p.name || "",
        x,
        y,
        mass: finite(cell.mass, p.massTotal, 10) || 10,
        radius: finite(cell.radius, 0) || 0,
      });
    });
  }

  food.forEach((f, idx) => {
    const x = finite(f.x);
    const y = finite(f.y);
    if (x == null || y == null) return;
    entities.push({
      id: f.id || `food:${idx}`,
      type: "food",
      x,
      y,
      mass: finite(f.mass, config.foodMass, 1) || 1,
      radius: finite(f.radius, 0) || 0,
    });
  });

  mass.forEach((m, idx) => {
    const x = finite(m.x);
    const y = finite(m.y);
    if (x == null || y == null) return;
    entities.push({
      id: m.id || `mass:${idx}`,
      type: "mass",
      x,
      y,
      mass: finite(m.mass, 1) || 1,
      radius: finite(m.radius, 0) || 0,
    });
  });

  viruses.forEach((v, idx) => {
    const x = finite(v.x);
    const y = finite(v.y);
    if (x == null || y == null) return;
    entities.push({
      id: v.id || `virus:${idx}`,
      type: "virus",
      x,
      y,
      mass: finite(v.mass, config.virus.defaultMass.from, 100) || 100,
      radius: finite(v.radius, 0) || 0,
    });
  });

  return {
    ts: Date.now(),
    width,
    height,
    players,
    food,
    mass,
    viruses,
    entities,
  };
}

function finite(...values) {
  for (const v of values) {
    const n = Number(v);
    if (Number.isFinite(n)) return n;
  }
  return null;
}

if (require.main === module) {
  connectSpectator({
    onStatus: msg => console.error(msg),
    onFrame: f => {
      const byType = {};
      for (const e of f.entities) byType[e.type] = (byType[e.type] || 0) + 1;
      process.stdout.write(JSON.stringify({ ts: f.ts, entities: f.entities.length, byType }) + "\n");
    },
  });
}

module.exports = { connectSpectator, normalizeFrame };
