"use strict";

const http = require("http");
const io = require("socket.io-client");

const GAME_URL = process.env.GW_AGAR_GAME_URL || "http://127.0.0.1:3000";
const STATE_URL = process.env.GW_AGAR_STATE_URL || `${GAME_URL.replace(/\/$/, "")}/state?entities=1`;
const NAME = process.env.GW_AGAR_AUTH_NAME || `gw_one_shot_${Date.now()}`;
const TIMEOUT_MS = parseInt(process.env.GW_AGAR_ONE_SHOT_TIMEOUT_MS || "30000", 10);
const TARGET = {
  x: parseFloat(process.env.GW_AGAR_ONE_SHOT_TARGET_X || "4500"),
  y: parseFloat(process.env.GW_AGAR_ONE_SHOT_TARGET_Y || "4500"),
};
const MIN_POST_HANDOFF_DISTANCE = parseFloat(process.env.GW_AGAR_ONE_SHOT_MIN_POST_HANDOFF_DISTANCE || "40");

function getJson(url) {
  return new Promise((resolve, reject) => {
    const req = http.get(url, res => {
      let body = "";
      res.setEncoding("utf8");
      res.on("data", chunk => { body += chunk; });
      res.on("end", () => {
        try { resolve(JSON.parse(body || "{}")); } catch (e) { reject(e); }
      });
    });
    req.setTimeout(5000, () => req.destroy(new Error(`timeout ${url}`)));
    req.on("error", reject);
  });
}

function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function numeric(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : 0;
}

function findRow(state, entityId) {
  return ((state && state.entityRows) || []).find(row => row.id === entityId) || null;
}

async function main() {
  const initialState = await getJson(STATE_URL).catch(() => null);
  const initialResponses = numeric((initialState || {}).commandResponses);
  const initialTimeouts = numeric((initialState || {}).commandTimeouts);
  const initialOwnerChangeResends = numeric((initialState || {}).ownerChangeResends);

  const socket = io(GAME_URL, { query: { type: "player" }, reconnection: false });
  let welcomed = false;
  let entityId = null;
  let sent = false;
  let frames = 0;
  let rip = false;
  let kicked = null;
  let start = null;
  let latestFrame = null;
  let latestState = null;
  let lastOwner = null;
  let ownerChanges = 0;
  let firstPostHandoffPos = null;
  let postHandoffDistance = 0;

  socket.on("connect", () => socket.emit("respawn"));
  socket.on("welcome", player => {
    welcomed = true;
    entityId = `${(player && player.id) || socket.id}:0`;
    socket.emit("gotit", {
      ...(player || {}),
      name: NAME,
      screenWidth: 900,
      screenHeight: 900,
    });
  });
  socket.on("serverTellPlayerMove", player => {
    frames++;
    latestFrame = player || null;
    if (!start && player && Number.isFinite(player.x) && Number.isFinite(player.y)) {
      start = { x: player.x, y: player.y };
    }
    if (!sent && player && Number.isFinite(player.x) && Number.isFinite(player.y)) {
      sent = true;
      socket.emit("0", TARGET);
    }
  });
  socket.on("RIP", () => { rip = true; });
  socket.on("kick", reason => { kicked = reason || "unknown"; });

  const deadline = Date.now() + TIMEOUT_MS;
  while (Date.now() < deadline) {
    await delay(250);
    try {
      latestState = await getJson(STATE_URL);
    } catch (_) {
      continue;
    }

    const row = entityId ? findRow(latestState, entityId) : null;
    const owner = row && (row.owner || row.region || null);
    if (owner) {
      if (lastOwner && owner !== lastOwner) {
        ownerChanges++;
        firstPostHandoffPos = { x: numeric(row.x), y: numeric(row.y) };
      }
      lastOwner = owner;
    }
    if (firstPostHandoffPos && row) {
      postHandoffDistance = Math.max(
        postHandoffDistance,
        Math.hypot(numeric(row.x) - firstPostHandoffPos.x, numeric(row.y) - firstPostHandoffPos.y)
      );
    }

    const responseDelta = numeric(latestState.commandResponses) - initialResponses;
    const timeoutDelta = numeric(latestState.commandTimeouts) - initialTimeouts;
    const ownerChangeResendDelta = numeric(latestState.ownerChangeResends) - initialOwnerChangeResends;
    const stuckPlayers = numeric(((latestState.commandHealth || {}).stuckPlayers));
    if (
      welcomed &&
      sent &&
      frames >= 5 &&
      row &&
      responseDelta >= 2 &&
      ownerChanges >= 1 &&
      ownerChangeResendDelta >= 1 &&
      postHandoffDistance >= MIN_POST_HANDOFF_DISTANCE &&
      timeoutDelta <= 0 &&
      stuckPlayers === 0 &&
      !rip &&
      !kicked
    ) {
      socket.close();
      console.log(JSON.stringify({
        ok: true,
        gate: "godworks_authoritative_one_shot_handoff",
        entityId,
        frames,
        target: TARGET,
        ownerChanges,
        lastOwner,
        responseDelta,
        ownerChangeResendDelta,
        timeoutDelta,
        postHandoffDistance,
        latestState,
      }, null, 2));
      return;
    }
  }

  socket.close();
  console.error(JSON.stringify({
    ok: false,
    gate: "godworks_authoritative_one_shot_handoff",
    entityId,
    welcomed,
    sent,
    frames,
    target: TARGET,
    ownerChanges,
    lastOwner,
    postHandoffDistance,
    rip,
    kicked,
    latestFrame,
    initialResponses,
    initialTimeouts,
    initialOwnerChangeResends,
    latestState,
  }, null, 2));
  process.exit(1);
}

main().catch(e => {
  console.error(e.stack || e.message);
  process.exit(1);
});
