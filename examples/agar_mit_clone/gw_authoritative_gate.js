"use strict";

const http = require("http");
const io = require("socket.io-client");

const GAME_URL = process.env.GW_AGAR_GAME_URL || "http://127.0.0.1:3000";
const STATE_URL = process.env.GW_AGAR_STATE_URL || `${GAME_URL.replace(/\/$/, "")}/state`;
const MONITOR_URL = process.env.GW_AGAR_MONITOR_URL || "";
const NAME = process.env.GW_AGAR_AUTH_NAME || `gw_auth_${Date.now()}`;
const TIMEOUT_MS = parseInt(process.env.GW_AGAR_AUTH_TIMEOUT_MS || "12000", 10);

function getJson(url) {
  return new Promise((resolve, reject) => {
    http.get(url, res => {
      let body = "";
      res.setEncoding("utf8");
      res.on("data", chunk => { body += chunk; });
      res.on("end", () => {
        try { resolve(JSON.parse(body)); } catch (e) { reject(e); }
      });
    }).on("error", reject);
  });
}

async function main() {
  const initialState = await getJson(STATE_URL).catch(() => null);
  const initialCommandResponses = Number((initialState || {}).commandResponses || 0);
  const socket = io(GAME_URL, { query: { type: "player" }, reconnection: false });
  let welcomed = false;
  let frames = 0;
  let start = null;
  let latest = null;
  let maxDistance = 0;
  let rip = false;
  let kicked = null;
  let monitorState = null;

  socket.on("connect", () => socket.emit("respawn"));
  socket.on("welcome", player => {
    welcomed = true;
    socket.emit("gotit", {
      ...(player || {}),
      name: NAME,
      screenWidth: 900,
      screenHeight: 900,
      target: { x: 4500, y: 4500 },
    });
  });
  socket.on("serverTellPlayerMove", player => {
    frames++;
    latest = player;
    if (!start && player && Number.isFinite(player.x) && Number.isFinite(player.y)) {
      start = { x: player.x, y: player.y };
    }
    if (start && player) {
      maxDistance = Math.max(maxDistance, Math.hypot(player.x - start.x, player.y - start.y));
    }
    socket.emit("0", { x: 4500, y: 4500 });
  });
  socket.on("RIP", () => { rip = true; });
  socket.on("kick", reason => { kicked = reason || "unknown"; });

  const deadline = Date.now() + TIMEOUT_MS;
  let state = null;
  while (Date.now() < deadline) {
    await new Promise(resolve => setTimeout(resolve, 250));
    if (latest) socket.emit("0", { x: 4500, y: 4500 });
    try { state = await getJson(STATE_URL); } catch (_) {}
    if (MONITOR_URL) {
      try { monitorState = await getJson(MONITOR_URL); } catch (_) {}
    }
    const monitorOk = !MONITOR_URL || (
      monitorState &&
      monitorState.godworksAuthoritative === true &&
      monitorState.upstream &&
      monitorState.upstream.entities > 0 &&
      Array.isArray(monitorState.entities) &&
      monitorState.entities.length > 0 &&
      Array.isArray(monitorState.bands) &&
      monitorState.bands.length === 4 &&
      Array.isArray(monitorState.dynamicLoads) &&
      monitorState.dynamicLoads.length >= 16
    );
    if (
      welcomed &&
      frames >= 10 &&
      maxDistance > 80 &&
      state &&
      state.godworksAuthoritative &&
      state.foods > 0 &&
      state.playerEntities > 0 &&
      Number(state.commandResponses || 0) > initialCommandResponses &&
      monitorOk &&
      !rip &&
      !kicked
    ) {
      socket.close();
      console.log(JSON.stringify({
        ok: true,
        gate: "godworks_authoritative_agar_v0",
        frames,
        maxDistance,
        commandResponseDelta: Number(state.commandResponses || 0) - initialCommandResponses,
        state,
        monitorState,
      }, null, 2));
      return;
    }
  }
  socket.close();
  console.error(JSON.stringify({
    ok: false,
    gate: "godworks_authoritative_agar_v0",
    welcomed,
    frames,
    maxDistance,
    rip,
    kicked,
    latest,
    initialCommandResponses,
    state,
    monitorState,
  }, null, 2));
  process.exit(1);
}

main().catch(e => {
  console.error(e.stack || e.message);
  process.exit(1);
});
