// Playable seam gate for the public-clean MIT agar.io clone adapter.
//
// This does not make the clone Godworks-authoritative. It proves the live,
// playable game command path is not just a passive monitor: a real player joins
// the stock clone, sends normal movement commands, appears in the :8091 shard
// monitor, crosses at least one dynamic worker-zone boundary, and keeps moving
// after the crossing. When a broker mirror view is supplied, the same player is
// also checked against the Godworks mirror.
"use strict";

const http = require("http");
const io = require("socket.io-client");

const GAME_URL = process.env.GW_AGAR_GAME_URL || process.env.AGAR_URL || "http://127.0.0.1:3000";
const MONITOR_URL = process.env.GW_AGAR_MONITOR_URL || "http://127.0.0.1:8091/state";
const BROKER_VIEW_URL = process.env.GW_AGAR_BROKER_VIEW_URL || "";
const NAME = process.env.GW_AGAR_PROBE_NAME || `gw_seam_${Date.now()}`;
const SCREEN = parseInt(process.env.GW_AGAR_PROBE_SCREEN || "900", 10);
const TIMEOUT_MS = parseInt(process.env.GW_AGAR_PLAYABLE_TIMEOUT_MS || "45000", 10);
const POLL_MS = parseInt(process.env.GW_AGAR_PLAYABLE_POLL_MS || "160", 10);
const COMMAND_MS = parseInt(process.env.GW_AGAR_PLAYABLE_COMMAND_MS || "70", 10);
const MIN_PATH = parseFloat(process.env.GW_AGAR_PLAYABLE_MIN_PATH || "120");
const MIN_POST_SEAM_PATH = parseFloat(process.env.GW_AGAR_PLAYABLE_MIN_POST_SEAM_PATH || "20");
const MATCH_RADIUS = parseFloat(process.env.GW_AGAR_BROKER_MATCH_RADIUS || "90");

const DIRECTIONS = [
  { name: "east", target: { x: 1e6, y: 0 } },
  { name: "west", target: { x: -1e6, y: 0 } },
  { name: "south", target: { x: 0, y: 1e6 } },
  { name: "north", target: { x: 0, y: -1e6 } },
];

function get(url) {
  return new Promise((resolve, reject) => {
    const req = http.get(url, res => {
      let body = "";
      res.setEncoding("utf8");
      res.on("data", chunk => { body += chunk; });
      res.on("end", () => resolve({ status: res.statusCode, body }));
    });
    req.setTimeout(5000, () => req.destroy(new Error(`timeout ${url}`)));
    req.on("error", reject);
  });
}

function parseJson(label, body) {
  try {
    return JSON.parse(body);
  } catch (err) {
    throw new Error(`${label} did not return JSON: ${err.message}`);
  }
}

function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function distance(a, b) {
  if (!a || !b) return 0;
  return Math.hypot(Number(a.x) - Number(b.x), Number(a.y) - Number(b.y));
}

function assertOk(condition, message, facts) {
  if (!condition) {
    const err = new Error(message);
    err.facts = facts;
    throw err;
  }
}

function findProbeEntity(monitor, socketId) {
  const entities = Array.isArray(monitor.entities) ? monitor.entities : [];
  return entities.find(e => e && e.type === "player" && e.owner_id === socketId)
    || entities.find(e => e && e.type === "player" && e.name === NAME)
    || null;
}

function chooseDirection(entity, monitor, attempt) {
  if (!entity || !monitor) return DIRECTIONS[attempt % DIRECTIONS.length];
  if (attempt === 0) {
    return Number(entity.x) < Number(monitor.width) / 2 ? DIRECTIONS[0] : DIRECTIONS[1];
  }
  if (attempt === 1) {
    return Number(entity.y) < Number(monitor.height) / 2 ? DIRECTIONS[2] : DIRECTIONS[3];
  }
  return DIRECTIONS[attempt % DIRECTIONS.length];
}

function nearBrokerEntity(brokerEntities, probe) {
  if (!Array.isArray(brokerEntities) || !probe) return null;
  const exactId = probe.id || `${probe.owner_id}:0`;
  const exact = brokerEntities.find(e => e && (e.id === exactId || e.entity === exactId));
  if (exact) return exact;
  return brokerEntities.find(e => {
    if (!e || !Array.isArray(e.p) || e.p.length < 2) return false;
    const dx = Number(e.p[0]) - Number(probe.x);
    const dy = Number(e.p[1]) - Number(probe.y);
    return Math.hypot(dx, dy) <= MATCH_RADIUS;
  }) || null;
}

function summarizeMonitor(monitor) {
  if (!monitor || typeof monitor !== "object") return null;
  const entities = Array.isArray(monitor.entities) ? monitor.entities : [];
  const loads = Array.isArray(monitor.loads)
    ? monitor.loads.map(v => Number(v)).filter(Number.isFinite)
    : [];
  const dynamicWidthClasses = new Set();
  const dynamicHeightClasses = new Set();
  for (const zone of Array.isArray(monitor.zones) ? monitor.zones : []) {
    if (!zone) continue;
    const width = Number(zone.x1) - Number(zone.x0);
    const height = Number(zone.y1) - Number(zone.y0);
    if (Number.isFinite(width)) dynamicWidthClasses.add(Math.round(width));
    if (Number.isFinite(height)) dynamicHeightClasses.add(Math.round(height));
  }
  for (const band of Array.isArray(monitor.bands) ? monitor.bands : []) {
    if (!band) continue;
    const width = Number(band.c1) - Number(band.c0);
    if (Number.isFinite(width)) dynamicWidthClasses.add(Math.round(width));
    for (const row of Array.isArray(band.rows) ? band.rows : []) {
      const height = Number(row && row.r1) - Number(row && row.r0);
      if (Number.isFinite(height)) dynamicHeightClasses.add(Math.round(height));
    }
  }
  return {
    entities: entities.length,
    players: entities.filter(e => e && e.type === "player").length,
    workers: loads.length,
    loads,
    rebalanceCount: Number.isFinite(Number(monitor.rebalanceCount)) ? Number(monitor.rebalanceCount) : 0,
    dynamicWidthClasses: dynamicWidthClasses.size,
    dynamicHeightClasses: dynamicHeightClasses.size,
  };
}

async function fetchMonitor() {
  const res = await get(MONITOR_URL);
  assertOk(res.status === 200, "monitor did not return HTTP 200", { status: res.status, url: MONITOR_URL });
  return parseJson("monitor", res.body);
}

async function fetchBrokerView() {
  if (!BROKER_VIEW_URL) return null;
  const res = await get(BROKER_VIEW_URL);
  assertOk(res.status === 200, "broker view did not return HTTP 200", { status: res.status, url: BROKER_VIEW_URL });
  return parseJson("broker view", res.body);
}

async function main() {
  const startedAt = Date.now();
  const socket = io(GAME_URL, {
    query: { type: "player" },
    reconnection: true,
    reconnectionDelay: 500,
    reconnectionDelayMax: 2000,
  });

  let socketId = null;
  let welcomed = false;
  let rip = false;
  let kicked = null;
  let serverFrames = 0;
  let commandCount = 0;
  let currentDirection = DIRECTIONS[0];
  let directionAttempt = 0;
  let firstProbe = null;
  let lastProbe = null;
  let lastProgressProbe = null;
  let firstBlock = null;
  let currentBlock = null;
  let blockChanges = 0;
  let firstSeamAt = null;
  let postSeamStart = null;
  let postSeamPath = 0;
  let path = 0;
  let missingStreak = 0;
  let maxMissingStreak = 0;
  let brokerMatch = null;
  let lastMonitor = null;
  const blocks = new Set();

  const cleanup = () => {
    try {
      socket.close();
    } catch (_) {}
  };

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
      target: currentDirection.target,
    });
    welcomed = true;
  });

  socket.on("serverTellPlayerMove", () => {
    serverFrames++;
  });

  socket.on("RIP", () => {
    rip = true;
  });

  socket.on("kick", reason => {
    kicked = reason || "unknown";
  });

  const commandTimer = setInterval(() => {
    if (!socket.connected || !welcomed || rip || kicked) return;
    socket.emit("0", currentDirection.target);
    commandCount++;
  }, COMMAND_MS);

  try {
    while (Date.now() - startedAt < TIMEOUT_MS) {
      assertOk(!rip, "probe player died before playable seam proof completed", { name: NAME, socketId });
      assertOk(!kicked, "probe player was kicked before playable seam proof completed", { name: NAME, socketId, kicked });

      const monitor = await fetchMonitor();
      lastMonitor = monitor;
      const probe = findProbeEntity(monitor, socketId);

      if (!probe) {
        missingStreak++;
        maxMissingStreak = Math.max(maxMissingStreak, missingStreak);
        await delay(POLL_MS);
        continue;
      }

      missingStreak = 0;
      if (!firstProbe) {
        firstProbe = { ...probe };
        firstBlock = probe.block || "?";
        assertOk(firstBlock !== "?", "probe player has unknown shard block", { probe, monitorUrl: MONITOR_URL });
        currentBlock = firstBlock;
        blocks.add(firstBlock);
        currentDirection = chooseDirection(probe, monitor, directionAttempt);
        lastProgressProbe = { ...probe };
      } else {
        const step = distance(lastProbe, probe);
        path += step;
        if (firstSeamAt) postSeamPath += step;
      }

      lastProbe = { ...probe };
      const block = probe.block || "?";
      blocks.add(block);

      if (currentBlock && block !== currentBlock) {
        blockChanges++;
        currentBlock = block;
        if (!firstSeamAt) {
          firstSeamAt = Date.now();
          postSeamStart = { ...probe };
          postSeamPath = 0;
        }
      }

      if (distance(lastProgressProbe, probe) < 6 && Date.now() - startedAt > 4000) {
        directionAttempt++;
        currentDirection = chooseDirection(probe, monitor, directionAttempt);
        lastProgressProbe = { ...probe };
      } else if (distance(lastProgressProbe, probe) >= 80) {
        lastProgressProbe = { ...probe };
      }

      if (firstSeamAt && postSeamPath >= MIN_POST_SEAM_PATH && path >= MIN_PATH) {
        const brokerEntities = await fetchBrokerView();
        if (brokerEntities) {
          brokerMatch = nearBrokerEntity(brokerEntities, probe);
          assertOk(!!brokerMatch, "broker mirror did not contain the probe player after playable seam", {
            probe: { id: probe.id, owner_id: probe.owner_id, x: probe.x, y: probe.y, block: probe.block },
            matchRadius: MATCH_RADIUS,
            brokerEntities: Array.isArray(brokerEntities) ? brokerEntities.length : null,
          });
        }
        break;
      }

      await delay(POLL_MS);
    }

    assertOk(welcomed, "probe player never completed stock clone welcome/gotit handshake", { socketId, name: NAME });
    assertOk(serverFrames > 0, "probe player never received serverTellPlayerMove frames", { socketId, name: NAME });
    assertOk(commandCount > 0, "probe player sent no movement commands", { socketId, name: NAME });
    assertOk(!!firstProbe, "probe player never appeared in shard monitor", { socketId, name: NAME });
    assertOk(path >= MIN_PATH, "probe player moved too little under live commands", { path, minPath: MIN_PATH, firstProbe, lastProbe });
    assertOk(blockChanges > 0, "probe player did not cross a dynamic shard block", {
      firstBlock,
      currentBlock,
      blocks: [...blocks],
      path,
      lastMonitor: lastMonitor ? {
        entities: Array.isArray(lastMonitor.entities) ? lastMonitor.entities.length : null,
        rebalanceCount: lastMonitor.rebalanceCount,
        loads: lastMonitor.loads,
      } : null,
    });
    assertOk(postSeamPath >= MIN_POST_SEAM_PATH, "probe player did not keep moving after shard-block crossing", {
      postSeamPath,
      minPostSeamPath: MIN_POST_SEAM_PATH,
      postSeamStart,
      lastProbe,
    });

    console.log(JSON.stringify({
      ok: true,
      game: GAME_URL,
      monitorUrl: MONITOR_URL,
      brokerViewUrl: BROKER_VIEW_URL || null,
      monitor: summarizeMonitor(lastMonitor),
      playableSeam: {
        probeName: NAME,
        socketId,
        firstBlock,
        finalBlock: currentBlock,
        blockChanges,
        blocks: [...blocks],
        path: Number(path.toFixed(3)),
        postSeamPath: Number(postSeamPath.toFixed(3)),
        commandCount,
        serverFrames,
        maxMissingStreak,
        firstProbe: firstProbe ? { x: firstProbe.x, y: firstProbe.y, block: firstProbe.block } : null,
        finalProbe: lastProbe ? { x: lastProbe.x, y: lastProbe.y, block: lastProbe.block } : null,
        brokerMirrorMatched: !!brokerMatch,
        brokerMirrorOwner: brokerMatch ? (brokerMatch.o || brokerMatch.owner || null) : null,
      },
    }, null, 2));
  } finally {
    clearInterval(commandTimer);
    cleanup();
  }
}

main().catch(err => {
  console.error(JSON.stringify({
    ok: false,
    error: err.message,
    facts: err.facts || null,
  }, null, 2));
  process.exit(1);
});
