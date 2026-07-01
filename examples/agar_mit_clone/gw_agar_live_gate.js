// Live gate for the public-clean MIT agar.io clone adapter. It checks the
// playable stock clone, the dynamic :8091 shard monitor, and optionally the
// Godworks broker mirror view/WAL when -MirrorBroker is used.
"use strict";

const fs = require("fs");
const http = require("http");

const GAME_URL = process.env.GW_AGAR_GAME_URL || "http://127.0.0.1:3000/";
const MONITOR_URL = process.env.GW_AGAR_MONITOR_URL || "http://127.0.0.1:8091/state";
const BROKER_VIEW_URL = process.env.GW_AGAR_BROKER_VIEW_URL || "";
const WAL_PATH = process.env.GW_AGAR_WAL || "";
const MIN_ENTITIES = parseInt(process.env.GW_AGAR_MIN_ENTITIES || "50", 10);
const MIN_PLAYERS = parseInt(process.env.GW_AGAR_MIN_PLAYERS || "1", 10);
const REQUIRE_REBALANCE_EVENT = process.env.GW_AGAR_REQUIRE_REBALANCE_EVENT === "1";

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

function assertOk(condition, message, facts) {
  if (!condition) {
    const err = new Error(message);
    err.facts = facts;
    throw err;
  }
}

function parseJson(label, body) {
  try {
    return JSON.parse(body);
  } catch (err) {
    throw new Error(`${label} did not return JSON: ${err.message}`);
  }
}

function flattenBlocks(bands) {
  const blocks = [];
  for (const band of bands || []) {
    for (const row of band.rows || []) {
      blocks.push({
        w: Number(band.c1) - Number(band.c0),
        h: Number(row.r1) - Number(row.r0),
      });
    }
  }
  return blocks;
}

function unique(values) {
  return [...new Set(values.map(v => String(v)))];
}

function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function retryReady(label, probe, attempts = 40, delayMs = 500) {
  let lastErr = null;
  for (let i = 0; i < attempts; i++) {
    try {
      return await probe();
    } catch (err) {
      lastErr = err;
      await delay(delayMs);
    }
  }
  throw lastErr || new Error(`${label} did not become ready`);
}

async function main() {
  await retryReady("game", async () => {
    const game = await get(GAME_URL);
    assertOk(game.status === 200 && /<title>Open Agar<\/title>/.test(game.body), "stock agar.io clone is not serving the playable game", {
      status: game.status,
      url: GAME_URL,
    });
    return game;
  });

  const monitorReady = await retryReady("monitor", async () => {
    const monitor = parseJson("monitor", (await get(MONITOR_URL)).body);
    const blocks = flattenBlocks(monitor.bands);
    const blockWidths = unique(blocks.map(b => b.w));
    const blockHeights = unique(blocks.map(b => b.h));
    const entities = Array.isArray(monitor.entities) ? monitor.entities : [];
    const playerCount = entities.filter(e => e.type === "player").length;
    assertOk(entities.length >= MIN_ENTITIES, "monitor sees too few live game entities", {
      entities: entities.length,
      minEntities: MIN_ENTITIES,
    });
    assertOk(playerCount >= MIN_PLAYERS, "monitor sees too few player entities", {
      playerCount,
      minPlayers: MIN_PLAYERS,
    });
    assertOk(monitor.workerCols === 4 && monitor.workerRows === 4 && blocks.length === 16, "monitor is not a 4x4 worker-zone view", {
      workerCols: monitor.workerCols,
      workerRows: monitor.workerRows,
      blocks: blocks.length,
    });
    assertOk(Array.isArray(monitor.loads) && monitor.loads.length === 16 && monitor.loads.every(n => Number.isFinite(Number(n))), "monitor load vector is missing or malformed", {
      loads: monitor.loads,
    });
    assertOk(blockWidths.length > 1 || blockHeights.length > 1, "worker zones look static/uniform, not dynamically resized", {
      widths: blockWidths,
      heights: blockHeights,
    });
    if (REQUIRE_REBALANCE_EVENT) {
      assertOk((monitor.rebalanceCount || 0) > 0, "dynamic monitor rebalance has not happened", {
        rebalanceCount: monitor.rebalanceCount,
      });
    }
    return { monitor, entities, playerCount, blockWidths, blockHeights };
  });

  let brokerView = null;
  if (BROKER_VIEW_URL) {
    brokerView = await retryReady("broker mirror", async () => {
      const brokerBody = (await get(BROKER_VIEW_URL)).body;
      const brokerEntities = parseJson("broker view", brokerBody);
      const owners = unique((Array.isArray(brokerEntities) ? brokerEntities : []).map(e => e.o || e.owner || e.region || "?"));
      assertOk(Array.isArray(brokerEntities) && brokerEntities.length > 0, "broker mirror view sees no entities", {
        brokerEntities: Array.isArray(brokerEntities) ? brokerEntities.length : null,
      });
      assertOk(owners.some(o => /^mit-Z\d+_\d+$/.test(o)), "broker mirror view does not expose MIT mirror workers", {
        owners,
      });
      return {
        entities: brokerEntities.length,
        owners,
      };
    });
  }

  let wal = null;
  if (WAL_PATH) {
    wal = await retryReady("broker mirror WAL", async () => {
      const stat = fs.statSync(WAL_PATH);
      assertOk(stat.size > 38, "broker mirror WAL is empty or missing transitions", {
        path: WAL_PATH,
        bytes: stat.size,
      });
      return { path: WAL_PATH, bytes: stat.size };
    });
  }

  console.log(JSON.stringify({
    ok: true,
    game: GAME_URL,
    monitor: {
      entities: monitorReady.entities.length,
      players: monitorReady.playerCount,
      rebalanceCount: monitorReady.monitor.rebalanceCount,
      loads: monitorReady.monitor.loads,
      dynamicWidthClasses: monitorReady.blockWidths.length,
      dynamicHeightClasses: monitorReady.blockHeights.length,
    },
    brokerView,
    wal,
  }, null, 2));
}

main().catch(err => {
  console.error(JSON.stringify({
    ok: false,
    error: err.message,
    facts: err.facts || null,
  }, null, 2));
  process.exit(1);
});
