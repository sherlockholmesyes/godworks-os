// Capacity/soak ruler for the public-clean MIT agar.io clone adapter.
// It samples the real :8091 dynamic shard monitor for a sustained window and,
// optionally, the :8092 Godworks broker mirror. This is a capacity floor gate,
// not an absolute benchmark claim.
"use strict";

const http = require("http");

const MONITOR_URL = process.env.GW_AGAR_MONITOR_URL || "http://127.0.0.1:8091/state";
const BROKER_VIEW_URL = process.env.GW_AGAR_BROKER_VIEW_URL || "";
const DURATION_MS = parseInt(process.env.GW_AGAR_CAPACITY_MS || "15000", 10);
const SAMPLE_MS = parseInt(process.env.GW_AGAR_CAPACITY_SAMPLE_MS || "500", 10);
const MIN_SAMPLES = parseInt(process.env.GW_AGAR_CAPACITY_MIN_SAMPLES || "8", 10);
const MIN_OK_SAMPLES = parseInt(process.env.GW_AGAR_CAPACITY_MIN_OK_SAMPLES || "8", 10);
const MIN_ENTITIES = parseInt(process.env.GW_AGAR_CAPACITY_MIN_ENTITIES || "800", 10);
const MIN_PLAYERS = parseInt(process.env.GW_AGAR_CAPACITY_MIN_PLAYERS || "30", 10);
const MIN_WORKERS = parseInt(process.env.GW_AGAR_CAPACITY_MIN_WORKERS || "16", 10);
const REQUIRE_DYNAMIC_GEOMETRY = process.env.GW_AGAR_CAPACITY_REQUIRE_DYNAMIC !== "0";
const MAX_PEAK_TO_MEAN = Number(process.env.GW_AGAR_CAPACITY_MAX_PEAK_TO_MEAN || "0");

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

function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function parseJson(label, body) {
  try {
    return JSON.parse(body);
  } catch (err) {
    throw new Error(`${label} did not return JSON: ${err.message}`);
  }
}

function assertOk(condition, message, facts) {
  if (!condition) {
    const err = new Error(message);
    err.facts = facts;
    throw err;
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

function finiteNumber(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function summarizeNumbers(values) {
  assertOk(values.length > 0, "cannot summarize an empty sample set", {});
  const sum = values.reduce((acc, value) => acc + value, 0);
  return {
    min: Math.min(...values),
    max: Math.max(...values),
    mean: sum / values.length,
  };
}

function sampleMonitor(monitor) {
  const entities = Array.isArray(monitor.entities) ? monitor.entities : [];
  const loads = Array.isArray(monitor.loads)
    ? monitor.loads.map(value => finiteNumber(value)).filter(Number.isFinite)
    : [];
  const blocks = flattenBlocks(monitor.bands);
  const blockWidths = unique(blocks.map(b => b.w));
  const blockHeights = unique(blocks.map(b => b.h));
  const players = entities.filter(e => e && e.type === "player").length;
  const loadSum = loads.reduce((acc, value) => acc + value, 0);
  const loadMean = loads.length ? loadSum / loads.length : 0;
  const loadPeak = loads.length ? Math.max(...loads) : 0;

  return {
    entities: entities.length,
    players,
    workers: loads.length,
    rebalanceCount: finiteNumber(monitor.rebalanceCount),
    dynamicWidthClasses: blockWidths.length,
    dynamicHeightClasses: blockHeights.length,
    loadMean,
    loadPeak,
    loadPeakToMean: loadMean > 0 ? loadPeak / loadMean : 0,
    loads,
  };
}

async function sampleBrokerView() {
  if (!BROKER_VIEW_URL) {
    return null;
  }
  const response = await get(BROKER_VIEW_URL);
  assertOk(response.status === 200, "broker view did not return 200", {
    status: response.status,
    url: BROKER_VIEW_URL,
  });
  const brokerEntities = parseJson("broker view", response.body);
  assertOk(Array.isArray(brokerEntities), "broker view did not return an entity array", {
    url: BROKER_VIEW_URL,
  });
  const owners = unique(brokerEntities.map(e => e && (e.o || e.owner || e.region || "?")));
  return {
    entities: brokerEntities.length,
    ownerCount: owners.length,
    mitOwnerCount: owners.filter(o => /^mit-Z\d+_\d+$/.test(o)).length,
  };
}

async function main() {
  assertOk(Number.isFinite(DURATION_MS) && DURATION_MS > 0, "invalid capacity duration", {
    durationMs: DURATION_MS,
  });
  assertOk(Number.isFinite(SAMPLE_MS) && SAMPLE_MS > 0, "invalid capacity sample interval", {
    sampleMs: SAMPLE_MS,
  });

  const started = Date.now();
  const samples = [];
  const brokerSamples = [];
  const errors = [];

  while (Date.now() - started < DURATION_MS || samples.length < MIN_SAMPLES) {
    try {
      const response = await get(MONITOR_URL);
      assertOk(response.status === 200, "monitor did not return 200", {
        status: response.status,
        url: MONITOR_URL,
      });
      samples.push(sampleMonitor(parseJson("monitor", response.body)));
    } catch (err) {
      errors.push(err.message);
    }

    try {
      const broker = await sampleBrokerView();
      if (broker) {
        brokerSamples.push(broker);
      }
    } catch (err) {
      errors.push(err.message);
    }

    await delay(SAMPLE_MS);
  }

  assertOk(samples.length >= MIN_SAMPLES, "capacity gate collected too few monitor samples", {
    samples: samples.length,
    minSamples: MIN_SAMPLES,
    errors,
  });

  const okSamples = samples.filter(sample =>
    sample.entities >= MIN_ENTITIES &&
    sample.players >= MIN_PLAYERS &&
    sample.workers >= MIN_WORKERS
  );
  const dynamicSamples = samples.filter(sample =>
    sample.dynamicWidthClasses > 1 || sample.dynamicHeightClasses > 1
  );
  const peakToMeanSamples = samples.map(sample => sample.loadPeakToMean).filter(Number.isFinite);
  const peakToMean = peakToMeanSamples.length ? Math.max(...peakToMeanSamples) : 0;

  assertOk(okSamples.length >= MIN_OK_SAMPLES, "capacity floor was not sustained for enough samples", {
    okSamples: okSamples.length,
    minOkSamples: MIN_OK_SAMPLES,
    minEntities: MIN_ENTITIES,
    minPlayers: MIN_PLAYERS,
    minWorkers: MIN_WORKERS,
  });
  if (REQUIRE_DYNAMIC_GEOMETRY) {
    assertOk(dynamicSamples.length > 0, "dynamic shard geometry was never observed", {
      dynamicSamples: dynamicSamples.length,
    });
  }
  if (MAX_PEAK_TO_MEAN > 0) {
    assertOk(peakToMean <= MAX_PEAK_TO_MEAN, "worker load peak/mean ratio exceeded configured cap", {
      peakToMean,
      maxPeakToMean: MAX_PEAK_TO_MEAN,
    });
  }
  if (BROKER_VIEW_URL) {
    assertOk(brokerSamples.length > 0, "broker mirror view produced no samples", {
      brokerViewUrl: BROKER_VIEW_URL,
    });
    assertOk(Math.max(...brokerSamples.map(sample => sample.mitOwnerCount)) > 0, "broker mirror never exposed MIT owners", {
      brokerViewUrl: BROKER_VIEW_URL,
    });
  }

  const entityStats = summarizeNumbers(samples.map(sample => sample.entities));
  const playerStats = summarizeNumbers(samples.map(sample => sample.players));
  const workerStats = summarizeNumbers(samples.map(sample => sample.workers));
  const loadPeakStats = summarizeNumbers(samples.map(sample => sample.loadPeak));
  const loadMeanStats = summarizeNumbers(samples.map(sample => sample.loadMean));
  const first = samples[0];
  const last = samples[samples.length - 1];
  const brokerView = brokerSamples.length
    ? {
        samples: brokerSamples.length,
        entitiesMax: Math.max(...brokerSamples.map(sample => sample.entities)),
        ownerCountMax: Math.max(...brokerSamples.map(sample => sample.ownerCount)),
        mitOwnerCountMax: Math.max(...brokerSamples.map(sample => sample.mitOwnerCount)),
      }
    : null;

  console.log(JSON.stringify({
    ok: true,
    gate: "mit_clone_capacity",
    monitorUrl: MONITOR_URL,
    brokerViewUrl: BROKER_VIEW_URL || null,
    thresholds: {
      durationMs: DURATION_MS,
      sampleMs: SAMPLE_MS,
      minSamples: MIN_SAMPLES,
      minOkSamples: MIN_OK_SAMPLES,
      minEntities: MIN_ENTITIES,
      minPlayers: MIN_PLAYERS,
      minWorkers: MIN_WORKERS,
      requireDynamicGeometry: REQUIRE_DYNAMIC_GEOMETRY,
      maxPeakToMean: MAX_PEAK_TO_MEAN,
    },
    monitor: {
      entities: entityStats.max,
      players: playerStats.max,
      rebalanceCount: last.rebalanceCount,
      loads: last.loads,
      dynamicWidthClasses: Math.max(...samples.map(sample => sample.dynamicWidthClasses)),
      dynamicHeightClasses: Math.max(...samples.map(sample => sample.dynamicHeightClasses)),
    },
    capacity: {
      samples: samples.length,
      okSamples: okSamples.length,
      durationMs: Date.now() - started,
      entitiesMin: entityStats.min,
      entitiesMax: entityStats.max,
      entitiesMean: entityStats.mean,
      playersMin: playerStats.min,
      playersMax: playerStats.max,
      playersMean: playerStats.mean,
      workersMin: workerStats.min,
      workersMax: workerStats.max,
      loadMeanMin: loadMeanStats.min,
      loadMeanMax: loadMeanStats.max,
      loadPeakMin: loadPeakStats.min,
      loadPeakMax: loadPeakStats.max,
      loadPeakToMeanMax: peakToMean,
      rebalanceStart: first.rebalanceCount,
      rebalanceEnd: last.rebalanceCount,
      rebalanceDelta: last.rebalanceCount - first.rebalanceCount,
    },
    brokerView,
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
