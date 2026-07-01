// Multi-player broker-command capacity gate for the MIT agar.io clone adapter.
//
// This is the intersection of the capacity gate and the broker-command seam
// gate: background bots may create load, but the proof requires N controlled
// stock-clone players to move only through Godworks CommandRequest routing.
"use strict";

const net = require("net");
const http = require("http");

const HOST = process.env.GW_HOST || "127.0.0.1";
const PORT = parseInt(process.env.GW_PORT || "7990", 10);
const CLIENT_ID = process.env.GW_AGAR_COMMAND_CAPACITY_CLIENT_ID || `agar-cmd-cap-${Date.now()}`;
const CLIENT_TOKEN = process.env.GW_CLIENT_TOKEN || process.env.GW_AGAR_CLIENT_TOKEN || "client-token";
const BRIDGE_URL = process.env.GW_AGAR_COMMAND_BRIDGE_URL || "http://127.0.0.1:8093";
const MONITOR_URL = process.env.GW_AGAR_MONITOR_URL || "http://127.0.0.1:8091/state";
const BROKER_VIEW_URL = process.env.GW_AGAR_BROKER_VIEW_URL || "http://127.0.0.1:8092/state";

const CONTROLLED_PLAYERS = parseInt(process.env.GW_AGAR_COMMAND_CAPACITY_PLAYERS || "4", 10);
const MIN_COMPLETED = parseInt(process.env.GW_AGAR_COMMAND_CAPACITY_MIN_COMPLETED || String(CONTROLLED_PLAYERS), 10);
const TIMEOUT_MS = parseInt(process.env.GW_AGAR_COMMAND_CAPACITY_TIMEOUT_MS || "90000", 10);
const POLL_MS = parseInt(process.env.GW_AGAR_COMMAND_CAPACITY_POLL_MS || "180", 10);
const COMMAND_MS = parseInt(process.env.GW_AGAR_COMMAND_CAPACITY_COMMAND_MS || "240", 10);
const RESPONSE_TIMEOUT_MS = parseInt(process.env.GW_AGAR_COMMAND_RESPONSE_TIMEOUT_MS || "5000", 10);
const MIN_PATH = parseFloat(process.env.GW_AGAR_COMMAND_CAPACITY_MIN_PATH || "100");
const MIN_POST_SEAM_PATH = parseFloat(process.env.GW_AGAR_COMMAND_CAPACITY_MIN_POST_SEAM_PATH || "16");

const MIN_SAMPLES = parseInt(process.env.GW_AGAR_CAPACITY_MIN_SAMPLES || "8", 10);
const MIN_OK_SAMPLES = parseInt(process.env.GW_AGAR_CAPACITY_MIN_OK_SAMPLES || "8", 10);
const MIN_ENTITIES = parseInt(process.env.GW_AGAR_CAPACITY_MIN_ENTITIES || "800", 10);
const MIN_PLAYERS = parseInt(process.env.GW_AGAR_CAPACITY_MIN_PLAYERS || "30", 10);
const MIN_WORKERS = parseInt(process.env.GW_AGAR_CAPACITY_MIN_WORKERS || "16", 10);

const DIRECTIONS = [
  { name: "east", target: { x: 1e6, y: 0 } },
  { name: "west", target: { x: -1e6, y: 0 } },
  { name: "south", target: { x: 0, y: 1e6 } },
  { name: "north", target: { x: 0, y: -1e6 } },
];

function frame(obj) {
  const body = Buffer.from(JSON.stringify(obj), "utf8");
  const header = Buffer.alloc(4);
  header.writeUInt32BE(body.length, 0);
  return Buffer.concat([header, body]);
}

function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function assertOk(condition, message, facts) {
  if (!condition) {
    const err = new Error(message);
    err.facts = facts;
    throw err;
  }
}

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

async function fetchJson(label, url) {
  const response = await get(url);
  assertOk(response.status === 200, `${label} did not return HTTP 200`, {
    status: response.status,
    url,
  });
  return parseJson(label, response.body);
}

function finiteNumber(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function distance(a, b) {
  if (!a || !b) return 0;
  return Math.hypot(Number(a.x) - Number(b.x), Number(a.y) - Number(b.y));
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
  return [...new Set(values.map(value => String(value)))];
}

function sampleMonitor(monitor) {
  const entities = Array.isArray(monitor.entities) ? monitor.entities : [];
  const loads = Array.isArray(monitor.loads)
    ? monitor.loads.map(value => finiteNumber(value)).filter(Number.isFinite)
    : [];
  const blocks = flattenBlocks(monitor.bands);
  const loadSum = loads.reduce((acc, value) => acc + value, 0);
  const loadMean = loads.length ? loadSum / loads.length : 0;
  const loadPeak = loads.length ? Math.max(...loads) : 0;
  return {
    entities: entities.length,
    players: entities.filter(e => e && e.type === "player").length,
    workers: loads.length,
    rebalanceCount: finiteNumber(monitor.rebalanceCount),
    dynamicWidthClasses: unique(blocks.map(block => block.w)).length,
    dynamicHeightClasses: unique(blocks.map(block => block.h)).length,
    loadMean,
    loadPeak,
    loadPeakToMean: loadMean > 0 ? loadPeak / loadMean : 0,
    loads,
  };
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

function summarizeOptionalNumbers(values) {
  const finite = values.map(value => Number(value)).filter(Number.isFinite);
  if (!finite.length) return null;
  finite.sort((a, b) => a - b);
  const sum = finite.reduce((acc, value) => acc + value, 0);
  const p95Index = Math.min(finite.length - 1, Math.max(0, Math.ceil(finite.length * 0.95) - 1));
  return {
    count: finite.length,
    min: finite[0],
    max: finite[finite.length - 1],
    mean: sum / finite.length,
    p95: finite[p95Index],
  };
}

function summarizeMonitorSamples(samples) {
  const entityStats = summarizeNumbers(samples.map(sample => sample.entities));
  const playerStats = summarizeNumbers(samples.map(sample => sample.players));
  const workerStats = summarizeNumbers(samples.map(sample => sample.workers));
  const loadPeakStats = summarizeNumbers(samples.map(sample => sample.loadPeak));
  const loadMeanStats = summarizeNumbers(samples.map(sample => sample.loadMean));
  const peakToMeanStats = summarizeNumbers(samples.map(sample => sample.loadPeakToMean));
  const first = samples[0];
  const last = samples[samples.length - 1];
  return {
    samples: samples.length,
    okSamples: samples.filter(sample =>
      sample.entities >= MIN_ENTITIES &&
      sample.players >= MIN_PLAYERS &&
      sample.workers >= MIN_WORKERS
    ).length,
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
    loadPeakToMeanMax: peakToMeanStats.max,
    rebalanceStart: first.rebalanceCount,
    rebalanceEnd: last.rebalanceCount,
    rebalanceDelta: last.rebalanceCount - first.rebalanceCount,
  };
}

function findMonitorProbe(monitor, player) {
  const entities = Array.isArray(monitor && monitor.entities) ? monitor.entities : [];
  return entities.find(e => e && e.id === player.entity)
    || entities.find(e => e && e.type === "player" && e.owner_id === player.socketId)
    || entities.find(e => e && e.type === "player" && e.name === player.name)
    || null;
}

function findBrokerProbe(view, entity) {
  const entities = Array.isArray(view) ? view : [];
  return entities.find(e => e && (e.id === entity || e.entity === entity)) || null;
}

function parseBlock(block) {
  const match = /^W(\d+)_(\d+)$/.exec(String(block || ""));
  if (!match) return null;
  return { bx: Number(match[1]), by: Number(match[2]) };
}

function blockBounds(probe, monitor) {
  const parsed = parseBlock(probe && probe.block);
  const bands = Array.isArray(monitor && monitor.bands) ? monitor.bands : [];
  if (!parsed || !bands[parsed.bx]) return null;
  const band = bands[parsed.bx];
  const rows = Array.isArray(band.rows) ? band.rows : [];
  const row = rows[parsed.by];
  if (!row) return null;
  return {
    bx: parsed.bx,
    by: parsed.by,
    cols: bands.length,
    rows: rows.length,
    x0: Number(band.c0) * Number(monitor.width) / 100,
    x1: Number(band.c1) * Number(monitor.width) / 100,
    y0: Number(row.r0) * Number(monitor.height) / 100,
    y1: Number(row.r1) * Number(monitor.height) / 100,
  };
}

function chooseDirection(probe, monitor, attempt) {
  const bounds = probe && monitor ? blockBounds(probe, monitor) : null;
  if (bounds) {
    const candidates = [];
    if (bounds.bx + 1 < bounds.cols) {
      candidates.push({ name: "east", distance: Math.abs(bounds.x1 - Number(probe.x)), direction: DIRECTIONS[0] });
    }
    if (bounds.bx > 0) {
      candidates.push({ name: "west", distance: Math.abs(Number(probe.x) - bounds.x0), direction: DIRECTIONS[1] });
    }
    if (bounds.by + 1 < bounds.rows) {
      candidates.push({ name: "south", distance: Math.abs(bounds.y1 - Number(probe.y)), direction: DIRECTIONS[2] });
    }
    if (bounds.by > 0) {
      candidates.push({ name: "north", distance: Math.abs(Number(probe.y) - bounds.y0), direction: DIRECTIONS[3] });
    }
    if (candidates.length) {
      candidates.sort((a, b) => a.distance - b.distance || a.name.localeCompare(b.name));
      return candidates[attempt % candidates.length].direction;
    }
  }
  if (!probe || !monitor) return DIRECTIONS[attempt % DIRECTIONS.length];
  if (attempt === 0) return Number(probe.x) < Number(monitor.width) / 2 ? DIRECTIONS[0] : DIRECTIONS[1];
  if (attempt === 1) return Number(probe.y) < Number(monitor.height) / 2 ? DIRECTIONS[2] : DIRECTIONS[3];
  return DIRECTIONS[attempt % DIRECTIONS.length];
}

function ackPayload(ack) {
  return ack && ack.payload && typeof ack.payload === "object" ? ack.payload : {};
}

function ackOwner(ack) {
  const payload = ackPayload(ack);
  return payload.owner || payload.handled_by || null;
}

function ackRoutedOwner(ack) {
  const payload = ackPayload(ack);
  return (ack && ack.routed_owner) || payload.routed_owner || null;
}

function ackAccepted(ack) {
  const payload = ackPayload(ack);
  return ack && ack.success !== false && payload.accepted !== false;
}

function ackEntity(ack) {
  return ackPayload(ack).entity || null;
}

class BrokerClient {
  constructor() {
    this.sock = null;
    this.buf = Buffer.alloc(0);
    this.waiters = new Map();
  }

  connect() {
    return new Promise((resolve, reject) => {
      const sock = net.connect(PORT, HOST, () => {
        this.sock = sock;
        const connect = { op: "WorkerConnect", worker_id: CLIENT_ID, region: "CLIENT" };
        if (CLIENT_TOKEN) connect.auth_token = CLIENT_TOKEN;
        this.send(connect);
        this.send({ op: "Interest", center: [2500, 2500], radius: 1e9 });
        resolve();
      });
      sock.on("data", data => this.onData(data));
      sock.on("error", reject);
      sock.on("close", () => {
        for (const [reqId, waiter] of this.waiters) {
          clearTimeout(waiter.timer);
          waiter.reject(new Error(`broker connection closed before ${reqId}`));
        }
        this.waiters.clear();
      });
    });
  }

  send(obj) {
    assertOk(this.sock && !this.sock.destroyed, "broker client is not connected");
    this.sock.write(frame(obj));
  }

  command(entity, target, seq) {
    const requestId = `${CLIENT_ID}-${Date.now()}-${seq}`;
    const promise = new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.waiters.delete(requestId);
        reject(new Error(`CommandResponse timeout for ${requestId}`));
      }, RESPONSE_TIMEOUT_MS);
      this.waiters.set(requestId, { resolve, reject, timer, sentAt: Date.now() });
    });
    this.send({
      op: "CommandRequest",
      request_id: requestId,
      entity,
      command: "move_target",
      payload: { target },
      timeout_ms: RESPONSE_TIMEOUT_MS,
    });
    return promise;
  }

  onData(data) {
    this.buf = Buffer.concat([this.buf, data]);
    while (this.buf.length >= 4) {
      const n = this.buf.readUInt32BE(0);
      if (this.buf.length < 4 + n) break;
      let msg = null;
      try {
        msg = JSON.parse(this.buf.slice(4, 4 + n).toString("utf8"));
      } catch (_) {}
      this.buf = this.buf.slice(4 + n);
      if (msg && msg.op === "CommandResponse") {
        const reqId = msg.request_id || "";
        const waiter = this.waiters.get(reqId);
        if (waiter) {
          clearTimeout(waiter.timer);
          this.waiters.delete(reqId);
          msg.command_round_trip_ms = Date.now() - waiter.sentAt;
          waiter.resolve(msg);
        }
      }
    }
  }

  close() {
    if (this.sock) this.sock.destroy();
  }
}

async function waitForControlledPlayers() {
  const deadline = Date.now() + TIMEOUT_MS;
  while (Date.now() < deadline) {
    const state = await fetchJson("command bridge players", `${BRIDGE_URL.replace(/\/$/, "")}/players`);
    const players = Array.isArray(state.players) ? state.players.filter(player => player && player.ok && player.entity && player.socketId) : [];
    if (players.length >= CONTROLLED_PLAYERS) {
      return players.slice(0, CONTROLLED_PLAYERS).map(player => ({
        entity: player.entity,
        socketId: player.socketId,
        name: player.name,
        index: player.index,
        initialRipCount: Number(player.ripCount) || 0,
        initialCommandCount: Number(player.commandCount) || 0,
      }));
    }
    await delay(POLL_MS);
  }
  throw new Error(`command bridge did not expose ${CONTROLLED_PLAYERS} ready controlled players`);
}

function makeTracker(player, index) {
  return {
    ...player,
    firstProbe: null,
    lastProbe: null,
    firstBlock: null,
    currentBlock: null,
    firstOwner: null,
    currentOwner: null,
    owners: new Set(),
    blocks: new Set(),
    ownerChanges: 0,
    blockChanges: 0,
    path: 0,
    postSeamPath: 0,
    firstSeamAt: null,
    postSeamCommandOk: false,
    commandResponses: 0,
    commandOwnerMatches: 0,
    commandLatenciesMs: [],
    bridgeCommandCount: 0,
    directionAttempt: index % DIRECTIONS.length,
    direction: DIRECTIONS[index % DIRECTIONS.length],
    lastCommandAt: 0,
    lastDirectionAt: 0,
    completed: false,
    failed: false,
    failure: null,
  };
}

async function validateSelectedPlayerAlive(tracker) {
  const state = await fetchJson("command bridge selected player", `${BRIDGE_URL.replace(/\/$/, "")}/state?entity=${encodeURIComponent(tracker.entity)}`);
  if (!state.ok) {
    tracker.failed = true;
    tracker.failure = {
      reason: "selected controlled player is no longer ready",
      state,
    };
    return false;
  }
  if (Number(state.ripCount) !== tracker.initialRipCount) {
    tracker.failed = true;
    tracker.failure = {
      reason: "selected controlled player died before seam proof completed",
      initialRipCount: tracker.initialRipCount,
      currentRipCount: state.ripCount,
      state,
    };
    return false;
  }
  tracker.bridgeCommandCount = Number(state.commandCount) || 0;
  return true;
}

function trackerComplete(tracker) {
  return tracker.ownerChanges > 0
    && tracker.blockChanges > 0
    && tracker.postSeamCommandOk
    && tracker.path >= MIN_PATH
    && tracker.postSeamPath >= MIN_POST_SEAM_PATH;
}

async function updateTracker(tracker, monitor, view, broker, seqRef, startedAt) {
  if (tracker.failed) return;
  if (!(await validateSelectedPlayerAlive(tracker))) return;
  const probe = findMonitorProbe(monitor, tracker);
  const brokerProbe = findBrokerProbe(view, tracker.entity);
  if (!probe || !brokerProbe) return;

  const owner = brokerProbe.o || brokerProbe.owner || null;
  assertOk(!!owner, "broker view found controlled player without a pos owner", {
    entity: tracker.entity,
    brokerProbe,
  });

  if (!tracker.firstProbe) {
    tracker.firstProbe = { ...probe };
    tracker.firstBlock = probe.block || "?";
    tracker.currentBlock = tracker.firstBlock;
    tracker.firstOwner = owner;
    tracker.currentOwner = owner;
    tracker.direction = chooseDirection(probe, monitor, tracker.directionAttempt);
    tracker.lastDirectionAt = Date.now();
  } else {
    const step = distance(tracker.lastProbe, probe);
    tracker.path += step;
    if (tracker.firstSeamAt) tracker.postSeamPath += step;
  }

  const block = probe.block || "?";
  tracker.blocks.add(block);
  tracker.owners.add(owner);
  if (tracker.currentBlock && block !== tracker.currentBlock) {
    tracker.blockChanges++;
    tracker.currentBlock = block;
  }
  if (tracker.currentOwner && owner !== tracker.currentOwner) {
    tracker.ownerChanges++;
    tracker.currentOwner = owner;
    if (!tracker.firstSeamAt) {
      tracker.firstSeamAt = Date.now();
      tracker.postSeamPath = 0;
    }
  }

  const now = Date.now();
  if (now - tracker.lastCommandAt >= COMMAND_MS) {
    const expectedOwner = owner;
    const ack = await broker.command(tracker.entity, tracker.direction.target, ++seqRef.value);
    tracker.commandResponses++;
    if (Number.isFinite(Number(ack.command_round_trip_ms))) {
      tracker.commandLatenciesMs.push(Number(ack.command_round_trip_ms));
    }
    assertOk(ack.success !== false, "broker CommandRequest returned failure", {
      entity: tracker.entity,
      expectedOwner,
      ack,
    });
    assertOk(ackAccepted(ack), "command bridge did not accept broker-routed command", {
      entity: tracker.entity,
      expectedOwner,
      ack,
    });
    assertOk(!ackEntity(ack) || ackEntity(ack) === tracker.entity, "broker command response named the wrong entity", {
      entity: tracker.entity,
      ackEntity: ackEntity(ack),
      ack,
    });
    const ownerAfterAck = ackOwner(ack);
    const routedOwner = ackRoutedOwner(ack);
    let freshOwner = null;
    let acceptedOwner = (!!routedOwner && ownerAfterAck === routedOwner) || ownerAfterAck === expectedOwner;
    if (!acceptedOwner && ownerAfterAck) {
      const freshView = await fetchJson("broker view", BROKER_VIEW_URL);
      const freshProbe = findBrokerProbe(freshView, tracker.entity);
      freshOwner = freshProbe && (freshProbe.o || freshProbe.owner || null);
      acceptedOwner = ownerAfterAck === freshOwner;
    }
    if (ownerAfterAck && routedOwner) {
      assertOk(ownerAfterAck === routedOwner, "broker routed owner and worker response owner diverged", {
        entity: tracker.entity,
        routedOwner,
        ownerAfterAck,
        ack,
      });
    }
    const acceptedCurrentOwner = routedOwner || freshOwner;
    if (acceptedOwner && acceptedCurrentOwner && tracker.currentOwner && acceptedCurrentOwner !== tracker.currentOwner) {
      tracker.ownerChanges++;
      tracker.currentOwner = acceptedCurrentOwner;
      tracker.owners.add(acceptedCurrentOwner);
      if (!tracker.firstSeamAt) {
        tracker.firstSeamAt = Date.now();
        tracker.postSeamPath = 0;
      }
    }
    assertOk(acceptedOwner, "broker command was handled by a stale or wrong owner", {
      entity: tracker.entity,
      expectedOwner,
      ownerAfterAck,
      routedOwner,
      freshOwner,
      ack,
    });
    tracker.commandOwnerMatches++;
    if (tracker.firstSeamAt && ownerAfterAck === tracker.currentOwner) {
      tracker.postSeamCommandOk = true;
    }
    tracker.lastCommandAt = now;
  }

  tracker.lastProbe = { ...probe };
  if (!tracker.completed && tracker.blockChanges === 0 && Date.now() - tracker.lastDirectionAt > 5000) {
    tracker.directionAttempt++;
    tracker.direction = chooseDirection(probe, monitor, tracker.directionAttempt);
    tracker.lastDirectionAt = Date.now();
  } else if (tracker.firstProbe && distance(tracker.firstProbe, probe) < 80 && Date.now() - startedAt > 5000) {
    tracker.directionAttempt++;
    tracker.direction = chooseDirection(probe, monitor, tracker.directionAttempt);
    tracker.lastDirectionAt = Date.now();
  }
  tracker.completed = trackerComplete(tracker);
}

function trackerSummary(tracker) {
  return {
    entity: tracker.entity,
    socketId: tracker.socketId,
    firstOwner: tracker.firstOwner,
    finalOwner: tracker.currentOwner,
    ownerChanges: tracker.ownerChanges,
    firstBlock: tracker.firstBlock,
    finalBlock: tracker.currentBlock,
    blockChanges: tracker.blockChanges,
    path: Number(tracker.path.toFixed(3)),
    postSeamPath: Number(tracker.postSeamPath.toFixed(3)),
    commandResponses: tracker.commandResponses,
    commandOwnerMatches: tracker.commandOwnerMatches,
    commandLatencyMs: summarizeOptionalNumbers(tracker.commandLatenciesMs),
    bridgeCommandCount: tracker.bridgeCommandCount,
    completed: tracker.completed,
    failed: tracker.failed,
    failure: tracker.failure,
  };
}

function brokerViewSummary(samples) {
  if (!samples.length) return null;
  return {
    samples: samples.length,
    entitiesMax: Math.max(...samples.map(sample => sample.entities)),
    ownerCountMax: Math.max(...samples.map(sample => sample.ownerCount)),
    mitOwnerCountMax: Math.max(...samples.map(sample => sample.mitOwnerCount)),
  };
}

function sampleBrokerView(view) {
  const entities = Array.isArray(view) ? view : [];
  const owners = unique(entities.map(e => e && (e.o || e.owner || e.region || "?")));
  return {
    entities: entities.length,
    ownerCount: owners.length,
    mitOwnerCount: owners.filter(owner => /^mit-Z\d+_\d+$/.test(owner)).length,
  };
}

async function main() {
  assertOk(CONTROLLED_PLAYERS > 0, "controlled player count must be positive", { CONTROLLED_PLAYERS });
  assertOk(MIN_COMPLETED > 0 && MIN_COMPLETED <= CONTROLLED_PLAYERS, "invalid minimum completed player count", {
    MIN_COMPLETED,
    CONTROLLED_PLAYERS,
  });

  const startedAt = Date.now();
  const broker = new BrokerClient();
  await broker.connect();
  const selected = await waitForControlledPlayers();
  const trackers = selected.map(makeTracker);
  const seqRef = { value: 0 };
  const monitorSamples = [];
  const brokerSamples = [];
  let lastMonitor = null;

  try {
    while (Date.now() - startedAt < TIMEOUT_MS) {
      const monitor = await fetchJson("monitor", MONITOR_URL);
      const view = await fetchJson("broker view", BROKER_VIEW_URL);
      lastMonitor = monitor;
      monitorSamples.push(sampleMonitor(monitor));
      brokerSamples.push(sampleBrokerView(view));

      for (const tracker of trackers) {
        if (!tracker.completed && !tracker.failed) {
          await updateTracker(tracker, monitor, view, broker, seqRef, startedAt);
        }
      }

      if (trackers.filter(tracker => tracker.completed).length >= MIN_COMPLETED && monitorSamples.length >= MIN_SAMPLES) {
        break;
      }
      if (trackers.filter(tracker => tracker.completed || !tracker.failed).length < MIN_COMPLETED) {
        break;
      }
      await delay(POLL_MS);
    }

    const capacity = summarizeMonitorSamples(monitorSamples);
    const completed = trackers.filter(tracker => tracker.completed);
    assertOk(capacity.okSamples >= MIN_OK_SAMPLES, "capacity floor was not sustained during broker-command run", {
      okSamples: capacity.okSamples,
      minOkSamples: MIN_OK_SAMPLES,
      minEntities: MIN_ENTITIES,
      minPlayers: MIN_PLAYERS,
      minWorkers: MIN_WORKERS,
    });
    assertOk(completed.length >= MIN_COMPLETED, "too few controlled players completed broker-command seam proof", {
      completed: completed.length,
      minCompleted: MIN_COMPLETED,
      players: trackers.map(trackerSummary),
    });

    const playerSummaries = trackers.map(trackerSummary);
    const allLatencies = trackers.flatMap(tracker => tracker.commandLatenciesMs);
    const completedLatencies = completed.flatMap(tracker => tracker.commandLatenciesMs);
    console.log(JSON.stringify({
      ok: true,
      gate: "mit_clone_broker_command_capacity",
      broker: `${HOST}:${PORT}`,
      bridgeUrl: BRIDGE_URL,
      monitorUrl: MONITOR_URL,
      brokerViewUrl: BROKER_VIEW_URL,
      monitor: {
        entities: capacity.entitiesMax,
        players: capacity.playersMax,
        rebalanceCount: lastMonitor ? finiteNumber(lastMonitor.rebalanceCount) : 0,
        loads: monitorSamples.length ? monitorSamples[monitorSamples.length - 1].loads : [],
        dynamicWidthClasses: Math.max(...monitorSamples.map(sample => sample.dynamicWidthClasses)),
        dynamicHeightClasses: Math.max(...monitorSamples.map(sample => sample.dynamicHeightClasses)),
      },
      capacity,
      brokerView: brokerViewSummary(brokerSamples),
      brokerCommandCapacity: {
        controlledPlayers: CONTROLLED_PLAYERS,
        completedPlayers: completed.length,
        failedPlayers: trackers.filter(tracker => tracker.failed).length,
        minCommandResponses: Math.min(...completed.map(tracker => tracker.commandResponses)),
        totalCommandResponses: trackers.reduce((acc, tracker) => acc + tracker.commandResponses, 0),
        totalCommandOwnerMatches: trackers.reduce((acc, tracker) => acc + tracker.commandOwnerMatches, 0),
        commandLatencyMs: summarizeOptionalNumbers(allLatencies),
        completedCommandLatencyMs: summarizeOptionalNumbers(completedLatencies),
        minOwnerChanges: Math.min(...completed.map(tracker => tracker.ownerChanges)),
        maxOwnerChanges: Math.max(...trackers.map(tracker => tracker.ownerChanges)),
        minBlockChanges: Math.min(...completed.map(tracker => tracker.blockChanges)),
        maxBlockChanges: Math.max(...trackers.map(tracker => tracker.blockChanges)),
        minPath: Math.min(...completed.map(tracker => tracker.path)),
        minPostSeamPath: Math.min(...completed.map(tracker => tracker.postSeamPath)),
        allPostSeamCommandOk: completed.every(tracker => tracker.postSeamCommandOk),
        players: playerSummaries,
      },
    }, null, 2));
  } finally {
    broker.close();
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
