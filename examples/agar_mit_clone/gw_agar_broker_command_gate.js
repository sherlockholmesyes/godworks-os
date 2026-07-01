// Broker-command gate for the public-clean MIT agar.io clone adapter.
//
// This gate is intentionally stricter than the playable seam gate. It does not
// emit stock-clone movement commands directly. It drives one controlled player
// only through:
//   client CommandRequest -> broker -> current pos-authority mirror worker
//   -> command bridge -> stock MIT socket -> CommandResponse -> client.
"use strict";

const net = require("net");
const http = require("http");

const HOST = process.env.GW_HOST || "127.0.0.1";
const PORT = parseInt(process.env.GW_PORT || "7990", 10);
const CLIENT_ID = process.env.GW_AGAR_COMMAND_CLIENT_ID || `agar-cmd-gate-${Date.now()}`;
const CLIENT_TOKEN = process.env.GW_CLIENT_TOKEN || process.env.GW_AGAR_CLIENT_TOKEN || "client-token";
const BRIDGE_URL = process.env.GW_AGAR_COMMAND_BRIDGE_URL || "http://127.0.0.1:8093";
const MONITOR_URL = process.env.GW_AGAR_MONITOR_URL || "http://127.0.0.1:8091/state";
const BROKER_VIEW_URL = process.env.GW_AGAR_BROKER_VIEW_URL || "http://127.0.0.1:8092/state";
const TIMEOUT_MS = parseInt(process.env.GW_AGAR_COMMAND_GATE_TIMEOUT_MS || "60000", 10);
const POLL_MS = parseInt(process.env.GW_AGAR_COMMAND_GATE_POLL_MS || "160", 10);
const COMMAND_MS = parseInt(process.env.GW_AGAR_COMMAND_GATE_COMMAND_MS || "180", 10);
const RESPONSE_TIMEOUT_MS = parseInt(process.env.GW_AGAR_COMMAND_RESPONSE_TIMEOUT_MS || "5000", 10);
const MIN_PATH = parseFloat(process.env.GW_AGAR_COMMAND_MIN_PATH || "120");
const MIN_POST_SEAM_PATH = parseFloat(process.env.GW_AGAR_COMMAND_MIN_POST_SEAM_PATH || "18");

const DIRECTIONS = [
  { name: "east", target: { x: 1e6, y: 0 } },
  { name: "west", target: { x: -1e6, y: 0 } },
  { name: "south", target: { x: 0, y: 1e6 } },
  { name: "north", target: { x: 0, y: -1e6 } },
];

function frame(obj) {
  const b = Buffer.from(JSON.stringify(obj), "utf8");
  const h = Buffer.alloc(4);
  h.writeUInt32BE(b.length, 0);
  return Buffer.concat([h, b]);
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
  const res = await get(url);
  assertOk(res.status === 200, `${label} did not return HTTP 200`, { status: res.status, url });
  return parseJson(label, res.body);
}

function distance(a, b) {
  if (!a || !b) return 0;
  return Math.hypot(Number(a.x) - Number(b.x), Number(a.y) - Number(b.y));
}

function findMonitorProbe(monitor, bridge) {
  const entities = Array.isArray(monitor && monitor.entities) ? monitor.entities : [];
  return entities.find(e => e && e.id === bridge.entity)
    || entities.find(e => e && e.type === "player" && e.owner_id === bridge.socketId)
    || entities.find(e => e && e.type === "player" && e.name === bridge.name)
    || null;
}

function findBrokerProbe(view, entity) {
  const entities = Array.isArray(view) ? view : [];
  return entities.find(e => e && (e.id === entity || e.entity === entity)) || null;
}

function chooseDirection(probe, monitor, attempt) {
  if (!probe || !monitor) return DIRECTIONS[attempt % DIRECTIONS.length];
  if (attempt === 0) {
    return Number(probe.x) < Number(monitor.width) / 2 ? DIRECTIONS[0] : DIRECTIONS[1];
  }
  if (attempt === 1) {
    return Number(probe.y) < Number(monitor.height) / 2 ? DIRECTIONS[2] : DIRECTIONS[3];
  }
  return DIRECTIONS[attempt % DIRECTIONS.length];
}

function summarizeMonitor(monitor) {
  if (!monitor || typeof monitor !== "object") return null;
  const entities = Array.isArray(monitor.entities) ? monitor.entities : [];
  const loads = Array.isArray(monitor.loads)
    ? monitor.loads.map(v => Number(v)).filter(Number.isFinite)
    : [];
  return {
    entities: entities.length,
    players: entities.filter(e => e && e.type === "player").length,
    workers: loads.length,
    loads,
    rebalanceCount: Number.isFinite(Number(monitor.rebalanceCount)) ? Number(monitor.rebalanceCount) : 0,
  };
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
      sock.on("data", d => this.onData(d));
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
    const payload = { target };
    const promise = new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.waiters.delete(requestId);
        reject(new Error(`CommandResponse timeout for ${requestId}`));
      }, RESPONSE_TIMEOUT_MS);
      this.waiters.set(requestId, { resolve, reject, timer });
    });
    this.send({
      op: "CommandRequest",
      request_id: requestId,
      entity,
      command: "move_target",
      payload,
      timeout_ms: RESPONSE_TIMEOUT_MS,
    });
    return promise;
  }

  onData(d) {
    this.buf = Buffer.concat([this.buf, d]);
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
          waiter.resolve(msg);
        }
      }
    }
  }

  close() {
    if (this.sock) this.sock.destroy();
  }
}

function ackOwner(ack) {
  const payload = ack && ack.payload && typeof ack.payload === "object" ? ack.payload : {};
  return payload.owner || payload.handled_by || null;
}

function ackAccepted(ack) {
  const payload = ack && ack.payload && typeof ack.payload === "object" ? ack.payload : {};
  return ack && ack.success !== false && payload.accepted !== false;
}

async function waitForBridge() {
  const deadline = Date.now() + TIMEOUT_MS;
  while (Date.now() < deadline) {
    const bridge = await fetchJson("command bridge", `${BRIDGE_URL.replace(/\/$/, "")}/state`);
    if (bridge.ok && bridge.entity && bridge.socketId) return bridge;
    await delay(POLL_MS);
  }
  throw new Error("controlled command bridge did not become ready");
}

async function fetchObservedState(entity, bridge) {
  const monitor = await fetchJson("monitor", MONITOR_URL);
  const view = await fetchJson("broker view", BROKER_VIEW_URL);
  const probe = findMonitorProbe(monitor, bridge);
  const brokerProbe = findBrokerProbe(view, entity);
  return { monitor, view, probe, brokerProbe };
}

async function main() {
  const startedAt = Date.now();
  const broker = new BrokerClient();
  await broker.connect();
  const bridge0 = await waitForBridge();
  const initialRipCount = Number(bridge0.ripCount) || 0;
  const entity = bridge0.entity;

  let seq = 0;
  let firstProbe = null;
  let lastProbe = null;
  let firstBlock = null;
  let currentBlock = null;
  let blockChanges = 0;
  let firstOwner = null;
  let currentOwner = null;
  let ownerChanges = 0;
  let firstSeamAt = null;
  let postSeamPath = 0;
  let path = 0;
  let commandResponses = 0;
  let commandOwnerMatches = 0;
  let postSeamCommandOk = false;
  let lastAck = null;
  let lastMonitor = null;
  let lastBridge = bridge0;
  let direction = DIRECTIONS[0];
  let directionAttempt = 0;
  let lastCommandAt = 0;
  const owners = new Set();
  const blocks = new Set();

  try {
    while (Date.now() - startedAt < TIMEOUT_MS) {
      lastBridge = await fetchJson("command bridge", `${BRIDGE_URL.replace(/\/$/, "")}/state`);
      assertOk(Number(lastBridge.ripCount) === initialRipCount, "controlled player died before broker command seam proof completed", {
        initialRipCount,
        currentRipCount: lastBridge.ripCount,
        entity,
      });

      const { monitor, probe, brokerProbe } = await fetchObservedState(entity, lastBridge);
      lastMonitor = monitor;
      if (!probe || !brokerProbe) {
        await delay(POLL_MS);
        continue;
      }

      const owner = brokerProbe.o || brokerProbe.owner || null;
      assertOk(!!owner, "broker view found controlled player without a pos owner", { brokerProbe, entity });

      if (!firstProbe) {
        firstProbe = { ...probe };
        firstBlock = probe.block || "?";
        currentBlock = firstBlock;
        firstOwner = owner;
        currentOwner = owner;
        direction = chooseDirection(probe, monitor, directionAttempt);
      } else {
        const step = distance(lastProbe, probe);
        path += step;
        if (firstSeamAt) postSeamPath += step;
      }

      const block = probe.block || "?";
      blocks.add(block);
      owners.add(owner);
      if (currentBlock && block !== currentBlock) {
        blockChanges++;
        currentBlock = block;
      }
      if (currentOwner && owner !== currentOwner) {
        ownerChanges++;
        currentOwner = owner;
        if (!firstSeamAt) {
          firstSeamAt = Date.now();
          postSeamPath = 0;
        }
      }

      const now = Date.now();
      if (now - lastCommandAt >= COMMAND_MS) {
        const expectedOwner = owner;
        const ack = await broker.command(entity, direction.target, ++seq);
        commandResponses++;
        lastAck = ack;
        assertOk(ack.success !== false, "broker CommandRequest returned failure", {
          entity,
          request_id: ack.request_id,
          expectedOwner,
          ack,
        });
        assertOk(ackAccepted(ack), "command bridge did not accept broker-routed command", {
          entity,
          expectedOwner,
          ack,
        });
        const ownerAfterAck = ackOwner(ack);
        let acceptedOwner = ownerAfterAck === expectedOwner;
        if (!acceptedOwner && ownerAfterAck) {
          const freshView = await fetchJson("broker view", BROKER_VIEW_URL);
          const freshProbe = findBrokerProbe(freshView, entity);
          const freshOwner = freshProbe && (freshProbe.o || freshProbe.owner || null);
          acceptedOwner = ownerAfterAck === freshOwner;
          if (acceptedOwner && currentOwner && freshOwner !== currentOwner) {
            ownerChanges++;
            currentOwner = freshOwner;
            owners.add(freshOwner);
            if (!firstSeamAt) {
              firstSeamAt = Date.now();
              postSeamPath = 0;
            }
          }
        }
        assertOk(acceptedOwner, "broker command was handled by a stale or wrong owner", {
          entity,
          expectedOwner,
          ownerAfterAck,
          ack,
        });
        commandOwnerMatches++;
        if (firstSeamAt && ownerAfterAck === currentOwner) postSeamCommandOk = true;
        lastCommandAt = now;
      }

      lastProbe = { ...probe };
      if (firstProbe && distance(firstProbe, probe) < 80 && Date.now() - startedAt > 5000) {
        directionAttempt++;
        direction = chooseDirection(probe, monitor, directionAttempt);
      }

      if (
        ownerChanges > 0
        && blockChanges > 0
        && postSeamCommandOk
        && path >= MIN_PATH
        && postSeamPath >= MIN_POST_SEAM_PATH
      ) {
        break;
      }

      await delay(POLL_MS);
    }

    assertOk(!!firstProbe, "controlled player never appeared in shard monitor", { entity, bridge: bridge0 });
    assertOk(!!firstOwner, "controlled player never appeared in broker mirror view", { entity, bridge: bridge0 });
    assertOk(commandResponses > 0, "gate received no broker CommandResponse frames", { entity });
    assertOk(path >= MIN_PATH, "controlled player moved too little through broker commands", { path, minPath: MIN_PATH, firstProbe, lastProbe });
    assertOk(blockChanges > 0, "controlled player did not cross a dynamic shard-monitor block", {
      firstBlock,
      currentBlock,
      blocks: [...blocks],
    });
    assertOk(ownerChanges > 0, "controlled player did not cross a Godworks broker ownership seam", {
      firstOwner,
      currentOwner,
      owners: [...owners],
    });
    assertOk(postSeamCommandOk, "no broker-routed command was accepted by the post-seam current owner", {
      firstOwner,
      currentOwner,
      lastAck,
    });
    assertOk(postSeamPath >= MIN_POST_SEAM_PATH, "controlled player did not keep moving after broker ownership seam", {
      postSeamPath,
      minPostSeamPath: MIN_POST_SEAM_PATH,
      lastProbe,
    });

    console.log(JSON.stringify({
      ok: true,
      broker: `${HOST}:${PORT}`,
      bridgeUrl: BRIDGE_URL,
      monitorUrl: MONITOR_URL,
      brokerViewUrl: BROKER_VIEW_URL,
      monitor: summarizeMonitor(lastMonitor),
      brokerCommand: {
        entity,
        socketId: bridge0.socketId,
        firstOwner,
        finalOwner: currentOwner,
        ownerChanges,
        owners: [...owners],
        firstBlock,
        finalBlock: currentBlock,
        blockChanges,
        blocks: [...blocks],
        path: Number(path.toFixed(3)),
        postSeamPath: Number(postSeamPath.toFixed(3)),
        commandResponses,
        commandOwnerMatches,
        bridgeCommandCount: lastBridge.commandCount,
        firstProbe: firstProbe ? { x: firstProbe.x, y: firstProbe.y, block: firstProbe.block } : null,
        finalProbe: lastProbe ? { x: lastProbe.x, y: lastProbe.y, block: lastProbe.block } : null,
        lastAckOwner: ackOwner(lastAck),
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
