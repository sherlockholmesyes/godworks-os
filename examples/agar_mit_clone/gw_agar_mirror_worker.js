// Mirror stock agar.io player cells into the Godworks broker as a one-region
// 2D-grid zone worker. The stock game remains authoritative for gameplay; this
// worker proves live clone entities can be projected into Godworks ownership
// without vendoring or rewriting the MIT game.
"use strict";

const net = require("net");
const http = require("http");
const { connectSpectator } = require("./_gw_spectator_tap");

const HOST = process.env.GW_HOST || "127.0.0.1";
const PORT = parseInt(process.env.GW_PORT || "7990", 10);
const REGION = process.env.GW_REGION || "Z0_0";
const WID = process.env.GW_WID || `d2-${REGION}`;
const CONNECT_TOKEN = process.env.GW_CONNECT_TOKEN || "";
const ARENA = parseFloat(process.env.GW_ARENA || "5000");
const GRID = (process.env.GW_GRID2D || "2x2").split("x").map(x => parseInt(x, 10));
const COLS = GRID[0] || 2;
const ROWS = GRID[1] || 2;
const HZ = parseFloat(process.env.GW_HZ || "20");
const COMMAND_URL = process.env.GW_AGAR_COMMAND_URL || "";

const known = new Set();
const pendingCreate = new Set();
const owned = new Map(); // eid -> authority_epoch
let lastPlayers = [];
let tick = 0;
let buf = Buffer.alloc(0);
let sock = null;
let requestSeq = 0;

function frame(obj) {
  const b = Buffer.from(JSON.stringify(obj), "utf8");
  const h = Buffer.alloc(4);
  h.writeUInt32BE(b.length, 0);
  return Buffer.concat([h, b]);
}

function send(obj) {
  if (sock && !sock.destroyed) sock.write(frame(obj));
}

function postJson(url, body) {
  return new Promise((resolve, reject) => {
    const parsed = new URL(url);
    const text = JSON.stringify(body);
    const req = http.request({
      hostname: parsed.hostname,
      port: parsed.port || 80,
      path: parsed.pathname,
      method: "POST",
      headers: {
        "content-type": "application/json",
        "content-length": Buffer.byteLength(text),
      },
      timeout: 3000,
    }, res => {
      let out = "";
      res.setEncoding("utf8");
      res.on("data", chunk => { out += chunk; });
      res.on("end", () => {
        let parsedBody = {};
        try { parsedBody = out ? JSON.parse(out) : {}; } catch (_) {}
        resolve({ status: res.statusCode, body: parsedBody });
      });
    });
    req.on("timeout", () => req.destroy(new Error(`timeout POST ${url}`)));
    req.on("error", reject);
    req.write(text);
    req.end();
  });
}

connectSpectator({
  onStatus: msg => console.error(`[${WID}] ${msg}`),
  onFrame: f => {
    lastPlayers = f.entities.filter(e => e.type === "player");
  },
});

connectBroker();

function connectBroker() {
  sock = net.connect(PORT, HOST, () => {
    const connect = { op: "WorkerConnect", worker_id: WID, region: REGION };
    if (CONNECT_TOKEN) connect.auth_token = CONNECT_TOKEN;
    send(connect);
    send({ op: "Interest", center: [ARENA / 2, ARENA / 2], radius: 1e9 });
    console.error(`[${WID}] connected broker ${HOST}:${PORT} region=${REGION}`);
  });

  sock.on("data", d => {
    buf = Buffer.concat([buf, d]);
    while (buf.length >= 4) {
      const n = buf.readUInt32BE(0);
      if (buf.length < 4 + n) break;
      let msg = null;
      try {
        msg = JSON.parse(buf.slice(4, 4 + n).toString("utf8"));
      } catch (_) {}
      buf = buf.slice(4 + n);
      if (msg) handleBroker(msg);
    }
  });

  sock.on("error", e => console.error(`[${WID}] broker error ${e.message}`));
  sock.on("close", () => {
    console.error(`[${WID}] broker closed`);
    setTimeout(connectBroker, 1000);
  });
}

function handleBroker(msg) {
  if (msg.op === "AddEntity") {
    known.add(msg.entity);
    pendingCreate.delete(msg.entity);
  } else if (msg.op === "RemoveEntity") {
    known.delete(msg.entity);
    pendingCreate.delete(msg.entity);
    owned.delete(msg.entity);
  } else if (msg.op === "AuthorityChange" && (!msg.comp || msg.comp === "pos")) {
    known.add(msg.entity);
    pendingCreate.delete(msg.entity);
    if (msg.authoritative) {
      owned.set(msg.entity, msg.authority_epoch || 1);
      console.error(`[${WID}] ADOPT ${msg.entity} epoch=${msg.authority_epoch || 1}`);
    } else {
      owned.delete(msg.entity);
      console.error(`[${WID}] LOSE ${msg.entity}`);
    }
  } else if (msg.op === "UpdateRejected") {
    const reason = msg.reason || "";
    if (msg.entity && reason.includes("not authoritative")) {
      owned.delete(msg.entity);
    } else if (msg.entity && msg.comp === "pos") {
      const m = /current=(\d+)/.exec(reason);
      if (m) owned.set(msg.entity, parseInt(m[1], 10));
    }
    console.error(`[${WID}] rejected entity=${msg.entity || ""} comp=${msg.comp || ""} reason=${reason}`);
  } else if (msg.op === "CreateEntityResponse") {
    pendingCreate.delete(msg.entity);
    if (msg.success) {
      known.add(msg.entity);
      console.error(`[${WID}] CREATE ok ${msg.entity}`);
    } else {
      known.delete(msg.entity);
      console.error(`[${WID}] CREATE rejected ${msg.entity} reason=${msg.reason || "exists-or-unknown"}`);
    }
  } else if (msg.op === "CommandRequest") {
    handleCommandRequest(msg);
  }
}

function handleCommandRequest(msg) {
  const requestId = msg.request_id || "";
  const entity = msg.entity || "";
  if (!owned.has(entity)) {
    send({
      op: "CommandResponse",
      request_id: requestId,
      success: false,
      reason: "worker received command for entity it does not own",
      payload: { handled_by: WID, entity, owner_current: false },
    });
    return;
  }
  if (!COMMAND_URL) {
    send({
      op: "CommandResponse",
      request_id: requestId,
      success: false,
      reason: "GW_AGAR_COMMAND_URL is not configured",
      payload: { handled_by: WID, entity, owner_current: true },
    });
    return;
  }
  postJson(`${COMMAND_URL.replace(/\/$/, "")}/input`, {
    owner: WID,
    region: REGION,
    entity,
    request_id: requestId,
    command: msg.command,
    payload: msg.payload,
  }).then(reply => {
    const ok = reply.status >= 200 && reply.status < 300 && reply.body && reply.body.ok;
    send({
      op: "CommandResponse",
      request_id: requestId,
      success: Boolean(ok),
      reason: ok ? null : (reply.body && (reply.body.reason || reply.body.error)) || `command bridge HTTP ${reply.status}`,
      payload: {
        accepted: Boolean(ok),
        handled_by: WID,
        owner: WID,
        region: REGION,
        entity,
        owner_current: true,
        bridge: reply.body || null,
      },
    });
  }).catch(err => {
    send({
      op: "CommandResponse",
      request_id: requestId,
      success: false,
      reason: err.message,
      payload: { handled_by: WID, entity, owner_current: true },
    });
  });
}

setInterval(() => {
  for (const e of lastPlayers) {
    const targetRegion = regionFor(e.x, e.y);
    const pos = [e.x, e.y];
    if (ownsRegion(targetRegion) && !known.has(e.id) && !pendingCreate.has(e.id)) {
      pendingCreate.add(e.id);
      send({
        op: "CreateEntity",
        request_id: `${WID}-${++requestSeq}`,
        entity: e.id,
        region: targetRegion,
        components: {
          pos,
          vel: [0, 0],
          mass: e.mass || 10,
        },
      });
    }

    if (owned.has(e.id)) {
      const epoch = owned.get(e.id) || 1;
      send({ op: "UpdateComponent", entity: e.id, comp: "pos", value: pos, authority_epoch: epoch });
      // `mass` is server-arbitrated and currently keeps epoch 1 while the physics-island
      // pos authority epoch bumps during handoff. Do not reuse the pos epoch here.
      send({ op: "UpdateComponent", entity: e.id, comp: "mass", value: e.mass || 10, authority_epoch: 1 });
    }
  }

  if (tick % Math.max(1, Math.floor(HZ)) === 0) {
    send({ op: "Heartbeat", worker_id: WID, load: owned.size });
    console.error(`[${WID}] tick=${tick} known=${known.size} pending=${pendingCreate.size} owned=${owned.size} stock_players=${lastPlayers.length}`);
  }
  tick++;
}, 1000 / HZ);

function regionFor(x, y) {
  const col = clamp(Math.floor((x / ARENA) * COLS), 0, COLS - 1);
  const row = clamp(Math.floor((y / ARENA) * ROWS), 0, ROWS - 1);
  return `Z${col}_${row}`;
}

function ownsRegion(region) {
  return region === REGION;
}

function clamp(v, lo, hi) {
  return Math.max(lo, Math.min(hi, v));
}
