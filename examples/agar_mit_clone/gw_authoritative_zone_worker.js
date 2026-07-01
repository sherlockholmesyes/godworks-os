"use strict";

const http = require("http");
const net = require("net");

const HOST = process.env.GW_HOST || "127.0.0.1";
const PORT = parseInt(process.env.GW_PORT || "7990", 10);
const REGION = process.env.GW_REGION || "Z0_0";
const WID = process.env.GW_WID || `auth-${REGION}`;
const CONNECT_TOKEN = process.env.GW_CONNECT_TOKEN || "";
const CONTROL_PORT = parseInt(process.env.GW_CONTROL_PORT || "0", 10);
const HZ = parseFloat(process.env.GW_HZ || "20");
const FOOD = parseInt(process.env.GW_FOOD || "64", 10);
const BOX = (process.env.GW_BOX || "0,1250,0,1250").split(",").map(Number);
const WORLD = (process.env.GW_WORLD || "0,5000,0,5000").split(",").map(Number);
const SPEED = parseFloat(process.env.GW_SPEED || "420");

const view = new Map();
const owned = new Map();
let sock = null;
let buf = Buffer.alloc(0);
let tick = 0;
let requestSeq = 0;

function frame(obj) {
  const body = Buffer.from(JSON.stringify(obj), "utf8");
  const header = Buffer.alloc(4);
  header.writeUInt32BE(body.length, 0);
  return Buffer.concat([header, body]);
}

function send(obj) {
  if (sock && !sock.destroyed) sock.write(frame(obj));
}

function components(entity) {
  return {
    pos: entity.pos,
    vel: entity.vel || [0, 0],
    mass: entity.mass,
    kind: entity.kind,
    name: entity.name || "",
    hue: entity.hue || 100,
  };
}

function radius(mass) {
  return 4 + Math.sqrt(Math.max(1, mass)) * 6;
}

function randomInBox() {
  return [
    BOX[0] + Math.random() * Math.max(1, BOX[1] - BOX[0]),
    BOX[2] + Math.random() * Math.max(1, BOX[3] - BOX[2]),
  ];
}

function clampPos(pos) {
  return [
    Math.max(WORLD[0], Math.min(WORLD[1], pos[0])),
    Math.max(WORLD[2], Math.min(WORLD[3], pos[1])),
  ];
}

function entityFromComponents(entity, c) {
  return {
    id: entity,
    pos: Array.isArray(c.pos) ? c.pos.slice() : [0, 0],
    vel: Array.isArray(c.vel) ? c.vel.slice() : [0, 0],
    mass: Number.isFinite(Number(c.mass)) ? Number(c.mass) : 1,
    kind: String(c.kind || "cell"),
    name: String(c.name || ""),
    hue: Number.isFinite(Number(c.hue)) ? Number(c.hue) : 100,
  };
}

function createEntity(entity) {
  view.set(entity.id, { ...entity, pos: entity.pos.slice(), vel: (entity.vel || [0, 0]).slice() });
  send({
    op: "CreateEntity",
    request_id: `create-${WID}-${requestSeq++}`,
    entity: entity.id,
    region: REGION,
    components: components(entity),
  });
}

function deleteOwned(entityId, epoch) {
  owned.delete(entityId);
  view.delete(entityId);
  send({
    op: "DeleteEntity",
    request_id: `delete-${WID}-${requestSeq++}`,
    entity: entityId,
    authority_epoch: epoch,
  });
}

function deleteIfOwned(entityId) {
  const entity = owned.get(entityId);
  if (!entity) return false;
  deleteOwned(entityId, entity.epoch || 1);
  return true;
}

function parseTarget(payload) {
  const raw = payload && Object.prototype.hasOwnProperty.call(payload, "target")
    ? payload.target
    : payload;
  if (Array.isArray(raw)) return [Number(raw[0]), Number(raw[1])];
  return [Number(raw && raw.x), Number(raw && raw.y)];
}

function connectBroker() {
  sock = net.connect(PORT, HOST, () => {
    const connect = { op: "WorkerConnect", worker_id: WID, region: REGION };
    if (CONNECT_TOKEN) connect.auth_token = CONNECT_TOKEN;
    send(connect);
    send({ op: "Interest", center: [(WORLD[0] + WORLD[1]) / 2, (WORLD[2] + WORLD[3]) / 2], radius: 1e9 });
    console.error(`[${WID}] connected broker ${HOST}:${PORT} region=${REGION} box=${BOX.join(",")}`);
    for (let i = 0; i < FOOD; i++) {
      const pos = randomInBox();
      createEntity({
        id: `${WID}-food-${i}`,
        pos,
        vel: [0, 0],
        mass: 1,
        kind: "food",
        name: "",
        hue: Math.floor(Math.random() * 360),
      });
    }
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
    view.set(msg.entity, entityFromComponents(msg.entity, msg.components || {}));
  } else if (msg.op === "ComponentUpdate") {
    const current = view.get(msg.entity) || { id: msg.entity, pos: [0, 0], vel: [0, 0], mass: 1, kind: "cell", name: "", hue: 100 };
    if (msg.comp === "pos" && Array.isArray(msg.value)) current.pos = msg.value.slice();
    else if (msg.comp === "vel" && Array.isArray(msg.value)) current.vel = msg.value.slice();
    else if (msg.comp === "mass") current.mass = Number(msg.value) || current.mass;
    else if (msg.comp === "kind") current.kind = String(msg.value || current.kind);
    else if (msg.comp === "name") current.name = String(msg.value || "");
    else if (msg.comp === "hue") current.hue = Number(msg.value) || current.hue;
    view.set(msg.entity, current);
    if (owned.has(msg.entity)) Object.assign(owned.get(msg.entity), current);
  } else if (msg.op === "RemoveEntity") {
    view.delete(msg.entity);
    owned.delete(msg.entity);
  } else if (msg.op === "AuthorityChange" && (!msg.comp || msg.comp === "pos")) {
    if (msg.authoritative) {
      const base = view.get(msg.entity) || { id: msg.entity, pos: [0, 0], vel: [0, 0], mass: 1, kind: "cell", name: "", hue: 100 };
      owned.set(msg.entity, {
        ...base,
        pos: base.pos.slice(),
        vel: (base.vel || [0, 0]).slice(),
        epoch: msg.authority_epoch || 1,
      });
      console.error(`[${WID}] ADOPT ${msg.entity} epoch=${msg.authority_epoch || 1}`);
    } else {
      owned.delete(msg.entity);
      console.error(`[${WID}] LOSE ${msg.entity}`);
    }
  } else if (msg.op === "CommandRequest") {
    const entity = owned.get(msg.entity);
    if (!entity) {
      send({
        op: "CommandResponse",
        request_id: msg.request_id || "",
        entity: msg.entity || "",
        success: false,
        reason: "entity not owned by this Godworks agar worker",
        payload: { handled_by: WID, owner_current: false },
      });
      return;
    }
    if (msg.command === "set_target") {
      const target = parseTarget(msg.payload);
      if (Number.isFinite(target[0]) && Number.isFinite(target[1])) {
        entity.target = clampPos(target);
        send({
          op: "CommandResponse",
          request_id: msg.request_id || "",
          entity: msg.entity || "",
          success: true,
          reason: null,
          payload: { handled_by: WID, owner_current: true },
        });
      }
    }
  } else if (msg.op === "UpdateRejected") {
    console.error(`[${WID}] rejected entity=${msg.entity || ""} comp=${msg.comp || ""} reason=${msg.reason || ""}`);
  }
}

function tickOwned() {
  const dt = 1 / HZ;
  const posUpdates = [];

  for (const [entityId, entity] of Array.from(owned.entries())) {
    if (entity.kind !== "player") continue;
    let vx = 0;
    let vy = 0;
    if (entity.target) {
      const dx = entity.target[0] - entity.pos[0];
      const dy = entity.target[1] - entity.pos[1];
      const d = Math.hypot(dx, dy);
      if (d > 2) {
        const speed = SPEED / Math.sqrt(Math.max(1, entity.mass / 10));
        vx = (dx / d) * speed;
        vy = (dy / d) * speed;
      }
    }
    entity.vel = [vx, vy];
    entity.pos = clampPos([entity.pos[0] + vx * dt, entity.pos[1] + vy * dt]);

    const eatRadius = radius(entity.mass) * 0.75;
    for (const [otherId, other] of Array.from(owned.entries())) {
      if (otherId === entityId) continue;
      if (other.kind !== "food" && other.kind !== "player") continue;
      if (entity.mass <= other.mass * 1.1) continue;
      if (Math.hypot(other.pos[0] - entity.pos[0], other.pos[1] - entity.pos[1]) > eatRadius) continue;
      entity.mass += other.mass;
      entity.massDirty = true;
      deleteOwned(otherId, other.epoch || entity.epoch);
    }

    posUpdates.push([entityId, entity.pos, entity.epoch]);
    if (entity.massDirty) {
      entity.massDirty = false;
      send({ op: "UpdateComponent", entity: entityId, comp: "mass", value: entity.mass, authority_epoch: entity.epoch });
    }
  }

  if (posUpdates.length) send({ op: "BatchUpdate", comp: "pos", updates: posUpdates });
  if (tick % Math.max(1, Math.floor(HZ / 4)) === 0) send({ op: "Heartbeat", worker_id: WID });
  if (tick % Math.max(1, Math.floor(HZ * 2)) === 0) {
    console.error(`[${WID}] tick=${tick} owned=${owned.size} view=${view.size}`);
  }
  tick++;
}

function readJson(req) {
  return new Promise((resolve, reject) => {
    let body = "";
    req.setEncoding("utf8");
    req.on("data", chunk => {
      body += chunk;
      if (body.length > 64 * 1024) req.destroy(new Error("request body too large"));
    });
    req.on("end", () => {
      try { resolve(body ? JSON.parse(body) : {}); } catch (e) { reject(e); }
    });
    req.on("error", reject);
  });
}

function sendJson(res, code, body) {
  const text = JSON.stringify(body);
  res.writeHead(code, { "content-type": "application/json", "cache-control": "no-store" });
  res.end(text);
}

if (CONTROL_PORT > 0) {
  http.createServer(async (req, res) => {
    try {
      if (req.method === "POST" && req.url === "/spawn") {
        const body = await readJson(req);
        const entity = {
          id: String(body.entity || ""),
          pos: clampPos(Array.isArray(body.pos) ? body.pos.map(Number) : randomInBox()),
          vel: [0, 0],
          mass: Number.isFinite(Number(body.mass)) ? Number(body.mass) : 10,
          kind: String(body.kind || "player"),
          name: String(body.name || ""),
          hue: Number.isFinite(Number(body.hue)) ? Number(body.hue) : 100,
        };
        if (!entity.id) return sendJson(res, 400, { ok: false, reason: "entity is required" });
        createEntity(entity);
        return sendJson(res, 200, { ok: true, worker: WID, region: REGION, entity: entity.id, pos: entity.pos });
      }
      if (req.method === "POST" && req.url === "/despawn") {
        const body = await readJson(req);
        const entityId = String(body.entity || "");
        if (!entityId) return sendJson(res, 400, { ok: false, reason: "entity is required" });
        if (!deleteIfOwned(entityId)) {
          return sendJson(res, 409, { ok: false, reason: "entity is not owned by this worker", worker: WID, region: REGION, entity: entityId });
        }
        return sendJson(res, 200, { ok: true, worker: WID, region: REGION, entity: entityId });
      }
      if (req.method === "GET" && req.url === "/state") {
        return sendJson(res, 200, { ok: true, worker: WID, region: REGION, owned: owned.size, view: view.size });
      }
      sendJson(res, 404, { ok: false, reason: "not found" });
    } catch (e) {
      sendJson(res, 500, { ok: false, reason: e.message });
    }
  }).listen(CONTROL_PORT, "127.0.0.1", () => {
    console.error(`[${WID}] control http://127.0.0.1:${CONTROL_PORT}`);
  });
}

connectBroker();
setInterval(tickOwned, 1000 / HZ);
