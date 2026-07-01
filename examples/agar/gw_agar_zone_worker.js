// Godworks agar.io zone worker.
//
// One process claims one region/cell over one WorkerConnect connection. The
// runner starts a pool of these workers for 1D or 2D topologies, which avoids
// the old broken harness shape of sending multiple WorkerConnect frames over a
// single socket.
const net = require("net");

const HOST = process.env.GW_HOST || "127.0.0.1";
const PORT = parseInt(process.env.GW_PORT || "7777", 10);
const REGION = process.env.GW_REGION || "W";
const WID = process.env.GW_WID || `agar-${REGION}`;
const TOKEN = process.env.GW_CONNECT_TOKEN || process.env.GW_AUTH_TOKEN || "";
const HZ = parseFloat(process.env.GW_HZ || "20");
const SPAWN = parseInt(process.env.GW_SPAWN || "10", 10);
const FOOD = parseInt(process.env.GW_FOOD || "35", 10);
const SPEED = parseFloat(process.env.GW_SPEED || "8");
const RADIUS_BASE = parseFloat(process.env.GW_RADIUS || "0.55");
const WORLD = parseWorld(process.env.GW_WORLD || "0,120,0,120");
const GRID = process.env.GW_GRID || process.env.GW_GRID2D || "";
const ARENA = parseArena(process.env.GW_ARENA || `${WORLD[1] - WORLD[0]},${WORLD[3] - WORLD[2]}`);
const BOX = parseBox(process.env.GW_BOX) || boxForRegion(REGION, GRID, ARENA, WORLD);
const PROTECT_PLAYERS = process.env.GW_AGAR_PROTECT_PLAYERS !== "0";

const view = new Map();
const owned = new Map();
let buf = Buffer.alloc(0);
let tick = 0;

function parseWorld(spec) {
  const v = spec.split(",").map(Number);
  return v.length === 4 && v.every(Number.isFinite) ? v : [0, 120, 0, 120];
}

function parseArena(spec) {
  const v = spec.split(",").map(Number).filter(Number.isFinite);
  return [v[0] || WORLD[1] - WORLD[0], v[1] || v[0] || WORLD[3] - WORLD[2]];
}

function parseBox(spec) {
  if (!spec) return null;
  const v = spec.split(",").map(Number);
  return v.length === 4 && v.every(Number.isFinite) ? v : null;
}

function parseGrid(spec) {
  const m = /^(\d+)x(\d+)$/.exec(spec || "");
  if (!m) return null;
  return [parseInt(m[1], 10), parseInt(m[2], 10)];
}

function boxForRegion(region, gridSpec, arena, world) {
  const grid = parseGrid(gridSpec);
  const m = /^Z(\d+)_(\d+)$/.exec(region);
  if (grid && m) {
    const cols = grid[0], rows = grid[1];
    const cx = parseInt(m[1], 10), cy = parseInt(m[2], 10);
    const cw = arena[0] / cols, ch = arena[1] / rows;
    const pad = Math.max(0.5, Math.min(cw, ch) * 0.08);
    return [cx * cw + pad, (cx + 1) * cw - pad, cy * ch + pad, (cy + 1) * ch - pad];
  }
  if (region === "W") return [world[0] + 1, (world[0] + world[1]) / 2 - 2, world[2] + 1, world[3] - 1];
  if (region === "E") return [(world[0] + world[1]) / 2 + 2, world[1] - 1, world[2] + 1, world[3] - 1];
  return [world[0] + 1, world[1] - 1, world[2] + 1, world[3] - 1];
}

function frame(obj) {
  const body = Buffer.from(JSON.stringify(obj), "utf8");
  const head = Buffer.alloc(4);
  head.writeUInt32BE(body.length, 0);
  return Buffer.concat([head, body]);
}

function send(sock, obj) {
  if (!sock.destroyed) sock.write(frame(obj));
}

function rnd(min, max) {
  return min + Math.random() * (max - min);
}

function isProtectedPlayer(eid, entity) {
  return PROTECT_PLAYERS && ((entity && entity.type === "player") || /^P-\d+$/.test(String(eid)));
}

function newComponents(pos, vel, mass, type) {
  return {
    pos,
    vel,
    mass,
    type
  };
}

function createEntity(sock, eid, pos, vel, mass, type) {
  view.set(eid, { pos: pos.slice(), vel: vel.slice(), mass, type });
  send(sock, {
    op: "CreateEntity",
    request_id: `create-${eid}`,
    entity: eid,
    region: REGION,
    components: newComponents(pos, vel, mass, type)
  });
}

function componentName(f) {
  return f.comp || f.component || "";
}

function componentValue(f) {
  return Object.prototype.hasOwnProperty.call(f, "value") ? f.value : f.fields && f.fields.value;
}

function applyComponent(eid, comp, value) {
  const e = view.get(eid) || { pos: [0, 0], vel: [0, 0], mass: 1, type: "cell" };
  if (comp === "pos" && Array.isArray(value)) e.pos = value;
  else if (comp === "vel" && Array.isArray(value)) e.vel = value;
  else if (comp === "mass" && Number.isFinite(Number(value))) e.mass = Number(value);
  else if (comp === "type" && typeof value === "string") e.type = value;
  view.set(eid, e);
  const o = owned.get(eid);
  if (o) {
    if (comp === "mass" && Number.isFinite(Number(value))) o.mass = Number(value);
    if (comp === "type" && typeof value === "string") o.type = value;
  }
}

const sock = net.connect(PORT, HOST, () => {
  const connect = { op: "WorkerConnect", worker_id: WID, region: REGION };
  if (TOKEN) connect.auth_token = TOKEN;
  send(sock, connect);
  send(sock, { op: "Interest", center: [(WORLD[0] + WORLD[1]) / 2, (WORLD[2] + WORLD[3]) / 2], radius: 1e9 });
  console.error(`[agar ${REGION}] connected ${HOST}:${PORT} box=${BOX.map(n => n.toFixed(1)).join(",")}`);

  for (let i = 0; i < SPAWN; i++) {
    const eid = `${REGION}-cell-${i}`;
    const pos = [rnd(BOX[0], BOX[1]), rnd(BOX[2], BOX[3])];
    const a = Math.random() * Math.PI * 2;
    createEntity(sock, eid, pos, [Math.cos(a) * SPEED, Math.sin(a) * SPEED], 2 + Math.random() * 4, "cell");
  }
  for (let i = 0; i < FOOD; i++) {
    const eid = `${REGION}-food-${i}`;
    createEntity(sock, eid, [rnd(BOX[0], BOX[1]), rnd(BOX[2], BOX[3])], [0, 0], 1, "food");
  }
});

sock.on("data", d => {
  buf = Buffer.concat([buf, d]);
  while (buf.length >= 4) {
    const n = buf.readUInt32BE(0);
    if (buf.length < 4 + n) break;
    const body = buf.slice(4, 4 + n);
    buf = buf.slice(4 + n);
    let f;
    try { f = JSON.parse(body.toString("utf8")); } catch (_) { continue; }

    if (f.op === "AuthReject") {
      console.error(`[agar ${REGION}] auth rejected: ${f.reason || f.error || "unknown"}`);
      process.exit(2);
    } else if (f.op === "AddEntity") {
      const c = f.components || {};
      const entityState = {
        pos: Array.isArray(c.pos) ? c.pos : [0, 0],
        vel: Array.isArray(c.vel) ? c.vel : [0, 0],
        mass: Number.isFinite(Number(c.mass)) ? Number(c.mass) : 1,
        type: typeof c.type === "string" ? c.type : (String(f.entity).includes("-food-") ? "food" : "cell")
      };
      view.set(f.entity, entityState);
      const existingOwned = owned.get(f.entity);
      if (existingOwned) {
        owned.set(f.entity, {
          pos: entityState.pos.slice(),
          vel: entityState.vel.slice(),
          mass: entityState.mass,
          type: entityState.type,
          target: existingOwned.target,
          epoch: existingOwned.epoch,
          hydrated: true
        });
      }
    } else if (f.op === "ComponentUpdate") {
      applyComponent(f.entity, componentName(f), componentValue(f));
    } else if (f.op === "RemoveEntity") {
      view.delete(f.entity);
      owned.delete(f.entity);
    } else if (f.op === "AuthorityChange") {
      const comp = componentName(f);
      if (comp && comp !== "pos") continue;
      if (f.authoritative) {
        const v = view.get(f.entity) || { pos: [0, 0], vel: [SPEED, 0], mass: 1, type: "cell" };
        const prev = owned.get(f.entity);
        owned.set(f.entity, {
          pos: (prev && prev.pos || v.pos || [0, 0]).slice(),
          vel: (prev && prev.vel || v.vel || [SPEED, 0]).slice(),
          mass: Number(prev && prev.mass || v.mass || 1),
          type: prev && prev.type || v.type || (String(f.entity).includes("-food-") ? "food" : "cell"),
          target: prev && prev.target,
          epoch: f.authority_epoch || (prev && prev.epoch) || 1,
          hydrated: !!view.get(f.entity) || !!(prev && prev.hydrated)
        });
      } else {
        owned.delete(f.entity);
      }
    } else if (f.op === "CommandRequest") {
      const o = owned.get(f.entity);
      if (o && f.command === "set_target" && Array.isArray(f.payload)) {
        o.target = [Number(f.payload[0]), Number(f.payload[1])];
        o.type = "player";
        send(sock, {
          op: "CommandResponse",
          request_id: f.request_id,
          success: true,
          payload: { accepted: true, entity: f.entity, owner: WID, region: REGION }
        });
      } else if (f.request_id) {
        send(sock, {
          op: "CommandResponse",
          request_id: f.request_id,
          success: false,
          payload: { accepted: false, entity: f.entity, owner: WID, region: REGION, reason: "not_owned_or_bad_command" }
        });
      }
    } else if (f.op === "UpdateRejected") {
      console.error(`[agar ${REGION}] rejected entity=${f.entity || "-"} comp=${f.comp || f.component || "-"} reason=${f.reason || f.error || "unknown"}`);
      const comp = f.comp || f.component || "";
      const reason = String(f.reason || "");
      if (comp === "pos" && (reason.includes("not authoritative") || reason.includes("entity not found"))) {
        owned.delete(f.entity);
      }
    }
  }
});

setInterval(() => {
  const dt = 1 / HZ;
  const posUpdates = [];

  for (const [eid, c] of Array.from(owned.entries())) {
    if (c.type === "food") continue;
    if (!c.hydrated) continue;
    let [x, y] = c.pos;
    let [vx, vy] = c.vel;
    const r = RADIUS_BASE * Math.sqrt(Math.max(1, c.mass));
    const speed = SPEED / Math.sqrt(Math.max(1, c.mass));

    if (c.target) {
      const dx = c.target[0] - x, dy = c.target[1] - y;
      const d = Math.hypot(dx, dy);
      if (d > 0.2) {
        vx = dx / d * speed;
        vy = dy / d * speed;
      } else {
        vx = 0;
        vy = 0;
      }
    } else {
      let best = null, bd = Infinity;
      for (const [oid, o] of view.entries()) {
        if (isProtectedPlayer(oid, o)) continue;
        if (oid === eid || !o.pos || (o.mass || 1) >= c.mass) continue;
        const d = Math.hypot(o.pos[0] - x, o.pos[1] - y);
        if (d < bd) { bd = d; best = o; }
      }
      if (best) {
        const dx = best.pos[0] - x, dy = best.pos[1] - y;
        const d = Math.hypot(dx, dy) || 1;
        vx = dx / d * speed;
        vy = dy / d * speed;
      }
    }

    x += vx * dt;
    y += vy * dt;
    if (x < WORLD[0]) { x = WORLD[0]; vx = Math.abs(vx); }
    if (x > WORLD[1]) { x = WORLD[1]; vx = -Math.abs(vx); }
    if (y < WORLD[2]) { y = WORLD[2]; vy = Math.abs(vy); }
    if (y > WORLD[3]) { y = WORLD[3]; vy = -Math.abs(vy); }

    c.pos = [x, y];
    c.vel = [vx, vy];
    view.set(eid, { pos: c.pos.slice(), vel: c.vel.slice(), mass: c.mass, type: c.type });

    for (const [oid, o] of Array.from(owned.entries())) {
      if (!o.hydrated) continue;
      if (isProtectedPlayer(oid, o)) continue;
      if (oid === eid || !o.pos || o.type !== "food" && c.mass <= o.mass * 1.1) continue;
      if (Math.hypot(o.pos[0] - x, o.pos[1] - y) < r) {
        c.mass += o.mass || 1;
        owned.delete(oid);
        view.delete(oid);
        send(sock, { op: "DeleteEntity", request_id: `eat-${tick}-${oid}`, entity: oid, authority_epoch: o.epoch || c.epoch });
      }
    }

    posUpdates.push([eid, [x, y], c.epoch]);
  }

  if (posUpdates.length) send(sock, { op: "BatchUpdate", comp: "pos", updates: posUpdates });
  if (tick % Math.max(1, Math.floor(HZ / 2)) === 0) send(sock, { op: "Heartbeat", worker_id: WID });
  if (tick % Math.max(1, Math.floor(HZ * 2)) === 0) {
    console.error(`[agar ${REGION}] tick=${tick} owned=${owned.size} view=${view.size}`);
  }
  tick++;
}, 1000 / HZ);

sock.on("error", e => console.error(`[agar ${REGION}] socket error: ${e.message}`));
sock.on("close", () => {
  console.error(`[agar ${REGION}] broker closed`);
  process.exit(0);
});
