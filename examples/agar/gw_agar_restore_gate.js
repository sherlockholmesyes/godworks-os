// Restore agreement gate for the Godworks agar.io demo.
//
// The PowerShell runner captures the last live broker/client truth cut, stops
// the cluster, restarts the broker from the demo WAL, then this script queries
// the restored broker directly through InspectorQuery.
const fs = require("fs");
const net = require("net");

const BROKER_HOST = process.env.GW_HOST || "127.0.0.1";
const BROKER_PORT = parseInt(process.env.GW_PORT || "7777", 10);
const OBS_TOKEN = process.env.GW_OBS_TOKEN || "obs-token";
const EXPECT_PATH = process.env.GW_AGAR_RESTORE_EXPECT || "";
const EPSILON = parseFloat(process.env.GW_AGAR_RESTORE_EPSILON || "1.75");

function frame(obj) {
  const body = Buffer.from(JSON.stringify(obj), "utf8");
  const head = Buffer.alloc(4);
  head.writeUInt32BE(body.length, 0);
  return Buffer.concat([head, body]);
}

function send(sock, obj) {
  sock.write(frame(obj));
}

function dist(a, b) {
  if (!Array.isArray(a) || !Array.isArray(b)) return Infinity;
  return Math.hypot(Number(a[0]) - Number(b[0]), Number(a[1]) - Number(b[1]));
}

function ownerOfRestored(row) {
  const authority = row && row.authority || {};
  return authority.pos && authority.pos.owner || row.owner || row.region || "?";
}

function logicalOwner(owner) {
  if (typeof owner !== "string" || owner === "?") return owner;
  const agarWorker = /^agar-(Z\d+_\d+)$/.exec(owner);
  return agarWorker ? agarWorker[1] : owner;
}

function byId(rows, idKey) {
  const out = new Map();
  for (const row of rows || []) {
    const id = row && row[idKey];
    if (id) out.set(id, row);
  }
  return out;
}

function queryInspector() {
  return new Promise((resolve, reject) => {
    const frames = [];
    let buf = Buffer.alloc(0);
    const sock = net.connect(BROKER_PORT, BROKER_HOST, () => {
      send(sock, {
        op: "WorkerConnect",
        worker_id: "agar-restore-inspector",
        region: "OBS",
        attributes: ["observer", "inspector"],
        auth_token: OBS_TOKEN
      });
      send(sock, { op: "Interest", center: [60, 60], radius: 1e9 });
      setTimeout(() => send(sock, { op: "InspectorQuery", request_id: "agar-restore-inspect", max_entities: 10000 }), 120);
      setTimeout(() => {
        sock.destroy();
        const authReject = frames.find(f => f.op === "AuthReject");
        if (authReject) reject(new Error(`restore inspector auth rejected: ${JSON.stringify(authReject)}`));
        const frame = frames.find(f => f.op === "InspectorFrame" && f.request_id === "agar-restore-inspect");
        if (!frame) reject(new Error(`restored broker did not return InspectorFrame; frames=${JSON.stringify(frames.slice(0, 5))}`));
        else resolve(frame);
      }, 900);
    });
    sock.on("data", d => {
      buf = Buffer.concat([buf, d]);
      while (buf.length >= 4) {
        const n = buf.readUInt32BE(0);
        if (buf.length < 4 + n) break;
        const body = buf.slice(4, 4 + n);
        buf = buf.slice(4 + n);
        try { frames.push(JSON.parse(body.toString("utf8"))); } catch (_) {}
      }
    });
    sock.on("error", reject);
  });
}

async function main() {
  if (!EXPECT_PATH) throw new Error("GW_AGAR_RESTORE_EXPECT is required");
  const expected = JSON.parse(fs.readFileSync(EXPECT_PATH, "utf8"));
  const liveBroker = Array.isArray(expected.broker_state) ? expected.broker_state : [];
  const liveClient = Array.isArray(expected.client_state) ? expected.client_state : [];
  if (liveBroker.length <= 0) throw new Error("expected live broker state is empty");

  const restoredFrame = await queryInspector();
  const restoredEntities = Array.isArray(restoredFrame.entities) ? restoredFrame.entities : [];
  const restored = byId(restoredEntities, "entity");
  const failures = [];

  if (restoredEntities.length !== liveBroker.length) {
    failures.push(`restored entity count mismatch: restored=${restoredEntities.length} live=${liveBroker.length}`);
  }

  for (const live of liveBroker) {
    const id = live && live.e;
    if (!id) continue;
    const row = restored.get(id);
    if (!row) {
      failures.push(`restored broker missing live entity ${id}`);
      continue;
    }
    if (dist(row.pos, live.p) > EPSILON) {
      failures.push(`restored position mismatch for ${id}: restored=${JSON.stringify(row.pos)} live=${JSON.stringify(live.p)}`);
    }
    const owner = ownerOfRestored(row);
    if (live.o && live.o !== "?" && logicalOwner(owner) !== logicalOwner(live.o)) {
      failures.push(`restored logical owner mismatch for ${id}: restored=${owner} live=${live.o}`);
    }
  }

  const liveBrokerById = byId(liveBroker, "e");
  let clientCompared = 0;
  for (const client of liveClient) {
    const id = client && client.e;
    if (!id) continue;
    const live = liveBrokerById.get(id);
    if (!live) {
      failures.push(`client stream had entity missing from live broker cut: ${id}`);
      continue;
    }
    const row = restored.get(id);
    if (!row) {
      failures.push(`restored broker missing client-visible entity ${id}`);
      continue;
    }
    if (Array.isArray(client.p) && dist(row.pos, client.p) > EPSILON * 2) {
      failures.push(`restored broker disagrees with client-visible pos for ${id}: restored=${JSON.stringify(row.pos)} client=${JSON.stringify(client.p)}`);
    }
    clientCompared++;
  }
  if (liveClient.length > 0 && clientCompared <= 0) failures.push("no client-visible entities were compared against restored broker");

  const report = {
    ok: failures.length === 0,
    expected_broker_entities: liveBroker.length,
    expected_client_entities: liveClient.length,
    restored_entities: restoredEntities.length,
    client_compared: clientCompared,
    epsilon: EPSILON
  };

  if (failures.length) {
    report.failures = failures;
    console.error(JSON.stringify(report, null, 2));
    process.exit(1);
  }
  console.log(JSON.stringify(report));
}

main().catch(err => {
  console.error(err && err.stack || String(err));
  process.exit(1);
});
