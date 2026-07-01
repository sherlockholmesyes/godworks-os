// Captures the final live broker/client cut for the Agar restore agreement gate.
const fs = require("fs");
const http = require("http");

const BASE = process.env.GW_AGAR_URL || `http://127.0.0.1:${process.env.GW_HTTP || "8091"}`;
const OUT = process.env.GW_AGAR_RESTORE_EXPECT || "";

function req(path) {
  return new Promise((resolve, reject) => {
    const u = new URL(path, BASE);
    const r = http.request({ method: "GET", hostname: u.hostname, port: u.port, path: u.pathname + u.search }, res => {
      let body = "";
      res.on("data", d => body += d);
      res.on("end", () => {
        try { resolve(JSON.parse(body || "null")); } catch (err) { reject(err); }
      });
    });
    r.on("error", reject);
    r.end();
  });
}

async function main() {
  if (!OUT) throw new Error("GW_AGAR_RESTORE_EXPECT is required");
  const broker = await req("/state");
  const client = await req("/client-state");
  if (!Array.isArray(broker) || broker.length <= 0) throw new Error(`live broker state was empty or not an array: ${JSON.stringify(broker).slice(0, 256)}`);
  if (!Array.isArray(client) || client.length <= 0) throw new Error(`live client state was empty or not an array: ${JSON.stringify(client).slice(0, 256)}`);
  const cut = { broker_state: broker, client_state: client };
  fs.writeFileSync(OUT, JSON.stringify(cut, null, 2), "utf8");
  console.log(JSON.stringify({ ok: true, broker_entities: broker.length, client_entities: client.length, path: OUT }));
}

main().catch(err => {
  console.error(err && err.stack || String(err));
  process.exit(1);
});
