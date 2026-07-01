// Godworks agar.io browser gateway.
//
// Two broker connections are intentional:
// - OBS + inspector claim reads InspectorFrame as the broker-truth oracle.
// - CLIENT claim maintains a non-privileged client stream for product truth.
// - ordinary worker claim acts as a trusted server adapter for spawn/commands.
const net = require("net");
const http = require("http");

const BHOST = process.env.GW_HOST || "127.0.0.1";
const BPORT = parseInt(process.env.GW_PORT || "7777", 10);
const HTTP_PORT = parseInt(process.env.GW_HTTP || "8091", 10);
const WORLD = parseWorld(process.env.GW_WORLD || "0,120,0,120");
const GRID = process.env.GW_GRID || process.env.GW_GRID2D || "";
const ARENA = parseArena(process.env.GW_ARENA || `${WORLD[1] - WORLD[0]},${WORLD[3] - WORLD[2]}`);
const OBS_TOKEN = process.env.GW_OBS_TOKEN || "obs-token";
const CLIENT_TOKEN = process.env.GW_CLIENT_TOKEN || "spawn-token";
const BROWSER_TOKEN = process.env.GW_BROWSER_TOKEN || "browser-token";

let snapshot = [];
let clientSnapshot = [];
let playerSeq = 0;
const massTable = new Map();
const ownerHistory = new Map();
const clientMassTable = new Map();
const pendingCommands = new Map();

function parseWorld(spec) {
  const v = spec.split(",").map(Number);
  return v.length === 4 && v.every(Number.isFinite) ? v : [0, 120, 0, 120];
}

function parseArena(spec) {
  const v = spec.split(",").map(Number).filter(Number.isFinite);
  return [v[0] || WORLD[1] - WORLD[0], v[1] || v[0] || WORLD[3] - WORLD[2]];
}

function parseGrid(spec) {
  const m = /^(\d+)x(\d+)$/.exec(spec || "");
  return m ? [parseInt(m[1], 10), parseInt(m[2], 10)] : null;
}

function regionForPos(pos) {
  const grid = parseGrid(GRID);
  if (grid) {
    const cw = ARENA[0] / grid[0], ch = ARENA[1] / grid[1];
    const cx = Math.max(0, Math.min(grid[0] - 1, Math.floor(pos[0] / cw)));
    const cy = Math.max(0, Math.min(grid[1] - 1, Math.floor(pos[1] / ch)));
    return `Z${cx}_${cy}`;
  }
  return pos[0] < (WORLD[0] + WORLD[1]) / 2 ? "W" : "E";
}

function frame(obj) {
  const body = Buffer.from(JSON.stringify(obj), "utf8");
  const head = Buffer.alloc(4);
  head.writeUInt32BE(body.length, 0);
  return Buffer.concat([head, body]);
}

function send(sock, obj) {
  if (sock && !sock.destroyed) sock.write(frame(obj));
}

function connectPeer(name, region, attributes, token, onFrame) {
  let buf = Buffer.alloc(0);
  const sock = net.connect(BPORT, BHOST, () => {
    const connect = { op: "WorkerConnect", worker_id: name, region, attributes };
    if (token) connect.auth_token = token;
    send(sock, connect);
    send(sock, { op: "Interest", center: [(WORLD[0] + WORLD[1]) / 2, (WORLD[2] + WORLD[3]) / 2], radius: 1e9 });
    console.error(`[agar-gateway] ${name} connected as ${region}`);
  });
  sock.on("data", d => {
    buf = Buffer.concat([buf, d]);
    while (buf.length >= 4) {
      const n = buf.readUInt32BE(0);
      if (buf.length < 4 + n) break;
      const body = buf.slice(4, 4 + n);
      buf = buf.slice(4 + n);
      try { onFrame(JSON.parse(body.toString("utf8")), sock); } catch (_) {}
    }
  });
  sock.on("error", e => console.error(`[agar-gateway] ${name} socket error: ${e.message}`));
  sock.on("close", () => console.error(`[agar-gateway] ${name} broker connection closed`));
  return sock;
}

const obsSock = connectPeer("agar-observer", "OBS", ["observer", "inspector"], OBS_TOKEN, f => {
  if (f.op === "AuthReject") {
    console.error(`[agar-gateway] observer auth rejected: ${f.reason || f.error || "unknown"}`);
    return;
  }
  if (f.op === "ComponentUpdate" && (f.comp || f.component) === "mass") {
    massTable.set(f.entity, Number(f.value));
  } else if (f.op === "AddEntity") {
    const c = f.components || {};
    if (Number.isFinite(Number(c.mass))) massTable.set(f.entity, Number(c.mass));
  } else if (f.op === "RemoveEntity") {
    massTable.delete(f.entity);
    ownerHistory.delete(f.entity);
  } else if (f.op === "InspectorFrame") {
    snapshot = (f.entities || []).map(e => {
      const auth = e.authority || {};
      const owner = (auth.pos && auth.pos.owner) || e.owner || e.region || "?";
      const prior = ownerHistory.get(e.entity);
      const changes = prior && prior.last !== owner ? prior.changes + 1 : (prior ? prior.changes : 0);
      ownerHistory.set(e.entity, { last: owner, changes });
      return {
        e: e.entity,
        p: e.pos,
        o: owner,
        r: e.region,
        m: Number(massTable.get(e.entity) || e.mass || 1),
        owner_changes: changes
      };
    });
  }
});

const writeSock = connectPeer("agar-spawner", "AGAR_SPAWNER", [], CLIENT_TOKEN, f => {
  if (f.op === "AuthReject") console.error(`[agar-gateway] spawner auth rejected: ${f.reason || f.error || "unknown"}`);
  if (f.op === "CommandResponse" && f.request_id) {
    const pending = pendingCommands.get(f.request_id);
    if (pending) {
      clearTimeout(pending.timer);
      pendingCommands.delete(f.request_id);
      pending.res.writeHead(200, { "content-type": "application/json" });
      pending.res.end(JSON.stringify({ ok: true, response: f }));
    }
  }
});

const clientSock = connectPeer("agar-browser-client", "CLIENT", ["role.client"], BROWSER_TOKEN, f => {
  if (f.op === "AuthReject") {
    console.error(`[agar-gateway] browser-client auth rejected: ${f.reason || f.error || "unknown"}`);
    return;
  }
  if (f.op === "AddEntity") {
    const c = f.components || {};
    const existing = clientSnapshot.find(v => v.e === f.entity);
    const next = {
      e: f.entity,
      p: Array.isArray(c.pos) ? c.pos : existing && existing.p,
      m: Number.isFinite(Number(c.mass)) ? Number(c.mass) : (existing && existing.m || 1),
      type: typeof c.type === "string" ? c.type : (existing && existing.type || "entity")
    };
    clientMassTable.set(f.entity, next.m);
    clientSnapshot = clientSnapshot.filter(v => v.e !== f.entity).concat([next]);
  } else if (f.op === "ComponentUpdate") {
    const comp = f.comp || f.component;
    const value = Object.prototype.hasOwnProperty.call(f, "value") ? f.value : f.fields && f.fields.value;
    const existing = clientSnapshot.find(v => v.e === f.entity) || { e: f.entity };
    if (comp === "pos" && Array.isArray(value)) existing.p = value;
    if (comp === "mass" && Number.isFinite(Number(value))) {
      existing.m = Number(value);
      clientMassTable.set(f.entity, existing.m);
    }
    if (comp === "type" && typeof value === "string") existing.type = value;
    clientSnapshot = clientSnapshot.filter(v => v.e !== f.entity).concat([existing]);
  } else if (f.op === "RemoveEntity") {
    clientMassTable.delete(f.entity);
    clientSnapshot = clientSnapshot.filter(v => v.e !== f.entity);
  }
});

setInterval(() => {
  send(obsSock, { op: "InspectorQuery", request_id: `inspect-${Date.now()}`, max_entities: 10000 });
}, 80);

function html() {
  return `<!doctype html><html><head><meta charset="utf-8"><title>Godworks agar.io</title>
<style>
body{margin:0;background:#08090f;color:#9f9;font:12px monospace;overflow:hidden}
#hud{position:fixed;left:8px;top:8px;white-space:pre;text-shadow:0 0 4px #000}
#btn{position:fixed;right:10px;top:10px;background:#16c94a;color:#041;border:0;border-radius:6px;padding:10px 14px;font:bold 13px sans-serif}
</style></head><body><canvas id="c"></canvas><div id="hud"></div><button id="btn">Herd</button><script>
const WORLD=${JSON.stringify(WORLD)}, GRID=${JSON.stringify(GRID)}, ARENA=${JSON.stringify(ARENA)};
const cv=document.getElementById('c'),cx=cv.getContext('2d'),hud=document.getElementById('hud'),btn=document.getElementById('btn');
function rz(){cv.width=innerWidth;cv.height=innerHeight;}addEventListener('resize',rz);rz();
let snap=[], myId=null, mx=innerWidth/2, my=innerHeight/2, cam=[(WORLD[0]+WORLD[1])/2,(WORLD[2]+WORLD[3])/2], scale=1;
const colors={}, pal=['#48f','#f84','#4d6','#e5d','#dd4','#4dd','#aaa','#f55','#5af','#af5'];let pi=0;
function col(o){return colors[o]||(colors[o]=pal[pi++%pal.length]);}
async function join(){const r=await fetch('/join',{method:'POST'});const j=await r.json();myId=j.id;}
join().catch(()=>{});
addEventListener('mousemove',e=>{mx=e.clientX;my=e.clientY;});
btn.onclick=()=>fetch('/herd',{method:'POST'}).catch(()=>{});
async function poll(){try{snap=await (await fetch('/state')).json();}catch(e){}setTimeout(poll,60);}poll();
setInterval(()=>{if(!myId)return;const target=[cam[0]+(mx-cv.width/2)/scale,cam[1]+(my-cv.height/2)/scale];fetch('/input',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({id:myId,target})}).catch(()=>{});},60);
function drawGrid(){
  const [x0,x1,y0,y1]=WORLD;
  cx.strokeStyle='#233';cx.lineWidth=1;
  if(GRID){const m=/^(\\d+)x(\\d+)$/.exec(GRID); if(m){const cols=+m[1], rows=+m[2], cw=ARENA[0]/cols, ch=ARENA[1]/rows; for(let x=0;x<=cols;x++){line([x*cw,0],[x*cw,ARENA[1]],x%cols===0?'#fff':'#233');} for(let y=0;y<=rows;y++){line([0,y*ch],[ARENA[0],y*ch],y%rows===0?'#fff':'#233');}}}
  else line([(x0+x1)/2,y0],[(x0+x1)/2,y1],'#fff');
  function line(a,b,s){cx.strokeStyle=s;cx.beginPath();cx.moveTo(sx(a),sy(a));cx.lineTo(sx(b),sy(b));cx.stroke();}
}
function sx(p){return cv.width/2+(p[0]-cam[0])*scale} function sy(p){return cv.height/2+(p[1]-cam[1])*scale}
function draw(){cx.clearRect(0,0,cv.width,cv.height);const me=snap.find(v=>v.e===myId);if(me&&me.p)cam=me.p;scale=Math.min(cv.width,cv.height)/Math.max(WORLD[1]-WORLD[0],WORLD[3]-WORLD[2])*1.15;drawGrid();const owners={};for(const c of snap){if(!c.p)continue;owners[c.o]=(owners[c.o]||0)+1;const r=Math.max(2,Math.sqrt(Math.max(1,c.m))*scale*0.7);cx.globalAlpha=c.e===myId?1:(c.m<=1.1?0.45:0.9);cx.fillStyle=c.e===myId?'#fff':col(c.o);cx.beginPath();cx.arc(sx(c.p),sy(c.p),r,0,7);cx.fill();}cx.globalAlpha=1;hud.textContent='Godworks agar.io reality demo\\nentities: '+snap.length+'  player: '+(myId||'-')+'\\n'+Object.keys(owners).sort().map(k=>k+': '+owners[k]).join('\\n');requestAnimationFrame(draw);}draw();
</script></body></html>`;
}

function readBody(req, cb) {
  let body = "";
  req.on("data", d => body += d);
  req.on("end", () => {
    try { cb(JSON.parse(body || "{}")); } catch (_) { cb({}); }
  });
}

http.createServer((req, res) => {
  const parsed = new URL(req.url, "http://localhost");
  const path = parsed.pathname;
  if (path === "/state") {
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify(snapshot));
  } else if (path === "/client-state") {
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify(clientSnapshot));
  } else if (path === "/join" && req.method === "POST") {
    readBody(req, b => {
      const id = `P-${playerSeq++}`;
      const requestedPos = Array.isArray(b.pos) ? [Number(b.pos[0]), Number(b.pos[1])] : null;
      const pos = requestedPos && requestedPos.every(Number.isFinite)
        ? [Math.max(WORLD[0], Math.min(WORLD[1], requestedPos[0])), Math.max(WORLD[2], Math.min(WORLD[3], requestedPos[1]))]
        : [(WORLD[0] + WORLD[1]) / 2 + (Math.random() - 0.5) * 4, (WORLD[2] + WORLD[3]) / 2 + (Math.random() - 0.5) * 4];
      send(writeSock, { op: "CreateEntity", request_id: `join-${id}`, entity: id, region: regionForPos(pos), components: {
        pos, vel: [0, 0], mass: 6, type: "player"
      }});
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ id, world: WORLD, pos }));
    });
  } else if (path === "/input" && req.method === "POST") {
    readBody(req, b => {
      if (b.id && Array.isArray(b.target)) {
        const requestId = `cmd-${b.id}-${Date.now()}`;
        const wait = parsed.searchParams.get("wait") === "1";
        if (wait) {
          const timer = setTimeout(() => {
            pendingCommands.delete(requestId);
            if (!res.writableEnded) {
              res.writeHead(504, { "content-type": "application/json" });
              res.end(JSON.stringify({ ok: false, error: "command_timeout", request_id: requestId }));
            }
          }, 2000);
          pendingCommands.set(requestId, { res, timer });
          send(writeSock, { op: "CommandRequest", request_id: requestId, entity: b.id, command: "set_target", payload: b.target });
          return;
        }
        send(writeSock, { op: "CommandRequest", request_id: requestId, entity: b.id, command: "set_target", payload: b.target });
      }
      res.writeHead(200);
      res.end("ok");
    });
  } else if (path === "/herd" && req.method === "POST") {
    const center = [(WORLD[0] + WORLD[1]) / 2, (WORLD[2] + WORLD[3]) / 2];
    for (const c of snapshot.filter(v => v.e && !String(v.e).includes("-food-")).slice(0, 150)) {
      send(writeSock, { op: "CommandRequest", request_id: `herd-${c.e}-${Date.now()}`, entity: c.e, command: "set_target", payload: center });
    }
    res.writeHead(200);
    res.end("ok");
  } else {
    res.writeHead(200, { "content-type": "text/html" });
    res.end(html());
  }
}).listen(HTTP_PORT, () => console.error(`[agar-gateway] http://localhost:${HTTP_PORT}`));
