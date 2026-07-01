// Live monitor for the Godworks-authoritative MIT agar.io path.
// It renders the real authoritative /state stream, actual broker owner loads,
// the fixed broker grid, and a dynamic load-balanced diagnostic partition.
"use strict";

const http = require("http");

const PORT = parseInt(process.env.GW_MONITOR_PORT || "8091", 10);
const STATE_URL = process.env.GW_AGAR_STATE_URL || "http://127.0.0.1:3000/state?entities=1";
const CELLS = parseInt(process.env.GW_GRID_CELLS || "100", 10);
const NX = parseInt(process.env.GW_WORKER_COLS || "4", 10);
const NY = parseInt(process.env.GW_WORKER_ROWS || "4", 10);

function getJson(url) {
  return new Promise((resolve, reject) => {
    const req = http.get(url, res => {
      let body = "";
      res.setEncoding("utf8");
      res.on("data", chunk => { body += chunk; });
      res.on("end", () => {
        try { resolve(JSON.parse(body || "{}")); } catch (e) { reject(e); }
      });
    });
    req.setTimeout(2000, () => req.destroy(new Error(`timeout ${url}`)));
    req.on("error", reject);
  });
}

function clamp(v, lo, hi) {
  return Math.max(lo, Math.min(hi, v));
}

function entityWeight(e) {
  if (e.kind === "player") return 4;
  if (e.kind === "virus") return 2;
  return 1;
}

function makeGridLoad(entities, width, height) {
  const load = Array.from({ length: CELLS }, () => Array(CELLS).fill(0));
  for (const e of entities) {
    if (!Number.isFinite(e.x) || !Number.isFinite(e.y)) continue;
    const cx = clamp(Math.floor((e.x / width) * CELLS), 0, CELLS - 1);
    const cy = clamp(Math.floor((e.y / height) * CELLS), 0, CELLS - 1);
    load[cx][cy] += entityWeight(e);
  }
  return load;
}

function balancedRanges(weights, parts) {
  const total = weights.reduce((a, b) => a + b, 0);
  const ranges = [];
  let start = 0;
  let acc = 0;
  for (let p = 1; p < parts; p++) {
    const target = (total * p) / parts;
    while (start < weights.length - (parts - p) && acc + weights[start] < target) {
      acc += weights[start];
      start++;
    }
    ranges.push([ranges.length ? ranges[ranges.length - 1][1] : 0, start + 1]);
    start++;
  }
  ranges.push([ranges.length ? ranges[ranges.length - 1][1] : 0, weights.length]);
  return sanitizeRanges(ranges, parts, weights.length);
}

function sanitizeRanges(ranges, parts, max) {
  const out = [];
  let cursor = 0;
  for (let i = 0; i < parts; i++) {
    const raw = ranges[i] || [cursor, max];
    let end = clamp(raw[1], cursor + 1, max - (parts - i - 1));
    if (i === parts - 1) end = max;
    out.push([cursor, end]);
    cursor = end;
  }
  return out;
}

function makePartition(load) {
  const colLoad = [];
  for (let x = 0; x < CELLS; x++) colLoad[x] = load[x].reduce((a, b) => a + b, 0);
  const colBands = balancedRanges(colLoad, NX);
  return colBands.map(([c0, c1]) => {
    const rowLoad = Array(CELLS).fill(0);
    for (let x = c0; x < c1; x++) {
      for (let y = 0; y < CELLS; y++) rowLoad[y] += load[x][y];
    }
    return { c0, c1, rows: balancedRanges(rowLoad, NY).map(([r0, r1]) => ({ r0, r1 })) };
  });
}

function blockLoads(load, bands) {
  const loads = [];
  for (const band of bands) {
    for (const row of band.rows) {
      let sum = 0;
      for (let x = band.c0; x < band.c1; x++) {
        for (let y = row.r0; y < row.r1; y++) sum += load[x][y];
      }
      loads.push(sum);
    }
  }
  return loads;
}

function blockFor(e, bands, width, height) {
  const cx = clamp(Math.floor((e.x / width) * CELLS), 0, CELLS - 1);
  const cy = clamp(Math.floor((e.y / height) * CELLS), 0, CELLS - 1);
  for (let bx = 0; bx < bands.length; bx++) {
    const band = bands[bx];
    if (cx < band.c0 || cx >= band.c1) continue;
    for (let by = 0; by < band.rows.length; by++) {
      const row = band.rows[by];
      if (cy >= row.r0 && cy < row.r1) return `W${bx}_${by}`;
    }
  }
  return "?";
}

function ownerIndex(owner) {
  const m = /^auth-Z(\d+)_(\d+)$/.exec(String(owner || ""));
  if (!m) return null;
  const col = Number(m[1]);
  const row = Number(m[2]);
  return row * NX + col;
}

async function state() {
  const upstream = await getJson(STATE_URL);
  const width = Number(upstream.width) || 5000;
  const height = Number(upstream.height) || 5000;
  const entities = Array.isArray(upstream.entityRows) ? upstream.entityRows : [];
  const load = makeGridLoad(entities, width, height);
  const bands = makePartition(load);
  const dynamicLoads = blockLoads(load, bands);
  const ownerLoads = {};
  for (const e of entities) {
    const key = e.owner || e.region || "?";
    ownerLoads[key] = (ownerLoads[key] || 0) + entityWeight(e);
  }
  return {
    ok: upstream.ok === true,
    godworksAuthoritative: upstream.godworksAuthoritative === true,
    ts: upstream.ts || Date.now(),
    stateUrl: STATE_URL,
    width,
    height,
    gridCells: CELLS,
    workerCols: NX,
    workerRows: NY,
    fixedBrokerGrid: upstream.grid || { cols: NX, rows: NY },
    partitionMode: "diagnostic_load_balanced_view",
    dynamicLoads,
    ownerLoads,
    upstream: {
      players: upstream.players || 0,
      entities: upstream.entities || 0,
      foods: upstream.foods || 0,
      playerEntities: upstream.playerEntities || 0,
      commandResponses: upstream.commandResponses || 0,
      commandRejects: upstream.commandRejects || 0,
      commandTransientRejects: upstream.commandTransientRejects || 0,
      entityRowsTruncated: upstream.entityRowsTruncated === true,
    },
    bands,
    entities: entities.map(e => ({
      id: e.id,
      x: e.x,
      y: e.y,
      mass: e.mass,
      kind: e.kind,
      hue: e.hue,
      owner: e.owner || e.region || "?",
      ownerIndex: ownerIndex(e.owner || e.region),
      block: blockFor(e, bands, width, height),
    })),
  };
}

const HTML = `<!doctype html><html><head><meta charset=utf8><title>Godworks authoritative agar.io monitor</title>
<style>
body{margin:0;background:#07080d;color:#7cff7c;font:12px monospace;overflow:hidden}
#h{position:fixed;top:7px;left:7px;white-space:pre;text-shadow:0 0 4px #000;font-weight:700}
#g{position:fixed;right:12px;bottom:12px;width:350px;height:120px}
</style></head>
<body><canvas id=c></canvas><div id=h></div><canvas id=g></canvas><script>
const cv=document.getElementById('c'),cx=cv.getContext('2d'),h=document.getElementById('h'),gc=document.getElementById('g'),gx=gc.getContext('2d');
function rz(){cv.width=innerWidth;cv.height=innerHeight}addEventListener('resize',rz);rz();gc.width=350;gc.height=120;
const palette=['#35a7ff','#ff9f1c','#2ec4b6','#ff477e','#b8f542','#8b5cf6','#ffe66d','#4ecdc4','#ff595e','#8ac926','#4361ee','#ffca3a','#00bbf9','#f15bb5','#00f5d4','#b8b8ff'];
let S=null; async function poll(){try{S=await(await fetch('/state')).json()}catch(e){}setTimeout(poll,150)}poll();
function draw(){cx.clearRect(0,0,cv.width,cv.height);if(!S){requestAnimationFrame(draw);return}
 const leftPanel=Math.min(520,Math.max(500,cv.width*0.34));
 const usableW=Math.max(320,cv.width-leftPanel-54), usableH=cv.height-8;
 const scale=Math.min(usableW/S.width,usableH/S.height), ox=leftPanel, oy=4;
 const X=x=>ox+x*scale,Y=y=>oy+y*scale;
 cx.fillStyle='#06110d';cx.fillRect(ox,oy,S.width*scale,S.height*scale);
 cx.strokeStyle='rgba(0,0,0,.38)';cx.lineWidth=1;
 const gridStep=S.width/10;
 for(let x=0;x<=S.width;x+=gridStep){cx.beginPath();cx.moveTo(X(x),oy);cx.lineTo(X(x),oy+S.height*scale);cx.stroke();}
 for(let y=0;y<=S.height;y+=gridStep){cx.beginPath();cx.moveTo(ox,Y(y));cx.lineTo(ox+S.width*scale,Y(y));cx.stroke();}
 let bi=0;
 for(const band of S.bands||[]){for(const row of band.rows||[]){const color=palette[bi%palette.length];cx.fillStyle=color+'18';cx.fillRect(X(band.c0*S.width/S.gridCells),Y(row.r0*S.height/S.gridCells),(band.c1-band.c0)*S.width/S.gridCells*scale,(row.r1-row.r0)*S.height/S.gridCells*scale);cx.strokeStyle=color;cx.lineWidth=2;cx.strokeRect(X(band.c0*S.width/S.gridCells),Y(row.r0*S.height/S.gridCells),(band.c1-band.c0)*S.width/S.gridCells*scale,(row.r1-row.r0)*S.height/S.gridCells*scale);bi++;}}
 cx.strokeStyle='rgba(255,255,255,.9)';cx.lineWidth=1.5;
 for(let x=1;x<S.fixedBrokerGrid.cols;x++){cx.beginPath();cx.moveTo(X(x*S.width/S.fixedBrokerGrid.cols),oy);cx.lineTo(X(x*S.width/S.fixedBrokerGrid.cols),oy+S.height*scale);cx.stroke();}
 for(let y=1;y<S.fixedBrokerGrid.rows;y++){cx.beginPath();cx.moveTo(ox,Y(y*S.height/S.fixedBrokerGrid.rows));cx.lineTo(ox+S.width*scale,Y(y*S.height/S.fixedBrokerGrid.rows));cx.stroke();}
 for(const e of S.entities){const idx=Number.isFinite(e.ownerIndex)?e.ownerIndex:Number(String(e.block||'').replace(/\\D+/g,''));cx.fillStyle=e.kind==='player'?(palette[(Number.isFinite(idx)?idx:0)%palette.length]):'#45ff62';cx.globalAlpha=e.kind==='player'?1:0.65;const r=e.kind==='player'?Math.max(2.5,Math.sqrt(Math.max(1,e.mass))*0.72):Math.max(1.1,Math.sqrt(Math.max(1,e.mass))*0.22);cx.beginPath();cx.arc(X(e.x),Y(e.y),r,0,7);cx.fill();}
 cx.globalAlpha=1;
 const dyn=(S.dynamicLoads||[]).map(x=>Math.round(x));
 const ownerNames=Object.keys(S.ownerLoads||{}).sort();
 const ownerVals=ownerNames.map(k=>S.ownerLoads[k]);
 const mean=dyn.length?dyn.reduce((a,b)=>a+b,0)/dyn.length:0;
 const max=dyn.length?Math.max(...dyn):0;
 h.textContent='Godworks authoritative agar.io - MIT client :3000, broker-owned state, monitor :8091\\n'
  +'entities: '+S.upstream.entities+' food:'+S.upstream.foods+' players:'+S.upstream.playerEntities+' commands:'+S.upstream.commandResponses+' rejects:'+S.upstream.commandRejects+'/'+S.upstream.commandTransientRejects+'\\n'
  +'fixed broker grid: '+S.fixedBrokerGrid.cols+'x'+S.fixedBrokerGrid.rows+'   diagnostic dynamic view: '+S.workerCols+'x'+S.workerRows+'   mean/view-worker:'+mean.toFixed(0)+' peak:'+max+'\\n'
  +ownerNames.map(k=>k.replace('auth-','')+':'+S.ownerLoads[k]).join('   ');
 drawLoadGraph(ownerVals);
 requestAnimationFrame(draw)}draw();
function drawLoadGraph(vals){
 gx.clearRect(0,0,gc.width,gc.height);gx.fillStyle='rgba(7,8,13,.88)';gx.fillRect(0,0,gc.width,gc.height);
 gx.fillStyle='#7cff7c';gx.font='11px monospace';gx.fillText('actual owner load per broker worker',4,12);
 const clean=vals.map(v=>Number(v)||0),mean=clean.length?clean.reduce((a,b)=>a+b,0)/clean.length:0,max=clean.length?Math.max(...clean):0,base=gc.height-16,w=14,gap=5,cap=Math.max(1,max,mean*1.6);
 gx.strokeStyle='rgba(255,255,255,.7)';gx.beginPath();const my=base-(mean/cap)*(gc.height-34);gx.moveTo(0,my);gx.lineTo(gc.width,my);gx.stroke();
 for(let i=0;i<clean.length;i++){const x=8+i*(w+gap),hgt=(clean[i]/cap)*(gc.height-34);gx.fillStyle=clean[i]>mean*1.25?'#ff3b3b':(clean[i]>mean*1.1?'#ffd84d':'#49ff49');gx.fillRect(x,base-hgt,w,hgt);}
}
</script></body></html>`;

http.createServer(async (req, res) => {
  if (req.url === "/state") {
    try {
      const s = await state();
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify(s));
    } catch (e) {
      res.writeHead(503, { "content-type": "application/json" });
      res.end(JSON.stringify({ ok: false, error: e.message, stateUrl: STATE_URL }));
    }
    return;
  }
  res.writeHead(200, { "content-type": "text/html" });
  res.end(HTML);
}).listen(PORT, () => console.error(`[auth-monitor] http://localhost:${PORT} -> ${STATE_URL}`));
