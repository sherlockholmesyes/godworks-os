// Live shard monitor for the stock MIT agar.io clone.
// It reads the real game through spectator mode, maps all entities onto a
// 100x100 grid, groups the grid into load-balanced worker regions, and serves
// the browser view on :8091 by default.
"use strict";

const http = require("http");
const { connectSpectator } = require("./_gw_spectator_tap");

const PORT = parseInt(process.env.GW_MONITOR_PORT || "8091", 10);
const CELLS = parseInt(process.env.GW_GRID_CELLS || "100", 10);
const NX = parseInt(process.env.GW_WORKER_COLS || "4", 10);
const NY = parseInt(process.env.GW_WORKER_ROWS || "4", 10);
const REBALANCE_RATIO = parseFloat(process.env.GW_REBALANCE_RATIO || "1.4");
const MIN_REBALANCE_MS = parseInt(process.env.GW_MIN_REBALANCE_MS || "1500", 10);

let latest = null;
let partition = null;
let lastRebalanceAt = 0;
let rebalanceCount = 0;

connectSpectator({
  onStatus: msg => console.error(msg),
  onFrame: frame => {
    latest = frame;
    updatePartition(frame);
  },
});

function updatePartition(frame) {
  const now = Date.now();
  const load = makeGridLoad(frame.entities, frame.width, frame.height);
  const proposal = makePartition(load);
  const proposedLoads = blockLoads(load, proposal);

  if (!partition) {
    partition = { bands: proposal, loads: proposedLoads };
    return;
  }

  const currentLoads = blockLoads(load, partition.bands);
  const mean = currentLoads.reduce((a, b) => a + b, 0) / Math.max(1, currentLoads.length);
  const max = Math.max(0, ...currentLoads);

  if (now - lastRebalanceAt >= MIN_REBALANCE_MS && max > mean * REBALANCE_RATIO) {
    partition = { bands: proposal, loads: proposedLoads };
    lastRebalanceAt = now;
    rebalanceCount++;
  } else {
    partition.loads = currentLoads;
  }
}

function makeGridLoad(entities, width, height) {
  const load = Array.from({ length: CELLS }, () => Array(CELLS).fill(0));
  for (const e of entities) {
    const cx = clamp(Math.floor((e.x / width) * CELLS), 0, CELLS - 1);
    const cy = clamp(Math.floor((e.y / height) * CELLS), 0, CELLS - 1);
    load[cx][cy] += entityWeight(e);
  }
  return load;
}

function entityWeight(e) {
  if (e.type === "player") return 4;
  if (e.type === "virus") return 2;
  return 1;
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

function state() {
  const frame = latest || { ts: Date.now(), width: 5000, height: 5000, entities: [] };
  const p = partition || { bands: [], loads: [] };
  return {
    ts: frame.ts,
    width: frame.width,
    height: frame.height,
    gridCells: CELLS,
    workerCols: NX,
    workerRows: NY,
    rebalanceCount,
    loads: p.loads,
    bands: p.bands,
    entities: frame.entities.map(e => ({ ...e, block: blockFor(e, p.bands, frame.width, frame.height) })),
  };
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

const HTML = `<!doctype html><html><head><meta charset=utf8><title>Godworks agar.io shard monitor</title>
<style>
body{margin:0;background:#07080d;color:#7cff7c;font:12px monospace;overflow:hidden}
#h{position:fixed;top:7px;left:7px;white-space:pre;text-shadow:0 0 4px #000;font-weight:700}
#g{position:fixed;right:12px;bottom:12px;width:330px;height:112px}
</style></head>
<body><canvas id=c></canvas><div id=h></div><canvas id=g></canvas><script>
const cv=document.getElementById('c'),cx=cv.getContext('2d'),h=document.getElementById('h'),gc=document.getElementById('g'),gx=gc.getContext('2d');
function rz(){cv.width=innerWidth;cv.height=innerHeight}addEventListener('resize',rz);rz();
gc.width=330;gc.height=112;
const palette=['#35a7ff','#ff9f1c','#2ec4b6','#ff477e','#b8f542','#8b5cf6','#ffe66d','#4ecdc4','#ff595e','#8ac926','#4361ee','#ffca3a','#00bbf9','#f15bb5','#00f5d4','#b8b8ff'];
let S=null; async function poll(){try{S=await(await fetch('/state')).json()}catch(e){}setTimeout(poll,100)}poll();
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
 cx.lineWidth=2;
 let bi=0, blockNames=[];
 for(const band of S.bands||[]){for(const row of band.rows||[]){const color=palette[bi%palette.length];blockNames.push('w'+bi);cx.fillStyle=color+'22';cx.fillRect(X(band.c0*S.width/S.gridCells),Y(row.r0*S.height/S.gridCells),(band.c1-band.c0)*S.width/S.gridCells*scale,(row.r1-row.r0)*S.height/S.gridCells*scale);cx.strokeStyle=color;cx.strokeRect(X(band.c0*S.width/S.gridCells),Y(row.r0*S.height/S.gridCells),(band.c1-band.c0)*S.width/S.gridCells*scale,(row.r1-row.r0)*S.height/S.gridCells*scale);bi++;}}
 const byBlock={};
 for(const e of S.entities){const b=e.block||'?';byBlock[b]=(byBlock[b]||0)+1;const idx=Number(String(b).replace(/\\D+/g,''));cx.fillStyle=e.type==='player'?(palette[(isFinite(idx)?idx:0)%palette.length]):(e.type==='virus'?'#34d399':'#45ff62');cx.globalAlpha=e.type==='player'?1:(e.type==='virus'?0.8:0.65);const r=e.type==='player'?Math.max(2.5,Math.sqrt(Math.max(1,e.mass))*0.72):Math.max(1.2,Math.sqrt(Math.max(1,e.mass))*0.22);cx.beginPath();cx.arc(X(e.x),Y(e.y),r,0,7);cx.fill();}
 cx.globalAlpha=1;
 const loads=(S.loads||[]).map(x=>Math.round(x));
 const mean=loads.length?loads.reduce((a,b)=>a+b,0)/loads.length:0;
 const max=loads.length?Math.max(...loads):0;
 h.textContent='Godworks agar.io — REAL game, map sharded into '+S.workerCols+'x'+S.workerRows+'='+loads.length+' worker-zones (2D, load-balanced)\\n'
  +'entities: '+S.entities.length+'   mean/worker: '+mean.toFixed(0)+'   peak: '+max+'   rebalance: '+S.rebalanceCount+'\\n'
  +loads.map((v,i)=>'w'+i+':'+v).join('   ');
 drawLoadGraph(loads,mean,max);
 requestAnimationFrame(draw)}draw();
function drawLoadGraph(loads,mean,max){
 gx.clearRect(0,0,gc.width,gc.height);
 gx.fillStyle='rgba(7,8,13,.88)';gx.fillRect(0,0,gc.width,gc.height);
 gx.fillStyle='#7cff7c';gx.font='11px monospace';gx.fillText('load per worker (white=mean, red=overloaded)',4,12);
 const base=gc.height-16,w=14,gap=5,cap=Math.max(1,max,mean*1.6);
 gx.strokeStyle='rgba(255,255,255,.7)';gx.beginPath();const my=base-(mean/cap)*(gc.height-34);gx.moveTo(0,my);gx.lineTo(gc.width,my);gx.stroke();
 for(let i=0;i<loads.length;i++){const x=8+i*(w+gap),hgt=(loads[i]/cap)*(gc.height-34);gx.fillStyle=loads[i]>mean*1.25?'#ff3b3b':(loads[i]>mean*1.1?'#ffd84d':'#49ff49');gx.fillRect(x,base-hgt,w,hgt);}
}
</script></body></html>`;

http.createServer((req, res) => {
  if (req.url === "/state") {
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify(state()));
  } else {
    res.writeHead(200, { "content-type": "text/html" });
    res.end(HTML);
  }
}).listen(PORT, () => console.error(`[monitor] http://localhost:${PORT}`));

function clamp(v, lo, hi) {
  return Math.max(lo, Math.min(hi, v));
}
