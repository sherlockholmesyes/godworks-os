// gw_d2_view.js — D4 (re-ordered before D3 for visibility): browser view of the REAL broker's distribution.
// Observes the D2 broker (InspectorQuery -> InspectorFrame), draws each entity colored by its OWNING WORKER
// (the real authority distribution from the engine, NOT a JS computation), over the 2D grid. = the real-broker
// version of the :8091 JS-monitor. Serves :8092. Raw TCP + built-in http, no deps.
const net = require('net'), http = require('http');
const BPORT = parseInt(process.env.GW_PORT || '7990', 10);
const HTTP_PORT = parseInt(process.env.GW_HTTP || '8092', 10);
const ARENA = parseFloat(process.env.GW_ARENA || '5000');
const NX = parseInt(process.env.NX || '2', 10), NY = parseInt(process.env.NY || '2', 10);
const OBS_TOKEN = process.env.GW_OBS_TOKEN || '';
function frame(o){ const b=Buffer.from(JSON.stringify(o),'utf8'); const h=Buffer.alloc(4); h.writeUInt32BE(b.length,0); return Buffer.concat([h,b]); }
let buf = Buffer.alloc(0), snap = [];
const s = net.connect(BPORT, '127.0.0.1', () => {
  const connect = {op:'WorkerConnect', worker_id:'d2-view', region:'OBS', attributes:['observer','inspector']};
  if (OBS_TOKEN) connect.auth_token = OBS_TOKEN;
  s.write(frame(connect));
  s.write(frame({op:'Interest', center:[ARENA/2, ARENA/2], radius:1e9}));
  setInterval(()=>s.write(frame({op:'InspectorQuery', request_id:'v', max_entities:1000})), 80);
  console.error('[d2-view] connected broker :'+BPORT);
});
s.on('data', d => { buf=Buffer.concat([buf,d]); while(buf.length>=4){ const n=buf.readUInt32BE(0); if(buf.length<4+n)break; let f; try{f=JSON.parse(buf.slice(4,4+n).toString('utf8'));}catch(e){} buf=buf.slice(4+n); if(f&&f.op==='InspectorFrame'){ snap=(f.entities||[]).map(e=>({id:e.entity||e.id||e.entity_id,p:e.pos, o:((e.authority||{}).pos||{}).owner||e.region||'?', r:e.region})); } }});
s.on('error', e=>console.error('[d2-view] broker', e.message));
s.on('close', ()=>{ console.error('[d2-view] broker closed'); process.exit(0); });

const HTML = `<!doctype html><html><head><meta charset=utf8><title>Godworks D4 — real broker view</title>
<style>body{margin:0;background:#0a0a10;overflow:hidden;font:12px monospace;color:#9f9}#h{position:fixed;top:6px;left:8px;white-space:pre;text-shadow:0 0 4px #000}</style></head>
<body><canvas id=c></canvas><div id=h></div><script>
const ARENA=${ARENA}, NX=${NX}, NY=${NY};
const cv=document.getElementById('c'),cx=cv.getContext('2d'),Hd=document.getElementById('h');
function rz(){cv.width=innerWidth;cv.height=innerHeight;}addEventListener('resize',rz);rz();
const wcol=['#4af','#fa4','#4fa','#f4a','#af4','#a4f','#ff5','#4ff','#f55','#5f5','#55f','#fa0'];
const cmap={}; let ci=0; function col(o){ if(!cmap[o])cmap[o]=wcol[ci++%wcol.length]; return cmap[o]; }
let D=[];
async function poll(){try{D=await(await fetch('/state')).json();}catch(e){}setTimeout(poll,100);}poll();
function draw(){
  const s=Math.min(cv.width,cv.height)/ARENA, ox=(cv.width-ARENA*s)/2, oy=(cv.height-ARENA*s)/2;
  const X=v=>ox+v*s, Y=v=>oy+v*s;
  cx.clearRect(0,0,cv.width,cv.height);
  cx.strokeStyle='#2a2a3a'; cx.lineWidth=2;
  for(let i=0;i<=NX;i++){const x=X(i*ARENA/NX);cx.beginPath();cx.moveTo(x,oy);cx.lineTo(x,oy+ARENA*s);cx.stroke();}
  for(let j=0;j<=NY;j++){const y=Y(j*ARENA/NY);cx.beginPath();cx.moveTo(ox,y);cx.lineTo(ox+ARENA*s,y);cx.stroke();}
  const byO={};
  for(const e of D){ if(!e.p)continue; cx.fillStyle=col(e.o); cx.beginPath();cx.arc(X(e.p[0]),Y(e.p[1]),5,0,7);cx.fill(); byO[e.o]=(byO[e.o]||0)+1; }
  Hd.textContent='Godworks D4 — REAL broker distribution ('+NX+'x'+NY+' zones; color = owning worker-process, from the broker InspectorFrame)\\n'
    +'entities: '+D.length+'\\n'+Object.keys(byO).sort().map(o=>'  '+o+': '+byO[o]).join('\\n');
  requestAnimationFrame(draw);
}draw();
</script></body></html>`;

http.createServer((req,res)=>{
  if(req.url==='/state'){ res.writeHead(200,{'content-type':'application/json'}); res.end(JSON.stringify(snap)); }
  else { res.writeHead(200,{'content-type':'text/html'}); res.end(HTML); }
}).listen(HTTP_PORT, ()=>console.error('[d2-view] http://localhost:'+HTTP_PORT+' (real broker distribution, '+NX+'x'+NY+')'));
