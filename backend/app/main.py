from contextlib import asynccontextmanager
from dataclasses import asdict
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from sqlalchemy import func

from .config import PORTAINER_API_TOKEN, PORTAINER_BASE_URL, REPO_ROOT
from .db import ObservedEvent, SessionLocal, init_db
from .portainer import PortainerClient
from .registry import load_registry

_DASHBOARD = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>ORC — Operator Dashboard</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css">
<style>
:root{--bg:#0d1117;--sur:#161b22;--bdr:#30363d;--txt:#e6edf3;--mut:#8b949e;--grn:#3fb950;--red:#f85149;--yel:#d29922;--blu:#58a6ff}
body{background:var(--bg);color:var(--txt);font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif}
.card{background:var(--sur);border:1px solid var(--bdr);border-radius:8px}
.nav-bar{background:var(--sur);border-bottom:1px solid var(--bdr);padding:12px 20px;display:flex;align-items:center;justify-content:space-between}
.brand{font-weight:700;font-size:1.1rem;letter-spacing:.02em}
.dot{display:inline-block;width:10px;height:10px;border-radius:50%;margin-right:6px;vertical-align:middle}
.ok{background:var(--grn)}.err{background:var(--red)}.unk{background:var(--mut)}
.stat-val{font-size:1.75rem;font-weight:700;line-height:1}
.stat-lbl{font-size:.75rem;color:var(--mut);margin-bottom:4px}
.sev-critical{color:var(--red);font-weight:700}
.sev-error{color:#f85149}
.sev-warning{color:var(--yel)}
.sev-info,.sev-debug{color:var(--mut)}
table{width:100%;border-collapse:collapse;font-size:.85rem}
th{color:var(--mut);font-weight:500;padding:6px 8px;border-bottom:1px solid var(--bdr);text-align:left}
td{padding:6px 8px;border-bottom:1px solid #21262d;vertical-align:top}
tr:last-child td{border-bottom:none}
.msg{max-width:420px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.cname{font-family:monospace;font-size:.8rem;color:var(--blu)}
.ts{color:var(--mut);font-family:monospace;font-size:.78rem;white-space:nowrap}
.row-item{display:flex;justify-content:space-between;align-items:center;padding:5px 0;border-bottom:1px solid #21262d}
.row-item:last-child{border-bottom:none}
.tag{background:#21262d;color:var(--mut);font-size:.7rem;padding:2px 6px;border-radius:4px;font-family:monospace}
.btn-ref{background:#21262d;color:var(--txt);border:1px solid var(--bdr);padding:5px 14px;border-radius:6px;cursor:pointer;font-size:.85rem}
.btn-ref:hover{background:var(--bdr)}
.empty{color:var(--mut);font-size:.85rem;padding:10px 0}
.scroll{max-height:440px;overflow-y:auto}
</style>
</head>
<body>
<div class="nav-bar">
  <span class="brand">&#9876; ORC</span>
  <span style="color:var(--mut);font-size:.8rem" id="upd"></span>
  <button class="btn-ref" onclick="loadAll()">&#8635; Refresh</button>
</div>
<div style="padding:20px">

  <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:16px;margin-bottom:20px">
    <div class="card" style="padding:16px">
      <div class="stat-lbl">API</div>
      <span class="dot unk" id="api-dot"></span><span id="api-txt">—</span>
    </div>
    <div class="card" style="padding:16px">
      <div class="stat-lbl">Portainer</div>
      <span class="dot unk" id="pt-dot"></span><span id="pt-txt">—</span>
    </div>
    <div class="card" style="padding:16px">
      <div class="stat-lbl">Events (24 h)</div>
      <div class="stat-val" id="ev-cnt">—</div>
    </div>
    <div class="card" style="padding:16px">
      <div class="stat-lbl">Agents loaded</div>
      <div class="stat-val" id="ag-cnt">—</div>
    </div>
  </div>

  <div style="display:grid;grid-template-columns:1fr 320px;gap:16px;align-items:start">
    <div class="card" style="padding:20px">
      <div style="font-weight:600;margin-bottom:12px">Recent Events</div>
      <div id="ev-body"><div class="empty">Loading…</div></div>
    </div>
    <div style="display:flex;flex-direction:column;gap:16px">
      <div class="card" style="padding:20px">
        <div style="font-weight:600;margin-bottom:12px">Agents</div>
        <div id="ag-body"><div class="empty">Loading…</div></div>
      </div>
      <div class="card" style="padding:20px">
        <div style="font-weight:600;margin-bottom:12px">Skills</div>
        <div id="sk-body"><div class="empty">Loading…</div></div>
      </div>
    </div>
  </div>

</div>
<script>
const SEV={critical:'sev-critical',error:'sev-error',warning:'sev-warning',info:'sev-info',debug:'sev-debug'};
function fmt(iso){const d=new Date(iso);return d.toLocaleDateString()+' '+d.toLocaleTimeString();}
async function loadStatus(){
  try{
    const d=await fetch('/health').then(r=>r.json());
    document.getElementById('api-txt').textContent=d.status;
    document.getElementById('api-dot').className='dot ok';
    document.getElementById('pt-txt').textContent=d.portainer?'Connected':'Disconnected';
    document.getElementById('pt-dot').className='dot '+(d.portainer?'ok':'err');
  }catch{document.getElementById('api-txt').textContent='Error';}
}
async function loadEvents(){
  try{
    const d=await fetch('/events?limit=100').then(r=>r.json());
    document.getElementById('ev-cnt').textContent=d.count_24h??0;
    if(!d.items.length){
      document.getElementById('ev-body').innerHTML='<div class="empty">No events yet — worker polls Portainer every 60 s.</div>';
      return;
    }
    const rows=d.items.map(e=>`<tr>
      <td class="ts">${fmt(e.occurred_at)}</td>
      <td><span class="cname">${e.container_name}</span></td>
      <td><span class="${SEV[e.severity]||'sev-info'}">${e.severity}</span></td>
      <td class="msg" title="${e.message.replace(/"/g,'&quot;')}">${e.message}</td>
    </tr>`).join('');
    document.getElementById('ev-body').innerHTML=`<div class="scroll"><table>
      <thead><tr><th>Time</th><th>Container</th><th>Severity</th><th>Message</th></tr></thead>
      <tbody>${rows}</tbody></table></div>`;
  }catch{document.getElementById('ev-body').innerHTML='<div class="empty">Could not load events.</div>';}
}
async function loadAgents(){
  const d=await fetch('/registry/agents').then(r=>r.json());
  document.getElementById('ag-cnt').textContent=d.count;
  document.getElementById('ag-body').innerHTML=d.items.map(a=>`
    <div class="row-item"><span style="font-size:.85rem">${a.name}</span><span class="tag">${a.version}</span></div>
  `).join('')||'<div class="empty">No agents found.</div>';
}
async function loadSkills(){
  const d=await fetch('/registry/skills').then(r=>r.json());
  document.getElementById('sk-body').innerHTML=d.items.map(s=>`
    <div class="row-item"><span style="font-size:.85rem">${s.name}</span><span class="tag">${s.role_or_category}</span></div>
  `).join('')||'<div class="empty">No skills found.</div>';
}
async function loadAll(){
  await Promise.all([loadStatus(),loadEvents(),loadAgents(),loadSkills()]);
  document.getElementById('upd').textContent='Updated '+new Date().toLocaleTimeString();
}
loadAll();
setInterval(loadAll,30000);
</script>
</body>
</html>"""


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


app = FastAPI(
    title="ORC API",
    version="0.1.0",
    description="Orchestration API for Portainer-centric agent operations.",
    lifespan=lifespan,
)


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
def dashboard() -> str:
    return _DASHBOARD


@app.get("/health")
def health() -> dict:
    portainer_ok = False
    if PORTAINER_BASE_URL and PORTAINER_API_TOKEN:
        portainer_ok = PortainerClient(PORTAINER_BASE_URL, PORTAINER_API_TOKEN).health_check()
    return {"status": "ok", "service": "orc-api", "portainer": portainer_ok}


@app.get("/registry/agents")
def registry_agents() -> dict:
    items = load_registry(REPO_ROOT, "agents")
    return {"count": len(items), "items": [asdict(item) for item in items]}


@app.get("/registry/skills")
def registry_skills() -> dict:
    items = load_registry(REPO_ROOT, "skills")
    return {"count": len(items), "items": [asdict(item) for item in items]}


@app.get("/events")
def get_events(limit: int = 50) -> dict:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    with SessionLocal() as session:
        items = (
            session.query(ObservedEvent)
            .order_by(ObservedEvent.occurred_at.desc())
            .limit(limit)
            .all()
        )
        count_24h = (
            session.query(func.count(ObservedEvent.id))
            .filter(ObservedEvent.occurred_at >= cutoff)
            .scalar()
        )
    return {
        "count": len(items),
        "count_24h": count_24h,
        "items": [
            {
                "id": e.id,
                "container_name": e.container_name,
                "severity": e.severity,
                "message": e.message,
                "occurred_at": e.occurred_at.isoformat(),
            }
            for e in items
        ],
    }
