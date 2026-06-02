from contextlib import asynccontextmanager
from dataclasses import asdict
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from sqlalchemy import func

from .config import REPO_ROOT
from .db import Connection, ObservedEvent, SessionLocal, init_db
from .portainer import PortainerClient
from .registry import load_registry


# ---------------------------------------------------------------------------
# Pydantic schemas
# ---------------------------------------------------------------------------

class ConnectionIn(BaseModel):
    name: str
    type: str = "portainer"
    base_url: str
    api_token: str = ""
    enabled: bool = True


class ConnectionTestIn(BaseModel):
    base_url: str
    api_token: str


# ---------------------------------------------------------------------------
# App HTML
# ---------------------------------------------------------------------------

_HTML = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>ORC</title>
<style>
:root{--bg:#0d1117;--sur:#161b22;--bdr:#30363d;--txt:#e6edf3;--mut:#8b949e;--grn:#3fb950;--red:#f85149;--yel:#d29922;--blu:#58a6ff;--pur:#a371f7}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--txt);font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;font-size:14px}
.nav{background:var(--sur);border-bottom:1px solid var(--bdr);padding:0 20px;display:flex;align-items:center;gap:16px;height:52px}
.brand{font-weight:700;font-size:1rem;margin-right:8px}
.tabs{display:flex;height:100%}
.tab{background:none;border:none;border-bottom:2px solid transparent;color:var(--mut);cursor:pointer;padding:0 14px;font-size:.9rem;font-weight:500;height:100%}
.tab:hover{color:var(--txt)}.tab.on{color:var(--txt);border-bottom-color:var(--pur)}
.nav-r{margin-left:auto;display:flex;align-items:center;gap:10px}
.card{background:var(--sur);border:1px solid var(--bdr);border-radius:8px}
.pane{padding:20px;display:none}.pane.on{display:block}
.stat-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:16px;margin-bottom:20px}
.stat-card{padding:16px}
.lbl{font-size:.75rem;color:var(--mut);margin-bottom:4px}
.val{font-size:1.75rem;font-weight:700}
.dot{display:inline-block;width:10px;height:10px;border-radius:50%;margin-right:6px;vertical-align:middle;background:var(--mut)}
.dot.ok{background:var(--grn)}.dot.err{background:var(--red)}
.fbtn{background:#21262d;border:1px solid var(--bdr);border-radius:6px;color:var(--mut);cursor:pointer;font-size:.8rem;padding:4px 12px}
.fbtn:hover{color:var(--txt)}.fbtn.on{background:var(--bdr);color:var(--txt)}
.btn{background:#21262d;border:1px solid var(--bdr);border-radius:6px;color:var(--txt);cursor:pointer;font-size:.85rem;padding:5px 12px}
.btn:hover{background:var(--bdr)}
.btn-p{background:var(--pur);border:none;border-radius:6px;color:#fff;cursor:pointer;font-size:.85rem;padding:6px 16px;font-weight:500}
.btn-p:hover{filter:brightness(1.1)}
.btn-d{background:#da3633;border:none;border-radius:6px;color:#fff;cursor:pointer;font-size:.8rem;padding:4px 10px}
.btn-s{background:#21262d;border:1px solid var(--bdr);border-radius:6px;color:var(--txt);cursor:pointer;font-size:.8rem;padding:4px 10px}
.btn-s:hover{background:var(--bdr)}
table{width:100%;border-collapse:collapse}
th{color:var(--mut);font-weight:500;padding:7px 10px;border-bottom:1px solid var(--bdr);text-align:left;font-size:.8rem}
td{padding:7px 10px;border-bottom:1px solid #21262d;font-size:.85rem;vertical-align:middle}
tr:last-child td{border-bottom:none}
.sc{color:var(--red);font-weight:700}.se{color:var(--red)}.sw{color:var(--yel)}.si{color:var(--mut)}
.scroll{max-height:500px;overflow-y:auto}
.msg{max-width:380px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.mono{font-family:monospace;font-size:.8rem}
.muted{color:var(--mut)}.small{font-size:.8rem}
.empty{color:var(--mut);padding:16px 0;font-size:.85rem}
.st-ok{color:var(--grn)}.st-er{color:var(--red)}.st-no{color:var(--mut)}
dialog{background:var(--sur);border:1px solid var(--bdr);border-radius:10px;color:var(--txt);padding:0;width:500px;max-width:96vw}
dialog::backdrop{background:rgba(0,0,0,.75)}
.mhdr{display:flex;justify-content:space-between;align-items:center;padding:16px 20px;border-bottom:1px solid var(--bdr);font-weight:600}
.mclose{background:none;border:none;color:var(--mut);cursor:pointer;font-size:1.3rem;line-height:1}
.mclose:hover{color:var(--txt)}
.mbody{padding:20px;display:flex;flex-direction:column;gap:14px}
.mftr{padding:14px 20px;border-top:1px solid var(--bdr);display:flex;justify-content:flex-end;gap:8px}
.fg{display:flex;flex-direction:column;gap:5px}
.fg label{font-size:.8rem;color:var(--mut)}
.fg input,.fg select{background:#0d1117;border:1px solid var(--bdr);border-radius:6px;color:var(--txt);font-size:.9rem;padding:7px 10px;outline:none;width:100%}
.fg input:focus,.fg select:focus{border-color:var(--pur)}
.tr{border-radius:6px;font-size:.85rem;padding:8px 12px;margin-top:4px}
.tr-ok{background:#1a3a1a;color:var(--grn);border:1px solid #2d5a2d}
.tr-er{background:#3a1a1a;color:var(--red);border:1px solid #5a2d2d}
.tr-no{background:#21262d;color:var(--mut);border:1px solid var(--bdr)}
</style>
</head>
<body>
<nav class="nav">
  <span class="brand">&#9876; ORC</span>
  <div class="tabs">
    <button class="tab on" id="tab-dash" onclick="showTab('dash')">Dashboard</button>
    <button class="tab" id="tab-conn" onclick="showTab('conn')">Connections</button>
  </div>
  <div class="nav-r">
    <span class="muted small" id="upd"></span>
    <button class="btn" onclick="loadAll()">&#8635; Refresh</button>
  </div>
</nav>

<!-- DASHBOARD -->
<div class="pane on" id="pane-dash">
  <div class="stat-grid">
    <div class="card stat-card">
      <div class="lbl">API</div>
      <span class="dot" id="api-dot"></span><span id="api-txt">—</span>
    </div>
    <div class="card stat-card">
      <div class="lbl">Servers</div>
      <div class="val" id="srv-txt">—</div>
    </div>
    <div class="card stat-card">
      <div class="lbl">Errors (24 h)</div>
      <div class="val se" id="err-cnt">—</div>
    </div>
    <div class="card stat-card">
      <div class="lbl">Warnings (24 h)</div>
      <div class="val sw" id="warn-cnt">—</div>
    </div>
  </div>
  <div class="card" style="padding:20px">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:14px">
      <div style="font-weight:600">Events</div>
      <div style="display:flex;gap:6px">
        <button class="fbtn on" id="f-all" onclick="setFilter('')">All</button>
        <button class="fbtn" id="f-critical" onclick="setFilter('critical')">Critical</button>
        <button class="fbtn" id="f-error" onclick="setFilter('error')">Errors</button>
        <button class="fbtn" id="f-warning" onclick="setFilter('warning')">Warnings</button>
      </div>
    </div>
    <div id="ev-body"><div class="empty">Loading…</div></div>
  </div>
</div>

<!-- CONNECTIONS -->
<div class="pane" id="pane-conn">
  <div class="card" style="padding:20px">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px">
      <div style="font-weight:600">Portainer Connections</div>
      <button class="btn-p" onclick="openModal()">+ Add Connection</button>
    </div>
    <div id="conn-body"><div class="empty">Loading…</div></div>
  </div>
</div>

<!-- MODAL -->
<dialog id="dlg">
  <div class="mhdr">
    <span id="dlg-title">Add Connection</span>
    <button class="mclose" onclick="closeDlg()">&#215;</button>
  </div>
  <div class="mbody">
    <div class="fg"><label>Name</label>
      <input id="f-name" type="text" placeholder="Production Server 1" required></div>
    <div class="fg"><label>Type</label>
      <select id="f-type"><option value="portainer">Portainer</option></select></div>
    <div class="fg"><label>URL</label>
      <input id="f-url" type="text" placeholder="https://portainer.example.com" required></div>
    <div class="fg"><label id="f-token-lbl">API Token</label>
      <input id="f-token" type="password" placeholder="API token"></div>
    <div class="fg"><label style="display:flex;align-items:center;gap:8px;color:var(--txt)">
      <input id="f-enabled" type="checkbox" checked> Enabled</label></div>
    <div id="tr" style="display:none"></div>
  </div>
  <div class="mftr">
    <button class="btn-s" onclick="closeDlg()">Cancel</button>
    <button class="btn-s" onclick="testDlg()">Test Connection</button>
    <button class="btn-p" onclick="saveDlg()">Save</button>
  </div>
</dialog>

<script>
let _evts=[], _filter='', _conns=[], _editId=null;

function showTab(id){
  document.querySelectorAll('.pane').forEach(p=>p.classList.remove('on'));
  document.querySelectorAll('.tab').forEach(t=>t.classList.remove('on'));
  document.getElementById('pane-'+id).classList.add('on');
  document.getElementById('tab-'+id).classList.add('on');
  if(id==='conn') loadConns();
}

function setFilter(s){
  _filter=s;
  ['all','critical','error','warning'].forEach(k=>{
    document.getElementById('f-'+k).classList.toggle('on', k===(s||'all'));
  });
  renderEvts();
}

function fmt(iso){const d=new Date(iso);return d.toLocaleDateString()+' '+d.toLocaleTimeString();}
function esc(s){return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');}

/* --- DASHBOARD --- */
async function loadStatus(){
  try{
    const d=await fetch('/health').then(r=>r.json());
    document.getElementById('api-txt').textContent=d.status;
    document.getElementById('api-dot').className='dot ok';
    const c=d.connections;
    document.getElementById('srv-txt').textContent=c.total?`${c.ok}/${c.total}`:'—';
  }catch{
    document.getElementById('api-txt').textContent='Error';
    document.getElementById('api-dot').className='dot err';
  }
}

async function loadEvts(){
  try{
    const d=await fetch('/events?limit=200').then(r=>r.json());
    document.getElementById('err-cnt').textContent=d.err_24h??0;
    document.getElementById('warn-cnt').textContent=d.warn_24h??0;
    _evts=d.items;
    renderEvts();
  }catch{
    document.getElementById('ev-body').innerHTML='<div class="empty">Could not load events.</div>';
  }
}

function renderEvts(){
  const SEV={critical:'sc',error:'se',warning:'sw',info:'si',debug:'si'};
  const items=_filter?_evts.filter(e=>e.severity===_filter):_evts;
  if(!items.length){
    document.getElementById('ev-body').innerHTML=
      `<div class="empty">${_evts.length?'No events match this filter.':'No events yet — worker polls Portainer every 60 s after a connection is configured.'}</div>`;
    return;
  }
  const rows=items.map(e=>`<tr>
    <td class="mono muted">${fmt(e.occurred_at)}</td>
    <td style="color:var(--pur);font-size:.82rem">${esc(e.server)}</td>
    <td class="mono" style="color:var(--blu)">${esc(e.container_name)}</td>
    <td><span class="${SEV[e.severity]||'si'}">${e.severity}</span></td>
    <td class="msg" title="${esc(e.message)}">${esc(e.message)}</td>
  </tr>`).join('');
  document.getElementById('ev-body').innerHTML=`<div class="scroll"><table>
    <thead><tr><th>Time</th><th>Server</th><th>Container</th><th>Severity</th><th>Message</th></tr></thead>
    <tbody>${rows}</tbody></table></div>`;
}

/* --- CONNECTIONS --- */
async function loadConns(){
  try{
    _conns=await fetch('/connections').then(r=>r.json());
    if(!_conns.length){
      document.getElementById('conn-body').innerHTML=
        '<div class="empty">No connections yet. Add a Portainer server to start ingesting logs.</div>';
      return;
    }
    const rows=_conns.map(c=>{
      const st=c.last_status==='ok'
        ?'<span class="st-ok">&#10003; OK</span>'
        :c.last_status==='error'
        ?`<span class="st-er" title="${esc(c.last_error||'')}">&#10007; Error</span>`
        :'<span class="st-no">—</span>';
      const dis=c.enabled?'':' style="opacity:.5"';
      return `<tr${dis}>
        <td style="font-weight:500">${esc(c.name)}</td>
        <td class="mono muted" style="max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${esc(c.base_url)}</td>
        <td>${c.type}</td>
        <td>${st}</td>
        <td class="muted small">${c.last_polled_at?fmt(c.last_polled_at):'Never'}</td>
        <td><div style="display:flex;gap:6px">
          <button class="btn-s" onclick="testExisting(${c.id},this)">Test</button>
          <button class="btn-s" onclick="openModal(${c.id})">Edit</button>
          <button class="btn-d" onclick="delConn(${c.id})">Delete</button>
        </div></td>
      </tr>`;
    }).join('');
    document.getElementById('conn-body').innerHTML=`<table>
      <thead><tr><th>Name</th><th>URL</th><th>Type</th><th>Status</th><th>Last Polled</th><th>Actions</th></tr></thead>
      <tbody>${rows}</tbody></table>`;
  }catch{
    document.getElementById('conn-body').innerHTML='<div class="empty">Could not load connections.</div>';
  }
}

function openModal(id){
  _editId=id||null;
  const c=id?_conns.find(x=>x.id===id):null;
  document.getElementById('dlg-title').textContent=c?'Edit Connection':'Add Connection';
  document.getElementById('f-name').value=c?c.name:'';
  document.getElementById('f-type').value=c?c.type:'portainer';
  document.getElementById('f-url').value=c?c.base_url:'';
  document.getElementById('f-token').value='';
  document.getElementById('f-token').placeholder=c?'Leave blank to keep existing token':'API token';
  document.getElementById('f-enabled').checked=c?c.enabled:true;
  document.getElementById('tr').style.display='none';
  document.getElementById('dlg').showModal();
}

function closeDlg(){document.getElementById('dlg').close();}

function showTr(ok,msg){
  const el=document.getElementById('tr');
  el.style.display='block';
  el.className='tr '+(ok===true?'tr-ok':ok===false?'tr-er':'tr-no');
  el.textContent=(ok===true?'✓ ':ok===false?'✗ ':'')+msg;
}

async function testDlg(){
  const url=document.getElementById('f-url').value.trim();
  const token=document.getElementById('f-token').value;
  if(!url){showTr(false,'Enter a URL first.');return;}
  if(!token&&!_editId){showTr(false,'Enter an API token first.');return;}
  showTr(null,'Testing…');
  try{
    let d;
    if(token){
      d=await fetch('/connections/test-url',{method:'POST',headers:{'Content-Type':'application/json'},
        body:JSON.stringify({base_url:url,api_token:token})}).then(r=>r.json());
    }else{
      d=await fetch(`/connections/${_editId}/test`,{method:'POST'}).then(r=>r.json());
    }
    showTr(d.ok, d.ok?'Connection successful!':(d.error||'Connection failed'));
  }catch(e){showTr(false,'Request failed: '+e.message);}
}

async function saveDlg(){
  const name=document.getElementById('f-name').value.trim();
  const url=document.getElementById('f-url').value.trim();
  const token=document.getElementById('f-token').value;
  if(!name||!url){showTr(false,'Name and URL are required.');return;}
  if(!_editId&&!token){showTr(false,'API token is required for new connections.');return;}
  const body={name,type:document.getElementById('f-type').value,base_url:url,
    api_token:token,enabled:document.getElementById('f-enabled').checked};
  try{
    const r=await fetch(_editId?`/connections/${_editId}`:'/connections',
      {method:_editId?'PUT':'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
    if(!r.ok)throw new Error(await r.text());
    closeDlg();
    await loadConns();
  }catch(e){showTr(false,'Save failed: '+e.message);}
}

async function testExisting(id,btn){
  const orig=btn.textContent;
  btn.textContent='…';btn.disabled=true;
  try{
    const d=await fetch(`/connections/${id}/test`,{method:'POST'}).then(r=>r.json());
    alert(d.ok?'✓ Connection successful!':'✗ '+(d.error||'Failed'));
    await loadConns();
  }finally{btn.textContent=orig;btn.disabled=false;}
}

async function delConn(id){
  if(!confirm('Delete this connection? Existing events will be preserved.'))return;
  await fetch(`/connections/${id}`,{method:'DELETE'});
  await loadConns();
}

async function loadAll(){
  await Promise.all([loadStatus(),loadEvts()]);
  if(document.getElementById('pane-conn').classList.contains('on'))await loadConns();
  document.getElementById('upd').textContent='Updated '+new Date().toLocaleTimeString();
}

loadAll();
setInterval(loadAll,30000);
</script>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


app = FastAPI(title="ORC API", version="0.1.0", lifespan=lifespan)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse, include_in_schema=False)
def dashboard() -> str:
    return _HTML


@app.get("/health")
def health() -> dict:
    with SessionLocal() as s:
        total = s.query(func.count(Connection.id)).scalar() or 0
        ok = s.query(func.count(Connection.id)).filter(Connection.last_status == "ok").scalar() or 0
        err = s.query(func.count(Connection.id)).filter(Connection.last_status == "error").scalar() or 0
    return {"status": "ok", "service": "orc-api", "connections": {"total": total, "ok": ok, "error": err}}


@app.get("/connections")
def list_connections() -> list:
    with SessionLocal() as s:
        return [_cdct(c) for c in s.query(Connection).order_by(Connection.name).all()]


@app.post("/connections", status_code=201)
def create_connection(body: ConnectionIn) -> dict:
    with SessionLocal() as s:
        c = Connection(
            name=body.name, type=body.type,
            base_url=body.base_url.rstrip("/"), api_token=body.api_token,
            enabled=body.enabled,
        )
        s.add(c)
        s.commit()
        s.refresh(c)
        return _cdct(c)


@app.put("/connections/{cid}")
def update_connection(cid: int, body: ConnectionIn) -> dict:
    with SessionLocal() as s:
        c = s.get(Connection, cid)
        if not c:
            raise HTTPException(404, "Not found")
        c.name = body.name
        c.type = body.type
        c.base_url = body.base_url.rstrip("/")
        c.enabled = body.enabled
        if body.api_token:
            c.api_token = body.api_token
        s.commit()
        s.refresh(c)
        return _cdct(c)


@app.delete("/connections/{cid}", status_code=204)
def delete_connection(cid: int) -> None:
    with SessionLocal() as s:
        c = s.get(Connection, cid)
        if not c:
            raise HTTPException(404, "Not found")
        s.delete(c)
        s.commit()


@app.post("/connections/test-url")
def test_connection_url(body: ConnectionTestIn) -> dict:
    ok = PortainerClient(body.base_url.rstrip("/"), body.api_token).health_check()
    return {"ok": ok, "error": None if ok else "Could not reach Portainer API"}


@app.post("/connections/{cid}/test")
def test_connection(cid: int) -> dict:
    with SessionLocal() as s:
        c = s.get(Connection, cid)
        if not c:
            raise HTTPException(404, "Not found")
        url, token = c.base_url, c.api_token
    ok = PortainerClient(url, token).health_check()
    return {"ok": ok, "error": None if ok else "Could not reach Portainer API"}


@app.get("/registry/agents")
def registry_agents() -> dict:
    items = load_registry(REPO_ROOT, "agents")
    return {"count": len(items), "items": [asdict(item) for item in items]}


@app.get("/registry/skills")
def registry_skills() -> dict:
    items = load_registry(REPO_ROOT, "skills")
    return {"count": len(items), "items": [asdict(item) for item in items]}


@app.get("/events")
def get_events(limit: int = 200) -> dict:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    with SessionLocal() as s:
        rows = (
            s.query(ObservedEvent, Connection)
            .outerjoin(Connection, ObservedEvent.connection_id == Connection.id)
            .order_by(ObservedEvent.occurred_at.desc())
            .limit(limit)
            .all()
        )
        err_24h = s.query(func.count(ObservedEvent.id)).filter(
            ObservedEvent.occurred_at >= cutoff,
            ObservedEvent.severity.in_(["error", "critical"]),
        ).scalar() or 0
        warn_24h = s.query(func.count(ObservedEvent.id)).filter(
            ObservedEvent.occurred_at >= cutoff,
            ObservedEvent.severity == "warning",
        ).scalar() or 0
    return {
        "err_24h": err_24h,
        "warn_24h": warn_24h,
        "items": [
            {
                "id": e.id,
                "server": c.name if c else "—",
                "container_name": e.container_name,
                "severity": e.severity,
                "message": e.message,
                "occurred_at": e.occurred_at.isoformat(),
            }
            for e, c in rows
        ],
    }


def _cdct(c: Connection) -> dict:
    return {
        "id": c.id, "name": c.name, "type": c.type, "base_url": c.base_url,
        "api_token": c.api_token, "enabled": c.enabled,
        "last_polled_at": c.last_polled_at.isoformat() if c.last_polled_at else None,
        "last_status": c.last_status, "last_error": c.last_error,
    }
