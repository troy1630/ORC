from contextlib import asynccontextmanager
from dataclasses import asdict
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from pydantic import BaseModel
from sqlalchemy import func

from .config import REDIS_URL, REPO_ROOT
from .db import Connection, ObservedEvent, SessionLocal, init_db
from .raven import CHANNEL
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
body{background:var(--bg);color:var(--txt);font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;font-size:14px;height:100vh;display:flex;flex-direction:column;overflow:hidden}
.nav{background:var(--sur);border-bottom:1px solid var(--bdr);padding:0 20px;display:flex;align-items:center;gap:16px;height:52px;flex-shrink:0}
.brand{font-weight:700;font-size:1rem;margin-right:8px}
.tabs{display:flex;height:100%}
.tab{background:none;border:none;border-bottom:2px solid transparent;color:var(--mut);cursor:pointer;padding:0 14px;font-size:.9rem;font-weight:500;height:100%}
.tab:hover{color:var(--txt)}.tab.on{color:var(--txt);border-bottom-color:var(--pur)}
.nav-r{margin-left:auto;display:flex;align-items:center;gap:10px}
.layout{display:grid;grid-template-columns:1fr 300px;flex:1;overflow:hidden}
.main{overflow-y:auto;padding:20px}
.aside{border-left:1px solid var(--bdr);display:flex;flex-direction:column;overflow:hidden;background:var(--sur)}
.hb-wrap{padding:12px 14px 10px;border-bottom:1px solid var(--bdr);flex-shrink:0}
.hb-lbl{display:flex;justify-content:space-between;align-items:center;margin-bottom:6px}
.hb-title{font-size:.78rem;font-weight:600;letter-spacing:.04em;color:var(--txt)}
.hb-status{font-size:.7rem;color:var(--mut)}
canvas{display:block;width:100%;height:52px}
.feed-hdr{display:flex;gap:5px;padding:7px 10px;border-bottom:1px solid var(--bdr);flex-shrink:0}
.ff{background:#21262d;border:1px solid var(--bdr);border-radius:12px;color:var(--mut);cursor:pointer;font-size:.72rem;padding:3px 0;flex:1;text-align:center}
.ff:hover{color:var(--txt)}.ff.on{background:var(--bdr);color:var(--txt)}
.feed{flex:1;overflow-y:auto;padding:8px 10px 16px;display:flex;flex-direction:column}
.pill{margin-bottom:8px;border-radius:14px;padding:9px 12px;font-size:.78rem;border:1px solid transparent;animation:fadein .3s ease}
@keyframes fadein{from{opacity:0;transform:translateY(6px)}to{opacity:1;transform:none}}
.p-start{background:#161f2e;border-color:#1d2d45;color:var(--blu)}
.p-error{background:#2a1515;border-color:#4a2020;color:var(--red)}
.p-warn{background:#2a2000;border-color:#4a3800;color:var(--yel)}
.p-ok{background:#152215;border-color:#1f3d1f;color:var(--grn)}
.p-clean{background:#161b22;border-color:var(--bdr);color:var(--mut)}
.p-done{background:#1c1529;border-color:#2d2040;color:var(--pur);font-size:.75rem}
.p-checking{background:#161b22;border-color:#21262d;color:var(--mut)}
.ph{color:var(--mut);font-size:.82rem;text-align:center;padding:24px 0}
.pill-hdr{display:flex;justify-content:space-between;align-items:baseline;margin-bottom:2px}
.pill-cn{font-family:monospace;font-weight:600;font-size:.8rem}
.pill-sv{font-size:.7rem;opacity:.75}
.pill-ts{font-size:.7rem;opacity:.6;text-align:right;margin-top:2px}
.card{background:var(--sur);border:1px solid var(--bdr);border-radius:8px}
.pane{display:none}.pane.on{display:block}
.sg{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:18px}
.sc{padding:14px}
.lbl{font-size:.72rem;color:var(--mut);margin-bottom:4px}
.val{font-size:1.6rem;font-weight:700}
.dot{display:inline-block;width:9px;height:9px;border-radius:50%;margin-right:5px;vertical-align:middle;background:var(--mut)}
.dot.ok{background:var(--grn)}.dot.er{background:var(--red)}
.fbtn{background:#21262d;border:1px solid var(--bdr);border-radius:6px;color:var(--mut);cursor:pointer;font-size:.78rem;padding:3px 11px}
.fbtn:hover{color:var(--txt)}.fbtn.on{background:var(--bdr);color:var(--txt)}
.btn{background:#21262d;border:1px solid var(--bdr);border-radius:6px;color:var(--txt);cursor:pointer;font-size:.85rem;padding:5px 12px}
.btn:hover{background:var(--bdr)}
.btnp{background:var(--pur);border:none;border-radius:6px;color:#fff;cursor:pointer;font-size:.85rem;padding:6px 16px;font-weight:500}
.btnp:hover{filter:brightness(1.1)}
.btnd{background:#da3633;border:none;border-radius:6px;color:#fff;cursor:pointer;font-size:.78rem;padding:4px 10px}
.btns{background:#21262d;border:1px solid var(--bdr);border-radius:6px;color:var(--txt);cursor:pointer;font-size:.78rem;padding:4px 10px}
.btns:hover{background:var(--bdr)}
table{width:100%;border-collapse:collapse}
th{color:var(--mut);font-weight:500;padding:6px 10px;border-bottom:1px solid var(--bdr);text-align:left;font-size:.78rem}
td{padding:6px 10px;border-bottom:1px solid #21262d;font-size:.82rem;vertical-align:middle}
tr:last-child td{border-bottom:none}
.sc2{color:var(--red);font-weight:700}.se2{color:var(--red)}.sw2{color:var(--yel)}.si2{color:var(--mut)}
.scroll{max-height:440px;overflow-y:auto}
.msg{max-width:360px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.mono{font-family:monospace;font-size:.78rem}
.muted{color:var(--mut)}.small{font-size:.78rem}
.empty{color:var(--mut);padding:16px 0;font-size:.83rem}
.st-ok{color:var(--grn)}.st-er{color:var(--red)}.st-no{color:var(--mut)}
dialog{background:var(--sur);border:1px solid var(--bdr);border-radius:10px;color:var(--txt);padding:0;width:500px;max-width:96vw}
dialog::backdrop{background:rgba(0,0,0,.75)}
.mh{display:flex;justify-content:space-between;align-items:center;padding:14px 20px;border-bottom:1px solid var(--bdr);font-weight:600}
.mx{background:none;border:none;color:var(--mut);cursor:pointer;font-size:1.3rem;line-height:1}
.mx:hover{color:var(--txt)}
.mb{padding:18px 20px;display:flex;flex-direction:column;gap:13px}
.mf{padding:12px 20px;border-top:1px solid var(--bdr);display:flex;justify-content:flex-end;gap:8px}
.fg{display:flex;flex-direction:column;gap:4px}
.fg label{font-size:.78rem;color:var(--mut)}
.fg input,.fg select{background:#0d1117;border:1px solid var(--bdr);border-radius:6px;color:var(--txt);font-size:.88rem;padding:6px 10px;outline:none;width:100%}
.fg input:focus,.fg select:focus{border-color:var(--pur)}
.tr{border-radius:6px;font-size:.83rem;padding:7px 11px}
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

<div class="layout">
  <!-- MAIN CONTENT -->
  <div class="main">

    <!-- DASHBOARD -->
    <div class="pane on" id="pane-dash">
      <div class="sg">
        <div class="card sc"><div class="lbl">API</div><span class="dot" id="api-dot"></span><span id="api-txt">—</span></div>
        <div class="card sc"><div class="lbl">Servers</div><div class="val" id="srv-txt">—</div></div>
        <div class="card sc"><div class="lbl">Errors (24 h)</div><div class="val se2" id="err-cnt">—</div></div>
        <div class="card sc"><div class="lbl">Warnings (24 h)</div><div class="val sw2" id="warn-cnt">—</div></div>
      </div>
      <div class="card" style="padding:18px">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px">
          <div style="font-weight:600">Events</div>
          <div style="display:flex;gap:5px">
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
      <div class="card" style="padding:18px">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:14px">
          <div style="font-weight:600">Portainer Connections</div>
          <button class="btnp" onclick="openModal()">+ Add Connection</button>
        </div>
        <div id="conn-body"><div class="empty">Loading…</div></div>
      </div>
    </div>

  </div><!-- /main -->

  <!-- RAVEN SIDEBAR -->
  <aside class="aside">
    <div class="hb-wrap">
      <div class="hb-lbl">
        <span class="hb-title">RAVEN</span>
        <span class="hb-status" id="hb-status">connecting…</span>
      </div>
      <canvas id="hb-cv" height="52"></canvas>
    </div>
    <div class="feed-hdr">
      <button class="ff on" id="rf-all" onclick="setRF('')">All</button>
      <button class="ff" id="rf-error" onclick="setRF('error')">Errors</button>
      <button class="ff" id="rf-warning" onclick="setRF('warning')">Warnings</button>
    </div>
    <div class="feed" id="feed">
      <div class="ph">Waiting for activity…</div>
    </div>
  </aside>
</div>

<!-- MODAL -->
<dialog id="dlg">
  <div class="mh"><span id="dlg-t">Add Connection</span><button class="mx" onclick="closeDlg()">&#215;</button></div>
  <div class="mb">
    <div class="fg"><label>Name</label><input id="f-name" type="text" placeholder="Production Server 1" required></div>
    <div class="fg"><label>Type</label><select id="f-type"><option value="portainer">Portainer</option></select></div>
    <div class="fg"><label>URL</label><input id="f-url" type="text" placeholder="https://portainer.example.com" required></div>
    <div class="fg"><label id="f-tl">API Token</label><input id="f-tok" type="password" placeholder="API token"></div>
    <div class="fg"><label style="display:flex;align-items:center;gap:8px;color:var(--txt)"><input id="f-en" type="checkbox" checked> Enabled</label></div>
    <div id="tr" style="display:none"></div>
  </div>
  <div class="mf">
    <button class="btns" onclick="closeDlg()">Cancel</button>
    <button class="btns" onclick="testDlg()">Test Connection</button>
    <button class="btnp" onclick="saveDlg()">Save</button>
  </div>
</dialog>

<script>
/* ---- state ---- */
let _evts=[], _filter='', _conns=[], _editId=null;
let _pills=[], _hbData=new Array(40).fill(0), _hbBucket=0;
const MAX_PILLS=60;

/* ---- tabs ---- */
function showTab(id){
  document.querySelectorAll('.pane').forEach(p=>p.classList.remove('on'));
  document.querySelectorAll('.tab').forEach(t=>t.classList.remove('on'));
  document.getElementById('pane-'+id).classList.add('on');
  document.getElementById('tab-'+id).classList.add('on');
  if(id==='conn')loadConns();
}

/* ---- utils ---- */
function fmt(iso){const d=new Date(iso);return d.toLocaleDateString()+' '+d.toLocaleTimeString();}
function fmtShort(iso){return new Date(iso).toLocaleTimeString();}
function esc(s){return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');}

/* ---- dashboard ---- */
async function loadStatus(){
  try{
    const d=await fetch('/health').then(r=>r.json());
    document.getElementById('api-txt').textContent=d.status;
    document.getElementById('api-dot').className='dot ok';
    const c=d.connections;
    document.getElementById('srv-txt').textContent=c.total?`${c.ok}/${c.total}`:'—';
  }catch{
    document.getElementById('api-txt').textContent='Error';
    document.getElementById('api-dot').className='dot er';
  }
}
async function loadEvts(){
  try{
    const d=await fetch('/events?limit=200').then(r=>r.json());
    document.getElementById('err-cnt').textContent=d.err_24h??0;
    document.getElementById('warn-cnt').textContent=d.warn_24h??0;
    _evts=d.items; renderEvts();
  }catch{document.getElementById('ev-body').innerHTML='<div class="empty">Could not load events.</div>';}
}
function setFilter(s){
  _filter=s;
  ['all','critical','error','warning'].forEach(k=>document.getElementById('f-'+k).classList.toggle('on',k===(s||'all')));
  renderEvts();
}
function renderEvts(){
  const S={critical:'sc2',error:'se2',warning:'sw2',info:'si2',debug:'si2'};
  const items=_filter?_evts.filter(e=>e.severity===_filter):_evts;
  if(!items.length){
    document.getElementById('ev-body').innerHTML=`<div class="empty">${_evts.length?'No events match this filter.':'No events yet — worker polls each connection in turn.'}</div>`;
    return;
  }
  const rows=items.map(e=>`<tr>
    <td class="mono muted">${fmt(e.occurred_at)}</td>
    <td style="color:var(--pur);font-size:.78rem">${esc(e.server)}</td>
    <td class="mono" style="color:var(--blu)">${esc(e.container_name)}</td>
    <td><span class="${S[e.severity]||'si2'}">${e.severity}</span></td>
    <td class="msg" title="${esc(e.message)}">${esc(e.message)}</td>
  </tr>`).join('');
  document.getElementById('ev-body').innerHTML=`<div class="scroll"><table>
    <thead><tr><th>Time</th><th>Server</th><th>Container</th><th>Severity</th><th>Message</th></tr></thead>
    <tbody>${rows}</tbody></table></div>`;
}

/* ---- connections ---- */
async function loadConns(){
  try{
    _conns=await fetch('/connections').then(r=>r.json());
    if(!_conns.length){document.getElementById('conn-body').innerHTML='<div class="empty">No connections yet. Add a Portainer server to start ingesting logs.</div>';return;}
    const rows=_conns.map(c=>{
      const st=c.last_status==='ok'?'<span class="st-ok">&#10003; OK</span>':c.last_status==='error'?`<span class="st-er" title="${esc(c.last_error||'')}">&#10007; Error</span>`:'<span class="st-no">—</span>';
      return `<tr${c.enabled?'':' style="opacity:.5"'}>
        <td style="font-weight:500">${esc(c.name)}</td>
        <td class="mono muted" style="max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${esc(c.base_url)}</td>
        <td>${c.type}</td><td>${st}</td>
        <td class="muted small">${c.last_polled_at?fmt(c.last_polled_at):'Never'}</td>
        <td><div style="display:flex;gap:5px">
          <button class="btnp" style="font-size:.75rem;padding:4px 10px" onclick="pollNow(${c.id},this)">&#9654; Poll Now</button>
          <button class="btns" onclick="testEx(${c.id},this)">Test</button>
          <button class="btns" onclick="openModal(${c.id})">Edit</button>
          <button class="btnd" onclick="delConn(${c.id})">Delete</button>
        </div></td>
      </tr>`;
    }).join('');
    document.getElementById('conn-body').innerHTML=`<table><thead><tr><th>Name</th><th>URL</th><th>Type</th><th>Status</th><th>Last Polled</th><th>Actions</th></tr></thead><tbody>${rows}</tbody></table>`;
  }catch{document.getElementById('conn-body').innerHTML='<div class="empty">Could not load connections.</div>';}
}
function openModal(id){
  _editId=id||null;
  const c=id?_conns.find(x=>x.id===id):null;
  document.getElementById('dlg-t').textContent=c?'Edit Connection':'Add Connection';
  document.getElementById('f-name').value=c?c.name:'';
  document.getElementById('f-type').value=c?c.type:'portainer';
  document.getElementById('f-url').value=c?c.base_url:'';
  document.getElementById('f-tok').value='';
  document.getElementById('f-tok').placeholder=c?'Leave blank to keep existing token':'API token';
  document.getElementById('f-en').checked=c?c.enabled:true;
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
  const url=document.getElementById('f-url').value.trim(), tok=document.getElementById('f-tok').value;
  if(!url){showTr(false,'Enter a URL first.');return;}
  showTr(null,'Testing…');
  try{
    let d;
    if(tok){d=await fetch('/connections/test-url',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({base_url:url,api_token:tok})}).then(r=>r.json());}
    else if(_editId){d=await fetch(`/connections/${_editId}/test`,{method:'POST'}).then(r=>r.json());}
    else{showTr(false,'Enter an API token first.');return;}
    showTr(d.ok,d.ok?'Connection successful!':(d.error||'Connection failed'));
  }catch(e){showTr(false,'Request failed: '+e.message);}
}
async function saveDlg(){
  const name=document.getElementById('f-name').value.trim(), url=document.getElementById('f-url').value.trim(), tok=document.getElementById('f-tok').value;
  if(!name||!url){showTr(false,'Name and URL are required.');return;}
  if(!_editId&&!tok){showTr(false,'API token is required.');return;}
  const body={name,type:document.getElementById('f-type').value,base_url:url,api_token:tok,enabled:document.getElementById('f-en').checked};
  try{
    const r=await fetch(_editId?`/connections/${_editId}`:'/connections',{method:_editId?'PUT':'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
    if(!r.ok)throw new Error(await r.text());
    closeDlg(); await loadConns();
  }catch(e){showTr(false,'Save failed: '+e.message);}
}
async function testEx(id,btn){
  const orig=btn.textContent; btn.textContent='…'; btn.disabled=true;
  try{const d=await fetch(`/connections/${id}/test`,{method:'POST'}).then(r=>r.json());alert(d.ok?'✓ Connection successful!':'✗ '+(d.error||'Failed'));await loadConns();}
  finally{btn.textContent=orig;btn.disabled=false;}
}
async function delConn(id){
  if(!confirm('Delete this connection? Events will be preserved.'))return;
  await fetch(`/connections/${id}`,{method:'DELETE'}); await loadConns();
}
async function pollNow(id,btn){
  const orig=btn.textContent; btn.textContent='Polling…'; btn.disabled=true;
  try{
    const d=await fetch(`/connections/${id}/poll`,{method:'POST'}).then(r=>r.json());
    if(!d.ok)addPill({type:'poll_error',server:_conns.find(c=>c.id===id)?.name||'',error:d.error||'Poll failed'});
    await loadConns();
  }catch(e){
    addPill({type:'poll_error',server:'',error:'Request failed: '+e.message});
  }finally{btn.textContent=orig;btn.disabled=false;}
}

/* ---- raven / activity feed ---- */
let _ravenFilter='';

function setRF(f){
  _ravenFilter=f;
  ['all','error','warning'].forEach(k=>document.getElementById('rf-'+k).classList.toggle('on',k===(f||'all')));
  renderFeed(false);
}

function pillVisible(msg){
  if(_ravenFilter==='')return true;
  if(_ravenFilter==='error')
    return msg.type==='poll_error'||(msg.type==='container_result'&&msg.errors>0);
  if(_ravenFilter==='warning')
    return msg.type==='container_result'&&msg.warnings>0&&msg.errors===0;
  return true;
}

function pillHtml(msg){
  const ts=msg.ts?fmtShort(msg.ts):'';
  if(msg.type==='no_connections')
    return `<div class="pill p-start">No connections configured — add one in the Connections tab.</div>`;
  if(msg.type==='queue_ready'){
    const iv=msg.interval?` · ${msg.interval}s/container`:'';
    return `<div class="pill p-start">&#9654; Scanning <strong>${msg.containers}</strong> containers${iv}</div>`;
  }
  if(msg.type==='container_checking')
    return `<div class="pill p-checking">
      <div class="pill-hdr"><span class="pill-cn">${esc(msg.container)}</span><span class="pill-sv">${esc(msg.server)}</span></div>
      <div style="opacity:.7">checking…</div>
    </div>`;
  if(msg.type==='poll_error')
    return `<div class="pill p-error">&#10007; <strong>${esc(msg.server)}</strong><div style="opacity:.8;font-size:.75rem;margin-top:2px">${esc(msg.error||'')}</div></div>`;
  if(msg.type==='container_result'){
    let cls='p-clean',detail='no changes';
    if(msg.errors>0){cls='p-error';detail=`${msg.errors} error${msg.errors>1?'s':''}${msg.warnings>0?`, ${msg.warnings} warn`:''}`;
    }else if(msg.warnings>0){cls='p-warn';detail=`${msg.warnings} warning${msg.warnings>1?'s':''}`;
    }else if(msg.events>0){cls='p-ok';detail=`${msg.events} new event${msg.events>1?'s':''}`;}
    return `<div class="pill ${cls}">
      <div class="pill-hdr"><span class="pill-cn">${esc(msg.container)}</span><span class="pill-sv">${esc(msg.server)}</span></div>
      <div style="display:flex;justify-content:space-between"><span>${detail}</span><span class="pill-ts">${ts}</span></div>
    </div>`;
  }
  return '';
}

function addPill(msg){
  _pills.push(msg); // newest at bottom
  if(_pills.length>MAX_PILLS)_pills.shift();
  const feed=document.getElementById('feed');
  const nearBottom=feed.scrollHeight-feed.clientHeight<=feed.scrollTop+80;
  renderFeed(false);
  if(nearBottom)feed.scrollTop=feed.scrollHeight;
}

function renderFeed(scrollToBottom){
  const feed=document.getElementById('feed');
  const visible=_pills.filter(pillVisible);
  if(!visible.length){
    feed.innerHTML=`<div class="ph">${_pills.length?'No events match this filter.':'Waiting for activity…'}</div>`;
    return;
  }
  feed.innerHTML=visible.map(pillHtml).join('');
  if(scrollToBottom)feed.scrollTop=feed.scrollHeight;
}

/* ---- heartbeat chart ---- */
function resizeCanvas(){
  const cv=document.getElementById('hb-cv');
  if(cv)cv.width=cv.offsetWidth||280;
}
function drawHb(){
  const cv=document.getElementById('hb-cv');
  if(!cv)return;
  const ctx=cv.getContext('2d'), w=cv.width, h=cv.height;
  ctx.clearRect(0,0,w,h);
  const d=_hbData, max=Math.max(...d,1), step=w/(d.length-1);
  // fill
  ctx.beginPath();
  d.forEach((v,i)=>{const x=i*step,y=h-(v/max)*(h-6)-3;i===0?ctx.moveTo(x,y):ctx.lineTo(x,y);});
  ctx.lineTo((d.length-1)*step,h);ctx.lineTo(0,h);ctx.closePath();
  ctx.fillStyle='rgba(63,185,80,0.12)';ctx.fill();
  // line
  ctx.beginPath();
  d.forEach((v,i)=>{const x=i*step,y=h-(v/max)*(h-6)-3;i===0?ctx.moveTo(x,y):ctx.lineTo(x,y);});
  ctx.strokeStyle='#3fb950';ctx.lineWidth=1.5;ctx.stroke();
  // label
  const cur=d[d.length-1];
  ctx.fillStyle=cur>0?'#3fb950':'#8b949e';
  ctx.font='10px monospace';ctx.textAlign='right';
  ctx.fillText(cur>0?`${cur} evt/5s`:'idle',w-4,12);
}
function tickHb(){
  _hbData.push(_hbBucket); _hbBucket=0;
  if(_hbData.length>40)_hbData.shift();
  drawHb();
}
setInterval(tickHb,5000);

/* ---- SSE ---- */
function connectRaven(){
  const es=new EventSource('/raven/stream');
  document.getElementById('hb-status').textContent='connecting…';
  es.onopen=()=>document.getElementById('hb-status').textContent='live';
  es.onmessage=e=>{
    try{
      const msg=JSON.parse(e.data);
      if(msg.type==='connected'){document.getElementById('hb-status').textContent='live';return;}
      if(msg.type==='container_result')_hbBucket+=msg.events;
      const show=['container_result','container_checking','poll_error','queue_ready','no_connections'];
      if(show.includes(msg.type))addPill(msg);
    }catch{}
  };
  es.onerror=()=>{
    document.getElementById('hb-status').textContent='reconnecting…';
    es.close(); setTimeout(connectRaven,5000);
  };
}

/* ---- init ---- */
async function loadAll(){
  await Promise.all([loadStatus(),loadEvts()]);
  if(document.getElementById('pane-conn').classList.contains('on'))await loadConns();
  document.getElementById('upd').textContent='Updated '+new Date().toLocaleTimeString();
}

window.addEventListener('resize',()=>{resizeCanvas();drawHb();});
resizeCanvas(); drawHb();
loadAll();
setInterval(loadAll,30000);
connectRaven();
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
# Routes — UI
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse, include_in_schema=False)
def dashboard() -> str:
    return _HTML


# ---------------------------------------------------------------------------
# Routes — Raven SSE stream
# ---------------------------------------------------------------------------

@app.get("/raven/stream")
async def raven_stream() -> StreamingResponse:
    from redis.asyncio import Redis as ARedis

    async def _gen():
        r = ARedis.from_url(REDIS_URL, decode_responses=True)
        ps = r.pubsub()
        await ps.subscribe(CHANNEL)
        try:
            yield 'data: {"type":"connected"}\n\n'
            # Poll with 10s timeout; sends a keepalive comment on timeout so the
            # browser connection stays alive through proxies and load balancers.
            while True:
                msg = await ps.get_message(ignore_subscribe_messages=True, timeout=10.0)
                if msg and msg["type"] == "message":
                    yield f'data: {msg["data"]}\n\n'
                else:
                    yield ': ka\n\n'
        except Exception:
            pass
        finally:
            try:
                await ps.unsubscribe(CHANNEL)
                await r.aclose()
            except Exception:
                pass

    return StreamingResponse(
        _gen(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# ---------------------------------------------------------------------------
# Routes — Health
# ---------------------------------------------------------------------------

@app.get("/health")
def health() -> dict:
    with SessionLocal() as s:
        total = s.query(func.count(Connection.id)).scalar() or 0
        ok = s.query(func.count(Connection.id)).filter(Connection.last_status == "ok").scalar() or 0
        err = s.query(func.count(Connection.id)).filter(Connection.last_status == "error").scalar() or 0
    return {"status": "ok", "service": "orc-api", "connections": {"total": total, "ok": ok, "error": err}}


# ---------------------------------------------------------------------------
# Routes — Connections
# ---------------------------------------------------------------------------

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


@app.post("/connections/{cid}/poll")
def poll_now(cid: int) -> dict:
    """Immediately scan all containers on one connection (runs in API process)."""
    from .db import IngestionCheckpoint
    from .ingest import parse_logs
    from . import raven as _raven

    with SessionLocal() as s:
        c = s.get(Connection, cid)
        if not c:
            raise HTTPException(404, "Not found")
        conn_id, name, url, token = c.id, c.name, c.base_url, c.api_token

    client = PortainerClient(url, token)
    try:
        endpoints = client.get_endpoints()
    except Exception as exc:
        _raven.publish({"type": "poll_error", "server": name, "error": str(exc)})
        with SessionLocal() as s:
            c2 = s.get(Connection, conn_id)
            if c2:
                c2.last_status = "error"
                c2.last_error = str(exc)
                s.commit()
        return {"ok": False, "error": str(exc)}

    total_events = 0
    for ep in endpoints:
        eid = ep["Id"]
        try:
            containers = client.get_containers(eid)
        except Exception:
            continue
        for container in containers:
            cid_c = container["Id"]
            cname = (container.get("Names") or [f"/{cid_c[:12]}"])[0].lstrip("/")
            _raven.publish({"type": "container_checking", "server": name, "container": cname})
            try:
                with SessionLocal() as session:
                    chk = session.query(IngestionCheckpoint).filter_by(
                        connection_id=conn_id, endpoint_id=eid, container_id=cid_c
                    ).first()
                    since = chk.last_unix_ts if chk else 0
                    raw = client.get_container_logs(eid, cid_c, since=since)
                    events, last_ts = parse_logs(raw, conn_id, eid, cid_c, cname)
                    if events:
                        session.add_all(events)
                        if chk:
                            chk.last_unix_ts = last_ts
                        else:
                            session.add(IngestionCheckpoint(
                                connection_id=conn_id, endpoint_id=eid,
                                container_id=cid_c, last_unix_ts=last_ts,
                            ))
                        session.commit()
                    err_c  = sum(1 for e in events if e.severity in ("error", "critical"))
                    warn_c = sum(1 for e in events if e.severity == "warning")
                    _raven.publish({
                        "type": "container_result",
                        "server": name, "container": cname,
                        "events": len(events), "errors": err_c, "warnings": warn_c,
                    })
                    total_events += len(events)
            except Exception as exc2:
                log.warning("poll_now %s/%s: %s", name, cname, exc2)

    with SessionLocal() as s:
        c2 = s.get(Connection, conn_id)
        if c2:
            c2.last_status = "ok"
            c2.last_polled_at = datetime.now(timezone.utc)
            c2.last_error = None
            s.commit()

    return {"ok": True, "total_events": total_events}


# ---------------------------------------------------------------------------
# Routes — Registry + Events
# ---------------------------------------------------------------------------

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
