from contextlib import asynccontextmanager
from dataclasses import asdict
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from pydantic import BaseModel
from sqlalchemy import func, text as _sa_text

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
    poll_interval_seconds: int | None = None


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
/* NAV */
.nav{background:var(--sur);border-bottom:1px solid var(--bdr);padding:0 16px;display:flex;align-items:center;gap:10px;height:48px;flex-shrink:0}
.brand{font-weight:700;font-size:1rem;margin-right:4px}
.tabs{display:flex;height:100%}
.tab{background:none;border:none;border-bottom:2px solid transparent;color:var(--mut);cursor:pointer;padding:0 12px;font-size:.88rem;font-weight:500;height:100%}
.tab:hover{color:var(--txt)}.tab.on{color:var(--txt);border-bottom-color:var(--pur)}
.nav-r{margin-left:auto;display:flex;align-items:center;gap:8px}
/* Status pills */
.sp{font-size:.75rem;padding:2px 8px;border-radius:10px;background:#21262d;border:1px solid var(--bdr);display:flex;align-items:center;gap:4px}
.dot{display:inline-block;width:8px;height:8px;border-radius:50%;background:var(--mut)}
.dot.ok{background:var(--grn)}.dot.er{background:var(--red)}
/* Layout */
.layout{display:grid;grid-template-columns:1fr 292px;flex:1;overflow:hidden}
.main{overflow-y:auto;padding:16px}
.pane{display:none}.pane.on{display:block}
/* STACK MAP */
.map-grid{display:flex;flex-wrap:wrap;gap:12px}
.stack-card{background:var(--sur);border:1px solid var(--bdr);border-radius:10px;padding:14px 10px 12px;width:152px;text-align:center;transition:border-color .2s}
.stack-card:hover{border-color:var(--pur)}
.char-svg{display:flex;justify-content:center;margin-bottom:6px}
.char-svg svg{width:68px;height:88px}
.stack-nm{font-size:.78rem;font-weight:600;margin-bottom:1px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.stack-sv{font-size:.68rem;color:var(--mut);margin-bottom:10px}
.ci-row{display:flex;flex-wrap:wrap;justify-content:center;gap:5px}
.ci{position:relative;cursor:pointer;display:flex;flex-direction:column;align-items:center;gap:2px;padding:4px 5px;border-radius:6px;transition:background .15s}
.ci:hover{background:#21262d}
.ci svg{width:20px;height:20px}
.ci-lbl{font-size:.6rem;color:var(--mut);max-width:36px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.badge{position:absolute;top:-4px;right:-4px;min-width:16px;height:16px;border-radius:8px;font-size:.6rem;font-weight:700;display:flex;align-items:center;justify-content:center;padding:0 3px;border:1.5px solid var(--bg)}
.b-ok{background:var(--grn);color:#000}
.b-warn{background:var(--yel);color:#000}
.b-err{background:var(--red);color:#fff}
.b-hide{display:none}
/* EVENTS */
.card{background:var(--sur);border:1px solid var(--bdr);border-radius:8px}
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
.scroll{max-height:calc(100vh - 200px);overflow-y:auto}
.msg{max-width:360px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.mono{font-family:monospace;font-size:.78rem}
.muted{color:var(--mut)}.small{font-size:.78rem}
.empty{color:var(--mut);padding:16px 0;font-size:.83rem}
/* RAVEN */
.aside{border-left:1px solid var(--bdr);display:flex;flex-direction:column;overflow:hidden;background:var(--sur)}
.hb-wrap{padding:10px 12px 8px;border-bottom:1px solid var(--bdr);flex-shrink:0}
.hb-lbl{display:flex;justify-content:space-between;align-items:center;margin-bottom:5px}
.hb-title{font-size:.75rem;font-weight:700;letter-spacing:.06em}
.hb-st{font-size:.68rem;color:var(--mut)}
canvas{display:block;width:100%;height:48px}
.raven-sl{padding:7px 12px;border-bottom:1px solid var(--bdr);font-size:.76rem;color:var(--mut);flex-shrink:0;min-height:32px;display:flex;align-items:center;gap:5px;overflow:hidden}
.raven-sl.live{color:var(--txt)}
.sl-icon{flex-shrink:0}.sl-txt{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.feed-hdr{display:flex;gap:4px;padding:6px 10px;border-bottom:1px solid var(--bdr);flex-shrink:0}
.ff{background:#21262d;border:1px solid var(--bdr);border-radius:12px;color:var(--mut);cursor:pointer;font-size:.7rem;padding:2px 0;flex:1;text-align:center}
.ff:hover{color:var(--txt)}.ff.on{background:var(--bdr);color:var(--txt)}
.feed{flex:1;overflow-y:auto;padding:8px 10px 16px;display:flex;flex-direction:column}
.pill{margin-bottom:7px;border-radius:12px;padding:8px 11px;font-size:.76rem;border:1px solid transparent}
.p-start{background:#161f2e;border-color:#1d2d45;color:var(--blu)}
.p-error{background:#2a1515;border-color:#4a2020;color:var(--red)}
.p-warn{background:#2a2000;border-color:#4a3800;color:var(--yel)}
.p-ok{background:#152215;border-color:#1f3d1f;color:var(--grn)}
.p-clean{background:#161b22;border-color:var(--bdr);color:var(--mut)}
.p-checking{background:#161b22;border-color:#21262d;color:var(--mut)}
.ph{color:var(--mut);font-size:.8rem;text-align:center;padding:20px 0}
.pill-hdr{display:flex;justify-content:space-between;align-items:baseline;margin-bottom:2px}
.pill-cn{font-family:monospace;font-weight:600;font-size:.78rem}
.pill-sv{font-size:.68rem;opacity:.75}
.pill-ts{font-size:.68rem;opacity:.6;text-align:right;margin-top:2px}
/* CONNECTIONS */
.st-ok{color:var(--grn)}.st-er{color:var(--red)}.st-no{color:var(--mut)}
/* MODAL */
dialog{background:var(--sur);border:1px solid var(--bdr);border-radius:10px;color:var(--txt);padding:0;width:500px;max-width:96vw}
dialog::backdrop{background:rgba(0,0,0,.75)}
.mh{display:flex;justify-content:space-between;align-items:center;padding:14px 18px;border-bottom:1px solid var(--bdr);font-weight:600}
.mx{background:none;border:none;color:var(--mut);cursor:pointer;font-size:1.3rem;line-height:1}
.mx:hover{color:var(--txt)}
.mb{padding:16px 18px;display:flex;flex-direction:column;gap:12px}
.mf{padding:12px 18px;border-top:1px solid var(--bdr);display:flex;justify-content:flex-end;gap:8px}
.fg{display:flex;flex-direction:column;gap:4px}
.fg label{font-size:.78rem;color:var(--mut)}
.fg input,.fg select{background:#0d1117;border:1px solid var(--bdr);border-radius:6px;color:var(--txt);font-size:.88rem;padding:6px 10px;outline:none;width:100%}
.fg input:focus,.fg select:focus{border-color:var(--pur)}
.tr{border-radius:6px;font-size:.82rem;padding:7px 11px}
.tr-ok{background:#1a3a1a;color:var(--grn);border:1px solid #2d5a2d}
.tr-er{background:#3a1a1a;color:var(--red);border:1px solid #5a2d2d}
.tr-no{background:#21262d;color:var(--mut);border:1px solid var(--bdr)}
</style>
</head>
<body>
<nav class="nav">
  <span class="brand">&#9876; ORC</span>
  <div class="tabs">
    <button class="tab on" id="tab-map" onclick="showTab('map')">Map</button>
    <button class="tab" id="tab-events" onclick="showTab('events')">Events</button>
    <button class="tab" id="tab-conn" onclick="showTab('conn')">Connections</button>
  </div>
  <div class="nav-r">
    <span class="sp"><span class="dot" id="api-dot"></span><span id="api-txt">API</span></span>
    <span class="sp"><span id="srv-txt" style="color:var(--mut)">—</span> <span style="color:var(--mut)">srv</span></span>
    <span class="sp" style="cursor:pointer" onclick="showTab('events');setEvFilter('severity','error')">
      <span id="err-cnt" class="se2">—</span><span class="muted"> err</span>
    </span>
    <span class="sp" style="cursor:pointer" onclick="showTab('events');setEvFilter('severity','warning')">
      <span id="warn-cnt" class="sw2">—</span><span class="muted"> warn</span>
    </span>
    <span class="small muted" id="upd"></span>
    <button class="btn" onclick="loadAll()">&#8635;</button>
  </div>
</nav>

<div class="layout">
<div class="main">

  <!-- MAP -->
  <div class="pane on" id="pane-map">
    <div class="map-grid" id="map-grid"><div class="empty">Loading stack map…</div></div>
  </div>

  <!-- EVENTS -->
  <div class="pane" id="pane-events">
    <div class="card" style="padding:16px">
      <div style="display:flex;flex-wrap:wrap;gap:7px;align-items:center;margin-bottom:12px">
        <div style="font-weight:600;margin-right:4px">Events</div>
        <div style="display:flex;gap:4px">
          <button class="fbtn on" id="f-all" onclick="setEvFilter('severity','')">All</button>
          <button class="fbtn" id="f-critical" onclick="setEvFilter('severity','critical')">Critical</button>
          <button class="fbtn" id="f-error" onclick="setEvFilter('severity','error')">Errors</button>
          <button class="fbtn" id="f-warning" onclick="setEvFilter('severity','warning')">Warnings</button>
        </div>
        <select id="ev-server" onchange="setEvFilter('server',this.value)" style="background:#0d1117;border:1px solid var(--bdr);border-radius:6px;color:var(--txt);font-size:.76rem;padding:3px 8px;cursor:pointer">
          <option value="">All servers</option>
        </select>
        <input id="ev-container" placeholder="Container…" oninput="setEvFilter('container',this.value)"
          style="background:#0d1117;border:1px solid var(--bdr);border-radius:6px;color:var(--txt);font-size:.76rem;padding:3px 8px;width:120px;outline:none">
        <button class="fbtn" onclick="clearEvFilters()" id="ev-clear" style="display:none">&#215; Clear</button>
      </div>
      <div id="ev-body"><div class="empty">Loading…</div></div>
    </div>
  </div>

  <!-- CONNECTIONS -->
  <div class="pane" id="pane-conn">
    <div class="card" style="padding:16px">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:14px">
        <div style="font-weight:600">Portainer Connections</div>
        <button class="btnp" onclick="openModal()">+ Add Connection</button>
      </div>
      <div id="conn-body"><div class="empty">Loading…</div></div>
    </div>
  </div>

</div><!-- /main -->

<!-- RAVEN -->
<aside class="aside">
  <div class="hb-wrap">
    <div class="hb-lbl">
      <span class="hb-title">RAVEN</span>
      <span class="hb-st" id="hb-status">connecting…</span>
    </div>
    <canvas id="hb-cv" height="48"></canvas>
  </div>
  <div class="raven-sl" id="raven-sl">
    <span class="sl-icon" id="sl-icon">—</span>
    <span class="sl-txt" id="sl-txt">Waiting for activity…</span>
  </div>
  <div class="feed-hdr">
    <button class="ff on" id="rf-all" onclick="setRF('')">All</button>
    <button class="ff" id="rf-error" onclick="setRF('error')">Errors</button>
    <button class="ff" id="rf-warning" onclick="setRF('warning')">Warnings</button>
  </div>
  <div class="feed" id="feed"><div class="ph">No issues found yet.</div></div>
</aside>
</div><!-- /layout -->

<!-- MODAL -->
<dialog id="dlg">
  <div class="mh"><span id="dlg-t">Add Connection</span><button class="mx" onclick="closeDlg()">&#215;</button></div>
  <div class="mb">
    <div class="fg"><label>Name</label><input id="f-name" type="text" placeholder="Production Server 1" required></div>
    <div class="fg"><label>Type</label><select id="f-type"><option value="portainer">Portainer</option></select></div>
    <div class="fg"><label>URL</label><input id="f-url" type="text" placeholder="https://portainer.example.com" required></div>
    <div class="fg"><label id="f-tl">API Token</label><input id="f-tok" type="password" placeholder="API token"></div>
    <div class="fg"><label>Poll interval (seconds per container)</label><input id="f-interval" type="number" min="1" max="120" placeholder="Auto (100 ÷ containers)"></div>
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
/* ============================================================
   STATE
   ============================================================ */
let _evts=[], _evFilters={severity:'',container:'',server:''};
let _conns=[], _editId=null;
let _hbData=new Array(40).fill(0), _hbBucket=0;
let _ravenFilter='', _issuePills=[];
const MAX_ISSUE_PILLS=20;

/* ============================================================
   CHARACTER & ICON SVGs
   ============================================================ */
const CHARS={
orc:`<svg viewBox="0 0 60 88" xmlns="http://www.w3.org/2000/svg">
  <rect x="12" y="52" width="36" height="28" rx="5" fill="#2d6a2d"/>
  <rect x="4" y="54" width="13" height="20" rx="5" fill="#2d6a2d"/>
  <rect x="43" y="54" width="13" height="20" rx="5" fill="#2d6a2d"/>
  <ellipse cx="11" cy="30" rx="6" ry="9" fill="#4a9e4a"/>
  <ellipse cx="49" cy="30" rx="6" ry="9" fill="#4a9e4a"/>
  <circle cx="30" cy="30" r="21" fill="#4a9e4a"/>
  <circle cx="21" cy="25" r="6" fill="#d4f0d4"/><circle cx="39" cy="25" r="6" fill="#d4f0d4"/>
  <circle cx="22" cy="25" r="3" fill="#1a3a1a"/><circle cx="40" cy="25" r="3" fill="#1a3a1a"/>
  <ellipse cx="30" cy="33" rx="5" ry="3.5" fill="#3a8a3a"/>
  <rect x="22" y="40" width="5" height="12" rx="2.5" fill="#fffacd"/>
  <rect x="33" y="40" width="5" height="12" rx="2.5" fill="#fffacd"/>
  <path d="M22 38 Q30 44 38 38" stroke="#2a5a2a" stroke-width="2" fill="none"/>
  <rect x="12" y="52" width="36" height="5" fill="#8B6914"/>
</svg>`,
wizard:`<svg viewBox="0 0 60 88" xmlns="http://www.w3.org/2000/svg">
  <path d="M13 52 L5 86 L55 86 L47 52 Z" fill="#4a3b8c"/>
  <rect x="17" y="42" width="26" height="14" rx="4" fill="#5a4a9c"/>
  <circle cx="30" cy="28" r="17" fill="#f5deb3"/>
  <ellipse cx="30" cy="13" rx="22" ry="4.5" fill="#2d1b5e"/>
  <path d="M10 13 L30 -6 L50 13 Z" fill="#2d1b5e"/>
  <circle cx="22" cy="26" r="4.5" fill="#fff"/><circle cx="38" cy="26" r="4.5" fill="#fff"/>
  <circle cx="22.5" cy="26" r="2.5" fill="#1a0066"/><circle cx="38.5" cy="26" r="2.5" fill="#1a0066"/>
  <path d="M20 34 Q30 42 40 34 L38 44 Q30 49 22 44 Z" fill="#d8d8d8"/>
  <rect x="52" y="28" width="3" height="52" rx="1.5" fill="#8B6914"/>
  <circle cx="53.5" cy="25" r="6.5" fill="#4fc3f7" opacity="0.9"/>
  <circle cx="53.5" cy="25" r="3" fill="#fff" opacity="0.6"/>
</svg>`,
fighter:`<svg viewBox="0 0 60 88" xmlns="http://www.w3.org/2000/svg">
  <rect x="17" y="62" width="11" height="24" rx="3" fill="#7a3b1a"/>
  <rect x="32" y="62" width="11" height="24" rx="3" fill="#7a3b1a"/>
  <rect x="11" y="42" width="38" height="24" rx="5" fill="#c8830a"/>
  <rect x="3" y="40" width="14" height="14" rx="5" fill="#d89030"/>
  <rect x="43" y="40" width="14" height="14" rx="5" fill="#d89030"/>
  <rect x="3" y="50" width="13" height="18" rx="4" fill="#c8830a"/>
  <rect x="44" y="50" width="13" height="18" rx="4" fill="#c8830a"/>
  <circle cx="30" cy="26" r="17" fill="#e8a060"/>
  <path d="M13 23 Q13 7 30 7 Q47 7 47 23" fill="#b8730a"/>
  <rect x="25" y="10" width="10" height="18" rx="2" fill="#9a5a0a"/>
  <circle cx="22" cy="26" r="4" fill="#fff"/><circle cx="38" cy="26" r="4" fill="#fff"/>
  <circle cx="22.5" cy="26" r="2" fill="#1a1a1a"/><circle cx="38.5" cy="26" r="2" fill="#1a1a1a"/>
  <path d="M32 21 L39 27" stroke="#c85000" stroke-width="1.5"/>
  <rect x="53" y="18" width="3" height="48" rx="1.5" fill="#b8b8b8"/>
  <rect x="47" y="30" width="15" height="3.5" rx="1.5" fill="#9a5a0a"/>
</svg>`,
ranger:`<svg viewBox="0 0 60 88" xmlns="http://www.w3.org/2000/svg">
  <path d="M10 48 L4 86 L56 86 L50 48 Z" fill="#3a5a2a"/>
  <rect x="15" y="40" width="30" height="14" rx="4" fill="#4a6a3a"/>
  <circle cx="30" cy="26" r="16" fill="#c8905a"/>
  <path d="M15 26 Q15 10 30 9 Q45 10 45 26 L43 21 Q30 16 17 21 Z" fill="#2a4a1a"/>
  <circle cx="23" cy="26" r="3.5" fill="#fff"/><circle cx="37" cy="26" r="3.5" fill="#fff"/>
  <circle cx="23.5" cy="26" r="2" fill="#2d1b00"/><circle cx="37.5" cy="26" r="2" fill="#2d1b00"/>
  <path d="M23 33 Q30 38 37 33" stroke="#8B4513" stroke-width="1.5" fill="none"/>
  <path d="M53 14 Q61 34 53 56" stroke="#6b3a1a" stroke-width="2.5" fill="none"/>
  <line x1="53" y1="14" x2="53" y2="56" stroke="#d4aa70" stroke-width="1"/>
  <line x1="49" y1="18" x2="57" y2="26" stroke="#d4aa70" stroke-width="0.8"/>
</svg>`};

const ICONS={
api:`<svg viewBox="0 0 20 20" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round">
  <rect x="2" y="4" width="16" height="12" rx="2"/>
  <line x1="5" y1="8" x2="15" y2="8"/>
  <line x1="5" y1="11" x2="11" y2="11"/>
</svg>`,
worker:`<svg viewBox="0 0 20 20" fill="none" stroke="currentColor" stroke-width="1.5">
  <circle cx="10" cy="10" r="3"/>
  <path d="M10 1v3M10 16v3M1 10h3M16 10h3M3.5 3.5l2.1 2.1M14.4 14.4l2.1 2.1M3.5 16.5l2.1-2.1M14.4 5.6l2.1-2.1"/>
</svg>`,
db:`<svg viewBox="0 0 20 20" fill="none" stroke="currentColor" stroke-width="1.5">
  <ellipse cx="10" cy="5" rx="7" ry="2.5"/>
  <path d="M3 5v10q0 2.5 7 2.5t7-2.5V5"/>
  <path d="M3 10q0 2.5 7 2.5t7-2.5"/>
</svg>`,
cache:`<svg viewBox="0 0 20 20" fill="currentColor">
  <path d="M11.5 2 L14 8h5l-4 3 1.5 6L11.5 14 6 17l1.5-6-4-3h5Z"/>
</svg>`,
ui:`<svg viewBox="0 0 20 20" fill="none" stroke="currentColor" stroke-width="1.5">
  <rect x="2" y="3" width="16" height="11" rx="2"/>
  <line x1="7" y1="18" x2="13" y2="18"/>
  <line x1="10" y1="14" x2="10" y2="18"/>
</svg>`,
service:`<svg viewBox="0 0 20 20" fill="none" stroke="currentColor" stroke-width="1.5">
  <path d="M10 2l6 3.5v7L10 16l-6-3.5v-7Z"/>
</svg>`};

/* ============================================================
   UTILS
   ============================================================ */
function showTab(id){
  document.querySelectorAll('.pane').forEach(p=>p.classList.remove('on'));
  document.querySelectorAll('.tab').forEach(t=>t.classList.remove('on'));
  document.getElementById('pane-'+id).classList.add('on');
  document.getElementById('tab-'+id).classList.add('on');
  if(id==='conn')loadConns();
  if(id==='map')loadMap();
  if(id==='events')loadEvts();
}
function fmt(iso){const d=new Date(iso);return d.toLocaleDateString()+' '+d.toLocaleTimeString();}
function fmtShort(iso){return new Date(iso).toLocaleTimeString();}
function esc(s){return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');}

/* ============================================================
   STATUS BAR
   ============================================================ */
async function loadStatus(){
  try{
    const d=await fetch('/health').then(r=>r.json());
    document.getElementById('api-txt').textContent=d.status;
    document.getElementById('api-dot').className='dot ok';
    const c=d.connections;
    document.getElementById('srv-txt').textContent=c.total?`${c.ok}/${c.total}`:'—';
  }catch{
    document.getElementById('api-txt').textContent='err';
    document.getElementById('api-dot').className='dot er';
  }
}

/* ============================================================
   STACK MAP
   ============================================================ */
async function loadMap(){
  try{
    const d=await fetch('/overview').then(r=>r.json());
    renderMap(d.stacks);
  }catch(e){
    document.getElementById('map-grid').innerHTML='<div class="empty">Could not load stack map.</div>';
  }
}

function renderMap(stacks){
  const grid=document.getElementById('map-grid');
  if(!stacks.length){grid.innerHTML='<div class="empty">No stacks found. Add a connection in the Connections tab.</div>';return;}
  grid.innerHTML=stacks.map(s=>{
    const char=CHARS[s.character]||CHARS.orc;
    const icons=s.containers.map(c=>{
      const icon=ICONS[c.type]||ICONS.service;
      const hasErr=c.errors_1h>0, hasWarn=c.warnings_1h>0;
      const badgeCls=hasErr?'b-err':hasWarn?'b-warn':c.errors_1h===0&&c.warnings_1h===0?'b-ok':'b-hide';
      const badgeTxt=hasErr?c.errors_1h:hasWarn?c.warnings_1h:'';
      const badgeVis=(hasErr||hasWarn)?'':'b-hide';
      const click=`jumpToEvents('${esc(s.server)}','${esc(c.full_name)}','${hasErr?'error':hasWarn?'warning':''}')`;
      const col=hasErr?'var(--red)':hasWarn?'var(--yel)':'var(--mut)';
      return `<div class="ci" onclick="${click}" title="${esc(c.full_name)}">
        <div style="color:${col}">${icon}</div>
        <span class="badge ${badgeVis?badgeCls:'b-hide'}">${badgeTxt}</span>
        <span class="ci-lbl">${esc(c.name.substring(0,8))}</span>
      </div>`;
    }).join('');
    const totalErr=s.containers.reduce((a,c)=>a+c.errors_1h,0);
    const totalWarn=s.containers.reduce((a,c)=>a+c.warnings_1h,0);
    const cardBorder=totalErr>0?'border-color:var(--red)':totalWarn>0?'border-color:var(--yel)':'';
    return `<div class="stack-card" style="${cardBorder}">
      <div class="char-svg">${char}</div>
      <div class="stack-nm" title="${esc(s.name)}">${esc(s.name)}</div>
      <div class="stack-sv">${esc(s.server)}</div>
      <div class="ci-row">${icons}</div>
    </div>`;
  }).join('');
}

/* ============================================================
   EVENTS
   ============================================================ */
function _evUrl(){
  const p=new URLSearchParams({limit:200});
  if(_evFilters.severity)p.set('severity',_evFilters.severity);
  if(_evFilters.container)p.set('container',_evFilters.container);
  if(_evFilters.server)p.set('server',_evFilters.server);
  return '/events?'+p.toString();
}
async function loadEvts(){
  try{
    const d=await fetch(_evUrl()).then(r=>r.json());
    document.getElementById('err-cnt').textContent=d.err_24h??0;
    document.getElementById('warn-cnt').textContent=d.warn_24h??0;
    _evts=d.items; renderEvts();
  }catch{document.getElementById('ev-body').innerHTML='<div class="empty">Could not load events.</div>';}
}
function setEvFilter(key,val){
  _evFilters[key]=val;
  if(key==='severity'){
    ['all','critical','error','warning'].forEach(k=>
      document.getElementById('f-'+k).classList.toggle('on',k===(val||'all')));
  }
  document.getElementById('ev-clear').style.display=Object.values(_evFilters).some(v=>v)?'':'none';
  loadEvts();
}
function clearEvFilters(){
  _evFilters={severity:'',container:'',server:''};
  document.getElementById('ev-container').value='';
  document.getElementById('ev-server').value='';
  ['all','critical','error','warning'].forEach(k=>document.getElementById('f-'+k).classList.toggle('on',k==='all'));
  document.getElementById('ev-clear').style.display='none';
  loadEvts();
}
function jumpToEvents(server,container,sev){
  _evFilters={severity:sev||'error',container:container||'',server:server||''};
  document.getElementById('ev-container').value=container||'';
  const sel=document.getElementById('ev-server');
  if(sel)sel.value=server||'';
  ['all','critical','error','warning'].forEach(k=>
    document.getElementById('f-'+k).classList.toggle('on',k===(_evFilters.severity||'all')));
  document.getElementById('ev-clear').style.display='';
  showTab('events');
  loadEvts();
}
function _populateServerDropdown(){
  const sel=document.getElementById('ev-server');
  const cur=sel.value;
  sel.innerHTML='<option value="">All servers</option>'+
    _conns.map(c=>`<option value="${esc(c.name)}"${c.name===cur?' selected':''}>${esc(c.name)}</option>`).join('');
}
function renderEvts(){
  const S={critical:'sc2',error:'se2',warning:'sw2',info:'si2',debug:'si2'};
  if(!_evts.length){
    document.getElementById('ev-body').innerHTML=
      `<div class="empty">${Object.values(_evFilters).some(v=>v)?'No events match these filters.':'No events yet — worker polls each connection in turn.'}</div>`;
    return;
  }
  const rows=_evts.map(e=>`<tr>
    <td class="mono muted">${fmt(e.occurred_at)}</td>
    <td style="color:var(--pur);font-size:.76rem;cursor:pointer" onclick="jumpToEvents('${esc(e.server)}','','error')">${esc(e.server)}</td>
    <td class="mono" style="color:var(--blu);cursor:pointer" onclick="jumpToEvents('${esc(e.server)}','${esc(e.container_name)}','error')">${esc(e.container_name)}</td>
    <td><span class="${S[e.severity]||'si2'}">${e.severity}</span></td>
    <td class="msg" title="${esc(e.message)}">${esc(e.message)}</td>
  </tr>`).join('');
  document.getElementById('ev-body').innerHTML=`<div class="scroll"><table>
    <thead><tr><th>Time</th><th>Server</th><th>Container</th><th>Severity</th><th>Message</th></tr></thead>
    <tbody>${rows}</tbody></table></div>`;
}

/* ============================================================
   CONNECTIONS
   ============================================================ */
async function loadConns(){
  try{
    _conns=await fetch('/connections').then(r=>r.json());
    _populateServerDropdown();
    if(!_conns.length){document.getElementById('conn-body').innerHTML='<div class="empty">No connections yet. Add a Portainer server to start ingesting logs.</div>';return;}
    const rows=_conns.map(c=>{
      const st=c.last_status==='ok'?'<span class="st-ok">&#10003; OK</span>':c.last_status==='error'?`<span class="st-er" title="${esc(c.last_error||'')}">&#10007; Error</span>`:'<span class="st-no">—</span>';
      return `<tr${c.enabled?'':' style="opacity:.5"'}>
        <td style="font-weight:500">${esc(c.name)}</td>
        <td class="mono muted" style="max-width:170px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${esc(c.base_url)}</td>
        <td>${c.type}</td><td>${st}</td>
        <td class="muted small">${c.last_polled_at?fmt(c.last_polled_at):'Never'}</td>
        <td><div style="display:flex;gap:5px">
          <button class="btnp" style="font-size:.72rem;padding:3px 9px" onclick="pollNow(${c.id},this)">&#9654; Poll</button>
          <button class="btns" onclick="testEx(${c.id},this)">Test</button>
          <button class="btns" onclick="openModal(${c.id})">Edit</button>
          <button class="btnd" onclick="delConn(${c.id})">Delete</button>
        </div></td>
      </tr>`;
    }).join('');
    document.getElementById('conn-body').innerHTML=`<table>
      <thead><tr><th>Name</th><th>URL</th><th>Type</th><th>Status</th><th>Last Polled</th><th>Actions</th></tr></thead>
      <tbody>${rows}</tbody></table>`;
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
  document.getElementById('f-interval').value=c&&c.poll_interval_seconds?c.poll_interval_seconds:'';
  document.getElementById('f-en').checked=c?c.enabled:true;
  document.getElementById('tr').style.display='none';
  document.getElementById('dlg').showModal();
}
function closeDlg(){document.getElementById('dlg').close();}
function showTr(ok,msg){const el=document.getElementById('tr');el.style.display='block';el.className='tr '+(ok===true?'tr-ok':ok===false?'tr-er':'tr-no');el.textContent=(ok===true?'✓ ':ok===false?'✗ ':'')+msg;}
async function testDlg(){
  const url=document.getElementById('f-url').value.trim(),tok=document.getElementById('f-tok').value;
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
  const name=document.getElementById('f-name').value.trim(),url=document.getElementById('f-url').value.trim(),tok=document.getElementById('f-tok').value;
  if(!name||!url){showTr(false,'Name and URL are required.');return;}
  if(!_editId&&!tok){showTr(false,'API token is required.');return;}
  const iv=document.getElementById('f-interval').value;
  const body={name,type:document.getElementById('f-type').value,base_url:url,api_token:tok,
    enabled:document.getElementById('f-en').checked,poll_interval_seconds:iv?parseInt(iv):null};
  try{
    const r=await fetch(_editId?`/connections/${_editId}`:'/connections',
      {method:_editId?'PUT':'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
    if(!r.ok)throw new Error(await r.text());
    closeDlg();await loadConns();
  }catch(e){showTr(false,'Save failed: '+e.message);}
}
async function testEx(id,btn){const o=btn.textContent;btn.textContent='…';btn.disabled=true;try{const d=await fetch(`/connections/${id}/test`,{method:'POST'}).then(r=>r.json());alert(d.ok?'✓ Connection successful!':'✗ '+(d.error||'Failed'));await loadConns();}finally{btn.textContent=o;btn.disabled=false;}}
async function delConn(id){if(!confirm('Delete this connection?'))return;await fetch(`/connections/${id}`,{method:'DELETE'});await loadConns();}
async function pollNow(id,btn){
  const o=btn.textContent;btn.textContent='…';btn.disabled=true;
  try{const d=await fetch(`/connections/${id}/poll`,{method:'POST'}).then(r=>r.json());if(!d.ok)alert('✗ '+(d.error||'Poll failed'));await loadConns();}
  catch(e){alert('Error: '+e.message);}
  finally{btn.textContent=o;btn.disabled=false;}
}

/* ============================================================
   RAVEN
   ============================================================ */
function setRF(f){
  _ravenFilter=f;
  ['all','error','warning'].forEach(k=>document.getElementById('rf-'+k).classList.toggle('on',k===(f||'all')));
  renderFeed();
}
function setStatus(icon,text,live){
  document.getElementById('sl-icon').textContent=icon;
  document.getElementById('sl-txt').textContent=text;
  document.getElementById('raven-sl').className='raven-sl'+(live?' live':'');
}
function addIssuePill(msg){
  _issuePills.push(msg);
  if(_issuePills.length>MAX_ISSUE_PILLS)_issuePills.shift();
  renderFeed();
}
function _filteredPills(){
  if(_ravenFilter==='error') return _issuePills.filter(m=>m.type==='poll_error'||(m.type==='container_result'&&(m.recent_errors||m.errors)>0));
  if(_ravenFilter==='warning') return _issuePills.filter(m=>m.type==='container_result'&&(m.recent_warnings||m.warnings)>0&&!(m.recent_errors||m.errors));
  return _issuePills;
}
function issuePillHtml(msg,opacity,isCurrent){
  const ts=msg.ts?fmtShort(msg.ts):'';
  const accent=isCurrent?'border-left:3px solid currentColor;padding-left:9px;':'';
  const style=`opacity:${opacity};${accent}`;
  if(msg.type==='poll_error')
    return `<div class="pill p-error" style="${style}">✗ <strong>${esc(msg.server)}</strong><div style="font-size:.72rem;margin-top:2px;opacity:.85">${esc(msg.error||'')}</div></div>`;
  if(msg.type==='container_result'){
    const re=msg.recent_errors||0,rw=msg.recent_warnings||0,ne=msg.errors||0,nw=msg.warnings||0;
    let cls,detail,sev;
    if(re>0||ne>0){cls='p-error';sev='error';const t=re||ne;detail=`${t} error${t!==1?'s':''} (24h)`;const tw=rw||nw;if(tw>0)detail+=`, ${tw} warn`;}
    else{cls='p-warn';sev='warning';const tw=rw||nw;detail=`${tw} warning${tw!==1?'s':''} (24h)`;}
    const click=`jumpToEvents('${esc(msg.server)}','${esc(msg.container)}','${sev}')`;
    return `<div class="pill ${cls}" style="${style};cursor:pointer" onclick="${click}" title="Click to filter Events">
      <div class="pill-hdr"><span class="pill-cn">${esc(msg.container)}</span><span class="pill-sv">${esc(msg.server)}</span></div>
      <div style="display:flex;justify-content:space-between;margin-top:2px"><span>${detail}</span><span class="pill-ts">${ts}</span></div>
    </div>`;
  }
  return '';
}
function renderFeed(){
  const feed=document.getElementById('feed');
  const src=_filteredPills().slice(-5);
  if(!src.length){feed.innerHTML='<div class="ph">No issues found yet.</div>';return;}
  const n=src.length,ops=[0.15,0.35,0.55,0.75,1.0];
  feed.innerHTML=src.map((msg,i)=>issuePillHtml(msg,ops[i+(5-n)]??1.0,i===n-1)).join('');
  feed.scrollTop=feed.scrollHeight;
}
function handleRaven(msg){
  switch(msg.type){
    case 'no_connections': setStatus('—','No connections configured.',false); break;
    case 'queue_ready':{const iv=msg.interval?` · ${msg.interval}s/ctr`:'';setStatus('▶',`Scanning ${msg.containers} containers${iv}`,true);break;}
    case 'container_checking': setStatus('🔍',`Checking ${msg.container} on ${msg.server}`,true); break;
    case 'container_result':{
      // Heartbeat: track errors+warnings per container scanned
      _hbBucket+=(msg.recent_errors||0)+(msg.recent_warnings||0);
      const re=msg.recent_errors||0,rw=msg.recent_warnings||0,ne=msg.errors||0,nw=msg.warnings||0;
      if(re>0||ne>0){const t=re||ne;setStatus('⚠',`${msg.container} · ${t} error${t!==1?'s':''} (24h)`,true);addIssuePill(msg);}
      else if(rw>0||nw>0){const tw=rw||nw;setStatus('⚠',`${msg.container} · ${tw} warning${tw!==1?'s':''} (24h)`,true);addIssuePill(msg);}
      else setStatus('✓',`${msg.container} · no changes`,true);
      break;
    }
    case 'poll_error': setStatus('✗',`${msg.server}: ${msg.error||'connection failed'}`,true);addIssuePill(msg); break;
  }
}

/* ============================================================
   HEARTBEAT CHART
   ============================================================ */
function resizeCanvas(){const cv=document.getElementById('hb-cv');if(cv)cv.width=cv.offsetWidth||270;}
function drawHb(){
  const cv=document.getElementById('hb-cv');if(!cv)return;
  const ctx=cv.getContext('2d'),w=cv.width,h=cv.height;
  ctx.clearRect(0,0,w,h);
  const d=_hbData,max=Math.max(...d,1),step=w/(d.length-1);
  // gradient fill under line
  const grad=ctx.createLinearGradient(0,0,0,h);
  grad.addColorStop(0,'rgba(248,81,73,0.25)');
  grad.addColorStop(1,'rgba(248,81,73,0.02)');
  ctx.beginPath();
  d.forEach((v,i)=>{const x=i*step,y=h-(v/max)*(h-6)-3;i===0?ctx.moveTo(x,y):ctx.lineTo(x,y);});
  ctx.lineTo((d.length-1)*step,h);ctx.lineTo(0,h);ctx.closePath();
  ctx.fillStyle=grad;ctx.fill();
  // line — red when active, muted when idle
  const cur=d[d.length-1];
  ctx.beginPath();
  d.forEach((v,i)=>{const x=i*step,y=h-(v/max)*(h-6)-3;i===0?ctx.moveTo(x,y):ctx.lineTo(x,y);});
  ctx.strokeStyle=cur>0?'#f85149':'#30363d';ctx.lineWidth=1.5;ctx.stroke();
  // label
  ctx.fillStyle=cur>0?'#f85149':'#8b949e';
  ctx.font='10px monospace';ctx.textAlign='right';
  ctx.fillText(cur>0?`${cur} issues/5s`:'idle',w-4,11);
}
function tickHb(){_hbData.push(_hbBucket);_hbBucket=0;if(_hbData.length>40)_hbData.shift();drawHb();}
setInterval(tickHb,5000);

/* ============================================================
   SSE
   ============================================================ */
function connectRaven(){
  const es=new EventSource('/raven/stream');
  document.getElementById('hb-status').textContent='connecting…';
  es.onopen=()=>document.getElementById('hb-status').textContent='live';
  es.onmessage=e=>{
    try{const msg=JSON.parse(e.data);if(msg.type==='connected'){document.getElementById('hb-status').textContent='live';return;}handleRaven(msg);}
    catch{}
  };
  es.onerror=()=>{document.getElementById('hb-status').textContent='reconnecting…';es.close();setTimeout(connectRaven,5000);};
}

/* ============================================================
   INIT
   ============================================================ */
async function loadAll(){
  _conns=await fetch('/connections').then(r=>r.json()).catch(()=>_conns);
  _populateServerDropdown();
  await Promise.all([loadStatus(),loadEvts(),loadMap()]);
  document.getElementById('upd').textContent=new Date().toLocaleTimeString();
}
window.addEventListener('resize',()=>{resizeCanvas();drawHb();});
resizeCanvas();drawHb();
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
            enabled=body.enabled, poll_interval_seconds=body.poll_interval_seconds,
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
        c.poll_interval_seconds = body.poll_interval_seconds
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
def get_events(
    limit: int = 200,
    severity: str = "",
    container: str = "",
    server: str = "",
) -> dict:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    with SessionLocal() as s:
        q = (
            s.query(ObservedEvent, Connection)
            .outerjoin(Connection, ObservedEvent.connection_id == Connection.id)
            .order_by(ObservedEvent.occurred_at.desc())
        )
        if severity:
            if severity == "error":
                q = q.filter(ObservedEvent.severity.in_(["error", "critical"]))
            else:
                q = q.filter(ObservedEvent.severity == severity)
        if container:
            q = q.filter(ObservedEvent.container_name.ilike(f"%{container}%"))
        if server:
            q = q.filter(Connection.name == server)

        rows = q.limit(limit).all()

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


def _infer_stack(cname: str) -> str:
    parts = cname.split("-")
    return "-".join(parts[:-2]) if len(parts) >= 3 else cname


def _container_type(name: str) -> str:
    n = name.lower()
    if any(x in n for x in ("postgres", "mysql", "mongo", "db", "sqlite", "maria")): return "db"
    if any(x in n for x in ("redis", "cache", "memcache", "rabbit", "kafka")): return "cache"
    if any(x in n for x in ("worker", "celery", "cron", "job", "task", "beat")): return "worker"
    if any(x in n for x in ("web", "frontend", "ui", "nginx", "react", "next", "vue")): return "ui"
    return "api"


def _stack_character(name: str) -> str:
    n = name.lower()
    if "orc" in n: return "orc"
    if any(x in n for x in ("ai", "ml", "kpi", "chatbot", "advisor", "analytics", "tower")): return "wizard"
    if any(x in n for x in ("simulator", "sppm", "presentation", "ux2")): return "fighter"
    return ("orc", "ranger", "wizard", "fighter")[abs(hash(name)) % 4]


@app.get("/overview")
def get_overview() -> dict:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=1)
    with SessionLocal() as s:
        connections = s.query(Connection).filter_by(enabled=True).all()
        err_rows = s.query(
            ObservedEvent.connection_id, ObservedEvent.container_name,
            func.count(ObservedEvent.id).label("n")
        ).filter(
            ObservedEvent.occurred_at >= cutoff,
            ObservedEvent.severity.in_(["error", "critical"])
        ).group_by(ObservedEvent.connection_id, ObservedEvent.container_name).all()

        warn_rows = s.query(
            ObservedEvent.connection_id, ObservedEvent.container_name,
            func.count(ObservedEvent.id).label("n")
        ).filter(
            ObservedEvent.occurred_at >= cutoff,
            ObservedEvent.severity == "warning"
        ).group_by(ObservedEvent.connection_id, ObservedEvent.container_name).all()

    errs  = {(r.connection_id, r.container_name): int(r.n) for r in err_rows}
    warns = {(r.connection_id, r.container_name): int(r.n) for r in warn_rows}

    stacks_out = []
    for conn in connections:
        client = PortainerClient(conn.base_url, conn.api_token)
        try:
            endpoints = client.get_endpoints()
        except Exception:
            continue
        stacks: dict[str, list] = {}
        for ep in endpoints:
            try:
                for c in client.get_containers(ep["Id"]):
                    cid = c["Id"]
                    cname = (c.get("Names") or [f"/{cid[:12]}"])[0].lstrip("/")
                    labels = c.get("Labels") or {}
                    stack_name = labels.get("com.docker.compose.project") or _infer_stack(cname)
                    service   = labels.get("com.docker.compose.service")  or cname
                    stacks.setdefault(stack_name, []).append({
                        "name": service,
                        "full_name": cname,
                        "type": _container_type(service),
                        "errors_1h":   errs.get((conn.id, cname), 0),
                        "warnings_1h": warns.get((conn.id, cname), 0),
                    })
            except Exception:
                continue
        for sname, containers in sorted(stacks.items()):
            stacks_out.append({
                "name": sname, "server": conn.name,
                "character": _stack_character(sname),
                "containers": sorted(containers, key=lambda x: x["type"]),
            })
    return {"stacks": stacks_out}


def _cdct(c: Connection) -> dict:
    return {
        "id": c.id, "name": c.name, "type": c.type, "base_url": c.base_url,
        "api_token": c.api_token, "enabled": c.enabled,
        "poll_interval_seconds": c.poll_interval_seconds,
        "last_polled_at": c.last_polled_at.isoformat() if c.last_polled_at else None,
        "last_status": c.last_status, "last_error": c.last_error,
    }
