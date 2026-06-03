import json
import os
import re
from collections import Counter
from contextlib import asynccontextmanager
from dataclasses import asdict
from datetime import datetime, timedelta, timezone

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from sqlalchemy import func, text as _sa_text

from .config import REDIS_URL, REPO_ROOT
from .db import Connection, ObservedEvent, SessionLocal, init_db
from .raven import CHANNEL
from .portainer import PortainerClient
from .registry import load_registry

OPENAI_API_URL = "https://api.openai.com/v1/chat/completions"
DEFAULT_OPENAI_MODEL = "gpt-4o-mini"
_ORACLE_UUID_RE = re.compile(r"\b[0-9a-f]{8,}\b", re.I)
_ORACLE_NUM_RE = re.compile(r"\b\d+\b")


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
.nav{background:rgba(22,27,34,.96);border-bottom:1px solid var(--bdr);padding:0 18px;display:flex;align-items:center;gap:14px;height:64px;flex-shrink:0;backdrop-filter:blur(12px)}
.brand{font-weight:800;font-size:1.14rem;margin-right:8px;display:flex;align-items:center;gap:10px;letter-spacing:.02em}
.brand-mark{width:46px;height:46px;border-radius:50%;overflow:hidden;display:inline-flex;align-items:center;justify-content:center;background:#0d1117;border:1px solid rgba(230,237,243,.24);box-shadow:0 0 0 1px rgba(0,0,0,.55) inset,0 8px 18px rgba(0,0,0,.28);flex-shrink:0}
.brand-mark img{width:148%;height:148%;object-fit:cover;object-position:center 36%;filter:saturate(.95) contrast(1.06);-webkit-mask-image:radial-gradient(circle at center,#000 46%,rgba(0,0,0,.75) 63%,transparent 84%);mask-image:radial-gradient(circle at center,#000 46%,rgba(0,0,0,.75) 63%,transparent 84%)}
.tabs{display:flex;height:100%}
.tab{background:none;border:none;border-bottom:2px solid transparent;color:var(--mut);cursor:pointer;padding:0 14px;font-size:.94rem;font-weight:600;height:100%}
.tab:hover{color:var(--txt)}.tab.on{color:var(--txt);border-bottom-color:var(--pur)}
.nav-r{margin-left:auto;display:flex;align-items:center;gap:8px}
.nav-sel{background:#21262d;border:1px solid var(--bdr);border-radius:8px;color:var(--txt);font-size:.78rem;padding:5px 9px;outline:none;cursor:pointer;min-width:72px}
/* Status pills */
.sp{font-size:.75rem;padding:2px 8px;border-radius:10px;background:#21262d;border:1px solid var(--bdr);display:flex;align-items:center;gap:4px}
.dot{display:inline-block;width:8px;height:8px;border-radius:50%;background:var(--mut)}
.dot.ok{background:var(--grn)}.dot.er{background:var(--red)}
/* Layout */
.layout{display:grid;grid-template-columns:1fr 292px;flex:1;overflow:hidden}
.main{overflow-y:auto;padding:16px}
.pane{display:none}.pane.on{display:block}
/* STACK MAP */
#pane-map{position:relative;min-height:calc(100vh - 96px);padding:14px;border-radius:14px;overflow:hidden;background:
  linear-gradient(rgba(13,17,23,.18),rgba(13,17,23,.34)),
  url('/assets/kingdoms/pale-strategy-map.png') center/cover no-repeat;
box-shadow:inset 0 0 0 1px rgba(230,237,243,.06)}
#pane-map::before{content:"";position:absolute;inset:0;background:radial-gradient(circle at center,rgba(255,255,255,.04),rgba(13,17,23,.06) 52%,rgba(13,17,23,.18) 100%);pointer-events:none}
.map-grid{position:relative;z-index:1;display:flex;flex-direction:column;gap:14px}
.kingdom{border:1px solid rgba(163,113,247,.42);border-radius:10px;padding:12px;background:rgba(13,17,23,.62);box-shadow:0 12px 30px rgba(0,0,0,.18);backdrop-filter:blur(2px)}
.kingdom.er{border-color:rgba(248,81,73,.68)}
.kingdom.warn{border-color:rgba(210,153,34,.7)}
.kingdom-hdr{display:flex;align-items:center;justify-content:space-between;gap:12px;margin-bottom:12px;min-width:0}
.kingdom-title{display:flex;align-items:center;gap:10px;min-width:0}
.kingdom-castle{width:48px;height:48px;object-fit:contain;filter:drop-shadow(0 3px 5px rgba(0,0,0,.45));flex-shrink:0}
.kingdom-copy{min-width:0}
.kingdom-name{font-size:1rem;font-weight:800;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.kingdom-sub{font-size:.72rem;color:var(--mut);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.kingdom-score{display:flex;align-items:center;gap:5px;flex-shrink:0}
.kingdom-stacks{display:grid;grid-template-columns:repeat(auto-fill,minmax(250px,1fr));gap:12px;align-items:start}
.stack-card{position:relative;background:var(--sur);border:1px solid var(--bdr);border-radius:8px;padding:10px;display:flex;flex-direction:column;gap:8px;min-width:0;transition:border-color .2s}
.stack-card:hover{border-color:var(--pur)}
.stack-card.er{border-color:var(--red)}
.stack-card.warn{border-color:var(--yel)}
.char-frame{position:relative;display:flex;align-items:center;justify-content:center;width:100%;aspect-ratio:1.62/1;background:#0d1117;border:1px solid #21262d;border-radius:6px;overflow:hidden;cursor:pointer;padding:0;color:inherit;font:inherit}
.char-frame:hover{border-color:var(--pur)}
.char-img{display:block;width:100%;height:100%;object-fit:contain}
.stack-banner{position:absolute;left:0;right:0;bottom:0;height:34px;display:flex;align-items:center;justify-content:center;padding:0 38px 0 10px;background:rgba(48,54,61,.94);backdrop-filter:blur(3px);font-size:.84rem;font-weight:800;letter-spacing:.01em;text-shadow:0 1px 2px rgba(0,0,0,.45);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.status-circles{position:absolute;top:7px;left:7px;display:flex;gap:5px}
.kingdom-score .status-circles{position:static}
.status-dot{width:25px;height:25px;border-radius:50%;font-size:.62rem;font-weight:800;display:flex;align-items:center;justify-content:center;border:1px solid rgba(0,0,0,.58);box-shadow:0 1px 5px rgba(0,0,0,.42);line-height:1}
.b-ok{background:var(--grn);color:#000}
.b-warn{background:var(--yel);color:#000}
.b-err{background:var(--red);color:#fff}
.b-hide{display:none}
.gear-btn{position:absolute;top:7px;right:7px;width:28px;height:28px;border-radius:50%;border:1px solid rgba(230,237,243,.28);background:rgba(13,17,23,.78);color:var(--txt);cursor:pointer;display:flex;align-items:center;justify-content:center;font-size:15px;line-height:1;z-index:2}
.gear-btn:hover{border-color:var(--pur);background:rgba(33,38,45,.92)}
.stack-nm{font-size:.9rem;font-weight:800;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.stack-sv{font-size:.68rem;color:var(--mut);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.stack-meta{display:flex;justify-content:space-between;gap:8px;align-items:flex-start;min-width:0}
.stack-copy{min-width:0}
.sub-list{display:flex;flex-direction:column;gap:6px;border-top:1px solid #21262d;padding-top:8px}
.sub-row{position:relative;display:flex;align-items:center;gap:8px;min-height:28px;padding:4px 64px 4px 10px;border-radius:6px;background:#0d1117;border:1px solid #21262d;cursor:pointer}
.sub-row:hover{border-color:var(--pur)}
.sub-name{font-size:.75rem;font-weight:650;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.sub-type{margin-left:auto;color:var(--mut);font-size:.58rem;text-transform:uppercase;border:1px solid #21262d;border-radius:4px;padding:2px 5px;line-height:1.15;flex-shrink:0}
.sub-row .status-circles{top:50%;left:auto;right:7px;transform:translateY(-50%)}
.sub-row .status-dot{width:21px;height:21px;font-size:.58rem}
.char-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(96px,1fr));gap:10px}
.char-choice{border:1px solid var(--bdr);background:#0d1117;color:var(--txt);border-radius:8px;padding:7px;cursor:pointer;text-align:left}
.char-choice:hover,.char-choice.on{border-color:var(--pur);background:#21262d}
.char-choice img{display:block;width:100%;aspect-ratio:1.62/1;object-fit:cover;border-radius:5px;margin-bottom:5px}
.char-choice span{display:block;font-size:.72rem;font-weight:650;text-align:center}
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
.hb-wrap{padding:10px 12px 8px;border-bottom:1px solid var(--bdr);flex-shrink:0;background:#12171f}
.hb-lbl{display:flex;justify-content:space-between;align-items:center;margin-bottom:5px}
.hb-title{font-size:.75rem;font-weight:700;letter-spacing:.06em;display:flex;align-items:center;gap:6px}
.raven-mark{width:42px;height:42px;object-fit:contain;filter:drop-shadow(0 3px 7px rgba(0,0,0,.55));margin-top:-2px;flex-shrink:0}
.hb-st{font-size:.68rem;color:var(--mut)}
.hb-canvas-wrap{background:#0d1117;border:1px solid #21262d;border-radius:10px;padding:6px 8px}
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
.oracle{border-top:1px solid var(--bdr);padding:12px;display:flex;flex-direction:column;gap:8px;background:rgba(13,17,23,.24)}
.oracle-hdr{display:flex;align-items:center;justify-content:space-between;gap:10px}
.oracle-title{font-size:.73rem;font-weight:800;letter-spacing:.08em;display:flex;align-items:center;gap:8px}
.oracle-mark{width:36px;height:36px;border-radius:50%;object-fit:cover;border:1px solid rgba(163,113,247,.35);box-shadow:0 4px 10px rgba(0,0,0,.28);flex-shrink:0}
.oracle-meta{font-size:.72rem;color:var(--mut);line-height:1.45}
.oracle-box{border:1px solid #21262d;border-radius:8px;background:#0d1117;padding:10px;min-height:108px;max-height:240px;overflow:auto;font-size:.77rem;line-height:1.45;white-space:pre-wrap}
.oracle-box.busy{color:var(--mut)}
.oracle-box.error{border-color:rgba(248,81,73,.4);color:var(--red)}
.oracle-box.empty{color:var(--mut)}
.oracle-summary{display:flex;gap:6px;flex-wrap:wrap}
.oracle-summary .sp{font-size:.7rem}
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
@media (max-width:700px){
  .nav{padding:0 10px;gap:8px;height:58px}
  .brand{font-size:.96rem;line-height:1.05;max-width:100px}
  .brand-mark{width:34px;height:34px}
  .tab{padding:0 9px}
  .nav-r{gap:4px}
  .nav-r .sp:nth-of-type(n+2),.nav-r .small{display:none}
  .layout{grid-template-columns:1fr}
  .aside{display:none}
  .main{padding:12px}
  #pane-map{padding:10px}
  .kingdom{padding:10px}
  .kingdom-hdr{align-items:flex-start}
  .kingdom-castle{width:38px;height:38px}
  .kingdom-stacks{grid-template-columns:repeat(auto-fill,minmax(210px,1fr));gap:10px}
}
</style>
</head>
<body>
<nav class="nav">
  <span class="brand"><span class="brand-mark"><img src="/assets/characters/orc.png" alt=""></span><span>ORC</span></span>
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
    <select class="nav-sel" id="window-hours" onchange="setWindowHours(this.value)" title="Issue time window">
      <option value="1">1 hour</option>
      <option value="6">6 hours</option>
      <option value="24" selected>24 hours</option>
    </select>
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
      <span class="hb-title"><img class="raven-mark" src="/assets/kingdoms/raven.png" alt="">RAVEN</span>
      <span class="hb-st" id="hb-status">connecting…</span>
    </div>
    <div class="hb-canvas-wrap"><canvas id="hb-cv" height="48"></canvas></div>
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
  <div class="oracle">
    <div class="oracle-hdr">
      <span class="oracle-title"><img class="oracle-mark" src="/assets/kingdoms/oracle.png" alt="">THE ORACLE</span>
      <button class="btns" id="oracle-btn" onclick="runOracle()">Activate</button>
    </div>
    <div class="oracle-meta">Review the last hour of warnings and errors on demand, then get the top three problems worth researching and fixing first.</div>
    <div class="oracle-summary" id="oracle-summary"></div>
    <div class="oracle-box empty" id="oracle-box">Ready to review the last hour of events.</div>
  </div>
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

<dialog id="char-dlg">
  <div class="mh"><span id="char-dlg-t">Stack Character</span><button class="mx" onclick="closeCharDlg()">&#215;</button></div>
  <div class="mb">
    <div class="fg">
      <label>Character</label>
      <div class="char-grid" id="char-grid"></div>
    </div>
  </div>
  <div class="mf">
    <button class="btns" onclick="closeCharDlg()">Close</button>
  </div>
</dialog>

<script>
/* ============================================================
   STATE
   ============================================================ */
let _evts=[], _evFilters={severity:'',container:'',server:''};
let _conns=[], _editId=null, _charEditKey='';
let _hbData=new Array(40).fill(0), _hbBucket=0;
let _ravenFilter='', _issuePills=[];
let _oracleState={busy:false,summary:null,analysis:'',error:''};
let _windowHours=24;
const MAX_ISSUE_PILLS=20;

/* ============================================================
   CHARACTER ASSETS
   ============================================================ */
const CHARACTERS=[
  {id:'orc',label:'Orc',src:'/assets/characters/orc.png'},
  {id:'wizard',label:'Wizard',src:'/assets/characters/wizard.png'},
  {id:'elf',label:'Elf',src:'/assets/characters/elf.png'},
  {id:'warrior',label:'Warrior',src:'/assets/characters/warrior.png'},
  {id:'fighter',label:'Fighter',src:'/assets/characters/fighter.png'},
  {id:'dwarf',label:'Dwarf',src:'/assets/characters/dwarf.png'},
  {id:'rogue',label:'Rogue',src:'/assets/characters/rogue.png'},
  {id:'cleric',label:'Cleric',src:'/assets/characters/cleric.png'},
  {id:'bard',label:'Bard',src:'/assets/characters/bard.png'},
  {id:'farmer',label:'Farmer',src:'/assets/characters/farmer.png'},
  {id:'vendor',label:'Vendor',src:'/assets/characters/vendor.png'},
  {id:'blacksmith',label:'Blacksmith',src:'/assets/characters/blacksmith.png'},
  {id:'shepherd',label:'Shepherd',src:'/assets/characters/shepherd.png'},
  {id:'herder',label:'Herder',src:'/assets/characters/herder.png'},
  {id:'sorceress',label:'Sorceress',src:'/assets/characters/sorceress.png'}
];
const CHARACTER_BY_ID=Object.fromEntries(CHARACTERS.map(c=>[c.id,c]));
const CHARACTER_STORAGE_PREFIX='orc.map.character.';
const MT_ZONE='America/Denver';
const DATE_FMT=new Intl.DateTimeFormat('en-US',{timeZone:MT_ZONE,year:'numeric',month:'short',day:'2-digit',hour:'numeric',minute:'2-digit',second:'2-digit',timeZoneName:'short'});
const TIME_FMT=new Intl.DateTimeFormat('en-US',{timeZone:MT_ZONE,hour:'numeric',minute:'2-digit',second:'2-digit',timeZoneName:'short'});

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
function fmt(iso){return iso?DATE_FMT.format(new Date(iso)):'';}
function fmtShort(iso){return iso?TIME_FMT.format(new Date(iso)):'';}
function esc(s){return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');}
function hashStr(s){let h=0;for(let i=0;i<s.length;i++)h=((h<<5)-h+s.charCodeAt(i))|0;return Math.abs(h);}
function storageGet(k){try{return localStorage.getItem(k);}catch{return null;}}
function storageSet(k,v){try{localStorage.setItem(k,v);}catch{}}
function stackCharacterKey(stack){return `${stack.server}::${stack.name}`;}
function defaultCharacterId(stack){
  if(stack&&CHARACTER_BY_ID[stack.character])return stack.character;
  return CHARACTERS[hashStr(stackCharacterKey(stack))%CHARACTERS.length].id;
}
function selectedCharacterId(stack){
  const saved=storageGet(CHARACTER_STORAGE_PREFIX+stackCharacterKey(stack));
  return CHARACTER_BY_ID[saved]?saved:defaultCharacterId(stack);
}
function issueCounts(app){
  return {
    errors:Number(app.errors ?? app.errors_24h ?? app.errors_1h ?? 0),
    warnings:Number(app.warnings ?? app.warnings_24h ?? app.warnings_1h ?? 0)
  };
}
function validWindowHours(v){
  const n=Number(v);
  return [1,6,24].includes(n)?n:24;
}
function setWindowHours(v,refresh=true){
  _windowHours=validWindowHours(v);
  storageSet('orc.window.hours',String(_windowHours));
  const sel=document.getElementById('window-hours');
  if(sel)sel.value=String(_windowHours);
  if(refresh)loadAll();
}
function countLabel(n){return n>99?'99+':String(n);}
function statusCircles(errors,warnings){
  const dots=[];
  if(errors>0)dots.push(`<span class="status-dot b-err" title="${errors} error${errors!==1?'s':''}">${esc(countLabel(errors))}</span>`);
  if(warnings>0)dots.push(`<span class="status-dot b-warn" title="${warnings} warning${warnings!==1?'s':''}">${esc(countLabel(warnings))}</span>`);
  if(!dots.length)dots.push('<span class="status-dot b-ok" title="No recent errors or warnings">0</span>');
  return `<div class="status-circles">${dots.join('')}</div>`;
}
function newIssueCounts(msg){
  return {
    errors:Number(msg.errors||0),
    warnings:Number(msg.warnings||0)
  };
}
function openCharDlg(btn){
  _charEditKey=btn.dataset.stackKey||'';
  const selected=storageGet(CHARACTER_STORAGE_PREFIX+_charEditKey)||btn.dataset.character||CHARACTERS[0].id;
  document.getElementById('char-dlg-t').textContent=`${btn.dataset.stackName||'Stack'} Character`;
  document.getElementById('char-grid').innerHTML=CHARACTERS.map(c=>`
    <button class="char-choice ${c.id===selected?'on':''}" type="button" onclick="chooseStackCharacter('${esc(c.id)}')">
      <img src="${esc(c.src)}" alt="${esc(c.label)}"><span>${esc(c.label)}</span>
    </button>`).join('');
  document.getElementById('char-dlg').showModal();
}
function closeCharDlg(){document.getElementById('char-dlg').close();}
function chooseStackCharacter(id){
  const ch=CHARACTER_BY_ID[id];
  if(!ch||!_charEditKey)return;
  storageSet(CHARACTER_STORAGE_PREFIX+_charEditKey,ch.id);
  const card=[...document.querySelectorAll('.stack-card')].find(c=>c.dataset.stackKey===_charEditKey);
  const img=card?.querySelector('.char-img');
  const gear=card?.querySelector('.gear-btn');
  if(img){img.src=ch.src;img.alt=ch.label;}
  if(gear){gear.dataset.character=ch.id;}
  document.querySelectorAll('.char-choice').forEach(b=>b.classList.toggle('on',b.textContent.trim()===ch.label));
  closeCharDlg();
}
function jumpToEventsFromEl(el){
  jumpToEvents(el.dataset.server||'',el.dataset.container||'',el.dataset.severity||'');
}
function renderOracle(){
  const box=document.getElementById('oracle-box');
  const summary=document.getElementById('oracle-summary');
  const btn=document.getElementById('oracle-btn');
  btn.disabled=!!_oracleState.busy;
  btn.textContent=_oracleState.busy?'Consulting...':'Activate';
  if(_oracleState.summary){
    const s=_oracleState.summary;
    summary.innerHTML=`
      <span class="sp"><span class="se2">${s.errors||0}</span><span class="muted"> err</span></span>
      <span class="sp"><span class="sw2">${s.warnings||0}</span><span class="muted"> warn</span></span>
      <span class="sp"><span>${s.total_events||0}</span><span class="muted"> events / 1h</span></span>`;
  }else{
    summary.innerHTML='';
  }
  if(_oracleState.busy){
    box.className='oracle-box busy';
    box.textContent='Gathering the last hour of events and asking the Oracle for a recommendation...';
    return;
  }
  if(_oracleState.error){
    box.className='oracle-box error';
    box.textContent=_oracleState.error;
    return;
  }
  if(_oracleState.analysis){
    box.className='oracle-box';
    box.textContent=_oracleState.analysis;
    return;
  }
  box.className='oracle-box empty';
  box.textContent='Ready to review the last hour of events.';
}
async function runOracle(){
  _oracleState={busy:true,summary:_oracleState.summary,analysis:'',error:''};
  renderOracle();
  try{
    const r=await fetch('/oracle/review',{method:'POST'});
    const d=await r.json();
    if(!r.ok)throw new Error(d.detail||'Oracle request failed.');
    _oracleState={busy:false,summary:d.summary||null,analysis:d.analysis||'No recommendation returned.',error:''};
  }catch(e){
    _oracleState={busy:false,summary:_oracleState.summary,analysis:'',error:e.message||'Oracle request failed.'};
  }
  renderOracle();
}

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
    const d=await fetch(`/overview?hours=${_windowHours}`).then(r=>r.json());
    renderMap(d.stacks);
  }catch(e){
    document.getElementById('map-grid').innerHTML='<div class="empty">Could not load stack map.</div>';
  }
}

function renderMap(stacks){
  const grid=document.getElementById('map-grid');
  if(!stacks.length){grid.innerHTML='<div class="empty">No stacks found. Add a connection in the Connections tab.</div>';return;}
  const kingdomMap=new Map();
  stacks.forEach(s=>{
    const key=s.server||'Unknown server';
    if(!kingdomMap.has(key))kingdomMap.set(key,{server:key,stacks:[]});
    const kingdom=kingdomMap.get(key);
    kingdom.stacks.push({...s,server:key,containers:s.containers||[]});
  });
  const kingdoms=[...kingdomMap.values()].filter(k=>k.stacks.some(s=>s.containers.length)).sort((a,b)=>a.server.localeCompare(b.server));
  if(!kingdoms.length){grid.innerHTML='<div class="empty">No containers found for the configured connections.</div>';return;}
  grid.innerHTML=kingdoms.map(k=>{
    k.stacks.sort((a,b)=>a.name.localeCompare(b.name));
    const totals=k.stacks.reduce((acc,stack)=>{
      stack.containers.forEach(app=>{const c=issueCounts(app);acc.errors+=c.errors;acc.warnings+=c.warnings;});
      return acc;
    },{errors:0,warnings:0});
    const kingdomCls=totals.errors>0?'er':totals.warnings>0?'warn':'';
    const stackList=k.stacks.map(s=>s.name).join(', ');
    const stackCards=k.stacks.map(stack=>{
      stack.containers.sort((a,b)=>a.type.localeCompare(b.type)||a.name.localeCompare(b.name));
      const stackTotals=stack.containers.reduce((acc,app)=>{const c=issueCounts(app);acc.errors+=c.errors;acc.warnings+=c.warnings;return acc;},{errors:0,warnings:0});
      const hasErr=stackTotals.errors>0, hasWarn=stackTotals.warnings>0;
      const severity=hasErr?'error':hasWarn?'warning':'';
      const cardCls=hasErr?'er':hasWarn?'warn':'';
      const charId=selectedCharacterId(stack);
      const ch=CHARACTER_BY_ID[charId]||CHARACTERS[0];
      const key=stackCharacterKey(stack);
      const subRows=stack.containers.map(app=>{
        const counts=issueCounts(app);
        const subErr=counts.errors>0, subWarn=counts.warnings>0;
        const subSeverity=subErr?'error':subWarn?'warning':'';
        return `<div class="sub-row" onclick="jumpToEventsFromEl(this)"
          data-server="${esc(stack.server)}" data-container="${esc(app.full_name)}" data-severity="${esc(subSeverity)}"
          title="${esc(app.full_name)}">
          <span class="sub-name">${esc(app.name)}</span>
          <span class="sub-type">${esc(app.type)}</span>
          ${statusCircles(counts.errors,counts.warnings)}
        </div>`;
      }).join('');
      return `<div class="stack-card ${cardCls}" data-stack-key="${esc(key)}">
      <button class="gear-btn" type="button" onclick="openCharDlg(this)"
        data-stack-key="${esc(key)}" data-stack-name="${esc(stack.name)}" data-character="${esc(charId)}"
        title="Configure ${esc(stack.name)} character">&#9881;</button>
      <button class="char-frame" type="button" onclick="jumpToEventsFromEl(this)"
        data-server="${esc(stack.server)}" data-container="${esc(stack.name)}" data-severity="${esc(severity)}"
        title="View events for ${esc(stack.name)}">
        <img class="char-img" src="${esc(ch.src)}" alt="${esc(ch.label)}">
        <span class="stack-banner">${esc(stack.name)}</span>
        ${statusCircles(stackTotals.errors,stackTotals.warnings)}
      </button>
      <div class="stack-meta">
        <div class="stack-copy">
          <div class="stack-sv">${stack.containers.length} subordinate${stack.containers.length!==1?'s':''}</div>
        </div>
      </div>
      <div class="sub-list">${subRows}</div>
    </div>`;
    }).join('');
    return `<section class="kingdom ${kingdomCls}">
      <div class="kingdom-hdr">
        <div class="kingdom-title">
          <img class="kingdom-castle" src="/assets/kingdoms/castle.png" alt="">
          <div class="kingdom-copy">
            <div class="kingdom-name" title="${esc(k.server)}">${esc(k.server)}</div>
            <div class="kingdom-sub" title="${esc(stackList)}">${k.stacks.length} stack${k.stacks.length!==1?'s':''} - ${esc(stackList)}</div>
          </div>
        </div>
        <div class="kingdom-score">${statusCircles(totals.errors,totals.warnings)}</div>
      </div>
      <div class="kingdom-stacks">${stackCards}</div>
    </section>`;
  }).join('');
}

/* ============================================================
   EVENTS
   ============================================================ */
function _evUrl(){
  const p=new URLSearchParams({limit:200,hours:String(_windowHours)});
  if(_evFilters.severity)p.set('severity',_evFilters.severity);
  if(_evFilters.container)p.set('container',_evFilters.container);
  if(_evFilters.server)p.set('server',_evFilters.server);
  return '/events?'+p.toString();
}
async function loadEvts(){
  try{
    const d=await fetch(_evUrl()).then(r=>r.json());
    document.getElementById('err-cnt').textContent=d.err_count??0;
    document.getElementById('warn-cnt').textContent=d.warn_count??0;
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
  _evFilters={severity:sev||'',container:container||'',server:server||''};
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
    <thead><tr><th>Time (MT)</th><th>Server</th><th>Container</th><th>Severity</th><th>Message</th></tr></thead>
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
      <thead><tr><th>Name</th><th>URL</th><th>Type</th><th>Status</th><th>Last Polled (MT)</th><th>Actions</th></tr></thead>
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
  if(_ravenFilter==='error') return _issuePills.filter(m=>m.type==='poll_error'||(m.type==='container_result'&&newIssueCounts(m).errors>0));
  if(_ravenFilter==='warning') return _issuePills.filter(m=>m.type==='container_result'&&newIssueCounts(m).warnings>0&&newIssueCounts(m).errors===0);
  return _issuePills;
}
function issuePillHtml(msg,opacity,isCurrent){
  const ts=msg.ts?fmtShort(msg.ts):'';
  const accent=isCurrent?'border-left:3px solid currentColor;padding-left:9px;':'';
  const style=`opacity:${opacity};${accent}`;
  if(msg.type==='poll_error')
    return `<div class="pill p-error" style="${style}">✗ <strong>${esc(msg.server)}</strong><div style="font-size:.72rem;margin-top:2px;opacity:.85">${esc(msg.error||'')}</div></div>`;
  if(msg.type==='container_result'){
    const ne=msg.errors||0,nw=msg.warnings||0;
    let cls,detail,sev;
    if(ne>0){cls='p-error';sev='error';detail=`${ne} new error${ne!==1?'s':''}`;if(nw>0)detail+=`, ${nw} new warn`;}
    else{cls='p-warn';sev='warning';detail=`${nw} new warning${nw!==1?'s':''}`;}
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
      const ne=msg.errors||0,nw=msg.warnings||0;
      _hbBucket+=ne+nw;
      if(ne>0){setStatus('⚠',`${msg.container} · ${ne} new error${ne!==1?'s':''}`,true);addIssuePill(msg);}
      else if(nw>0){setStatus('⚠',`${msg.container} · ${nw} new warning${nw!==1?'s':''}`,true);addIssuePill(msg);}
      else setStatus('✓',`${msg.container} · no new issues`,true);
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
  document.getElementById('upd').textContent=fmtShort(new Date().toISOString());
}
window.addEventListener('resize',()=>{resizeCanvas();drawHb();});
resizeCanvas();drawHb();
renderOracle();
setWindowHours(storageGet('orc.window.hours')||24,false);
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
app.mount("/assets", StaticFiles(directory=REPO_ROOT / "app" / "static"), name="assets")


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


def _oracle_summary(window_hours: int = 1) -> dict:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=window_hours)
    with SessionLocal() as s:
        rows = (
            s.query(ObservedEvent, Connection)
            .outerjoin(Connection, ObservedEvent.connection_id == Connection.id)
            .filter(
                ObservedEvent.occurred_at >= cutoff,
                ObservedEvent.severity.in_(["warning", "error", "critical"]),
            )
            .order_by(ObservedEvent.occurred_at.desc())
            .all()
        )

    container_rollup: dict[tuple[str, str, str], dict] = {}
    stack_rollup: dict[tuple[str, str], dict] = {}
    pattern_rollup: dict[tuple[str, str, str, str], dict] = {}
    totals = {"total_events": len(rows), "errors": 0, "warnings": 0}

    for event, conn in rows:
        server = conn.name if conn else "Unknown server"
        stack = event.stack_name or _infer_stack(event.container_name)
        stack_key = (server, stack)
        key = (server, stack, event.container_name)
        bucket = container_rollup.setdefault(
            key,
            {
                "server": server,
                "stack": stack,
                "container": event.container_name,
                "errors": 0,
                "warnings": 0,
                "latest_at": event.occurred_at.isoformat(),
                "messages": Counter(),
            },
        )
        stack_bucket = stack_rollup.setdefault(
            stack_key,
            {
                "server": server,
                "stack": stack,
                "errors": 0,
                "warnings": 0,
                "containers": set(),
            },
        )
        severity = "error" if event.severity in ("error", "critical") else "warning"
        if severity == "error":
            bucket["errors"] += 1
            totals["errors"] += 1
            stack_bucket["errors"] += 1
        else:
            bucket["warnings"] += 1
            totals["warnings"] += 1
            stack_bucket["warnings"] += 1
        stack_bucket["containers"].add(event.container_name)
        bucket["messages"][event.message[:220]] += 1
        bucket["latest_at"] = max(bucket["latest_at"], event.occurred_at.isoformat())
        pattern = _oracle_pattern(event.message)
        pattern_key = (server, stack, event.container_name, pattern)
        pattern_bucket = pattern_rollup.setdefault(
            pattern_key,
            {
                "server": server,
                "stack": stack,
                "container": event.container_name,
                "pattern": pattern,
                "count": 0,
                "errors": 0,
                "warnings": 0,
                "examples": Counter(),
            },
        )
        pattern_bucket["count"] += 1
        if severity == "error":
            pattern_bucket["errors"] += 1
        else:
            pattern_bucket["warnings"] += 1
        pattern_bucket["examples"][event.message[:220]] += 1

    top_containers = []
    for item in sorted(
        container_rollup.values(),
        key=lambda x: (-x["errors"], -x["warnings"], x["container"].lower()),
    )[:20]:
        top_containers.append(
            {
                "server": item["server"],
                "stack": item["stack"],
                "container": item["container"],
                "errors": item["errors"],
                "warnings": item["warnings"],
                "latest_at": item["latest_at"],
                "top_messages": [
                    {"message": msg, "count": count}
                    for msg, count in item["messages"].most_common(3)
                ],
            }
        )

    stacks = [
        {
            "server": item["server"],
            "stack": item["stack"],
            "errors": item["errors"],
            "warnings": item["warnings"],
            "containers": len(item["containers"]),
        }
        for item in sorted(
            stack_rollup.values(),
            key=lambda x: (-x["errors"], -x["warnings"], x["stack"].lower()),
        )[:12]
    ]

    top_patterns = [
        {
            "server": item["server"],
            "stack": item["stack"],
            "container": item["container"],
            "pattern": item["pattern"],
            "count": item["count"],
            "errors": item["errors"],
            "warnings": item["warnings"],
            "example": item["examples"].most_common(1)[0][0],
        }
        for item in sorted(
            pattern_rollup.values(),
            key=lambda x: (-x["errors"], -x["warnings"], -x["count"], x["container"].lower()),
        )[:20]
    ]

    return {
        "window_hours": window_hours,
        "window_start": cutoff.isoformat(),
        "window_end": datetime.now(timezone.utc).isoformat(),
        "total_events": totals["total_events"],
        "errors": totals["errors"],
        "warnings": totals["warnings"],
        "unique_containers": len(container_rollup),
        "stacks": stacks,
        "top_containers": top_containers,
        "top_patterns": top_patterns,
    }


def _oracle_pattern(message: str) -> str:
    text = message.strip().lower()
    text = _ORACLE_UUID_RE.sub("<id>", text)
    text = _ORACLE_NUM_RE.sub("<n>", text)
    text = re.sub(r"\s+", " ", text)
    return text[:220]


def _oracle_prompt(summary: dict) -> list[dict[str, str]]:
    system_prompt = (
        "You are The Oracle inside ORC, an operations advisor reviewing the last hour "
        "of container warnings and errors collected from Portainer-managed applications. "
        "Prioritize only the top three problems worth researching and fixing first. "
        "Use the stack, container, message patterns, and event frequencies in the summary. "
        "Do not try to solve every issue in the data. Respond in plain text with exactly this format:\n"
        "Top 3 observations\n"
        "1. <stack / container / pattern / why it matters>\n"
        "2. <...>\n"
        "3. <...>\n\n"
        "Possible root cause\n"
        "1. <most likely cause for observation 1>\n"
        "2. <most likely cause for observation 2>\n"
        "3. <most likely cause for observation 3>\n\n"
        "What you should do to fix it\n"
        "1. <first action>\n"
        "2. <second action>\n"
        "3. <third action>\n"
        "Keep each line specific and short."
    )
    return [
        {"role": "system", "content": system_prompt},
        {
            "role": "user",
            "content": (
                "Review this one-hour event summary and recommend the best next fixes.\n\n"
                + json.dumps(summary, indent=2)
            ),
        },
    ]


def _oracle_review(summary: dict) -> str:
    if not summary["total_events"]:
        return (
            "Top 3 observations\n"
            "1. No warnings or errors were found in the last hour.\n"
            "2. There is no active stack or container pattern to prioritize.\n"
            "3. The environment looked quiet during this review window.\n\n"
            "Possible root cause\n"
            "1. Systems may be healthy.\n"
            "2. Activity may have been low during the last hour.\n"
            "3. There may be nothing urgent to research right now.\n\n"
            "What you should do to fix it\n"
            "1. Keep monitoring.\n"
            "2. Re-run the Oracle when fresh issues appear.\n"
            "3. Use the Events tab for a manual spot check if needed."
        )

    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        raise HTTPException(
            status_code=503,
            detail="The Oracle is not configured. Set OPENAI_API_KEY using the same Portainer env pattern as BADGE.",
        )

    model = os.getenv("OPENAI_MODEL", DEFAULT_OPENAI_MODEL).strip() or DEFAULT_OPENAI_MODEL
    payload = {
        "model": model,
        "temperature": 0.2,
        "messages": _oracle_prompt(summary),
    }
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

    try:
        resp = httpx.post(OPENAI_API_URL, headers=headers, json=payload, timeout=45.0)
        resp.raise_for_status()
        data = resp.json()
        content = data["choices"][0]["message"]["content"].strip()
    except httpx.HTTPStatusError as exc:
        detail = exc.response.text[:400] if exc.response is not None else str(exc)
        raise HTTPException(status_code=502, detail=f"Oracle request failed: {detail}") from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Oracle request failed: {exc}") from exc

    return content or "Summary:\n- The Oracle did not return any content."


def _hours_window(hours: int) -> int:
    return hours if hours in (1, 6, 24) else 24


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
    hours: int = 24,
) -> dict:
    hours = _hours_window(hours)
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    with SessionLocal() as s:
        q = (
            s.query(ObservedEvent, Connection)
            .outerjoin(Connection, ObservedEvent.connection_id == Connection.id)
            .filter(ObservedEvent.occurred_at >= cutoff)
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

        err_count = s.query(func.count(ObservedEvent.id)).filter(
            ObservedEvent.occurred_at >= cutoff,
            ObservedEvent.severity.in_(["error", "critical"]),
        ).scalar() or 0
        warn_count = s.query(func.count(ObservedEvent.id)).filter(
            ObservedEvent.occurred_at >= cutoff,
            ObservedEvent.severity == "warning",
        ).scalar() or 0

    return {
        "hours": hours,
        "err_count": err_count,
        "warn_count": warn_count,
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


@app.post("/oracle/review")
def review_with_oracle() -> dict:
    summary = _oracle_summary(window_hours=1)
    analysis = _oracle_review(summary)
    return {
        "summary": {
            "total_events": summary["total_events"],
            "errors": summary["errors"],
            "warnings": summary["warnings"],
            "unique_containers": summary["unique_containers"],
            "window_start": summary["window_start"],
            "window_end": summary["window_end"],
        },
        "analysis": analysis,
        "model": os.getenv("OPENAI_MODEL", DEFAULT_OPENAI_MODEL).strip() or DEFAULT_OPENAI_MODEL,
        "generated_at": datetime.now(timezone.utc).isoformat(),
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
    return ("orc", "rogue", "wizard", "fighter")[abs(hash(name)) % 4]


@app.get("/overview")
def get_overview(hours: int = 24) -> dict:
    hours = _hours_window(hours)
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    with SessionLocal() as s:
        connections = s.query(Connection).filter_by(enabled=True).all()
        err_rows = s.query(
            ObservedEvent.connection_id, ObservedEvent.container_id,
            func.count(ObservedEvent.id).label("n")
        ).filter(
            ObservedEvent.occurred_at >= cutoff,
            ObservedEvent.severity.in_(["error", "critical"])
        ).group_by(ObservedEvent.connection_id, ObservedEvent.container_id).all()

        warn_rows = s.query(
            ObservedEvent.connection_id, ObservedEvent.container_id,
            func.count(ObservedEvent.id).label("n")
        ).filter(
            ObservedEvent.occurred_at >= cutoff,
            ObservedEvent.severity == "warning"
        ).group_by(ObservedEvent.connection_id, ObservedEvent.container_id).all()

    errs  = {(r.connection_id, r.container_id): int(r.n) for r in err_rows}
    warns = {(r.connection_id, r.container_id): int(r.n) for r in warn_rows}

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
                        "container_id": cid,
                        "type": _container_type(service),
                        "errors": errs.get((conn.id, cid), 0),
                        "warnings": warns.get((conn.id, cid), 0),
                    })
            except Exception:
                continue
        for sname, containers in sorted(stacks.items()):
            stacks_out.append({
                "name": sname, "server": conn.name,
                "character": _stack_character(sname),
                "containers": sorted(containers, key=lambda x: x["type"]),
            })
    return {"hours": hours, "stacks": stacks_out}


def _cdct(c: Connection) -> dict:
    return {
        "id": c.id, "name": c.name, "type": c.type, "base_url": c.base_url,
        "api_token": c.api_token, "enabled": c.enabled,
        "poll_interval_seconds": c.poll_interval_seconds,
        "last_polled_at": c.last_polled_at.isoformat() if c.last_polled_at else None,
        "last_status": c.last_status, "last_error": c.last_error,
    }
