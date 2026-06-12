import json
import os
import re
import hashlib
import hmac
import secrets
import time as _time
from collections import Counter, defaultdict
from contextlib import asynccontextmanager
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from sqlalchemy import func, text as _sa_text

from .config import REDIS_URL, REPO_ROOT
from .db import (
    AIUsageLog,
    AgentMessage,
    AgentRuntimeState,
    ApprovalRequest,
    Connection,
    FocusedWatch,
    IngestionCheckpoint,
    LearningEntry,
    ObservedEvent,
    SessionLocal,
    SystemSetting,
    UserAccount,
    UserSession,
    init_db,
)
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
    server_name: str = ""
    logo_data: str = ""


class ConnectionTestIn(BaseModel):
    base_url: str
    api_token: str


class OracleReviewIn(BaseModel):
    friendly_names: dict[str, str] = Field(default_factory=dict)


class LoginIn(BaseModel):
    username: str
    password: str


class UserCreateIn(BaseModel):
    username: str
    password: str
    role: str = "user"


class AgentTrustIn(BaseModel):
    trust_mode: str = "recommend_only"
    enabled: bool = True


class AgentMessageIn(BaseModel):
    source_agent: str
    target_agent: str = ""
    message_type: str = "instruction"
    summary: str
    thread_id: str = "operations"
    payload: dict | None = None


class AgentCreateIn(BaseModel):
    agent_name: str
    agent_id: str = ""
    role: str = "specialist"
    risk_level: str = "low"
    approval_required: bool = False
    purpose: str
    allowed_skills: str = ""
    rules: str = ""
    icon: str = ""
    logo_data: str = ""


class SkillBuildIn(BaseModel):
    agent_id: str
    skill_name: str
    skill_id: str = ""
    category: str = "automation"
    risk_level: str = "medium"
    approval_required: bool = True
    purpose: str
    inputs: str = ""
    outputs: str = ""
    procedure: str
    rollback: str = ""
    success_criteria: str = ""


class ApprovalCreateIn(BaseModel):
    title: str
    requester_agent: str = "oracle"
    action_type: str = "container_refresh"
    target: str = ""
    rationale: str = ""
    risk_level: str = "medium"
    requested_by: str = "operator"


class ApprovalDecisionIn(BaseModel):
    decision: str
    decided_by: str = "operator"
    reason: str = ""


class LearningCreateIn(BaseModel):
    title: str
    source_agent: str = "sage"
    incident_ref: str = ""
    outcome: str = "proposed"
    summary: str


class AgentChatIn(BaseModel):
    message: str
    thread_id: str = "operations"


TRUST_MODES = {"recommend_only", "approval_required", "autonomous"}
COOKIE_NAME = "orc_session"
SESSION_DAYS = 7
DEFAULT_CORPORATE_LOGO = "/assets/characters/black-hd-logo.png"
DEFAULT_ORCHESTRATION_AGENTS = [
    {
        "agent_id": "raven",
        "name": "Raven",
        "role": "observer and message bus",
        "icon": "/assets/kingdoms/raven.png",
        "logo_data": DEFAULT_CORPORATE_LOGO,
        "trust_mode": "recommend_only",
    },
    {
        "agent_id": "oracle",
        "name": "The Oracle",
        "role": "investigator",
        "icon": "/assets/kingdoms/oracle.png",
        "logo_data": DEFAULT_CORPORATE_LOGO,
        "trust_mode": "recommend_only",
    },
    {
        "agent_id": "gate-keeper",
        "name": "Gate Keeper",
        "role": "approval and policy",
        "icon": "/assets/characters/agent-gate-keeper.png",
        "logo_data": DEFAULT_CORPORATE_LOGO,
        "legacy_icons": ["/assets/characters/warrior.png"],
        "trust_mode": "approval_required",
    },
    {
        "agent_id": "executioner",
        "name": "Executioner",
        "role": "approved execution",
        "icon": "/assets/characters/agent-executioner.png",
        "logo_data": DEFAULT_CORPORATE_LOGO,
        "legacy_icons": ["/assets/characters/blacksmith.png"],
        "trust_mode": "approval_required",
    },
    {
        "agent_id": "sage",
        "name": "Sage",
        "role": "learning and skill authoring",
        "icon": "/assets/characters/agent-sage.png",
        "logo_data": DEFAULT_CORPORATE_LOGO,
        "legacy_icons": ["/assets/characters/wizard.png"],
        "trust_mode": "recommend_only",
    },
    {
        "agent_id": "orc-orchestrator",
        "name": "ORC Orchestrator",
        "role": "router",
        "icon": "/assets/characters/orc.png",
        "logo_data": DEFAULT_CORPORATE_LOGO,
        "trust_mode": "approval_required",
    },
]


def _slug(value: str, fallback: str = "item") -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return slug or fallback


def _bool_text(value: bool) -> str:
    return "true" if value else "false"


def _bullet_block(value: str) -> str:
    lines = [line.strip(" -\t") for line in value.splitlines() if line.strip()]
    return "\n".join(f"- {line}" for line in lines) if lines else "- Not specified"


def _safe_markdown_path(root: Path, *parts: str) -> Path:
    base = root.resolve()
    target = (root / Path(*parts)).resolve()
    try:
        target.relative_to(base)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid Markdown path") from exc
    return target


def _markdown_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"true", "1", "yes", "y", "on"}


def _skill_markdown(body: SkillBuildIn, skill_id: str) -> str:
    return "\n".join(
        [
            "# Skill Definition",
            "",
            f"name: {body.skill_name.strip()}",
            f"id: {skill_id}",
            "version: 0.1.0",
            f"category: {body.category.strip() or 'automation'}",
            f"risk_level: {body.risk_level.strip() or 'medium'}",
            f"approval_required: {_bool_text(body.approval_required)}",
            f"agent: {body.agent_id.strip()}",
            "",
            "## Purpose",
            "",
            body.purpose.strip(),
            "",
            "## Inputs",
            "",
            _bullet_block(body.inputs),
            "",
            "## Outputs",
            "",
            _bullet_block(body.outputs),
            "",
            "## Procedure",
            "",
            _bullet_block(body.procedure),
            "",
            "## Rollback",
            "",
            _bullet_block(body.rollback),
            "",
            "## Success Criteria",
            "",
            _bullet_block(body.success_criteria),
            "",
            "## Audit Requirements",
            "",
            "- Record requesting user, deciding agent, approval decision, action target, result, and evidence links.",
            "",
        ]
    )


def _skill_file_for_id(skill_id: str) -> tuple[Path, str]:
    requested = skill_id.strip()
    slugged = _slug(requested, "skill")
    for item in load_registry(REPO_ROOT, "skills"):
        item_slug = _slug(item.item_id, "skill")
        path = Path(item.path)
        folder_slug = _slug(path.parent.name, "skill")
        if requested in {item.item_id, path.parent.name} or slugged in {item_slug, folder_slug}:
            target = _safe_markdown_path(REPO_ROOT, item.path)
            if not target.exists():
                break
            return target, item.item_id
    raise HTTPException(status_code=404, detail="Skill not found")


def _skill_section_text(sections: dict[str, list[str]], name: str, bullets: bool = False) -> str:
    lines = sections.get(name.lower(), [])
    cleaned: list[str] = []
    for line in lines:
        value = line.rstrip()
        if not value.strip():
            if cleaned and cleaned[-1] != "":
                cleaned.append("")
            continue
        if bullets:
            value = re.sub(r"^\s*[-*]\s+", "", value).strip()
            if value.lower() == "not specified":
                continue
        cleaned.append(value)
    while cleaned and cleaned[-1] == "":
        cleaned.pop()
    while cleaned and cleaned[0] == "":
        cleaned.pop(0)
    return "\n".join(cleaned).strip()


def _parse_skill_file(file_path: Path) -> dict:
    raw = file_path.read_text(encoding="utf-8")
    metadata: dict[str, str] = {}
    sections: dict[str, list[str]] = {}
    current_section = ""
    for line in raw.splitlines():
        stripped = line.strip()
        if stripped.startswith("## "):
            current_section = stripped[3:].strip().lower()
            sections[current_section] = []
            continue
        if current_section:
            sections[current_section].append(line)
            continue
        if stripped and not stripped.startswith("#") and ":" in stripped:
            key, value = stripped.split(":", 1)
            metadata[key.strip().lower()] = value.strip()

    return {
        "agent_id": metadata.get("agent", ""),
        "skill_name": metadata.get("name", file_path.parent.name),
        "skill_id": metadata.get("id", file_path.parent.name),
        "category": metadata.get("category", "automation"),
        "risk_level": metadata.get("risk_level", "medium"),
        "approval_required": _markdown_bool(metadata.get("approval_required"), True),
        "purpose": _skill_section_text(sections, "purpose"),
        "inputs": _skill_section_text(sections, "inputs", bullets=True),
        "outputs": _skill_section_text(sections, "outputs", bullets=True),
        "procedure": _skill_section_text(sections, "procedure", bullets=True),
        "rollback": _skill_section_text(sections, "rollback", bullets=True),
        "success_criteria": _skill_section_text(sections, "success criteria", bullets=True),
        "raw_markdown": raw,
        "path": str(file_path.relative_to(REPO_ROOT)),
    }


def _hash_password(password: str) -> str:
    salt = secrets.token_bytes(16)
    iterations = 120_000
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    return f"pbkdf2_sha256${iterations}${salt.hex()}${digest.hex()}"


def _verify_password(password: str, stored: str) -> bool:
    try:
        scheme, iterations, salt_hex, digest_hex = stored.split("$", 3)
        if scheme != "pbkdf2_sha256":
            return False
        candidate = hashlib.pbkdf2_hmac(
            "sha256",
            password.encode("utf-8"),
            bytes.fromhex(salt_hex),
            int(iterations),
        )
        return hmac.compare_digest(candidate.hex(), digest_hex)
    except Exception:
        return False


def _hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _normalize_role(role: str) -> str:
    return "admin" if role.strip().lower() == "admin" else "user"


def _user_dict(row: UserAccount) -> dict:
    return {
        "id": row.id,
        "username": row.username,
        "role": row.role,
        "enabled": row.enabled,
        "created_at": row.created_at.isoformat() if row.created_at else None,
    }


def _ensure_default_admin() -> None:
    with SessionLocal() as s:
        existing = s.query(UserAccount).filter_by(username="admin").first()
        if existing:
            return
        s.add(
            UserAccount(
                username="admin",
                password_hash=_hash_password("admin"),
                role="admin",
                enabled=True,
            )
        )
        s.commit()


def _current_user(request: Request) -> dict | None:
    token = request.cookies.get(COOKIE_NAME, "")
    if not token:
        return None
    now = datetime.now(timezone.utc)
    with SessionLocal() as s:
        sess = s.query(UserSession).filter_by(token_hash=_hash_token(token)).first()
        if not sess:
            return None
        expires_at = sess.expires_at
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        if expires_at <= now:
            s.delete(sess)
            s.commit()
            return None
        user = s.get(UserAccount, sess.user_id)
        if not user or not user.enabled:
            return None
        return _user_dict(user)


def _require_user(request: Request) -> dict:
    user = _current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Login required")
    return user


def _require_admin(request: Request) -> dict:
    user = _require_user(request)
    if user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    return user


def _default_agent_icon(agent_id: str, role: str = "") -> str:
    icons = {item["agent_id"]: item["icon"] for item in DEFAULT_ORCHESTRATION_AGENTS}
    if agent_id in icons:
        return icons[agent_id]
    r = role.lower()
    if "observer" in r:
        return "/assets/kingdoms/raven.png"
    if "gate" in r or "approval" in r or "policy" in r:
        return "/assets/characters/agent-gate-keeper.png"
    if "execution" in r or "execute" in r:
        return "/assets/characters/agent-executioner.png"
    if "learning" in r or "skill" in r or "sage" in r:
        return "/assets/characters/agent-sage.png"
    if "blacksmith" in r or "code" in r or "builder" in r:
        return "/assets/characters/blacksmith.png"
    if "scribe" in r or "document" in r:
        return "/assets/characters/bard.png"
    if "communicator" in r:
        return "/assets/characters/cleric.png"
    return "/assets/characters/agent-scout.png"


def _ensure_orchestration_agents(session) -> None:
    desired = {item["agent_id"]: item for item in DEFAULT_ORCHESTRATION_AGENTS}
    for item in load_registry(REPO_ROOT, "agents"):
        desired.setdefault(
            item.item_id,
            {
                "agent_id": item.item_id,
                "name": item.name,
                "role": item.role_or_category,
                "icon": item.icon or _default_agent_icon(item.item_id, item.role_or_category),
                "logo_data": DEFAULT_CORPORATE_LOGO,
                "trust_mode": "approval_required" if item.approval_required else "recommend_only",
            },
        )

    existing = {row.agent_id: row for row in session.query(AgentRuntimeState).all()}
    now = datetime.now(timezone.utc)
    for agent_id, item in desired.items():
        row = existing.get(agent_id)
        if row:
            if not row.icon or row.icon in item.get("legacy_icons", []):
                row.icon = item["icon"]
            if not row.logo_data:
                row.logo_data = item.get("logo_data", DEFAULT_CORPORATE_LOGO)
            if not row.name:
                row.name = item["name"]
            if not row.role:
                row.role = item["role"]
            continue
        session.add(
            AgentRuntimeState(
                agent_id=agent_id,
                name=item["name"],
                role=item["role"],
                icon=item["icon"],
                logo_data=item.get("logo_data", DEFAULT_CORPORATE_LOGO),
                trust_mode=item["trust_mode"],
                enabled=True,
                created_at=now,
                updated_at=now,
            )
        )
    session.commit()


def _ensure_seed_messages(session) -> None:
    if session.query(func.count(AgentMessage.id)).scalar():
        return
    seeds = [
        ("raven", "oracle", "observation", "Raven is ready to route operational observations into investigations."),
        ("oracle", "gate-keeper", "recommendation", "The Oracle will keep recommendations separate from approved actions."),
        ("gate-keeper", "executioner", "approval_request", "Gate Keeper requires human approval before any container refresh or git pull."),
        ("sage", "orc-orchestrator", "lesson_learned", "Sage will write accepted lessons and proposed skills into Markdown."),
    ]
    for source, target, msg_type, summary in seeds:
        session.add(
            AgentMessage(
                source_agent=source,
                target_agent=target,
                message_type=msg_type,
                summary=summary,
                payload="{}",
            )
        )
    session.commit()


def _agent_dict(row: AgentRuntimeState) -> dict:
    return {
        "id": row.agent_id,
        "name": row.name,
        "role": row.role,
        "icon": row.icon or _default_agent_icon(row.agent_id, row.role),
        "logo_data": row.logo_data or DEFAULT_CORPORATE_LOGO,
        "trust_mode": row.trust_mode,
        "enabled": row.enabled,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
    }


def _message_dict(row: AgentMessage) -> dict:
    return {
        "id": row.id,
        "thread_id": row.thread_id,
        "source_agent": row.source_agent,
        "target_agent": row.target_agent or "",
        "message_type": row.message_type,
        "summary": row.summary,
        "payload": json.loads(row.payload or "{}"),
        "visibility": row.visibility,
        "created_at": row.created_at.isoformat(),
    }


def _approval_dict(row: ApprovalRequest) -> dict:
    return {
        "id": row.id,
        "title": row.title,
        "requester_agent": row.requester_agent,
        "approver_agent": row.approver_agent,
        "action_type": row.action_type,
        "target": row.target,
        "rationale": row.rationale,
        "risk_level": row.risk_level,
        "status": row.status,
        "requested_by": row.requested_by,
        "decided_by": row.decided_by or "",
        "decision_reason": row.decision_reason or "",
        "execution_allowed": row.execution_allowed,
        "requested_at": row.requested_at.isoformat(),
        "decided_at": row.decided_at.isoformat() if row.decided_at else None,
    }


def _learning_dict(row: LearningEntry) -> dict:
    return {
        "id": row.id,
        "title": row.title,
        "source_agent": row.source_agent,
        "incident_ref": row.incident_ref,
        "outcome": row.outcome,
        "summary": row.summary,
        "markdown_path": row.markdown_path,
        "created_at": row.created_at.isoformat(),
    }


def _focused_watch_dict(row: FocusedWatch, conn: Connection | None = None) -> dict:
    server_name = ""
    connection_name = ""
    if conn is not None:
        connection_name = conn.name
        server_name = conn.server_name or conn.name
    return {
        "id": row.id,
        "connection_id": row.connection_id,
        "connection_name": connection_name,
        "server_name": server_name,
        "endpoint_id": row.endpoint_id,
        "container_id": row.container_id,
        "container_name": row.container_name,
        "interval_seconds": row.interval_seconds,
        "expires_at": row.expires_at.isoformat(),
        "last_polled_at": row.last_polled_at.isoformat() if row.last_polled_at else None,
        "created_by": row.created_by or "",
        "rationale": row.rationale or "",
        "created_at": row.created_at.isoformat(),
    }


def _record_agent_message(
    session,
    source: str,
    target: str,
    message_type: str,
    summary: str,
    payload: dict | None = None,
    thread_id: str = "operations",
) -> AgentMessage:
    row = AgentMessage(
        thread_id=thread_id or "operations",
        source_agent=source,
        target_agent=target or None,
        message_type=message_type,
        summary=summary,
        payload=json.dumps(payload or {}),
    )
    session.add(row)
    session.commit()
    session.refresh(row)
    try:
        from . import raven as _raven

        _raven.publish(
            {
                "type": "agent_message",
                "source_agent": source,
                "target_agent": target,
                "message_type": message_type,
                "summary": summary,
            }
        )
    except Exception:
        pass
    return row


# ---------------------------------------------------------------------------
# App HTML
# ---------------------------------------------------------------------------

_HTML = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>ORC</title>
<link rel="icon" href="/assets/favicon.ico" sizes="any">
<style>
:root{--bg:#0d1117;--sur:#161b22;--bdr:#30363d;--txt:#e6edf3;--mut:#8b949e;--grn:#3fb950;--red:#f85149;--yel:#d29922;--blu:#58a6ff;--pur:#a371f7}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--txt);font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;font-size:14px;height:100vh;display:flex;flex-direction:column;overflow:hidden}
.app-shell{height:100vh;display:flex;flex-direction:column;overflow:hidden}
.app-shell.hidden{display:none}
.login-screen{height:100vh;display:flex;align-items:center;justify-content:center;padding:18px;background:#0f141b}
.login-screen.hidden{display:none}
.login-panel{width:min(390px,100%);border:1px solid var(--bdr);border-radius:8px;background:var(--sur);padding:22px;box-shadow:0 20px 40px rgba(0,0,0,.28)}
.login-brand{display:flex;align-items:center;gap:11px;margin-bottom:18px}
.login-brand img{width:52px;height:52px;border-radius:50%;object-fit:cover;border:1px solid rgba(230,237,243,.18)}
.login-title{font-size:1.2rem;font-weight:900}
.login-sub{font-size:.78rem;color:var(--mut);margin-top:2px}
.login-form{display:flex;flex-direction:column;gap:10px}
.login-error{min-height:18px;color:var(--red);font-size:.78rem}
/* NAV */
.nav{background:rgba(22,27,34,.96);border-bottom:1px solid var(--bdr);padding:0 18px;display:flex;align-items:center;gap:14px;height:64px;flex-shrink:0;backdrop-filter:blur(12px)}
.brand{background:none;border:0;color:var(--txt);cursor:pointer;padding:0;font:inherit;font-weight:800;font-size:1.14rem;margin-right:8px;display:flex;align-items:center;gap:10px;letter-spacing:.02em}
.brand:hover .brand-mark{border-color:rgba(163,113,247,.5)}
.brand-mark{width:46px;height:46px;border-radius:50%;overflow:hidden;display:inline-flex;align-items:center;justify-content:center;background:#0d1117;border:1px solid rgba(230,237,243,.24);box-shadow:0 0 0 1px rgba(0,0,0,.55) inset,0 8px 18px rgba(0,0,0,.28);flex-shrink:0}
.brand-mark .brand-char{width:148%;height:148%;object-fit:cover;object-position:center 36%;filter:saturate(.95) contrast(1.06);-webkit-mask-image:radial-gradient(circle at center,#000 46%,rgba(0,0,0,.75) 63%,transparent 84%);mask-image:radial-gradient(circle at center,#000 46%,rgba(0,0,0,.75) 63%,transparent 84%)}
.brand-corp-icon{display:none;width:52%;height:52%;flex-shrink:0;color:#c8d3de}
[data-view-mode="corporate"] .brand-char{display:none}
[data-view-mode="corporate"] .brand-corp-icon{display:block}
.tabs{display:flex;height:100%;overflow-x:auto;scrollbar-width:none}
.tabs::-webkit-scrollbar{display:none}
.tab{background:none;border:none;border-bottom:2px solid transparent;color:var(--mut);cursor:pointer;padding:0 14px;font-size:.94rem;font-weight:600;height:100%}
.tab:hover{color:var(--txt)}.tab.on{color:var(--txt);border-bottom-color:var(--pur)}
.nav-r{margin-left:auto;display:flex;align-items:center;gap:8px}
.nav-sel{background:#21262d;border:1px solid var(--bdr);border-radius:8px;color:var(--txt);font-size:.78rem;padding:5px 9px;outline:none;cursor:pointer;min-width:72px}
.user-chip{display:flex;align-items:center;gap:6px;color:var(--txt)}
/* Status pills */
.sp{font-size:.75rem;padding:2px 8px;border-radius:10px;background:#21262d;border:1px solid var(--bdr);display:flex;align-items:center;gap:4px}
.dot{display:inline-block;width:8px;height:8px;border-radius:50%;background:var(--mut)}
.dot.ok{background:var(--grn)}.dot.er{background:var(--red)}
/* Layout */
.layout{display:grid;grid-template-columns:minmax(0,1fr) var(--aside-width,292px);flex:1;overflow:hidden}
.main{overflow-y:auto;padding:14px}
.pane{display:none}.pane.on{display:block}
/* STACK MAP */
#pane-overview,#pane-network{position:relative;min-height:calc(100vh - 92px);padding:12px;border-radius:14px;overflow:hidden;background:#0f141b;box-shadow:inset 0 0 0 1px rgba(230,237,243,.06)}
html[data-view-mode="character"] #pane-overview,html[data-view-mode="character"] #pane-network{background:
  linear-gradient(rgba(13,17,23,.18),rgba(13,17,23,.34)),
  url('/assets/kingdoms/pale-strategy-map.png') center/cover no-repeat}
html[data-view-mode="character"] #pane-overview::before,html[data-view-mode="character"] #pane-network::before{content:"";position:absolute;inset:0;background:radial-gradient(circle at center,rgba(255,255,255,.04),rgba(13,17,23,.06) 52%,rgba(13,17,23,.18) 100%);pointer-events:none}
#pane-home{min-height:calc(100vh - 92px);padding:12px;background:#0f141b}
.home-grid{display:grid;grid-template-rows:auto auto minmax(0,1fr);gap:12px;min-height:calc(100vh - 116px)}
.dash-section{border:1px solid var(--bdr);border-radius:8px;background:var(--sur);padding:12px;min-width:0}
.dash-title{font-size:.78rem;font-weight:850;letter-spacing:.04em;text-transform:uppercase;color:var(--txt);margin-bottom:9px}
.issue-list{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:7px;min-width:0}
.metric-list,.recent-list{display:flex;flex-direction:column;gap:7px;min-width:0}
.home-issue-row{cursor:pointer;min-height:28px;padding:3px 50px 3px 7px}
.home-issue-main{min-width:0;display:flex;flex-direction:column;gap:1px}
.home-issue-row .sub-name{font-size:.65rem;line-height:1.08}
.home-issue-server{color:var(--mut);font-size:.55rem;line-height:1.08;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.home-issue-row .sub-type{margin-right:4px;font-size:.54rem;padding:1px 4px}
.home-issue-row .status-dot{width:16px;height:16px;font-size:.48rem}
.metric-table-head,.metric-summary,.metric-stack{display:grid;grid-template-columns:minmax(210px,1fr) 74px 74px 92px 152px;gap:10px;align-items:center}
.metric-table-head{color:var(--mut);font-size:.62rem;font-weight:800;text-transform:uppercase;letter-spacing:.04em;padding:0 10px 4px}
.metric-connection{border:1px solid #21262d;border-radius:7px;background:#0d1117;min-width:0;overflow:hidden}
.metric-connection summary{list-style:none;cursor:pointer}
.metric-connection summary::-webkit-details-marker{display:none}
.metric-summary{padding:8px 10px;color:var(--txt)}
.metric-summary:hover{background:#111821}
.metric-name{display:flex;align-items:center;gap:8px;min-width:0;font-weight:850;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.metric-caret{width:0;height:0;border-top:4px solid transparent;border-bottom:4px solid transparent;border-left:6px solid var(--mut);transition:transform .14s;flex-shrink:0}
.metric-connection[open] .metric-caret{transform:rotate(90deg)}
.metric-num{font-size:.73rem;font-variant-numeric:tabular-nums;text-align:right;color:var(--txt)}
.metric-children{border-top:1px solid #21262d;background:#0b1118}
.metric-stack{padding:6px 10px;color:var(--mut);font-size:.74rem}
.metric-stack+.metric-stack{border-top:1px solid rgba(33,38,45,.72)}
.metric-stack-name{position:relative;color:var(--txt);font-weight:700;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;padding-left:20px}
.metric-stack-name::before{content:"";position:absolute;left:4px;top:0;width:9px;height:50%;border-left:1px solid #30363d;border-bottom:1px solid #30363d}
.home-health{display:flex;align-items:center;justify-content:flex-end;gap:7px;min-width:0}
.home-health-bar{width:82px;height:7px;border-radius:999px;overflow:hidden;background:rgba(139,148,158,.22);flex-shrink:0}
.home-health-bar span{display:block;height:100%;border-radius:inherit;background:var(--grn)}
.home-health.warn .home-health-bar span{background:var(--yel)}
.home-health.err .home-health-bar span{background:var(--red)}
.home-health-pct{font-size:.68rem;font-weight:850;font-variant-numeric:tabular-nums;min-width:34px;text-align:right}
.recent-list{max-height:270px;overflow:auto;padding-right:2px}
.recent-issue{display:grid;grid-template-columns:auto minmax(0,1fr) auto;gap:9px;align-items:center;border:1px solid #21262d;border-radius:7px;background:#0d1117;padding:7px 9px;cursor:pointer}
.recent-issue:hover{border-color:var(--pur)}
.recent-name{overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-weight:700;font-size:.77rem}
.recent-msg{grid-column:2 / 4;color:var(--mut);font-size:.7rem;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.recent-sev{font-size:.62rem;font-weight:850;text-transform:uppercase;border-radius:999px;padding:3px 7px}
.recent-sev.err{background:rgba(248,81,73,.16);color:var(--red);border:1px solid rgba(248,81,73,.34)}
.recent-sev.warn{background:rgba(210,153,34,.16);color:var(--yel);border:1px solid rgba(210,153,34,.34)}
.map-grid{position:relative;z-index:1;display:flex;flex-direction:column;gap:11px}
.kingdom{border:1px solid rgba(163,113,247,.42);border-radius:9px;padding:10px;background:rgba(13,17,23,.62);box-shadow:0 10px 24px rgba(0,0,0,.16);backdrop-filter:blur(2px)}
.kingdom.corp{background:#151a22;border-color:#2f3844;box-shadow:none;backdrop-filter:none}
.kingdom.er{border-color:rgba(248,81,73,.68)}
.kingdom.warn{border-color:rgba(210,153,34,.7)}
.kingdom-hdr{display:flex;align-items:center;justify-content:space-between;gap:10px;margin-bottom:10px;min-width:0}
.kingdom-title{display:flex;align-items:center;gap:8px;min-width:0}
.kingdom-castle{width:38px;height:38px;object-fit:contain;filter:drop-shadow(0 3px 5px rgba(0,0,0,.45));flex-shrink:0}
.kingdom-logo{width:38px;height:38px;border-radius:8px;object-fit:cover;background:#0d1117;border:1px solid rgba(230,237,243,.18);flex-shrink:0}
.kingdom-copy{min-width:0}
.kingdom-name{font-size:.92rem;font-weight:800;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.kingdom-sub{font-size:.66rem;color:var(--mut);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.kingdom-score{display:flex;align-items:center;gap:5px;flex-shrink:0}
.kingdom-stacks{display:grid;grid-template-columns:repeat(auto-fill,minmax(200px,1fr));gap:9px;align-items:start}
.stack-card{position:relative;background:var(--sur);border:1px solid var(--bdr);border-radius:8px;padding:8px;display:flex;flex-direction:column;gap:6px;min-width:0;transition:border-color .2s}
.stack-card:hover{border-color:var(--pur)}
.stack-card.er{border-color:var(--red)}
.stack-card.warn{border-color:var(--yel)}
.char-frame{position:relative;display:flex;align-items:center;justify-content:center;width:100%;aspect-ratio:1.62/1;background:#0d1117;border:1px solid #21262d;border-radius:6px;overflow:hidden;cursor:pointer;padding:0;color:inherit;font:inherit}
.char-frame:hover{border-color:var(--pur)}
.char-img{display:block;width:100%;height:100%;object-fit:contain}
.corp .char-img{object-fit:contain;padding:14px}
.stack-banner{position:absolute;left:0;right:0;bottom:0;height:27px;display:flex;align-items:center;justify-content:center;padding:0 32px 0 8px;background:rgba(48,54,61,.94);backdrop-filter:blur(3px);font-size:.72rem;font-weight:800;letter-spacing:0;text-shadow:0 1px 2px rgba(0,0,0,.45);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.status-circles{position:absolute;top:6px;left:6px;display:flex;gap:4px}
.kingdom-score .status-circles{position:static}
.status-dot{width:21px;height:21px;border-radius:50%;font-size:.56rem;font-weight:800;display:flex;align-items:center;justify-content:center;border:1px solid rgba(0,0,0,.58);box-shadow:0 1px 5px rgba(0,0,0,.42);line-height:1}
.b-ok{background:var(--grn);color:#000}
.b-warn{background:var(--yel);color:#000}
.b-err{background:var(--red);color:#fff}
.b-hide{display:none}
.gear-btn{position:absolute;top:6px;right:6px;width:24px;height:24px;border-radius:50%;border:1px solid rgba(230,237,243,.28);background:rgba(13,17,23,.78);color:var(--txt);cursor:pointer;display:flex;align-items:center;justify-content:center;font-size:13px;line-height:1;z-index:2}
.gear-btn:hover{border-color:var(--pur);background:rgba(33,38,45,.92)}
.stack-nm{font-size:.8rem;font-weight:800;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.stack-sv{font-size:.62rem;color:var(--mut);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.stack-meta{display:flex;justify-content:space-between;gap:8px;align-items:flex-start;min-width:0}
.stack-copy{min-width:0}
.sub-list{display:flex;flex-direction:column;gap:5px;border-top:1px solid #21262d;padding-top:6px}
.sub-row{position:relative;display:flex;align-items:center;gap:6px;min-height:23px;padding:3px 54px 3px 8px;border-radius:6px;background:#0d1117;border:1px solid #21262d;cursor:pointer}
.sub-row:hover{border-color:var(--pur)}
.sub-name{font-size:.68rem;font-weight:650;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.sub-type{margin-left:auto;color:var(--mut);font-size:.58rem;text-transform:uppercase;border:1px solid #21262d;border-radius:4px;padding:2px 5px;line-height:1.15;flex-shrink:0}
.sub-row .status-circles{top:50%;left:auto;right:7px;transform:translateY(-50%)}
.sub-row .status-dot{width:18px;height:18px;font-size:.52rem}
.char-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(96px,1fr));gap:10px}
.char-choice{border:1px solid var(--bdr);background:#0d1117;color:var(--txt);border-radius:8px;padding:7px;cursor:pointer;text-align:left}
.char-choice:hover,.char-choice.on{border-color:var(--pur);background:#21262d}
.char-choice img{display:block;width:100%;aspect-ratio:1.62/1;object-fit:cover;border-radius:5px;margin-bottom:5px}
.char-choice span{display:block;font-size:.72rem;font-weight:650;text-align:center}
.logo-choice img,.agent-logo-choice img{object-fit:contain;background:#0d1117}
.file-row{display:flex;align-items:center;gap:9px;min-width:0}
.logo-preview{width:42px;height:42px;border-radius:8px;object-fit:cover;background:#0d1117;border:1px solid var(--bdr);flex-shrink:0}
.logo-preview.empty{display:none}
.net-tools{position:absolute;z-index:5;right:14px;top:14px;display:flex;gap:5px;background:rgba(13,17,23,.72);border:1px solid rgba(230,237,243,.12);border-radius:8px;padding:5px}
.network-stage{position:relative;z-index:1;height:calc(100vh - 118px);min-height:680px;overflow:hidden;cursor:grab;touch-action:none;border-radius:10px}
.network-stage.dragging{cursor:grabbing}
.raven-flight{position:absolute;z-index:9;left:0;top:0;width:60px;height:42px;object-fit:contain;pointer-events:none;filter:drop-shadow(0 0 1px rgba(255,255,255,.82)) drop-shadow(0 7px 12px rgba(0,0,0,.5));animation:raven-flight-to-center 1.15s ease-in-out forwards}
.raven-flight.focused{z-index:10;filter:drop-shadow(0 0 2px rgba(255,214,102,.95)) drop-shadow(0 7px 12px rgba(0,0,0,.52)) hue-rotate(-14deg) saturate(1.2);animation:raven-flight-to-worker 1.25s ease-in-out forwards}
.raven-scroll-drop{position:absolute;z-index:8;left:0;top:0;width:58px;height:auto;object-fit:contain;pointer-events:none;filter:drop-shadow(0 5px 7px rgba(0,0,0,.42));animation:raven-scroll-drop 1.65s ease-out forwards}
@keyframes raven-flight-to-center{0%{opacity:0;transform:translate(calc(var(--sx) - 30px),calc(var(--sy) - 21px)) scale(.38) rotate(var(--start-rot));filter:drop-shadow(0 0 1px rgba(255,255,255,.82)) drop-shadow(0 7px 12px rgba(0,0,0,.5)) blur(0)}16%{opacity:1}82%{opacity:1}100%{opacity:0;transform:translate(calc(var(--ex) - 30px),calc(var(--ey) - 21px)) scale(.76) rotate(var(--end-rot));filter:drop-shadow(0 0 5px rgba(230,237,243,.28)) blur(1.4px)}}
@keyframes raven-flight-to-worker{0%{opacity:0;transform:translate(calc(var(--sx) - 30px),calc(var(--sy) - 21px)) scale(.42) rotate(var(--start-rot));filter:drop-shadow(0 0 2px rgba(255,214,102,.95)) drop-shadow(0 7px 12px rgba(0,0,0,.52)) blur(0)}16%{opacity:1}82%{opacity:1}100%{opacity:0;transform:translate(calc(var(--ex) - 30px),calc(var(--ey) - 21px)) scale(.8) rotate(var(--end-rot));filter:drop-shadow(0 0 6px rgba(255,214,102,.55)) blur(1.3px)}}
@keyframes raven-scroll-drop{0%{opacity:0;transform:translate(calc(var(--x) - 29px),calc(var(--y) - 8px)) scale(.28) rotate(-9deg)}18%{opacity:1}72%{opacity:.95}100%{opacity:0;transform:translate(calc(var(--x) - 29px),calc(var(--y) + 44px)) scale(1) rotate(3deg)}}
.net-pan-surface{position:absolute;left:0;top:0;transform-origin:0 0;will-change:transform}
.net-backbone{position:absolute;z-index:0;left:0;top:0;overflow:visible;pointer-events:none}
.net-backbone line{stroke:rgba(139,148,158,.58);stroke-width:4.2;vector-effect:non-scaling-stroke}
.net-backbone line.hub-link{stroke:rgba(163,113,247,.55);stroke-width:5}
.net-backbone line.err{stroke:rgba(248,81,73,.86);stroke-width:5.4}.net-backbone line.warn{stroke:rgba(210,153,34,.88);stroke-width:5.4}
.net-backbone line.worker-link{stroke:rgba(31,37,45,.86);stroke-width:3.05}
.net-backbone line.worker-link.err{stroke:rgba(248,81,73,.88);stroke-width:3.35}.net-backbone line.worker-link.warn{stroke:rgba(210,153,34,.9);stroke-width:3.35}
.net-hub,.network-stack,.net-worker{position:absolute;left:var(--x);top:var(--y);transform:translate(-50%,-50%)}
.net-hub,.network-stack,.net-worker{cursor:grab}
.net-hub:active,.network-stack:active,.net-worker:active{cursor:grabbing}
.net-hub{z-index:3;width:66px;height:66px;border-radius:50%;display:flex;align-items:center;justify-content:center;background:rgba(13,17,23,.9);border:2px solid rgba(88,166,255,.8);box-shadow:0 0 0 5px rgba(88,166,255,.14),0 10px 24px rgba(0,0,0,.26)}
.net-hub img{width:45px;height:45px;border-radius:50%;object-fit:cover}
.net-hub-meta{position:absolute;left:50%;top:70px;transform:translateX(-50%);display:flex;align-items:center;gap:5px;max-width:260px}
.net-hub-label{position:static;max-width:168px;background:rgba(48,54,61,.92);border:1px solid rgba(230,237,243,.14);border-radius:5px;padding:3px 7px;font-size:.65rem;font-weight:800;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:var(--txt)}
.net-health{height:21px;display:inline-flex;align-items:center;gap:5px;padding:2px 6px;border-radius:999px;background:rgba(13,17,23,.88);border:1px solid rgba(230,237,243,.16);box-shadow:0 2px 8px rgba(0,0,0,.24);font-size:.58rem;font-weight:900;color:var(--txt);white-space:nowrap;flex-shrink:0}
.net-health-bar{width:36px;height:6px;overflow:hidden;border-radius:999px;background:rgba(139,148,158,.22)}
.net-health-bar span{display:block;height:100%;border-radius:inherit;background:var(--grn)}
.net-health.warn .net-health-bar span{background:var(--yel)}.net-health.err .net-health-bar span{background:var(--red)}
.network-stack{z-index:4;width:82px;height:82px;isolation:isolate}
.net-stack-node{position:relative;width:82px;height:82px;border-radius:50%;overflow:hidden;border:1px solid rgba(230,237,243,.28);box-shadow:0 0 0 3px rgba(13,17,23,.68),0 8px 18px rgba(0,0,0,.32);background:#0d1117;cursor:grab}
.net-stack-node img{width:100%;height:100%;object-fit:cover;filter:saturate(.9) contrast(1.05);opacity:.82;-webkit-mask-image:radial-gradient(circle at center,#000 55%,rgba(0,0,0,.62) 72%,transparent 90%);mask-image:radial-gradient(circle at center,#000 55%,rgba(0,0,0,.62) 72%,transparent 90%)}
.net-stack-name{position:absolute;left:6px;right:6px;bottom:6px;min-width:0;max-width:none;background:rgba(48,54,61,.94);border:1px solid rgba(230,237,243,.14);border-radius:5px;padding:3px 5px;font-size:.61rem;line-height:1.05;font-weight:850;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;text-align:center;color:var(--txt)}
.net-worker{display:flex;align-items:center;gap:5px;min-width:0;max-width:142px;z-index:3;filter:drop-shadow(0 7px 10px rgba(0,0,0,.28))}
.net-worker.left{flex-direction:row-reverse}.net-worker.left .net-worker-name{text-align:right}
.worker-avatar{position:relative;width:48px;height:48px;overflow:visible;background:transparent;border:0;flex:0 0 48px}
.worker-avatar::after{content:"";display:none;position:absolute;right:0;bottom:4px;width:13px;height:13px;border-radius:50%;background:#020407;border:2px solid rgba(255,255,255,.72);box-shadow:0 2px 8px rgba(0,0,0,.55)}
.net-worker.checking .worker-avatar::after{display:block}
.net-worker.focused-checking .worker-avatar::after{display:block;background:#d29922;border-color:rgba(255,241,179,.9)}
.worker-avatar img{width:100%;height:100%;object-fit:contain;position:static;display:block}
.net-dot{position:absolute;left:34px;top:-3px;width:21px;height:21px;border-radius:50%;box-shadow:0 0 0 2px #0d1117,0 2px 8px rgba(0,0,0,.35);display:flex;align-items:center;justify-content:center;font-size:.58rem;line-height:1;font-weight:900;color:#fff;flex-shrink:0}
.net-worker.left .net-dot{left:auto;right:34px}
.net-dot.err{background:var(--red)}.net-dot.warn{background:var(--yel);color:#211300}.net-dot.none{display:none}
.net-worker-name{display:block;max-width:86px;background:rgba(48,54,61,.88);border:1px solid rgba(230,237,243,.12);border-radius:5px;padding:2px 5px;font-size:.62rem;line-height:1.05;font-weight:750;color:var(--txt);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.net-detail-bubble{position:absolute;z-index:20;left:calc(100% + 10px);top:50%;transform:translate(4px,-50%);width:230px;border:1px solid rgba(88,166,255,.32);border-radius:8px;background:rgba(13,17,23,.96);box-shadow:0 14px 28px rgba(0,0,0,.38);padding:8px 9px;opacity:0;pointer-events:none;transition:opacity .12s,transform .12s;color:var(--txt);text-align:left}
.net-worker.left .net-detail-bubble{left:auto;right:calc(100% + 10px);transform:translate(-4px,-50%)}
.net-worker:hover .net-detail-bubble{opacity:1;transform:translate(0,-50%)}
.net-worker.left:hover .net-detail-bubble{transform:translate(0,-50%)}
.net-detail-title{font-size:.74rem;font-weight:850;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;margin-bottom:5px}
.net-detail-grid{display:grid;grid-template-columns:68px minmax(0,1fr);gap:3px 8px;font-size:.65rem;line-height:1.22}
.net-detail-grid span:nth-child(odd){color:var(--mut)}
.net-detail-grid span:nth-child(even){overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
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
.aside{position:relative;border-left:1px solid var(--bdr);display:flex;flex-direction:column;overflow:hidden;background:var(--sur)}
.aside-width-grip{position:absolute;z-index:12;left:-5px;top:0;bottom:0;width:10px;cursor:col-resize;background:transparent}
.aside-width-grip::before{content:"";position:absolute;left:4px;top:0;bottom:0;width:1px;background:rgba(230,237,243,.08)}
.hb-wrap{position:relative;padding:8px 10px 7px;border-bottom:1px solid var(--bdr);flex-shrink:0;background:#12171f}
.hb-lbl{position:absolute;z-index:2;left:14px;right:14px;top:10px;display:flex;align-items:center;justify-content:space-between;gap:8px;pointer-events:none}
.hb-title{font-size:.76rem;font-weight:800;letter-spacing:.06em;display:flex;align-items:center;gap:7px;text-shadow:0 2px 5px rgba(0,0,0,.9)}
.raven-mark{width:48px;height:48px;object-fit:contain;filter:drop-shadow(0 0 1px rgba(255,255,255,.95)) drop-shadow(0 0 4px rgba(230,237,243,.7)) drop-shadow(0 4px 8px rgba(0,0,0,.7));margin-top:-2px;flex-shrink:0}
.hb-st{display:none}
.hb-canvas-wrap{height:76px;background:#0d1117;border:1px solid #21262d;border-radius:10px;padding:10px 8px 6px;overflow:hidden}
canvas{display:block;width:100%;height:58px}
.hb-tip{position:absolute;z-index:4;display:none;max-width:220px;padding:6px 8px;border-radius:8px;border:1px solid rgba(230,237,243,.16);background:rgba(13,17,23,.96);color:var(--txt);font-size:.7rem;line-height:1.3;pointer-events:none;box-shadow:0 10px 20px rgba(0,0,0,.36)}
.raven-sl{padding:5px 10px;border-bottom:1px solid var(--bdr);font-size:.73rem;color:var(--mut);flex-shrink:0;min-height:26px;display:flex;align-items:center;gap:5px;overflow:hidden}
.sl-icon{flex-shrink:0}.sl-txt{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.raven-next{padding:3px 10px 4px;border-bottom:1px solid var(--bdr);font-size:.69rem;color:var(--mut);min-height:22px;display:flex;align-items:center;gap:5px;overflow:hidden;flex-shrink:0}
.raven-next-lbl{color:#8b949e;font-weight:700;flex-shrink:0;font-size:.62rem;text-transform:uppercase;letter-spacing:.04em}
#raven-next-txt{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.feed-hdr{display:flex;gap:4px;padding:5px 8px;border-bottom:1px solid var(--bdr);flex-shrink:0}
.ff{background:#21262d;border:1px solid var(--bdr);border-radius:12px;color:var(--mut);cursor:pointer;font-size:.64rem;padding:2px 0;flex:1;text-align:center}
.ff:hover{color:var(--txt)}.ff.on{background:var(--bdr);color:var(--txt)}
.feed{flex:0 1 var(--raven-feed-height,150px);min-height:52px;overflow-y:auto;padding:6px 8px 8px;display:flex;flex-direction:column}
.pill{margin-bottom:5px;border-radius:8px;padding:6px 8px;font-size:.68rem;border:1px solid transparent}
.p-start{background:#161f2e;border-color:#1d2d45;color:var(--blu)}
.p-error{background:#2a1515;border-color:#4a2020;color:var(--red)}
.p-warn{background:#2a2000;border-color:#4a3800;color:var(--yel)}
.p-ok{background:#152215;border-color:#1f3d1f;color:var(--grn)}
.p-clean{background:#161b22;border-color:var(--bdr);color:var(--mut)}
.p-checking{background:#161b22;border-color:#21262d;color:var(--mut)}
.ph{color:var(--mut);font-size:.72rem;text-align:center;padding:12px 0}
.pill-hdr{display:flex;justify-content:space-between;align-items:baseline;margin-bottom:2px}
.pill-cn{font-family:monospace;font-weight:650;font-size:.76rem}
.pill-sv{font-size:.64rem;opacity:.75}
.pill-msg{font-size:.71rem;line-height:1.3;margin-top:2px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;opacity:.88}
.pill-ts{font-size:.62rem;opacity:.6;text-align:right;margin-top:1px}
.oracle-resizer{height:9px;border-top:1px solid var(--bdr);border-bottom:1px solid var(--bdr);background:#10151c;cursor:ns-resize;flex-shrink:0;display:flex;align-items:center;justify-content:center}
.oracle-resizer::before{content:"";width:34px;height:3px;border-radius:5px;background:#30363d}
.oracle{padding:10px 12px 12px;display:flex;flex:1;min-height:170px;flex-direction:column;gap:7px;background:rgba(13,17,23,.24);overflow:hidden}
.oracle-hdr{display:flex;align-items:center;justify-content:space-between;gap:10px}
.oracle-title{font-size:.73rem;font-weight:800;letter-spacing:.08em;display:flex;align-items:center;gap:8px}
.oracle-mark{width:36px;height:36px;border-radius:50%;object-fit:cover;border:1px solid rgba(163,113,247,.35);box-shadow:0 4px 10px rgba(0,0,0,.28);flex-shrink:0}
.oracle-meta{font-size:.72rem;color:var(--mut);line-height:1.45}
.oracle-box{border:1px solid #21262d;border-radius:8px;background:#0d1117;padding:10px;min-height:108px;flex:1;overflow:auto;font-size:.77rem;line-height:1.45;white-space:pre-wrap}
.oracle-box strong{color:var(--txt);font-weight:850}
.oracle-box.busy{color:var(--mut)}
.oracle-box.error{border-color:rgba(248,81,73,.4);color:var(--red)}
.oracle-box.empty{color:var(--mut)}
.oracle-summary{display:flex;gap:6px;flex-wrap:wrap}
.oracle-summary .sp{font-size:.7rem}
/* ORCHESTRATION */
#pane-orchestration{min-height:calc(100vh - 92px);padding:12px;background:#0f141b}
.orch-shell{display:flex;flex-direction:column;gap:12px;min-width:0}
.orch-top{display:flex;align-items:center;justify-content:space-between;gap:10px}
.orch-title{font-size:1rem;font-weight:850;letter-spacing:.03em}
.orch-actions{display:flex;align-items:center;gap:7px}
.orch-subtabs{display:flex;gap:6px;align-items:center;overflow-x:auto;padding-bottom:2px}
.orch-tab{background:#21262d;border:1px solid var(--bdr);border-radius:6px;color:var(--mut);cursor:pointer;font-size:.78rem;font-weight:750;padding:6px 11px;white-space:nowrap}
.orch-tab:hover{color:var(--txt)}
.orch-tab.on{background:var(--bdr);color:var(--txt);border-color:rgba(163,113,247,.58)}
.orch-view{display:none}
.orch-view.on{display:block}
.orch-page-grid{display:grid;grid-template-columns:minmax(340px,.9fr) minmax(360px,1.1fr);gap:12px;align-items:start}
.orch-page-grid.single{grid-template-columns:minmax(0,1fr)}
.orch-grid{display:grid;grid-template-columns:minmax(280px,.85fr) minmax(360px,1.15fr);gap:12px;align-items:start}
.orch-stack{display:flex;flex-direction:column;gap:12px;min-width:0}
.orch-panel{border:1px solid var(--bdr);border-radius:8px;background:var(--sur);padding:12px;min-width:0}
.orch-panel-head{display:flex;align-items:center;justify-content:space-between;gap:8px;margin-bottom:10px}
.orch-panel-title{font-size:.78rem;font-weight:850;letter-spacing:.04em;text-transform:uppercase}
.orch-count{font-size:.68rem;color:var(--mut)}
.agent-list,.approval-list,.learning-list,.skill-list{display:flex;flex-direction:column;gap:8px;min-width:0}
.agent-card{display:grid;grid-template-columns:42px minmax(0,1fr);gap:9px;border:1px solid #21262d;border-radius:8px;background:#0d1117;padding:9px;min-width:0}
.agent-avatar{width:42px;height:42px;border-radius:50%;object-fit:cover;background:#05080d;border:1px solid rgba(230,237,243,.16)}
.agent-avatar.corp,.chat-avatar.corp{object-fit:contain;padding:4px;background:#0d1117}
.agent-name{font-size:.83rem;font-weight:850;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.agent-role{font-size:.68rem;color:var(--mut);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;margin-top:1px}
.agent-controls{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:7px;align-items:center;margin-top:8px}
.trust-select,.orch-input,.orch-select,.orch-textarea{width:100%;background:#0d1117;border:1px solid var(--bdr);border-radius:6px;color:var(--txt);font-size:.78rem;padding:6px 8px;outline:none}
.orch-textarea{min-height:70px;resize:vertical;line-height:1.35;font-family:inherit}
.trust-select:focus,.orch-input:focus,.orch-select:focus,.orch-textarea:focus{border-color:var(--pur)}
.agent-enabled{display:flex;align-items:center;gap:6px;color:var(--mut);font-size:.7rem;white-space:nowrap}
.orch-form{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:9px}
.orch-form .wide{grid-column:1/-1}
.orch-field{display:flex;flex-direction:column;gap:4px;min-width:0}
.orch-field label{color:var(--mut);font-size:.7rem}
.orch-chat{height:560px;display:flex;flex-direction:column}
.chat-list{flex:1;overflow:auto;display:flex;flex-direction:column;gap:9px;padding:4px 2px 10px}
.chat-row{display:flex;align-items:flex-end;gap:8px;max-width:86%}
.chat-row.right{margin-left:auto;flex-direction:row-reverse}
.chat-row.system{opacity:.55;max-width:100%}
.chat-avatar{width:34px;height:34px;border-radius:50%;object-fit:cover;background:#05080d;border:1px solid rgba(230,237,243,.14);flex-shrink:0}
.chat-bubble{border:1px solid #253041;border-radius:16px 16px 16px 5px;background:#111b27;padding:8px 10px;min-width:0}
.chat-row.right .chat-bubble{border-radius:16px 16px 5px 16px;background:#182236;border-color:rgba(163,113,247,.45)}
.chat-row.system .chat-bubble{background:#0d1117;border-color:#21262d}
.chat-row.agent-raven .chat-bubble{background:#1f312c;border-color:rgba(63,185,80,.46)}
.chat-row.agent-oracle .chat-bubble{background:#342914;border-color:rgba(210,153,34,.46)}
.chat-row.agent-gate-keeper .chat-bubble{background:#3a2416;border-color:rgba(248,81,73,.42)}
.chat-row.agent-executioner .chat-bubble{background:#331c24;border-color:rgba(255,123,114,.42)}
.chat-row.agent-sage .chat-bubble{background:#1b2a39;border-color:rgba(88,166,255,.44)}
.chat-row.agent-orc-orchestrator .chat-bubble{background:#2d243a;border-color:rgba(163,113,247,.46)}
.chat-bubble.waiting-approval{background:linear-gradient(180deg,rgba(74,53,14,.9),rgba(49,35,10,.92));border-color:rgba(210,153,34,.65);box-shadow:0 0 0 1px rgba(210,153,34,.16) inset}

.chat-meta{font-size:.69rem;color:var(--mut);margin-bottom:4px;display:flex;gap:5px;flex-wrap:wrap;align-items:center}
.chat-text{font-size:.85rem;line-height:1.4;overflow-wrap:anywhere}
.chat-approval-pill{display:inline-flex;align-items:center;gap:8px;margin:0 0 7px;padding:7px 10px;border-radius:999px;border:1px solid rgba(245,194,66,.58);background:rgba(245,194,66,.24);color:#fff1b3;font-size:.79rem;font-weight:800}
.chat-approval-pill a,.chat-approval-pill button{font-size:.76rem}
.chat-approval-actions{display:inline-flex;gap:6px;align-items:center}
.chat-compose{border-top:1px solid #21262d;padding-top:9px;display:flex;gap:7px;align-items:center}
.chat-compose .orch-input{flex:1}
.chat-compose .btnp{flex-shrink:0;white-space:nowrap}
.approval-row,.learning-row,.skill-row{border:1px solid #21262d;border-radius:8px;background:#0d1117;padding:9px;min-width:0}
.approval-head,.learning-head,.skill-head{display:flex;align-items:center;justify-content:space-between;gap:9px;margin-bottom:5px}
.approval-title,.learning-title,.skill-title{font-size:.8rem;font-weight:800;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.approval-meta,.learning-meta,.skill-meta{font-size:.67rem;color:var(--mut);display:flex;gap:7px;flex-wrap:wrap}
.approval-copy,.learning-copy{font-size:.72rem;color:var(--mut);line-height:1.35;margin-top:5px;overflow-wrap:anywhere}
.status-chip{border:1px solid #30363d;border-radius:999px;padding:2px 7px;font-size:.62rem;font-weight:850;text-transform:uppercase}
.status-chip.pending{color:var(--yel);background:rgba(210,153,34,.12);border-color:rgba(210,153,34,.34)}
.status-chip.approved{color:var(--grn);background:rgba(63,185,80,.12);border-color:rgba(63,185,80,.34)}
.status-chip.rejected{color:var(--red);background:rgba(248,81,73,.12);border-color:rgba(248,81,73,.34)}
.approval-actions{display:flex;gap:6px;margin-top:8px;flex-wrap:wrap}
.skill-actions{display:flex;align-items:center;gap:6px;flex-shrink:0}
.skill-list{max-height:220px;overflow:auto;padding-right:2px}
.user-list{display:flex;flex-direction:column;gap:8px}
.user-row{display:grid;grid-template-columns:minmax(0,1fr) auto auto;gap:9px;align-items:center;border:1px solid #21262d;border-radius:8px;background:#0d1117;padding:9px}
.user-name{font-size:.82rem;font-weight:850;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.user-role{font-size:.68rem;color:var(--mut);text-transform:capitalize}
.setup-note{font-size:.72rem;color:var(--mut);line-height:1.35}
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
.fg input[type=file]{padding:5px;font-size:.76rem}
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
  .aside-width-grip{display:none}
  .main{padding:12px}
  #pane-home,#pane-overview,#pane-network,#pane-orchestration{padding:10px}
  .issue-list{grid-template-columns:repeat(auto-fit,minmax(180px,1fr))}
  .metric-table-head,.metric-summary,.metric-stack{grid-template-columns:minmax(150px,1fr) 52px 54px 70px}
  .metric-table-head span:nth-child(5),.metric-summary .home-health,.metric-stack .home-health{display:none}
  .kingdom{padding:8px}
  .kingdom-hdr{align-items:flex-start}
  .kingdom-castle,.kingdom-logo{width:34px;height:34px}
  .kingdom-stacks{grid-template-columns:repeat(auto-fill,minmax(180px,1fr));gap:8px}
  .network-stage{grid-template-columns:1fr}
  .orch-grid{grid-template-columns:1fr}
  .orch-page-grid{grid-template-columns:1fr}
  .orch-form{grid-template-columns:1fr}
  .orch-chat{height:520px}
  .chat-compose{flex-direction:column}
}
</style>
</head>
<body>
<div class="login-screen" id="login-screen">
  <div class="login-panel">
    <div class="login-brand">
      <img src="/assets/characters/orc.png" alt="">
      <div>
        <div class="login-title">ORC</div>
        <div class="login-sub">Operations command login</div>
      </div>
    </div>
    <div class="login-form">
      <input class="orch-input" id="login-username" autocomplete="username" placeholder="Username">
      <input class="orch-input" id="login-password" autocomplete="current-password" placeholder="Password" type="password">
      <div class="login-error" id="login-error"></div>
      <button class="btnp" type="button" onclick="login()">Login</button>
    </div>
  </div>
</div>
<div class="app-shell hidden" id="app-shell">
<nav class="nav">
  <button class="brand" type="button" onclick="showTab('home')" title="Dashboard"><span class="brand-mark"><img class="brand-char" src="/assets/characters/orc.png" alt=""><svg class="brand-corp-icon" viewBox="0 0 24 24" aria-hidden="true"><path fill="currentColor" d="M12 3v10.55A4 4 0 1 0 14 17V7h4V3z"/></svg></span><span>ORC</span></button>
  <div class="tabs">
    <button class="tab" id="tab-overview" onclick="showTab('overview')">Overview</button>
    <button class="tab" id="tab-network" onclick="showTab('network')">Network</button>
    <button class="tab" id="tab-events" onclick="showTab('events')">Events</button>
    <button class="tab" id="tab-orchestration" onclick="showTab('orchestration')">Orchestration</button>
    <button class="tab" id="tab-admin" onclick="showTab('admin')" style="display:none">Admin</button>
  </div>
  <div class="nav-r">
    <button class="nav-sel" id="view-mode-toggle" onclick="toggleViewMode()" title="Switch Corporate/Character view">Corporate View</button>
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
    <span class="sp user-chip"><span id="user-name">â€”</span><span class="muted" id="user-role"></span></span>
    <button class="btns" onclick="logout()">Logout</button>
    <button class="btn" onclick="loadAll()">&#8635;</button>
  </div>
</nav>

<div class="layout">
<div class="main">

  <!-- HOME -->
  <div class="pane on" id="pane-home">
    <div class="home-grid">
      <section class="dash-section">
        <div class="dash-title">Containers with issues</div>
        <div class="issue-list" id="home-issues"><div class="empty">Loading issue containers...</div></div>
      </section>
      <section class="dash-section">
        <div class="dash-title">Metrics</div>
        <div class="metric-list" id="home-metrics"><div class="empty">Loading metrics...</div></div>
      </section>
      <section class="dash-section">
        <div class="dash-title">Last 7 issues</div>
        <div class="recent-list" id="home-recent"><div class="empty">Loading recent issues...</div></div>
      </section>
    </div>
  </div>

  <!-- OVERVIEW -->
  <div class="pane" id="pane-overview">
    <div class="map-grid" id="overview-grid"><div class="empty">Loading overview...</div></div>
  </div>

  <!-- NETWORK -->
  <div class="pane" id="pane-network">
    <div class="net-tools">
      <button class="btns" onclick="zoomNetwork(-0.1)">-</button>
      <button class="btns" onclick="zoomNetwork(0.1)">+</button>
      <button class="btns" onclick="resetNetworkView()">&#8982;</button>
    </div>
    <div class="network-stage" id="network-stage"><div class="empty">Loading network...</div></div>
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

  <!-- ADMIN -->
  <div class="pane" id="pane-admin">
    <div class="orch-shell">
      <div class="orch-top">
        <div class="orch-title">Admin</div>
      </div>
      <div class="orch-subtabs">
        <button class="orch-tab on" id="admin-tab-connections" onclick="showAdminTab('connections')">Connections</button>
        <button class="orch-tab" id="admin-tab-ai-usage" onclick="showAdminTab('ai-usage')">AI Usage</button>
        <button class="orch-tab" id="admin-tab-retention" onclick="showAdminTab('retention')">Retention</button>
      </div>

      <!-- ADMIN: Connections -->
      <section class="orch-view on" id="admin-view-connections">
        <div class="card" style="padding:16px;margin-top:12px">
          <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:14px">
            <div style="font-weight:600">Portainer Connections</div>
            <button class="btnp" onclick="openModal()">+ Add Connection</button>
          </div>
          <div id="conn-body"><div class="empty">Loading…</div></div>
        </div>
      </section>

      <!-- ADMIN: AI Usage -->
      <section class="orch-view" id="admin-view-ai-usage">
        <div style="margin-top:12px">
          <div style="display:flex;gap:8px;align-items:center;margin-bottom:16px">
            <div style="font-weight:600;font-size:1rem">AI Usage</div>
            <select id="ai-usage-days" onchange="loadAiUsage()" style="padding:5px 8px;border-radius:6px;border:1px solid var(--border,#333);background:var(--card,#1e1e1e);color:inherit;font-size:0.82rem;">
              <option value="7" selected>Last 7 days</option>
              <option value="30">Last 30 days</option>
              <option value="90">Last 90 days</option>
            </select>
            <button class="btns" onclick="loadAiUsage()">Refresh</button>
          </div>
          <div id="ai-usage-cards" style="display:flex;gap:12px;flex-wrap:wrap;margin-bottom:20px"></div>
          <div class="card" style="padding:16px;margin-bottom:16px">
            <div style="font-weight:600;margin-bottom:10px">Usage by Agent</div>
            <div id="ai-usage-table"><div class="empty">Loading...</div></div>
          </div>
          <div class="card" style="padding:16px">
            <div style="font-weight:600;margin-bottom:10px">Daily Token Usage</div>
            <canvas id="ai-usage-chart" width="700" height="160" style="max-width:100%;display:block"></canvas>
          </div>
        </div>
      </section>

      <!-- ADMIN: Retention -->
      <section class="orch-view" id="admin-view-retention">
        <div style="margin-top:12px">
          <div class="card" style="padding:16px;margin-bottom:16px">
            <div style="font-weight:600;font-size:1rem;margin-bottom:4px">Event Retention</div>
            <div style="font-size:.78rem;color:var(--mut);margin-bottom:14px">The worker automatically deletes events older than the retention window every 5 minutes. Pruning does not affect current polling.</div>
            <div style="display:flex;flex-wrap:wrap;gap:16px;align-items:flex-end;margin-bottom:14px">
              <div class="fg">
                <label>Retention window (days)</label>
                <input id="retention-days" type="number" min="1" max="365" class="orch-input" style="width:120px" placeholder="30">
              </div>
              <button class="btnp" type="button" onclick="saveRetentionSettings()">Save</button>
              <span id="retention-save-msg" style="font-size:.78rem;color:var(--green,#3fb950)"></span>
            </div>
          </div>
          <div class="card" style="padding:16px;margin-bottom:16px">
            <div style="font-weight:600;margin-bottom:10px">Event Database Stats</div>
            <div id="retention-stats"><div class="empty">Loading…</div></div>
          </div>
          <div class="card" style="padding:16px">
            <div style="font-weight:600;margin-bottom:10px">Manual Prune</div>
            <div style="font-size:.78rem;color:var(--mut);margin-bottom:10px">Delete all events older than the current retention window immediately.</div>
            <button class="btns" type="button" onclick="pruneEventsNow()" id="prune-btn">Prune Now</button>
            <span id="prune-msg" style="font-size:.78rem;margin-left:10px"></span>
          </div>
        </div>
      </section>
    </div>
  </div>

  <!-- ORCHESTRATION -->
  <div class="pane" id="pane-orchestration">
    <div class="orch-shell">
      <div class="orch-top">
        <div class="orch-title">Orchestration</div>
        <div class="orch-actions">
          <span class="sp"><span id="orch-agent-count">0</span><span class="muted"> agents</span></span>
          <span class="sp"><span id="orch-approval-count">0</span><span class="muted"> pending</span></span>
          <button class="btns" onclick="loadOrchestration()">Refresh</button>
        </div>
      </div>

      <div class="orch-subtabs">
        <button class="orch-tab on" id="orch-tab-chat" onclick="showOrchTab('chat')">Agent Chat</button>
        <button class="orch-tab" id="orch-tab-agents" onclick="showOrchTab('agents')">Agents</button>
        <button class="orch-tab" id="orch-tab-skills" onclick="showOrchTab('skills')">Skills</button>
        <button class="orch-tab" id="orch-tab-approvals" onclick="showOrchTab('approvals')">Approvals</button>
        <button class="orch-tab" id="orch-tab-learning" onclick="showOrchTab('learning')">Learning</button>
        <button class="orch-tab" id="orch-tab-setup" onclick="showOrchTab('setup')">Setup</button>
      </div>

      <section class="orch-view on" id="orch-view-chat">
        <div class="orch-page-grid single">
          <section class="orch-panel orch-chat">
            <div class="orch-panel-head">
              <div class="orch-panel-title">Agent Chat</div>
              <div class="orch-count" id="orch-message-count"></div>
            </div>
            <div class="chat-list" id="orch-chat-list"><div class="empty">Loading messages...</div></div>
            <div id="chat-typing" style="display:none;padding:4px 12px;font-size:0.8rem;color:var(--muted,#888);font-style:italic;">Agent is thinking...</div>
            <div class="chat-compose">
              <select id="chat-agent-target" style="padding:6px 8px;border-radius:6px;border:1px solid var(--border,#333);background:var(--card,#1e1e1e);color:inherit;font-size:0.85rem;min-width:160px;"></select>
              <input class="orch-input" id="msg-summary" placeholder="Message the selected agent..." onkeydown="if(event.key==='Enter'&&!event.shiftKey){event.preventDefault();sendAgentMessage();}">
              <button class="btnp" id="chat-send-btn" onclick="sendAgentMessage()">Send</button>
            </div>
          </section>
        </div>
      </section>

      <section class="orch-view" id="orch-view-agents">
        <div class="orch-page-grid">
          <section class="orch-panel">
            <div class="orch-panel-head">
              <div class="orch-panel-title">Agents</div>
              <div class="orch-count" id="orch-agent-path"></div>
            </div>
            <div class="agent-list" id="orch-agents"><div class="empty">Loading agents...</div></div>
          </section>
          <section class="orch-panel">
            <div class="orch-panel-head"><div class="orch-panel-title" id="agent-builder-title">Agent Builder</div></div>
            <div class="orch-form">
              <div class="orch-field"><label>Name</label><input class="orch-input" id="agent-name" placeholder="Reliability Scout"></div>
              <div class="orch-field"><label>ID</label><input class="orch-input" id="agent-id" placeholder="reliability-scout"></div>
              <div class="orch-field"><label>Role</label><input class="orch-input" id="agent-role" placeholder="observer"><div class="field-help" id="agent-role-meta">Role says who the agent is and what decisions it can make.</div></div>
              <div class="orch-field"><label>Risk</label><select class="orch-select" id="agent-risk"><option>low</option><option>medium</option><option>high</option></select></div>
              <label class="agent-enabled wide"><input id="agent-approval" type="checkbox"> Approval required</label>
              <div class="orch-field wide"><label>Purpose</label><textarea class="orch-textarea" id="agent-purpose"></textarea></div>
              <div class="orch-field wide"><label>Allowed Skills</label><textarea class="orch-textarea" id="agent-skills"></textarea></div>
              <div class="orch-field wide"><label>Rules</label><textarea class="orch-textarea" id="agent-rules"></textarea></div>
              <div class="orch-field wide"><label>Character</label><div class="char-grid" id="agent-character-grid"></div></div>
              <div class="orch-field wide"><label>Corporate Logo</label><div class="char-grid" id="agent-logo-grid"></div></div>
              <div class="orch-field wide">
                <label>Upload Logo</label>
                <div class="file-row">
                  <img class="logo-preview" id="agent-logo-preview" src="/assets/characters/black-hd-logo.png" alt="">
                  <input id="agent-logo" type="file" accept="image/*">
                  <button class="btns" type="button" onclick="clearAgentLogo()">Clear</button>
                </div>
              </div>
              <div class="approval-actions wide">
                <button class="btnp" id="agent-save-btn" onclick="createAgent()">Create Agent</button>
                <button class="btns" id="agent-cancel-btn" onclick="resetAgentForm()" style="display:none">Cancel</button>
              </div>
            </div>
          </section>
        </div>
      </section>

      <section class="orch-view" id="orch-view-skills">
        <div class="orch-page-grid">
          <section class="orch-panel">
            <div class="orch-panel-head">
              <div class="orch-panel-title">Skill Registry</div>
              <div class="orch-count" id="orch-skill-path"></div>
            </div>
            <div class="skill-list" id="orch-skills"><div class="empty">Loading skills...</div></div>
          </section>
          <section class="orch-panel">
            <div class="orch-panel-head"><div class="orch-panel-title">Skill Builder</div></div>
            <div class="orch-form">
              <div class="orch-field"><label>Agent</label><select class="orch-select" id="skill-agent"></select></div>
              <div class="orch-field"><label>Skill Name</label><input class="orch-input" id="skill-name" placeholder="refresh container from git"></div>
              <div class="orch-field"><label>Skill ID</label><input class="orch-input" id="skill-id" placeholder="refresh-container-from-git"></div>
              <div class="orch-field"><label>Category</label><input class="orch-input" id="skill-category" value="automation"></div>
              <div class="orch-field"><label>Risk</label><select class="orch-select" id="skill-risk"><option>low</option><option selected>medium</option><option>high</option></select></div>
              <label class="agent-enabled"><input id="skill-approval" type="checkbox" checked> Approval required</label>
              <div class="orch-field wide"><label>Purpose</label><textarea class="orch-textarea" id="skill-purpose"></textarea></div>
              <div class="orch-field wide"><label>Inputs</label><textarea class="orch-textarea" id="skill-inputs"></textarea></div>
              <div class="orch-field wide"><label>Outputs</label><textarea class="orch-textarea" id="skill-outputs"></textarea></div>
              <div class="orch-field wide"><label>Procedure</label><textarea class="orch-textarea" id="skill-procedure"></textarea></div>
              <div class="orch-field wide"><label>Rollback</label><textarea class="orch-textarea" id="skill-rollback"></textarea></div>
              <div class="orch-field wide"><label>Success Criteria</label><textarea class="orch-textarea" id="skill-success"></textarea></div>
              <div class="approval-actions wide">
                <button class="btnp" id="skill-save-btn" onclick="createSkill()">Create Skill</button>
                <button class="btns" id="skill-cancel-btn" onclick="resetSkillForm()" style="display:none">Cancel</button>
              </div>
            </div>
          </section>
        </div>
      </section>

      <section class="orch-view" id="orch-view-approvals">
        <div class="orch-page-grid">
          <section class="orch-panel">
            <div class="orch-panel-head"><div class="orch-panel-title">Approval Request</div></div>
            <div class="orch-form">
              <div class="orch-field"><label>Requester</label><select class="orch-select" id="approval-agent"></select></div>
              <div class="orch-field"><label>Action</label><select class="orch-select" id="approval-action"><option value="git_pull_container_refresh">git pull + refresh</option><option value="container_restart">container restart</option><option value="container_refresh">container refresh</option></select></div>
              <div class="orch-field wide"><label>Title</label><input class="orch-input" id="approval-title" placeholder="Refresh app from main"></div>
              <div class="orch-field wide"><label>Target</label><input class="orch-input" id="approval-target" placeholder="stack/container/repository"></div>
              <div class="orch-field wide"><label>Rationale</label><textarea class="orch-textarea" id="approval-rationale"></textarea></div>
              <button class="btnp wide" onclick="createApproval()">Route to Gate Keeper</button>
            </div>
          </section>
          <section class="orch-panel">
            <div class="orch-panel-head"><div class="orch-panel-title">Approval Inbox</div></div>
            <div class="approval-list" id="orch-approvals"><div class="empty">Loading approvals...</div></div>
          </section>
        </div>
      </section>

      <section class="orch-view" id="orch-view-learning">
        <div class="orch-page-grid">
          <section class="orch-panel">
            <div class="orch-panel-head">
              <div class="orch-panel-title">Sage Learning</div>
              <div class="orch-count" id="orch-knowledge-path"></div>
            </div>
            <div class="orch-form">
              <div class="orch-field"><label>Source</label><select class="orch-select" id="learning-agent"></select></div>
              <div class="orch-field"><label>Outcome</label><select class="orch-select" id="learning-outcome"><option>proposed</option><option>success</option><option>failed</option><option>false_positive</option></select></div>
              <div class="orch-field wide"><label>Title</label><input class="orch-input" id="learning-title" placeholder="Restart cleared stale worker lock"></div>
              <div class="orch-field wide"><label>Incident Ref</label><input class="orch-input" id="learning-ref" placeholder="incident or approval id"></div>
              <div class="orch-field wide"><label>Summary</label><textarea class="orch-textarea" id="learning-summary"></textarea></div>
              <button class="btnp wide" onclick="createLearning()">Record Learning</button>
            </div>
          </section>
          <section class="orch-panel">
            <div class="orch-panel-head"><div class="orch-panel-title">Learning Entries</div></div>
            <div class="learning-list" id="orch-learnings"><div class="empty">Loading learning entries...</div></div>
          </section>
        </div>
      </section>

      <section class="orch-view" id="orch-view-setup">
        <div class="orch-page-grid">
          <section class="orch-panel">
            <div class="orch-panel-head"><div class="orch-panel-title">Add User</div></div>
            <div class="orch-form">
              <div class="orch-field"><label>Username</label><input class="orch-input" id="setup-username" autocomplete="off"></div>
              <div class="orch-field"><label>Password</label><input class="orch-input" id="setup-password" type="password" autocomplete="new-password"></div>
              <div class="orch-field"><label>Type</label><select class="orch-select" id="setup-role"><option value="admin">Admin</option><option value="user" selected>User</option></select></div>
              <div class="orch-field"><label>Status</label><div class="setup-note" id="setup-status">Only admins can add users.</div></div>
              <button class="btnp wide" onclick="createUser()">Create User</button>
            </div>
          </section>
          <section class="orch-panel">
            <div class="orch-panel-head"><div class="orch-panel-title">Users</div></div>
            <div class="user-list" id="setup-users"><div class="empty">Loading users...</div></div>
          </section>
        </div>
      </section>
    </div>
  </div>

</div><!-- /main -->

<!-- RAVEN -->
<aside class="aside">
  <div class="aside-width-grip" id="aside-width-grip" title="Resize Raven and Oracle panel"></div>
  <div class="hb-wrap">
    <div class="hb-lbl">
      <span class="hb-title"><img class="raven-mark" src="/assets/kingdoms/raven.png" alt="">RAVEN</span>
      <span class="hb-st" id="hb-status"></span>
    </div>
    <div class="hb-canvas-wrap"><canvas id="hb-cv" height="48"></canvas></div>
    <div class="hb-tip" id="hb-tip"></div>
  </div>
  <div class="raven-sl" id="raven-sl">
    <span class="sl-icon" id="sl-icon">—</span>
    <span class="sl-txt" id="sl-txt">Waiting for activity…</span>
  </div>
  <div class="raven-next" id="raven-next" style="display:none">
    <span class="raven-next-lbl">Up Next</span>
    <span id="raven-next-txt"></span>
  </div>
  <div class="feed-hdr">
    <button class="ff on" id="rf-all" onclick="setRF('')">All</button>
    <button class="ff" id="rf-critical" onclick="setRF('critical')">Critical</button>
    <button class="ff" id="rf-error" onclick="setRF('error')">Errors</button>
    <button class="ff" id="rf-warning" onclick="setRF('warning')">Warnings</button>
  </div>
  <div class="feed" id="feed"><div class="ph">No issues found yet.</div></div>
  <div class="oracle-resizer" id="oracle-resizer" title="Drag to resize Raven and Oracle"></div>
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
    <div class="fg"><label>Server name</label><input id="f-server-name" type="text" placeholder="Friendly corporate name"></div>
    <div class="fg">
      <label>Logo</label>
      <div class="file-row">
        <img class="logo-preview empty" id="f-logo-preview" alt="">
        <input id="f-logo" type="file" accept="image/*">
        <button class="btns" type="button" onclick="clearConnLogo()">Clear</button>
      </div>
    </div>
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
    <div class="fg"><label>Friendly name</label><input id="char-friendly" type="text" placeholder="Display name"></div>
    <div class="fg">
      <label>Character</label>
      <div class="char-grid" id="char-grid"></div>
    </div>
    <div class="fg">
      <label>Corporate logo</label>
      <div class="char-grid" id="logo-grid"></div>
    </div>
    <div class="fg">
      <label>Upload logo</label>
      <div class="file-row">
        <img class="logo-preview empty" id="char-logo-preview" alt="">
        <input id="char-logo" type="file" accept="image/*">
        <button class="btns" type="button" onclick="clearStackLogo()">Clear</button>
      </div>
    </div>
  </div>
  <div class="mf">
    <button class="btns" onclick="closeCharDlg()">Cancel</button>
    <button class="btnp" onclick="saveStackSettings()">Save</button>
  </div>
</dialog>

</div><!-- /app-shell -->

<script>
/* ============================================================
   STATE
   ============================================================ */
let _evts=[], _homeRecent=[], _evFilters={severity:'',container:'',server:''};
let _conns=[], _editId=null, _charEditKey='', _charDraftCharacter='', _charLogoDraft='', _charDefaultLogo='';
let _stacks=[], _connLogoDraft='', _networkZoom=1;
let _networkPan={x:0,y:0,worldKey:'',centeredStageId:'',centeredVisible:false,dragging:false,startX:0,startY:0,originX:0,originY:0,suppressClick:false};
let _networkDrag={active:false,nodeId:'',startX:0,startY:0,originX:0,originY:0,moved:false};
let _networkChecking={server:'',container:''};
let _focusedNetworkChecking={};
let _viewMode='corporate';
let _orch={agents:[],skills:[],messages:[],approvals:[],learnings:[],paths:{}};
let _orchTab='chat', _adminTab='connections', _currentUser=null, _users=[], _ravenConnected=false, _loadAllTimer=null, _skillEditId='', _agentEditId='';
let _agentDraftIcon='/assets/characters/agent-scout.png', _agentLogoDraft='';
let _hbData=new Array(60).fill(0), _hbBucket=0, _hbAlerts=[], _hbAlertBuf=[], _hbAlertPoints=[];
let _ravenFilter='', _issuePills=[], _issueKeys=new Set();
let _oracleState={busy:false,summary:null,analysis:'',error:''};
let _windowHours=24;
const MAX_ISSUE_PILLS=20;

/* ============================================================
   CHARACTER ASSETS
   ============================================================ */
const CHARACTERS=[
  {id:'orc',label:'Orc',src:'/assets/characters/orc.png'},
  {id:'wizard',label:'Wizard',src:'/assets/characters/wizard.png'},
  {id:'executioner',label:'Executioner',src:'/assets/characters/agent-executioner.png'},
  {id:'sage',label:'Sage',src:'/assets/characters/agent-sage.png'},
  {id:'gate-keeper',label:'Gate Keeper',src:'/assets/characters/agent-gate-keeper.png'},
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
  {id:'scout',label:'Scout',src:'/assets/characters/agent-scout.png'},
  {id:'shepherd',label:'Shepherd',src:'/assets/characters/shepherd.png'},
  {id:'herder',label:'Herder',src:'/assets/characters/herder.png'},
  {id:'sorceress',label:'Sorceress',src:'/assets/characters/sorceress.png'}
];
const AGENT_CHARACTERS=[
  {id:'executioner',label:'Executioner',src:'/assets/characters/agent-executioner.png'},
  {id:'sage',label:'Sage',src:'/assets/characters/agent-sage.png'},
  {id:'gate-keeper',label:'Gate Keeper',src:'/assets/characters/agent-gate-keeper.png'},
  {id:'blacksmith',label:'Blacksmith',src:'/assets/characters/blacksmith.png'},
  {id:'scout',label:'Scout',src:'/assets/characters/agent-scout.png'},
  {id:'raven',label:'Raven',src:'/assets/kingdoms/raven.png'},
  {id:'oracle',label:'Oracle',src:'/assets/kingdoms/oracle.png'}
];
const BLACK_LOGO_SRC='/assets/characters/black-hd-logo.png';
const BLACK_LOGO={id:'corp-black-hd',label:'Black HD Logo',src:BLACK_LOGO_SRC};
const FAVICON_LOGO={id:'corp-favicon',label:'Favicon',src:'/assets/favicon.ico'};
const CORPORATE_LOGOS=[
  BLACK_LOGO,
  FAVICON_LOGO,
  {id:'corp-db',label:'Database',src:'/assets/characters/corporate-worker-0.png'},
  {id:'corp-worker',label:'Worker App',src:'/assets/characters/corporate-worker-1.png'},
  {id:'corp-redis',label:'Redis',src:'/assets/characters/corporate-worker-2.png'},
  {id:'corp-ui',label:'UI Panel',src:'/assets/characters/corporate-worker-3.png'}
];
const CHARACTER_BY_ID=Object.fromEntries(CHARACTERS.map(c=>[c.id,c]));
const CORPORATE_LOGO_BY_ID=Object.fromEntries(CORPORATE_LOGOS.map(c=>[c.id,c]));
const CORPORATE_LOGO_BY_SRC=Object.fromEntries(CORPORATE_LOGOS.map(c=>[c.src,c]));
const WORKER_ASSETS=[
  '/assets/characters/worker-medieval-0.png',
  '/assets/characters/worker-medieval-1.png',
  '/assets/characters/worker-medieval-2.png',
  '/assets/characters/worker-medieval-3.png'
];
const CHARACTER_STORAGE_PREFIX='orc.map.character.';
const STACK_STORAGE_PREFIX='orc.stack.';
const CONTAINER_NAME_PREFIX='orc.container.name.';
const RAVEN_FEED_HEIGHT_KEY='orc.raven.feed.height';
const ASIDE_WIDTH_KEY='orc.aside.width';
const VIEW_MODE_KEY='orc.view.mode';
const MT_ZONE='America/Denver';
const DATE_FMT=new Intl.DateTimeFormat('en-US',{timeZone:MT_ZONE,year:'numeric',month:'short',day:'2-digit',hour:'numeric',minute:'2-digit',second:'2-digit',timeZoneName:'short'});
const TIME_FMT=new Intl.DateTimeFormat('en-US',{timeZone:MT_ZONE,hour:'numeric',minute:'2-digit',second:'2-digit',timeZoneName:'short'});

/* ============================================================
   UTILS
   ============================================================ */
function setAuthView(user){
  _currentUser=user||null;
  const loggedIn=!!_currentUser;
  document.getElementById('login-screen').classList.toggle('hidden',loggedIn);
  document.getElementById('app-shell').classList.toggle('hidden',!loggedIn);
  if(loggedIn){
    document.getElementById('user-name').textContent=_currentUser.username;
    document.getElementById('user-role').textContent=_currentUser.role;
    const isAdmin=_currentUser.role==='admin';
    document.getElementById('orch-tab-setup').style.display=isAdmin?'':'none';
    document.getElementById('tab-admin').style.display=isAdmin?'':'none';
  }
}
async function checkAuth(){
  try{
    const d=await fetch('/auth/me').then(r=>r.json());
    if(d.authenticated){setAuthView(d.user);return true;}
  }catch{}
  setAuthView(null);
  return false;
}
async function login(){
  const username=document.getElementById('login-username').value.trim();
  const password=document.getElementById('login-password').value;
  const err=document.getElementById('login-error');
  err.textContent='';
  try{
    const r=await fetch('/auth/login',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({username,password})});
    const d=await r.json();
    if(!r.ok)throw new Error(d.detail||'Login failed');
    setAuthView(d.user);
    await startApp();
  }catch(e){err.textContent=e.message||'Login failed';}
}
async function logout(){
  await fetch('/auth/logout',{method:'POST'}).catch(()=>{});
  location.reload();
}
async function startApp(){
  showTab('home');
  renderOracle();
  setViewMode(storageGet(VIEW_MODE_KEY)||'corporate',false);
  setWindowHours(storageGet('orc.window.hours')||24,false);
  await loadAll();
  if(!_loadAllTimer)_loadAllTimer=setInterval(loadAll,30000);
  if(!_ravenConnected){_ravenConnected=true;connectRaven();}
}
function showOrchTab(id){
  if(id==='setup'&&_currentUser?.role!=='admin')id='chat';
  _orchTab=id||'chat';
  document.querySelectorAll('.orch-tab').forEach(t=>t.classList.remove('on'));
  document.querySelectorAll('.orch-view').forEach(v=>v.classList.remove('on'));
  const tab=document.getElementById('orch-tab-'+_orchTab);
  const view=document.getElementById('orch-view-'+_orchTab);
  if(tab)tab.classList.add('on');
  if(view)view.classList.add('on');
  if(_orchTab==='setup')loadUsers();
}
function showAdminTab(id){
  if(_currentUser?.role!=='admin'){showTab('overview');return;}
  _adminTab=id||'connections';
  document.querySelectorAll('#pane-admin .orch-tab').forEach(t=>t.classList.remove('on'));
  document.querySelectorAll('#pane-admin .orch-view').forEach(v=>v.classList.remove('on'));
  const tab=document.getElementById('admin-tab-'+_adminTab);
  const view=document.getElementById('admin-view-'+_adminTab);
  if(tab)tab.classList.add('on');
  if(view)view.classList.add('on');
  if(_adminTab==='connections')loadConns();
  if(_adminTab==='ai-usage')loadAiUsage();
  if(_adminTab==='retention')loadRetentionSettings();
}
function showTab(id){
  if(id==='admin'&&_currentUser?.role!=='admin')id=firstTabForMode();
  if(!tabAllowed(id))id=firstTabForMode();
  document.querySelectorAll('.pane').forEach(p=>p.classList.remove('on'));
  document.querySelectorAll('.tab').forEach(t=>t.classList.remove('on'));
  document.getElementById('pane-'+id)?.classList.add('on');
  const tab=document.getElementById('tab-'+id);
  if(tab)tab.classList.add('on');
  if(id==='home')renderHomeDashboard();
  if(id==='overview')loadOverview();
  if(id==='network')loadNetwork();
  if(id==='events')loadEvts();
  if(id==='orchestration'){showOrchTab(_orchTab||'chat');loadOrchestration();}
  if(id==='admin'){showAdminTab(_adminTab||'connections');}
}
function tabsForViewMode(mode=_viewMode){
  return ['overview','network','events','orchestration','admin'];
}
function tabAllowed(id){return id==='home'||tabsForViewMode().includes(id);}
function firstTabForMode(){return tabsForViewMode()[0];}
function normalizeViewMode(mode){
  if(mode==='default')return 'corporate';
  return mode==='character'?'character':'corporate';
}
function setViewMode(mode,persist=true){
  _viewMode=normalizeViewMode(mode);
  if(persist)storageSet(VIEW_MODE_KEY,_viewMode);
  document.documentElement.setAttribute('data-view-mode',_viewMode);
  const btn=document.getElementById('view-mode-toggle');
  if(btn)btn.textContent=_viewMode==='corporate'?'Corporate View':'Character View';
  const active=document.querySelector('.pane.on')?.id.replace('pane-','')||'';
  if(!tabAllowed(active))showTab(firstTabForMode());
  renderVisualViews();
  renderOrchestration();
}
function toggleViewMode(){
  setViewMode(_viewMode==='corporate'?'character':'corporate');
}
function fmt(iso){return iso?DATE_FMT.format(new Date(iso)):'';}
function fmtShort(iso){return iso?TIME_FMT.format(new Date(iso)):'';}
function esc(s){return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');}
function hashStr(s){let h=0;for(let i=0;i<s.length;i++)h=((h<<5)-h+s.charCodeAt(i))|0;return Math.abs(h);}
function storageGet(k){try{return localStorage.getItem(k);}catch{return null;}}
function storageSet(k,v){try{localStorage.setItem(k,v);}catch{}}
function storageDel(k){try{localStorage.removeItem(k);}catch{}}
function stackCharacterKey(stack){return `${stack.server}::${stack.name}`;}
function stackSettingKey(key,name){return `${STACK_STORAGE_PREFIX}${key}.${name}`;}
function stackSetting(key,name){return storageGet(stackSettingKey(key,name))||'';}
function setStackSetting(key,name,val){val?storageSet(stackSettingKey(key,name),val):storageDel(stackSettingKey(key,name));}
function defaultCharacterId(stack){
  if(stack&&CHARACTER_BY_ID[stack.character])return stack.character;
  return CHARACTERS[hashStr(stackCharacterKey(stack))%CHARACTERS.length].id;
}
function legacyCorporateLogoForKey(key){
  const saved=stackSetting(key,'Character')||storageGet(CHARACTER_STORAGE_PREFIX+key);
  return CORPORATE_LOGO_BY_ID[saved]?.src||'';
}
function selectedCharacterId(stack){
  const key=stackCharacterKey(stack);
  const saved=stackSetting(key,'Character')||storageGet(CHARACTER_STORAGE_PREFIX+key);
  return CHARACTER_BY_ID[saved]?saved:defaultCharacterId(stack);
}
function defaultCorporateLogo(stack){
  return BLACK_LOGO;
}
function stackFriendlyName(stack){
  return stackSetting(stackCharacterKey(stack),'FriendlyName')||stack.name;
}
function stackLogo(stack){
  const key=stackCharacterKey(stack);
  return stackSetting(key,'Logo')||legacyCorporateLogoForKey(key);
}
function selectedStackLogo(stack){
  return stackLogo(stack)||BLACK_LOGO_SRC;
}
function containerDisplayName(name){
  return storageGet(CONTAINER_NAME_PREFIX+name)||name;
}
function containerFriendlyName(app){
  return containerDisplayName(app.full_name||app.name||'');
}
function serverDisplayName(k){
  return k.server_name||k.server||'Unknown server';
}
function connectionDisplayName(name){
  const c=_conns.find(conn=>conn.name===name);
  return c?(c.server_name||c.name):name;
}
function ravenServerKey(msg){
  return msg.server_key||msg.connection_name||msg.server||'';
}
function ravenServerDisplay(msg){
  const key=ravenServerKey(msg);
  return msg.server||connectionDisplayName(key)||'Unknown server';
}
function evServerDisplay(serverKey){
  const conn=_conns.find(c=>c.name===serverKey);
  return conn?.server_name||serverKey||'Unknown server';
}
function serverLogo(k){
  return k.server_logo||BLACK_LOGO_SRC;
}
function agentArt(agent){
  return _viewMode==='corporate'
    ? (agent.logo_data||BLACK_LOGO_SRC)
    : (agent.icon||'/assets/characters/agent-scout.png');
}
function renderAgentBuilderChoices(){
  const charGrid=document.getElementById('agent-character-grid');
  if(charGrid){
    charGrid.innerHTML=AGENT_CHARACTERS.map(c=>`
      <button class="char-choice ${c.src===_agentDraftIcon?'on':''}" data-agent-icon="${esc(c.src)}" type="button" onclick="chooseAgentCharacter('${esc(c.src)}')">
        <img src="${esc(c.src)}" alt="${esc(c.label)}"><span>${esc(c.label)}</span>
      </button>`).join('');
  }
  const logoGrid=document.getElementById('agent-logo-grid');
  if(logoGrid){
    logoGrid.innerHTML=CORPORATE_LOGOS.map(c=>{
      const active=(_agentLogoDraft||BLACK_LOGO_SRC)===c.src;
      return `<button class="char-choice agent-logo-choice ${active?'on':''}" data-agent-logo="${esc(c.src)}" type="button" onclick="chooseAgentLogo('${esc(c.src)}')">



















































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































































        <img src="${esc(c.src)}" alt="${esc(c.label)}"><span>${esc
