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
    IncidentRecord,
    LearningEntry,
    ObservedEvent,
    SessionLocal,
    SystemSetting,
    RunbookExecution,
    ToolPromotion,
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


class SkillMarkdownIn(BaseModel):
    skill_id: str = ""
    markdown: str


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


class IncidentCreateIn(BaseModel):
    title: str
    symptom: str
    context: str = ""
    root_cause: str = ""
    action: str = ""
    outcome: str = "open"
    confidence: str = "medium"
    governance: str = "yellow"
    autonomy_level: int = 1


class RunbookExecuteIn(BaseModel):
    target: str = ""
    rationale: str = ""
    requested_by: str = "operator"
    approval_id: int | None = None


class ToolPromotionIn(BaseModel):
    tool_id: str
    title: str
    source_path: str = ""
    test_summary: str
    dry_run_summary: str
    artifact_markdown: str = ""
    requested_by: str = "operator"
    approval_id: int | None = None


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

CAPABILITY_AGENT_REMAP = {
    "api-harvester": "orc-orchestrator",
    "documenter": "sage",
    "outlook-comms": "orc-orchestrator",
    "portainer-log-collector": "raven",
}


def _slug(value: str, fallback: str = "item") -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return slug or fallback


def _bool_text(value: bool) -> str:
    return "true" if value else "false"


def _bullet_block(value: str) -> str:
    lines = [line.strip(" -\t") for line in value.splitlines() if line.strip()]
    return "\n".join(f"- {line}" for line in lines) if lines else "- Not specified"


def _skill_template_comment() -> str:
    return "\n".join(
        [
            "<!--",
            "Author this file like a developer-maintained runbook for an AI teammate.",
            "",
            "Before replacing this guidance",
            "- Keep instructions concise, imperative, and specific to this skill.",
            "- Put stable procedural rules here, not general AI advice.",
            "- Include exact inputs, outputs, approval boundaries, and validation checks.",
            "- Move long reference material into a linked reference file when the skill grows.",
            "- Prefer scripts for fragile or repeated operations that should be deterministic.",
            "- Ask Codex to edit this Markdown directly when the procedure becomes clearer.",
            "",
            "Delete this comment when the skill is ready.",
            "-->",
        ]
    )


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
            "autonomy_level: 1",
            "governance: yellow",
            "allowed_plane: reasoning",
            f"approval_required: {_bool_text(body.approval_required)}",
            f"agent: {body.agent_id.strip()}",
            "",
            _skill_template_comment(),
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
            "## Approval Boundary",
            "",
            "- State which actions are read-only and which require approval.",
            "- Route risky, destructive, or external-system changes to the assigned approval path.",
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


def _skill_metadata_from_markdown(raw: str) -> dict[str, str]:
    metadata: dict[str, str] = {}
    for line in raw.splitlines():
        stripped = line.strip()
        if stripped.startswith("## "):
            break
        if not stripped or stripped.startswith("#") or stripped.startswith("<!--") or stripped.startswith("-->"):
            continue
        if ":" not in stripped:
            continue
        key, value = stripped.split(":", 1)
        metadata[key.strip().lower()] = value.strip()
    return metadata


def _markdown_metadata(raw: str) -> dict[str, str]:
    metadata: dict[str, str] = {}
    for line in raw.splitlines():
        stripped = line.strip()
        if stripped.startswith("## "):
            break
        if not stripped or stripped.startswith("#") or stripped.startswith("<!--") or stripped.startswith("-->"):
            continue
        if ":" not in stripped:
            continue
        key, value = stripped.split(":", 1)
        metadata[key.strip().lower()] = value.strip()
    return metadata


def _markdown_sections(raw: str) -> dict[str, str]:
    sections: dict[str, list[str]] = {}
    current = ""
    for line in raw.splitlines():
        if line.startswith("## "):
            current = line[3:].strip().lower()
            sections[current] = []
            continue
        if current:
            sections[current].append(line)
    return {key: "\n".join(value).strip() for key, value in sections.items()}


def _registry_file_for_id(kind: str, item_id: str) -> Path:
    file_name = {"tools": "tool.md", "runbooks": "runbook.md", "skills": "skills.md", "agents": "agent.md"}.get(kind)
    if not file_name:
        raise HTTPException(400, "Unknown registry kind")
    root = REPO_ROOT / kind
    requested = item_id.strip()
    slugged = _slug(requested, kind[:-1] or "item")
    for item in load_registry(REPO_ROOT, kind):
        path = Path(item.path)
        if requested in {item.item_id, item.name, path.parent.name} or slugged in {
            _slug(item.item_id, "item"),
            _slug(item.name, "item"),
            _slug(path.parent.name, "item"),
        }:
            target = _safe_markdown_path(REPO_ROOT, item.path)
            if target.exists():
                return target
    target = _safe_markdown_path(root, slugged) / file_name
    if target.exists():
        return target
    raise HTTPException(404, f"{kind[:-1].title()} not found")


def _read_registry_definition(kind: str, item_id: str) -> dict:
    path = _registry_file_for_id(kind, item_id)
    raw = path.read_text(encoding="utf-8")
    metadata = _markdown_metadata(raw)
    sections = _markdown_sections(raw)
    return {
        "id": metadata.get("id", path.parent.name),
        "name": metadata.get("name", path.parent.name),
        "version": metadata.get("version", "0.0.0"),
        "category": metadata.get("category", metadata.get("plane", "unknown")),
        "risk_level": metadata.get("risk_level", "medium"),
        "autonomy_level": int(metadata.get("autonomy_level", "1") or "1"),
        "governance": metadata.get("governance", metadata.get("governance_boundary", "yellow")),
        "approval_required": _markdown_bool(metadata.get("approval_required"), True),
        "metadata": metadata,
        "sections": sections,
        "raw_markdown": raw,
        "path": str(path.relative_to(REPO_ROOT)),
    }


def _write_markdown_under(root_name: str, *parts: str, content: str) -> Path:
    root = REPO_ROOT / root_name
    target_dir = _safe_markdown_path(root, *parts[:-1]) if len(parts) > 1 else root
    target_dir.mkdir(parents=True, exist_ok=True)
    target_file = _safe_markdown_path(root, *parts)
    target_file.write_text(content.rstrip() + "\n", encoding="utf-8")
    return target_file


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
        if item.item_id in CAPABILITY_AGENT_REMAP:
            continue
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


def _markdown_list_items(section: str) -> list[str]:
    items: list[str] = []
    for raw_line in (section or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith(("-", "*")):
            line = line[1:].strip()
        elif re.match(r"^\d+\.\s+", line):
            line = re.sub(r"^\d+\.\s+", "", line).strip()
        items.append(line)
    return items


def _empty_agent_profile() -> dict:
    return {
        "plane": "",
        "risk_level": "",
        "autonomy_level": "",
        "governance_boundary": "",
        "approval_required": None,
        "purpose": "",
        "inputs": [],
        "outputs": [],
        "allowed_skills": [],
        "rules": [],
        "definition_path": "",
        "remapped_capabilities": [],
    }


def _agent_profile_from_definition(agent_id: str) -> dict:
    profile = _empty_agent_profile()
    try:
        path = _registry_file_for_id("agents", agent_id)
        raw = path.read_text(encoding="utf-8")
        metadata = _markdown_metadata(raw)
        sections = _markdown_sections(raw)
        profile.update(
            {
                "plane": metadata.get("plane", ""),
                "risk_level": metadata.get("risk_level", ""),
                "autonomy_level": metadata.get("autonomy_level", ""),
                "governance_boundary": metadata.get("governance_boundary", metadata.get("governance", "")),
                "approval_required": _markdown_bool(metadata.get("approval_required"), False),
                "purpose": re.sub(r"\s+", " ", sections.get("purpose", "")).strip(),
                "inputs": _markdown_list_items(sections.get("inputs", "")),
                "outputs": _markdown_list_items(sections.get("outputs", "")),
                "allowed_skills": _markdown_list_items(sections.get("allowed skills", "")),
                "rules": _markdown_list_items(sections.get("rules", "")),
                "definition_path": str(path.relative_to(REPO_ROOT)),
            }
        )
    except Exception:
        pass
    return profile


def _merge_unique_list(target: list[str], incoming: list[str]) -> list[str]:
    seen = {item.lower(): item for item in target}
    merged = list(target)
    for item in incoming:
        key = item.lower()
        if key not in seen:
            seen[key] = item
            merged.append(item)
    return merged


def _agent_profile_metadata(agent_id: str) -> dict:
    profile = _agent_profile_from_definition(agent_id)
    remapped: list[dict] = []
    for source_id, target_id in CAPABILITY_AGENT_REMAP.items():
        if target_id != agent_id:
            continue
        source = _agent_profile_from_definition(source_id)
        source_name = source_id.replace("-", " ").title()
        try:
            source_def = _read_registry_definition("agents", source_id)
            source_name = source_def.get("name") or source_name
        except Exception:
            pass
        profile["allowed_skills"] = _merge_unique_list(profile["allowed_skills"], source["allowed_skills"])
        profile["inputs"] = _merge_unique_list(profile["inputs"], source["inputs"])
        profile["outputs"] = _merge_unique_list(profile["outputs"], source["outputs"])
        profile["rules"] = _merge_unique_list(profile["rules"], source["rules"])
        remapped.append(
            {
                "id": source_id,
                "name": source_name,
                "purpose": source["purpose"],
                "definition_path": source["definition_path"],
                "allowed_skills": source["allowed_skills"],
            }
        )
    profile["remapped_capabilities"] = remapped
    return profile


def _agent_dict(row: AgentRuntimeState) -> dict:
    return {
        "id": row.agent_id,
        "name": row.name,
        "role": row.role,
        "icon": row.icon or _default_agent_icon(row.agent_id, row.role),
        "logo_data": row.logo_data or DEFAULT_CORPORATE_LOGO,
        "trust_mode": row.trust_mode,
        "enabled": row.enabled,
        "profile": _agent_profile_metadata(row.agent_id),
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


def _incident_dict(row: IncidentRecord) -> dict:
    return {
        "id": row.id,
        "title": row.title,
        "symptom": row.symptom,
        "context": row.context,
        "root_cause": row.root_cause,
        "action": row.action,
        "outcome": row.outcome,
        "confidence": row.confidence,
        "governance": row.governance,
        "autonomy_level": row.autonomy_level,
        "markdown_path": row.markdown_path,
        "status": row.status,
        "created_at": row.created_at.isoformat(),
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
    }


def _runbook_execution_dict(row: RunbookExecution) -> dict:
    return {
        "id": row.id,
        "runbook_id": row.runbook_id,
        "title": row.title,
        "requested_by": row.requested_by,
        "approval_id": row.approval_id,
        "governance": row.governance,
        "autonomy_level": row.autonomy_level,
        "status": row.status,
        "target": row.target,
        "rationale": row.rationale,
        "verification": row.verification,
        "rollback": row.rollback,
        "result": row.result,
        "evidence_path": row.evidence_path,
        "created_at": row.created_at.isoformat(),
        "completed_at": row.completed_at.isoformat() if row.completed_at else None,
    }


def _tool_promotion_dict(row: ToolPromotion) -> dict:
    return {
        "id": row.id,
        "tool_id": row.tool_id,
        "title": row.title,
        "source_path": row.source_path,
        "artifact_path": row.artifact_path,
        "requested_by": row.requested_by,
        "approval_id": row.approval_id,
        "status": row.status,
        "test_summary": row.test_summary,
        "dry_run_summary": row.dry_run_summary,
        "created_at": row.created_at.isoformat(),
        "decided_at": row.decided_at.isoformat() if row.decided_at else None,
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
<script src="https://unpkg.com/vis-network@10.0.2/standalone/umd/vis-network.min.js"></script>
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
#pane-home,#pane-instructions{min-height:calc(100vh - 92px);padding:12px;background:#0f141b}
.instructions-page{display:flex;flex-direction:column;gap:12px;max-width:1180px}
.instruction-hero{border:1px solid var(--bdr);border-radius:8px;background:#111820;padding:18px;display:grid;grid-template-columns:minmax(0,1fr) auto;gap:16px;align-items:end}
.instruction-kicker{font-size:.72rem;color:var(--mut);font-weight:850;letter-spacing:.08em;text-transform:uppercase}
.instruction-title{font-size:1.35rem;font-weight:900;margin-top:5px}
.instruction-copy{font-size:.86rem;color:#c9d5e2;line-height:1.5;max-width:780px;margin-top:8px}
.instruction-pillbox{display:flex;gap:6px;flex-wrap:wrap;justify-content:flex-end}
.instruction-subtabs{display:flex;gap:6px;align-items:center;border:1px solid var(--bdr);border-radius:8px;background:#111820;padding:6px}
.instruction-subtab{border:1px solid transparent;background:transparent;color:var(--mut);border-radius:7px;padding:7px 12px;font-size:.78rem;font-weight:850;cursor:pointer}
.instruction-subtab:hover{color:var(--txt);background:#17202b}.instruction-subtab.on{color:var(--txt);background:#243044;border-color:#334154}
.instruction-view{display:none;flex-direction:column;gap:12px}.instruction-view.on{display:flex}
.instruction-section{border:1px solid var(--bdr);border-radius:8px;background:var(--sur);padding:14px}
.instruction-section h2{font-size:.82rem;letter-spacing:.05em;text-transform:uppercase;margin:0 0 10px}
.instruction-section p{font-size:.82rem;color:#c9d5e2;line-height:1.48;margin:0 0 8px}
.plane-grid,.layer-grid,.governance-grid,.memory-grid{display:grid;gap:8px}
.plane-grid{grid-template-columns:repeat(3,minmax(0,1fr))}
.layer-grid{grid-template-columns:repeat(3,minmax(0,1fr))}
.governance-grid,.memory-grid{grid-template-columns:repeat(4,minmax(0,1fr))}
.instruction-tile{border:1px solid #21262d;border-radius:8px;background:#0d1117;padding:10px;min-width:0}
.instruction-tile strong{display:block;font-size:.8rem;margin-bottom:4px}
.instruction-tile span,.instruction-tile li{font-size:.74rem;color:var(--mut);line-height:1.38}
.instruction-tile ul{margin:7px 0 0 16px;padding:0}
.learning-loop{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:8px;counter-reset:loop}
.learning-step{border:1px solid #21262d;border-radius:8px;background:#0d1117;padding:10px;min-height:92px}
.learning-step::before{counter-increment:loop;content:counter(loop);display:inline-grid;place-items:center;width:22px;height:22px;border-radius:50%;background:#253041;color:#e6edf3;font-size:.72rem;font-weight:900;margin-bottom:8px}
.learning-step strong{display:block;font-size:.78rem;margin-bottom:4px}
.learning-step span{font-size:.72rem;color:var(--mut);line-height:1.35}
.profile-map-shell{border:1px solid var(--bdr);border-radius:8px;background:#101720;overflow:hidden}
.profile-map-top{display:flex;align-items:flex-start;justify-content:space-between;gap:12px;padding:12px 14px;border-bottom:1px solid #21262d}
.profile-map-title{font-size:.86rem;font-weight:900}
.profile-map-copy{font-size:.74rem;color:var(--mut);line-height:1.38;margin-top:3px;max-width:760px}
.profile-map-tools{display:flex;flex-direction:column;align-items:flex-end;gap:8px}
.profile-legend{display:flex;gap:6px;flex-wrap:wrap;justify-content:flex-end}
.profile-legend span{border:1px solid #30363d;border-radius:999px;padding:3px 8px;font-size:.66rem;color:#c9d5e2;background:#0d1117}
.profile-filter{display:flex;align-items:center;gap:7px;font-size:.72rem;color:var(--mut);white-space:nowrap}
.profile-filter select{background:#0d1117;border:1px solid var(--bdr);border-radius:7px;color:var(--txt);font-size:.76rem;padding:5px 9px;min-width:180px;outline:none}
.profile-filter select:focus{border-color:var(--pur)}
.profile-map-canvas{height:480px;min-height:370px;overflow:hidden;background:#0d1117;position:relative}
.profile-map-canvas canvas{outline:none}
.profile-map-canvas .vis-network{background:#0d1117}
.vis-tooltip{position:absolute;background:#111820!important;border:1px solid #30363d!important;border-radius:7px!important;color:#e6edf3!important;font-size:12px!important;line-height:1.35!important;padding:8px 10px!important;max-width:300px!important;white-space:normal!important;box-shadow:0 16px 30px rgba(0,0,0,.35)!important}
.vis-tooltip .profile-tooltip-body{margin-top:4px;color:#c9d5e2}
.profile-card-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px;margin-top:10px}
.profile-card{border:1px solid #21262d;border-radius:8px;background:#0d1117;padding:10px;min-width:0}
.profile-card-head{display:flex;align-items:center;gap:8px;margin-bottom:6px;min-width:0}.profile-card-head img{width:32px;height:32px;border-radius:50%;object-fit:cover;border:1px solid #30363d}
.profile-card-name{font-weight:900;font-size:.8rem;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.profile-card-meta{font-size:.68rem;color:var(--mut);margin-top:1px}
.profile-card-purpose{font-size:.72rem;color:#c9d5e2;line-height:1.36;margin:7px 0}
.profile-chip-row{display:flex;gap:5px;flex-wrap:wrap}.profile-skill-chip{border:1px solid #30363d;border-radius:999px;background:#111820;color:#c9d5e2;font-size:.64rem;padding:2px 7px}
.profile-empty{padding:18px;color:var(--mut);font-size:.8rem}
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
.chat-meta{font-size:.69rem;color:var(--mut);margin-bottom:4px;display:flex;gap:5px;flex-wrap:wrap}
.chat-channel{color:#f2f7ff;font-weight:850}
.chat-arrow{color:#9fb3c8;font-weight:850}
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
.skill-row.selected{border-color:rgba(163,113,247,.62);background:#111827}
.skill-file-panel{min-height:620px;display:flex;flex-direction:column}
.skill-editor-head{align-items:flex-start}
.skill-editor-actions{display:flex;align-items:center;gap:6px;flex-wrap:wrap;justify-content:flex-end}
.skill-file-meta{display:flex;gap:6px;flex-wrap:wrap;min-height:22px;margin-bottom:8px}
.skill-file-meta .status-chip{text-transform:none;font-size:.66rem;letter-spacing:0}
.skill-md-editor{flex:1;min-height:520px;font-family:Consolas,"Cascadia Mono","SFMono-Regular",monospace;font-size:.78rem;line-height:1.5;white-space:pre;overflow:auto;tab-size:2}
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
  #pane-home,#pane-instructions,#pane-overview,#pane-network,#pane-orchestration{padding:10px}
  .instruction-hero{grid-template-columns:1fr}
  .instruction-pillbox{justify-content:flex-start}
  .plane-grid,.layer-grid,.governance-grid,.memory-grid,.learning-loop{grid-template-columns:1fr}
  .profile-map-top{flex-direction:column}.profile-map-tools{align-items:flex-start}.profile-legend{justify-content:flex-start}.profile-card-grid{grid-template-columns:1fr}.profile-map-canvas{height:420px;min-height:360px}
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
    <button class="tab" id="tab-instructions" onclick="showTab('instructions')">Instructions</button>
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

  <!-- INSTRUCTIONS -->
  <div class="pane" id="pane-instructions">
    <div class="instructions-page">
      <section class="instruction-hero">
        <div>
          <div class="instruction-kicker">Operator Training</div>
          <div class="instruction-title">ORC Agent Framework</div>
          <div class="instruction-copy">Use this map to understand how ORC separates durable agent judgment from disposable execution. Agents decide, remember, approve, and verify. Workers run bounded jobs. The builder sandbox creates candidate tools that need human approval before promotion.</div>
        </div>
        <div class="instruction-pillbox">
          <span class="status-chip approved">Stable Core</span>
          <span class="status-chip pending">Worker Pool</span>
          <span class="status-chip rejected">Builder Sandbox</span>
        </div>
      </section>

      <div class="instruction-subtabs">
        <button class="instruction-subtab on" id="instruction-tab-framework" type="button" onclick="showInstructionSubpage('framework')">Framework</button>
        <button class="instruction-subtab" id="instruction-tab-profiles" type="button" onclick="showInstructionSubpage('profiles')">Profiles</button>
      </div>

      <div class="instruction-view on" id="instruction-view-framework">
      <section class="instruction-section">
        <h2>Chat-First Operation</h2>
        <div class="governance-grid">
          <div class="instruction-tile"><strong>Start With ORC</strong><span>Use Agent Chat as the command surface. ORC routes the request across Raven, Oracle, Sage, Gatekeeper, and Executioner.</span></div>
          <div class="instruction-tile"><strong>Green Commands Run</strong><span>Ask for log review or memory retrieval and ORC will run the bounded workflow immediately.</span></div>
          <div class="instruction-tile"><strong>Red Commands Wait</strong><span>Ask for redeploy, restart, or tool promotion and ORC prepares a Gatekeeper approval instead of mutating anything silently.</span></div>
          <div class="instruction-tile"><strong>Watch The Thread</strong><span>Approvals, runbook evidence, and memory results are attached to chat messages so the conversation stays the source of truth.</span></div>
        </div>
      </section>

      <section class="instruction-section">
        <h2>What Is Wired Now</h2>
        <div class="governance-grid">
          <div class="instruction-tile"><strong>Agent Chat Autonomy</strong><span>Natural chat commands can search memory, execute green runbooks, and prepare red approval requests.</span></div>
          <div class="instruction-tile"><strong>Sage Retrieval</strong><span>Searches Markdown memory, knowledge, runbooks, tools, and framework docs for similar patterns.</span></div>
          <div class="instruction-tile"><strong>Incident Memory</strong><span>Creates episodic Markdown records with symptom, context, root cause, action, outcome, and confidence.</span></div>
          <div class="instruction-tile"><strong>Runbook Execution</strong><span>Green runbooks run immediately and write evidence. Red runbooks create approval-gated Executioner handoffs.</span></div>
          <div class="instruction-tile"><strong>Tool Promotion</strong><span>Builder artifacts become worker tools only after Gatekeeper plus human approval.</span></div>
        </div>
      </section>

      <section class="instruction-section">
        <h2>Six Planes</h2>
        <div class="plane-grid">
          <div class="instruction-tile"><strong>Control: ORC</strong><span>Routes work, owns state, assembles plans, and keeps the workflow moving in order.</span></div>
          <div class="instruction-tile"><strong>Observation: Raven</strong><span>Collects logs, events, health signals, messages, and before/after verification.</span></div>
          <div class="instruction-tile"><strong>Reasoning: Oracle</strong><span>Diagnoses evidence, ranks hypotheses, and recommends next steps.</span></div>
          <div class="instruction-tile"><strong>Memory: Sage</strong><span>Retrieves history, writes learnings, and proposes reusable patterns.</span></div>
          <div class="instruction-tile"><strong>Policy: Gatekeeper</strong><span>Checks autonomy, governance color, approval rules, and escalation boundaries.</span></div>
          <div class="instruction-tile"><strong>Action: Executioner</strong><span>Runs approved actions only, records results, and stops on policy mismatch.</span></div>
        </div>
      </section>

      <section class="instruction-section">
        <h2>Three Layers</h2>
        <div class="layer-grid">
          <div class="instruction-tile">
            <strong>1. Stable Core</strong>
            <span>Long-lived application layer.</span>
            <ul><li>ORC and core agents</li><li>message bus and registries</li><li>approval matrix and memory</li></ul>
          </div>
          <div class="instruction-tile">
            <strong>2. Worker Pool</strong>
            <span>Disposable Docker workers.</span>
            <ul><li>run scripts</li><li>analyze logs</li><li>execute approved runbooks</li></ul>
          </div>
          <div class="instruction-tile">
            <strong>3. Builder Sandbox</strong>
            <span>Isolated development workspace.</span>
            <ul><li>write code</li><li>test, lint, dry-run</li><li>submit promotion requests</li></ul>
          </div>
        </div>
      </section>

      <section class="instruction-section">
        <h2>Autonomy And Governance</h2>
        <div class="governance-grid">
          <div class="instruction-tile"><strong>Level 0</strong><span>Observe only. Read, collect, classify, and report.</span></div>
          <div class="instruction-tile"><strong>Level 1</strong><span>Recommend. Diagnose and propose plans.</span></div>
          <div class="instruction-tile"><strong>Level 2</strong><span>Execute approved runbooks after policy allows it.</span></div>
          <div class="instruction-tile"><strong>Level 3</strong><span>Build and test new tools in sandbox. Promotion still needs human approval.</span></div>
        </div>
        <div class="governance-grid" style="margin-top:8px">
          <div class="instruction-tile"><strong>Green</strong><span>Read-only retrieval, classification, summaries, and reports.</span></div>
          <div class="instruction-tile"><strong>Yellow</strong><span>Policy-checked work following registered skills or runbooks.</span></div>
          <div class="instruction-tile"><strong>Red</strong><span>Human-approved redeploys, destructive changes, credentials, and promotion.</span></div>
          <div class="instruction-tile"><strong>Rule</strong><span>Generated tools never promote themselves. Gatekeeper and a human decide.</span></div>
        </div>
      </section>

      <section class="instruction-section">
        <h2>Learning Loop</h2>
        <div class="learning-loop">
          <div class="learning-step"><strong>Observe</strong><span>Raven gathers evidence.</span></div>
          <div class="learning-step"><strong>Diagnose</strong><span>Oracle analyzes the current case.</span></div>
          <div class="learning-step"><strong>Compare</strong><span>Sage retrieves similar incidents and patterns.</span></div>
          <div class="learning-step"><strong>Decide</strong><span>ORC builds a recommended plan.</span></div>
          <div class="learning-step"><strong>Check Policy</strong><span>Gatekeeper approves, downgrades, or blocks.</span></div>
          <div class="learning-step"><strong>Execute</strong><span>Executioner runs the allowed action.</span></div>
          <div class="learning-step"><strong>Verify</strong><span>Raven checks whether the signal improved.</span></div>
          <div class="learning-step"><strong>Retrospective</strong><span>Sage records symptom, root cause, action, outcome, confidence, and promotion recommendation.</span></div>
        </div>
      </section>

      <section class="instruction-section">
        <h2>Sage Memory</h2>
        <div class="memory-grid">
          <div class="instruction-tile"><strong>Episodic</strong><span>Who, what, where, when, and why for an incident.</span></div>
          <div class="instruction-tile"><strong>Semantic</strong><span>Facts, notes, policies, and environment knowledge.</span></div>
          <div class="instruction-tile"><strong>Procedural</strong><span>How to handle issues, escalate, roll back, and promote runbooks.</span></div>
          <div class="instruction-tile"><strong>Evaluative</strong><span>Success rates, what worked, what failed, and what should improve.</span></div>
        </div>
      </section>
      </div>

      <div class="instruction-view" id="instruction-view-profiles">
        <section class="profile-map-shell">
          <div class="profile-map-top">
            <div>
              <div class="profile-map-title">Agent Capability Profiles</div>
              <div class="profile-map-copy">A live registry map showing each agent as a character circle and every skill, memory class, runbook, worker tool, and policy surface as its own hoverable dot.</div>
            </div>
            <div class="profile-map-tools">
              <label class="profile-filter">Agent <select id="profile-agent-filter" onchange="renderInstructionProfiles()"><option value="all">All agents</option></select></label>
              <div class="profile-legend">
                <span>Agent</span>
                <span>Skill</span>
                <span>Memory</span>
                <span>Runbook</span>
                <span>Worker Tool</span>
                <span>Policy/Bus</span>
                <span>Declared Only</span>
              </div>
            </div>
          </div>
          <div class="profile-map-canvas" id="profile-map-canvas"><div class="profile-empty">Loading profiles...</div></div>
        </section>
        <section class="instruction-section">
          <h2>Profile Details</h2>
          <div id="profile-card-grid" class="profile-card-grid"><div class="profile-empty">Loading profiles...</div></div>
        </section>
      </div>
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
            <span class="muted" style="font-size:0.82rem">Last 30 days</span>
            <button class="btns" onclick="loadAiUsage()">Refresh</button>
          </div>
          <div class="card" style="padding:16px;margin-bottom:16px">
            <div style="font-weight:600;margin-bottom:10px">Daily Token Usage</div>
            <canvas id="ai-usage-chart" width="1000" height="340" style="width:100%;height:340px;max-width:100%;display:block"></canvas>
          </div>
          <div id="ai-usage-cards" style="display:flex;gap:12px;flex-wrap:wrap;margin-bottom:20px"></div>
          <div class="card" style="padding:16px;margin-bottom:16px">
            <div style="font-weight:600;margin-bottom:10px">Usage by Agent</div>
            <div id="ai-usage-table"><div class="empty">Loading...</div></div>
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
        <button class="orch-tab" id="orch-tab-runbooks" onclick="showOrchTab('runbooks')">Runbooks</button>
        <button class="orch-tab" id="orch-tab-builder" onclick="showOrchTab('builder')">Builder</button>
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
              <input class="orch-input" id="msg-summary" placeholder="Tell ORC: review logs, search memory, or prepare a redeploy approval..." onkeydown="if(event.key==='Enter'&&!event.shiftKey){event.preventDefault();sendAgentMessage();}">
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
          <section class="orch-panel skill-file-panel">
            <div class="orch-panel-head skill-editor-head">
              <div>
                <div class="orch-panel-title" id="skill-editor-title">Skill File</div>
                <div class="orch-count" id="skill-file-path">skills/new-skill/skills.md</div>
              </div>
              <div class="skill-editor-actions">
                <button class="btns" type="button" onclick="resetSkillForm()">New</button>
                <button class="btns" id="skill-reload-btn" type="button" onclick="reloadSkillFile()" style="display:none">Reload</button>
                <button class="btnp" id="skill-save-btn" type="button" onclick="saveSkillMarkdown()">Create File</button>
              </div>
            </div>
            <div class="skill-file-meta" id="skill-file-meta"></div>
            <textarea class="orch-textarea skill-md-editor" id="skill-markdown" spellcheck="false"></textarea>
          </section>
        </div>
      </section>

      <section class="orch-view" id="orch-view-runbooks">
        <div class="orch-page-grid">
          <section class="orch-panel">
            <div class="orch-panel-head">
              <div class="orch-panel-title">Runbook Registry</div>
              <div class="orch-count" id="orch-runbook-path"></div>
            </div>
            <div class="learning-list" id="orch-runbooks"><div class="empty">Loading runbooks...</div></div>
          </section>
          <section class="orch-panel">
            <div class="orch-panel-head"><div class="orch-panel-title">Execute Runbook</div></div>
            <div class="orch-form">
              <div class="orch-field wide"><label>Runbook</label><select class="orch-select" id="runbook-id"></select></div>
              <div class="orch-field wide"><label>Target</label><input class="orch-input" id="runbook-target" placeholder="container, service, or evidence scope"></div>
              <div class="orch-field wide"><label>Rationale</label><textarea class="orch-textarea" id="runbook-rationale"></textarea></div>
              <button class="btnp wide" onclick="executeRunbook()">Execute or Request Approval</button>
            </div>
            <div class="orch-panel-head" style="margin-top:14px"><div class="orch-panel-title">Recent Executions</div></div>
            <div class="learning-list" id="orch-runbook-executions"><div class="empty">No runbook executions yet.</div></div>
          </section>
        </div>
      </section>

      <section class="orch-view" id="orch-view-builder">
        <div class="orch-page-grid">
          <section class="orch-panel">
            <div class="orch-panel-head"><div class="orch-panel-title">Builder Promotion Request</div></div>
            <div class="orch-form">
              <div class="orch-field"><label>Tool ID</label><input class="orch-input" id="promotion-tool-id" placeholder="generated-diagnostic-tool"></div>
              <div class="orch-field"><label>Title</label><input class="orch-input" id="promotion-title" placeholder="Generated Diagnostic Tool"></div>
              <div class="orch-field wide"><label>Source Path</label><input class="orch-input" id="promotion-source-path" placeholder="builder/workspace/..."></div>
              <div class="orch-field wide"><label>Test Summary</label><textarea class="orch-textarea" id="promotion-tests"></textarea></div>
              <div class="orch-field wide"><label>Dry Run Summary</label><textarea class="orch-textarea" id="promotion-dry-run"></textarea></div>
              <div class="orch-field wide"><label>Artifact Markdown</label><textarea class="orch-textarea" id="promotion-artifact" placeholder="Optional complete tool.md content"></textarea></div>
              <button class="btnp wide" onclick="createToolPromotion()">Request Tool Promotion</button>
            </div>
          </section>
          <section class="orch-panel">
            <div class="orch-panel-head"><div class="orch-panel-title">Promotion Queue</div></div>
            <div class="learning-list" id="orch-tool-promotions"><div class="empty">No promotion requests yet.</div></div>
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
let _orch={agents:[],skills:[],tools:[],runbooks:[],messages:[],approvals:[],learnings:[],paths:{}};
let _orchTab='chat', _adminTab='connections', _currentUser=null, _users=[], _ravenConnected=false, _loadAllTimer=null, _skillEditId='', _agentEditId='';
let _instructionTab='framework';
let _profileAgentFilter='all', _profileNetwork=null;
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
const CHAT_STAMP_FMT=new Intl.DateTimeFormat('en-US',{timeZone:MT_ZONE,month:'numeric',day:'numeric',year:'2-digit',hour:'numeric',minute:'2-digit',timeZoneName:'short'});

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
function showInstructionSubpage(id){
  _instructionTab=id==='profiles'?'profiles':'framework';
  document.querySelectorAll('.instruction-subtab').forEach(t=>t.classList.remove('on'));
  document.querySelectorAll('.instruction-view').forEach(v=>v.classList.remove('on'));
  document.getElementById('instruction-tab-'+_instructionTab)?.classList.add('on');
  document.getElementById('instruction-view-'+_instructionTab)?.classList.add('on');
  if(_instructionTab==='profiles')ensureInstructionProfiles();
}
async function ensureInstructionProfiles(){
  const needsData=!((_orch.agents||[]).length);
  if(needsData){
    try{
      _orch=await fetch('/orchestration/summary').then(r=>r.json());
    }catch(e){
      const canvas=document.getElementById('profile-map-canvas');
      const cards=document.getElementById('profile-card-grid');
      const msg=`Could not load profiles: ${esc(e.message||'request failed')}`;
      if(canvas)canvas.innerHTML=`<div class="profile-empty">${msg}</div>`;
      if(cards)cards.innerHTML=`<div class="profile-empty">${msg}</div>`;
      return;
    }
  }
  renderInstructionProfiles();
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
  if(id==='instructions')showInstructionSubpage(_instructionTab||'framework');
  if(id==='admin'){showAdminTab(_adminTab||'connections');}
}
function tabsForViewMode(mode=_viewMode){
  return ['overview','network','events','orchestration','instructions','admin'];
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
function fmtChatStamp(iso){return iso?CHAT_STAMP_FMT.format(new Date(iso)):'';}
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
        <img src="${esc(c.src)}" alt="${esc(c.label)}"><span>${esc(c.label)}</span>
      </button>`;
    }).join('');
  }
  showLogoPreview('agent-logo-preview',_agentLogoDraft||BLACK_LOGO_SRC);
}
function chooseAgentCharacter(src){
  _agentDraftIcon=src||'/assets/characters/agent-scout.png';
  document.querySelectorAll('[data-agent-icon]').forEach(b=>b.classList.toggle('on',b.dataset.agentIcon===_agentDraftIcon));
}
function chooseAgentLogo(src){
  _agentLogoDraft=src||'';
  const file=document.getElementById('agent-logo');
  if(file)file.value='';
  renderAgentBuilderChoices();
}
function clearAgentLogo(){
  _agentLogoDraft='';
  const file=document.getElementById('agent-logo');
  if(file)file.value='';
  renderAgentBuilderChoices();
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
  if(refresh){
    _issuePills=[];
    _issueKeys=new Set();
    renderFeed();
    loadAll();
  }
}
function countLabel(n){return n>99?'99+':String(n);}
function statusCircles(errors,warnings,showOk=true){
  const dots=[];
  if(errors>0)dots.push(`<span class="status-dot b-err" title="${errors} error${errors!==1?'s':''}">${esc(countLabel(errors))}</span>`);
  if(warnings>0)dots.push(`<span class="status-dot b-warn" title="${warnings} warning${warnings!==1?'s':''}">${esc(countLabel(warnings))}</span>`);
  if(!dots.length&&showOk)dots.push('<span class="status-dot b-ok" title="No recent errors or warnings">0</span>');
  if(!dots.length)return '';
  return `<div class="status-circles">${dots.join('')}</div>`;
}
function newIssueCounts(msg){
  return {
    errors:Number(msg.errors||0),
    warnings:Number(msg.warnings||0)
  };
}
function findStackByKey(key){
  return _stacks.find(stack=>stackCharacterKey(stack)===key)||null;
}
function openCharDlg(btn){
  _charEditKey=btn.dataset.stackKey||'';
  const stack=findStackByKey(_charEditKey);
  _charDraftCharacter=selectedCharacterId(stack||{server:'',name:''});
  _charLogoDraft=stackSetting(_charEditKey,'Logo')||legacyCorporateLogoForKey(_charEditKey);
  _charDefaultLogo=stack?defaultCorporateLogo(stack).src:CORPORATE_LOGOS[0].src;
  document.getElementById('char-dlg-t').textContent=`${btn.dataset.stackName||'Stack'} Settings`;
  document.getElementById('char-friendly').value=stackSetting(_charEditKey,'FriendlyName')||'';
  document.getElementById('char-logo').value='';
  document.getElementById('char-grid').innerHTML=CHARACTERS.map(c=>`
    <button class="char-choice ${c.id===_charDraftCharacter?'on':''}" data-character-id="${esc(c.id)}" type="button" onclick="chooseStackCharacter('${esc(c.id)}')">
      <img src="${esc(c.src)}" alt="${esc(c.label)}"><span>${esc(c.label)}</span>
    </button>`).join('');
  document.getElementById('logo-grid').innerHTML=CORPORATE_LOGOS.map(c=>{
    const active=(_charLogoDraft||_charDefaultLogo)===c.src;
    return `<button class="char-choice logo-choice ${active?'on':''}" data-logo-src="${esc(c.src)}" type="button" onclick="chooseStackLogo('${esc(c.src)}')">
      <img src="${esc(c.src)}" alt="${esc(c.label)}"><span>${esc(c.label)}</span>
    </button>`;
  }).join('');
  showLogoPreview('char-logo-preview',_charLogoDraft||_charDefaultLogo);
  document.getElementById('char-dlg').showModal();
}
function closeCharDlg(){document.getElementById('char-dlg').close();}
function chooseStackCharacter(id){
  const ch=CHARACTER_BY_ID[id];
  if(!ch||!_charEditKey)return;
  _charDraftCharacter=ch.id;
  document.querySelectorAll('.char-choice[data-character-id]').forEach(b=>b.classList.toggle('on',b.dataset.characterId===ch.id));
}
function setStackLogoDraft(src){
  _charLogoDraft=src||'';
  showLogoPreview('char-logo-preview',_charLogoDraft||_charDefaultLogo);
  document.querySelectorAll('#logo-grid .logo-choice').forEach(b=>b.classList.toggle('on',(_charLogoDraft||_charDefaultLogo)===b.dataset.logoSrc));
}
function chooseStackLogo(src){
  if(!_charEditKey)return;
  setStackLogoDraft(src);
  document.getElementById('char-logo').value='';
}
function saveStackSettings(){
  if(!_charEditKey)return;
  setStackSetting(_charEditKey,'Character',_charDraftCharacter);
  setStackSetting(_charEditKey,'FriendlyName',document.getElementById('char-friendly').value.trim());
  setStackSetting(_charEditKey,'Logo',_charLogoDraft);
  closeCharDlg();
  renderVisualViews();
}
function clearStackLogo(){
  setStackLogoDraft('');
  document.getElementById('char-logo').value='';
}
function showLogoPreview(id,src){
  const img=document.getElementById(id);
  if(!img)return;
  img.src=src||'';
  img.classList.toggle('empty',!src);
}
function readImageFile(file){
  return new Promise((resolve,reject)=>{
    if(!file){resolve('');return;}
    const reader=new FileReader();
    reader.onload=()=>resolve(String(reader.result||''));
    reader.onerror=()=>reject(new Error('Could not read image file.'));
    reader.readAsDataURL(file);
  });
}
function clearConnLogo(){
  _connLogoDraft='';
  document.getElementById('f-logo').value='';
  showLogoPreview('f-logo-preview','');
}
function stackPollCount(stack){
  return (stack.containers||[]).reduce((sum,app)=>{
    const n=Number(app.polls ?? app.poll_count ?? 0);
    return sum+(Number.isFinite(n)&&n>0?n:0);
  },0);
}
function stackIssueCount(stack){
  return (stack.containers||[]).reduce((sum,app)=>{
    const c=issueCounts(app);
    return sum+c.errors+c.warnings;
  },0);
}
function healthForContainers(containers){
  const totals=(containers||[]).reduce((acc,app)=>{
    const c=issueCounts(app);
    acc.defects+=c.errors+c.warnings;
    acc.lines+=observedLineCount(app);
    return acc;
  },{defects:0,lines:0});
  const denominator=Math.max(totals.lines,totals.defects);
  const pct=denominator>0?Math.round((1-Math.min(totals.defects,denominator)/denominator)*100):100;
  const percent=Math.max(0,Math.min(100,pct));
  return {percent,cls:percent>=95?'ok':percent>=80?'warn':'err',defects:totals.defects,lines:totals.lines};
}
function homeHealthHtml(health){
  const title=`Health = 1 - (${health.defects} defects / ${health.lines} lines queried)`;
  return `<span class="home-health ${esc(health.cls)}" title="${esc(title)}">
    <span class="home-health-bar"><span style="width:${health.percent}%"></span></span>
    <span class="home-health-pct">${health.percent}%</span>
  </span>`;
}
function metricTotalsForStacks(stacks){
  const containers=(stacks||[]).flatMap(stack=>stack.containers||[]);
  return {
    polls:(stacks||[]).reduce((sum,stack)=>sum+stackPollCount(stack),0),
    issues:(stacks||[]).reduce((sum,stack)=>sum+stackIssueCount(stack),0),
    containers:containers.length,
    health:healthForContainers(containers)
  };
}
function renderHomeIssues(stacks){
  const el=document.getElementById('home-issues');
  if(!el)return;
  const rows=[];
  (stacks||[]).forEach(stack=>{
    (stack.containers||[]).forEach(app=>{
      const counts=issueCounts(app);
      if(counts.errors<=0&&counts.warnings<=0)return;
      const severity=counts.errors>0?'error':'warning';
      const containerName=app.full_name||app.name||'';
      rows.push({stack,app,counts,severity,containerName,total:counts.errors+counts.warnings});
    });
  });
  rows.sort((a,b)=>b.counts.errors-a.counts.errors||b.counts.warnings-a.counts.warnings||a.containerName.localeCompare(b.containerName));
  if(!rows.length){
    el.innerHTML='<div class="empty">No containers have warning or error dots in this window.</div>';
    return;
  }
  el.innerHTML=rows.map(row=>`
    <div class="sub-row home-issue-row" onclick="jumpToEventsFromEl(this)"
      data-server="${esc(row.stack.server)}" data-container="${esc(row.containerName)}" data-severity="${esc(row.severity)}"
      title="Open Event Log for ${esc(row.containerName)}">
      <span class="home-issue-main">
        <span class="sub-name">${esc(containerFriendlyName(row.app))}</span>
        <span class="home-issue-server">${esc(row.stack.server_name||row.stack.server||'Unknown server')}</span>
      </span>
      <span class="sub-type">${esc(row.app.type||'api')}</span>
      ${statusCircles(row.counts.errors,row.counts.warnings,false)}
    </div>`).join('');
}
function renderHomeMetrics(stacks){
  const el=document.getElementById('home-metrics');
  if(!el)return;
  const kingdoms=groupKingdoms(stacks||[]);
  if(!kingdoms.length){
    el.innerHTML='<div class="empty">No connection metrics yet.</div>';
    return;
  }
  const head=`<div class="metric-table-head">
    <span>Connection</span><span>Polls</span><span>Issues</span><span>Containers</span><span>Health</span>
  </div>`;
  el.innerHTML=head+kingdoms.map(k=>{
    k.stacks.sort((a,b)=>stackFriendlyName(a).localeCompare(stackFriendlyName(b)));
    const totals=metricTotalsForStacks(k.stacks);
    const stackRows=k.stacks.map(stack=>{
      const polls=stackPollCount(stack);
      const issues=stackIssueCount(stack);
      const containers=(stack.containers||[]).length;
      const health=healthForContainers(stack.containers||[]);
      return `<div class="metric-stack" title="${esc(stack.name)}">
        <span class="metric-stack-name">${esc(stackFriendlyName(stack))}</span>
        <span class="metric-num">${polls}</span>
        <span class="metric-num">${issues}</span>
        <span class="metric-num">${containers}</span>
        ${homeHealthHtml(health)}
      </div>`;
    }).join('');
    return `<details class="metric-connection">
      <summary class="metric-summary" title="Expand ${esc(serverDisplayName(k))}">
        <span class="metric-name"><span class="metric-caret"></span>${esc(serverDisplayName(k))}</span>
        <span class="metric-num">${totals.polls}</span>
        <span class="metric-num">${totals.issues}</span>
        <span class="metric-num">${totals.containers}</span>
        ${homeHealthHtml(totals.health)}
      </summary>
      <div class="metric-children">${stackRows}</div>
    </details>`;
  }).join('');
}
function renderHomeRecent(){
  const el=document.getElementById('home-recent');
  if(!el)return;
  const issues=(_homeRecent||[]).filter(e=>['critical','error','warning'].includes(e.severity)).slice(0,7);
  if(!issues.length){
    el.innerHTML='<div class="empty">No recent issues in this window.</div>';
    return;
  }
  el.innerHTML=issues.map(e=>{
    const sev=e.severity==='warning'?'warning':'error';
    const cls=e.severity==='warning'?'warn':'err';
    const container=e.container_name||'';
    const display=containerDisplayName(container);
    return `<div class="recent-issue" onclick="jumpToEventsFromEl(this)"
      data-server="${esc(e.server||'')}" data-container="${esc(container)}" data-severity="${esc(sev)}"
      title="Open Event Log for ${esc(container)}">
      <span class="recent-sev ${cls}">${esc(e.severity)}</span>
      <span class="recent-name">${esc(display)}</span>
      <span class="muted small">${esc(fmtShort(e.occurred_at))}</span>
      <span class="recent-msg">${esc(e.message||'')}</span>
    </div>`;
  }).join('');
}
function renderHomeDashboard(){
  renderHomeIssues(_stacks);
  renderHomeMetrics(_stacks);
  renderHomeRecent();
}
async function loadHomeRecent(){
  try{
    const qs=`hours=${_windowHours}`;
    const [err,warn]=await Promise.all([
      fetch(`/events?limit=40&${qs}&severity=error`).then(r=>r.json()),
      fetch(`/events?limit=20&${qs}&severity=warning`).then(r=>r.json())
    ]);
    _homeRecent=[...(err.items||[]),...(warn.items||[])]
      .sort((a,b)=>new Date(b.occurred_at)-new Date(a.occurred_at))
      .slice(0,7);
    renderHomeDashboard();
  }catch{
    const el=document.getElementById('home-recent');
    if(el)el.innerHTML='<div class="empty">Could not load recent issues.</div>';
  }
}
function renderVisualViews(){
  renderHomeDashboard();
  _populateServerDropdown();
  if(_stacks.length){
    renderOverview(_stacks);
    renderNetwork(_stacks);
  }
}
function jumpToEventsFromEl(el){
  jumpToEvents(el.dataset.server||'',el.dataset.container||'',el.dataset.severity||'');
}
function oracleNamesPayload(){
  const friendly_names={};
  _stacks.forEach(stack=>{
    friendly_names[stack.name]=stackFriendlyName(stack);
    (stack.containers||[]).forEach(app=>{
      const friendly=containerFriendlyName(app);
      if(app.full_name)friendly_names[app.full_name]=friendly;
      if(app.name)friendly_names[app.name]=friendly;
    });
  });
  return {friendly_names};
}
function oracleAnalysisHtml(text){
  const parts=esc(text||'').split('**');
  return parts.map((part,i)=>i%2&&i<parts.length-1?`<strong>${part}</strong>`:part).join('');
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
    box.innerHTML=oracleAnalysisHtml(_oracleState.analysis);
    return;
  }
  box.className='oracle-box empty';
  box.textContent='Ready to review the last hour of events.';
}
async function runOracle(){
  _oracleState={busy:true,summary:_oracleState.summary,analysis:'',error:''};
  renderOracle();
  try{
    const r=await fetch('/oracle/review',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(oracleNamesPayload())});
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
async function loadStacks(){
  try{
    const d=await fetch(`/overview?hours=${_windowHours}`).then(r=>r.json());
    _stacks=d.stacks||[];
    renderVisualViews();
  }catch(e){
    document.getElementById('home-issues').innerHTML='<div class="empty">Could not load issue containers.</div>';
    document.getElementById('home-metrics').innerHTML='<div class="empty">Could not load metrics.</div>';
    document.getElementById('overview-grid').innerHTML='<div class="empty">Could not load overview.</div>';
    document.getElementById('network-stage').innerHTML='<div class="empty">Could not load network.</div>';
  }
}
async function loadOverview(){if(_stacks.length)renderOverview(_stacks);else await loadStacks();}
async function loadNetwork(){if(_stacks.length)renderNetwork(_stacks);else await loadStacks();}

function groupKingdoms(stacks){
  const kingdomMap=new Map();
  stacks.forEach(s=>{
    const key=s.server||'Unknown server';
    if(!kingdomMap.has(key))kingdomMap.set(key,{server:key,server_name:s.server_name||key,server_logo:s.server_logo||'',stacks:[]});
    const kingdom=kingdomMap.get(key);
    if(s.server_name)kingdom.server_name=s.server_name;
    if(s.server_logo)kingdom.server_logo=s.server_logo;
    kingdom.stacks.push({...s,server:key,containers:s.containers||[]});
  });
  return [...kingdomMap.values()].filter(k=>k.stacks.some(s=>s.containers.length)).sort((a,b)=>serverDisplayName(a).localeCompare(serverDisplayName(b)));
}
function renderOverview(stacks){renderStackGrid(stacks,'overview-grid');}
function renderStackGrid(stacks,targetId){
  const grid=document.getElementById(targetId);
  if(!stacks.length){grid.innerHTML='<div class="empty">No stacks found. Add a connection in the Connections tab.</div>';return;}
  const kingdoms=groupKingdoms(stacks);
  if(!kingdoms.length){grid.innerHTML='<div class="empty">No containers found for the configured connections.</div>';return;}
  const corporate=_viewMode==='corporate';
  grid.innerHTML=kingdoms.map(k=>{
    k.stacks.sort((a,b)=>a.name.localeCompare(b.name));
    const totals=k.stacks.reduce((acc,stack)=>{
      stack.containers.forEach(app=>{const c=issueCounts(app);acc.errors+=c.errors;acc.warnings+=c.warnings;});
      return acc;
    },{errors:0,warnings:0});
    const kingdomCls=totals.errors>0?'er':totals.warnings>0?'warn':'';
    const totalCtr=k.stacks.reduce((a,s)=>a+s.containers.length,0);
    const kIcon=corporate
      ? `<img class="kingdom-logo" src="${esc(serverLogo(k)||'/assets/kingdoms/castle.png')}" alt="">`
      : `<img class="kingdom-castle" src="/assets/kingdoms/castle.png" alt="">`;
    const stackCards=k.stacks.map(stack=>{
      stack.containers.sort((a,b)=>a.type.localeCompare(b.type)||a.name.localeCompare(b.name));
      const stackTotals=stack.containers.reduce((acc,app)=>{const c=issueCounts(app);acc.errors+=c.errors;acc.warnings+=c.warnings;return acc;},{errors:0,warnings:0});
      const hasErr=stackTotals.errors>0, hasWarn=stackTotals.warnings>0;
      const severity=hasErr?'error':hasWarn?'warning':'';
      const cardCls=hasErr?'er':hasWarn?'warn':'';
      const charId=selectedCharacterId(stack);
      const ch=CHARACTER_BY_ID[charId]||CHARACTERS[0];
      const key=stackCharacterKey(stack);
      const friendly=stackFriendlyName(stack);
      const art=corporate?selectedStackLogo(stack):ch.src;
      const subRows=stack.containers.map(app=>{
        const counts=issueCounts(app);
        const subErr=counts.errors>0, subWarn=counts.warnings>0;
        const subSeverity=subErr?'error':subWarn?'warning':'';
        const displayName=containerFriendlyName(app);
        return `<div class="sub-row" onclick="jumpToEventsFromEl(this)"
          data-server="${esc(stack.server)}" data-container="${esc(app.full_name)}" data-severity="${esc(subSeverity)}"
          title="${esc(app.full_name)}">
          <span class="sub-name">${esc(displayName)}</span>
          <span class="sub-type">${esc(app.type)}</span>
          ${statusCircles(counts.errors,counts.warnings,false)}
        </div>`;
      }).join('');
      return `<div class="stack-card ${cardCls}" data-stack-key="${esc(key)}">
      <button class="gear-btn" type="button" onclick="openCharDlg(this)"
        data-stack-key="${esc(key)}" data-stack-name="${esc(stack.name)}" data-character="${esc(charId)}"
        title="Configure ${esc(stack.name)} settings">&#9881;</button>
      <button class="char-frame" type="button" onclick="jumpToEventsFromEl(this)"
        data-server="${esc(stack.server)}" data-container="${esc(stack.name)}" data-severity="${esc(severity)}"
        title="View events for ${esc(stack.name)}">
        <img class="char-img" src="${esc(art)}" alt="${esc(friendly)}">
        <span class="stack-banner">${esc(friendly)}</span>
        ${statusCircles(stackTotals.errors,stackTotals.warnings,false)}
      </button>
      <div class="stack-meta">
        <div class="stack-copy">
          <div class="stack-sv">${stack.containers.length} subordinate${stack.containers.length!==1?'s':''}</div>
        </div>
      </div>
      <div class="sub-list">${subRows}</div>
    </div>`;
    }).join('');
    return `<section class="kingdom ${corporate?'corp ':''}${kingdomCls}">
      <div class="kingdom-hdr">
        <div class="kingdom-title">
          ${kIcon}
          <div class="kingdom-copy">
            <div class="kingdom-name" title="${esc(k.server)}">${esc(serverDisplayName(k))}</div>
            <div class="kingdom-sub">${k.stacks.length} stack${k.stacks.length!==1?'s':''} · ${totalCtr} container${totalCtr!==1?'s':''}</div>
          </div>
        </div>
        <div class="kingdom-score">${statusCircles(totals.errors,totals.warnings,false)}</div>
      </div>
      <div class="kingdom-stacks">${stackCards}</div>
    </section>`;
  }).join('');
}

function zoomNetwork(delta){
  _networkZoom=Math.min(1.8,Math.max(0.35,_networkZoom+delta));
  applyNetworkTransform();
}
function resetNetworkView(){
  _networkPan.worldKey='';
  _networkPan.centeredStageId='';
  _networkPan.centeredVisible=false;
  renderNetwork(_stacks);
}
function networkRand(key,salt){
  return (hashStr(`${key}::${salt}`)%10000)/10000;
}
function networkClamp(v,min,max){return Math.max(min,Math.min(max,v));}
function networkPx(v){return `${v.toFixed(1)}px`;}
function networkNodeStorageKey(id){return `orc.network.node.${encodeURIComponent(id)}`;}
function loadNetworkNodePosition(id){
  try{
    const raw=localStorage.getItem(networkNodeStorageKey(id));
    if(!raw)return null;
    const pos=JSON.parse(raw);
    return Number.isFinite(pos.x)&&Number.isFinite(pos.y)?pos:null;
  }catch{return null;}
}
function saveNetworkNodePosition(id,x,y){
  try{localStorage.setItem(networkNodeStorageKey(id),JSON.stringify({x,y}));}catch{}
}
function stackIssueTotals(stack){
  return stack.containers.reduce((acc,app)=>{const c=issueCounts(app);acc.errors+=c.errors;acc.warnings+=c.warnings;return acc;},{errors:0,warnings:0});
}
function observedLineCount(app){
  const n=Number(app.lines_queried ?? app.total_lines ?? app.lines ?? app.events ?? app.event_count ?? 0);
  return Number.isFinite(n)&&n>0?n:0;
}
function kingdomHealth(k){
  const totals=(k.stacks||[]).reduce((acc,stack)=>{
    (stack.containers||[]).forEach(app=>{
      const c=issueCounts(app);
      acc.defects+=c.errors+c.warnings;
      acc.lines+=observedLineCount(app);
    });
    return acc;
  },{defects:0,lines:0});
  const denominator=Math.max(totals.lines,totals.defects);
  const percent=denominator>0?Math.round((1-Math.min(totals.defects,denominator)/denominator)*100):100;
  const pct=networkClamp(percent,0,100);
  return {
    percent:pct,
    cls:pct>=95?'ok':pct>=80?'warn':'err',
    defects:totals.defects,
    lines:totals.lines
  };
}
function issueSeverityFromCounts(counts){
  return counts.errors>0?'err':counts.warnings>0?'warn':'';
}
function layoutNetworkTopology(k){
  const stacks=[...k.stacks].sort((a,b)=>a.name.localeCompare(b.name));
  const totalContainers=stacks.reduce((sum,s)=>sum+(s.containers||[]).length,0);
  const width=1040;
  const height=Math.max(660,Math.min(920,560+Math.sqrt(totalContainers)*24+Math.max(0,stacks.length-6)*20));
  const hub={id:`hub:${k.server}`,type:'hub',fixed:true,x:width/2,y:height/2,r:60};
  const nodes=[hub],stackNodes=[],workerNodes=[],links=[];
  const count=Math.max(1,stacks.length);
  const angleOffset=(networkRand(k.server||'network','angle')-.5)*.72;
  const ringRx=Math.min(width*.31,205+Math.min(100,stacks.length*8+totalContainers*.75));
  const ringRy=Math.min(height*.3,160+Math.min(95,stacks.length*7+totalContainers*.65));
  stacks.forEach((stack,i)=>{
    const angle=(-Math.PI/2)+angleOffset+(Math.PI*2*i/count);
    const stagger=count>7&&i%2?.12:0;
    const sx=hub.x+Math.cos(angle)*ringRx*(1+stagger);
    const sy=hub.y+Math.sin(angle)*ringRy*(1+stagger);
    const stackNode={id:`stack:${stackCharacterKey(stack)}`,type:'stack',stack,x:sx,y:sy,targetX:sx,targetY:sy,r:72,angle};
    nodes.push(stackNode);
    stackNodes.push(stackNode);
    links.push({source:hub,target:stackNode,type:'hub',severity:issueSeverityFromCounts(stackIssueTotals(stack))});
    const apps=[...(stack.containers||[])].sort((a,b)=>containerFriendlyName(a).localeCompare(containerFriendlyName(b)));
    const spread=Math.min(1.5,.52+apps.length*.13);
    apps.forEach((app,j)=>{
      const rel=apps.length===1?0:(j-(apps.length-1)/2)/(apps.length-1);
      const appKey=app.full_name||app.name||`${stack.name}:${j}`;
      const fanAngle=angle+rel*spread+(networkRand(appKey,'fan')-.5)*.14;
      const dist=112+Math.floor(j/5)*36+(apps.length>4?16:0);
      const workerNode={
        id:`worker:${stackCharacterKey(stack)}:${app.full_name||app.name||j}`,type:'worker',stack,stackNode,app,
        x:sx+Math.cos(fanAngle)*dist,
        y:sy+Math.sin(fanAngle)*dist*.88,
        targetX:sx+Math.cos(fanAngle)*dist,
        targetY:sy+Math.sin(fanAngle)*dist*.88,
        r:48,angle:fanAngle
      };
      nodes.push(workerNode);
      workerNodes.push(workerNode);
      links.push({source:stackNode,target:workerNode,type:'worker',severity:issueSeverityFromCounts(issueCounts(app))});
    });
  });
  for(let iter=0;iter<210;iter++){
    links.forEach(link=>{
      const a=link.source,b=link.target;
      const dx=b.x-a.x,dy=b.y-a.y;
      const d=Math.hypot(dx,dy)||1;
      const desired=link.type==='hub'?Math.min(270,178+(b.stack?.containers?.length||0)*7):104;
      const strength=link.type==='hub'?.03:.078;
      const shift=(d-desired)/d*strength;
      if(!a.fixed){
        const aw=a.type==='stack'?.42:.5;
        a.x+=dx*shift*aw;
        a.y+=dy*shift*aw;
      }
      if(!b.fixed){
        const bw=b.type==='stack'?.78:.86;
        b.x-=dx*shift*bw;
        b.y-=dy*shift*bw;
      }
    });
    nodes.forEach(node=>{
      if(node.fixed)return;
      const pull=node.type==='stack'?.03:.044;
      node.x+=(node.targetX-node.x)*pull;
      node.y+=(node.targetY-node.y)*pull;
    });
    for(let i=0;i<nodes.length;i++){
      for(let j=i+1;j<nodes.length;j++){
        const a=nodes[i],b=nodes[j];
        let dx=b.x-a.x,dy=b.y-a.y;
        let d=Math.hypot(dx,dy)||.01;
        const min=a.r+b.r+(a.type===b.type?18:10);
        if(d<min){
          const push=(min-d)/d*.43;
          dx*=push;dy*=push;
          if(!a.fixed){
            const aw=a.type==='stack'?.42:.58;
            a.x-=dx*aw;a.y-=dy*aw;
          }
          if(!b.fixed){
            const bw=b.type==='stack'?.42:.58;
            b.x+=dx*bw;b.y+=dy*bw;
          }
        }
      }
    }
    nodes.forEach(node=>{
      if(node.fixed)return;
      const padX=node.type==='stack'?90:58;
      const padY=node.type==='stack'?90:56;
      node.x=networkClamp(node.x,padX,width-padX);
      node.y=networkClamp(node.y,padY,height-padY);
    });
    hub.x=width/2;hub.y=height/2;
  }
  return {width,height,hub,stacks:stackNodes,workers:workerNodes,links,kingdom:k};
}
function layoutNetworkWorld(kingdoms){
  const layouts=kingdoms.map(k=>layoutNetworkTopology(k));
  const cols=layouts.length<=2?layouts.length:Math.ceil(Math.sqrt(layouts.length));
  const gap=90;
  const cellW=Math.max(...layouts.map(l=>l.width),1000)+gap;
  const cellH=Math.max(...layouts.map(l=>l.height),760)+gap;
  const world={width:Math.max(cellW,cols*cellW),height:Math.max(cellH,Math.ceil(layouts.length/cols)*cellH),groups:[],links:[],hubs:[],stacks:[],workers:[]};
  layouts.forEach((layout,i)=>{
    const col=i%cols,row=Math.floor(i/cols);
    const ox=col*cellW+(cellW-layout.width)/2;
    const oy=row*cellH+(cellH-layout.height)/2;
    const offsetNode=node=>{
      const moved=loadNetworkNodePosition(node.id);
      return {...node,x:moved?moved.x:node.x+ox,y:moved?moved.y:node.y+oy};
    };
    const hub={...offsetNode(layout.hub),kingdom:layout.kingdom};
    const stacks=layout.stacks.map(offsetNode);
    const workers=layout.workers.map(node=>({...offsetNode(node),stackNode:stacks[layout.stacks.indexOf(node.stackNode)]}));
    const stackById=Object.fromEntries(stacks.map(n=>[n.id,n]));
    const workerById=Object.fromEntries(workers.map(n=>[n.id,n]));
    layout.links.forEach(link=>{
      const source=link.source.type==='hub'?hub:stackById[link.source.id];
      const target=link.target.type==='stack'?stackById[link.target.id]:workerById[link.target.id];
      if(source&&target)world.links.push({...link,source,target});
    });
    world.groups.push({...layout,ox,oy,hub,stacks,workers});
    world.hubs.push(hub);
    world.stacks.push(...stacks);
    world.workers.push(...workers);
  });
  return world;
}
function networkWorldKey(kingdoms){
  return kingdoms.map(k=>`${k.server}:${k.stacks.length}:${k.stacks.reduce((n,s)=>n+s.containers.length,0)}`).join('|');
}
function applyNetworkTransform(){
  document.querySelectorAll('.net-pan-surface').forEach(surface=>{
    surface.style.transform=`translate(${_networkPan.x}px,${_networkPan.y}px) scale(${_networkZoom})`;
  });
}
function centerNetworkWorld(world,stage){
  const sw=stage.clientWidth||1000,sh=stage.clientHeight||700;
  const fit=Math.min(1,(sw-40)/world.width,(sh-40)/world.height);
  _networkZoom=Math.max(.5,Math.min(.92,fit*1.18));
  _networkPan.x=(sw-world.width*_networkZoom)/2;
  _networkPan.y=(sh-world.height*_networkZoom)/2;
}
function getNetworkNodeEl(id,root=document){
  return [...root.querySelectorAll('[data-node-id]')].find(el=>el.dataset.nodeId===id)||null;
}
function updateNetworkLines(id,x,y,root=document){
  root.querySelectorAll('.net-backbone line').forEach(line=>{
    if(line.dataset.source===id){line.setAttribute('x1',x.toFixed(1));line.setAttribute('y1',y.toFixed(1));}
    if(line.dataset.target===id){line.setAttribute('x2',x.toFixed(1));line.setAttribute('y2',y.toFixed(1));}
  });
}
function setNetworkNodePosition(el,x,y,persist=true){
  const root=el.closest('.net-pan-surface')||document;
  el.dataset.x=x.toFixed(1);
  el.dataset.y=y.toFixed(1);
  el.style.setProperty('--x',networkPx(x));
  el.style.setProperty('--y',networkPx(y));
  updateNetworkLines(el.dataset.nodeId,x,y,root);
  if(el.classList.contains('net-worker')){
    const stackEl=getNetworkNodeEl(el.dataset.stackNodeId||'',root);
    if(stackEl){
      const sx=Number(stackEl.dataset.x||0);
      el.classList.toggle('left',x<sx);
    }
  }
  if(persist)saveNetworkNodePosition(el.dataset.nodeId,x,y);
}
function renderTopology(stacks,stageId,artMode='character'){
  const stage=document.getElementById(stageId);
  if(!stage)return;
  const label='network';
  if(!stacks.length){stage.innerHTML='<div class="empty">No stacks found. Add a connection in the Connections tab.</div>';return;}
  const kingdoms=groupKingdoms(stacks);
  if(!kingdoms.length){stage.innerHTML=`<div class="empty">No containers found for the configured ${label}.</div>`;return;}
  const key=networkWorldKey(kingdoms);
  const world=layoutNetworkWorld(kingdoms);
  const visible=!!stage.closest('.pane.on')&&stage.clientWidth>0;
  if(_networkPan.worldKey!==key||(visible&&(_networkPan.centeredStageId!==stageId||!_networkPan.centeredVisible))){
    _networkPan.worldKey=key;
    _networkPan.centeredStageId=stageId;
    _networkPan.centeredVisible=visible;
    centerNetworkWorld(world,stage);
  }
  const lineHtml=world.links.map(link=>{
    const cls=`${link.type==='hub'?'hub-link':'worker-link'} ${link.severity}`.trim();
    return `<line class="${esc(cls)}" data-source="${esc(link.source.id)}" data-target="${esc(link.target.id)}" x1="${link.source.x.toFixed(1)}" y1="${link.source.y.toFixed(1)}" x2="${link.target.x.toFixed(1)}" y2="${link.target.y.toFixed(1)}"></line>`;
  }).join('');
  const hubHtml=world.hubs.map(node=>{
    const health=kingdomHealth(node.kingdom||{});
    const healthTitle=`Health = 1 - (${health.defects} defects / ${health.lines} lines queried)`;
    const hubArt=artMode==='logo'?serverLogo(node.kingdom||{}):'/assets/kingdoms/castle.png';
    return `<div class="net-hub" data-node-id="${esc(node.id)}" data-x="${node.x.toFixed(1)}" data-y="${node.y.toFixed(1)}" style="--x:${networkPx(node.x)};--y:${networkPx(node.y)}" title="${esc(serverDisplayName(node.kingdom||{}))}">
      <img src="${esc(hubArt)}" alt="">
      <span class="net-hub-meta">
        <span class="net-hub-label">${esc(serverDisplayName(node.kingdom||{}))}</span>
        <span class="net-health ${esc(health.cls)}" title="${esc(healthTitle)}">
          <span class="net-health-bar"><span style="width:${health.percent}%"></span></span>
          <span>${health.percent}%</span>
        </span>
      </span>
    </div>`;
  }).join('');
  const stacksHtml=world.stacks.map(node=>{
    const stack=node.stack;
    const charId=selectedCharacterId(stack);
    const ch=CHARACTER_BY_ID[charId]||CHARACTERS[0];
    const art=artMode==='logo'?selectedStackLogo(stack):ch.src;
    const stackTotals=stackIssueTotals(stack);
    const severity=stackTotals.errors>0?'error':stackTotals.warnings>0?'warning':'';
    return `<div class="network-stack" data-node-id="${esc(node.id)}" data-x="${node.x.toFixed(1)}" data-y="${node.y.toFixed(1)}" style="--x:${networkPx(node.x)};--y:${networkPx(node.y)}">
      <div class="net-stack-node" onclick="jumpToEvents('${esc(stack.server)}','${esc(stack.name)}','${esc(severity)}')" title="${esc(stack.name)}">
        <img src="${esc(art)}" alt="${esc(stackFriendlyName(stack))}">
        <span class="net-stack-name">${esc(stackFriendlyName(stack))}</span>
      </div>
    </div>`;
  }).join('');
  const workersHtml=world.workers.map(node=>{
    const app=node.app;
    const counts=issueCounts(app);
    const dot=counts.errors>0?'err':counts.warnings>0?'warn':'none';
    const dotCount=counts.errors>0?counts.errors:counts.warnings;
    const dotText=dotCount>99?'99+':String(dotCount);
    const workerPool=WORKER_ASSETS;
    const workerAsset=artMode==='logo'?BLACK_LOGO_SRC:workerPool[hashStr(app.full_name||app.name)%workerPool.length];
    const displayName=containerFriendlyName(app);
    const side=node.x<node.stackNode.x?'left':'right';
    const containerName=app.full_name||app.name||'';
    const pollCount=Number(app.polls ?? app.poll_count ?? 0);
    const lineCount=observedLineCount(app);
    const serverName=node.stack.server_name||node.stack.server||'Unknown server';
    const issueText=`${counts.errors} error${counts.errors!==1?'s':''}, ${counts.warnings} warning${counts.warnings!==1?'s':''}`;
    const checking=_networkChecking.server===node.stack.server&&_networkChecking.container===containerName;
    const focusedChecking=!!_focusedNetworkChecking[`${node.stack.server}::${containerName}`];
    return `<div class="net-worker ${side} ${checking?'checking':''} ${focusedChecking?'focused-checking':''}" data-node-id="${esc(node.id)}" data-stack-node-id="${esc(node.stackNode.id)}" data-x="${node.x.toFixed(1)}" data-y="${node.y.toFixed(1)}" style="--x:${networkPx(node.x)};--y:${networkPx(node.y)}" onclick="jumpToEventsFromEl(this)" data-server="${esc(node.stack.server)}" data-container="${esc(containerName)}" data-severity="${esc(dot==='err'?'error':dot==='warn'?'warning':'')}" aria-label="${esc(containerName)}">
      <span class="worker-avatar"><img src="${esc(workerAsset)}" alt=""></span>
      <span class="net-dot ${dot}">${esc(dotText)}</span>
      <span class="net-worker-name">${esc(displayName)}</span>
      <span class="net-detail-bubble" role="tooltip">
        <span class="net-detail-title">${esc(displayName)}</span>
        <span class="net-detail-grid">
          <span>Server</span><span>${esc(serverName)}</span>
          <span>Stack</span><span>${esc(stackFriendlyName(node.stack))}</span>
          <span>Container</span><span>${esc(containerName)}</span>
          <span>Type</span><span>${esc(app.type||'api')}</span>
          <span>Issues</span><span>${esc(issueText)}</span>
          <span>Polls</span><span>${Number.isFinite(pollCount)?pollCount:0}</span>
          <span>Lines</span><span>${lineCount}</span>
        </span>
      </span>
    </div>`;
  }).join('');
  stage.innerHTML=`<div class="net-pan-surface" data-topology-mode="${esc(artMode)}" style="width:${world.width}px;height:${world.height}px">
    <svg class="net-backbone" width="${world.width}" height="${world.height}" viewBox="0 0 ${world.width} ${world.height}" aria-hidden="true">${lineHtml}</svg>
    ${hubHtml}
    ${stacksHtml}
    ${workersHtml}
  </div>`;
  applyNetworkTransform();
}
function renderNetwork(stacks){renderTopology(stacks,'network-stage',_viewMode==='corporate'?'logo':'character');}
function activeTopologyStage(){
  return document.querySelector('#pane-network.on .network-stage')||null;
}
function findNetworkWorker(server,container,stage=activeTopologyStage()){
  if(!stage||!container)return null;
  const workers=[...stage.querySelectorAll('.net-worker')];
  return workers.find(el=>(el.dataset.container||'')===container&&(!server||(el.dataset.server||'')===server))||
    workers.find(el=>(el.dataset.container||'')===container)||null;
}
function setNetworkCheckingContainer(server,container,active=true){
  if(active)_networkChecking={server:server||'',container:container||''};
  else if(_networkChecking.container===container&&(!_networkChecking.server||_networkChecking.server===server))_networkChecking={server:'',container:''};
  document.querySelectorAll('.net-worker.checking').forEach(el=>{
    if(!active||el.dataset.container!==container||((server||'')&&(el.dataset.server||'')!==server))el.classList.remove('checking');
  });
  if(active){
    document.querySelectorAll('.network-stage').forEach(stage=>{
      const node=findNetworkWorker(server,container,stage);
      if(node)node.classList.add('checking');
    });
  }
}
function focusedNetworkKey(server,container){
  return `${server||''}::${container||''}`;
}
function setFocusedNetworkChecking(server,container,active=true){
  const key=focusedNetworkKey(server,container);
  if(!container)return;
  if(active)_focusedNetworkChecking[key]=true;
  else{
    if(server)delete _focusedNetworkChecking[key];
    else Object.keys(_focusedNetworkChecking).forEach(k=>{if(k.endsWith(`::${container}`))delete _focusedNetworkChecking[k];});
  }
  document.querySelectorAll('.net-worker.focused-checking').forEach(el=>{
    const match=server
      ? focusedNetworkKey(el.dataset.server||'',el.dataset.container||'')===key
      : (el.dataset.container||'')===container;
    if(!active||!match)el.classList.remove('focused-checking');
  });
  if(active){
    document.querySelectorAll('.network-stage').forEach(stage=>{
      const node=findNetworkWorker(server,container,stage);
      if(node)node.classList.add('focused-checking');
    });
  }
}
function networkElementCenter(el,stage){
  const r=el.getBoundingClientRect();
  const sr=stage.getBoundingClientRect();
  return {x:r.left+r.width/2-sr.left,y:r.top+r.height/2-sr.top};
}
function networkHubForWorker(worker){
  const stackId=worker?.dataset.stackNodeId||'';
  if(!stackId)return null;
  const root=worker.closest('.net-pan-surface')||document;
  const line=[...root.querySelectorAll('.net-backbone line.hub-link')].find(l=>l.dataset.source===stackId||l.dataset.target===stackId);
  if(!line)return null;
  const hubId=line.dataset.source===stackId?line.dataset.target:line.dataset.source;
  return getNetworkNodeEl(hubId,root);
}
function dropRavenScroll(stage,x,y){
  const scroll=document.createElement('img');
  scroll.className='raven-scroll-drop';
  scroll.src='/assets/kingdoms/message-scroll.png';
  scroll.alt='';
  scroll.style.setProperty('--x',`${x.toFixed(1)}px`);
  scroll.style.setProperty('--y',`${y.toFixed(1)}px`);
  stage.appendChild(scroll);
  scroll.addEventListener('animationend',()=>scroll.remove(),{once:true});
  setTimeout(()=>scroll.remove(),1900);
}
function launchRavenFromContainer(server,container){
  const stage=activeTopologyStage();
  if(!stage||!container)return;
  const node=findNetworkWorker(server,container,stage);
  if(!node)return;
  const hub=networkHubForWorker(node);
  const start=networkElementCenter(node,stage);
  const end=hub?networkElementCenter(hub,stage):{x:stage.clientWidth/2,y:stage.clientHeight/2};
  const bird=document.createElement('img');
  bird.className='raven-flight';
  bird.src='/assets/kingdoms/raven-flying.png';
  bird.alt='';
  const dir=end.x>=start.x?1:-1;
  bird.style.setProperty('--sx',`${start.x.toFixed(1)}px`);
  bird.style.setProperty('--sy',`${start.y.toFixed(1)}px`);
  bird.style.setProperty('--ex',`${end.x.toFixed(1)}px`);
  bird.style.setProperty('--ey',`${end.y.toFixed(1)}px`);
  bird.style.setProperty('--start-rot',`${(dir*14).toFixed(1)}deg`);
  bird.style.setProperty('--end-rot',`${(dir*3).toFixed(1)}deg`);
  stage.appendChild(bird);
  setTimeout(()=>{if(stage.isConnected)dropRavenScroll(stage,end.x,end.y+30);},820);
  bird.addEventListener('animationend',()=>bird.remove(),{once:true});
  setTimeout(()=>bird.remove(),1400);
}
function launchFocusedRavenToContainer(server,container){
  const stage=activeTopologyStage();
  if(!stage||!container)return;
  const node=findNetworkWorker(server,container,stage);
  if(!node)return;
  const hub=networkHubForWorker(node);
  const start=hub?networkElementCenter(hub,stage):{x:stage.clientWidth/2,y:stage.clientHeight/2};
  const end=networkElementCenter(node,stage);
  const bird=document.createElement('img');
  bird.className='raven-flight focused';
  bird.src='/assets/kingdoms/raven-flying.png';
  bird.alt='';
  const dir=end.x>=start.x?1:-1;
  bird.style.setProperty('--sx',`${start.x.toFixed(1)}px`);
  bird.style.setProperty('--sy',`${start.y.toFixed(1)}px`);
  bird.style.setProperty('--ex',`${end.x.toFixed(1)}px`);
  bird.style.setProperty('--ey',`${end.y.toFixed(1)}px`);
  bird.style.setProperty('--start-rot',`${(dir*4).toFixed(1)}deg`);
  bird.style.setProperty('--end-rot',`${(dir*16).toFixed(1)}deg`);
  stage.appendChild(bird);
  bird.addEventListener('animationend',()=>bird.remove(),{once:true});
  setTimeout(()=>bird.remove(),1500);
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
  if(!sel)return;
  const cur=sel.value;
  sel.innerHTML='<option value="">All servers</option>'+
    _conns.map(c=>`<option value="${esc(c.name)}"${c.name===cur?' selected':''}>${esc(c.server_name||c.name)}</option>`).join('');
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
    <td style="color:var(--pur);font-size:.76rem;cursor:pointer" onclick="jumpToEvents('${esc(e.server)}','','error')">${esc(e.server_name||evServerDisplay(e.server))}</td>
    <td class="mono" style="color:var(--blu);cursor:pointer" onclick="jumpToEvents('${esc(e.server)}','${esc(e.container_name)}','error')">${esc(e.container_name)}</td>
    <td><span class="${S[e.severity]||'si2'}">${e.severity}</span></td>
    <td class="msg" title="${esc(e.message)}">${esc(e.message)}</td>
  </tr>`).join('');
  document.getElementById('ev-body').innerHTML=`<div class="scroll"><table>
    <thead><tr><th>Time (MT)</th><th>Server</th><th>Container</th><th>Severity</th><th>Message</th></tr></thead>
    <tbody>${rows}</tbody></table></div>`;
}

/* ============================================================
   ADMIN — RETENTION
   ============================================================ */
async function loadRetentionSettings(){
  try{
    const [settings,stats]=await Promise.all([
      fetch('/admin/settings').then(r=>r.json()),
      fetch('/admin/event-stats').then(r=>r.json())
    ]);
    const days=settings.settings?.event_retention_days||'30';
    const el=document.getElementById('retention-days');
    if(el)el.value=days;
    renderRetentionStats(stats);
  }catch(e){
    const el=document.getElementById('retention-stats');
    if(el)el.innerHTML=`<div class="empty">Could not load stats: ${esc(e.message)}</div>`;
  }
}
function renderRetentionStats(d){
  const el=document.getElementById('retention-stats');
  if(!el)return;
  if(!d||d.total_events===undefined){el.innerHTML='<div class="empty">No data.</div>';return;}
  const oldest=d.oldest_event?fmt(d.oldest_event):'—';
  const newest=d.newest_event?fmt(d.newest_event):'—';
  const sev=d.by_severity||{};
  const sevRows=Object.entries(sev).sort((a,b)=>b[1]-a[1]).map(([k,v])=>
    `<tr><td style="padding:4px 10px;text-transform:capitalize">${esc(k)}</td><td style="padding:4px 10px">${v.toLocaleString()}</td></tr>`
  ).join('');
  el.innerHTML=`<div style="display:flex;gap:24px;flex-wrap:wrap;margin-bottom:12px">
    <div><div style="font-size:1.4rem;font-weight:700">${d.total_events.toLocaleString()}</div><div class="muted" style="font-size:.75rem">Total events</div></div>
    <div><div style="font-size:.85rem;font-weight:600">${oldest}</div><div class="muted" style="font-size:.75rem">Oldest event</div></div>
    <div><div style="font-size:.85rem;font-weight:600">${newest}</div><div class="muted" style="font-size:.75rem">Newest event</div></div>
  </div>
  ${sevRows?`<table style="font-size:.8rem"><thead><tr><th style="padding:4px 10px;text-align:left">Severity</th><th style="padding:4px 10px;text-align:left">Count</th></tr></thead><tbody>${sevRows}</tbody></table>`:''}`;
}
async function saveRetentionSettings(){
  const days=(document.getElementById('retention-days')?.value||'').trim();
  const msg=document.getElementById('retention-save-msg');
  if(!days||isNaN(days)||Number(days)<1){if(msg)msg.textContent='Enter a valid number of days (min 1).';return;}
  try{
    await postJson('/admin/settings/event_retention_days',{value:days},'PUT');
    if(msg){msg.textContent='Saved.';setTimeout(()=>{msg.textContent='';},3000);}
  }catch(e){if(msg)msg.textContent=e.message||'Save failed.';}
}
async function pruneEventsNow(){
  const btn=document.getElementById('prune-btn');
  const msg=document.getElementById('prune-msg');
  if(btn)btn.disabled=true;
  if(msg)msg.textContent='Pruning…';
  try{
    const d=await postJson('/admin/prune-events',{});
    if(msg)msg.textContent=`Deleted ${d.deleted} event(s).`;
    loadRetentionSettings();
  }catch(e){
    if(msg)msg.textContent=e.message||'Prune failed.';
  }finally{
    if(btn)btn.disabled=false;
  }
}

/* ============================================================
   ADMIN — AI USAGE
   ============================================================ */
async function loadAiUsage(){
  try{
    const d=await fetch('/admin/ai-usage?days=30').then(r=>r.json());
    renderAiUsageCards(d.totals||{});
    renderAiUsageTable(d.rows||[]);
    renderAiUsageChart(d.daily||[]);
  }catch(e){
    document.getElementById('ai-usage-table').innerHTML=`<div class="empty">Could not load AI usage: ${esc(e.message)}</div>`;
  }
}
function renderAiUsageCards(t){
  const el=document.getElementById('ai-usage-cards');
  if(!el)return;
  const cost=((t.prompt_tokens||0)*0.15/1e6+(t.completion_tokens||0)*0.60/1e6).toFixed(4);
  el.innerHTML=[
    {label:'Total Calls',value:t.calls||0},
    {label:'Prompt Tokens',value:(t.prompt_tokens||0).toLocaleString()},
    {label:'Completion Tokens',value:(t.completion_tokens||0).toLocaleString()},
    {label:'Total Tokens',value:(t.total_tokens||0).toLocaleString()},
    {label:'Est. Cost (USD)',value:'$'+cost},
  ].map(c=>`<div class="card" style="padding:14px 20px;min-width:130px;text-align:center"><div style="font-size:1.4rem;font-weight:700">${esc(String(c.value))}</div><div class="muted" style="font-size:0.78rem;margin-top:4px">${esc(c.label)}</div></div>`).join('');
}
function renderAiUsageTable(rows){
  const el=document.getElementById('ai-usage-table');
  if(!el)return;
  if(!rows.length){el.innerHTML='<div class="empty">No AI calls recorded yet.</div>';return;}
  const grouped={};
  rows.forEach(r=>{
    const k=r.agent_id+'|'+r.model;
    if(!grouped[k])grouped[k]={agent_id:r.agent_id,model:r.model,calls:0,prompt_tokens:0,completion_tokens:0,total_tokens:0};
    grouped[k].calls+=r.calls;
    grouped[k].prompt_tokens+=r.prompt_tokens;
    grouped[k].completion_tokens+=r.completion_tokens;
    grouped[k].total_tokens+=r.total_tokens;
  });
  const agg=Object.values(grouped).sort((a,b)=>b.total_tokens-a.total_tokens);
  el.innerHTML=`<table style="width:100%;border-collapse:collapse;font-size:0.82rem">
    <thead><tr style="border-bottom:1px solid var(--border,#333)">${['Agent','Model','Calls','Prompt Tokens','Completion Tokens','Total Tokens','Est. Cost'].map(h=>`<th style="text-align:left;padding:6px 10px;color:var(--muted)">${h}</th>`).join('')}</tr></thead>
    <tbody>${agg.map(r=>{
      const cost=((r.prompt_tokens||0)*0.15/1e6+(r.completion_tokens||0)*0.60/1e6).toFixed(4);
      return `<tr style="border-bottom:1px solid var(--border,#222)"><td style="padding:6px 10px">${esc(r.agent_id)}</td><td style="padding:6px 10px">${esc(r.model)}</td><td style="padding:6px 10px">${r.calls}</td><td style="padding:6px 10px">${r.prompt_tokens.toLocaleString()}</td><td style="padding:6px 10px">${r.completion_tokens.toLocaleString()}</td><td style="padding:6px 10px">${r.total_tokens.toLocaleString()}</td><td style="padding:6px 10px">$${cost}</td></tr>`;
    }).join('')}</tbody></table>`;
}
function renderAiUsageChart(daily){
  const canvas=document.getElementById('ai-usage-chart');
  if(!canvas)return;
  const cssW=Math.max(520,Math.floor(canvas.getBoundingClientRect().width||canvas.parentElement?.clientWidth||1000));
  const cssH=340;
  const dpr=Math.max(1,window.devicePixelRatio||1);
  const backingW=Math.round(cssW*dpr),backingH=Math.round(cssH*dpr);
  if(canvas.width!==backingW||canvas.height!==backingH){
    canvas.width=backingW;
    canvas.height=backingH;
  }
  canvas.style.height=cssH+'px';
  const ctx=canvas.getContext('2d');
  ctx.setTransform(dpr,0,0,dpr,0,0);
  const W=cssW,H=cssH;
  ctx.clearRect(0,0,W,H);
  if(!daily.length)return;
  const rawMax=Math.max(...daily.map(d=>d.total_tokens),0);
  const maxTokens=rawMax>0?rawMax:1;

  function fmtCompact(n){
    if(n>=1e6)return (n/1e6).toFixed(1)+'M';
    if(n>=1e3)return (n/1e3).toFixed(1)+'K';
    return String(n);
  }

  const pad={l:92,r:24,t:24,b:78};
  const chartW=W-pad.l-pad.r;
  const chartH=H-pad.t-pad.b;
  const step=chartW/daily.length;
  const barW=Math.max(5,Math.floor(step*.62));

  const yTicks=[0,0.25,0.5,0.75,1];
  ctx.strokeStyle='#30363d';
  ctx.lineWidth=1;
  ctx.beginPath();
  ctx.moveTo(pad.l,pad.t);
  ctx.lineTo(pad.l,pad.t+chartH);
  ctx.lineTo(W-pad.r,pad.t+chartH);
  ctx.stroke();

  ctx.textAlign='right';
  ctx.textBaseline='middle';
  ctx.font='700 13px sans-serif';
  yTicks.forEach(pct=>{
    const val=Math.round(maxTokens*pct);
    const yPos=pad.t+chartH-(pct*chartH);
    ctx.strokeStyle=pct===0?'#30363d':'#263241';
    ctx.lineWidth=1;
    ctx.beginPath();
    ctx.moveTo(pad.l,yPos);
    ctx.lineTo(W-pad.r,yPos);
    ctx.stroke();
    ctx.fillStyle='#c9d1d9';
    ctx.fillText(fmtCompact(val),pad.l-12,yPos);
  });

  ctx.textAlign='center';
  ctx.textBaseline='top';
  daily.forEach((d,i)=>{
    const h=Math.floor((d.total_tokens/maxTokens)*chartH);
    const x=Math.round(pad.l+i*step+(step-barW)/2);
    const y=pad.t+chartH-h;
    ctx.fillStyle='#3b82f6';
    ctx.fillRect(x,y,barW,h);
    if(i%5===0||i===daily.length-1){
      const label=(d.date||'').slice(5);
      ctx.fillStyle='#e2e8f0';
      ctx.font='700 13px sans-serif';
      ctx.save();
      ctx.translate(x+barW/2,pad.t+chartH+18);
      ctx.rotate(-Math.PI/6);
      ctx.textAlign='right';
      ctx.textBaseline='middle';
      ctx.fillText(label,0,0);
      ctx.restore();
    }
  });

  ctx.save();
  ctx.translate(14,pad.t+chartH/2);
  ctx.rotate(-Math.PI/2);
  ctx.textAlign='center';
  ctx.fillStyle='#e2e8f0';
  ctx.font='700 13px sans-serif';
  ctx.fillText('Tokens',0,0);
  ctx.restore();
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
      const logo=c.logo_data?`<img class="kingdom-logo" src="${esc(c.logo_data)}" alt="">`:'';
      const display=c.server_name||c.name;
      return `<tr${c.enabled?'':' style="opacity:.5"'}>
        <td style="font-weight:500"><div style="display:flex;align-items:center;gap:8px;min-width:0">${logo}<span>${esc(display)}</span></div></td>
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
  document.getElementById('f-server-name').value=c?(c.server_name||''):'';
  _connLogoDraft=c?(c.logo_data||''):'';
  showLogoPreview('f-logo-preview',_connLogoDraft);
  document.getElementById('f-logo').value='';
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
    enabled:document.getElementById('f-en').checked,poll_interval_seconds:iv?parseInt(iv):null,
    server_name:document.getElementById('f-server-name').value.trim(),logo_data:_connLogoDraft};
  try{
    const r=await fetch(_editId?`/connections/${_editId}`:'/connections',
      {method:_editId?'PUT':'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
    if(!r.ok)throw new Error(await r.text());
    closeDlg();await loadAll();
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
   ORCHESTRATION
   ============================================================ */
function orchAgent(id){
  return _orch.agents.find(a=>a.id===id)||{id:id||'',name:id||'system',role:'agent',icon:'/assets/characters/agent-scout.png',logo_data:BLACK_LOGO_SRC,trust_mode:'recommend_only',enabled:true};
}
function orchVal(id){return (document.getElementById(id)?.value||'').trim();}
function orchChecked(id){return !!document.getElementById(id)?.checked;}
function optionHtml(items,selected=''){
  return (items||[]).map(a=>`<option value="${esc(a.id)}"${a.id===selected?' selected':''}>${esc(a.name||a.id)}</option>`).join('');
}
async function postJson(url,body,method='POST'){
  const r=await fetch(url,{method,headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
  const d=await r.json().catch(()=>({}));
  if(r.status===401){setAuthView(null);throw new Error('Login required');}
  if(!r.ok)throw new Error(d.detail||d.message||'Request failed');
  return d;
}
async function loadOrchestration(){
  try{
    _orch=await fetch('/orchestration/summary').then(r=>r.json());
    renderOrchestration();
    renderInstructionProfiles();
  }catch(e){
    const el=document.getElementById('orch-agents');
    if(el)el.innerHTML=`<div class="empty">Could not load orchestration: ${esc(e.message||'request failed')}</div>`;
  }
}
function fillOrchSelects(){
  ['skill-agent','approval-agent','learning-agent'].forEach(id=>{
    const el=document.getElementById(id);
    if(el)el.innerHTML=optionHtml(_orch.agents,el.value);
  });
  const runbookSelect=document.getElementById('runbook-id');
  if(runbookSelect){
    const cur=runbookSelect.value;
    runbookSelect.innerHTML=(_orch.runbooks||[]).map(r=>`<option value="${esc(r.item_id)}"${r.item_id===cur?' selected':''}>${esc(r.name||r.item_id)}</option>`).join('');
  }
  const chatTarget=document.getElementById('chat-agent-target');
  if(chatTarget){
    const cur=chatTarget.value||'orc-orchestrator';
    chatTarget.innerHTML=(_orch.agents||[]).filter(a=>a.enabled).map(a=>`<option value="${esc(a.id)}"${a.id===cur?' selected':''}>${esc(a.name||a.id)}</option>`).join('');
  }
}
function profileSkillIds(agent){
  const skills=agent?.profile?.allowed_skills||[];
  return Array.isArray(skills)?skills.map(s=>String(s).trim()).filter(Boolean):[];
}
function profileNorm(value){
  return String(value||'').toLowerCase().replace(/[^a-z0-9]+/g,'-').replace(/^-+|-+$/g,'');
}
function syncProfileAgentFilter(agents){
  const el=document.getElementById('profile-agent-filter');
  if(!el)return 'all';
  const current=el.value||_profileAgentFilter||'all';
  const options=['<option value="all">All agents</option>'].concat(
    agents.map(agent=>`<option value="${esc(agent.id)}">${esc(agent.name||agent.id)}</option>`)
  ).join('');
  if(el.dataset.options!==options){
    el.innerHTML=options;
    el.dataset.options=options;
  }
  const valid=current==='all'||agents.some(agent=>agent.id===current);
  el.value=valid?current:'all';
  _profileAgentFilter=el.value||'all';
  return _profileAgentFilter;
}
function profileSurfaceIdsForAgent(agent){
  const plane=String(agent.profile?.plane||agent.role||'').toLowerCase();
  const id=agent.id;
  const surfaceIds=[];
  if(id==='raven'||id==='orc-orchestrator')surfaceIds.push('message-bus');
  if(id==='sage'||id==='oracle'||id==='orc-orchestrator'||plane.includes('memory'))surfaceIds.push('memory');
  if(['orc-orchestrator','oracle','raven','gate-keeper','executioner'].includes(id))surfaceIds.push('runbooks');
  if(['orc-orchestrator','oracle','gate-keeper','executioner'].includes(id))surfaceIds.push('approvals');
  if(['orc-orchestrator','sage','executioner'].includes(id))surfaceIds.push('tools');
  return [...new Set(surfaceIds)];
}
function profileTooltip(title,rows=[]){
  const root=document.createElement('div');
  const name=document.createElement('strong');
  name.textContent=title||'Profile item';
  root.appendChild(name);
  const cleanRows=rows.map(row=>String(row||'').trim()).filter(Boolean);
  if(cleanRows.length){
    const body=document.createElement('div');
    body.className='profile-tooltip-body';
    cleanRows.forEach(row=>{
      const line=document.createElement('div');
      line.textContent=row;
      body.appendChild(line);
    });
    root.appendChild(body);
  }
  return root;
}
function profileColor(kind){
  return {
    agent:{background:'#142033',border:'#58a6ff'},
    skills:{background:'#56d364',border:'#173b25'},
    memory:{background:'#38bdf8',border:'#164e63'},
    runbooks:{background:'#3b82f6',border:'#1d4ed8'},
    tools:{background:'#a371f7',border:'#5b21b6'},
    approvals:{background:'#f59e0b',border:'#92400e'},
    message:{background:'#94a3b8',border:'#475569'},
    missing:{background:'#8b949e',border:'#30363d'},
  }[kind]||{background:'#8b949e',border:'#30363d'};
}
function profileLabelColor(kind){
  return {
    skills:'#07130d',
    memory:'#06111a',
    runbooks:'#f0f6fc',
    tools:'#f8f2ff',
    approvals:'#111827',
    message:'#0f172a',
    missing:'#f0f6fc',
  }[kind]||'#f0f6fc';
}
function profileNetworkNode(id,kind,title,rows=[],extra={}){
  const color=profileColor(kind);
  const label=extra.label||'';
  return {
    id,
    shape:extra.shape||'dot',
    size:extra.size||8,
    label,
    title:profileTooltip(title,rows),
    font:label?{size:extra.fontSize||10,color:extra.fontColor||profileLabelColor(kind),face:'Segoe UI, sans-serif',bold:{color:extra.fontColor||profileLabelColor(kind)},strokeWidth:0,multi:false}:{size:0,color:'transparent'},
    color:{background:color.background,border:color.border,highlight:{background:color.background,border:'#f0f6fc'},hover:{background:color.background,border:'#f0f6fc'}},
    borderWidth:extra.borderWidth||2,
    mass:extra.mass||1,
    x:extra.x,
    y:extra.y,
  };
}
function profileAgentNetworkNode(agent,index,total){
  const angle=total<=1?Math.PI:Math.PI*1.5+(index/Math.max(total,1))*Math.PI;
  const x=total<=1?-220:-220+Math.cos(angle)*78;
  const y=total<=1?0:Math.sin(angle)*145;
  const profile=agent.profile||{};
  return {
    id:`agent:${agent.id}`,
    shape:'circularImage',
    image:agent.icon||agentArt(agent),
    brokenImage:'/assets/characters/agent-scout.png',
    size:24,
    label:'',
    title:profileTooltip(agent.name||agent.id,[profile.plane||agent.role||'agent',profile.purpose||'']),
    font:{size:0,color:'transparent'},
    color:{border:'#58a6ff',background:'#142033',highlight:{border:'#f0f6fc'},hover:{border:'#f0f6fc'}},
    borderWidth:3,
    mass:3,
    x,
    y,
  };
}
function profileNetworkEdge(from,to,kind='default'){
  const colors={default:'#3d4656',missing:'#8b949e',hub:'#586174'};
  return {from,to,color:{color:colors[kind]||colors.default,highlight:'#f0f6fc',hover:'#f0f6fc'},width:kind==='hub'?1.6:1,smooth:{type:'continuous'},selectionWidth:2,hoverWidth:2};
}
function renderInstructionProfiles(){
  const canvas=document.getElementById('profile-map-canvas');
  const cards=document.getElementById('profile-card-grid');
  if(!canvas&&!cards)return;
  const coreOrder=['orc-orchestrator','raven','oracle','sage','gate-keeper','executioner'];
  const allAgents=[...(_orch.agents||[])].sort((a,b)=>{
    const ai=coreOrder.indexOf(a.id), bi=coreOrder.indexOf(b.id);
    if(ai!==-1||bi!==-1)return (ai===-1?99:ai)-(bi===-1?99:bi);
    return String(a.name||a.id).localeCompare(String(b.name||b.id));
  });
  const selectedAgent=syncProfileAgentFilter(allAgents);
  const agents=selectedAgent==='all'?allAgents:allAgents.filter(agent=>agent.id===selectedAgent);
  if(!agents.length){
    if(canvas)canvas.innerHTML='<div class="profile-empty">No agent profiles found.</div>';
    if(cards)cards.innerHTML='<div class="profile-empty">No agent profiles found.</div>';
    return;
  }

  const skillLookup=new Map();
  (_orch.skills||[]).forEach(skill=>{
    skillLookup.set(profileNorm(skill.item_id),skill);
    skillLookup.set(profileNorm(skill.name),skill);
  });
  const declared=new Map();
  agents.forEach(agent=>{
    profileSkillIds(agent).forEach(raw=>{
      const key=profileNorm(raw);
      if(!key)return;
      const found=skillLookup.get(key);
      if(!declared.has(key)){
        declared.set(key,{
          id:key,
          label:found?.name||raw,
          sub:found?`${found.role_or_category||'skill'} skill`:'declared in profile only',
          path:found?.path||'',
          detail:found?.approval_required?'Approval required':'Available skill',
          missing:!found,
          type:'skill',
        });
      }
    });
  });
  if(!declared.size){
    (_orch.skills||[]).slice(0,12).forEach(skill=>{
      declared.set(profileNorm(skill.item_id),{
        id:profileNorm(skill.item_id),
        label:skill.name||skill.item_id,
        sub:`${skill.role_or_category||'skill'} skill`,
        path:skill.path||'',
        detail:skill.approval_required?'Approval required':'Available skill',
        missing:false,
        type:'skill',
      });
    });
  }

  const skillNodes=[...declared.values()].sort((a,b)=>a.label.localeCompare(b.label));
  const visibleSurfaceIds=new Set();
  agents.forEach(agent=>profileSurfaceIdsForAgent(agent).forEach(id=>visibleSurfaceIds.add(id)));
  const memoryItems=[
    {id:'memory-episodic',label:'Episodic',sub:'who, what, where, when, why',path:'memory/episodic/',surface:'memory',type:'memory'},
    {id:'memory-semantic',label:'Semantic',sub:'facts, notes, policies',path:'memory/semantic/',surface:'memory',type:'memory'},
    {id:'memory-procedural',label:'Procedural',sub:'runbooks, escalation, rollback',path:'memory/procedural/',surface:'memory',type:'memory'},
    {id:'memory-evaluative',label:'Evaluative',sub:'success rates and improvements',path:'memory/evaluative/',surface:'memory',type:'memory'},
  ];
  const runbookItems=(_orch.runbooks||[]).map(item=>({
    id:`runbook-${profileNorm(item.item_id)}`,
    label:item.name||item.item_id,
    sub:`${item.role_or_category||'runbook'} runbook`,
    path:item.path||'',
    detail:item.approval_required?'Approval required':'Can run within policy',
    surface:'runbooks',
    type:'runbook',
  }));
  const toolItems=(_orch.tools||[]).map(item=>({
    id:`tool-${profileNorm(item.item_id)}`,
    label:item.name||item.item_id,
    sub:`${item.role_or_category||'worker'} tool`,
    path:item.path||'',
    detail:item.approval_required?'Promotion or execution gated':'Worker pool tool',
    surface:'tools',
    type:'tool',
  }));
  const policyItems=[
    {id:'message-bus',label:'Message Bus',sub:'agent messages and routing',path:'agent_messages',surface:'message-bus',type:'message'},
    {id:'approval-matrix',label:'Approval Matrix',sub:'Gatekeeper policy checks',path:'docs/approval-matrix.md',surface:'approvals',type:'approval'},
  ];
  const allResources=[...memoryItems,...runbookItems,...toolItems,...policyItems];
  const resources=selectedAgent==='all'
    ? allResources
    : allResources.filter(item=>visibleSurfaceIds.has(item.surface));

  if(canvas){
    if(!window.vis||!window.vis.Network){
      canvas.innerHTML='<div class="profile-empty">Interactive map library did not load. Check network access for vis-network.</div>';
    }else{
      const categoryDefs=[
        {id:'skills',label:'Skills',nodeLabel:'Skills',kind:'skills',surface:'skills',description:'Skills this agent can use',x:-60,y:-110},
        {id:'memory',label:'Memory',nodeLabel:'Memory',kind:'memory',surface:'memory',description:'Sage memory classes and knowledge stores',x:90,y:-110},
        {id:'runbooks',label:'Runbooks',nodeLabel:'Runbooks',kind:'runbooks',surface:'runbooks',description:'Repeatable operating procedures',x:-60,y:18},
        {id:'tools',label:'Worker Tools',nodeLabel:'Tools',kind:'tools',surface:'tools',description:'Disposable worker-pool tools',x:90,y:18},
        {id:'approvals',label:'Policy and Approvals',nodeLabel:'Policy',kind:'approvals',surface:'approvals',description:'Gatekeeper policy and approval checks',x:-60,y:145},
        {id:'message-bus',label:'Message Bus',nodeLabel:'Bus',kind:'message',surface:'message-bus',description:'Agent message routing',x:90,y:145},
      ];
      const activeCategories=categoryDefs.filter(cat=>{
        if(cat.surface==='skills')return skillNodes.length>0;
        return resources.some(item=>item.surface===cat.surface);
      });
      const nodes=[];
      const edges=[];
      agents.forEach((agent,index)=>{
        nodes.push(profileAgentNetworkNode(agent,index,agents.length));
      });
      const itemBuckets={skills:skillNodes};
      activeCategories.forEach(cat=>{
        if(cat.surface!=='skills')itemBuckets[cat.id]=resources.filter(item=>item.surface===cat.surface);
      });
      activeCategories.forEach(cat=>{
        const itemCount=(itemBuckets[cat.id]||[]).length;
        nodes.push(profileNetworkNode(
          `cat:${cat.id}`,
          cat.kind,
          cat.label,
          [cat.description, `${itemCount} ${itemCount===1?'item':'items'}`],
          {shape:'circle',label:cat.nodeLabel,size:22,mass:2.6,x:cat.x,y:cat.y,borderWidth:3,fontSize:10}
        ));
      });
      activeCategories.forEach(cat=>{
        const items=itemBuckets[cat.id]||[];
        const radius=cat.id==='skills'?74:52;
        items.forEach((item,index)=>{
          const angle=(Math.PI*2*index/Math.max(items.length,1))-(Math.PI/2);
          const x=cat.x+Math.cos(angle)*radius;
          const y=cat.y+Math.sin(angle)*radius;
          const kind=item.missing?'missing':cat.kind;
          const itemId=`item:${cat.id}:${item.id}`;
          nodes.push(profileNetworkNode(itemId,kind,item.label,[item.sub,item.path,item.detail],{size:cat.id==='skills'?8:7,mass:0.65,x,y,borderWidth:2}));
          edges.push(profileNetworkEdge(`cat:${cat.id}`,itemId,item.missing?'missing':'hub'));
        });
      });
      agents.forEach(agent=>{
        const agentId=`agent:${agent.id}`;
        if(skillNodes.length)edges.push(profileNetworkEdge(agentId,'cat:skills','hub'));
        profileSurfaceIdsForAgent(agent).forEach(surfaceId=>{
          if(activeCategories.some(cat=>cat.surface===surfaceId))edges.push(profileNetworkEdge(agentId,`cat:${surfaceId}`,'hub'));
        });
      });
      if(_profileNetwork){
        try{_profileNetwork.destroy();}catch{}
        _profileNetwork=null;
      }
      canvas.innerHTML='';
      _profileNetwork=new vis.Network(
        canvas,
        {nodes:new vis.DataSet(nodes),edges:new vis.DataSet(edges)},
        {
          autoResize:true,
          nodes:{font:{size:0,color:'transparent'},shadow:false},
          edges:{width:1,color:{color:'#3d4656',opacity:.55},smooth:{type:'dynamic',roundness:.25}},
          interaction:{hover:true,tooltipDelay:120,dragNodes:true,dragView:true,zoomView:true,multiselect:false,navigationButtons:false},
          physics:{
            enabled:true,
            solver:'forceAtlas2Based',
            forceAtlas2Based:{gravitationalConstant:-44,centralGravity:.026,springLength:78,springConstant:.09,damping:.36,avoidOverlap:.46},
            stabilization:{enabled:true,iterations:140,fit:true}
          }
        }
      );
      _profileNetwork.once('stabilized',()=>_profileNetwork.fit({animation:{duration:420,easingFunction:'easeInOutQuad'}}));
    }
  }
  if(cards){
    cards.innerHTML=agents.map(agent=>{
      const profile=agent.profile||{};
      const skills=profileSkillIds(agent);
      const chips=skills.length?skills.map(skill=>`<span class="profile-skill-chip">${esc(skill)}</span>`).join(''):'<span class="profile-skill-chip">No declared skills</span>';
      const meta=[profile.plane||agent.role||'agent', profile.governance_boundary||agent.trust_mode||'', profile.autonomy_level?`L${profile.autonomy_level}`:''].filter(Boolean).join(' / ');
      const remapped=Array.isArray(profile.remapped_capabilities)?profile.remapped_capabilities:[];
      const remapHtml=remapped.length?`<div class="profile-card-purpose">Includes capability profiles: ${remapped.map(item=>esc(item.name||item.id)).join(', ')}.</div>`:'';
      return `<div class="profile-card">
        <div class="profile-card-head">
          <img src="${esc(agentArt(agent))}" alt="">
          <div style="min-width:0">
            <div class="profile-card-name" title="${esc(agent.name||agent.id)}">${esc(agent.name||agent.id)}</div>
            <div class="profile-card-meta">${esc(meta)}${profile.definition_path?` / ${esc(profile.definition_path)}`:''}</div>
          </div>
        </div>
        <div class="profile-card-purpose">${esc(profile.purpose||'No purpose statement declared.')}</div>
        ${remapHtml}
        <div class="profile-chip-row">${chips}</div>
      </div>`;
    }).join('');
  }
}
function renderAgents(){
  const el=document.getElementById('orch-agents');
  if(!el)return;
  if(!_orch.agents.length){el.innerHTML='<div class="empty">No agents registered.</div>';return;}
  const canAdmin=_currentUser?.role==='admin';
  el.innerHTML=_orch.agents.map(a=>`
    <div class="agent-card" data-agent-id="${esc(a.id)}">
      <img class="agent-avatar ${_viewMode==='corporate'?'corp':''}" src="${esc(agentArt(a))}" alt="">
      <div style="min-width:0;flex:1">
        <div class="agent-name" title="${esc(a.name)}">${esc(a.name)}</div>
        <div class="agent-role" title="${esc(a.role)}">${esc(a.role)}</div>
        <div class="agent-capabilities">Trust: ${esc(a.trust_mode||'recommend_only')} · ${a.enabled?'Enabled':'Disabled'} · Uses skills, approvals, and learning</div>
        <div class="agent-controls">
          <select class="trust-select" onchange="setAgentTrustFromEl(this)" ${canAdmin?'':'disabled'}>
            <option value="recommend_only"${a.trust_mode==='recommend_only'?' selected':''}>recommend only</option>
            <option value="approval_required"${a.trust_mode==='approval_required'?' selected':''}>approval required</option>
            <option value="autonomous"${a.trust_mode==='autonomous'?' selected':''}>autonomous</option>
          </select>
          <label class="agent-enabled"><input type="checkbox" ${a.enabled?'checked':''} onchange="setAgentTrustFromEl(this)" ${canAdmin?'':'disabled'}> enabled</label>
          ${canAdmin?`<button class="btns" type="button" data-agent-id="${esc(a.id)}" onclick="openAgentFromEl(this)" style="margin-left:auto">Edit</button>`:''}
        </div>
      </div>
    </div>`).join('');
}
function setAgentEditMode(agentId){
  _agentEditId=agentId||'';
  const save=document.getElementById('agent-save-btn');
  const cancel=document.getElementById('agent-cancel-btn');
  const idField=document.getElementById('agent-id');
  const title=document.getElementById('agent-builder-title');
  if(save)save.textContent=_agentEditId?'Save Agent':'Create Agent';
  if(cancel)cancel.style.display=_agentEditId?'':'none';
  if(idField)idField.disabled=!!_agentEditId;
  if(title)title.textContent=_agentEditId?'Edit Agent':'Agent Builder';
}
function resetAgentForm(){
  ['agent-name','agent-id','agent-role','agent-purpose','agent-skills','agent-rules'].forEach(id=>{
    const el=document.getElementById(id);
    if(el){el.value='';el.disabled=false;}
  });
  const risk=document.getElementById('agent-risk');
  const approval=document.getElementById('agent-approval');
  if(risk)risk.value='low';
  if(approval)approval.checked=false;
  _agentDraftIcon='/assets/characters/agent-scout.png';
  _agentLogoDraft='';
  renderAgentBuilderChoices();
  setAgentEditMode('');
}
function fillAgentForm(a){
  const set=(id,val)=>{const el=document.getElementById(id);if(el)el.value=val||'';};
  set('agent-name', a.name||'');
  set('agent-id', a.id||'');
  set('agent-role', a.role||'');
  set('agent-purpose', a.purpose||'');
  set('agent-skills', a.allowed_skills||'');
  set('agent-rules', a.rules||'');
  const risk=document.getElementById('agent-risk');
  if(risk)risk.value=a.risk_level||'low';
  const approval=document.getElementById('agent-approval');
  if(approval)approval.checked=!!a.approval_required;
  if(a.icon){_agentDraftIcon=a.icon;}
  if(a.logo_data){_agentLogoDraft=a.logo_data;}
  renderAgentBuilderChoices();
  setAgentEditMode(a.id);
  const meta=document.getElementById('agent-role-meta');
  if(meta){
    meta.textContent='Role says who the agent is and what kind of decisions it can make. Skills, approvals, and learning are visible because ORC owns the workflow, not the agent alone.';
  }
  showOrchTab('agents');
  document.getElementById('agent-name')?.focus();
}
async function openAgentFromEl(btn){
  const agentId=btn?.dataset?.agentId||'';
  if(!agentId)return;
  const original=btn.textContent;
  btn.textContent='…';btn.disabled=true;
  try{
    const r=await fetch(`/orchestration/agents/${encodeURIComponent(agentId)}`);
    const d=await r.json().catch(()=>({}));
    if(r.status===401){setAuthView(null);throw new Error('Login required');}
    if(!r.ok)throw new Error(d.detail||d.message||'Could not load agent');
    fillAgentForm(d);
  }catch(e){alert('Open agent failed: '+e.message);}
  finally{btn.textContent=original;btn.disabled=false;}
}
function renderSkills(){
  const el=document.getElementById('orch-skills');
  if(!el)return;
  if(!_orch.skills.length){el.innerHTML='<div class="empty">No skills registered.</div>';return;}
  el.innerHTML=_orch.skills.map(s=>`
    <div class="skill-row ${s.item_id===_skillEditId?'selected':''}">
      <div class="skill-head">
        <div class="skill-title" title="${esc(s.name)}">${esc(s.name)}</div>
        <div class="skill-actions">
          <span class="status-chip ${s.approval_required?'pending':'approved'}">${s.approval_required?'gated':'available'}</span>
          <button class="btns" type="button" data-skill-id="${esc(s.item_id)}" onclick="openSkillFromEl(this)">File</button>
        </div>
      </div>
      <div class="skill-meta"><span>${esc(s.role_or_category||'unknown')}</span><span>${esc(s.version||'0.0.0')}</span><span>${esc(s.path||'')}</span></div>
    </div>`).join('');
}
const CHAT_COLLAPSE_LEN=280;
function chatBubbleText(id,text){
  if(!text)return '<div class="chat-text"><em class="muted">—</em></div>';
  if(text.length<=CHAT_COLLAPSE_LEN)return `<div class="chat-text">${esc(text)}</div>`;
  return `<div class="chat-text" id="ct-${id}"><span class="chat-short">${esc(text.slice(0,CHAT_COLLAPSE_LEN))}<span class="muted">…</span></span><span class="chat-full" style="display:none">${esc(text)}</span><br><button class="chat-expand-btn btns" style="margin-top:4px;font-size:0.75rem" onclick="toggleChatExpand(${id})">▸ Show more</button></div>`;
}
function splitAgentChatText(m){
  const raw=(m?.summary||'').trim();
  const detailFromPayload=(m?.payload&&typeof m.payload==='object')?String(m.payload.detail||m.payload.response||m.payload.rationale||m.payload.note||'').trim():'';
  const clean=raw.replace(/^Plain-English summary:\\s*/i,'').replace(/^Summary:\\s*/i,'').replace(/\\s+/g,' ').trim();
  const firstSentence=clean.split(/(?<=[.!?])\\s+/)[0]||clean;
  const short=firstSentence.length>220?firstSentence.slice(0,217)+'…':firstSentence;
  const detail=detailFromPayload||clean;
  return {short:short||clean||'—', detail:detail!==short?detail:''};
}
function chatBubbleSummaryDetail(id,short,detail){
  const safeShort=short||'—';
  const safeDetail=(detail||'').trim();
  if(!safeDetail||safeDetail===safeShort)return `<div class="chat-text">${esc(safeShort)}</div>`;
  return `<div class="chat-text" id="ct-${id}"><span class="chat-short">${esc(safeShort)}<span class="muted">…</span></span><span class="chat-full" style="display:none">${esc(safeDetail)}</span><br><button class="chat-expand-btn btns" style="margin-top:4px;font-size:0.75rem" onclick="toggleChatExpand(${id})">▸ Show more</button></div>`;
}
function pendingApprovalForMessage(m){
  const approvalId=Number(m?.payload?.approval_id||0);
  if(!approvalId)return null;
  const row=(_orch.approvals||[]).find(a=>Number(a.id)===approvalId);
  if(!row||row.status!=='pending')return null;
  return row;
}
function openApprovalFromChat(approvalId){
  showTab('orchestration');
  showOrchTab('approvals');
  const el=document.querySelector(`#orch-approvals [onclick*="decideApproval(${approvalId},"]`);
  if(el){
    const row=el.closest('.approval-row');
    if(row)row.scrollIntoView({behavior:'smooth',block:'center'});
  }
}
function approvalBubbleHtml(m){
  const row=pendingApprovalForMessage(m);
  if(!row)return '';
  const canApprove=_currentUser?.role==='admin';
  return `<div class="chat-approval-pill">
    <span>Waiting for Approval</span>
    <span class="chat-approval-actions">
      <button class="btns" type="button" onclick="openApprovalFromChat(${row.id})">Open</button>
      ${canApprove?`<button class="btnp" type="button" onclick="decideApproval(${row.id},'approved')">Approve</button>`:''}
    </span>
  </div>`;
}
function chatAgentClass(agentId){
  const raw=String(agentId||'').toLowerCase().replace(/[^a-z0-9]+/g,'-').replace(/^-+|-+$/g,'');
  return raw?`agent-${raw}`:'';
}
function toggleChatExpand(id){
  const el=document.getElementById('ct-'+id);
  if(!el)return;
  const short=el.querySelector('.chat-short');
  const full=el.querySelector('.chat-full');
  const btn=el.querySelector('.chat-expand-btn');
  const expanded=full.style.display!=='none';
  if(short)short.style.display=expanded?'':'none';
  if(full)full.style.display=expanded?'none':'';
  if(btn)btn.textContent=expanded?'▸ Show more':'▴ Show less';
}
function renderChat(){
  const el=document.getElementById('orch-chat-list');
  if(!el)return;
  const msgs=[...(_orch.messages||[])].reverse();
  document.getElementById('orch-message-count').textContent=`${msgs.length} messages`;
  if(!msgs.length){el.innerHTML='<div class="empty">No messages yet. Tell ORC what you need.</div>';return;}
  el.innerHTML=msgs.map(m=>{
    const src=orchAgent(m.source_agent),tgt=orchAgent(m.target_agent);
    const isOperator=m.source_agent==='operator';
    const isSystem=!isOperator&&['sage','raven'].includes(m.source_agent)&&!m.target_agent;
    const right=isOperator;
    const agentCls=isOperator?'operator':chatAgentClass(m.source_agent);
    const cls=[right?'right':'',isSystem?'system':'',agentCls].filter(Boolean).join(' ');
    const sourceLabel=isOperator?(_currentUser?.username||'You'):src.name;
    const targetLabel=m.target_agent?(m.target_agent==='operator'?'operator':tgt.name):'';
    const channelHtml=`${esc(sourceLabel)}${targetLabel?` <span class="chat-arrow">-&gt;</span> ${esc(targetLabel)}`:''}`;
    const avatarSrc=isOperator?'/assets/characters/orc.png':agentArt(src);
    const chat=splitAgentChatText(m);
    const approvalRow=pendingApprovalForMessage(m);
    const bodyHtml=chatBubbleSummaryDetail(m.id,chat.short,chat.detail);
    const approvalHtml=approvalBubbleHtml(m);
    return `<div class="chat-row ${cls}">
      <img class="chat-avatar ${_viewMode==='corporate'&&!isOperator?'corp':''}" src="${esc(avatarSrc)}" alt="">
      <div class="chat-bubble ${approvalRow?'waiting-approval':''}">
        <div class="chat-meta"><span class="chat-channel">${channelHtml}</span><span>${esc(m.message_type)}</span><span>${esc(fmtChatStamp(m.created_at))}</span></div>
        ${approvalHtml}
        ${bodyHtml}
      </div>
    </div>`;
  }).join('');
  el.scrollTop=el.scrollHeight;
}
function renderApprovals(){
  const el=document.getElementById('orch-approvals');
  if(!el)return;
  const approvals=_orch.approvals||[];
  const pending=approvals.filter(a=>a.status==='pending').length;
  document.getElementById('orch-approval-count').textContent=pending;
  if(!approvals.length){el.innerHTML='<div class="empty">No approval requests yet.</div>';return;}
  el.innerHTML=approvals.map(a=>`
    <div class="approval-row">
      <div class="approval-head">
        <div class="approval-title" title="${esc(a.title)}">${esc(a.title)}</div>
        <span class="status-chip ${esc(a.status)}">${esc(a.status)}</span>
      </div>
      <div class="approval-meta"><span>${esc(a.requester_agent)} -> ${esc(a.approver_agent)}</span><span>${esc(a.action_type)}</span><span>${esc(a.risk_level)}</span><span>${esc(fmtShort(a.requested_at))}</span></div>
      <div class="approval-copy">${esc(a.target||'No target')}${a.rationale?' - '+esc(a.rationale):''}</div>
      ${a.status==='pending'?`<div class="approval-actions">
        <button class="btns" onclick="decideApproval(${a.id},'approved')">Approve</button>
        <button class="btnd" onclick="decideApproval(${a.id},'rejected')">Reject</button>
      </div>`:''}
    </div>`).join('');
}
function renderLearnings(){
  const el=document.getElementById('orch-learnings');
  if(!el)return;
  const rows=_orch.learnings||[];
  if(!rows.length){el.innerHTML='<div class="empty">No learning entries yet.</div>';return;}
  el.innerHTML=rows.map(l=>`
    <div class="learning-row">
      <div class="learning-head">
        <div class="learning-title" title="${esc(l.title)}">${esc(l.title)}</div>
        <span class="status-chip approved">${esc(l.outcome)}</span>
      </div>
      <div class="learning-meta"><span>${esc(l.source_agent)}</span><span>${esc(l.incident_ref||'no ref')}</span><span>${esc(l.markdown_path||'')}</span><span>${esc(fmtShort(l.created_at))}</span></div>
      <div class="learning-copy">${esc(l.summary)}</div>
    </div>`).join('');
}
function renderRunbooks(){
  const list=document.getElementById('orch-runbooks');
  const executions=document.getElementById('orch-runbook-executions');
  const path=document.getElementById('orch-runbook-path');
  if(path)path.textContent=_orch.paths?.runbooks||'runbooks';
  if(list){
    const rows=_orch.runbooks||[];
    if(!rows.length)list.innerHTML='<div class="empty">No runbooks registered.</div>';
    else list.innerHTML=rows.map(r=>`
      <div class="learning-row">
        <div class="learning-head">
          <div class="learning-title" title="${esc(r.name)}">${esc(r.name)}</div>
          <span class="status-chip ${r.approval_required?'pending':'approved'}">${r.approval_required?'approval':'ready'}</span>
        </div>
        <div class="learning-meta"><span>${esc(r.item_id)}</span><span>${esc(r.role_or_category||'runbook')}</span><span>${esc(r.path||'')}</span></div>
      </div>`).join('');
  }
  if(executions){
    const rows=_orch.runbook_executions||[];
    if(!rows.length)executions.innerHTML='<div class="empty">No runbook executions yet.</div>';
    else executions.innerHTML=rows.map(r=>`
      <div class="learning-row">
        <div class="learning-head">
          <div class="learning-title" title="${esc(r.title)}">${esc(r.title)}</div>
          <span class="status-chip ${statusChipClass(r.status)}">${esc(r.status)}</span>
        </div>
        <div class="learning-meta"><span>${esc(r.runbook_id)}</span><span>${esc(r.governance)}</span><span>${esc(r.evidence_path||'no evidence yet')}</span></div>
        <div class="learning-copy">${esc(r.result||r.rationale||'No result yet')}</div>
      </div>`).join('');
  }
}
function renderToolPromotions(){
  const el=document.getElementById('orch-tool-promotions');
  if(!el)return;
  const rows=_orch.tool_promotions||[];
  if(!rows.length){el.innerHTML='<div class="empty">No promotion requests yet.</div>';return;}
  el.innerHTML=rows.map(p=>`
    <div class="learning-row">
        <div class="learning-head">
          <div class="learning-title" title="${esc(p.title)}">${esc(p.title)}</div>
        <span class="status-chip ${statusChipClass(p.status)}">${esc(p.status)}</span>
      </div>
      <div class="learning-meta"><span>${esc(p.tool_id)}</span><span>approval ${esc(String(p.approval_id||'none'))}</span><span>${esc(p.artifact_path||'')}</span></div>
      <div class="learning-copy">${esc(p.test_summary||'No test summary')}</div>
      ${p.status!=='promoted'?`<div class="approval-actions"><button class="btns" onclick="promoteTool(${p.id})">Promote Approved Tool</button></div>`:''}
    </div>`).join('');
}
function statusChipClass(status){
  if(['completed','promoted','approved','approved_handoff'].includes(status))return 'approved';
  if(['pending','pending_approval','running','queued','draft'].includes(status))return 'pending';
  return 'rejected';
}
function renderUsers(){
  const el=document.getElementById('setup-users');
  if(!el)return;
  if(_currentUser?.role!=='admin'){el.innerHTML='<div class="empty">Admin access required.</div>';return;}
  if(!_users.length){el.innerHTML='<div class="empty">No users found.</div>';return;}
  el.innerHTML=_users.map(u=>`
    <div class="user-row">
      <div style="min-width:0">
        <div class="user-name">${esc(u.username)}</div>
        <div class="user-role">${esc(u.role)}</div>
      </div>
      <span class="status-chip ${u.enabled?'approved':'rejected'}">${u.enabled?'active':'disabled'}</span>
      <span class="muted small">${esc(fmtShort(u.created_at))}</span>
    </div>`).join('');
}
async function loadUsers(){
  if(_currentUser?.role!=='admin')return;
  try{
    const d=await fetch('/setup/users').then(r=>r.json());
    _users=d.items||[];
    renderUsers();
  }catch(e){
    const el=document.getElementById('setup-users');
    if(el)el.innerHTML='<div class="empty">Could not load users.</div>';
  }
}
function renderOrchestration(){
  document.getElementById('orch-agent-count').textContent=_orch.agents.length;
  document.getElementById('orch-agent-path').textContent=_orch.paths?.agents||'agents';
  document.getElementById('orch-skill-path').textContent=_orch.paths?.skills||'skills';
  document.getElementById('orch-knowledge-path').textContent=_orch.paths?.knowledge||'knowledge';
  const setupTab=document.getElementById('orch-tab-setup');
  if(setupTab)setupTab.style.display=_currentUser?.role==='admin'?'':'none';
  if(_orchTab==='setup'&&_currentUser?.role!=='admin')showOrchTab('chat');
  fillOrchSelects();
  renderAgents();
  renderSkills();
  const skillEditor=document.getElementById('skill-markdown');
  if(skillEditor&&!skillEditor.value&&!_skillEditId)resetSkillForm();
  renderChat();
  renderApprovals();
  renderLearnings();
  renderRunbooks();
  renderToolPromotions();
  if(_orchTab==='setup')loadUsers();
}
async function setAgentTrustFromEl(el){
  const card=el.closest('.agent-card');
  if(!card)return;
  const id=card.dataset.agentId;
  const mode=card.querySelector('.trust-select').value;
  const enabled=card.querySelector('.agent-enabled input').checked;
  try{
    await postJson(`/orchestration/agents/${encodeURIComponent(id)}/trust`,{trust_mode:mode,enabled},'PUT');
    await loadOrchestration();
  }catch(e){alert('Trust update failed: '+e.message);}
}
async function createAgent(){
  const body={agent_name:orchVal('agent-name'),agent_id:_agentEditId||orchVal('agent-id'),role:orchVal('agent-role')||'specialist',risk_level:orchVal('agent-risk')||'low',approval_required:orchChecked('agent-approval'),purpose:orchVal('agent-purpose'),allowed_skills:orchVal('agent-skills'),rules:orchVal('agent-rules'),icon:_agentDraftIcon,logo_data:_agentLogoDraft};
  if(!body.agent_name||!body.purpose){alert('Agent name and purpose are required.');return;}
  try{
    await postJson('/orchestration/agents',body);
    resetAgentForm();
    await loadOrchestration();
  }catch(e){alert((_agentEditId?'Save':'Create')+' agent failed: '+e.message);}
}
function skillDraftMarkdown(){
  const agent=(_orch.agents||[]).find(a=>a.id==='sage')?.id||(_orch.agents?.[0]?.id)||'sage';
  return `# Skill Definition

name: new-skill
id: new-skill
version: 0.1.0
category: automation
risk_level: medium
autonomy_level: 1
governance: yellow
allowed_plane: reasoning
approval_required: true
agent: ${agent}

<!--
Author this file like a developer-maintained runbook for an AI teammate.

Before replacing this guidance
- Keep instructions concise, imperative, and specific to this skill.
- Put stable procedural rules here, not general AI advice.
- Include exact inputs, outputs, approval boundaries, and validation checks.
- Move long reference material into a linked reference file when the skill grows.
- Prefer scripts for fragile or repeated operations that should be deterministic.
- Ask Codex to edit this Markdown directly when the procedure becomes clearer.

Delete this comment when the skill is ready.
-->

## Purpose

State the repeatable capability this skill gives the assigned agent.

## Inputs

- List the information, files, credentials, tools, or context the agent needs.

## Outputs

- List the artifacts, decisions, messages, or changed systems this skill produces.

## Procedure

1. Describe the first action the agent should take.
2. Describe the decision points that change the workflow.
3. Describe the validation step before reporting success.

## Approval Boundary

- State which actions are read-only.
- State which actions require Gate Keeper or human approval.

## Rollback

- List how to undo or stop the work if the procedure changes state.

## Success Criteria

- List the observable conditions that mean the skill worked.

## Audit Requirements

- Record requesting user, deciding agent, approval decision, action target, result, and evidence links.
`;
}
function skillIdFromMarkdown(markdown){
  const idMatch=markdown.match(/^id:\\s*(.+)$/mi);
  const nameMatch=markdown.match(/^name:\\s*(.+)$/mi);
  return (idMatch?.[1]||nameMatch?.[1]||'skill').trim();
}
function setSkillEditMode(skillId){
  _skillEditId=skillId||'';
  const save=document.getElementById('skill-save-btn');
  const reload=document.getElementById('skill-reload-btn');
  if(save)save.textContent=_skillEditId?'Save File':'Create File';
  if(reload)reload.style.display=_skillEditId?'':'none';
  renderSkills();
}
function resetSkillForm(){
  const editor=document.getElementById('skill-markdown');
  if(editor)editor.value=skillDraftMarkdown();
  const title=document.getElementById('skill-editor-title');
  const path=document.getElementById('skill-file-path');
  const meta=document.getElementById('skill-file-meta');
  if(title)title.textContent='New Skill Draft';
  if(path)path.textContent='skills/new-skill/skills.md';
  if(meta)meta.innerHTML='<span class="status-chip pending">draft</span><span class="status-chip">Markdown</span>';
  setSkillEditMode('');
  editor?.focus();
}
function fillSkillForm(skill){
  const editor=document.getElementById('skill-markdown');
  const title=document.getElementById('skill-editor-title');
  const path=document.getElementById('skill-file-path');
  const meta=document.getElementById('skill-file-meta');
  if(editor)editor.value=skill.raw_markdown||'';
  if(title)title.textContent=skill.skill_name||'Skill File';
  if(path)path.textContent=skill.path||'skills.md';
  if(meta){
    meta.innerHTML=[
      `<span class="status-chip">${esc(skill.skill_id||'skill')}</span>`,
      `<span class="status-chip">${esc(skill.category||'automation')}</span>`,
      `<span class="status-chip ${skill.approval_required?'pending':'approved'}">${skill.approval_required?'approval required':'available'}</span>`,
      skill.agent_id?`<span class="status-chip">${esc(skill.agent_id)}</span>`:''
    ].filter(Boolean).join('');
  }
  setSkillEditMode(skill.skill_id||'');
}
async function openSkillFromEl(btn){
  const skillId=btn?.dataset?.skillId||'';
  if(!skillId)return;
  const original=btn.textContent;
  btn.textContent='Opening';
  btn.disabled=true;
  try{
    const r=await fetch(`/orchestration/skills/${encodeURIComponent(skillId)}`);
    const d=await r.json().catch(()=>({}));
    if(r.status===401){setAuthView(null);throw new Error('Login required');}
    if(!r.ok)throw new Error(d.detail||d.message||'Skill could not be opened');
    fillSkillForm(d);
    showOrchTab('skills');
    document.getElementById('skill-markdown')?.focus();
  }catch(e){
    alert('Open skill failed: '+e.message);
  }finally{
    btn.textContent=original;
    btn.disabled=false;
  }
}
async function reloadSkillFile(){
  if(!_skillEditId){resetSkillForm();return;}
  try{
    const r=await fetch(`/orchestration/skills/${encodeURIComponent(_skillEditId)}`);
    const d=await r.json().catch(()=>({}));
    if(r.status===401){setAuthView(null);throw new Error('Login required');}
    if(!r.ok)throw new Error(d.detail||d.message||'Skill could not be reloaded');
    fillSkillForm(d);
  }catch(e){alert('Reload skill failed: '+e.message);}
}
async function saveSkillMarkdown(){
  const editor=document.getElementById('skill-markdown');
  const markdown=editor?.value||'';
  if(!markdown.trim()){alert('Skill Markdown is required.');editor?.focus();return;}
  const btn=document.getElementById('skill-save-btn');
  if(btn){btn.textContent='Saving';btn.disabled=true;}
  try{
    const updating=!!_skillEditId;
    const url=updating?`/orchestration/skills/${encodeURIComponent(_skillEditId)}/markdown`:'/orchestration/skills/markdown';
    const body={skill_id:updating?_skillEditId:skillIdFromMarkdown(markdown),markdown};
    const d=await postJson(url,body,updating?'PUT':'POST');
    fillSkillForm(d);
    await loadOrchestration();
  }catch(e){alert((_skillEditId?'Save':'Create')+' skill failed: '+e.message);}
  finally{
    if(btn)btn.disabled=false;
    setSkillEditMode(_skillEditId);
  }
}
async function sendAgentMessage(){
  const summary=orchVal('msg-summary');
  if(!summary.trim()){document.getElementById('msg-summary').focus();return;}
  const agentId=document.getElementById('chat-agent-target')?.value||'orc-orchestrator';
  const btn=document.getElementById('chat-send-btn');
  const typing=document.getElementById('chat-typing');
  if(btn){btn.disabled=true;btn.textContent='Sending...';}
  if(typing)typing.style.display='block';
  try{
    await postJson(`/orchestration/agents/${encodeURIComponent(agentId)}/chat`,{message:summary});
    document.getElementById('msg-summary').value='';
    await loadOrchestration();
  }catch(e){alert('Send failed: '+e.message);}
  finally{
    if(btn){btn.disabled=false;btn.textContent='Send';}
    if(typing)typing.style.display='none';
  }
}
async function createApproval(){
  const body={title:orchVal('approval-title'),requester_agent:orchVal('approval-agent')||'oracle',action_type:orchVal('approval-action')||'container_refresh',target:orchVal('approval-target'),rationale:orchVal('approval-rationale'),risk_level:'high',requested_by:'operator'};
  if(!body.title||!body.target){alert('Title and target are required.');return;}
  try{
    await postJson('/orchestration/approvals',body);
    ['approval-title','approval-target','approval-rationale'].forEach(id=>document.getElementById(id).value='');
    await loadOrchestration();
  }catch(e){alert('Approval request failed: '+e.message);}
}
async function decideApproval(id,decision){
  const reason=prompt(decision==='approved'?'Approval reason':'Rejection reason')||'';
  try{
    await postJson(`/orchestration/approvals/${id}/decision`,{decision,decided_by:'operator',reason});
    await loadOrchestration();
  }catch(e){alert('Decision failed: '+e.message);}
}
async function createLearning(){
  const body={title:orchVal('learning-title'),source_agent:orchVal('learning-agent')||'sage',incident_ref:orchVal('learning-ref'),outcome:orchVal('learning-outcome')||'proposed',summary:orchVal('learning-summary')};
  if(!body.title||!body.summary){alert('Title and summary are required.');return;}
  try{
    await postJson('/orchestration/learnings',body);
    ['learning-title','learning-ref','learning-summary'].forEach(id=>document.getElementById(id).value='');
    await loadOrchestration();
  }catch(e){alert('Learning entry failed: '+e.message);}
}
async function executeRunbook(){
  const runbookId=orchVal('runbook-id');
  if(!runbookId){alert('Select a runbook first.');return;}
  const body={target:orchVal('runbook-target'),rationale:orchVal('runbook-rationale'),requested_by:'operator'};
  try{
    await postJson(`/orchestration/runbooks/${encodeURIComponent(runbookId)}/execute`,body);
    ['runbook-target','runbook-rationale'].forEach(id=>document.getElementById(id).value='');
    await loadOrchestration();
  }catch(e){alert('Runbook execution failed: '+e.message);}
}
async function createToolPromotion(){
  const body={
    tool_id:orchVal('promotion-tool-id'),
    title:orchVal('promotion-title'),
    source_path:orchVal('promotion-source-path'),
    test_summary:orchVal('promotion-tests'),
    dry_run_summary:orchVal('promotion-dry-run'),
    artifact_markdown:document.getElementById('promotion-artifact')?.value||'',
    requested_by:'operator'
  };
  if(!body.tool_id||!body.title||!body.test_summary||!body.dry_run_summary){alert('Tool ID, title, test summary, and dry run summary are required.');return;}
  try{
    await postJson('/orchestration/tool-promotions',body);
    ['promotion-tool-id','promotion-title','promotion-source-path','promotion-tests','promotion-dry-run','promotion-artifact'].forEach(id=>document.getElementById(id).value='');
    await loadOrchestration();
  }catch(e){alert('Promotion request failed: '+e.message);}
}
async function promoteTool(promotionId){
  try{
    await postJson(`/orchestration/tool-promotions/${promotionId}/promote`,{},'POST');
    await loadOrchestration();
  }catch(e){alert('Tool promotion failed: '+e.message);}
}
async function createUser(){
  const body={username:orchVal('setup-username'),password:orchVal('setup-password'),role:orchVal('setup-role')||'user'};
  const status=document.getElementById('setup-status');
  if(!body.username||!body.password){status.textContent='Username and password are required.';return;}
  try{
    await postJson('/setup/users',body);
    document.getElementById('setup-username').value='';
    document.getElementById('setup-password').value='';
    status.textContent='User created.';
    await loadUsers();
  }catch(e){status.textContent=e.message||'Create user failed.';}
}

/* ============================================================
   RAVEN
   ============================================================ */
function setRF(f){
  _ravenFilter=f;
  ['all','critical','error','warning'].forEach(k=>document.getElementById('rf-'+k).classList.toggle('on',k===(f||'all')));
  renderFeed();
}
function setStatus(icon,text,_state){
  document.getElementById('sl-icon').textContent=icon;
  document.getElementById('sl-txt').textContent=text;
  document.getElementById('raven-sl').className='raven-sl';
}
function issueKey(msg){
  if(msg.type==='issue_event'&&msg.event_id)return `event:${msg.event_id}`;
  return [msg.type,ravenServerKey(msg),msg.server||'',msg.container||'',msg.severity||'',msg.occurred_at||msg.ts||'',msg.message||msg.error||''].join('|');
}
function issueTime(msg){
  return new Date(msg.occurred_at||msg.ts||0).getTime()||0;
}
function pillSeverity(msg){
  if(msg.type==='poll_error')return 'error';
  if(msg.type==='issue_event')return msg.severity||'error';
  if(msg.type==='container_result'){
    const c=newIssueCounts(msg);
    if(c.errors>0)return 'error';
    if(c.warnings>0)return 'warning';
  }
  return '';
}
function addIssuePill(msg,repaint=true){
  if(msg.type==='container_result'&&msg.issue_events)return;
  const key=issueKey(msg);
  if(_issueKeys.has(key))return;
  msg._key=key;
  _issueKeys.add(key);
  _issuePills.push(msg);
  _issuePills.sort((a,b)=>issueTime(a)-issueTime(b));
  while(_issuePills.length>MAX_ISSUE_PILLS){
    const old=_issuePills.shift();
    if(old&&old._key)_issueKeys.delete(old._key);
  }
  if(repaint)renderFeed();
}
function _filteredPills(){
  if(_ravenFilter==='critical')return _issuePills.filter(m=>pillSeverity(m)==='critical');
  if(_ravenFilter==='error')return _issuePills.filter(m=>['critical','error'].includes(pillSeverity(m)));
  if(_ravenFilter==='warning')return _issuePills.filter(m=>pillSeverity(m)==='warning');
  return _issuePills;
}
function eventToRavenIssue(e){
  return {
    type:'issue_event',
    event_id:e.id,
    server:connectionDisplayName(e.server),
    server_key:e.server,
    container:e.container_name,
    severity:e.severity,
    message:e.message,
    occurred_at:e.occurred_at,
    ts:e.occurred_at
  };
}
function issueKeywords(text){
  const stop=new Set(['the','and','for','with','from','this','that','have','has','was','were','error','warning','critical','exception','failed','failure','info','true','false','null','undefined']);
  const words=String(text||'').toLowerCase().match(/[a-z][a-z0-9_-]{3,}/g)||[];
  const picked=[];
  words.forEach(w=>{if(!stop.has(w)&&!picked.includes(w))picked.push(w);});
  return picked.slice(0,3).join(' / ');
}
function issueSummary(msg){
  const words=issueKeywords(msg.message);
  if(words)return words;
  return (msg.message||'').slice(0,80);
}
async function loadRavenBacklog(){
  try{
    const qs=`limit=${MAX_ISSUE_PILLS}&hours=${_windowHours}`;
    const [err,warn]=await Promise.all([
      fetch(`/events?${qs}&severity=error`).then(r=>r.json()),
      fetch(`/events?${qs}&severity=warning`).then(r=>r.json())
    ]);
    const items=[...(err.items||[]),...(warn.items||[])]
      .filter(e=>['critical','error','warning'].includes(e.severity))
      .sort((a,b)=>new Date(a.occurred_at)-new Date(b.occurred_at))
      .slice(-MAX_ISSUE_PILLS);
    items.forEach(e=>addIssuePill(eventToRavenIssue(e),false));
    renderFeed();
  }catch{}
}
function issuePillHtml(msg,opacity,isCurrent){
  const ts=fmtShort(msg.occurred_at||msg.ts);
  const serverKey=ravenServerKey(msg);
  const serverDisplay=ravenServerDisplay(msg);
  const accent=isCurrent?'border-left:3px solid currentColor;padding-left:9px;':'';
  const style=`opacity:${opacity};${accent}`;
  if(msg.type==='issue_event'){
    const sev=msg.severity||'error';
    const cls=sev==='warning'?'p-warn':'p-error';
    const clickSev=sev==='critical'?'critical':(sev==='warning'?'warning':'error');
    return `<div class="pill ${cls}" style="${style};cursor:pointer" data-server="${esc(serverKey)}" data-container="${esc(msg.container||'')}" data-severity="${esc(clickSev)}" onclick="jumpToEventsFromEl(this)">
      <div class="pill-hdr"><span class="pill-cn">${esc(msg.container||'')}</span><span class="pill-sv">${esc(serverDisplay)} - ${esc(sev.toUpperCase())}</span></div>
      <div class="pill-msg">${esc(issueSummary(msg))}</div>
      <div class="pill-ts">${ts}</div>
    </div>`;
  }
  if(msg.type==='poll_error')
    return `<div class="pill p-error" style="${style}">✗ <strong>${esc(serverDisplay)}</strong><div style="font-size:.72rem;margin-top:2px;opacity:.85">${esc(msg.error||'')}</div></div>`;
  if(msg.type==='container_result'){
    const ne=msg.errors||0,nw=msg.warnings||0;
    let cls,detail,sev;
    if(ne>0){cls='p-error';sev='error';detail=`${ne} new error${ne!==1?'s':''}`;if(nw>0)detail+=`, ${nw} new warn`;}
    else{cls='p-warn';sev='warning';detail=`${nw} new warning${nw!==1?'s':''}`;}
    return `<div class="pill ${cls}" style="${style};cursor:pointer" data-server="${esc(serverKey)}" data-container="${esc(msg.container)}" data-severity="${esc(sev)}" onclick="jumpToEventsFromEl(this)">
      <div class="pill-hdr"><span class="pill-cn">${esc(msg.container)}</span><span class="pill-sv">${esc(serverDisplay)}</span></div>
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
  const serverKey=ravenServerKey(msg);
  const serverDisplay=ravenServerDisplay(msg);
  switch(msg.type){
    case 'no_connections': setStatus('—','No connections configured.',false); break;
    case 'queue_ready':{const iv=msg.interval?` · ${msg.interval}s/ctr`:'';setStatus('▶',`Scanning ${msg.containers} containers${iv}`,true);break;}
    case 'container_checking':{
      _hbBucket=1;
      setStatus('🔍',`Checking ${msg.container} on ${serverDisplay}`,true);
      setNetworkCheckingContainer(serverKey,msg.container||'',true);
      launchRavenFromContainer(serverKey,msg.container||'');
      const _nextEl=document.getElementById('raven-next'),_nextTxt=document.getElementById('raven-next-txt');
      if(_nextEl&&_nextTxt){if(msg.next_container){_nextEl.style.display='';_nextTxt.textContent=msg.next_container;}else{_nextEl.style.display='none';}}
      break;
    }
    case 'focused_watch_checking':{
      _hbBucket=1;
      setStatus('🜂',`Focused watch on ${msg.container} in ${serverDisplay}`,true);
      setFocusedNetworkChecking(serverKey,msg.container||'',true);
      launchFocusedRavenToContainer(serverKey,msg.container||'');
      break;
    }
    case 'issue_event':{
      const sev=msg.severity||'error';
      _hbAlertBuf.push({severity:sev,container:msg.container||'',server:serverDisplay,ts:msg.occurred_at||msg.ts||''});
      setStatus('!',`${msg.container||'unknown'} - ${sev}`,true);
      addIssuePill(msg);
      break;
    }
    case 'container_result':{
      setNetworkCheckingContainer(serverKey,msg.container||'',false);
      const ne=msg.errors||0,nw=msg.warnings||0;
      if(!msg.issue_events&&(ne>0||nw>0))_hbAlertBuf.push({severity:ne>0?'error':'warning',container:msg.container||'',server:serverDisplay,ts:msg.ts||''});
      if(ne>0){setStatus('⚠',`${msg.container} · ${ne} new error${ne!==1?'s':''}`,true);addIssuePill(msg);}
      else if(nw>0){setStatus('⚠',`${msg.container} · ${nw} new warning${nw!==1?'s':''}`,true);addIssuePill(msg);}
      else setStatus('✓',`${msg.container} · no new issues`,true);
      break;
    }
    case 'focused_watch_result':{
      setFocusedNetworkChecking(serverKey,msg.container||'',false);
      const ne=msg.errors||0,nw=msg.warnings||0;
      if(ne>0||nw>0)_hbAlertBuf.push({severity:ne>0?'error':'warning',container:msg.container||'',server:serverDisplay,ts:msg.ts||''});
      if(ne>0){setStatus('⚠',`${msg.container} focused watch found ${ne} error${ne!==1?'s':''}`,true);}
      else if(nw>0){setStatus('⚠',`${msg.container} focused watch found ${nw} warning${nw!==1?'s':''}`,true);}
      else setStatus('✓',`${msg.container} focused watch complete`,true);
      break;
    }
    case 'focused_watch_expired':
      if(msg.container)setFocusedNetworkChecking('',msg.container,false);
      setStatus('⌛',`${msg.container||'Focused watch'} expired`,true);
      break;
    case 'poll_error': setStatus('✗',`${serverDisplay}: ${msg.error||'connection failed'}`,true);addIssuePill(msg); break;
    case 'agent_message':
      setStatus('✉',`${msg.source_agent||'agent'} -> ${msg.target_agent||'all'}: ${msg.message_type||'message'}`,true);
      if(document.getElementById('pane-orchestration')?.classList.contains('on'))loadOrchestration();
      break;
  }
}

/* ============================================================
   HEARTBEAT CHART
   ============================================================ */
function resizeCanvas(){const cv=document.getElementById('hb-cv');if(cv)cv.width=cv.offsetWidth||270;}
function _catmullRom(ctx,pts,t){
  t=t===undefined?0.4:t;
  if(pts.length<2)return;
  ctx.moveTo(pts[0].x,pts[0].y);
  for(let i=0;i<pts.length-1;i++){
    const p0=pts[Math.max(0,i-1)],p1=pts[i],p2=pts[i+1],p3=pts[Math.min(pts.length-1,i+2)];
    ctx.bezierCurveTo(p1.x+(p2.x-p0.x)*t,p1.y+(p2.y-p0.y)*t,p2.x-(p3.x-p1.x)*t,p2.y-(p3.y-p1.y)*t,p2.x,p2.y);
  }
}
function drawHb(){
  const cv=document.getElementById('hb-cv');if(!cv)return;
  const ctx=cv.getContext('2d'),w=cv.width,h=cv.height;
  ctx.clearRect(0,0,w,h);
  _hbAlertPoints=[];
  const d=_hbData,N=d.length;if(N<2)return;
  const step=w/(N-1),bot=h-3,top=5;
  const pts=d.map((v,i)=>({x:i*step,y:v>0?top:bot}));
  const grad=ctx.createLinearGradient(0,0,0,h);
  grad.addColorStop(0,'rgba(63,185,80,0.28)');
  grad.addColorStop(1,'rgba(63,185,80,0.03)');
  ctx.beginPath();_catmullRom(ctx,pts);
  ctx.lineTo(pts[N-1].x,h);ctx.lineTo(0,h);ctx.closePath();
  ctx.fillStyle=grad;ctx.fill();
  ctx.beginPath();_catmullRom(ctx,pts);
  ctx.strokeStyle='#3fb950';ctx.lineWidth=1.8;ctx.stroke();
  _hbAlerts.forEach(a=>{
    const idx=N-1-a.age;if(idx<0||idx>=N)return;
    const px=pts[idx].x,py=top;
    const col=a.severity==='warning'?'#d29922':'#f85149';
    _hbAlertPoints.push({
      x:px,
      y:py,
      r:7,
      container:a.container||'Unknown container',
      server:a.server||'',
      severity:a.severity||'error',
      ts:a.ts||'',
    });
    ctx.beginPath();ctx.arc(px,py,5,0,Math.PI*2);
    ctx.fillStyle=col;ctx.fill();
    ctx.strokeStyle='rgba(13,17,23,0.7)';ctx.lineWidth=1.2;ctx.stroke();
  });
}
function tickHb(){
  _hbData.push(_hbBucket?1:0);_hbBucket=0;
  if(_hbData.length>60)_hbData.shift();
  _hbAlertBuf.forEach(a=>_hbAlerts.push({age:0,severity:a.severity,container:a.container||'',server:a.server||'',ts:a.ts||''}));
  _hbAlertBuf=[];
  _hbAlerts.forEach(a=>a.age++);
  _hbAlerts=_hbAlerts.filter(a=>a.age<60);
  drawHb();
}
function hideHbTip(){
  const tip=document.getElementById('hb-tip');
  if(tip)tip.style.display='none';
}
function showHbTip(point,ev){
  const tip=document.getElementById('hb-tip');
  const wrap=document.querySelector('.hb-wrap');
  const cv=document.getElementById('hb-cv');
  if(!tip||!wrap||!cv)return;
  const rect=cv.getBoundingClientRect();
  const wrapRect=wrap.getBoundingClientRect();
  const left=Math.max(8, Math.min(rect.left - wrapRect.left + point.x + 10, wrap.clientWidth - 180));
  const top=Math.max(8, rect.top - wrapRect.top + point.y + 10);
  const ts=point.ts?fmtShort(point.ts):'';
  tip.innerHTML=`<strong>${esc(point.container)}</strong>${point.server?`<div>${esc(point.server)}</div>`:''}${ts?`<div class="muted">${esc(ts)}</div>`:''}`;
  tip.style.left=`${left}px`;
  tip.style.top=`${top}px`;
  tip.style.display='block';
}
function setupHeartbeatHover(){
  const cv=document.getElementById('hb-cv');
  if(!cv||cv.dataset.hoverReady)return;
  cv.dataset.hoverReady='1';
  cv.addEventListener('mousemove',ev=>{
    const rect=cv.getBoundingClientRect();
    const x=(ev.clientX-rect.left)*(cv.width/Math.max(rect.width,1));
    const y=(ev.clientY-rect.top)*(cv.height/Math.max(rect.height,1));
    const hit=_hbAlertPoints.find(p=>Math.hypot(p.x-x,p.y-y)<=p.r);
    if(hit){
      cv.style.cursor='pointer';
      showHbTip(hit,ev);
    }else{
      cv.style.cursor='default';
      hideHbTip();
    }
  });
  cv.addEventListener('mouseleave',()=>{
    cv.style.cursor='default';
    hideHbTip();
  });
}
setInterval(tickHb,1000);

/* ============================================================
   SSE
   ============================================================ */
function connectRaven(){
  const es=new EventSource('/raven/stream');
  document.getElementById('hb-status').textContent='';
  es.onopen=()=>{};
  es.onmessage=e=>{
    try{const msg=JSON.parse(e.data);if(msg.type==='connected')return;handleRaven(msg);}
    catch{}
  };
  es.onerror=()=>{es.close();setTimeout(connectRaven,5000);};
}

function setupNetworkPan(){
  document.querySelectorAll('.network-stage').forEach(stage=>{
  if(stage.dataset.panReady)return;
  stage.dataset.panReady='1';
  stage.addEventListener('pointerdown',e=>{
    if(e.button!==0)return;
    const nodeEl=e.target.closest('.net-hub,.network-stack,.net-worker');
    if(nodeEl){
      e.preventDefault();
      _networkDrag.active=true;
      _networkDrag.nodeId=nodeEl.dataset.nodeId||'';
      _networkDrag.startX=e.clientX;
      _networkDrag.startY=e.clientY;
      _networkDrag.originX=Number(nodeEl.dataset.x||0);
      _networkDrag.originY=Number(nodeEl.dataset.y||0);
      _networkDrag.moved=false;
      stage.setPointerCapture(e.pointerId);
      return;
    }
    if(e.target.closest('.net-tools'))return;
    e.preventDefault();
    _networkPan.dragging=true;
    _networkPan.startX=e.clientX;
    _networkPan.startY=e.clientY;
    _networkPan.originX=_networkPan.x;
    _networkPan.originY=_networkPan.y;
    stage.classList.add('dragging');
    stage.setPointerCapture(e.pointerId);
  });
  stage.addEventListener('pointermove',e=>{
    if(_networkDrag.active){
      const el=getNetworkNodeEl(_networkDrag.nodeId,stage);
      if(!el)return;
      const sx=e.clientX-_networkDrag.startX,sy=e.clientY-_networkDrag.startY;
      if(Math.hypot(sx,sy)>4)_networkDrag.moved=true;
      const x=_networkDrag.originX+sx/_networkZoom;
      const y=_networkDrag.originY+sy/_networkZoom;
      setNetworkNodePosition(el,x,y);
      return;
    }
    if(!_networkPan.dragging)return;
    _networkPan.x=_networkPan.originX+(e.clientX-_networkPan.startX);
    _networkPan.y=_networkPan.originY+(e.clientY-_networkPan.startY);
    applyNetworkTransform();
  });
  const stop=e=>{
    if(_networkDrag.active){
      if(_networkDrag.moved){
        _networkPan.suppressClick=true;
        setTimeout(()=>{_networkPan.suppressClick=false;},80);
      }
      _networkDrag.active=false;
      _networkDrag.nodeId='';
      try{stage.releasePointerCapture(e.pointerId);}catch{}
      return;
    }
    if(!_networkPan.dragging)return;
    _networkPan.dragging=false;
    stage.classList.remove('dragging');
    try{stage.releasePointerCapture(e.pointerId);}catch{}
  };
  stage.addEventListener('pointerup',stop);
  stage.addEventListener('pointercancel',stop);
  stage.addEventListener('click',e=>{
    if(!_networkPan.suppressClick)return;
    e.preventDefault();
    e.stopPropagation();
  },true);
  stage.addEventListener('wheel',e=>{
    if(!stage.closest('.pane.on'))return;
    e.preventDefault();
    const rect=stage.getBoundingClientRect();
    const before={x:(e.clientX-rect.left-_networkPan.x)/_networkZoom,y:(e.clientY-rect.top-_networkPan.y)/_networkZoom};
    _networkZoom=networkClamp(_networkZoom+(e.deltaY<0?.08:-.08),.35,1.8);
    _networkPan.x=e.clientX-rect.left-before.x*_networkZoom;
    _networkPan.y=e.clientY-rect.top-before.y*_networkZoom;
    applyNetworkTransform();
  },{passive:false});
  });
}

function applyAsideWidth(width){
  const n=networkClamp(Number(width)||292,260,620);
  document.documentElement.style.setProperty('--aside-width',`${n}px`);
  resizeCanvas();
  drawHb();
  return n;
}
function setupAsideWidth(){
  const saved=storageGet(ASIDE_WIDTH_KEY);
  if(saved)applyAsideWidth(saved);
  const grip=document.getElementById('aside-width-grip');
  if(!grip)return;
  grip.addEventListener('pointerdown',e=>{
    e.preventDefault();
    const startX=e.clientX;
    const start=parseInt(getComputedStyle(document.documentElement).getPropertyValue('--aside-width'))||292;
    grip.setPointerCapture(e.pointerId);
    const move=ev=>{
      const next=applyAsideWidth(start-(ev.clientX-startX));
      storageSet(ASIDE_WIDTH_KEY,String(next));
    };
    const up=ev=>{
      grip.releasePointerCapture(ev.pointerId);
      grip.removeEventListener('pointermove',move);
      grip.removeEventListener('pointerup',up);
      grip.removeEventListener('pointercancel',up);
    };
    grip.addEventListener('pointermove',move);
    grip.addEventListener('pointerup',up);
    grip.addEventListener('pointercancel',up);
  });
}

function setupInputs(){
  ['login-username','login-password'].forEach(id=>{
    const el=document.getElementById(id);
    if(el)el.addEventListener('keydown',e=>{if(e.key==='Enter')login();});
  });
  const fLogo=document.getElementById('f-logo');
  if(fLogo)fLogo.addEventListener('change',async e=>{
    _connLogoDraft=await readImageFile(e.target.files?.[0]);
    showLogoPreview('f-logo-preview',_connLogoDraft);
  });
  const cLogo=document.getElementById('char-logo');
  if(cLogo)cLogo.addEventListener('change',async e=>{
    setStackLogoDraft(await readImageFile(e.target.files?.[0]));
  });
  const aLogo=document.getElementById('agent-logo');
  if(aLogo)aLogo.addEventListener('change',async e=>{
    _agentLogoDraft=await readImageFile(e.target.files?.[0]);
    renderAgentBuilderChoices();
  });
  renderAgentBuilderChoices();
  const savedFeed=storageGet(RAVEN_FEED_HEIGHT_KEY);
  if(savedFeed)document.documentElement.style.setProperty('--raven-feed-height',savedFeed+'px');
  const handle=document.getElementById('oracle-resizer');
  if(handle)handle.addEventListener('pointerdown',e=>{
    e.preventDefault();
    const startY=e.clientY;
    const start=parseInt(getComputedStyle(document.documentElement).getPropertyValue('--raven-feed-height'))||150;
    const move=ev=>{
      const next=Math.max(52,Math.min(310,start+(ev.clientY-startY)));
      document.documentElement.style.setProperty('--raven-feed-height',next+'px');
      storageSet(RAVEN_FEED_HEIGHT_KEY,String(next));
    };
    const up=()=>{window.removeEventListener('pointermove',move);window.removeEventListener('pointerup',up);};
    window.addEventListener('pointermove',move);
    window.addEventListener('pointerup',up);
  });
  setupNetworkPan();
  setupAsideWidth();
}

/* ============================================================
   INIT
   ============================================================ */
async function loadAll(){
  _conns=await fetch('/connections').then(r=>r.json()).catch(()=>_conns);
  _populateServerDropdown();
  await Promise.all([loadStatus(),loadEvts(),loadStacks(),loadRavenBacklog(),loadHomeRecent()]);
  if(document.getElementById('pane-orchestration')?.classList.contains('on'))await loadOrchestration();
  renderHomeDashboard();
  document.getElementById('upd').textContent=fmtShort(new Date().toISOString());
}
window.addEventListener('resize',()=>{resizeCanvas();drawHb();});
setupInputs();
resizeCanvas();drawHb();
setupHeartbeatHover();
checkAuth().then(ok=>{if(ok)startApp();});
</script>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    _ensure_default_admin()
    yield


app = FastAPI(title="ORC API", version="0.1.0", lifespan=lifespan)
STATIC_DIR = REPO_ROOT / "app" / "static"
if not STATIC_DIR.exists():
    STATIC_DIR = REPO_ROOT / "backend" / "app" / "static"
app.mount("/assets", StaticFiles(directory=STATIC_DIR), name="assets")


@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    path = request.url.path
    public = (
        path == "/"
        or path == "/health"
        or path.startswith("/assets/")
        or path.startswith("/auth/")
        or path == "/favicon.ico"
    )
    if not public and not _current_user(request):
        return JSONResponse({"detail": "Login required"}, status_code=401)
    return await call_next(request)


# ---------------------------------------------------------------------------
# Routes — UI
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse, include_in_schema=False)
def dashboard() -> str:
    return _HTML


# ---------------------------------------------------------------------------
# Routes - Auth + Setup
# ---------------------------------------------------------------------------

@app.get("/auth/me")
def auth_me(request: Request) -> dict:
    user = _current_user(request)
    return {"authenticated": bool(user), "user": user}


@app.post("/auth/login")
def auth_login(body: LoginIn, response: Response) -> dict:
    username = body.username.strip()
    with SessionLocal() as s:
        user = s.query(UserAccount).filter_by(username=username).first()
        if not user or not user.enabled or not _verify_password(body.password, user.password_hash):
            raise HTTPException(status_code=401, detail="Invalid username or password")
        token = secrets.token_urlsafe(32)
        s.add(
            UserSession(
                token_hash=_hash_token(token),
                user_id=user.id,
                expires_at=datetime.now(timezone.utc) + timedelta(days=SESSION_DAYS),
            )
        )
        s.commit()
        response.set_cookie(
            COOKIE_NAME,
            token,
            httponly=True,
            samesite="lax",
            max_age=SESSION_DAYS * 24 * 60 * 60,
        )
        return {"ok": True, "user": _user_dict(user)}


@app.post("/auth/logout")
def auth_logout(request: Request, response: Response) -> dict:
    token = request.cookies.get(COOKIE_NAME, "")
    if token:
        with SessionLocal() as s:
            sess = s.query(UserSession).filter_by(token_hash=_hash_token(token)).first()
            if sess:
                s.delete(sess)
                s.commit()
    response.delete_cookie(COOKIE_NAME)
    return {"ok": True}


@app.get("/setup/users")
def setup_users(request: Request) -> dict:
    _require_admin(request)
    with SessionLocal() as s:
        users = [_user_dict(row) for row in s.query(UserAccount).order_by(UserAccount.username).all()]
    return {"items": users}


@app.post("/setup/users", status_code=201)
def setup_create_user(body: UserCreateIn, request: Request) -> dict:
    _require_admin(request)
    username = body.username.strip()
    if not username or len(username) < 3:
        raise HTTPException(status_code=400, detail="Username must be at least 3 characters")
    if len(body.password) < 4:
        raise HTTPException(status_code=400, detail="Password must be at least 4 characters")
    role = _normalize_role(body.role)
    with SessionLocal() as s:
        if s.query(UserAccount).filter_by(username=username).first():
            raise HTTPException(status_code=409, detail="Username already exists")
        row = UserAccount(
            username=username,
            password_hash=_hash_password(body.password),
            role=role,
            enabled=True,
        )
        s.add(row)
        s.commit()
        s.refresh(row)
        return _user_dict(row)


# ---------------------------------------------------------------------------
# Routes — Raven SSE stream
# ---------------------------------------------------------------------------

@app.get("/raven/stream")
async def raven_stream() -> StreamingResponse:
    import asyncio
    from redis.asyncio import Redis as ARedis

    async def _gen():
        r = None
        ps = None
        try:
            r = ARedis.from_url(REDIS_URL, decode_responses=True)
            ps = r.pubsub()
            await ps.subscribe(CHANNEL)
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
            yield 'data: {"type":"connected","degraded":true}\n\n'
            while True:
                await asyncio.sleep(10)
                yield ': raven unavailable\n\n'
        finally:
            try:
                if ps is not None:
                    await ps.unsubscribe(CHANNEL)
                if r is not None:
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
# Routes - Orchestration
# ---------------------------------------------------------------------------

@app.get("/orchestration/summary")
def orchestration_summary() -> dict:
    with SessionLocal() as s:
        _ensure_orchestration_agents(s)
        _ensure_seed_messages(s)
        agents = [
            _agent_dict(row)
            for row in s.query(AgentRuntimeState).order_by(AgentRuntimeState.name).all()
            if row.agent_id not in CAPABILITY_AGENT_REMAP
        ]
        messages = [
            _message_dict(row)
            for row in s.query(AgentMessage).order_by(AgentMessage.created_at.desc(), AgentMessage.id.desc()).limit(80).all()
        ]
        approvals = [
            _approval_dict(row)
            for row in s.query(ApprovalRequest).order_by(ApprovalRequest.requested_at.desc(), ApprovalRequest.id.desc()).limit(40).all()
        ]
        learnings = [
            _learning_dict(row)
            for row in s.query(LearningEntry).order_by(LearningEntry.created_at.desc(), LearningEntry.id.desc()).limit(30).all()
        ]
        incidents = [
            _incident_dict(row)
            for row in s.query(IncidentRecord).order_by(IncidentRecord.created_at.desc(), IncidentRecord.id.desc()).limit(30).all()
        ]
        runbook_executions = [
            _runbook_execution_dict(row)
            for row in s.query(RunbookExecution).order_by(RunbookExecution.created_at.desc(), RunbookExecution.id.desc()).limit(30).all()
        ]
        tool_promotions = [
            _tool_promotion_dict(row)
            for row in s.query(ToolPromotion).order_by(ToolPromotion.created_at.desc(), ToolPromotion.id.desc()).limit(30).all()
        ]
        focused_watches = [
            _focused_watch_dict(row, conn)
            for row, conn in (
                s.query(FocusedWatch, Connection)
                .join(Connection, FocusedWatch.connection_id == Connection.id)
                .order_by(FocusedWatch.expires_at.asc(), FocusedWatch.id.desc())
                .limit(30)
                .all()
            )
        ]
    skills = [asdict(item) for item in load_registry(REPO_ROOT, "skills")]
    tools = [asdict(item) for item in load_registry(REPO_ROOT, "tools")]
    runbooks = [asdict(item) for item in load_registry(REPO_ROOT, "runbooks")]
    return {
        "agents": agents,
        "skills": skills,
        "tools": tools,
        "runbooks": runbooks,
        "messages": messages,
        "approvals": approvals,
        "learnings": learnings,
        "incidents": incidents,
        "runbook_executions": runbook_executions,
        "tool_promotions": tool_promotions,
        "focused_watches": focused_watches,
        "paths": {
            "agents": str((REPO_ROOT / "agents").relative_to(REPO_ROOT)),
            "skills": str((REPO_ROOT / "skills").relative_to(REPO_ROOT)),
            "tools": str((REPO_ROOT / "tools").relative_to(REPO_ROOT)),
            "runbooks": str((REPO_ROOT / "runbooks").relative_to(REPO_ROOT)),
            "memory": str((REPO_ROOT / "memory").relative_to(REPO_ROOT)),
            "knowledge": str((REPO_ROOT / "knowledge").relative_to(REPO_ROOT)),
        },
    }


@app.post("/orchestration/agents", status_code=201)
def create_orchestration_agent(body: AgentCreateIn) -> dict:
    agent_id = _slug(body.agent_id or body.agent_name, "agent")
    agents_root = REPO_ROOT / "agents"
    target_dir = _safe_markdown_path(agents_root, agent_id)
    target_file = target_dir / "agent.md"
    target_dir.mkdir(parents=True, exist_ok=True)

    markdown = "\n".join(
        [
            "# Agent Definition",
            "",
            f"name: {body.agent_name.strip()}",
            f"id: {agent_id}",
            "version: 0.1.0",
            f"role: {body.role.strip() or 'specialist'}",
            f"risk_level: {body.risk_level.strip() or 'low'}",
            "plane: control",
            "autonomy_level: 1",
            "governance_boundary: yellow",
            f"approval_required: {_bool_text(body.approval_required)}",
            f"icon: {body.icon.strip() or _default_agent_icon(agent_id, body.role)}",
            "",
            "## Purpose",
            "",
            body.purpose.strip() or "Not specified",
            "",
            "## Inputs",
            "",
            "- Structured messages from Raven",
            "- Assigned skills and operator requests",
            "",
            "## Outputs",
            "",
            "- Agent messages",
            "- Recommendations or approved action requests",
            "",
            "## Allowed Skills",
            "",
            _bullet_block(body.allowed_skills),
            "",
            "## Rules",
            "",
            _bullet_block(body.rules or "Stay within assigned skills and route risky work to Gate Keeper."),
            "",
        ]
    )
    target_file.write_text(markdown, encoding="utf-8")

    with SessionLocal() as s:
        row = s.query(AgentRuntimeState).filter_by(agent_id=agent_id).first()
        now = datetime.now(timezone.utc)
        if not row:
            row = AgentRuntimeState(
                agent_id=agent_id,
                name=body.agent_name.strip(),
                role=body.role.strip() or "specialist",
                icon=body.icon.strip() or _default_agent_icon(agent_id, body.role),
                logo_data=body.logo_data.strip() or None,
                trust_mode="approval_required" if body.approval_required else "recommend_only",
                enabled=True,
                created_at=now,
                updated_at=now,
            )
            s.add(row)
        else:
            row.name = body.agent_name.strip()
            row.role = body.role.strip() or row.role
            row.icon = body.icon.strip() or row.icon
            row.logo_data = body.logo_data.strip() or row.logo_data
            row.updated_at = now
        s.commit()
        s.refresh(row)
        _record_agent_message(
            s,
            "sage",
            agent_id,
            "skill_proposal",
            f"Sage registered {row.name} as an orchestration agent.",
            {"path": str(target_file.relative_to(REPO_ROOT))},
        )
        return {"agent": _agent_dict(row), "path": str(target_file.relative_to(REPO_ROOT))}


@app.get("/orchestration/agents/{agent_id}")
def get_orchestration_agent(agent_id: str, request: Request) -> dict:
    _require_user(request)
    with SessionLocal() as s:
        _ensure_orchestration_agents(s)
        row = s.query(AgentRuntimeState).filter_by(agent_id=agent_id).first()
        if not row:
            raise HTTPException(404, "Agent not found")
        base = _agent_dict(row)

    # Enrich with content from agent.md if it exists
    purpose = ""
    allowed_skills = ""
    rules = ""
    risk_level = "low"
    approval_required = False
    try:
        agent_file = _safe_markdown_path(REPO_ROOT / "agents", agent_id) / "agent.md"
        if agent_file.exists():
            content = agent_file.read_text(encoding="utf-8")
            # Parse key:value header lines
            for line in content.splitlines():
                stripped = line.strip()
                if stripped.startswith("risk_level:"):
                    risk_level = stripped.split(":", 1)[1].strip()
                elif stripped.startswith("approval_required:"):
                    approval_required = stripped.split(":", 1)[1].strip().lower() == "true"
            # Parse markdown sections
            sections: dict[str, list[str]] = {}
            current: str | None = None
            for line in content.splitlines():
                if line.startswith("## "):
                    current = line[3:].strip().lower()
                    sections[current] = []
                elif current is not None:
                    sections[current].append(line)
            purpose = "\n".join(sections.get("purpose", [])).strip()
            allowed_skills = "\n".join(sections.get("allowed skills", [])).strip()
            rules = "\n".join(sections.get("rules", [])).strip()
    except Exception:
        pass

    return {**base, "purpose": purpose, "allowed_skills": allowed_skills,
            "rules": rules, "risk_level": risk_level, "approval_required": approval_required}


@app.put("/orchestration/agents/{agent_id}/trust")
def update_agent_trust(agent_id: str, body: AgentTrustIn, request: Request) -> dict:
    _require_admin(request)
    trust_mode = body.trust_mode if body.trust_mode in TRUST_MODES else "recommend_only"
    with SessionLocal() as s:
        _ensure_orchestration_agents(s)
        row = s.query(AgentRuntimeState).filter_by(agent_id=agent_id).first()
        if not row:
            raise HTTPException(404, "Agent not found")
        row.trust_mode = trust_mode
        row.enabled = body.enabled
        row.updated_at = datetime.now(timezone.utc)
        s.commit()
        s.refresh(row)
        _record_agent_message(
            s,
            "gate-keeper",
            row.agent_id,
            "approval_decision",
            f"Gate Keeper set {row.name} to {trust_mode.replace('_', ' ')}.",
            {"enabled": row.enabled, "trust_mode": row.trust_mode},
        )
        return _agent_dict(row)


@app.post("/orchestration/messages", status_code=201)
def create_agent_message(body: AgentMessageIn) -> dict:
    if not body.source_agent.strip() or not body.summary.strip():
        raise HTTPException(400, "source_agent and summary are required")
    with SessionLocal() as s:
        row = _record_agent_message(
            s,
            body.source_agent.strip(),
            body.target_agent.strip(),
            body.message_type.strip() or "status",
            body.summary.strip(),
            body.payload or {"thread_id": body.thread_id.strip() or "operations"},
            thread_id=body.thread_id.strip() or "operations",
        )
        return _message_dict(row)


@app.post("/orchestration/agents/{agent_id}/chat")
def agent_chat(agent_id: str, body: AgentChatIn, request: Request) -> dict:
    _require_user(request)
    user_message = body.message.strip()
    thread_id = body.thread_id.strip() or "operations"
    if not user_message:
        raise HTTPException(400, "message is required")

    with SessionLocal() as s:
        # Verify the agent exists and is enabled
        agent = s.query(AgentRuntimeState).filter_by(agent_id=agent_id).first()
        if not agent:
            raise HTTPException(404, f"Agent '{agent_id}' not found")
        if not agent.enabled:
            raise HTTPException(409, f"Agent '{agent.name}' is disabled")

        # Store operator's message for audit before any early clarification exit.
        _record_agent_message(s, "operator", agent_id, "instruction", user_message, thread_id=thread_id)

        reply_payload: dict = {}

        if agent_id == "orc-orchestrator":
            clarification = _clarify_ambiguous_target(user_message, s)
            if clarification:
                _record_agent_message(
                    s,
                    "orc-orchestrator",
                    "operator",
                    "response",
                    clarification,
                    {"needs_clarification": True},
                    thread_id=thread_id,
                )
                return {
                    "reply": clarification,
                    "agent_id": agent_id,
                    "created_at": datetime.now(timezone.utc).isoformat(),
                    "payload": {"needs_clarification": True},
                }

        autonomous_reply = _chat_autonomy_action(agent_id, user_message, s, thread_id=thread_id)
        if autonomous_reply:
            reply, reply_payload = autonomous_reply
        elif agent_id == "orc-orchestrator":
            # Full multi-agent routing loop
            reply = _run_orc_loop(user_message, s, thread_id=thread_id)
        elif agent_id == "oracle" and _is_hourly_critical_error_review_request(user_message):
            # This skill has a real backend implementation; run it instead of letting
            # the Oracle merely promise to do it.
            reply = _run_hourly_critical_error_review(s)["message"]
        else:
            # Direct single-agent call with agent-filtered skill context
            reply = _agent_chat_internal(agent_id, user_message, s, thread_id=thread_id)

        if not reply:
            reply = f"*(No response from {agent.name})*"

        reply_row = _record_agent_message(s, agent_id, "operator", "response", reply, reply_payload, thread_id=thread_id)
        response = {"reply": reply, "agent_id": agent_id, "created_at": reply_row.created_at.isoformat()}
        if reply_payload:
            response["payload"] = reply_payload
        return response


@app.post("/orchestration/skills", status_code=201)
def create_skill(body: SkillBuildIn) -> dict:
    skill_id = _slug(body.skill_id or body.skill_name, "skill")
    skills_root = REPO_ROOT / "skills"
    try:
        target_dir = _safe_markdown_path(skills_root, skill_id)
        target_file = target_dir / "skills.md"
        target_dir.mkdir(parents=True, exist_ok=True)
        target_file.write_text(_skill_markdown(body, skill_id), encoding="utf-8")
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to write skill file: {exc}") from exc

    with SessionLocal() as s:
        _record_agent_message(
            s,
            "sage",
            body.agent_id.strip(),
            "skill_proposal",
            f"Sage drafted skill {body.skill_name.strip()} for {body.agent_id.strip()}.",
            {"path": str(target_file.relative_to(REPO_ROOT)), "risk_level": body.risk_level},
        )
    return {"ok": True, "skill_id": skill_id, "path": str(target_file.relative_to(REPO_ROOT))}


@app.post("/orchestration/skills/markdown", status_code=201)
def create_skill_markdown(body: SkillMarkdownIn) -> dict:
    markdown = body.markdown.rstrip()
    if not markdown.strip():
        raise HTTPException(status_code=400, detail="Skill Markdown is required")

    metadata = _skill_metadata_from_markdown(markdown)
    skill_id = _slug(body.skill_id or metadata.get("id") or metadata.get("name") or "skill", "skill")
    skills_root = REPO_ROOT / "skills"
    try:
        target_dir = _safe_markdown_path(skills_root, skill_id)
        target_file = target_dir / "skills.md"
        target_dir.mkdir(parents=True, exist_ok=True)
        target_file.write_text(markdown + "\n", encoding="utf-8")
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to write skill file: {exc}") from exc

    skill_name = metadata.get("name") or skill_id
    target_agent = metadata.get("agent") or ""
    with SessionLocal() as s:
        _record_agent_message(
            s,
            "sage",
            target_agent,
            "skill_proposal",
            f"Sage drafted skill {skill_name} as Markdown.",
            {"path": str(target_file.relative_to(REPO_ROOT)), "risk_level": metadata.get("risk_level", "medium")},
        )
    return {"ok": True, **_parse_skill_file(target_file)}


@app.get("/orchestration/skills/{skill_id}")
def get_skill(skill_id: str) -> dict:
    target_file, _ = _skill_file_for_id(skill_id)
    return _parse_skill_file(target_file)


@app.put("/orchestration/skills/{skill_id}")
def update_skill(skill_id: str, body: SkillBuildIn) -> dict:
    target_file, canonical_id = _skill_file_for_id(skill_id)
    target_file.write_text(_skill_markdown(body, canonical_id), encoding="utf-8")

    with SessionLocal() as s:
        _record_agent_message(
            s,
            "sage",
            body.agent_id.strip(),
            "skill_update",
            f"Sage updated skill {body.skill_name.strip()} for {body.agent_id.strip()}.",
            {"path": str(target_file.relative_to(REPO_ROOT)), "risk_level": body.risk_level},
        )
    return {"ok": True, "skill_id": canonical_id, "path": str(target_file.relative_to(REPO_ROOT))}


@app.put("/orchestration/skills/{skill_id}/markdown")
def update_skill_markdown(skill_id: str, body: SkillMarkdownIn) -> dict:
    markdown = body.markdown.rstrip()
    if not markdown.strip():
        raise HTTPException(status_code=400, detail="Skill Markdown is required")

    target_file, canonical_id = _skill_file_for_id(skill_id)
    target_file.write_text(markdown + "\n", encoding="utf-8")
    metadata = _skill_metadata_from_markdown(markdown)

    with SessionLocal() as s:
        _record_agent_message(
            s,
            "sage",
            metadata.get("agent") or "",
            "skill_update",
            f"Sage updated skill {metadata.get('name') or canonical_id} as Markdown.",
            {"path": str(target_file.relative_to(REPO_ROOT)), "risk_level": metadata.get("risk_level", "medium")},
        )
    return {"ok": True, **_parse_skill_file(target_file)}


@app.post("/orchestration/approvals", status_code=201)
def create_approval(body: ApprovalCreateIn) -> dict:
    with SessionLocal() as s:
        row = ApprovalRequest(
            title=body.title.strip(),
            requester_agent=body.requester_agent.strip() or "oracle",
            approver_agent="gate-keeper",
            action_type=body.action_type.strip() or "container_refresh",
            target=body.target.strip(),
            rationale=body.rationale.strip(),
            risk_level=body.risk_level.strip() or "medium",
            requested_by=body.requested_by.strip() or "operator",
        )
        s.add(row)
        s.commit()
        s.refresh(row)
        _record_agent_message(
            s,
            row.requester_agent,
            row.approver_agent,
            "approval_request",
            f"{row.requester_agent} requested approval for {row.action_type}: {row.title}",
            {"approval_id": row.id, "target": row.target, "risk_level": row.risk_level},
        )
        return _approval_dict(row)


@app.post("/orchestration/approvals/{approval_id}/decision")
def decide_approval(approval_id: int, body: ApprovalDecisionIn) -> dict:
    decision = body.decision.lower().strip()
    if decision not in {"approved", "rejected"}:
        raise HTTPException(400, "decision must be approved or rejected")
    with SessionLocal() as s:
        row = s.get(ApprovalRequest, approval_id)
        if not row:
            raise HTTPException(404, "Approval request not found")
        row.status = decision
        row.decided_by = body.decided_by.strip() or "operator"
        row.decision_reason = body.reason.strip() or None
        row.decided_at = datetime.now(timezone.utc)
        row.execution_allowed = decision == "approved"
        s.commit()
        s.refresh(row)
        should_execute = row.execution_allowed and _requires_execution_approval(row.title, row.rationale or "")
        target = "executioner" if should_execute else row.requester_agent
        summary = (
            f"Gate Keeper approved {row.action_type} for {row.target or row.title}."
            if row.execution_allowed
            else f"Gate Keeper rejected {row.action_type} for {row.target or row.title}."
        )
        _record_agent_message(
            s,
            "gate-keeper",
            target,
            "approval_decision",
            summary,
            {"approval_id": row.id, "status": row.status, "reason": row.decision_reason},
        )
        if should_execute:
            try:
                _run_executioner(row.id, row.title, row.rationale or "", s)
            except Exception as _exc:
                _record_agent_message(
                    s,
                    "executioner",
                    "operator",
                    "execution_error",
                    f"Execution failed: {_exc}",
                    {"approval_id": row.id},
                )
        return _approval_dict(row)


@app.post("/orchestration/learnings", status_code=201)
def create_learning(body: LearningCreateIn) -> dict:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    title_slug = _slug(body.title, "lesson")
    knowledge_root = REPO_ROOT / "knowledge"
    lessons_root = _safe_markdown_path(knowledge_root, "lessons")
    lessons_root.mkdir(parents=True, exist_ok=True)
    target_file = lessons_root / f"{stamp}-{title_slug}.md"
    markdown = "\n".join(
        [
            "# Lesson Learned",
            "",
            f"title: {body.title.strip()}",
            f"source_agent: {body.source_agent.strip() or 'sage'}",
            f"incident_ref: {body.incident_ref.strip()}",
            f"outcome: {body.outcome.strip() or 'proposed'}",
            f"created_at: {datetime.now(timezone.utc).isoformat()}",
            "",
            "## Summary",
            "",
            body.summary.strip(),
            "",
            "## Reuse Notes",
            "",
            "- Review this entry during future similar incidents.",
            "- Promote it into a skill when the procedure is repeatable and trusted.",
            "",
        ]
    )
    target_file.write_text(markdown, encoding="utf-8")

    with SessionLocal() as s:
        row = LearningEntry(
            title=body.title.strip(),
            source_agent=body.source_agent.strip() or "sage",
            incident_ref=body.incident_ref.strip(),
            outcome=body.outcome.strip() or "proposed",
            summary=body.summary.strip(),
            markdown_path=str(target_file.relative_to(REPO_ROOT)),
        )
        s.add(row)
        s.commit()
        s.refresh(row)
        _record_agent_message(
            s,
            row.source_agent,
            "orc-orchestrator",
            "lesson_learned",
            f"Sage recorded a learning note: {row.title}",
            {"path": row.markdown_path, "outcome": row.outcome},
        )
        return _learning_dict(row)


def _memory_search(query: str, limit: int = 10) -> list[dict]:
    terms = [t for t in re.findall(r"[a-z0-9]+", query.lower()) if len(t) > 2]
    roots = ["memory", "knowledge", "runbooks", "tools", "docs"]
    results: list[dict] = []
    for root_name in roots:
        root = REPO_ROOT / root_name
        if not root.exists():
            continue
        for path in root.rglob("*.md"):
            try:
                raw = path.read_text(encoding="utf-8")
            except Exception:
                continue
            haystack = raw.lower()
            if terms:
                score = sum(haystack.count(term) for term in terms)
            else:
                score = 1
            if score <= 0:
                continue
            metadata = _markdown_metadata(raw)
            sections = _markdown_sections(raw)
            summary = sections.get("summary") or sections.get("purpose") or sections.get("fact") or raw[:240]
            results.append(
                {
                    "score": score,
                    "path": str(path.relative_to(REPO_ROOT)),
                    "title": metadata.get("title") or metadata.get("name") or path.stem,
                    "kind": root_name,
                    "summary": re.sub(r"\s+", " ", summary).strip()[:360],
                }
            )
    results.sort(key=lambda item: (-item["score"], item["path"]))
    return results[: max(1, min(limit, 50))]


@app.get("/memory/search")
def search_memory(q: str = "", limit: int = 10) -> dict:
    return {"query": q, "items": _memory_search(q, limit)}


@app.get("/orchestration/incidents")
def list_incidents() -> dict:
    with SessionLocal() as s:
        rows = s.query(IncidentRecord).order_by(IncidentRecord.created_at.desc(), IncidentRecord.id.desc()).limit(100).all()
        return {"items": [_incident_dict(row) for row in rows]}


@app.post("/orchestration/incidents", status_code=201)
def create_incident(body: IncidentCreateIn) -> dict:
    now = datetime.now(timezone.utc)
    stamp = now.strftime("%Y%m%d-%H%M%S")
    incident_slug = _slug(body.title, "incident")
    markdown = "\n".join(
        [
            "# Episodic Memory",
            "",
            "memory_type: episodic",
            f"id: {stamp}-{incident_slug}",
            f"title: {body.title.strip()}",
            f"created_at: {now.isoformat()}",
            f"outcome: {body.outcome.strip() or 'open'}",
            f"confidence: {body.confidence.strip() or 'medium'}",
            f"governance: {body.governance.strip() or 'yellow'}",
            f"autonomy_level: {body.autonomy_level}",
            "",
            "## Symptom",
            "",
            body.symptom.strip(),
            "",
            "## Context",
            "",
            body.context.strip() or "Not specified.",
            "",
            "## Root Cause",
            "",
            body.root_cause.strip() or "Unknown.",
            "",
            "## Action",
            "",
            body.action.strip() or "No action recorded.",
            "",
            "## Outcome",
            "",
            body.outcome.strip() or "open",
            "",
            "## Promotion",
            "",
            "- Review for runbook promotion when the same symptom recurs.",
            "",
        ]
    )
    target_file = _write_markdown_under(
        "memory",
        "episodic",
        f"{stamp}-{incident_slug}.md",
        content=markdown,
    )
    with SessionLocal() as s:
        row = IncidentRecord(
            title=body.title.strip(),
            symptom=body.symptom.strip(),
            context=body.context.strip(),
            root_cause=body.root_cause.strip(),
            action=body.action.strip(),
            outcome=body.outcome.strip() or "open",
            confidence=body.confidence.strip() or "medium",
            governance=body.governance.strip() or "yellow",
            autonomy_level=body.autonomy_level,
            markdown_path=str(target_file.relative_to(REPO_ROOT)),
            status="closed" if body.outcome.strip() and body.outcome.strip() != "open" else "open",
            created_at=now,
            updated_at=now,
        )
        s.add(row)
        s.commit()
        s.refresh(row)
        _record_agent_message(
            s,
            "sage",
            "orc-orchestrator",
            "incident_memory",
            f"Sage wrote episodic memory for incident: {row.title}",
            {"incident_id": row.id, "path": row.markdown_path},
        )
        return _incident_dict(row)


def _approval_is_approved(session, approval_id: int | None) -> bool:
    if not approval_id:
        return False
    row = session.get(ApprovalRequest, approval_id)
    return bool(row and row.status == "approved" and row.execution_allowed)


def _create_policy_approval(session, *, title: str, action_type: str, target: str, rationale: str, requested_by: str) -> ApprovalRequest:
    row = ApprovalRequest(
        title=title,
        requester_agent=requested_by or "operator",
        approver_agent="gate-keeper",
        action_type=action_type,
        target=target,
        rationale=rationale,
        risk_level="high",
        requested_by=requested_by or "operator",
    )
    session.add(row)
    session.commit()
    session.refresh(row)
    _record_agent_message(
        session,
        "orc-orchestrator",
        "gate-keeper",
        "approval_request",
        f"ORC requested Gatekeeper approval: {title}",
        {"approval_id": row.id, "target": target, "risk_level": row.risk_level},
    )
    return row


def _execute_green_runbook(runbook: dict, body: RunbookExecuteIn) -> tuple[str, str, str]:
    now = datetime.now(timezone.utc)
    if runbook["id"] == "approved-log-review":
        summary = _oracle_summary(window_hours=1)
        result = (
            f"Reviewed the last hour of warnings/errors. "
            f"Events: {summary.get('total_events', 0)}. "
            f"Containers: {len(summary.get('containers', []))}. "
            f"Top issue: {(summary.get('containers') or [{'container': 'none'}])[0].get('container', 'none')}."
        )
        evidence = json.dumps(summary, indent=2, default=str)
    else:
        result = "Runbook completed as a policy-checked dry run. No mutating action was performed."
        evidence = f"Runbook: {runbook['id']}\nTarget: {body.target}\nRationale: {body.rationale}\n"
    stamp = now.strftime("%Y%m%d-%H%M%S")
    evidence_md = "\n".join(
        [
            "# Runbook Execution Evidence",
            "",
            f"runbook_id: {runbook['id']}",
            f"created_at: {now.isoformat()}",
            f"governance: {runbook['governance']}",
            f"autonomy_level: {runbook['autonomy_level']}",
            "",
            "## Result",
            "",
            result,
            "",
            "## Evidence",
            "",
            "```text",
            evidence,
            "```",
            "",
        ]
    )
    evidence_file = _write_markdown_under(
        "knowledge",
        "runbook-executions",
        f"{stamp}-{_slug(runbook['id'], 'runbook')}.md",
        content=evidence_md,
    )
    return result, str(evidence_file.relative_to(REPO_ROOT)), runbook["sections"].get("verification", "")


@app.get("/orchestration/runbooks")
def list_runbooks() -> dict:
    return {"items": [asdict(item) for item in load_registry(REPO_ROOT, "runbooks")]}


@app.get("/orchestration/runbooks/{runbook_id}")
def get_runbook(runbook_id: str) -> dict:
    return _read_registry_definition("runbooks", runbook_id)


@app.get("/orchestration/runbook-executions")
def list_runbook_executions() -> dict:
    with SessionLocal() as s:
        rows = s.query(RunbookExecution).order_by(RunbookExecution.created_at.desc(), RunbookExecution.id.desc()).limit(100).all()
        return {"items": [_runbook_execution_dict(row) for row in rows]}


@app.post("/orchestration/runbooks/{runbook_id}/execute", status_code=201)
def execute_runbook(runbook_id: str, body: RunbookExecuteIn) -> dict:
    runbook = _read_registry_definition("runbooks", runbook_id)
    governance = runbook["governance"].lower()
    requires_human = governance == "red" or runbook["approval_required"]
    with SessionLocal() as s:
        approval_id = body.approval_id
        if requires_human and not _approval_is_approved(s, approval_id):
            approval = _create_policy_approval(
                s,
                title=f"Runbook approval required: {runbook['name']}",
                action_type=f"runbook:{runbook['id']}",
                target=body.target or runbook["id"],
                rationale=body.rationale or f"Execute runbook {runbook['id']}",
                requested_by=body.requested_by or "operator",
            )
            row = RunbookExecution(
                runbook_id=runbook["id"],
                title=runbook["name"],
                requested_by=body.requested_by or "operator",
                approval_id=approval.id,
                governance=runbook["governance"],
                autonomy_level=runbook["autonomy_level"],
                status="pending_approval",
                target=body.target.strip(),
                rationale=body.rationale.strip(),
                verification=runbook["sections"].get("verification", ""),
                rollback=runbook["sections"].get("rollback", ""),
                result="Gatekeeper approval required before this runbook can proceed.",
            )
            s.add(row)
            s.commit()
            s.refresh(row)
            return _runbook_execution_dict(row)

        row = RunbookExecution(
            runbook_id=runbook["id"],
            title=runbook["name"],
            requested_by=body.requested_by or "operator",
            approval_id=approval_id,
            governance=runbook["governance"],
            autonomy_level=runbook["autonomy_level"],
            status="running",
            target=body.target.strip(),
            rationale=body.rationale.strip(),
            verification=runbook["sections"].get("verification", ""),
            rollback=runbook["sections"].get("rollback", ""),
        )
        s.add(row)
        s.commit()
        s.refresh(row)

        if governance == "red":
            row.status = "approved_handoff"
            row.result = "Human approval confirmed. Executioner handoff recorded; no direct mutation was performed by the runbook API."
            row.completed_at = datetime.now(timezone.utc)
            _record_agent_message(
                s,
                "gate-keeper",
                "executioner",
                "runbook_handoff",
                f"Approved red runbook ready for Executioner: {runbook['name']}",
                {"runbook_execution_id": row.id, "approval_id": approval_id, "target": row.target},
            )
        else:
            result, evidence_path, verification = _execute_green_runbook(runbook, body)
            row.status = "completed"
            row.result = result
            row.evidence_path = evidence_path
            row.verification = verification or row.verification
            row.completed_at = datetime.now(timezone.utc)
            _record_agent_message(
                s,
                "executioner",
                "raven",
                "runbook_result",
                f"Runbook completed: {runbook['name']}",
                {"runbook_execution_id": row.id, "evidence_path": row.evidence_path},
            )
        s.commit()
        s.refresh(row)
        return _runbook_execution_dict(row)


@app.get("/orchestration/tool-promotions")
def list_tool_promotions() -> dict:
    with SessionLocal() as s:
        rows = s.query(ToolPromotion).order_by(ToolPromotion.created_at.desc(), ToolPromotion.id.desc()).limit(100).all()
        return {"items": [_tool_promotion_dict(row) for row in rows]}


@app.post("/orchestration/tool-promotions", status_code=201)
def create_tool_promotion(body: ToolPromotionIn) -> dict:
    tool_id = _slug(body.tool_id, "tool")
    now = datetime.now(timezone.utc)
    artifact = body.artifact_markdown.strip() or "\n".join(
        [
            "# Tool Definition",
            "",
            f"name: {body.title.strip()}",
            f"id: {tool_id}",
            "version: 0.1.0",
            "category: generated",
            "risk_level: medium",
            "autonomy_level: 3",
            "governance: red",
            "execution_surface: worker-pool",
            "approval_required: true",
            "",
            "## Purpose",
            "",
            "Generated in the builder sandbox and pending promotion.",
            "",
            "## Test Summary",
            "",
            body.test_summary.strip(),
            "",
            "## Dry Run Summary",
            "",
            body.dry_run_summary.strip(),
            "",
            "## Execution Boundary",
            "",
            "- Run only in the worker pool after approval.",
            "- Do not mutate Stable Core.",
            "",
        ]
    )
    artifact_file = _write_markdown_under(
        "builder",
        "workspace",
        "proposals",
        tool_id,
        "tool.md",
        content=artifact,
    )
    with SessionLocal() as s:
        approval_id = body.approval_id
        status = "approved"
        if not _approval_is_approved(s, approval_id):
            approval = _create_policy_approval(
                s,
                title=f"Promote generated tool: {body.title.strip()}",
                action_type="tool_promotion",
                target=tool_id,
                rationale=f"Tests: {body.test_summary.strip()}\nDry run: {body.dry_run_summary.strip()}",
                requested_by=body.requested_by or "operator",
            )
            approval_id = approval.id
            status = "pending_approval"
        row = ToolPromotion(
            tool_id=tool_id,
            title=body.title.strip(),
            source_path=body.source_path.strip(),
            artifact_path=str(artifact_file.relative_to(REPO_ROOT)),
            requested_by=body.requested_by or "operator",
            approval_id=approval_id,
            status=status,
            test_summary=body.test_summary.strip(),
            dry_run_summary=body.dry_run_summary.strip(),
            created_at=now,
        )
        s.add(row)
        s.commit()
        s.refresh(row)
        return _tool_promotion_dict(row)


def _chat_has_any(text: str, terms: tuple[str, ...]) -> bool:
    return any(term in text for term in terms)


def _chat_is_memory_request(text: str) -> bool:
    return (
        _chat_has_any(text, ("search memory", "search sage", "query memory", "look up memory", "check memory"))
        or _chat_has_any(text, ("what do we know about", "similar incidents", "known patterns", "past cases"))
    )


def _chat_memory_query(message: str) -> str:
    cleaned = message.strip()
    patterns = (
        r"^\s*(?:please\s+)?(?:ask\s+)?sage\s+(?:to\s+)?(?:search|query|check|look\s+up)\s+(?:memory\s+)?(?:for|about)?\s*",
        r"^\s*(?:please\s+)?(?:search|query|check|look\s+up)\s+(?:sage\s+)?(?:memory|knowledge|learnings)\s+(?:for|about)?\s*",
        r"^\s*(?:what\s+do\s+we\s+know\s+about|show\s+similar\s+incidents\s+for|find\s+similar\s+incidents\s+for|similar\s+incidents\s+for)\s+",
    )
    for pattern in patterns:
        cleaned = re.sub(pattern, "", cleaned, flags=re.I).strip()
    return cleaned or message.strip()


def _chat_is_log_review_request(text: str) -> bool:
    if "approved-log-review" in text:
        return True
    has_review_verb = _chat_has_any(text, ("review", "check", "analyze", "analyse", "inspect", "run"))
    has_log_subject = _chat_has_any(text, ("logs", "log review", "warnings", "errors", "critical events"))
    return has_review_verb and has_log_subject and not _chat_is_red_action_request(text)


def _chat_is_red_action_request(text: str) -> bool:
    return _chat_has_any(
        text,
        (
            "redeploy",
            "restart",
            "refresh container",
            "recreate container",
            "container redeploy",
            "container restart",
            "git pull",
            "delete",
            "remove",
            "rotate credential",
            "rotate secret",
        ),
    )


def _chat_extract_target(message: str) -> str:
    candidates: list[str] = []
    patterns = (
        r"(?:target|for|container|service|app)\s+(?:the\s+)?([A-Za-z0-9_.:/-]+)",
        r"(?:redeploy|restart|refresh|recreate)\s+(?:the\s+)?(?:container|service|app)?\s*([A-Za-z0-9_.:/-]+)",
    )
    for pattern in patterns:
        match = re.search(pattern, message, flags=re.I)
        if match:
            candidates.append(match.group(1).strip(" .,;:"))
    ignored = {"", "the", "a", "an", "container", "service", "app", "for", "target"}
    for candidate in candidates:
        if candidate.lower() not in ignored:
            return candidate
    return "unspecified target"


def _chat_field(message: str, *names: str) -> str:
    escaped = [re.escape(name).replace(r"\ ", r"[\s_-]+") for name in names]
    pattern = r"(?:^|[;\n])\s*(?:" + "|".join(escaped) + r")\s*[:=]\s*([^;\n]+)"
    match = re.search(pattern, message, flags=re.I)
    return match.group(1).strip() if match else ""


def _chat_is_tool_promotion_request(text: str) -> bool:
    return "promote" in text and _chat_has_any(text, ("tool", "worker", "skill"))


def _format_memory_hits(items: list[dict]) -> str:
    if not items:
        return "No matching memory entries were found."
    lines = []
    for index, item in enumerate(items[:3], start=1):
        lines.append(f"{index}. {item['title']} ({item['path']}) - {item['summary']}")
    return "\n".join(lines)


def _chat_autonomy_action(
    agent_id: str,
    user_message: str,
    session,
    thread_id: str = "operations",
) -> tuple[str, dict] | None:
    text = user_message.lower()
    actor = agent_id or "orc-orchestrator"

    if actor in {"orc-orchestrator", "sage"} and _chat_is_memory_request(text):
        query = _chat_memory_query(user_message)
        _record_agent_message(
            session,
            "orc-orchestrator",
            "sage",
            "routing",
            f"Sage, search memory for: {query}",
            {"query": query},
            thread_id=thread_id,
        )
        items = _memory_search(query, limit=5)
        detail = _format_memory_hits(items)
        _record_agent_message(
            session,
            "sage",
            "orc-orchestrator",
            "memory_results",
            f"Sage searched memory for '{query}' and found {len(items)} matching item(s).",
            {"query": query, "results": items, "detail": detail},
            thread_id=thread_id,
        )
        reply = f"Sage searched memory for '{query}'.\n\n{detail}"
        return reply, {"autonomy_action": "memory_search", "query": query, "results": items, "detail": detail}

    if actor in {"orc-orchestrator", "raven", "oracle"} and _chat_is_log_review_request(text):
        _record_agent_message(
            session,
            "orc-orchestrator",
            "gate-keeper",
            "policy_check",
            "Gatekeeper classified the requested log review as green: read-only evidence gathering.",
            {"runbook_id": "approved-log-review", "governance": "green"},
            thread_id=thread_id,
        )
        result = execute_runbook(
            "approved-log-review",
            RunbookExecuteIn(target=_chat_extract_target(user_message), rationale=user_message, requested_by="operator"),
        )
        _record_agent_message(
            session,
            "raven",
            "oracle",
            "observation",
            "Raven completed the bounded log-review evidence collection for Oracle.",
            {
                "runbook_execution_id": result["id"],
                "evidence_path": result.get("evidence_path", ""),
                "status": result.get("status", ""),
            },
            thread_id=thread_id,
        )
        reply = (
            f"I ran the green Approved Log Review runbook. Status: {result['status']}. "
            f"{result.get('result') or 'Evidence was recorded.'}"
        )
        if result.get("evidence_path"):
            reply += f"\n\nEvidence: {result['evidence_path']}"
        return reply, {
            "autonomy_action": "runbook_execute",
            "runbook_id": result["runbook_id"],
            "runbook_execution_id": result["id"],
            "status": result["status"],
            "evidence_path": result.get("evidence_path", ""),
            "detail": result.get("result", ""),
        }

    if actor in {"orc-orchestrator", "gate-keeper", "executioner"} and _chat_is_red_action_request(text):
        target = _chat_extract_target(user_message)
        result = execute_runbook(
            "container-redeploy-human-approved",
            RunbookExecuteIn(target=target, rationale=user_message, requested_by="operator"),
        )
        _record_agent_message(
            session,
            "orc-orchestrator",
            "gate-keeper",
            "policy_check",
            f"Gatekeeper classified this as red and requires human approval before Executioner acts on {target}.",
            {
                "approval_id": result.get("approval_id"),
                "runbook_execution_id": result["id"],
                "runbook_id": result["runbook_id"],
                "target": target,
                "governance": "red",
            },
            thread_id=thread_id,
        )
        approval_id = result.get("approval_id")
        reply = (
            f"I prepared the red Container Redeploy Human Approved runbook for {target}. "
            f"It is waiting for Gatekeeper/human approval {approval_id}; Executioner will not act until that approval is granted."
        )
        return reply, {
            "autonomy_action": "approval_gated_runbook",
            "runbook_id": result["runbook_id"],
            "runbook_execution_id": result["id"],
            "approval_id": approval_id,
            "target": target,
            "status": result["status"],
            "detail": result.get("result", ""),
        }

    if actor == "orc-orchestrator" and _chat_is_tool_promotion_request(text):
        tool_id = _chat_field(user_message, "tool_id", "tool id", "id")
        title = _chat_field(user_message, "title", "name") or tool_id
        tests = _chat_field(user_message, "tests", "test summary")
        dry_run = _chat_field(user_message, "dry run", "dry_run", "dry run summary")
        if not (tool_id and title and tests and dry_run):
            reply = (
                "I can prepare a tool promotion from chat, but I need the promotion packet first: "
                "tool_id, title, tests, and dry run. Example: "
                "promote tool; tool_id=log-parser; title=Log Parser; tests=unit tests passed; dry run=parsed sample logs without mutation."
            )
            return reply, {"autonomy_action": "tool_promotion_missing_fields"}
        result = create_tool_promotion(
            ToolPromotionIn(
                tool_id=tool_id,
                title=title,
                test_summary=tests,
                dry_run_summary=dry_run,
                requested_by="operator",
            )
        )
        reply = (
            f"I prepared the generated tool promotion for {result['tool_id']}. "
            f"Status: {result['status']}. Approval {result.get('approval_id')} is required before it enters the worker registry."
        )
        return reply, {
            "autonomy_action": "tool_promotion",
            "promotion_id": result["id"],
            "tool_id": result["tool_id"],
            "approval_id": result.get("approval_id"),
            "artifact_path": result.get("artifact_path", ""),
            "status": result["status"],
        }

    return None


@app.post("/orchestration/tool-promotions/{promotion_id}/promote")
def promote_tool(promotion_id: int) -> dict:
    with SessionLocal() as s:
        row = s.get(ToolPromotion, promotion_id)
        if not row:
            raise HTTPException(404, "Tool promotion not found")
        if not _approval_is_approved(s, row.approval_id):
            raise HTTPException(403, "Human approval is required before tool promotion")
        artifact_file = _safe_markdown_path(REPO_ROOT, row.artifact_path)
        if not artifact_file.exists():
            raise HTTPException(404, "Promotion artifact not found")
        target_dir = _safe_markdown_path(REPO_ROOT / "tools", row.tool_id)
        target_dir.mkdir(parents=True, exist_ok=True)
        target_file = target_dir / "tool.md"
        if target_file.exists():
            raise HTTPException(409, "A promoted tool with this id already exists")
        target_file.write_text(artifact_file.read_text(encoding="utf-8"), encoding="utf-8")
        row.status = "promoted"
        row.decided_at = datetime.now(timezone.utc)
        s.commit()
        s.refresh(row)
        _record_agent_message(
            s,
            "gate-keeper",
            "orc-orchestrator",
            "tool_promoted",
            f"Generated tool promoted into worker registry: {row.tool_id}",
            {"promotion_id": row.id, "path": str(target_file.relative_to(REPO_ROOT))},
        )
        return _tool_promotion_dict(row)


def _oracle_summary(
    window_hours: int = 1,
    friendly_names: dict[str, str] | None = None,
    severities: tuple[str, ...] = ("warning", "error", "critical"),
) -> dict:
    friendly_names = friendly_names or {}
    cutoff = datetime.now(timezone.utc) - timedelta(hours=window_hours)
    with SessionLocal() as s:
        rows = (
            s.query(ObservedEvent, Connection)
            .outerjoin(Connection, ObservedEvent.connection_id == Connection.id)
            .filter(
                ObservedEvent.occurred_at >= cutoff,
                ObservedEvent.severity.in_(list(severities)),
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
        friendly_name = friendly_names.get(event.container_name) or event.container_name
        stack_key = (server, stack)
        key = (server, stack, event.container_name)
        bucket = container_rollup.setdefault(
            key,
            {
                "server": server,
                "stack": stack,
                "container": event.container_name,
                "friendly_name": friendly_name,
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
                "friendly_name": friendly_name,
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
                "friendly_name": item["friendly_name"],
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

    top_pattern_items = sorted(
        pattern_rollup.values(),
        key=lambda x: (-x["errors"], -x["warnings"], -x["count"], x["container"].lower()),
    )

    top_patterns = [
        {
            "server": item["server"],
            "stack": item["stack"],
            "container": item["container"],
            "friendly_name": item["friendly_name"],
            "pattern": item["pattern"],
            "count": item["count"],
            "errors": item["errors"],
            "warnings": item["warnings"],
            "example": item["examples"].most_common(1)[0][0],
        }
        for item in top_pattern_items[:20]
    ]

    top_issues = [
        {
            "rank": idx,
            "server": item["server"],
            "stack": item["stack"],
            "container": item["container"],
            "friendly_name": item["friendly_name"],
            "pattern": item["pattern"],
            "events": item["count"],
            "errors": item["errors"],
            "warnings": item["warnings"],
            "example": item["examples"].most_common(1)[0][0],
        }
        for idx, item in enumerate(top_pattern_items[:3], start=1)
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
        "top_issues": top_issues,
    }


def _oracle_pattern(message: str) -> str:
    text = message.strip().lower()
    text = _ORACLE_UUID_RE.sub("<id>", text)
    text = _ORACLE_NUM_RE.sub("<n>", text)
    text = re.sub(r"\s+", " ", text)
    return text[:220]


_AGENT_DESCRIPTIONS: dict[str, str] = {
    "raven": (
        "You are Raven, the observer and message bus of the ORC platform. "
        "You watch operational activity across containers and infrastructure, publish structured events, "
        "and route messages between agents. You do NOT decide on remediations or mutate infrastructure.\n\n"
        "CRITICAL LIMITATION: You CANNOT change your own polling frequency, poll interval, or any connection "
        "settings. Your polling behavior is controlled externally by the Executioner modifying the connection's "
        "poll_interval_seconds field in the database. If an operator asks you to 'poll every 30 seconds' or "
        "'increase polling frequency', you must respond: "
        "'I cannot modify my own polling interval — that is an infrastructure configuration change. "
        "ORC Orchestrator should route this to Gate Keeper for approval, then Executioner will apply the change.' "
        "If an operator asks you to create or start a focused watch, temporary watch, or increased container-specific monitoring, "
        "you must respond that Executioner creates focused watches after Gate Keeper approval and that Raven only reports observations. "
        "Never generate YAML or plans claiming you have registered or will perform a polling frequency change. "
        "When answering questions, focus on observation, event patterns, routing context, and any active focused watches visible in context."
    ),
    "oracle": (
        "You are The Oracle, the investigator of the ORC platform. "
        "You investigate container changes and anomalies, explain root causes, and recommend next actions. "
        "You show evidence before making recommendations and clearly separate facts from hypotheses. "
        "You route risky infrastructure actions to Gate Keeper for approval. "
        "Read-only analysis skills with approval_required=false are available when they appear in the registry; "
        "do not call them draft or unapproved."
    ),
    "gate-keeper": (
        "You are Gate Keeper, the approval and policy authority of the ORC platform. "
        "You review risky proposed actions, enforce policy, and decide whether execution should proceed. "
        "Speak in plain English. Start with a short summary of what is being requested, then state the decision and any conditions. "
        "Keep the rest compressed. You require human approval for git pulls, restarts, redeploys, and credential changes. "
        "You record who requested what and why for every decision. "
        "Do not approve read-only skill lookup, skill availability checks, or analysis-only skills; those are not execution requests. "
        "If a registered skill has approval_required=false, state that it is available without execution approval."
    ),
    "executioner": (
        "You are Executioner, the approved-action executor of the ORC platform. "
        "You only perform infrastructure actions (git pulls, container refreshes, restarts) "
        "when an approved request exists. You never act without a matching approval. "
        "Do not execute read-only analysis skills or skill availability checks; those belong to ORC, Sage, or the owning agent. "
        "When the request refers to a container, workload, or service by name, use search_containers first. "
        "If more than one match is returned, ask which one the operator means before acting. "
        "When a target name is ambiguous, ask for clarification before acting. "
        "When you finish, respond in plain English with a short summary of exactly what changed."
    ),
    "sage": (
        "You are Sage, the learning and skill authoring agent of the ORC platform. "
        "You capture lessons from incidents, document outcomes, and propose reusable skills. "
        "You do not execute actions. You keep proposed documentation readable. "
        "A skill that exists in the live registry is available according to its metadata; "
        "only uncommitted proposals outside the registry should be described as drafts."
    ),
    "orc-orchestrator": (
        "You are ORC Orchestrator, the master coordinator and AI router of the ORC platform. "
        "Your job is to receive operator requests, reason about which agents and skills are needed, "
        "delegate sub-tasks to the appropriate agents, and synthesize their responses into a clear answer. "
        "Be less literal and prefer practical interpretation over exact wording when the intent is obvious.\n\n"
        "The agents available to you are:\n"
        "- **raven** (observer): Monitors and reports on events and container activity. Use ONLY for read-only observation queries — Raven CANNOT change its own polling frequency or any connection settings.\n"
        "- **oracle** (investigator): Investigates anomalies, explains root causes, recommends fixes. Use for analysis and diagnosis.\n"
        "- **gate-keeper** (approval authority): Reviews and approves infrastructure actions. ALWAYS route to gate-keeper before any change to infrastructure state.\n"
        "- **executioner** (executor): Carries out approved actions using registered tools. Only invoked automatically after gate-keeper approval.\n"
        "- **sage** (learning & skills): Looks up skill definitions, captures lessons, authors new skills.\n\n"
        "CONTEXT RULES:\n"
        "- Treat the Available Skills and Skill Metadata Index as the source of truth for registered skill availability.\n"
        "- A registered skill with approval_required=false is available and is not in draft status.\n"
        "- Skill lookup, skill availability checks, and read-only analysis skills are not infrastructure mutations and must not be routed to gate-keeper or executioner.\n"
        "- Treat the operational configuration snapshot as the source of truth for currently configured connections and poll intervals.\n"
        "- Treat the active focused watches list as the source of truth for temporary container-specific accelerated monitoring.\n"
        "- Treat the target inventory hint as the source of truth for matching containers when it is present.\n"
        "- A request to 'watch a container every X seconds' is a container-observation request first; do not silently convert it into a different connection change.\n"
        "- A request to restart a container or pull git is a target-specific infrastructure action; always identify the exact container/repo/connection before routing execution.\n"
        "- If the target is ambiguous, ask a single short clarification question instead of guessing.\n\n"
        "INFRASTRUCTURE MUTATIONS — always route to gate-keeper (never to raven or directly execute):\n"
        "- Changing poll interval or polling frequency for any connection\n"
        "- Restarting, redeploying, or reconfiguring containers\n"
        "- Git pulls, config changes, credential rotations\n"
        "- Any action that modifies a database record or system setting\n\n"
        "AMBIGUITY RULES:\n"
        "- If a request mentions a container, service, or connection name that matches multiple things, pause and ask which one the operator means.\n"
        "- If the operator does not specify which UAR target to use and more than one matches, say you can watch all matching UAR targets unless they choose one.\n"
        "- Do not guess silently when the target is ambiguous.\n\n"
        "ROUTING FORMAT: When you need to delegate, emit one or more route blocks exactly like this:\n"
        "```route\nagent_id: gate-keeper\ninstruction: Approve changing the UAR connection poll interval to 30 seconds for 10 minutes, then revert.\n```\n\n"
        "RULES:\n"
        "1. Route to sage FIRST if the operator references a skill you need to look up; for an agent's own read-only skill, route directly to that owning agent.\n"
        "2. Route to gate-keeper for ANY infrastructure mutation — including poll interval changes.\n"
        "3. If the request is a simple question you can answer from context, answer directly — no routing needed.\n"
        "4. If a request names a container or workload and more than one match exists, ask which one before routing execution.\n"
        "5. After agents respond, execute any remaining routes.\n"
        "6. Final answers to the operator must be 2-4 sentences maximum. No bullet lists. No markdown headers. Plain concise language.\n"
        "7. Never fabricate agent responses — only summarize what the agents actually said.\n"
        "8. Do NOT route 'poll every X seconds' requests to raven — raven cannot change poll intervals. Route to gate-keeper instead.\n"
        "9. Do NOT route focused watch creation, increased monitoring frequency, or temporary container-specific watch setup to raven. Those must go to gate-keeper, then executioner. Raven only reports what it sees.\n"
        "10. Do NOT ask Gate Keeper to approve a skill merely because Sage once drafted it; if the skill is listed with approval_required=false, it is already available."
    ),
}

_AGENT_SYSTEM_BASE = (
    "\n\nYou are operating inside ORC, a policy-driven agent orchestration platform for "
    "infrastructure monitoring and safe automated remediation. "
    "The operator is a human administrator who communicates with you through the ORC chat interface. "
    "Be concise, accurate, and stay in character. Put the plain-English summary first, keep detail compressed, "
    "and use Markdown only when it clearly improves readability."
)


def _agent_system_prompt(agent: AgentRuntimeState) -> str:
    description = _AGENT_DESCRIPTIONS.get(
        agent.agent_id,
        f"You are {agent.name}, a {agent.role} agent inside the ORC platform.",
    )
    return description + _AGENT_SYSTEM_BASE


def _build_orc_context(session, agent_id: str = "") -> str:
    """Build a context block for agent chat. Puts this agent's skills first."""
    lines: list[str] = ["## ORC Platform Context\n"]

    # Agents
    agents = session.query(AgentRuntimeState).order_by(AgentRuntimeState.name).all()
    lines.append("### Registered Agents")
    for a in agents:
        status = "enabled" if a.enabled else "disabled"
        lines.append(f"- **{a.name}** (id: `{a.agent_id}`, role: {a.role}, trust: {a.trust_mode}, {status})")
    lines.append("")

    lines.append("### Operational Configuration Snapshot")
    connections = session.query(Connection).order_by(Connection.name).all()
    if not connections:
        lines.append("- No enabled connections are registered.")
    else:
        for c in connections:
            poll = f"{c.poll_interval_seconds}s" if c.poll_interval_seconds else "auto/default"
            server = c.server_name or c.name
            status = c.last_status or "unknown"
            lines.append(
                f"- **{server}** (connection: `{c.name}`, id: `{c.id}`, enabled: {str(bool(c.enabled)).lower()}, "
                f"poll: {poll}, status: {status})"
            )
    lines.append("")

    lines.append("### Active Focused Watches")
    now = datetime.now(timezone.utc)
    watches = (
        session.query(FocusedWatch, Connection)
        .join(Connection, FocusedWatch.connection_id == Connection.id)
        .filter(FocusedWatch.expires_at > now)
        .order_by(FocusedWatch.expires_at.asc(), FocusedWatch.id.asc())
        .all()
    )
    if not watches:
        lines.append("- No active focused container watches.")
    else:
        for watch, conn in watches:
            server = conn.server_name or conn.name
            lines.append(
                f"- Watch `{watch.id}`: {watch.container_name} on {server} every {watch.interval_seconds}s "
                f"until {watch.expires_at.isoformat()}"
            )
    lines.append("")

    # Load all skills and split into mine vs others
    skill_items = load_registry(REPO_ROOT, "skills")
    mine: list = []
    others: list = []
    for item in skill_items:
        skill_path = REPO_ROOT / item.path
        try:
            content = skill_path.read_text(encoding="utf-8")
        except Exception:
            content = ""
        # Parse the agent: field from the skill file
        assigned = ""
        for line in content.splitlines():
            if line.strip().startswith("agent:"):
                assigned = line.split(":", 1)[1].strip()
                break
        entry = (item.name, item.item_id, content.strip(), assigned)
        if agent_id and assigned == agent_id:
            mine.append(entry)
        else:
            others.append(entry)

    def _render_skill(name, sid, content, _agent):
        return f"\n#### {name} (id: `{sid}`)\n{content}"

    if agent_id and mine:
        lines.append("### Your Assigned Skills")
        for e in mine:
            lines.append(_render_skill(*e))
        lines.append("")

    label = "### Other Available Skills" if (agent_id and mine) else "### Available Skills"
    lines.append(label)
    if not others and not mine:
        lines.append("- No skills registered.")
    elif not others:
        lines.append("- No other skills.")
    else:
        for e in others:
            assigned_note = f" (assigned to: {e[3]})" if e[3] else ""
            lines.append(_render_skill(e[0], e[1], e[2], e[3]) + (f"\n*Assigned to: {e[3]}*" if e[3] else ""))
    lines.append("")

    lines.append("### Skill Metadata Index")
    for item in skill_items:
        skill_path = REPO_ROOT / item.path
        try:
            content = skill_path.read_text(encoding="utf-8")
        except Exception:
            content = ""
        metadata: dict[str, str] = {}
        for line in content.splitlines():
            line = line.strip()
            if not line or line.startswith("#") or ":" not in line:
                continue
            key, value = line.split(":", 1)
            key = key.strip().lower()
            if key in {"name", "id", "version", "category", "risk_level", "approval_required", "agent"}:
                metadata[key] = value.strip()
        if metadata:
            display = ", ".join(f"{k}={v}" for k, v in metadata.items())
            lines.append(f"- {display}")
    lines.append("")

    return "\n".join(lines)


def _recent_thread_context(session, thread_id: str = "operations", limit: int = 14) -> str:
    rows = (
        session.query(AgentMessage)
        .filter(AgentMessage.thread_id == (thread_id or "operations"))
        .order_by(AgentMessage.created_at.desc(), AgentMessage.id.desc())
        .limit(limit)
        .all()
    )
    if not rows:
        return ""
    lines = ["### Recent Conversation"]
    for row in reversed(rows):
        source = row.source_agent or "system"
        target = f" -> {row.target_agent}" if row.target_agent else ""
        summary = re.sub(r"\s+", " ", row.summary or "").strip()
        if len(summary) > 220:
            summary = summary[:217] + "..."
        lines.append(f"- {source}{target} [{row.message_type}]: {summary}")
    return "\n".join(lines)


def _parse_route_blocks(text: str) -> list[dict]:
    """Extract ```route ... ``` blocks from an LLM response."""
    pattern = re.compile(r"```route\s*\n(.*?)```", re.DOTALL)
    results = []
    for match in pattern.finditer(text):
        block = match.group(1).strip()
        entry: dict = {}
        for line in block.splitlines():
            if ":" in line:
                k, v = line.split(":", 1)
                entry[k.strip()] = v.strip()
        if entry.get("agent_id") and entry.get("instruction"):
            results.append({"agent_id": entry["agent_id"], "instruction": entry["instruction"]})
    return results


def _find_connection_candidates(session, term: str) -> list[dict]:
    needle = term.strip().lower()
    if not needle:
        return []
    matches: list[dict] = []
    for conn in session.query(Connection).filter_by(enabled=True).all():
        fields = [conn.name or "", conn.server_name or ""]
        if any(needle in value.lower() for value in fields):
            matches.append({
                "id": conn.id,
                "name": conn.name,
                "server_name": conn.server_name or "",
                "poll_interval_seconds": conn.poll_interval_seconds,
                "last_status": conn.last_status or "",
            })
    return matches


def _find_connection_by_label(session, label: str) -> Connection | None:
    needle = label.strip().lower()
    if not needle:
        return None
    candidates = session.query(Connection).filter_by(enabled=True).all()
    for conn in candidates:
        if needle == (conn.server_name or "").lower() or needle == (conn.name or "").lower():
            return conn
    for conn in candidates:
        if needle in (conn.server_name or "").lower() or needle in (conn.name or "").lower():
            return conn
    return None


def _mentioned_connection(session, text: str) -> Connection | None:
    raw = text.lower()
    for conn in session.query(Connection).filter_by(enabled=True).all():
        names = [conn.name or "", conn.server_name or ""]
        if any(name and name.lower() in raw for name in names):
            return conn
    return None


def _extract_container_target_phrase(user_message: str) -> str:
    text = user_message.strip()
    patterns = [
        r"\b(?:monitor|watch|poll|observe|inspect|track|review|diagnose|restart|refresh|revert|change|update|restart)\s+(?:the\s+)?(.+?)(?:\s+(?:for|every|each|in|on|with|using|via|to|then|and|after)\b|[?.!,]|$)",
        r"\b(?:container|service|app|connection)\s+(?:named|called)?\s+(.+?)(?:\s+(?:for|every|each|in|on|with|using|via|to|then|and|after)\b|[?.!,]|$)",
    ]
    for pattern in patterns:
        m = re.search(pattern, text, flags=re.I)
        if m:
            candidate = m.group(1).strip(" \"'`.,;:()[]{}")
            if candidate:
                return candidate
    return ""


def _find_container_candidates(session, term: str) -> list[dict]:
    needle = term.strip().lower()
    if not needle:
        return []
    matches: list[dict] = []
    connections = session.query(Connection).filter_by(enabled=True).all()
    for conn in connections:
        client = PortainerClient(conn.base_url, conn.api_token)
        try:
            endpoints = client.get_endpoints()
        except Exception:
            continue
        for ep in endpoints:
            endpoint_id = ep.get("Id")
            if endpoint_id is None:
                continue
            try:
                containers = client.get_containers(endpoint_id)
            except Exception:
                continue
            for c in containers:
                cid = c.get("Id", "")
                cname = (c.get("Names") or [f"/{cid[:12]}"])[0].lstrip("/")
                labels = c.get("Labels") or {}
                stack_name = labels.get("com.docker.compose.project") or _infer_stack(cname)
                if any(needle in value.lower() for value in [cname, stack_name, conn.name or "", conn.server_name or ""]):
                    matches.append({
                        "connection_id": conn.id,
                        "server": conn.name,
                        "server_name": conn.server_name or conn.name,
                        "endpoint_id": endpoint_id,
                        "container_id": cid,
                        "container_name": cname,
                        "stack_name": stack_name,
                    })
    return matches


def _find_container_candidates_for_connection(session, term: str, connection: Connection | None) -> list[dict]:
    matches = _find_container_candidates(session, term)
    if not connection:
        return matches
    return [m for m in matches if int(m["connection_id"]) == int(connection.id)]


def _target_inventory_context(session, user_message: str) -> str:
    phrase = _extract_container_target_phrase(user_message)
    if not phrase:
        return ""
    connection = _mentioned_connection(session, user_message)
    matches = _find_container_candidates_for_connection(session, phrase, connection)
    if not matches:
        scope = f" on {connection.server_name or connection.name}" if connection else ""
        return f"### Target Inventory Hint\n- No live container inventory matches were found for `{phrase}`{scope}.\n"
    lines = ["### Target Inventory Hint"]
    if connection:
        lines.append(f"- Requested connection: {connection.server_name or connection.name} (id: `{connection.id}`)")
    lines.append(f"- Matching term: `{phrase}`")
    for match in matches[:12]:
        lines.append(
            f"- Match: {match['container_name']} on {match['server_name']} "
            f"(connection `{match['connection_id']}`, endpoint `{match['endpoint_id']}`)"
        )
    if len(matches) > 12:
        lines.append(f"- And {len(matches) - 12} more matches.")
    return "\n".join(lines) + "\n"


def _clarify_ambiguous_target(user_message: str, session) -> str | None:
    phrase = _extract_container_target_phrase(user_message)
    if not phrase:
        return None

    candidates = _find_container_candidates(session, phrase)
    if len(candidates) <= 1:
        return None

    labels = []
    for c in candidates[:5]:
        labels.append(
            f"{c['container_name']} on {c['server_name']} (connection {c['connection_id']})"
        )

    if len(candidates) > 5:
        labels.append(f"and {len(candidates) - 5} more")

    return (
        f"I found more than one match for '{phrase}', so I need one more detail before I route this. "
        f"The matches are: {', '.join(labels)}. "
        "Tell me which one to use, or say 'all of them' and I’ll treat it as a broader watch."
    )


def _agent_chat_internal(agent_id: str, instruction: str, session, extra_context: str = "", thread_id: str = "operations") -> str:
    """Call an agent's LLM, store the exchange as AgentMessages, return the reply."""
    agent = session.query(AgentRuntimeState).filter_by(agent_id=agent_id).first()
    if not agent:
        return f"*(Agent '{agent_id}' not found)*"
    if not agent.enabled:
        return f"*(Agent '{agent.name}' is disabled)*"

    context = _build_orc_context(session, agent_id)
    recent = _recent_thread_context(session, thread_id)
    if recent:
        context += f"\n\n{recent}"
    inventory_hint = _target_inventory_context(session, instruction)
    if inventory_hint:
        context += f"\n\n{inventory_hint}"
    if extra_context:
        context += f"\n\n{extra_context}"

    messages: list[dict] = [
        {"role": "system", "content": _agent_system_prompt(agent)},
        {"role": "user", "content": context},
        {"role": "assistant", "content": "Understood. I have reviewed the ORC platform context and my assigned skills."},
        {"role": "user", "content": instruction},
    ]

    reply = _llm_call(messages, agent_id, "agent_chat", session)
    if not reply:
        reply = f"*(No response from {agent.name})*"
    return reply


_EXECUTION_APPROVAL_KEYWORDS = (
    "restart",
    "redeploy",
    "refresh",
    "git pull",
    "pull from git",
    "update container",
    "recreate container",
    "change poll",
    "poll interval",
    "polling frequency",
    "focused watch",
    "temporary watch",
    "increase monitoring",
    "credential",
    "secret",
    "config change",
    "configuration change",
    "delete",
    "remove",
    "rotate",
)

_READ_ONLY_SKILL_KEYWORDS = (
    "skill",
    "draft",
    "available",
    "availability",
    "approved",
    "critical error review",
    "hourly critical error review",
)


def _requires_execution_approval(instruction: str, rationale: str = "") -> bool:
    """Return true only for approvals that should wake Executioner."""
    text = f"{instruction or ''}\n{rationale or ''}".lower()
    has_execution_action = any(keyword in text for keyword in _EXECUTION_APPROVAL_KEYWORDS)
    if not has_execution_action:
        return False
    if any(keyword in text for keyword in _READ_ONLY_SKILL_KEYWORDS) and not any(
        keyword in text
        for keyword in (
            "restart",
            "redeploy",
            "refresh",
            "git pull",
            "poll interval",
            "polling frequency",
            "focused watch",
            "credential",
            "secret",
            "delete",
            "remove",
            "rotate",
        )
    ):
        return False
    return True


def _auto_create_approval(instruction: str, gate_keeper_reply: str, session) -> ApprovalRequest:
    """Create an ApprovalRequest when Gate Keeper is involved in a routing decision."""
    req = ApprovalRequest(
        title=instruction[:256],
        requester_agent="orc-orchestrator",
        approver_agent="gate-keeper",
        action_type="agent_instruction",
        target="operator_request",
        rationale=(gate_keeper_reply or instruction)[:500],
        risk_level="medium",
        status="pending",
        requested_by="operator",
        requested_at=datetime.now(timezone.utc),
    )
    session.add(req)
    session.commit()
    session.refresh(req)
    try:
        from . import raven as _raven
        _raven.publish({"type": "approval_request", "title": req.title, "status": "pending"})
    except Exception:
        pass
    return req


def _run_orc_loop(user_message: str, session, thread_id: str = "operations") -> str:
    """ORC Orchestrator iterative multi-agent routing loop. Up to 3 ORC turns."""
    MAX_ROUNDS = 3
    MAX_ROUTES_PER_ROUND = 3

    orc = session.query(AgentRuntimeState).filter_by(agent_id="orc-orchestrator").first()
    if not orc:
        return "*(ORC Orchestrator agent not found)*"

    if _is_hourly_critical_error_review_request(user_message):
        instruction = "Run the Hourly Critical Error Review and report the results back to ORC."
        _record_agent_message(session, "orc-orchestrator", "oracle", "routing", instruction, thread_id=thread_id)
        result = _run_hourly_critical_error_review(session)
        _record_agent_message(
            session,
            "oracle",
            "orc-orchestrator",
            "analysis_result",
            result["message"],
            result["payload"],
            thread_id=thread_id,
        )
        return "The Oracle completed the Hourly Critical Error Review and posted the results here."

    orc_context = _build_orc_context(session, "orc-orchestrator")
    recent = _recent_thread_context(session, thread_id)
    if recent:
        orc_context += f"\n\n{recent}"
    inventory_hint = _target_inventory_context(session, user_message)
    if inventory_hint:
        orc_context += f"\n\n{inventory_hint}"

    # Build the conversation that persists across rounds
    conversation: list[dict] = [
        {"role": "system", "content": _agent_system_prompt(orc)},
        {"role": "user", "content": orc_context},
        {"role": "assistant", "content": "Understood. I have reviewed the platform context, all registered agents, and available skills."},
        {"role": "user", "content": user_message},
    ]

    last_reply = ""
    for _round in range(MAX_ROUNDS):
        endpoint = "orchestration" if _round == 0 else "orchestration_synthesis"
        orc_reply = _llm_call(conversation, "orc-orchestrator", endpoint, session)
        last_reply = orc_reply

        routes = _parse_route_blocks(orc_reply)

        if not routes:
            # ORC gave a direct answer — done
            return orc_reply

        # Store ORC's routing plan for this round
        _record_agent_message(session, "orc-orchestrator", "", "routing_plan", orc_reply, thread_id=thread_id)
        conversation.append({"role": "assistant", "content": orc_reply})

        # Execute each route in this round
        agent_responses: list[str] = []
        for route in routes[:MAX_ROUTES_PER_ROUND]:
            target_id = route["agent_id"]
            instruction = route["instruction"]

            _record_agent_message(session, "orc-orchestrator", target_id, "routing", instruction, thread_id=thread_id)
            approval_row = None
            agent_payload = None
            if target_id == "oracle" and _is_hourly_critical_error_review_request(instruction):
                result = _run_hourly_critical_error_review(session)
                agent_reply = result["message"]
                agent_payload = result["payload"]
            else:
                agent_reply = _agent_chat_internal(target_id, instruction, session, thread_id=thread_id)

            # Gate Keeper: auto-create an ApprovalRequest only for actual execution work.
            # Read-only skill checks should never wake Executioner.
            if target_id == "gate-keeper" and _requires_execution_approval(instruction, agent_reply):
                approval_row = _auto_create_approval(instruction, agent_reply, session)

            _record_agent_message(
                session,
                target_id,
                "orc-orchestrator",
                "response",
                agent_reply,
                {"approval_id": approval_row.id} if approval_row else agent_payload,
                thread_id=thread_id,
            )

            if approval_row and approval_row.status == "pending":
                return (
                    f"Waiting for approval {approval_row.id}. "
                    "Open the approval from chat or the Approvals tab, and Executioner will continue after you approve it."
                )

            target_agent = session.query(AgentRuntimeState).filter_by(agent_id=target_id).first()
            agent_name = target_agent.name if target_agent else target_id
            agent_responses.append(f"### {agent_name}:\n{agent_reply}")

        # Feed all responses back into the conversation for the next ORC turn
        agent_context = "\n\n".join(agent_responses)
        conversation.append({
            "role": "user",
            "content": (
                f"Agent responses received:\n\n{agent_context}\n\n"
                "Now provide your next routing decision OR, if all work is done, "
                "give the operator a concise final summary (2-4 sentences). "
                "Do NOT include route blocks in a final summary."
            ),
        })

    # Exhausted all rounds — strip any dangling route blocks and return last reply
    clean = re.sub(r"```route.*?```", "[routed]", last_reply, flags=re.DOTALL).strip()
    return clean or last_reply


def _oracle_prompt(summary: dict) -> list[dict[str, str]]:
    system_prompt = (
        "You are The Oracle inside ORC, an operations advisor reviewing recent "
        "container operational events collected from Portainer-managed applications. "
        "Use only summary.top_issues and show at most three issues. "
        "Each issue must include the friendly_name, falling back to the container if needed. "
        "Respond in plain text using Markdown bold for important information. "
        "Use exactly this compact format for each issue:\n"
        "1. **<friendly_name>** (`<container>`) - **Issue:** <brief issue summary and frequency>.\n"
        "   **Possible root cause:** <most likely cause>.\n"
        "   **What I should do:** <specific fix action>.\n"
        "Do not add sections, preambles, or extra issues. Keep each issue brief and specific."
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


def _llm_call(messages: list[dict], agent_id: str, endpoint: str, session) -> str:
    """Call OpenAI, log usage, and return the response text."""
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        raise HTTPException(
            status_code=503,
            detail="AI is not configured. Set OPENAI_API_KEY in the environment.",
        )

    model = os.getenv("OPENAI_MODEL", DEFAULT_OPENAI_MODEL).strip() or DEFAULT_OPENAI_MODEL
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    payload = {"model": model, "messages": messages}

    t0 = _time.monotonic()
    try:
        resp = httpx.post(OPENAI_API_URL, headers=headers, json=payload, timeout=60.0)
        resp.raise_for_status()
        data = resp.json()
        content = data["choices"][0]["message"]["content"].strip()
    except httpx.HTTPStatusError as exc:
        detail = exc.response.text[:400] if exc.response is not None else str(exc)
        raise HTTPException(status_code=502, detail=f"LLM request failed: {detail}") from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"LLM request failed: {exc}") from exc
    finally:
        response_ms = int((_time.monotonic() - t0) * 1000)

    usage = data.get("usage", {})
    try:
        log = AIUsageLog(
            agent_id=agent_id,
            endpoint=endpoint,
            model=model,
            prompt_tokens=usage.get("prompt_tokens", 0),
            completion_tokens=usage.get("completion_tokens", 0),
            total_tokens=usage.get("total_tokens", 0),
            response_ms=response_ms,
        )
        session.add(log)
        session.commit()
    except Exception:
        pass

    return content or ""


def _llm_call_with_tools(
    messages: list[dict], tools: list[dict], agent_id: str, endpoint: str, session
) -> tuple[str, list[dict]]:
    """Call OpenAI with function-calling enabled. Returns (text, []) when done or ("", tool_calls) when tools are needed."""
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        raise HTTPException(status_code=503, detail="AI is not configured. Set OPENAI_API_KEY in the environment.")

    model = os.getenv("OPENAI_MODEL", DEFAULT_OPENAI_MODEL).strip() or DEFAULT_OPENAI_MODEL
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    payload = {"model": model, "messages": messages, "tools": tools, "tool_choice": "auto"}

    t0 = _time.monotonic()
    data: dict = {}
    try:
        resp = httpx.post(OPENAI_API_URL, headers=headers, json=payload, timeout=60.0)
        resp.raise_for_status()
        data = resp.json()
    except httpx.HTTPStatusError as exc:
        detail = exc.response.text[:400] if exc.response is not None else str(exc)
        raise HTTPException(status_code=502, detail=f"LLM request failed: {detail}") from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"LLM request failed: {exc}") from exc
    finally:
        response_ms = int((_time.monotonic() - t0) * 1000)

    usage = data.get("usage", {})
    try:
        log_entry = AIUsageLog(
            agent_id=agent_id, endpoint=endpoint, model=model,
            prompt_tokens=usage.get("prompt_tokens", 0),
            completion_tokens=usage.get("completion_tokens", 0),
            total_tokens=usage.get("total_tokens", 0),
            response_ms=response_ms,
        )
        session.add(log_entry)
        session.commit()
    except Exception:
        pass

    msg = data["choices"][0]["message"]
    tool_calls = msg.get("tool_calls") or []
    if tool_calls:
        return "", tool_calls
    return (msg.get("content") or "").strip(), []


def _run_executioner(approval_id: int, instruction: str, rationale: str, session) -> None:
    """Run the Executioner tool-calling loop to carry out an approved action."""
    import json as _json
    from .tools import call_tool, get_tool_schemas

    tools = get_tool_schemas()
    messages: list[dict] = [
        {
            "role": "system",
            "content": (
                "You are ORC Executioner. Your job is to carry out approved infrastructure actions "
                "using the provided tools. Be precise: use list_connections first to find the correct "
                "connection, and if the name is ambiguous use the most specific match or ask for clarification before acting. "
                "For container-specific focused watches, use search_containers followed by create_focused_container_watch. "
                "Do not use modify_connection_poll_interval unless the request explicitly asks to change the whole connection cadence. "
                "After completing all actions, respond with a concise plain-text summary of exactly what was done."
            ),
        },
        {
            "role": "user",
            "content": f"Approved instruction: {instruction}\n\nRationale: {rationale}",
        },
    ]

    result_text = ""
    for _ in range(5):
        text, tool_calls = _llm_call_with_tools(messages, tools, "executioner", "execution", session)
        if text:
            result_text = text
            break
        if not tool_calls:
            result_text = "*(Executioner produced no output)*"
            break
        # Append the assistant turn with its tool_calls
        messages.append({"role": "assistant", "content": None, "tool_calls": tool_calls})
        # Execute each tool and feed results back
        for tc in tool_calls:
            fn_name = tc["function"]["name"]
            args = _json.loads(tc["function"].get("arguments", "{}"))
            result = call_tool(fn_name, args)
            messages.append({
                "role": "tool",
                "tool_call_id": tc["id"],
                "content": _json.dumps(result),
            })

    _record_agent_message(
        session,
        "executioner",
        "operator",
        "execution_result",
        result_text or "*(Execution complete)*",
        {"approval_id": approval_id},
    )


def _oracle_review(summary: dict, session) -> str:
    if not summary["total_events"]:
        return (
            "**No issues found in the last hour.**\n\n"
            "**What I should do:** Keep monitoring and re-run the Oracle when Raven captures fresh critical or error events."
        )
    return _llm_call(_oracle_prompt(summary), "oracle", "oracle_review", session)


def _is_hourly_critical_error_review_request(text: str) -> bool:
    value = (text or "").lower()
    if "why" in value and any(phrase in value for phrase in ("not post", "not posted", "didn't post", "did not post", "not respond", "didn't respond", "did not respond")):
        return False
    mentions_review = (
        "hourly critical error review" in value
        or "critical error review" in value
        or ("hourly" in value and "critical" in value and "error" in value and "review" in value)
    )
    if not mentions_review:
        return False
    return any(
        word in value
        for word in (
            "run",
            "execute",
            "perform",
            "start",
            "activate",
            "review",
            "analyze",
            "analyse",
            "post",
            "report",
            "results",
        )
    )


def _format_hourly_critical_error_review(summary: dict, analysis: str) -> str:
    return (
        "**Hourly Critical Error Review completed.**\n\n"
        f"Window: {summary['window_start']} to {summary['window_end']}\n"
        f"Critical/error events: {summary['errors']}; affected containers: {summary['unique_containers']}.\n\n"
        f"{analysis}"
    )


def _run_hourly_critical_error_review(session) -> dict:
    summary = _oracle_summary(window_hours=1, severities=("error", "critical"))
    analysis = _oracle_review(summary, session)
    message = _format_hourly_critical_error_review(summary, analysis)
    payload = {
        "skill_id": "hourly-critical-error-review",
        "window_start": summary["window_start"],
        "window_end": summary["window_end"],
        "total_events": summary["total_events"],
        "errors": summary["errors"],
        "warnings": summary["warnings"],
        "unique_containers": summary["unique_containers"],
        "top_issues": summary["top_issues"],
    }
    return {"message": message, "payload": payload, "summary": summary, "analysis": analysis}


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
            server_name=body.server_name.strip() or None,
            logo_data=body.logo_data.strip() or None,
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
        c.server_name = body.server_name.strip() or None
        c.logo_data = body.logo_data.strip() or None
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
    from .ingest import parse_logs
    from . import raven as _raven

    with SessionLocal() as s:
        c = s.get(Connection, cid)
        if not c:
            raise HTTPException(404, "Not found")
        conn_id, name, server_display, url, token = c.id, c.name, c.server_name or c.name, c.base_url, c.api_token

    client = PortainerClient(url, token)
    try:
        endpoints = client.get_endpoints()
    except Exception as exc:
        _raven.publish({"type": "poll_error", "server": server_display, "server_key": name, "error": str(exc)})
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
            containers = client.get_running_containers(eid)
        except Exception:
            continue
        for container in containers:
            cid_c = container["Id"]
            cname = (container.get("Names") or [f"/{cid_c[:12]}"])[0].lstrip("/")
            _raven.publish({"type": "container_checking", "server": server_display, "server_key": name, "container": cname})
            try:
                event_count = 0
                err_c = 0
                warn_c = 0
                issue_payloads: list[dict] = []
                with SessionLocal() as session:
                    chk = session.query(IngestionCheckpoint).filter_by(
                        connection_id=conn_id, endpoint_id=eid, container_id=cid_c
                    ).first()
                    if not chk:
                        chk = IngestionCheckpoint(
                            connection_id=conn_id, endpoint_id=eid,
                            container_id=cid_c, last_unix_ts=0, poll_count=0,
                        )
                        session.add(chk)
                        session.flush()

                    since = chk.last_unix_ts or 0
                    raw = client.get_container_logs(eid, cid_c, since=since)
                    events, last_ts = parse_logs(raw, conn_id, eid, cid_c, cname)
                    event_count = len(events)
                    err_c = sum(1 for e in events if e.severity in ("error", "critical"))
                    warn_c = sum(1 for e in events if e.severity == "warning")
                    chk.poll_count = (chk.poll_count or 0) + 1

                    if events:
                        session.add_all(events)
                        session.flush()
                        issue_payloads = _raven.issue_event_payloads(server_display, events, server_key=name)
                        chk.last_unix_ts = last_ts

                    session.commit()
                for payload in issue_payloads:
                    _raven.publish(payload)
                _raven.publish({
                    "type": "container_result",
                    "server": server_display, "server_key": name, "container": cname,
                    "events": event_count, "errors": err_c, "warnings": warn_c,
                    "issue_events": len(issue_payloads),
                })
                total_events += event_count
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
# Routes — Admin Settings + Retention
# ---------------------------------------------------------------------------

@app.get("/admin/settings")
def admin_get_settings(request: Request) -> dict:
    _require_admin(request)
    with SessionLocal() as s:
        rows = s.query(SystemSetting).all()
        settings = {row.key: row.value for row in rows}
    return {"settings": settings}


class SettingUpdateIn(BaseModel):
    value: str


@app.put("/admin/settings/{key}")
def admin_update_setting(key: str, body: SettingUpdateIn, request: Request) -> dict:
    _require_admin(request)
    now = datetime.now(timezone.utc)
    with SessionLocal() as s:
        row = s.get(SystemSetting, key)
        if row:
            row.value = body.value.strip()
            row.updated_at = now
        else:
            s.add(SystemSetting(key=key, value=body.value.strip(), updated_at=now))
        s.commit()
    return {"ok": True, "key": key, "value": body.value.strip()}


@app.get("/admin/event-stats")
def admin_event_stats(request: Request) -> dict:
    _require_admin(request)
    with SessionLocal() as s:
        total = s.query(func.count(ObservedEvent.id)).scalar() or 0
        oldest = s.query(func.min(ObservedEvent.occurred_at)).scalar()
        newest = s.query(func.max(ObservedEvent.occurred_at)).scalar()
        by_severity = (
            s.query(ObservedEvent.severity, func.count(ObservedEvent.id))
            .group_by(ObservedEvent.severity)
            .all()
        )
    return {
        "total_events": total,
        "oldest_event": oldest.isoformat() if oldest else None,
        "newest_event": newest.isoformat() if newest else None,
        "by_severity": {sev: cnt for sev, cnt in by_severity},
    }


@app.post("/admin/prune-events")
def admin_prune_events(request: Request) -> dict:
    _require_admin(request)
    with SessionLocal() as s:
        row = s.get(SystemSetting, "event_retention_days")
        days = max(1, int(row.value)) if row else 30
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    with SessionLocal() as s:
        deleted = (
            s.query(ObservedEvent)
            .filter(ObservedEvent.occurred_at < cutoff)
            .delete(synchronize_session=False)
        )
        s.commit()
    return {"ok": True, "deleted": deleted, "retention_days": days, "cutoff": cutoff.isoformat()}


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


@app.get("/registry/tools")
def registry_tools() -> dict:
    items = load_registry(REPO_ROOT, "tools")
    return {"count": len(items), "items": [asdict(item) for item in items]}


@app.get("/registry/runbooks")
def registry_runbooks() -> dict:
    items = load_registry(REPO_ROOT, "runbooks")
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
                "server_name": c.server_name if c and c.server_name else (c.name if c else "—"),
                "container_name": e.container_name,
                "severity": e.severity,
                "message": e.message,
                "occurred_at": e.occurred_at.isoformat(),
            }
            for e, c in rows
        ],
    }


@app.post("/oracle/review")
def review_with_oracle(body: OracleReviewIn | None = None) -> dict:
    friendly_names = body.friendly_names if body else {}
    summary = _oracle_summary(window_hours=1, friendly_names=friendly_names)
    with SessionLocal() as s:
        analysis = _oracle_review(summary, s)
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


@app.get("/orchestration/tools")
def list_tools(request: Request) -> dict:
    _require_admin(request)
    from .tools import _REGISTRY
    return {
        "tools": [
            {"name": k, "description": v["schema"]["function"]["description"]}
            for k, v in _REGISTRY.items()
        ]
    }


@app.get("/admin/ai-usage")
def get_ai_usage(request: Request, days: int = 30) -> dict:
    _require_admin(request)
    days = max(1, min(days, 365))
    today = datetime.now(timezone.utc).date()
    start_date = today - timedelta(days=days - 1)
    cutoff = datetime.combine(start_date, datetime.min.time(), tzinfo=timezone.utc)
    with SessionLocal() as s:
        rows = (
            s.query(AIUsageLog)
            .filter(AIUsageLog.request_at >= cutoff)
            .order_by(AIUsageLog.request_at.asc())
            .all()
        )

    # Group by agent + model + date
    by_day: dict = defaultdict(lambda: {"calls": 0, "prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0})
    seen: dict = {}

    totals = {"calls": 0, "prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
    for row in rows:
        request_at = row.request_at
        if request_at.tzinfo is None:
            request_at = request_at.replace(tzinfo=timezone.utc)
        date_str = request_at.astimezone(timezone.utc).date().isoformat()
        key = f"{row.agent_id}|{row.model}|{date_str}"
        if key not in seen:
            seen[key] = {"agent_id": row.agent_id, "model": row.model, "date": date_str,
                         "calls": 0, "prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
        seen[key]["calls"] += 1
        seen[key]["prompt_tokens"] += row.prompt_tokens
        seen[key]["completion_tokens"] += row.completion_tokens
        seen[key]["total_tokens"] += row.total_tokens

        by_day[date_str]["calls"] += 1
        by_day[date_str]["prompt_tokens"] += row.prompt_tokens
        by_day[date_str]["completion_tokens"] += row.completion_tokens
        by_day[date_str]["total_tokens"] += row.total_tokens

        totals["calls"] += 1
        totals["prompt_tokens"] += row.prompt_tokens
        totals["completion_tokens"] += row.completion_tokens
        totals["total_tokens"] += row.total_tokens

    daily = []
    for offset in range(days):
        date_str = (start_date + timedelta(days=offset)).isoformat()
        daily.append({"date": date_str, **by_day[date_str]})
    return {"rows": list(seen.values()), "daily": daily, "totals": totals}


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

        line_rows = s.query(
            ObservedEvent.connection_id, ObservedEvent.container_id,
            func.count(ObservedEvent.id).label("n")
        ).filter(
            ObservedEvent.occurred_at >= cutoff
        ).group_by(ObservedEvent.connection_id, ObservedEvent.container_id).all()

        poll_rows = s.query(
            IngestionCheckpoint.connection_id, IngestionCheckpoint.container_id,
            func.sum(IngestionCheckpoint.poll_count).label("n")
        ).group_by(
            IngestionCheckpoint.connection_id, IngestionCheckpoint.container_id
        ).all()

    errs  = {(r.connection_id, r.container_id): int(r.n) for r in err_rows}
    warns = {(r.connection_id, r.container_id): int(r.n) for r in warn_rows}
    lines = {(r.connection_id, r.container_id): int(r.n) for r in line_rows}
    polls = {(r.connection_id, r.container_id): int(r.n) for r in poll_rows}

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
                        "total_lines": lines.get((conn.id, cid), 0),
                        "lines_queried": lines.get((conn.id, cid), 0),
                        "polls": polls.get((conn.id, cid), 0),
                    })
            except Exception:
                continue
        for sname, containers in sorted(stacks.items()):
            stacks_out.append({
                "name": sname, "server": conn.name,
                "server_name": conn.server_name or conn.name,
                "server_logo": conn.logo_data or "",
                "character": _stack_character(sname),
                "containers": sorted(containers, key=lambda x: x["type"]),
            })
    return {"hours": hours, "stacks": stacks_out}


def _cdct(c: Connection) -> dict:
    return {
        "id": c.id, "name": c.name, "type": c.type, "base_url": c.base_url,
        "api_token": c.api_token, "enabled": c.enabled,
        "poll_interval_seconds": c.poll_interval_seconds,
        "server_name": c.server_name or "",
        "logo_data": c.logo_data or "",
        "last_polled_at": c.last_polled_at.isoformat() if c.last_polled_at else None,
        "last_status": c.last_status, "last_error": c.last_error,
    }
