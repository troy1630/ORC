"""
ORC Executioner Tool Registry.

Each function decorated with @orc_tool becomes available to the Executioner LLM
as an OpenAI function-calling tool. To add a new capability, add a new @orc_tool
function here — no other code changes needed.
"""

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .config import REPO_ROOT
from .db import Connection, SessionLocal
from .portainer import PortainerClient

_REGISTRY: dict[str, dict] = {}  # name → {fn, schema}


def orc_tool(description: str, parameters: dict):
    """Registers a Python function as a callable Executioner tool."""
    def decorator(fn):
        _REGISTRY[fn.__name__] = {
            "fn": fn,
            "schema": {
                "type": "function",
                "function": {
                    "name": fn.__name__,
                    "description": description,
                    "parameters": parameters,
                },
            },
        }
        return fn
    return decorator


def get_tool_schemas() -> list[dict]:
    """Return all registered tool schemas in OpenAI function-calling format."""
    return [v["schema"] for v in _REGISTRY.values()]


def call_tool(name: str, arguments: dict) -> Any:
    """Invoke a registered tool by name with the given arguments."""
    if name not in _REGISTRY:
        return {"error": f"Unknown tool: {name}"}
    try:
        return _REGISTRY[name]["fn"](**arguments)
    except Exception as exc:
        return {"error": str(exc)}


def _container_match_text(container: dict, connection: Connection) -> str:
    cid = container.get("Id", "")
    cname = (container.get("Names") or [f"/{cid[:12]}"])[0].lstrip("/")
    labels = container.get("Labels") or {}
    stack_name = labels.get("com.docker.compose.project") or cname
    service_name = labels.get("com.docker.compose.service") or cname
    fields = [
        cname,
        stack_name,
        service_name,
        connection.name or "",
        connection.server_name or "",
    ]
    return " | ".join(fields).lower()


# ---------------------------------------------------------------------------
# Registered Tools
# ---------------------------------------------------------------------------

@orc_tool(
    description="List all enabled ORC connections with their ID, name, type, and current poll interval in seconds.",
    parameters={"type": "object", "properties": {}, "required": []},
)
def list_connections() -> list[dict]:
    with SessionLocal() as s:
        rows = s.query(Connection).filter_by(enabled=True).all()
        return [
            {
                "id": r.id,
                "name": r.name,
                "type": r.type,
                "poll_interval_seconds": r.poll_interval_seconds,
                "last_status": r.last_status,
            }
            for r in rows
        ]


@orc_tool(
    description=(
        "Search enabled ORC connections and Portainer containers for a name fragment. "
        "Use this before changing a poll interval when the operator refers to a container or workload by name. "
        "If the result shows multiple matches, ask the operator which one they mean."
    ),
    parameters={
        "type": "object",
        "properties": {
            "term": {
                "type": "string",
                "description": "Name fragment to search for across connection names, server names, and container names",
            }
        },
        "required": ["term"],
    },
)
def search_containers(term: str) -> dict:
    needle = term.strip().lower()
    if not needle:
        return {"error": "term is required"}

    matches: list[dict] = []
    with SessionLocal() as s:
        connections = s.query(Connection).filter_by(enabled=True).all()
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
                for container in containers:
                    match_text = _container_match_text(container, conn)
                    if needle not in match_text:
                        continue
                    cid = container.get("Id", "")
                    cname = (container.get("Names") or [f"/{cid[:12]}"])[0].lstrip("/")
                    labels = container.get("Labels") or {}
                    matches.append({
                        "connection_id": conn.id,
                        "connection_name": conn.name,
                        "server_name": conn.server_name or conn.name,
                        "endpoint_id": endpoint_id,
                        "container_id": cid,
                        "container_name": cname,
                        "stack_name": labels.get("com.docker.compose.project") or "",
                        "service_name": labels.get("com.docker.compose.service") or "",
                    })
    return {"term": term, "count": len(matches), "matches": matches}


def _safe_skill_file(skill_id: str) -> Path:
    skill_root = (REPO_ROOT / "skills").resolve()
    target = (skill_root / skill_id / "skills.md").resolve()
    try:
        target.relative_to(skill_root)
    except ValueError as exc:
        raise ValueError("Invalid skill id") from exc
    return target


@orc_tool(
    description=(
        "Read a skill Markdown definition by skill ID so agents can inspect the live registry entry "
        "before proposing or applying documentation changes."
    ),
    parameters={
        "type": "object",
        "properties": {
            "skill_id": {
                "type": "string",
                "description": "Canonical skill ID / folder name under skills/",
            }
        },
        "required": ["skill_id"],
    },
)
def read_skill_definition(skill_id: str) -> dict:
    try:
        target = _safe_skill_file(skill_id)
        if not target.exists():
            return {"error": f"Skill '{skill_id}' not found"}
        return {
            "skill_id": skill_id,
            "path": str(target.relative_to(REPO_ROOT)),
            "content": target.read_text(encoding="utf-8"),
        }
    except Exception as exc:
        return {"error": str(exc)}


@orc_tool(
    description=(
        "Write a full skill Markdown definition back to the registry. Use this only after Gate Keeper "
        "has approved a skill documentation update."
    ),
    parameters={
        "type": "object",
        "properties": {
            "skill_id": {
                "type": "string",
                "description": "Canonical skill ID / folder name under skills/",
            },
            "content": {
                "type": "string",
                "description": "Complete Markdown content for skills.md",
            },
        },
        "required": ["skill_id", "content"],
    },
)
def write_skill_definition(skill_id: str, content: str) -> dict:
    try:
        target = _safe_skill_file(skill_id)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
        return {"ok": True, "skill_id": skill_id, "path": str(target.relative_to(REPO_ROOT))}
    except Exception as exc:
        return {"error": str(exc)}


@orc_tool(
    description="Get the current status and poll interval for a specific connection by its ID.",
    parameters={
        "type": "object",
        "properties": {
            "connection_id": {
                "type": "integer",
                "description": "The connection ID obtained from list_connections",
            }
        },
        "required": ["connection_id"],
    },
)
def get_connection_status(connection_id: int) -> dict:
    with SessionLocal() as s:
        conn = s.get(Connection, connection_id)
        if not conn:
            return {"error": f"Connection {connection_id} not found"}
        return {
            "id": conn.id,
            "name": conn.name,
            "poll_interval_seconds": conn.poll_interval_seconds,
            "last_status": conn.last_status,
            "last_polled_at": conn.last_polled_at.isoformat() if conn.last_polled_at else None,
            "revert_poll_interval": conn.revert_poll_interval,
            "revert_at": conn.revert_at.isoformat() if conn.revert_at else None,
        }


@orc_tool(
    description=(
        "Change the polling frequency for a connection. Optionally auto-reverts to the original "
        "interval after a set number of minutes. Use this to increase or decrease how often Raven "
        "polls a connection (e.g. set to 30 seconds for intensive monitoring). "
        "Prefer a specific connection_id when available, but you may also provide a connection_name "
        "to resolve the best matching enabled connection by friendly name or server name."
    ),
    parameters={
        "type": "object",
        "properties": {
            "connection_id": {
                "type": "integer",
                "description": "The connection ID obtained from list_connections",
            },
            "connection_name": {
                "type": "string",
                "description": "Optional fuzzy search text that matches the connection name or server name",
            },
            "new_interval_seconds": {
                "type": "integer",
                "description": "New poll interval in seconds (e.g. 30 for every 30 seconds)",
            },
            "revert_after_minutes": {
                "type": "integer",
                "description": (
                    "Automatically restore the original interval after this many minutes. "
                    "Pass 0 or omit to make the change permanent."
                ),
            },
        },
        "required": ["new_interval_seconds"],
    },
)
def modify_connection_poll_interval(
    connection_id: int = 0,
    connection_name: str = "",
    new_interval_seconds: int = 0,
    revert_after_minutes: int = 0,
) -> dict:
    with SessionLocal() as s:
        conn = None
        matches: list[dict] = []
        if new_interval_seconds <= 0:
            return {"error": "new_interval_seconds must be greater than 0"}
        if connection_id:
            conn = s.get(Connection, connection_id)
        elif connection_name.strip():
            term = connection_name.strip().lower()
            candidates = [
                c for c in s.query(Connection).filter_by(enabled=True).all()
                if term in (c.name or "").lower() or term in (c.server_name or "").lower()
            ]
            matches = [
                {"id": c.id, "name": c.name, "server_name": c.server_name or "", "poll_interval_seconds": c.poll_interval_seconds}
                for c in candidates
            ]
            if len(candidates) == 1:
                conn = candidates[0]
            elif len(candidates) > 1:
                return {
                    "needs_clarification": True,
                    "error": f"Ambiguous connection match for '{connection_name}'",
                    "matches": matches,
                }
            else:
                return {
                    "error": f"No enabled connection matched '{connection_name}'",
                    "matches": [],
                }
        if not conn:
            return {"error": "Provide either connection_id or connection_name"}
        old_interval = conn.poll_interval_seconds
        if revert_after_minutes and revert_after_minutes > 0:
            conn.revert_poll_interval = old_interval
            conn.revert_at = datetime.now(timezone.utc) + timedelta(minutes=revert_after_minutes)
        conn.poll_interval_seconds = new_interval_seconds
        s.commit()
        return {
            "success": True,
            "connection_id": connection_id,
            "name": conn.name,
            "old_interval_seconds": old_interval,
            "new_interval_seconds": new_interval_seconds,
            "revert_at": conn.revert_at.isoformat() if conn.revert_at else None,
        }
