"""
ORC Executioner Tool Registry.

Each function decorated with @orc_tool becomes available to the Executioner LLM
as an OpenAI function-calling tool. To add a new capability, add a new @orc_tool
function here — no other code changes needed.
"""

from datetime import datetime, timedelta, timezone
from typing import Any

from .db import Connection, SessionLocal

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
        "polls a connection (e.g. set to 30 seconds for intensive monitoring)."
    ),
    parameters={
        "type": "object",
        "properties": {
            "connection_id": {
                "type": "integer",
                "description": "The connection ID obtained from list_connections",
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
        "required": ["connection_id", "new_interval_seconds"],
    },
)
def modify_connection_poll_interval(
    connection_id: int,
    new_interval_seconds: int,
    revert_after_minutes: int = 0,
) -> dict:
    with SessionLocal() as s:
        conn = s.get(Connection, connection_id)
        if not conn:
            return {"error": f"Connection {connection_id} not found"}
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
