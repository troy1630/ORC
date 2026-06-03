import re
from datetime import datetime, timezone

from .db import ObservedEvent

_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
_TS_RE = re.compile(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})\.(\d+)Z\s*")
_ACCESS_LOG_RE = re.compile(
    r'"(?P<method>GET|POST|PUT|PATCH|DELETE|HEAD|OPTIONS)\s+(?P<target>[^"\s]+)\s+HTTP/[0-9.]+"\s+(?P<status>\d{3})\b',
    re.I,
)
_ORC_API_PATH_RE = re.compile(r"^/(?:events|overview|health|connections|raven|oracle)(?:/|$)", re.I)
_LEADING_LEVEL_RE = re.compile(r"^\s*(?:\[[^\]]+\]\s*)?(CRITICAL|FATAL|ERROR|ERR|WARNING|WARN|INFO|DEBUG|TRACE)\b[:\s-]*", re.I)
_KEY_LEVEL_RE = re.compile(
    r"(?<![?&/])\b(?:level|lvl|loglevel|severity)\s*=\s*[\"']?(CRITICAL|FATAL|ERROR|ERR|WARNING|WARN|INFO|DEBUG|TRACE)\b",
    re.I,
)
_SEVERITY: list[tuple[re.Pattern, str]] = [
    (re.compile(r"\b(CRITICAL|FATAL)\b", re.I), "critical"),
    (re.compile(r"\bERROR\b", re.I), "error"),
    (re.compile(r"\b(WARNING|WARN)\b", re.I), "warning"),
]
_LEVELS = {
    "critical": "critical",
    "fatal": "critical",
    "error": "error",
    "err": "error",
    "warning": "warning",
    "warn": "warning",
    "info": "info",
    "debug": "info",
    "trace": "info",
}


def _normalize_level(level: str) -> str:
    return _LEVELS.get(level.lower(), "info")


def _access_log(msg: str) -> dict[str, int | str] | None:
    match = _ACCESS_LOG_RE.search(msg)
    if not match:
        return None
    target = match.group("target")
    return {
        "method": match.group("method").upper(),
        "target": target,
        "path": target.split("?", 1)[0],
        "status": int(match.group("status")),
    }


def _http_status(msg: str) -> int | None:
    access = _access_log(msg)
    return int(access["status"]) if access else None


def _explicit_level(msg: str) -> str | None:
    match = _LEADING_LEVEL_RE.search(msg) or _KEY_LEVEL_RE.search(msg)
    return _normalize_level(match.group(1)) if match else None


def _without_query_strings(msg: str) -> str:
    msg = re.sub(
        r'("(?:GET|POST|PUT|PATCH|DELETE|HEAD|OPTIONS)\s+)(\S+)(\s+HTTP/[0-9.]+"\s+\d{3}\b)',
        lambda m: f"{m.group(1)}{m.group(2).split('?', 1)[0]}{m.group(3)}",
        msg,
        flags=re.I,
    )
    msg = re.sub(r"https?://[^\s\"']+", lambda m: m.group(0).split("?", 1)[0], msg, flags=re.I)
    return re.sub(r"[?&][^\s\"']+", "", msg)


def _detect_severity(msg: str) -> str:
    access = _access_log(msg)
    status = int(access["status"]) if access else None
    level = _explicit_level(msg)

    if access and status < 400 and _ORC_API_PATH_RE.match(str(access["path"])):
        return "info"

    if status is not None:
        if status >= 500:
            return "error"
        if status >= 400:
            return "warning"
        if level in ("info", None):
            return "info"

    if level:
        return level

    for pattern, level in _SEVERITY:
        if pattern.search(_without_query_strings(msg)):
            return level
    return "info"


def parse_logs(
    raw: str,
    connection_id: int,
    endpoint_id: int,
    container_id: str,
    container_name: str,
) -> tuple[list[ObservedEvent], int]:
    """Parse raw Docker log text into ObservedEvent rows. Returns (events, last_unix_ts)."""
    events: list[ObservedEvent] = []
    last_ts = 0

    for line in raw.splitlines():
        line = _ANSI_RE.sub("", line).strip()
        if not line:
            continue

        m = _TS_RE.match(line)
        if m:
            frac = m.group(2)[:6].ljust(6, "0")
            message = line[m.end():]
            try:
                occurred_at = datetime.fromisoformat(f"{m.group(1)}.{frac}+00:00")
            except ValueError:
                occurred_at = datetime.now(timezone.utc)
        else:
            message = line
            occurred_at = datetime.now(timezone.utc)

        last_ts = max(last_ts, int(occurred_at.timestamp()))
        events.append(
            ObservedEvent(
                connection_id=connection_id,
                endpoint_id=endpoint_id,
                container_id=container_id,
                container_name=container_name,
                severity=_detect_severity(message),
                message=message[:2048],
                occurred_at=occurred_at,
                ingested_at=datetime.now(timezone.utc),
            )
        )

    return events, last_ts
