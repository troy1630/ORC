"""
ORC Worker — polls one container at a time.

Interval: use the connection's poll_interval_seconds if set, otherwise
auto-compute as 100s / total-containers so a full cycle takes ~100 seconds.

Each Raven message includes both new events this cycle AND 24h recent counts
so the UI can show a container's true health state, not just latest activity.
"""

import logging
import time
from collections import deque
from datetime import datetime, timedelta, timezone

from sqlalchemy import func

from . import raven
from .config import POLL_INTERVAL_SECONDS
from .db import Connection, IngestionCheckpoint, ObservedEvent, SessionLocal, init_db
from .ingest import parse_logs
from .portainer import PortainerClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("orc.worker")

# Each entry: (conn_id, conn_name, endpoint_id, container_id, container_name, client, interval_s)
_queue: deque = deque()
_sleep: float = float(POLL_INTERVAL_SECONDS)
_warned_no_connections: bool = False


def _rebuild_queue() -> None:
    global _sleep, _warned_no_connections
    _queue.clear()
    now = datetime.now(timezone.utc)

    with SessionLocal() as s:
        connections = s.query(Connection).filter_by(enabled=True).all()
        conn_list = [
            (c.id, c.name, c.base_url, c.api_token, c.poll_interval_seconds)
            for c in connections
        ]

    if not conn_list:
        if not _warned_no_connections:
            log.info("No enabled connections — add one via the Connections tab")
            raven.publish({"type": "no_connections"})
            _warned_no_connections = True
        _sleep = 10.0
        return

    _warned_no_connections = False
    total = 0

    for conn_id, conn_name, base_url, api_token, conn_interval in conn_list:
        client = PortainerClient(base_url, api_token)
        conn_ok, conn_error = True, None
        try:
            endpoints = client.get_endpoints()
        except Exception as exc:
            conn_ok, conn_error = False, str(exc)
            log.error("[%s] cannot reach Portainer: %s", conn_name, exc)
            raven.publish({"type": "poll_error", "server": conn_name, "error": conn_error})

        _update_conn(conn_id, "ok" if conn_ok else "error", conn_error, now)

        if not conn_ok:
            continue

        for ep in endpoints:
            eid = ep["Id"]
            try:
                for c in client.get_containers(eid):
                    cid = c["Id"]
                    cname = (c.get("Names") or [f"/{cid[:12]}"])[0].lstrip("/")
                    _queue.append((conn_id, conn_name, eid, cid, cname, client, conn_interval))
                    total += 1
            except Exception as exc:
                log.error("[%s] endpoint %s: %s", conn_name, eid, exc)

    if total:
        # Use per-connection interval if set, else auto-compute from queue size
        auto = round(100.0 / total, 2)
        _sleep = auto  # default; overridden per-item in _poll_next
        log.info("Queue: %d container(s), auto interval=%.2fs", total, auto)
        raven.publish({
            "type": "queue_ready",
            "containers": total,
            "connections": len(conn_list),
            "interval": auto,
        })
    else:
        _sleep = 10.0


def _recent_counts(conn_id: int, cid: str) -> tuple[int, int]:
    """Return (error_count, warning_count) for this container in the last 24h."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    with SessionLocal() as s:
        errors = s.query(func.count(ObservedEvent.id)).filter(
            ObservedEvent.connection_id == conn_id,
            ObservedEvent.container_id == cid,
            ObservedEvent.severity.in_(["error", "critical"]),
            ObservedEvent.occurred_at >= cutoff,
        ).scalar() or 0
        warnings = s.query(func.count(ObservedEvent.id)).filter(
            ObservedEvent.connection_id == conn_id,
            ObservedEvent.container_id == cid,
            ObservedEvent.severity == "warning",
            ObservedEvent.occurred_at >= cutoff,
        ).scalar() or 0
    return errors, warnings


def _poll_next() -> None:
    global _sleep
    conn_id, conn_name, eid, cid, cname, client, conn_interval = _queue.popleft()

    # Use connection's configured interval, or keep current auto interval
    if conn_interval:
        _sleep = float(conn_interval)

    raven.publish({"type": "container_checking", "server": conn_name, "container": cname})

    try:
        with SessionLocal() as session:
            checkpoint = (
                session.query(IngestionCheckpoint)
                .filter_by(connection_id=conn_id, endpoint_id=eid, container_id=cid)
                .first()
            )
            since = checkpoint.last_unix_ts if checkpoint else 0
            raw = client.get_container_logs(eid, cid, since=since)
            events, last_ts = parse_logs(raw, conn_id, eid, cid, cname)

            if events:
                session.add_all(events)
                if checkpoint:
                    checkpoint.last_unix_ts = last_ts
                else:
                    session.add(IngestionCheckpoint(
                        connection_id=conn_id, endpoint_id=eid,
                        container_id=cid, last_unix_ts=last_ts,
                    ))
                session.commit()

        new_errors  = sum(1 for e in events if e.severity in ("error", "critical"))
        new_warnings = sum(1 for e in events if e.severity == "warning")
        recent_errors, recent_warnings = _recent_counts(conn_id, cid)

        raven.publish({
            "type": "container_result",
            "server": conn_name,
            "container": cname,
            "events": len(events),
            "errors": new_errors,
            "warnings": new_warnings,
            "recent_errors": recent_errors,
            "recent_warnings": recent_warnings,
        })

        if events:
            log.info("[%s] %s — %d events (%d errors)", conn_name, cname, len(events), new_errors)

    except Exception as exc:
        log.error("[%s] %s: %s", conn_name, cname, exc)
        recent_errors, recent_warnings = _recent_counts(conn_id, cid)
        raven.publish({
            "type": "container_result",
            "server": conn_name,
            "container": cname,
            "events": 0,
            "errors": 0,
            "warnings": 0,
            "recent_errors": recent_errors,
            "recent_warnings": recent_warnings,
        })


def _update_conn(conn_id: int, status: str, error: str | None, ts: datetime) -> None:
    with SessionLocal() as s:
        c = s.get(Connection, conn_id)
        if c:
            c.last_polled_at = ts
            c.last_status = status
            c.last_error = error
            s.commit()


def main() -> None:
    init_db()
    log.info("ORC worker started")
    while True:
        try:
            if not _queue:
                _rebuild_queue()
            if _queue:
                _poll_next()
        except Exception as exc:
            log.error("Worker error: %s", exc)
        time.sleep(_sleep)


if __name__ == "__main__":
    main()
