"""
ORC Worker — polls one container every POLL_INTERVAL_SECONDS.

With 30 containers and a 3-second interval a full cycle takes ~90 seconds.
When the queue drains it is rebuilt by re-querying each Portainer endpoint,
picking up any containers that started or stopped since the last cycle.
"""

import logging
import time
from collections import deque
from datetime import datetime, timezone

from . import raven
from .config import POLL_INTERVAL_SECONDS
from .db import Connection, IngestionCheckpoint, SessionLocal, init_db
from .ingest import parse_logs
from .portainer import PortainerClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("orc.worker")

# Each item: (conn_id, conn_name, endpoint_id, container_id, container_name, client)
_queue: deque = deque()


def _rebuild_queue() -> None:
    """Query every enabled connection, enumerate running containers, fill the queue."""
    _queue.clear()
    now = datetime.now(timezone.utc)

    with SessionLocal() as s:
        connections = s.query(Connection).filter_by(enabled=True).all()
        conn_list = [(c.id, c.name, c.base_url, c.api_token) for c in connections]

    if not conn_list:
        log.info("No enabled connections — waiting for one to be configured")
        return

    total = 0
    for conn_id, conn_name, base_url, api_token in conn_list:
        client = PortainerClient(base_url, api_token)
        conn_ok, conn_error = True, None

        try:
            endpoints = client.get_endpoints()
        except Exception as exc:
            conn_ok, conn_error = False, str(exc)
            log.error("[%s] cannot reach Portainer: %s", conn_name, exc)
            raven.publish({"type": "poll_error", "server": conn_name, "error": conn_error})

        _update_conn_status(conn_id, "ok" if conn_ok else "error", conn_error, now)

        if not conn_ok:
            continue

        for ep in endpoints:
            eid = ep["Id"]
            try:
                containers = client.get_containers(eid)
                for c in containers:
                    cid = c["Id"]
                    cname = (c.get("Names") or [f"/{cid[:12]}"])[0].lstrip("/")
                    _queue.append((conn_id, conn_name, eid, cid, cname, client))
                    total += 1
            except Exception as exc:
                log.error("[%s] endpoint %s listing failed: %s", conn_name, eid, exc)

    log.info("Queue built: %d container(s) across %d connection(s)", total, len(conn_list))
    if total:
        raven.publish({
            "type": "queue_ready",
            "containers": total,
            "connections": len(conn_list),
        })


def _poll_next() -> None:
    """Pop one container from the queue, ingest its new logs, publish to Raven."""
    conn_id, conn_name, eid, cid, cname, client = _queue.popleft()
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

        err_count  = sum(1 for e in events if e.severity in ("error", "critical"))
        warn_count = sum(1 for e in events if e.severity == "warning")

        raven.publish({
            "type": "container_result",
            "server": conn_name,
            "container": cname,
            "events": len(events),
            "errors": err_count,
            "warnings": warn_count,
        })

        if events:
            log.info("[%s] %s — %d events (%d errors)", conn_name, cname, len(events), err_count)

    except Exception as exc:
        log.error("[%s] %s: %s", conn_name, cname, exc)
        raven.publish({
            "type": "container_result",
            "server": conn_name,
            "container": cname,
            "events": 0,
            "errors": 0,
            "warnings": 0,
        })


def _update_conn_status(conn_id: int, status: str, error: str | None, ts: datetime) -> None:
    with SessionLocal() as s:
        c = s.get(Connection, conn_id)
        if c:
            c.last_polled_at = ts
            c.last_status = status
            c.last_error = error
            s.commit()


def main() -> None:
    init_db()
    log.info("ORC worker started — %ds per container", POLL_INTERVAL_SECONDS)
    while True:
        try:
            if not _queue:
                _rebuild_queue()
            if _queue:
                _poll_next()
        except Exception as exc:
            log.error("Worker error: %s", exc)
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
