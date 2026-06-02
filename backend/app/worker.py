import logging
import time

from .config import POLL_INTERVAL_SECONDS, PORTAINER_API_TOKEN, PORTAINER_BASE_URL
from .db import IngestionCheckpoint, SessionLocal, init_db
from .ingest import parse_logs
from .portainer import PortainerClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("orc.worker")


def _poll(client: PortainerClient) -> None:
    endpoints = client.get_endpoints()
    with SessionLocal() as session:
        for ep in endpoints:
            eid = ep["Id"]
            try:
                containers = client.get_containers(eid)
            except Exception as exc:
                log.error("Could not list containers for endpoint %s: %s", eid, exc)
                continue

            for container in containers:
                cid = container["Id"]
                cname = (container.get("Names") or [f"/{cid[:12]}"])[0].lstrip("/")
                try:
                    checkpoint = (
                        session.query(IngestionCheckpoint)
                        .filter_by(endpoint_id=eid, container_id=cid)
                        .first()
                    )
                    since = checkpoint.last_unix_ts if checkpoint else 0
                    raw = client.get_container_logs(eid, cid, since=since)
                    events, last_ts = parse_logs(raw, eid, cid, cname)

                    if events:
                        session.add_all(events)
                        if checkpoint:
                            checkpoint.last_unix_ts = last_ts
                        else:
                            session.add(
                                IngestionCheckpoint(
                                    endpoint_id=eid,
                                    container_id=cid,
                                    last_unix_ts=last_ts,
                                )
                            )
                        session.commit()
                        log.info("Ingested %d event(s) from %s", len(events), cname)
                except Exception as exc:
                    log.error("Failed to ingest %s: %s", cname, exc)


def main() -> None:
    init_db()
    log.info("ORC worker started — poll interval %ds", POLL_INTERVAL_SECONDS)

    if not PORTAINER_BASE_URL or not PORTAINER_API_TOKEN:
        log.warning("PORTAINER_BASE_URL or PORTAINER_API_TOKEN not configured — idling")
        while True:
            time.sleep(POLL_INTERVAL_SECONDS)

    client = PortainerClient(PORTAINER_BASE_URL, PORTAINER_API_TOKEN)
    while True:
        try:
            _poll(client)
        except Exception as exc:
            log.error("Poll cycle failed: %s", exc)
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
