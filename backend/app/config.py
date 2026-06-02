import os
from pathlib import Path

# In the container: __file__ is /app/app/config.py, so parents[1] == /app
# Locally: resolves to the repo root backend/ parent, matching the same layout
REPO_ROOT = Path(__file__).resolve().parents[1]

PORTAINER_BASE_URL: str = os.getenv("PORTAINER_BASE_URL", "").rstrip("/")
PORTAINER_API_TOKEN: str = os.getenv("PORTAINER_API_TOKEN", "")
DATABASE_URL: str = os.getenv("DATABASE_URL", "postgresql://orc:orc@postgres:5432/orc")
REDIS_URL: str = os.getenv("REDIS_URL", "redis://redis:6379/0")
POLL_INTERVAL_SECONDS: int = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))
