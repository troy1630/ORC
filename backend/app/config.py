import os
from pathlib import Path

# In the container, __file__ is /app/app/config.py and parents[1] == /app.
# Locally, __file__ is repo/backend/app/config.py and parents[1] == backend.
_APP_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = _APP_ROOT if (_APP_ROOT / "agents").exists() else _APP_ROOT.parent

PORTAINER_BASE_URL: str = os.getenv("PORTAINER_BASE_URL", "").rstrip("/")
PORTAINER_API_TOKEN: str = os.getenv("PORTAINER_API_TOKEN", "")
DATABASE_URL: str = os.getenv("DATABASE_URL", "postgresql://orc:orc@postgres:5432/orc")
REDIS_URL: str = os.getenv("REDIS_URL", "redis://redis:6379/0")
POLL_INTERVAL_SECONDS: int = int(os.getenv("POLL_INTERVAL_SECONDS", "3"))
# Target cycle: spread N connections evenly across this window
CYCLE_SECONDS: int = int(os.getenv("CYCLE_SECONDS", "180"))
