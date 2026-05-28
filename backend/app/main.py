from dataclasses import asdict

from fastapi import FastAPI

from .config import REPO_ROOT
from .registry import load_registry

app = FastAPI(
    title="ORC API",
    version="0.1.0",
    description="Initial orchestration scaffold for Portainer-centric agent operations.",
)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "service": "orc-api"}


@app.get("/registry/agents")
def registry_agents() -> dict[str, object]:
    items = load_registry(REPO_ROOT, "agents")
    return {"count": len(items), "items": [asdict(item) for item in items]}


@app.get("/registry/skills")
def registry_skills() -> dict[str, object]:
    items = load_registry(REPO_ROOT, "skills")
    return {"count": len(items), "items": [asdict(item) for item in items]}