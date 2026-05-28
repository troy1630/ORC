# ORC

ORC is a containerized agent orchestration and observability application designed to run as a Portainer stack on Linux. Its job is to correlate container activity, Portainer logs, application API telemetry, agent interactions, human approvals, and knowledge-base updates into a single operational picture.

This repository is initialized as a Claude-ready build package:

- Product and architecture documents live in `docs/`.
- Agent definitions live in `agents/*/agent.md`.
- Skill definitions live in `skills/*/skills.md`.
- A minimal orchestration API scaffold lives in `backend/`.
- Deployment assets for Docker and Portainer live at the repo root and in `deploy/`.

## Recommended Build Direction

Start with a reliable operations core before adding the game-like presentation layer.

1. Use a Python `FastAPI` backend for orchestration APIs, approval workflows, scheduling, and integration adapters.
2. Treat Markdown as the source of truth for agent and skill rules so the system remains inspectable by humans and other coding agents.
3. Run ORC as a small stack in Portainer: API, worker, Postgres, Redis, and a future UI container.
4. Keep all server credentials out of the repo. Mount secrets at runtime through Portainer.
5. Add the visual “mystical orc” activity map only after log ingestion, correlation, and approval flows are working.

## Initial Layout

```text
docs/
agents/
skills/
backend/
deploy/
docker-compose.yml
```

## Secret Handling

The Portainer token file referenced by the user must not be committed. Pass it into the stack as a secret or environment variable at deploy time, for example:

- `PORTAINER_BASE_URL`
- `PORTAINER_API_TOKEN_FILE`

## First Build Sequence

1. Read `docs/claude-build-brief.md`.
2. Read `docs/PRD.md`.
3. Read `docs/architecture.md`.
4. Implement the milestones in `docs/implementation-plan.md`.

## Current Status

This repo is intentionally initialized with documentation and a thin service scaffold, not a finished product.