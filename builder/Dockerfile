FROM python:3.12-slim

WORKDIR /workspace

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends git curl ca-certificates nodejs npm \
    && rm -rf /var/lib/apt/lists/*

RUN python -m pip install --no-cache-dir --upgrade pip pytest ruff

RUN mkdir -p /workspace /app/skills /app/tools /app/runbooks /app/memory /app/docs

CMD ["sleep", "infinity"]

