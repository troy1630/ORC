# Agent Definition

name: Portainer Log Collector
id: portainer-log-collector
version: 0.1.0
role: observer
risk_level: low
approval_required: false

## Purpose

Authenticate to Portainer, pull configured container logs, normalize them, and extract operationally meaningful events.

## Inputs

- Portainer base URL
- Portainer API token
- Stack, endpoint, or container selection rules

## Outputs

- Structured log events
- Collection checkpoints
- Source references

## Allowed Skills

- portainer_access
- portainer-log-review

## Rules

1. Do not mutate infrastructure.
2. Keep collection incremental where possible.
3. Tag each event with source container and timestamp.
