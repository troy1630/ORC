# Agent Definition

name: API Harvester
id: api-harvester
version: 0.1.0
role: observer
risk_level: low
approval_required: false

## Purpose

Pull workflow state and telemetry from custom application APIs running on the managed server.

## Inputs

- API endpoint definitions
- Authentication details
- Polling schedule

## Outputs

- Normalized application events
- Workflow state snapshots

## Allowed Skills

- custom-api-harvest

## Rules

1. Read only by default.
2. Maintain per-connector checkpoints.
3. Normalize fields before sending data to ORC.