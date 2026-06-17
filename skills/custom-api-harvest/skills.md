# Skill Definition

name: custom-api-harvest
version: 0.1.0
category: integration
risk_level: low
autonomy_level: 0
governance: green
allowed_plane: observation

## Purpose

Pull state and event data from custom applications through their APIs.

## Inputs

- Base URL
- Auth strategy
- Endpoint map
- Checkpoint or cursor

## Outputs

- Normalized records
- Connector checkpoint

## Procedure

1. Authenticate.
2. Pull configured resources.
3. Normalize data to internal event schema.
4. Return records with source metadata.

## Audit Requirements

- Record endpoints called
- Record request outcome
- Record item counts
