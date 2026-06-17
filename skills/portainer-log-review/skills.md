# Skill Definition

name: portainer-log-review
version: 0.1.0
category: observability
risk_level: low
autonomy_level: 0
governance: green
allowed_plane: observation

## Purpose

Collect and classify logs from Portainer-managed containers.

## Inputs

- Portainer endpoint
- Authentication token
- Container selection rules
- Time window

## Outputs

- Structured events
- Severity classification
- Event tags such as `critical_error`, `login`, `restart`, `deploy`

## Procedure

1. Authenticate to Portainer.
2. Query configured containers or stacks.
3. Pull recent logs.
4. Normalize timestamps and sources.
5. Classify high-value events.
6. Emit structured results.

## Audit Requirements

- Record collection window
- Record containers queried
- Record classification counts
