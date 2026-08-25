# Skill Definition

name: Container Diagnostic Review
id: container-diagnostic-review
version: 0.1.0
category: observability
risk_level: low
autonomy_level: 0
governance: green
allowed_plane: reasoning
approval_required: false
agent: oracle
canonical_intent: container-diagnostic-review

## Purpose

Diagnose errors for a named Portainer-managed container from read-only live logs, related same-stack service logs, and recent event evidence. Help the operator develop likely root cause and countermeasure without mutating infrastructure.

## Inputs

- Natural-language diagnostic request.
- Target container, service, app, or workload name.
- Read-only Portainer container inventory.
- Bounded log tails for the target container and obvious related containers.
- Existing observed events when live Portainer evidence is unavailable.

## Outputs

- Target server, stack, container, state, and log window.
- Top repeated error/warning patterns.
- Recent issue lines with secrets redacted.
- Related-service correlation notes.
- Issue, evidence, possible root cause, countermeasure, and next read-only check.
- Approval boundary for any mutating follow-up.

## Procedure

1. Interpret natural language flexibly for diagnostic intent such as root cause, countermeasure, reduce errors, diagnose, investigate, or why errors are occurring.
2. Resolve the target container from Portainer inventory.
3. If more than one live target matches, ask one clarification question.
4. Pull a bounded log tail for the target container.
5. Infer same-stack related services from cron/worker naming and URLs in logs, then pull bounded log tails for those containers.
6. Normalize repeated patterns, count issue lines, successes, durations, and row counts when present.
7. Ask Oracle to reason from the structured evidence only.
8. Separate facts from hypotheses and mark uncertainty.
9. Recommend read-only checks first; route restart, redeploy, configuration, credential, or scale changes to Gate Keeper.

## Safety Rules

1. Do not mutate infrastructure.
2. Do not print or store API tokens, bearer tokens, passwords, or secrets from logs.
3. Keep log retrieval bounded.
4. Do not infer beyond the supplied evidence without labeling it as a hypothesis.
5. Treat remediation as approval-gated.

## Audit Requirements

- Record target container, server, stack, endpoint, and container id.
- Record source log tail size and timestamp bounds.
- Record related containers checked.
- Record top repeated patterns and counts.
- Record generated diagnosis and recommended follow-up checks.
