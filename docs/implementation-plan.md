# ORC Implementation Plan

## Build Assumption

Agents are durable roles, not disposable workers. ORC owns the control flow, workers run bounded jobs, and the builder sandbox creates candidate tools that require human approval before promotion.

## Architecture Planes

1. Control Plane: ORC owns routing, state, planning, incident lifecycle, and registry coordination.
2. Observation Plane: Raven gathers signals and verifies outcomes.
3. Reasoning Plane: Oracle diagnoses evidence and recommends next steps.
4. Memory / Learning Plane: Sage retrieves history and writes structured learnings.
5. Policy Plane: Gatekeeper enforces autonomy, risk, and approval boundaries.
6. Action Plane: Executioner runs only approved actions.

## Runtime Layers

1. Stable Core: long-lived ORC service, agents, registry, message bus, approval matrix, incident store, and memory files.
2. Tool Runner / Worker Pool: disposable Docker workers that run scripts, log analysis, approved runbooks, and promoted tools.
3. Builder / Sandbox Dev: isolated Docker workspace for generated code, tests, lint, dry-runs, packaging, and promotion requests.

## Autonomy Levels

- Level 0: observe only.
- Level 1: recommend.
- Level 2: execute approved runbooks.
- Level 3: build and test new tools in sandbox. Promotion still requires human approval.

## Governance Colors

- Green: read-only, retrieve, classify, summarize, report.
- Yellow: policy-checked workflows that must follow registered skills or runbooks.
- Red: human-approved production mutation, redeploy, credential, destructive, or promotion action.

## Phase 1: Stabilize The Architecture

Goal: clear separation of observe, reason, remember, approve, and act.

Build:

1. Plane metadata on core agents.
2. Separate `skills/`, `tools/`, and `runbooks/` registries.
3. Approval matrix and governance metadata.
4. Incident schema and retrospective learning template.
5. Markdown memory classes for episodic, semantic, procedural, and evaluative memory.
6. Instructions tab that teaches the model to operators.

Exit criteria:

- Agents declare plane, autonomy level, and governance boundary.
- Skills/tools/runbooks declare autonomy level and governance color.
- ORC UI explains the mental model.
- No production action path bypasses Gatekeeper.

## Phase 2: Add Retrieval And Learnings

Goal: improve from past cases.

Status: foundation implemented with Markdown search, incident records, episodic memory writeback, and retrospective templates.

Build:

1. Incident repository.
2. Pattern library.
3. Procedural runbook library.
4. Countermeasure knowledge base.
5. Retrieval scoring across memory classes.
6. Sage retrospective writer.

Exit criteria:

- Oracle can request similar incidents from Sage.
- Sage can return matching symptoms, actions, outcomes, and confidence.
- Retrospectives can recommend runbook promotion.

## Phase 3: Add Safe Autonomy

Goal: approved actions can run without redeploying the Stable Core.

Status: foundation implemented with runbook execution records, green runbook execution evidence, red runbook approval gates, and Executioner handoff records.

Build:

1. Docker-based worker pool.
2. Runbook execution records.
3. Verification steps.
4. Rollback steps.
5. Action confidence thresholds.
6. Worker job workspace and audit evidence.

Exit criteria:

- Executioner runs only approved runbooks.
- Raven verifies before/after signals.
- Failed verification routes back to Gatekeeper and Sage.

## Phase 4: Add Tool Generation

Goal: the platform can extend itself safely.

Status: foundation implemented with builder artifacts, promotion records, approval requests, and approved promotion into the `tools/` registry.

Build:

1. Builder sandbox container.
2. Generated tool template.
3. Tests, lint, dry-run, and package workflow.
4. Promotion workflow.
5. Canary deployment for new worker tools.

Exit criteria:

- Generated tools are created in the builder sandbox.
- Gatekeeper and a human approve promotion.
- Promoted tools run only in the worker pool, not in the Stable Core.

## Learning Loop

1. Observe: Raven gathers data.
2. Diagnose: Oracle analyzes the data.
3. Compare: Sage retrieves similar incidents and known patterns.
4. Decide: ORC builds a recommended next-step plan.
5. Check policy: Gatekeeper approves, downgrades, or blocks.
6. Execute: Executioner runs the action if allowed.
7. Verify: Raven confirms whether the issue improved or worsened.
8. Retrospective: Sage writes symptom, root cause, action, outcome, confidence, and promotion recommendation.
