# ORC Agent Framework

## Mental Map

Think of ORC as an operating system for agents:

- The Stable Core decides, records, and governs.
- The Worker Pool executes bounded jobs and can be recreated at any time.
- The Builder Sandbox writes and tests new tools before a human approves promotion.

Agents are durable roles. Workers are disposable execution surfaces.

## Chat-First Operation

Agent Chat is the primary operator surface. The operator should be able to talk to ORC the way they would direct a senior teammate:

- "Review the logs from the last hour."
- "Search memory for restart failures."
- "Prepare a redeploy approval for api-worker."
- "Promote tool; tool_id=log-parser; title=Log Parser; tests=unit tests passed; dry run=parsed sample logs without mutation."

ORC turns those instructions into governed work:

- ORC should classify the autonomy level early. For read-only observation and analysis it should say that the request is Level 0 / Green and proceed without approval.
- ORC should check skill fit before execution. If a registered skill is too narrow for the requested inputs, ORC should solve the safe read-only portion first and ask Sage to preserve the improvement as a learning or skill proposal.
- Green requests, such as read-only log review and memory search, can run immediately.
- Red requests, such as redeploy, restart, destructive change, credential change, or tool promotion, create Gatekeeper approval requests first.
- Chat messages carry runbook, evidence, and approval metadata so the conversation remains the operational record.

## Six Planes

| Plane | Agent | Job |
| --- | --- | --- |
| Control | ORC | Route work, own state, assemble the next-step plan |
| Observation | Raven | Gather evidence and verify outcomes |
| Reasoning | Oracle | Diagnose, explain, and recommend |
| Memory / Learning | Sage | Retrieve history and write structured learnings |
| Policy | Gatekeeper | Approve, downgrade, or block |
| Action | Executioner | Run approved actions only |

## Three Layers

### 1. Stable Core

Long-lived application layer:

- ORC API
- core agents
- message bus
- skill registry
- tool registry
- runbook registry
- approval matrix
- incident records
- Sage memory

Stable Core should not be edited by generated tools or worker jobs.

### 2. Tool Runner / Worker Pool

Disposable Docker workers:

- run scripts
- execute generated tools after promotion
- analyze logs
- run short-lived tasks
- execute approved runbook steps

If a worker breaks, recreate it.

### 3. Builder / Sandbox Dev

Isolated Docker workspace:

- write code
- run tests
- lint/check
- dry-run
- package tools
- prepare promotion requests

Builder can create candidate tools. It cannot promote them into production without Gatekeeper review and human approval.

## Autonomy Levels

| Level | Name | Meaning |
| --- | --- | --- |
| 0 | Observe | Read, collect, classify, and report |
| 1 | Recommend | Diagnose and propose plans |
| 2 | Execute approved runbooks | Run known procedures after policy allows it |
| 3 | Build in sandbox | Generate and test tools, then request promotion |

Level 3 does not mean autonomous production deployment. Promotion remains human-approved.

## Governance Colors

| Color | Boundary | Examples |
| --- | --- | --- |
| Green | Autonomous read/report | log review, retrieval, summaries, verification |
| Yellow | Policy checked | registered skills, bounded runbooks, non-destructive workflow steps |
| Red | Human approved | redeploys, restarts, destructive changes, credential changes, tool promotion |

## Learning Loop

1. Raven observes the environment.
2. Oracle diagnoses the evidence.
3. Sage compares the case against prior incidents and known patterns.
4. ORC decides the recommended next-step plan.
5. Gatekeeper checks policy and risk.
6. Executioner runs the approved action.
7. Raven verifies whether the issue improved or worsened.
8. Sage writes the retrospective.

## Sage Memory

Sage writes Markdown memory in four classes:

- Episodic: who, what, where, when, and why for an incident.
- Semantic: facts, notes, policies, and environment knowledge.
- Procedural: how to handle an issue, escalation, rollback, and runbook candidates.
- Evaluative: success rates, what worked, what failed, and what should improve.

## Retrospective Shape

Every meaningful incident should end with:

- symptom
- context
- root cause
- action
- outcome
- verification signal
- confidence
- whether to promote to a runbook

That final retrospective is how ORC improves over time.

## Working Surfaces

The current implementation exposes these surfaces:

- Agent Chat can search memory, execute green runbooks, run configurable Oracle critical/error reviews, and prepare red approval-gated work from natural operator commands.
- ORC announces Level 0 / Green for read-only Oracle reviews, runs the requested time window, and records Sage learning when a non-default window exposes a skill-fit improvement.
- Memory search scans Markdown memory, knowledge, runbooks, tools, and framework docs.
- Incident creation writes episodic memory under `memory/episodic/`.
- Green runbooks can execute immediately and write evidence under `knowledge/runbook-executions/`.
- Red runbooks create approval-gated Executioner handoffs.
- Builder tool proposals write artifacts under `builder/workspace/proposals/`.
- Tool promotion requires a Gatekeeper approval record with human approval before a new `tools/*/tool.md` is created.

## Promotion Rule

A generated tool is never allowed to promote itself. Promotion requires:

1. Builder artifact.
2. Test summary.
3. Dry-run summary.
4. Gatekeeper approval request.
5. Human approval.
6. Copy into the `tools/` registry.
