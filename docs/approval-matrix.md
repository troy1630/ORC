# ORC Approval Matrix

## Decision Inputs

- autonomy_level
- governance color
- risk_level
- requested action
- target system
- approval_required
- verification signal
- rollback plan

## Governance Rules

| Color | Default Decision | Examples |
| --- | --- | --- |
| Green | Allow autonomously | read logs, retrieve memory, summarize, verify |
| Yellow | Allow only through registered skill or runbook | policy-checked analysis, documentation writeback, bounded workflow steps |
| Red | Human approval required | redeploy, restart, destructive change, credential change, generated tool promotion |

## Autonomy Rules

| Level | Allowed Without Human Approval | Required Gate |
| --- | --- | --- |
| 0 | observe and report | none beyond read-only boundary |
| 1 | recommend and draft | Gatekeeper check before escalation |
| 2 | execute approved runbooks | registered runbook plus approval when governance is red |
| 3 | build and test in sandbox | human approval before promotion |

## Hard Blocks

Gatekeeper must block when:

- the target is ambiguous
- no rollback or stop condition is defined for a mutating action
- the request crosses the declared allowed_plane
- a generated tool attempts to promote itself
- a worker attempts to mutate the Stable Core
- a red action lacks explicit human approval

## Red Approval Record

Every red approval must record:

- approver
- requesting agent
- exact target
- exact action
- risk
- rollback
- verification signal
- approval time
- expiration or one-time-use boundary

