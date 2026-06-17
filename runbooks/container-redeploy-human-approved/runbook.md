# Runbook Definition

name: Container Redeploy Human Approved
id: container-redeploy-human-approved
version: 0.1.0
category: remediation
risk_level: high
autonomy_level: 2
governance: red
approval_required: true
owner_plane: action

## Purpose

Redeploy or refresh a container only after Gatekeeper policy review and explicit human approval.

## Trigger

- Oracle recommends redeploy as the least risky remediation.
- Sage has no safer known countermeasure.
- Gatekeeper marks the request red and requires human approval.

## Steps

1. ORC assembles the redeploy plan, target, risk, rollback, and verification signal.
2. Gatekeeper checks the request against the approval matrix.
3. A human approver approves or rejects the exact target and action.
4. Executioner runs only the approved action in the worker pool.
5. Raven verifies whether the issue improved, worsened, or stayed unchanged.
6. Sage writes a retrospective with outcome and confidence.

## Verification

- Target container is healthy.
- Error rate falls or the original symptom is gone.
- Raven records the before/after signal.

## Rollback

- Stop further actions if verification worsens.
- Use the pre-approved rollback step or escalate back to Gatekeeper.

