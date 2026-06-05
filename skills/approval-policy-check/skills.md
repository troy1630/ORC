# Skill Definition

name: approval-policy-check
id: approval-policy-check
version: 0.1.0
category: governance
risk_level: high
approval_required: true

## Purpose

Evaluate risky action requests and decide whether human approval is required.

## Inputs

- Requested action
- Target system or container
- Risk level
- Evidence summary
- Requesting agent

## Outputs

- Approval decision
- Rejection reason
- Execution permission flag

## Procedure

1. Classify the requested action.
2. Check the requesting agent trust mode.
3. Require human approval for mutating infrastructure actions.
4. Record the decision and route approved requests to Executioner.

## Audit Requirements

- Record approver
- Record decision reason
- Record execution permission
