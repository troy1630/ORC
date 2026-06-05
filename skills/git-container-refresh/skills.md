# Skill Definition

name: git-container-refresh
id: git-container-refresh
version: 0.1.0
category: execution
risk_level: high
approval_required: true

## Purpose

Refresh a containerized application from Git after Gate Keeper approval.

## Inputs

- Approved request ID
- Repository path
- Branch or ref
- Container or stack target
- Rollback notes

## Outputs

- Git pull result
- Container refresh result
- Verification status
- Audit record

## Procedure

1. Confirm the approval request is approved and execution is allowed.
2. Verify the repository and target container match the approved request.
3. Pull the approved Git ref.
4. Refresh the approved container or stack.
5. Ask Raven and Oracle to verify post-action health.

## Rollback

- Stop if the repository or target differs from the approval.
- Use the recorded previous ref or image if rollback is required.

## Audit Requirements

- Record request ID
- Record repository path and ref
- Record container or stack target
- Record execution output and verification result
