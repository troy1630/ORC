# Tool Definition

name: Log Analysis Worker
id: log-analysis-worker
version: 0.1.0
category: observation
risk_level: low
autonomy_level: 1
governance: green
execution_surface: worker-pool
approval_required: false

## Purpose

Run bounded log parsing and summarization jobs in an ephemeral worker container.

## Inputs

- Log slice or event query
- Time window
- Container or service identifier
- Read-only connection context

## Outputs

- Structured findings
- Severity counts
- Evidence references
- Suggested follow-up questions

## Execution Boundary

- Read-only analysis only.
- No container restarts, redeploys, shell mutation, credential access, or filesystem writes outside the job workspace.

## Verification

- Return source counts and time window.
- Include enough evidence for Oracle to inspect the finding.

