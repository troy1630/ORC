# Skill Definition

name: Critical Error Review
id: hourly-critical-error-review
version: 0.2.0
category: observability
risk_level: low
autonomy_level: 0
governance: green
allowed_plane: reasoning
approval_required: false
agent: oracle
canonical_intent: critical-error-review

## Purpose

Review critical and error-level operational events over an operator-requested time window, identify repeated patterns, and produce an operator-ready summary with likely root cause and countermeasure for the top issues.

## Inputs

- `time_window`: requested review window such as 1 hour, 6 hours, or 24 hours.
- If no time window is provided, ask once when the situation is ambiguous; otherwise default to 1 hour for routine reviews.
- `severity`: defaults to critical and error.
- Connection, stack, container, and friendly-name metadata.
- Prior Oracle review summaries and Sage memory when available.

## Outputs

- Critical error review summary.
- Top recurring error patterns with counts and affected services.
- Evidence references to source events.
- Severity assessment and confidence.
- Likely root cause for the top issue.
- Countermeasure recommendation.
- Sage learning or runbook-promotion recommendation when the pattern is reusable.

## Procedure

1. Classify read-only review requests as Level 0 / Green and say so before proceeding.
2. Parse the operator-requested `time_window`; do not silently downgrade a six-hour request to one hour.
3. If the time window is missing and no safe default is obvious, ask the operator for the intended window.
4. Select observed events inside the requested window.
5. Filter to critical and error-level events before considering lower-severity context.
6. Group events by connection, stack, container, and recurring message pattern.
7. Identify repeated failures, newly appearing critical errors, and errors affecting ORC itself.
8. Separate confirmed facts from hypotheses.
9. Produce a concise review with the top issues, evidence counts, affected services, likely root cause, and countermeasure.
10. Stop before any remediation; route mutating recommendations to Gatekeeper for human approval.
11. Ask Sage to write a learning when the request exposes a skill gap, reusable pattern, or countermeasure worth retaining.

## Safety Rules

1. Do not mutate infrastructure.
2. Do not send external notifications directly; return a recommendation for ORC's notification layer.
3. Do not include secrets, tokens, credentials, or full sensitive log lines in the review.
4. Link recommendations to evidence and mark uncertainty clearly.
5. Treat remediation, redeploy, restart, credential, and configuration changes as approval-gated follow-ups.

## Audit Requirements

- Record autonomy level and governance color.
- Record requested review window, start time, and end time.
- Record event counts by severity.
- Record grouped pattern counts and affected services.
- Record whether operator notification was recommended and why.
- Record Sage learning path when a learning is written.
- Record the source agent and generated review timestamp.
