# Skill Definition

name: Hourly Critical Error Review
id: hourly-critical-error-review
version: 0.1.0
category: observability
risk_level: low
approval_required: false
agent: oracle

## Purpose

Review recent critical and error-level operational events on a recurring hourly cadence, identify repeated patterns, and produce an operator-ready summary for ORC and The Oracle.

## Inputs

- Last-hour `error` and `critical` observed events
- Connection, stack, and container metadata
- Prior Oracle review summaries when available
- Known issue patterns and incident-learning entries

## Outputs

- Hourly critical error review summary
- Top recurring error patterns with counts and affected services
- Evidence references to source events
- Severity assessment and confidence
- Recommended next steps
- Notification recommendation such as `notify_operator: true` or `false`

## Procedure

1. Select observed events from the last complete review window, normally 60 minutes.
2. Filter to critical and error-level events before considering lower-severity context.
3. Group events by connection, stack, container, and recurring message pattern.
4. Identify repeated failures, newly appearing critical errors, and errors affecting ORC itself.
5. Separate confirmed facts from hypotheses.
6. Produce a concise review with the top issues, evidence counts, affected services, and recommended next steps.
7. Recommend operator notification only for critical issues, repeated failures, ORC self-health issues, or unusual error spikes.
8. Do not execute remediation or change polling cadence from this skill.

## Safety Rules

1. Do not mutate infrastructure.
2. Do not send external notifications directly; return a recommendation for ORC's notification layer.
3. Do not include secrets, tokens, credentials, or full sensitive log lines in the review.
4. Link recommendations to evidence and mark uncertainty clearly.

## Audit Requirements

- Record review window start and end.
- Record event counts by severity.
- Record grouped pattern counts and affected services.
- Record whether operator notification was recommended and why.
- Record the source agent and generated review timestamp.
