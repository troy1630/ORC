# Runbook Definition

name: Approved Log Review
id: approved-log-review
version: 0.1.0
category: observability
risk_level: low
autonomy_level: 1
governance: green
approval_required: false
owner_plane: observation

## Purpose

Review recent warning and error logs, summarize likely causes, and recommend whether escalation is needed.

## Trigger

- Operator requests a log review.
- Raven observes repeated warning or error events.
- Oracle needs evidence before diagnosis.

## Steps

1. Raven collects the bounded log window.
2. A worker runs the log-analysis-worker tool.
3. Oracle reviews findings and separates facts from hypotheses.
4. ORC returns a recommendation or opens a higher-risk runbook request.

## Verification

- The response includes time window, source containers, event counts, and key evidence lines.

## Rollback

- No rollback required; this runbook is read-only.

