# Skill Definition

name: incident-learning-writeback
id: incident-learning-writeback
version: 0.1.0
category: learning
risk_level: low
autonomy_level: 1
governance: green
allowed_plane: memory-learning
approval_required: false

## Purpose

Write incident outcomes, fixes, and lessons into Markdown so operators can inspect and reuse them.

## Inputs

- Lesson title
- Incident reference
- Outcome
- Summary
- Source agent

## Outputs

- Markdown learning entry
- Learning registry record
- Sage message

## Procedure

1. Capture the incident or workflow reference.
2. Separate symptoms, action, outcome, and reuse notes.
3. Write the lesson Markdown file.
4. Publish a Sage learning message.

## Audit Requirements

- Record output path
- Record incident reference
- Record source agent
