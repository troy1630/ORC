# Skill Definition

name: knowledge-base-writeback
version: 0.1.0
category: documentation
risk_level: low

## Purpose

Write Markdown knowledge entries based on incidents, workflows, and approved actions.

## Inputs

- Incident summary
- Evidence links
- Action result
- Template selection

## Outputs

- Markdown file path
- Index update

## Procedure

1. Select the template.
2. Populate facts, evidence, actions, and outcome.
3. Write the Markdown file.
4. Update index metadata.

## Audit Requirements

- Record output path
- Record source incident
- Record template used