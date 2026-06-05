# Skill Definition

name: agent-skill-authoring
id: agent-skill-authoring
version: 0.1.0
category: learning
risk_level: low
approval_required: false

## Purpose

Help users and agents turn repeatable procedures into Markdown skill definitions.

## Inputs

- Agent owner
- Skill name
- Purpose
- Inputs and outputs
- Procedure
- Rollback and success criteria

## Outputs

- Markdown skill draft
- Registry entry
- Sage proposal message

## Procedure

1. Collect skill details from the user or agent.
2. Normalize the skill ID.
3. Write the Markdown skill definition.
4. Notify the assigned agent and Sage.

## Audit Requirements

- Record generated path
- Record assigned agent
- Record risk level
