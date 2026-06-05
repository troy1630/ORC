# Skill Definition

name: agent-message-routing
id: agent-message-routing
version: 0.1.0
category: orchestration
risk_level: low
approval_required: false

## Purpose

Route structured messages between ORC agents while preserving visibility and audit metadata.

## Inputs

- Source agent
- Target agent
- Message type
- Summary
- Evidence payload

## Outputs

- Agent message record
- Raven stream event

## Procedure

1. Validate the source agent.
2. Attach timestamp and message type.
3. Persist the message.
4. Publish the message to Raven.

## Audit Requirements

- Record source and target agent
- Record message type
- Record timestamp
