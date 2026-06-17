# Skill Definition

name: outlook-approval-routing
version: 0.1.0
category: communication
risk_level: medium
autonomy_level: 1
governance: yellow
allowed_plane: policy

## Purpose

Route approval requests and incident summaries through Outlook using Microsoft Graph.

## Inputs

- Approval request payload
- Recipient list
- Message template

## Outputs

- Message delivery record
- Response mapping
- Approval status update

## Procedure

1. Build approval message.
2. Send through Graph.
3. Track message identifier.
4. Capture operator response.
5. Return normalized status.

## Audit Requirements

- Record recipient list
- Record message ID
- Record approval outcome
