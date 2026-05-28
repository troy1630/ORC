# Claude Build Brief

Build ORC as a production-minded but incremental orchestration platform.

## Constraints

- Run in Docker and Portainer on Linux.
- Do not commit secrets.
- Treat Markdown definitions as first-class configuration.
- All remediation actions require approval in phase 1.
- Favor clean interfaces over framework-heavy abstractions.

## Required Deliverables

1. Expand the FastAPI scaffold into a working API and worker system.
2. Implement Markdown registry loading for agents and skills.
3. Add a Portainer integration that can collect and normalize container logs.
4. Add at least one custom API connector example.
5. Add approval flows and Outlook notification support.
6. Add Markdown knowledge-base output.
7. Create a first-pass UI for incidents, workflows, and approvals.

## Build Principles

1. Keep modules small and explicit.
2. Make every agent action auditable.
3. Return evidence with conclusions.
4. Keep the UI operator-first, not gimmick-first.
5. Preserve the ability for humans to inspect the exact agent and skill rules.

## Important Domain Notes

- ORC is not just a chatbot. It is an orchestrator, observer, and approval router.
- The Portainer integration is a core source of truth for runtime activity.
- Agent-to-agent communication should be structured and persisted.
- The knowledge base should remain in Markdown so it stays durable and portable.

## First Implementation Targets

1. Database schema for incidents, events, approvals, actions, agents, skills
2. Registry parser for Markdown definitions
3. Portainer adapter and polling job
4. Incident summarization service
5. Approval endpoints
6. Knowledge entry writer

## Definition Of Good

The system is good when an operator can see what happened, why ORC believes it happened, what action is proposed, and what evidence supports that proposal.