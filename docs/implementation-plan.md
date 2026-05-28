# ORC Implementation Plan

## Phase 0: Foundation

Goal: make the project buildable and inspectable.

1. Stand up the FastAPI service, worker process, Postgres, and Redis.
2. Implement loading of `agent.md` and `skills.md` files.
3. Add health checks, config loading, and audit logging.
4. Deploy the stack in Portainer on a non-production Linux machine.

## Phase 1: Portainer Observability

Goal: collect and classify operational events.

1. Build Portainer API client.
2. Support configured environments, stacks, and containers.
3. Normalize logs into structured event records.
4. Detect high-value categories:
   - critical errors
   - authentication activity
   - restarts and crashes
   - deployments and image changes

## Phase 2: Context Correlation

Goal: explain what happened, not just what was logged.

1. Build custom API connector framework.
2. Add one or two concrete application connectors.
3. Correlate container events with application state.
4. Create workflow episode and incident grouping.

## Phase 3: Approval and Actioning

Goal: allow safe intervention.

1. Create approval request model and UI/API endpoints.
2. Add Outlook notification support through Microsoft Graph.
3. Allow agents to propose remediations.
4. Require approval before execution.
5. Write action audit logs and outcomes.

## Phase 4: Knowledge Base

Goal: convert operations into reusable documentation.

1. Create Markdown knowledge entry templates.
2. Add documenter agent workflows.
3. Store incident summaries, runbooks, and post-action notes.
4. Link entries back to source evidence.

## Phase 5: Visual Experience

Goal: express workflow as an understandable, distinctive interface.

1. Build dashboard and timeline views.
2. Add agent status views and conversation traces.
3. Design mystical orc branding and motion language.
4. Introduce the video game-inspired workflow scene after the core operator UX is stable.

## Suggested Build Order For Claude

1. Finish backend domain models and persistence.
2. Implement registry loading from Markdown.
3. Implement Portainer connector and log ingestion.
4. Add incident correlation and approval workflow.
5. Add Outlook notifications.
6. Add knowledge writer.
7. Build the operator UI.

## Exit Criteria Per Phase

Each phase should be considered done only when it has:

- runnable containers
- configuration docs
- tests for the new core logic
- auditability for actions taken
- example Markdown definitions where relevant