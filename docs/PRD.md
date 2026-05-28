# ORC Product Requirements Document

## Product Summary

ORC is an agent orchestration application that observes containerized workloads, application APIs, and agent activity across a Linux server managed in Portainer. It builds a coherent workflow narrative, surfaces issues that matter, routes proposed remediations for approval, and writes durable operational knowledge into Markdown.

## Problem Statement

Current operations data is fragmented:

- Container logs show symptoms but not business context.
- Custom application APIs expose state but not cross-system causality.
- Human operators approve fixes through separate channels.
- Lessons learned are rarely captured in a reusable format.

ORC should unify these surfaces into one operator-facing system.

## Product Vision

Build a containerized orchestration layer that feels understandable and inspectable. Agents should be composable, skills should be human-readable, and all corrective actions should be approval-gated.

## Primary Users

- Operator reviewing Portainer-managed environments
- Technical lead supervising agent actions
- Developer extending integrations or adding new agents
- Documentation owner maintaining operational knowledge

## Key Objectives

1. Ingest and classify Portainer and container log events.
2. Pull workflow context from custom application APIs.
3. Correlate events into incidents, timelines, and narratives.
4. Enable agents to propose corrective actions without executing them until approved.
5. Route human communications through Outlook.
6. Persist learned procedures and observations into Markdown knowledge files.
7. Support plug-in style expansion through `agent.md` and `skills.md` files.

## Non-Goals For Phase 1

- Fully autonomous remediation without approval
- Broad multi-cluster support beyond one Linux server and one Portainer instance
- Deep SIEM replacement features
- Final high-fidelity game UI implementation

## Day In The Life

1. ORC polls Portainer and selected containers for recent logs.
2. The log collector agent flags authentication failures, restart loops, error spikes, and deployment changes.
3. The API harvester agent gathers workflow state from server-side applications.
4. ORC correlates both streams into a timeline of what happened and which services were involved.
5. A remediation agent proposes a fix, such as restarting a failing service, rotating a stale session, or requesting a configuration update.
6. ORC asks the user for approval through the UI and optionally Outlook.
7. Once approved, the action is executed and logged.
8. The documenter agent writes a Markdown summary into the knowledge base.

## Core Features

### 1. Observability Ingestion

- Pull logs from Portainer-managed containers
- Normalize logs into structured events
- Detect critical errors, warnings, auth events, deployments, and unusual activity

### 2. Workflow Correlation

- Correlate log events with API data and prior incidents
- Group events into workflows, incidents, and narratives
- Display confidence and supporting evidence for each conclusion

### 3. Agent Registry

- Load agent definitions from `agents/*/agent.md`
- Load skill definitions from `skills/*/skills.md`
- Allow enabling, disabling, and versioning of agents and skills

### 4. Approval-Gated Remediation

- Agents may propose actions
- ORC must require human approval before executing a corrective action in phase 1
- Approval records must be stored with user identity, timestamp, reason, and outcome

### 5. Communication Layer

- Send summaries and approval requests through Outlook
- Record responses and associate them with workflows or incidents

### 6. Knowledge Capture

- Create Markdown incident summaries
- Record remediation steps, false positives, and open questions
- Preserve links to the source logs and APIs that informed the conclusion

### 7. Operator Experience

- Dashboard showing incidents, workflows, approvals, and agent status
- Future visual layer where agents are represented as characters moving across a workflow map
- Branding centered on a mystical orc motif

## Functional Requirements

1. The system shall run as containers in Portainer on Linux.
2. The system shall authenticate to Portainer using externally managed secrets.
3. The system shall ingest logs from configured stacks, services, or containers.
4. The system shall support custom API connectors with per-connector authentication.
5. The system shall register agent and skill definitions from Markdown files on startup.
6. The system shall allow agents to exchange structured messages through an internal orchestration bus.
7. The system shall require explicit user approval before remediation actions are executed.
8. The system shall store event history, approvals, and knowledge records.
9. The system shall expose APIs for UI, automation, and future external agent integrations.
10. The system shall write audit entries for every action taken by ORC or a child agent.

## Non-Functional Requirements

- Reliability: degraded integrations must not break the core app
- Security: secrets are mounted, never committed
- Traceability: every conclusion links back to evidence
- Extensibility: new agents and skills can be added without changing core architecture
- Explainability: the operator can inspect prompts, rules, evidence, and approvals

## Data Model Overview

- `AgentDefinition`
- `SkillDefinition`
- `IntegrationConnection`
- `ObservedEvent`
- `WorkflowEpisode`
- `Incident`
- `ApprovalRequest`
- `ActionExecution`
- `KnowledgeEntry`

## Risks

- Logs alone may be noisy and ambiguous
- Portainer API scope may vary across environments
- Outlook integration adds identity and tenant complexity
- Premature UI gamification could distract from core operator value

## Success Metrics

- Time from event occurrence to operator-ready summary
- Reduction in manual log review time
- Approval turnaround time
- Percentage of incidents captured into Markdown knowledge files
- False positive rate for critical incident detection