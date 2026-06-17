# ORC Architecture Recommendation

## Recommendation

Build ORC as a modular service-oriented stack with a Python orchestration core.

### Why This Stack

- Python is the fastest path for Portainer integration, API connectors, background jobs, LLM workflows, and Markdown processing.
- FastAPI provides a clean API surface for a future web client and external tools.
- Postgres gives durable relational state for incidents, approvals, and execution history.
- Redis supports lightweight queues, scheduling, and transient agent messages.
- Markdown-based specs keep agent and skill logic reviewable by humans and coding assistants.

## Logical Components

1. API service
2. Worker service
3. Registry loader for Markdown agent and skill definitions
4. Portainer adapter
5. Custom API adapter framework
6. Approval service
7. Knowledge writer
8. Notification service for Outlook
9. UI service in a later phase

## Deployment Shape

```mermaid
flowchart LR
    Operator[Operator] --> UI[ORC UI]
    UI --> API[ORC API]
    API --> DB[(Postgres)]
    API --> Redis[(Redis)]
    Worker[ORC Worker] --> Redis
    Worker --> DB
    Worker --> Portainer[Portainer API]
    Worker --> CustomApps[Custom App APIs]
    Worker --> Outlook[Microsoft Graph / Outlook]
    Worker --> KB[Markdown Knowledge Base]
```

## Containerization Pattern

Run ORC as a Portainer stack with these containers:

- `orc-api`
- `orc-worker`
- `orc-postgres`
- `orc-redis`
- `orc-ui` in phase 2

### Runtime Notes

- Mount a configuration volume for agent and skill definitions if they need live updates.
- Mount secrets through Portainer secrets or environment file references.
- Limit outbound access to approved endpoints only.

## Agent Model

An agent is a policy-bearing actor with:

- identity
- plane
- scope
- trigger rules
- allowed skills
- autonomy level
- governance boundary
- approval boundary
- output contract

A skill is a reusable capability with:

- purpose
- inputs
- outputs
- integration dependencies
- risk level
- audit requirements

## Six-Plane Operating Model

ORC separates durable decision roles from disposable execution surfaces:

1. **Control Plane - ORC**: owns routing, state, incident lifecycle, next-step planning, and registry coordination.
2. **Observation Plane - Raven**: gathers logs, events, health signals, messages, and post-action verification.
3. **Reasoning Plane - Oracle**: diagnoses evidence, ranks hypotheses, and recommends next steps.
4. **Memory / Learning Plane - Sage**: retrieves prior patterns and writes structured learning.
5. **Policy Plane - Gatekeeper**: enforces autonomy, governance color, approval policy, and escalation.
6. **Action Plane - Executioner**: executes only approved actions and records evidence.

## Three Runtime Layers

1. **Stable Core**: long-lived ORC API, agent definitions, registry loader, message bus, approval matrix, incident store, and Markdown memory.
2. **Tool Runner / Worker Pool**: disposable Docker workers for scripts, log analysis, approved runbooks, and promoted tools.
3. **Builder / Sandbox Dev**: isolated Docker workspace for generated code, tests, lint, dry-runs, packaging, and promotion requests.

Agents do not directly mutate the Stable Core. They produce plans, learning entries, tool proposals, runbook proposals, and approval requests.

## Separate Registries

ORC uses separate Markdown registries:

- `skills/*/skills.md`: instructions and runbook-like procedures that guide an agent.
- `tools/*/tool.md`: executable capabilities that run in workers or the builder sandbox.
- `runbooks/*/runbook.md`: approved operational procedures with verification and rollback.
- `memory/*/*.md`: Sage-managed episodic, semantic, procedural, and evaluative memory.

## Autonomy And Governance

Autonomy levels:

- Level 0: observe only.
- Level 1: recommend.
- Level 2: execute approved runbooks.
- Level 3: build and test new tools in sandbox.

Governance colors:

- Green: read-only analysis and reporting.
- Yellow: policy-checked workflows that follow registered skills or runbooks.
- Red: human-approved production mutation, redeploy, destructive action, credential change, or tool promotion.

## Orchestration Flow

```mermaid
sequenceDiagram
    participant Scheduler
    participant LogAgent as Portainer Log Agent
    participant ApiAgent as API Harvester Agent
    participant Orc as ORC Orchestrator
    participant Approver as User Approval
    participant DocAgent as Documenter Agent

    Scheduler->>LogAgent: Collect recent log slices
    Scheduler->>ApiAgent: Pull workflow state
    LogAgent->>Orc: Structured events
    ApiAgent->>Orc: Workflow context
    Orc->>Orc: Correlate into episode or incident
    Orc->>Approver: Proposed corrective action
    Approver-->>Orc: Approve or reject
    Orc->>DocAgent: Write outcome and evidence
    DocAgent->>Orc: Knowledge entry path
```

## Integration Standards

### Portainer

- Use Portainer API tokens supplied at runtime
- Prefer pull-based collection to start
- Scope initial support to configured endpoints and stacks

### Custom APIs

- Standard connector interface: auth, fetch, normalize, checkpoint
- Keep connector code isolated from orchestration logic

### Outlook

- Use Microsoft Graph for email and approval routing
- Model outbound messages as notifications, not free-form agent actions

## Data Handling

- Store raw evidence references and normalized event summaries
- Preserve original timestamps and source identifiers
- Maintain explicit links from incidents to approvals and remediation actions

## Security Model

1. Secrets never stored in Git.
2. Remediation requires explicit approval in phase 1.
3. Agent permissions are allow-list based.
4. Each action produces an audit record.
5. External connectors are isolated and retry-bound.

## UI Recommendation

Do not begin with a game-first interface. Build:

1. an operator dashboard
2. a workflow timeline
3. an approval inbox
4. an agent registry viewer

Then add a stylized “mystical orc command table” presentation where agents appear as visual characters moving messages across the workflow map.
