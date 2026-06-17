# Tool Definition

name: Sandbox Python Runner
id: sandbox-python-runner
version: 0.1.0
category: builder
risk_level: medium
autonomy_level: 3
governance: red
execution_surface: builder-sandbox
approval_required: true

## Purpose

Run generated Python code, tests, lint checks, and dry-runs inside the isolated builder sandbox before any promotion into the worker pool.

## Inputs

- Proposed tool source
- Test plan
- Dry-run fixtures
- Expected safety boundary

## Outputs

- Test result summary
- Lint/check output
- Dry-run evidence
- Promotion recommendation

## Execution Boundary

- Builder-only execution.
- No production credentials.
- No direct Stable Core mutation.
- No promotion without Gatekeeper review and human approval.

## Verification

- All tests must pass.
- Dry-run output must be attached to the promotion request.
- The generated artifact must declare autonomy level, governance color, rollback, and verification steps.

