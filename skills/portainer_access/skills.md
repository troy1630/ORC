# Skill Definition

name: portainer_access
version: 0.1.0
category: integration
risk_level: medium
approval_required: false

## Purpose

Load Portainer access details from the local sentinel credential file and use them for read-only Portainer API inspection.

## Credential Source

- Local credential file: `C:\AI_dev\API_Key\portainer_sentinel.txt`
- Expected format: first non-empty line is the Portainer base URL; second non-empty line is the Portainer API token.
- Treat the file as a secret source. Never copy the URL/token into Git, Raven messages, event records, logs, screenshots, or user-facing output.

## Inputs

- Portainer sentinel credential file
- Endpoint, stack, container, or log selection rules
- Optional time window or checkpoint

## Outputs

- Authenticated Portainer client context
- Endpoint, stack, container, or log metadata
- Read-only inspection results with secrets redacted

## Procedure

1. Verify the sentinel file exists before attempting Portainer access.
2. Read only the first two non-empty lines from the file.
3. Trim whitespace, require an `http://` or `https://` base URL, and require a non-empty token.
4. Normalize the base URL by removing trailing slashes.
5. Authenticate with the Portainer API using the token as the `X-API-Key` header.
6. Prefer the existing ORC Portainer adapter when operating inside the backend.
7. Use read-only endpoints by default, such as status, endpoints, container listing, and container logs.
8. Redact credentials from all returned records, diagnostics, and audit entries.

## Safety Rules

1. Do not call mutating Portainer endpoints unless a separate approved remediation workflow explicitly requires it.
2. Do not persist the token in the database or any Markdown registry file.
3. Do not print the token while debugging; report only that the credential file was found or missing.
4. Fail closed if the sentinel file is missing, malformed, unreadable, or contains fewer than two non-empty lines.
5. Keep Portainer access scoped to configured endpoints and containers.

## Audit Requirements

- Record that the sentinel file path was used, not its contents.
- Record endpoint IDs and API paths called.
- Record request outcome and item counts.
- Record whether TLS verification was disabled for an internal/self-signed Portainer endpoint.
