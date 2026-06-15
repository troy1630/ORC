# DGX Local Model Desktop Setup

This guide sets up a Windows desktop to use the shared DGX/GX10 Ollama server at:

```text
http://192.168.1.33:11434
```

It covers:

- Continue setup for VS Code
- GPT OSS and Qwen model profiles
- model warmup commands
- Codex `local-model-handoff` skill installation
- using the same local-model collaboration workflow in any repo

No API keys or secrets are required for the Ollama endpoint described here. The user must be on the same network or VPN that can reach `192.168.1.33`.

## Desktop Parity Note

The shareable setup below connects directly to the DGX Ollama service at `http://192.168.1.33:11434`. That is the most portable setup for another desktop because it does not depend on anything running on this machine.

This desktop may also use a local forwarding or proxy URL such as `http://127.0.0.1:8787` in some tools. Only use that localhost URL on another desktop if you also install the same local proxy there. For a clean setup, use `192.168.1.33:11434` everywhere.

## 1. Prerequisites

Install on the desktop:

- VS Code
- Continue VS Code extension
- Python 3.10 or newer
- Git
- Codex, if using Codex skills

Confirm the DGX server is reachable:

```powershell
Invoke-RestMethod http://192.168.1.33:11434/api/version
Invoke-RestMethod http://192.168.1.33:11434/api/tags
```

Expected installed models include:

```text
gpt-oss:120b
qwen3.6:latest
```

Check loaded models:

```powershell
Invoke-RestMethod http://192.168.1.33:11434/api/ps
```

If `/api/ps` returns an empty `models` list, the models are installed but not loaded in VRAM yet.

## 2. Continue Config

Create or replace:

```text
C:\Users\<your-user>\.continue\config.yaml
```

with:

```yaml
name: DGX Local
version: 1.0.0
schema: v1

models:
  - name: DGX Qwen 36B Patch Writer
    provider: ollama
    model: qwen3.6:latest
    apiBase: http://192.168.1.33:11434
    roles:
      - chat
      - edit
      - apply
    defaultCompletionOptions:
      contextLength: 8192
      maxTokens: 2048
      temperature: 0.1
      reasoning: false
      keepAlive: -1
    requestOptions:
      timeout: 180000
      extraBodyProperties:
        think: false
        keep_alive: -1

  - name: DGX Qwen 36B
    provider: ollama
    model: qwen3.6:latest
    apiBase: http://192.168.1.33:11434
    roles:
      - chat
      - edit
      - apply
      - autocomplete
    capabilities:
      - tool_use
    defaultCompletionOptions:
      contextLength: 16384
      maxTokens: 4096
      temperature: 0.2
      reasoning: false
      keepAlive: -1
    autocompleteOptions:
      debounceDelay: 450
      maxPromptTokens: 1024
      modelTimeout: 15000
      onlyMyCode: true
    requestOptions:
      timeout: 180000
      extraBodyProperties:
        think: false
        keep_alive: -1

  - name: DGX GPT OSS 120B
    provider: ollama
    model: gpt-oss:120b
    apiBase: http://192.168.1.33:11434
    roles:
      - chat
      - edit
      - apply
    capabilities:
      - tool_use
    defaultCompletionOptions:
      contextLength: 16384
      maxTokens: 4096
      temperature: 0.2
      reasoning: false
      keepAlive: -1
    requestOptions:
      timeout: 600000
      extraBodyProperties:
        think: low
        keep_alive: -1

rules:
  - Act as an implementation executor. Follow the supplied task packet instead of re-planning the project from scratch.
  - Keep edits scoped to the files and behavior requested. Do not perform unrelated refactors.
  - Before editing, identify the files in scope and the intended change in one short note.
  - For large files, do not read the whole file in one pass. Use focused search, file reads, and task-packet anchors.
  - Prefer small, reviewable changes that can be verified quickly.
  - Run the most relevant local checks or tests when available, and report exactly what passed or failed.
  - Never call the same tool with the same arguments more than once. If a search or edit attempt repeats, stop and ask for a narrower snippet.
  - If blocked, report the exact blocker, what you tried, and the smallest question needed to continue.
```

Reload VS Code after saving.

## 3. Recommended Model Roles

Use `DGX GPT OSS 120B` for:

- Continue Agent mode
- repo search and edit tasks
- implementation passes
- diff generation
- code review
- debugging

Use `DGX Qwen 36B` for:

- autocomplete
- quick inline edits
- small selected-code rewrites

Use `DGX Qwen 36B Patch Writer` for:

- normal Chat mode
- replacement snippets
- no-tools patch drafting

## 4. Warm The Models

Create:

```text
C:\Users\<your-user>\.continue\warm-dgx-continue.ps1
```

with:

```powershell
param(
  [ValidateSet("qwen", "gptoss", "both")]
  [string]$Model = "gptoss",
  [string]$OllamaBaseUrl = "http://192.168.1.33:11434",
  [object]$KeepAlive = -1,
  [int]$ContextLength = 16384,
  [switch]$Exclusive
)

$ErrorActionPreference = "Stop"

function Invoke-OllamaWarmup {
  param(
    [string]$ModelName,
    [object]$Think
  )

  $body = @{
    model = $ModelName
    messages = @(
      @{
        role = "user"
        content = "Reply with exactly: ok"
      }
    )
    stream = $false
    think = $Think
    keep_alive = $KeepAlive
    options = @{
      num_ctx = $ContextLength
      num_predict = 32
      temperature = 0.1
    }
  } | ConvertTo-Json -Depth 8

  $started = Get-Date
  $response = Invoke-RestMethod `
    -Uri "$OllamaBaseUrl/api/chat" `
    -Method Post `
    -ContentType "application/json" `
    -Body $body `
    -TimeoutSec 600

  $elapsed = [Math]::Round(((Get-Date) - $started).TotalSeconds, 1)
  [PSCustomObject]@{
    Model = $ModelName
    Seconds = $elapsed
    Reply = $response.message.content
    LoadSeconds = [Math]::Round(($response.load_duration / 1000000000), 1)
  }
}

function Stop-OllamaModel {
  param([string]$ModelName)

  $body = @{
    model = $ModelName
    keep_alive = 0
  } | ConvertTo-Json -Depth 4

  Invoke-RestMethod `
    -Uri "$OllamaBaseUrl/api/generate" `
    -Method Post `
    -ContentType "application/json" `
    -Body $body `
    -TimeoutSec 60 | Out-Null
}

if ($Exclusive -and $Model -eq "qwen") {
  Stop-OllamaModel -ModelName "gpt-oss:120b"
}

if ($Exclusive -and $Model -eq "gptoss") {
  Stop-OllamaModel -ModelName "qwen3.6:latest"
}

if ($Model -eq "qwen" -or $Model -eq "both") {
  Invoke-OllamaWarmup -ModelName "qwen3.6:latest" -Think $false
}

if ($Model -eq "gptoss" -or $Model -eq "both") {
  Invoke-OllamaWarmup -ModelName "gpt-oss:120b" -Think "low"
}

Invoke-RestMethod -Uri "$OllamaBaseUrl/api/ps" -TimeoutSec 15 |
  Select-Object -ExpandProperty models |
  Select-Object name, size_vram, context_length, expires_at
```

Warm GPT OSS:

```powershell
cd C:\Users\<your-user>\.continue
.\warm-dgx-continue.ps1 -Model gptoss -Exclusive
```

Warm Qwen:

```powershell
cd C:\Users\<your-user>\.continue
.\warm-dgx-continue.ps1 -Model qwen -Exclusive
```

## 5. Continue Smoke Tests

After selecting `DGX GPT OSS 120B` in Continue, test Agent mode with read-only work:

```text
Find the function renderAiUsageChart in backend/app/main.py. Do not edit it. Report the line number and stop.
```

Then test one harmless edit in a disposable branch or test repo:

```text
In backend/app/main.py, add one harmless comment above renderAiUsageChart saying: // AI usage chart renderer. Then show the diff and stop.
```

If either test loops or repeats the same search, stop the run and use Codex/local-model-handoff instead of Continue Agent mode.

## 6. Install The Codex Skill

Copy this folder from the original desktop:

```text
C:\Users\tfickert\.codex\skills\local-model-handoff
```

to the target desktop:

```text
C:\Users\<your-user>\.codex\skills\local-model-handoff
```

If sharing as a zip:

```powershell
Compress-Archive `
  -Path C:\Users\tfickert\.codex\skills\local-model-handoff `
  -DestinationPath local-model-handoff.zip
```

On the target desktop, unzip so the final path is:

```text
C:\Users\<your-user>\.codex\skills\local-model-handoff\SKILL.md
```

Validate the skill if the skill creator validator exists:

```powershell
python C:\Users\<your-user>\.codex\skills\.system\skill-creator\scripts\quick_validate.py C:\Users\<your-user>\.codex\skills\local-model-handoff
```

Restart or reload Codex so the skill appears in the slash menu.

## 7. Configure The Skill For This DGX

The handoff script can use the DGX directly with:

```powershell
$env:LOCAL_MODEL_BASE_URL = "http://192.168.1.33:11434"
```

To make that permanent for the current Windows user:

```powershell
[Environment]::SetEnvironmentVariable("LOCAL_MODEL_BASE_URL", "http://192.168.1.33:11434", "User")
```

Open a new terminal after setting it.

Alternatively, pass the base URL every time:

```powershell
python C:\Users\<your-user>\.codex\skills\local-model-handoff\scripts\local_model_handoff.py .codex-handoffs\current.md --mode patch --profile gptoss --base-url http://192.168.1.33:11434
```

## 8. Use The Skill In Any Repo

In each repo, add this to `.gitignore`:

```text
.codex-handoffs/
```

Create a packet:

```powershell
mkdir .codex-handoffs -Force
notepad .codex-handoffs\current.md
```

Run GPT OSS patch mode:

```powershell
python C:\Users\<your-user>\.codex\skills\local-model-handoff\scripts\local_model_handoff.py .codex-handoffs\current.md --mode patch --profile gptoss
```

Run GPT OSS review mode:

```powershell
python C:\Users\<your-user>\.codex\skills\local-model-handoff\scripts\local_model_handoff.py .codex-handoffs\current.md --mode review --profile gptoss
```

Run Qwen snippet mode:

```powershell
python C:\Users\<your-user>\.codex\skills\local-model-handoff\scripts\local_model_handoff.py .codex-handoffs\current.md --mode snippet --profile qwen
```

Read results:

```text
.codex-handoffs\latest_response.md
.codex-handoffs\latest_extracted_response.md
.codex-handoffs\latest_metadata.json
```

## 9. Packet Template

Use small, bounded tasks.

```markdown
# Local Model Packet

## Mode

patch

## Objective

Implement one clear behavior.

## Scope

Files in scope:
- path/to/file.ext

Files out of scope:
- Do not edit unrelated modules.

## Anchors

Search terms:
- `exactFunctionName`
- `exact-css-selector`

Relevant snippet:

```language
paste current code here
```

## Acceptance Criteria

- Behavior A works.
- Behavior B is unchanged.

## Verification

Run:

```powershell
command here
```

## Output

Return a unified diff only.
```

## 10. Recommended Operating Model

Use:

- Codex for planning, repo inspection, final patch application, verification, commit, push, and deployment.
- GPT OSS for Continue Agent tasks, implementation passes, patch drafting, and code reviews.
- Qwen for quick snippets, autocomplete, and selected-code rewrites.

Do not let local models deploy, commit, push, or handle secrets.

## 11. Troubleshooting

Check server reachability:

```powershell
Invoke-RestMethod http://192.168.1.33:11434/api/version
Invoke-RestMethod http://192.168.1.33:11434/api/tags
Invoke-RestMethod http://192.168.1.33:11434/api/ps
```

If no model is loaded:

```powershell
cd C:\Users\<your-user>\.continue
.\warm-dgx-continue.ps1 -Model gptoss -Exclusive
```

If Continue Agent loops:

- Stop it.
- Switch to `DGX GPT OSS 120B` if not already selected.
- Reduce the task size.
- Add a hard stop condition: `show the diff and stop`.
- Use the Codex `local-model-handoff` script instead.

If the Codex skill cannot reach the model:

```powershell
$env:LOCAL_MODEL_BASE_URL = "http://192.168.1.33:11434"
python C:\Users\<your-user>\.codex\skills\local-model-handoff\scripts\local_model_handoff.py .codex-handoffs\current.md --mode snippet --profile gptoss
```

If the model name differs on the DGX, override it:

```powershell
python C:\Users\<your-user>\.codex\skills\local-model-handoff\scripts\local_model_handoff.py .codex-handoffs\current.md --mode patch --profile gptoss --model gpt-oss:120b
```

## 12. Notes For Administrators

The DGX Ollama server must listen on the network, not only localhost. On the DGX, Ollama typically needs:

```text
OLLAMA_HOST=0.0.0.0:11434
```

Firewall rules must allow desktops to reach:

```text
192.168.1.33:11434
```

Model memory use is expected:

- `qwen3.6:latest` uses roughly 23 GB VRAM at 16k context.
- `gpt-oss:120b` uses roughly 64 GB VRAM at 16k context.

Use exclusive warmup when switching models to avoid wasting VRAM.
