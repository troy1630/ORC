# Continue Task Packet

## Status

Ready for Continue as a patch-generation task.

## Important Mode

Do not use agent tool calls if Continue is producing XML or tool-call parser errors.

For this test, generating a unified diff is enough. If direct file editing fails, stop trying tools and return:

1. A unified diff for `backend/app/main.py`.
2. A short result note with checks you would run.

Avoid XML-like output. Avoid raw markup snippets unless they are inside a unified diff.

## Objective

Fix the Admin > AI Usage Daily Token Usage chart:

1. Move Daily Token Usage to the top of the AI Usage view.
2. Give the chart more vertical and horizontal space so it is not squashed.
3. Always show a 30-day window and size the bars appropriately.

Keep this as a small, scoped implementation in `backend/app/main.py`.

## Scope

Files likely in scope:
- `backend/app/main.py`

Files out of scope:
- Do not edit unrelated Admin tabs.
- Do not change AI usage logging.
- Do not change authentication, connection management, orchestration chat, or retention behavior.
- Do not introduce a charting library.

## Relevant Anchors

Do not read all of `backend/app/main.py`. Use focused search.

Search terms:
- `ADMIN: AI Usage`
- `admin-view-ai-usage`
- `function loadAiUsage`
- `function renderAiUsageChart`
- `/admin/ai-usage`

Areas to edit:
- Admin AI Usage HTML around line 1188.
- AI usage JS functions around line 2849.
- AI usage backend endpoint around line 5819.

## Current Behavior Summary

The Admin AI Usage screen currently has:
- A day selector with 7, 30, and 90 day options.
- Summary cards first.
- Usage by Agent table second.
- Daily Token Usage chart last.
- A small chart canvas around 700 by 160.
- The backend only returns days that have usage records.

## Required Behavior

The Admin AI Usage screen should have:
- Header row with title, a non-interactive `Last 30 days` label, and Refresh button.
- Daily Token Usage chart immediately below the header.
- A larger chart canvas, for example 1000 by 260, styled to fill available width.
- Summary cards below the chart.
- Usage by Agent table below the cards.
- No interactive 7/30/90 selector.

The data loader should:
- Always fetch `/admin/ai-usage?days=30`.
- Not read from `ai-usage-days`.

The chart renderer should:
- Keep using canvas.
- Handle exactly 30 daily points.
- Use robust bar sizing based on available chart width.
- Avoid negative or zero bar widths.
- Label dates sparsely, such as every 5th day and the last day.
- Clear the chart if there is no data.

The backend endpoint should:
- Default `days` to 30.
- Clamp days between 1 and 365.
- Return a complete daily series with one entry per day.
- For `days=30`, return exactly 30 daily entries.
- Include zero totals for days without usage.
- Keep totals and rows behavior otherwise intact.

Implementation hint for backend dates:
- Use `today = datetime.now(timezone.utc).date()`.
- Use `start_date = today - timedelta(days=days - 1)`.
- Use `cutoff = datetime.combine(start_date, datetime.min.time(), tzinfo=timezone.utc)`.
- Group row dates using UTC dates.
- Build `daily` by looping over `range(days)`.

## Definition Of Done

- Daily Token Usage appears above cards and table.
- Chart has more room and is readable.
- UI always uses 30 days.
- Backend returns 30 daily points for `days=30`, including missing days as zero totals.
- Summary cards and Usage by Agent still work.
- No unrelated changes.

## Verification

Suggested check:

```powershell
python -m py_compile backend\app\main.py
```

Optional if app is running and authenticated:

```powershell
Invoke-RestMethod "http://127.0.0.1:8000/admin/ai-usage?days=30"
```

## Report Back

Preferred for this test: paste a unified diff in chat instead of relying on Continue file-writing tools.

If direct editing succeeds, also write a result note to `docs/continue-results/current.md`.
