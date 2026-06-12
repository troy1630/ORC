# Continue Task: Orchestration Chat Bubble Metadata

## Objective

Update the ORC orchestration chat bubbles so each message shows a short date plus time, and make the communication channel text bolder and brighter.

## Context

The orchestration chat UI is rendered inline in `backend/app/main.py`.

Important locations:

- CSS for chat bubbles and metadata: `backend/app/main.py`, around line 983
- Date/time formatters: `backend/app/main.py`, around lines 1594-1595
- Formatter helpers: `backend/app/main.py`, around lines 1709-1710
- Chat rendering: `backend/app/main.py`, function `renderChat()`, around line 3208

Do not read all of `backend/app/main.py` in one pass. It is too large for the local model context. Use the line map and snippets below, or use search for:

- `.chat-meta`
- `const DATE_FMT`
- `function fmtShort`
- `function renderChat`

The current chat metadata uses `fmtShort(m.created_at)`, which only shows time. Other parts of the dashboard also use `fmtShort()`, so avoid changing it globally.

## Current Code Map

CSS near line 983:

```css
.chat-meta{font-size:.69rem;color:var(--mut);margin-bottom:4px;display:flex;gap:5px;flex-wrap:wrap}
.chat-text{font-size:.85rem;line-height:1.4;overflow-wrap:anywhere}
```

Date/time constants near lines 1594-1595:

```js
const DATE_FMT=new Intl.DateTimeFormat('en-US',{timeZone:MT_ZONE,year:'numeric',month:'short',day:'2-digit',hour:'numeric',minute:'2-digit',second:'2-digit',timeZoneName:'short'});
const TIME_FMT=new Intl.DateTimeFormat('en-US',{timeZone:MT_ZONE,hour:'numeric',minute:'2-digit',second:'2-digit',timeZoneName:'short'});
```

Formatter helpers near lines 1709-1710:

```js
function fmt(iso){return iso?DATE_FMT.format(new Date(iso)):'';}
function fmtShort(iso){return iso?TIME_FMT.format(new Date(iso)):'';}
```

Start of `renderChat()` near line 3208:

```js
function renderChat(){
  const el=document.getElementById('orch-chat-list');
  if(!el)return;
  const msgs=[...(_orch.messages||[])].reverse();
  document.getElementById('orch-message-count').textContent=`${msgs.length} messages`;
  if(!msgs.length){el.innerHTML='<div class="empty">No messages yet. Tell ORC what you need.</div>';return;}
  el.innerHTML=msgs.map(m=>{
    const src=orchAgent(m.source_agent),tgt=orchAgent(m.target_agent);
    const isOperator=m.source_agent==='operator';
    const isSystem=!isOperator&&['sage','raven'].includes(m.source_agent)&&!m.target_agent;
    const right=isOperator;
    const agentCls=isOperator?'operator':chatAgentClass(m.source_agent);
    const cls=[right?'right':'',isSystem?'system':'',agentCls].filter(Boolean).join(' ');
    const senderLabel=isOperator?(_currentUser?.username||'You'):esc(src.name);
    const avatarSrc=isOperator?'/assets/characters/orc.png':agentArt(src);
    const chat=splitAgentChatText(m);
    const approvalRow=pendingApprovalForMessage(m);
    const bodyHtml=chatBubbleSummaryDetail(m.id,chat.short,chat.detail);
    const approvalHtml=approvalBubbleHtml(m);
```

Current metadata line inside `renderChat()` near line 3230:

```js
<div class="chat-meta"><span>${senderLabel}</span>${m.target_agent&&!isOperator?`<span>... ${esc(tgt.name)}</span>`:''}<span>${esc(m.message_type)}</span><span>${esc(fmtShort(m.created_at))}</span></div>
```

Replace the whole metadata line rather than trying to preserve the existing inline target logic.

## Requirements

1. On the Orchestration > Agent Chat view, each chat bubble should show a short date plus time.
2. The communication channel should be bold and brighter than the rest of the metadata.
3. Channel examples:
   - `Executioner -> operator`
   - `You -> Oracle`
   - `Raven -> ORC Orchestrator`
4. Operator messages should show their target when one exists.
5. Message type should remain visible but less prominent than the channel.
6. Do not change unrelated timestamps elsewhere in the dashboard.
7. Preserve the compact layout and wrapping behavior on narrow screens.
8. After implementation, provide the resulting diff back to Codex for review.

## Suggested Implementation

Add a chat-specific date formatter near the existing formatter constants:

```js
const CHAT_STAMP_FMT=new Intl.DateTimeFormat('en-US',{
  timeZone:MT_ZONE,
  month:'numeric',
  day:'numeric',
  year:'2-digit',
  hour:'numeric',
  minute:'2-digit',
  timeZoneName:'short'
});
```

Add a helper near `fmt()` and `fmtShort()`:

```js
function fmtChatStamp(iso){return iso?CHAT_STAMP_FMT.format(new Date(iso)):'';}
```

In `renderChat()`, build a channel label from the source and target:

- Source label should be the current username or `You` for operator messages.
- Source label should be the agent display name for agent messages.
- Target label should use `orchAgent(m.target_agent).name` when `target_agent` exists.
- Use `operator` or `Operator` consistently for messages targeting the operator.
- Escape visible labels before injecting them into HTML.
- Avoid double-escaping values that were already escaped.

Replace the current metadata row that uses `fmtShort(m.created_at)` with markup similar to:

```js
<div class="chat-meta">
  <span class="chat-channel">${channelHtml}</span>
  <span>${esc(m.message_type)}</span>
  <span>${esc(fmtChatStamp(m.created_at))}</span>
</div>
```

Add CSS near `.chat-meta`:

```css
.chat-channel{color:#f2f7ff;font-weight:850}
.chat-arrow{color:#9fb3c8;font-weight:850}
```

Prefer an exact small edit over broad rewrites. This task should only change the formatter/helper area, the chat metadata CSS, and the metadata block in `renderChat()`.

If using a separate arrow span in `channelHtml`, keep it ASCII:

```html
<span class="chat-arrow">-></span>
```

## Acceptance Criteria

- Chat bubbles show a short date plus time, for example `6/12/26, 3:45 PM MDT`.
- Channel text is visibly brighter and bold compared with message type and timestamp.
- Operator messages show the target agent when available.
- Existing non-chat timestamps still use their previous format.
- No raw user or agent text is injected without escaping.
- The chat view remains readable on desktop and mobile widths.

## Verification

After implementation:

1. Start the app using the repo's normal local workflow.
2. Open the Orchestration view.
3. Inspect existing chat bubbles.
4. Send a test message to an agent.
5. Confirm the new bubble includes:
   - source and target channel
   - message type
   - short date plus time
6. Confirm another dashboard area that uses `fmtShort()` still shows time-only formatting.

## Report Back To Codex

When finished, provide:

1. The unified diff for all changed files.
2. Any checks or manual verification performed.
3. Any assumptions, especially if the local model could not use search tools or could not run the app.
