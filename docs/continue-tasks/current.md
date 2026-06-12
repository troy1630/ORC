# Continue Task Packet

## Status

Ready for Qwen snippet-only rewrite.

## Mode

Use `DGX Qwen 36B Patch Writer` in normal Chat mode.

Do not use Agent mode.
Do not call tools.
Do not search.
Do not edit files directly.

## Task

Rewrite only the JavaScript function below so the Daily Token Usage chart axes are readable.

Return only the complete replacement function. Do not return a diff. Do not include explanation before the code.

## Requirements

- Keep the function name `renderAiUsageChart`.
- Keep the existing blue bars.
- Keep empty data handling.
- Use larger, higher-contrast axis labels.
- Add readable horizontal grid lines.
- Add Y-axis tick labels at 0%, 25%, 50%, 75%, and 100% of the max value.
- Format large Y-axis values compactly, such as `1.2K`, `45K`, and `2.3M`.
- Keep X-axis labels sparse for 30 days, such as every 5th day and the last day.
- Make X-axis labels readable without overlap. Rotating labels is acceptable.
- Do not add dependencies.
- Do not change any other function.

## Current Function

```js
function renderAiUsageChart(daily){
  const canvas=document.getElementById('ai-usage-chart');
  if(!canvas)return;
  const ctx=canvas.getContext('2d');
  const W=canvas.width,H=canvas.height;
  ctx.clearRect(0,0,W,H);
  if(!daily.length)return;
  const maxTokens=Math.max(...daily.map(d=>d.total_tokens),1);
  const pad={l:58,r:16,t:16,b:42};
  const chartW=W-pad.l-pad.r;
  const chartH=H-pad.t-pad.b;
  const step=chartW/daily.length;
  const barW=Math.max(4,Math.floor(step*.68));
  ctx.fillStyle='#1e293b';
  daily.forEach((d,i)=>{
    const h=Math.floor((d.total_tokens/maxTokens)*chartH);
    const x=Math.round(pad.l+i*step+(step-barW)/2);
    const y=pad.t+chartH-h;
    ctx.fillStyle='#3b82f6';
    ctx.fillRect(x,y,barW,h);
    if(i%5===0||i===daily.length-1){
      ctx.fillStyle='#888';
      ctx.font='10px sans-serif';
      ctx.fillText((d.date||'').slice(5),x,H-12);
    }
  });
  ctx.fillStyle='#888';
  ctx.font='10px sans-serif';
  ctx.fillText(maxTokens.toLocaleString(),2,pad.t+8);
  ctx.fillText('0',2,H-pad.b);
}
```

## Output

Return only:

```js
function renderAiUsageChart(daily){
  ...
}
```
