import { readFileSync } from 'node:fs';
import { join } from 'node:path';
// A plan as a timeline that is edited directly, with its critical path, a
// simulated spread of finish dates and the cost of people doing one thing at a time.
export const needs = ['title', 'sample'];
export function render(s, site, h) {
  if (!/^\/data\/[a-z][a-z0-9-]*\/$/.test(s.data)) throw Error('The planner requires packaged material.');
  if (!/^v-[a-f0-9]{64}\/$/.test(s.asset_prefix)) throw Error('The planner requires a versioned package.');
  return `<section id="${h.esc(s.id)}" class="dep-plan" data-plan><style>${readFileSync(join(import.meta.dir, '../plan-network.css'), 'utf8')}</style>
<header class="dp-top"><a href="/#samples">← All work samples</a><a href="/#ask">Discuss planning for your projects</a></header>
<div class="dp-intro"><p class="dp-eyebrow">Plans, dependencies and dates</p><h1>${h.esc(s.title)}</h1><p class="dp-lede">${h.esc(s.lede || 'Drag tasks into order, give each a best, likely and worst length, and see when the plan really ends and what holds it up.')}</p></div>
<p data-ui="loading" class="dp-hint">Opening the plan…</p>
<div data-ui="ready" hidden>
<div class="dp-bar"><label class="dp-name">Plan<input data-ui="name" maxlength="80"></label><label>Starts<input data-ui="start" type="date"></label>
<button type="button" data-ui="people" aria-pressed="false">With people's time</button><label>Zoom<input data-ui="zoom" type="range" min="12" max="60" value="30"></label><span data-ui="save-state" class="dp-hint"></span></div>
<p data-ui="assumed" class="dp-hint"></p><p data-ui="message" class="dp-message" role="status" aria-live="polite"></p><p data-ui="problems" class="dp-problems" role="alert"></p>
<div class="dp-work"><div class="dp-scroll"><svg data-ui="gantt" class="dp-gantt" role="application" aria-label="Timeline of tasks. Drag the end of a bar to change its length; drag the dot after a bar onto another task to make it wait."></svg></div>
<aside data-ui="inspector" class="dp-inspector"></aside></div>
<p data-ui="legend" class="dp-hint"></p>
<form data-ui="add" class="dp-add"><input name="name" placeholder="A new task (it waits on the selected one)" maxlength="80" aria-label="New task"><button type="submit">Add task</button></form>
<div class="dp-end"><div><h2>When it ends</h2><div data-ui="outlook"></div></div><div><canvas data-ui="spread" class="dp-spread" aria-label="How finish dates spread across simulated runs; drag to choose a date"></canvas>
<div class="dp-pills"><button type="button" data-ui="dl-csv">Schedule (CSV)</button><button type="button" data-ui="dl-json">Plan (JSON)</button><label class="dp-file">Open a plan (CSV or JSON)<input data-ui="open" type="file" accept=".csv,.json,text/csv,application/json"></label><button type="button" data-ui="reset" class="dp-quiet">Back to the example</button></div>
<p class="dp-hint">A CSV needs a task name and a length in days; best, worst, waits-on (ids separated by ;) and owner columns are read when present.</p></div></div>
<p class="dp-foot">Everything is worked out in this browser: the critical path from likely lengths, 3,000 runs drawing each task from its best, likely and worst lengths, and a schedule in which nobody does two tasks at once. Working days skip weekends.</p>
</div><noscript>JavaScript is needed to plan.</noscript>
<script type="module" src="${h.esc(s.data + s.asset_prefix)}plan-network-client.mjs"></script></section>`;
}
