import { readFileSync } from 'node:fs';
import { join } from 'node:path';
// A queued process as a working simulator: the map and the day are edited
// directly, worked out by formula at once and by event simulation in the
// page's worker. The sample replays a real day of NYC 311 phone requests.
export const needs = ['title', 'sample'];
export function render(s, site, h) {
  if (!/^\/data\/[a-z][a-z0-9-]*\/$/.test(s.data)) throw Error('The simulator requires packaged material.');
  if (!/^v-[a-f0-9]{64}\/$/.test(s.asset_prefix)) throw Error('The simulator requires a versioned package.');
  const json = JSON.stringify({ nycDay: s.sample.day }).replace(/</g, '\\u003c');
  return `<section id="${h.esc(s.id)}" class="process-sim" data-simulation><style>${readFileSync(join(import.meta.dir, '../process-sim.css'), 'utf8')}</style>
<script type="application/json" data-simulation-config>${json}</script>
<header class="ps-top"><a href="/#samples">← All work samples</a><a href="/#ask">Discuss a model of your own process</a></header>
<div class="ps-intro"><p class="ps-eyebrow">Queues, staffing and flow</p><h1>${h.esc(s.title)}</h1><p class="ps-lede">${h.esc(s.lede || "Draw how work moves through your team, set staff hour by hour, and play back a real day. Formula at once, simulation to check.")}</p></div>
<p data-ui="loading" class="ps-hint ps-pad">Opening the simulator…</p>
<div data-ui="ready" hidden>
<div class="ps-bar-row" role="toolbar" aria-label="Process">
<label class="ps-name">Process<input data-ui="name" maxlength="80"></label>
<button type="button" data-ui="add-station">Add a step</button><button type="button" data-ui="play">Play the day</button>
<label class="ps-inline">Speed<select data-ui="speed"><option value="600">10 min a second</option><option value="1800" selected>30 min a second</option><option value="3600">1 hour a second</option></select></label>
<output data-ui="clock" class="ps-clock-face" aria-label="Time of day in the playback">09:00</output>
<span data-ui="save-state" class="ps-hint"></span></div>
<p data-ui="assumed" class="ps-hint ps-pad"></p>
<p data-ui="message" class="ps-message" role="status" aria-live="polite"></p><p data-ui="problems" class="ps-problems" role="alert"></p>
<div class="ps-work">
<div class="ps-stage">
<svg data-ui="map" class="ps-map" viewBox="0 0 1000 520" role="application" aria-label="Process map. Drag steps to move them; drag the dot on a step's edge onto another step to send work there; Delete removes a selected step."></svg>
<svg data-ui="strip" class="ps-strip" viewBox="0 0 960 220" preserveAspectRatio="none" role="application" aria-label="The day by the hour. Drag the staff line to change staff for the selected step; click to move the playback clock."></svg>
<p data-ui="strip-legend" class="ps-legend"></p>
</div>
<aside data-ui="inspector" class="ps-inspector" aria-label="Selected step"></aside>
</div>
<div class="ps-outcome">
<div><div class="ps-run-row"><h2>What happens</h2><span data-ui="run-state" class="ps-hint" role="status"></span></div>
<div data-ui="results"></div></div>
<div><h2 data-ui="histogram-label" class="ps-h3"></h2><canvas data-ui="histogram" class="ps-histogram" aria-label="Spread of waiting times"></canvas>
<div class="ps-controls"><label>Runs<select data-ui="reps"><option>5</option><option selected>10</option><option>20</option><option>30</option></select></label>
<label data-ui="days-wrap">Days per run<select data-ui="days"><option>1</option><option selected>3</option><option>7</option></select></label>
<label>Random seed<input data-ui="seed" type="number" min="1" step="1" value="1"></label></div>
<div class="ps-pills"><button type="button" data-ui="keep">Keep for comparison</button><button type="button" data-ui="save">Save under this name</button><button type="button" data-ui="share">Copy a link</button></div>
<input data-ui="share-url" class="ps-share" readonly hidden aria-label="Link to this process">
<div class="ps-pills"><button type="button" data-ui="download-results">Results (CSV)</button><button type="button" data-ui="download-log">Every visit (CSV)</button><button type="button" data-ui="download-model">Process (JSON)</button><label class="ps-file ps-pill">Open a process<input data-ui="import-model" type="file" accept=".json,application/json"></label></div>
<h3 class="ps-h3">From hours to hires</h3><p class="ps-hint" data-ui="hire-need"></p>
<div class="ps-controls ps-hire"><label>People today<input data-hire="current" type="number" min="0" step="1"></label><label>Hours each a day<input data-hire="hours" type="number" min="1" max="12" step="0.5" value="7.5"></label><label>Time away (%)<input data-hire="shrinkage" type="number" min="0" max="80" step="1" value="30"></label>
<label>Leave a month (%)<input data-hire="attrition" type="number" min="0" max="30" step="0.5" value="2"></label><label>Months to full speed<input data-hire="ramp" type="number" min="0" max="12" step="1" value="3"></label><label>Months to start<input data-hire="lead" type="number" min="0" max="6" step="1" value="1"></label>
<label>Work grows a month (%)<input data-hire="growth" type="number" min="-10" max="20" step="0.5" value="1"></label></div>
<svg data-ui="hire-chart" class="ps-hire-chart" viewBox="0 0 520 170" role="img" aria-label="People needed and people working, month by month"></svg><ol data-ui="hire-steps" class="ps-hire-steps"></ol>
<h3 class="ps-h3">Saved in this browser</h3><ul data-ui="saved" class="ps-saved"></ul>
<div class="ps-pills"><button type="button" data-ui="new" class="ps-quiet">Start from one step</button><button type="button" data-ui="reset" class="ps-quiet">Back to the NYC 311 example</button></div></div>
</div>
<p class="ps-foot">Everything runs in this browser. Arrivals in the example are the city's published records, read from <a href="${h.esc(s.sample.source.dataset)}" target="_blank" rel="noopener">NYC Open Data</a>; handling times and staff are starting assumptions until you replace them.</p>
</div>
<noscript>JavaScript is needed to run the simulator.</noscript>
<script type="module" src="${h.esc(s.data + s.asset_prefix)}process-sim-client.mjs"></script></section>`;
}
