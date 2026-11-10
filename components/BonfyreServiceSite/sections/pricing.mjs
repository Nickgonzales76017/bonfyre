import { readFileSync } from 'node:fs';
import { join } from 'node:path';
// Pricing with the arithmetic in view: cost floor, a demand curve with a draggable price, tiers tried against a
// spread of customers, a buyer survey's acceptable range, and a proposal that sets nothing.
export const needs = ['title', 'sample'];
export function render(s, site, h) {
  if (!/^\/data\/[a-z][a-z0-9-]*\/$/.test(s.data)) throw Error('The pricing desk requires packaged material.');
  if (!/^v-[a-f0-9]{64}\/$/.test(s.asset_prefix)) throw Error('The pricing desk requires a versioned package.');
  return `<section id="${h.esc(s.id)}" class="pricing-desk" data-pricing><style>${readFileSync(join(import.meta.dir, '../pricing-desk.css'), 'utf8')}</style>
<header class="pd-top"><a href="/#samples">← All work samples</a><a href="/#ask">Discuss pricing for your product</a></header>
<div class="pd-intro"><p class="pd-eyebrow">Prices, plans and margins</p><h1>${h.esc(s.title)}</h1><p class="pd-lede">${h.esc(s.lede || 'Drag the price and watch volume, profit and break-even move, try plans against real spreads of use, and leave with a proposal.')}</p></div>
<p data-ui="loading" class="pd-hint">Opening the desk…</p>
<div data-ui="ready" hidden><p data-ui="message" class="pd-message" role="status" aria-live="polite"></p>
<div class="pd-work"><nav class="pd-nav" aria-label="What to price"><h2>Waiting for a price</h2><p class="pd-hint">Aurekai's own desk lists these as advertised with no asking price, retained ${h.esc(new Date(s.sample.fetched_at).toLocaleDateString('en-GB', { day: 'numeric', month: 'long', year: 'numeric' }))}.</p><ul data-ui="list" class="pd-list"></ul>
<form data-ui="add-item" class="pd-add"><input name="title" placeholder="Price something of yours" maxlength="60" aria-label="Your product"><button type="submit">Add</button></form></nav>
<div class="pd-main"><svg data-ui="curve" class="pd-curve" viewBox="0 0 720 300" tabindex="0" role="slider" aria-label="Price. Drag along the chart or use the arrow keys."></svg><p data-ui="legend" class="pd-legend"></p>
<dl data-ui="readout" class="pd-readout"></dl><p data-ui="verdict" class="pd-verdict"></p>
<h2>Plans</h2><div data-ui="tiers"></div>
<div class="pd-pills"><label class="pd-file">Load a price survey (CSV)<input data-ui="survey" type="file" accept=".csv,text/csv"></label><button type="button" data-ui="propose" class="pd-primary">Download a price proposal</button><button type="button" data-ui="reset" class="pd-quiet">Start again</button></div>
<p class="pd-hint">A survey needs four columns per answer: too cheap, cheap, expensive, too expensive (Van Westendorp). The proposal records the price, its cost, margin, break-even and the assumptions behind it.</p></div>
<aside data-ui="assumptions" class="pd-side"></aside></div>
<p class="pd-foot">Worked out in this browser. Demand is a model through one point you state; the plan view draws customers' monthly use from a lognormal spread with a fixed seed so plans can be compared fairly.</p>
</div><noscript>JavaScript is needed to price.</noscript>
<script type="module" src="${h.esc(s.data + s.asset_prefix)}pricing-desk-client.mjs"></script></section>`;
}
