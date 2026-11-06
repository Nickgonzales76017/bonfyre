import { readFileSync } from 'node:fs';
import { join } from 'node:path';
// Buyers' requirements as working instruments: coverage, weighted comparison, proof of concept, and discovery.
export const needs = ['title', 'sample'];
export function render(s, site, h) {
  if (!/^\/data\/[a-z][a-z0-9-]*\/$/.test(s.data)) throw Error('The opportunity desk requires packaged material.');
  if (!/^v-[a-f0-9]{64}\/$/.test(s.asset_prefix)) throw Error('The opportunity desk requires a versioned package.');
  return `<section id="${h.esc(s.id)}" class="opp-desk" data-opportunity><style>${readFileSync(join(import.meta.dir, '../opportunity-desk.css'), 'utf8')}</style>
<header class="od-top"><a href="/#samples">← All work samples</a><a href="/#ask">Discuss a bid or a product plan</a></header>
<div class="od-intro"><p class="od-eyebrow">Requirements, bids and roadmaps</p><h1>${h.esc(s.title)}</h1><p class="od-lede">${h.esc(s.lede || 'Every requirement in the buyer\'s own words: mark what you cover, compare options on their priorities, prove it, and see which needs recur across buyers.')}</p></div>
<p data-ui="loading" class="od-hint">Reading the requests for proposals…</p>
<div data-ui="ready" hidden><p data-ui="message" class="od-message" role="status" aria-live="polite"></p>
<nav class="od-tabs" role="tablist" aria-label="Views"><button type="button" data-tab="grid" role="tab" id="od-tab-grid" aria-controls="od-panel">Requirements</button><button type="button" data-tab="compare" role="tab" id="od-tab-compare" aria-controls="od-panel">Compare</button><button type="button" data-tab="poc" role="tab" id="od-tab-poc" aria-controls="od-panel">Proof of concept</button><button type="button" data-tab="discovery" role="tab" id="od-tab-discovery" aria-controls="od-panel">Discovery and roadmap</button></nav>
<div class="od-work"><div data-ui="view" class="od-view" id="od-panel" role="tabpanel" aria-labelledby="od-tab-grid"></div><div class="od-separator" role="separator" aria-orientation="vertical" aria-label="Resize source inspector" aria-valuemin="240" aria-valuemax="600" aria-valuenow="380" tabindex="0"></div><aside data-ui="context" class="od-context"></aside></div>
<div class="od-pills"><button type="button" data-ui="dl-compliance">Requirements and coverage (CSV)</button><button type="button" data-ui="dl-outline">Response outline (Markdown)</button><button type="button" data-ui="dl-roadmap">Roadmap (CSV)</button>
<button type="button" data-ui="read-sources">Read source documents</button><label class="od-file">Read another request (HTML or text)<input data-ui="add-doc" type="file" accept=".html,.htm,.txt,text/html,text/plain"></label><button type="button" data-ui="reset" class="od-quiet">Back to the example</button></div>
<p class="od-foot">The four requests for proposals are public pages this estate read and kept, with their digests. Sentences that ask for something are found by their words (must, shall, should, required); priority and theme are suggestions you can change. Use Read source documents to select exact passages from any retained brief, even when no automatic candidates were found.</p>
</div><noscript>JavaScript is needed for the desk.</noscript>
<script type="module" src="${h.esc(s.data + s.asset_prefix)}opportunity-desk-client.mjs"></script></section>`;
}
