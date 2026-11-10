import { readFileSync } from 'node:fs';
import { join } from 'node:path';
// The shipment document check as a working page: a visitor's own files are
// read and checked in their browser. The sample is real files run through
// the same rules.
export const needs = ['title', 'sample'];
export function render(s, site, h) {
  if (!/^\/data\/[a-z][a-z0-9-]*\/$/.test(s.data)) throw Error('The shipment check requires packaged material.');
  if (!/^v-[a-f0-9]{64}\/$/.test(s.asset_prefix)) throw Error('The shipment check requires a versioned package.');
  return `<section id="${h.esc(s.id)}" class="shipment-check" data-shipments><style>${readFileSync(join(import.meta.dir, '../shipment-check.css'), 'utf8')}</style>
<header class="sc-top"><a href="/#samples">← All work samples</a><a href="/#ask">Discuss a document check for your business</a></header>
<div class="sc-body"><p class="sc-eyebrow">Shipment documents</p><h1>${h.esc(s.title)}</h1><p class="sc-lede">${h.esc(s.lede || '')}</p>
<div class="sc-steps">
<div class="sc-card"><h2>1. Shipments</h2><label for="sc-shipments">Paste the shipment list (CSV with a shipment column; date, origin and destination if you have them)</label>
<textarea id="sc-shipments" data-ui="shipments-text" spellcheck="false" placeholder="shipment_id,ship_date,origin,destination"></textarea>
<div class="sc-row"><label class="sc-file"><span>Choose CSV</span><input type="file" accept=".csv,.txt,text/csv" data-ui="shipments-file" aria-label="Choose shipment list CSV"></label></div>
<p class="sc-status" data-ui="shipments-status" role="status">No shipment list yet.</p></div>
<div class="sc-card"><h2>2. Files</h2><div class="sc-drop" data-ui="drop">Drop photos, scans and PDFs here
<div class="sc-row" style="justify-content:center"><label class="sc-file"><span>Choose files</span><input type="file" multiple data-ui="files" aria-label="Choose shipment files"></label>
<button type="button" class="sc-secondary" data-ui="clear">Clear files</button></div></div>
<p class="sc-status" data-ui="files-status" role="status"></p>
<div class="sc-row"><button type="button" data-ui="sample">Try the sample: 8 files, 3 shipments</button></div></div>
</div>
<p class="sc-empty" data-ui="empty">Add a shipment list and some files, or try the sample. Each file is matched to its shipment, duplicates are found by content, dates come from the document or the photo, and every shipment is checked for a delivery proof.</p>
<div data-ui="results" hidden>
<div class="sc-filters" data-ui="filters" role="group" aria-label="Show files"></div>
<div class="sc-table-wrap"><table class="sc-table"><thead><tr><th>File</th><th>Shipment</th><th>Document</th><th>Date</th><th>Result</th><th>Proposed name</th></tr></thead><tbody data-ui="rows"></tbody></table></div>
<div class="sc-actions"><button type="button" data-ui="zip">Download renamed files (ZIP)</button><button type="button" class="sc-secondary" data-ui="csv">Download checked list (CSV)</button><button type="button" class="sc-secondary" data-ui="missing-csv">Download missing-proof list (CSV)</button></div>
<div class="sc-missing"><h2 data-ui="missing-count"></h2><ul data-ui="missing"></ul></div>
</div>
<p class="sc-foot">Files are read on this device and never uploaded. Your corrections are kept in this browser. Text is read from PDFs that contain text and from photo details; a scan with no text needs its shipment chosen by hand.</p>
<noscript>JavaScript is needed to read and check files.</noscript></div>
<script type="module" src="${h.esc(s.data + s.asset_prefix)}shipment-client.mjs"></script></section>`;
}
