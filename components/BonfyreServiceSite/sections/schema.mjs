import { readFileSync } from 'node:fs';
import { join } from 'node:path';
// A SQLite database's structure as a working instrument: drawn with its
// references, changed by staged edits, checked against its rows, migrated on a
// copy in the browser and handed back as a file.
export const needs = ['title', 'sample'];
export function render(s, site, h) {
  if (!/^\/data\/[a-z][a-z0-9-]*\/$/.test(s.data)) throw Error('The schema workspace requires packaged material.');
  if (!/^v-[a-f0-9]{64}\/$/.test(s.asset_prefix)) throw Error('The schema workspace requires a versioned package.');
  return `<section id="${h.esc(s.id)}" class="schema-lab" data-schema><style>${readFileSync(join(import.meta.dir, '../schema-lab.css'), 'utf8')}</style>
<header class="sl-top"><a href="/#samples">← All work samples</a><a href="/#ask">Discuss a migration for your database</a></header>
<div class="sl-intro"><p class="sl-eyebrow">Database structure</p><h1>${h.esc(s.title)}</h1><p class="sl-lede">${h.esc(s.lede || "Open a SQLite file in your browser, stage changes on its diagram, and try the migration on a copy first.")}</p></div>
<p data-ui="loading" class="sl-hint">Starting SQLite in this browser…</p>
<div data-ui="ready" hidden>
<div class="sl-bar"><label class="sl-file">Open a SQLite file<input data-ui="open" type="file" accept=".sqlite,.sqlite3,.db,application/vnd.sqlite3,application/x-sqlite3"></label><button type="button" data-ui="sample">Use the sample</button><span data-ui="file-name" class="sl-hint"></span>
<form data-ui="new-table" class="sl-inline"><input name="name" placeholder="new table name" maxlength="60" aria-label="New table name"><button type="submit">Add table</button></form></div>
<p data-ui="message" class="sl-message" role="status" aria-live="polite"></p>
<div class="sl-work"><div class="sl-canvas"><svg data-ui="diagram" class="sl-diagram" role="application" aria-label="Tables and their references. Drag a table to move it; select it to change it."></svg></div><aside data-ui="inspector" class="sl-inspector"></aside></div>
<div class="sl-plan">
<div><h2>Staged changes</h2><ol data-ui="changes" class="sl-changes"></ol><div class="sl-pills"><button type="button" data-ui="try" class="sl-primary" disabled>Try it on a copy</button><span data-ui="try-note" class="sl-hint"></span></div><div data-ui="trial"></div>
<div class="sl-pills"><button type="button" data-ui="download-db" disabled>Download the changed database</button><button type="button" data-ui="download-sql">Migration (SQL)</button><button type="button" data-ui="download-diagram">Diagram (SVG)</button></div></div>
<div><h2>The migration</h2><pre data-ui="sql" class="sl-sql" aria-label="Migration statements"></pre></div></div>
<div class="sl-rows-wrap"><div class="sl-run-row"><h2>Rows</h2><div data-ui="rows-which" class="sl-pills"></div></div><div data-ui="rows" class="sl-rows"></div></div>
<p class="sl-foot">The database is opened in this browser with SQLite compiled to WebAssembly (sql.js 1.12.0, MIT). Nothing is uploaded and the original file is never written to. The sample is built from records this site keeps: papers from OpenAlex and Europe PMC, and NYC 311 phone requests from NYC Open Data.</p>
</div><noscript>JavaScript is needed to open a database.</noscript>
<script type="module" src="${h.esc(s.data + s.asset_prefix)}schema-lab-client.mjs"></script></section>`;
}
