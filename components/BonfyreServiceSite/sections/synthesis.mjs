import { readFileSync } from 'node:fs';
import { join } from 'node:path';
// A literature review as working instruments: real searches of OpenAlex and
// Europe PMC, screening with reasons, quotations that are exact spans of each
// abstract, claims weighed by what is filed under them, and a write-up that
// carries only those quotations.
export const needs = ['title', 'sample'];
export function render(s, site, h) {
  if (!/^\/data\/[a-z][a-z0-9-]*\/$/.test(s.data)) throw Error('The research desk requires packaged material.');
  if (!/^v-[a-f0-9]{64}\/$/.test(s.asset_prefix)) throw Error('The research desk requires a versioned package.');
  return `<section id="${h.esc(s.id)}" class="research-desk" data-synthesis><style>${readFileSync(join(import.meta.dir, '../synthesis.css'), 'utf8')}</style>
<header class="rs-top"><a href="/#samples">← All work samples</a><a href="/#ask">Discuss a review for your team</a></header>
<div class="rs-intro"><p class="rs-eyebrow">Literature review</p><h1>${h.esc(s.title)}</h1><p class="rs-lede">${h.esc(s.lede || "Search two open research indexes, screen each paper with a reason, and write up only what exact quotations support.")}</p></div>
<p data-ui="loading" class="rs-hint">Opening the research desk…</p>
<div data-ui="ready" hidden>
<label class="rs-question">Your question<input data-ui="question" maxlength="200"></label>
<form data-ui="search" class="rs-search"><label class="rs-grow">Search<input data-ui="query" maxlength="300" placeholder="Words, phrases in quotes, OR between alternatives"></label>
<label>From<input data-ui="from" type="number" min="1900" max="2100" placeholder="year"></label><label>To<input data-ui="to" type="number" min="1900" max="2100" placeholder="year"></label>
<label class="rs-check"><input type="checkbox" data-ui="src-openalex" checked> OpenAlex</label><label class="rs-check"><input type="checkbox" data-ui="src-epmc" checked> Europe PMC</label>
<button type="submit" class="rs-primary">Search and add</button></form>
<p data-ui="search-state" class="rs-hint" role="status"></p>
<p data-ui="message" class="rs-message" role="status" aria-live="polite"></p>
<div class="rs-desk">
<nav class="rs-library" aria-label="Records"><div data-ui="filters" class="rs-filters" role="group" aria-label="Show"></div>
<div class="rs-pair"><label>Find<input data-ui="find" type="search" placeholder="in titles and abstracts"></label><label>Order<select data-ui="sort"><option value="order">As found</option><option value="year">Newest</option><option value="cited">Most cited</option></select></label></div>
<ul data-ui="list" class="rs-list"></ul></nav>
<article data-ui="reader" class="rs-reader" tabindex="-1" aria-live="polite"></article>
<aside class="rs-board" aria-label="Claims"><h2>Claims</h2><div data-ui="claims"></div>
<form data-ui="add-claim" class="rs-add"><label>Add a claim<input data-ui="claim-text" maxlength="160" placeholder="Something the evidence could support or not"></label><button type="submit">Add</button></form></aside>
</div>
<datalist id="rs-designs"><option>randomised trial</option><option>quasi-experiment</option><option>longitudinal or panel</option><option>survey</option><option>interviews or qualitative</option><option>case study</option><option>systematic review or meta-analysis</option><option>review or commentary</option></datalist>
<section aria-label="Saved artboards"><h2>Artboards in this review</h2><div data-ui="campaigns"></div></section>
<section data-ui="compose" class="rs-compose" hidden aria-label="Make a post from a quotation"><div class="rs-compose-head"><h2>Make a post</h2><button type="button" data-c="close" class="rs-quiet">Close</button></div>
<p class="rs-hint">The words and citation are shared by every format. Select an artboard to change its own size, alignment and colours.</p>
<div class="rs-boards">${['square', 'story', 'card'].map(f => `<figure data-pick="${f}" tabindex="0"><canvas data-format="${f}"></canvas><figcaption>${{ square: 'Square post', story: 'Story', card: 'Link card' }[f]}</figcaption></figure>`).join('')}</div>
<div class="rs-pair"><label>Words<textarea data-c="quote" rows="3"></textarea></label><label>Claim line<input data-c="claim" maxlength="120"></label></div>
<p data-c="exact" class="rs-hint"></p><p data-c="notes" class="rs-hint"></p>
<div class="rs-pills"><span class="rs-hint">Adjusting: <strong data-c="which"></strong></span><label>Type size<input data-c="scale" type="range" min="0.6" max="1.3" step="0.05" value="1"></label>
<button type="button" data-align="left">Left</button><button type="button" data-align="center">Centred</button><button type="button" data-theme="night">Night</button><button type="button" data-theme="lime">Lime</button><button type="button" data-theme="paper">Paper</button></div>
<div class="rs-pills"><button type="button" data-c="png" class="rs-primary">Download this format (PNG)</button><button type="button" data-c="png-all">All three (PNG)</button><button type="button" data-c="webm">Record the words appearing (WebM)</button><span data-c="status" class="rs-hint" role="status"></span></div></section>
<div class="rs-record"><div><h2>How the review went</h2><svg data-ui="flow" class="rs-flow" role="img" aria-label="Counts of records found, merged, screened, excluded and included"></svg></div>
<div><h2>Write-up</h2><p class="rs-hint">Only claims with included evidence appear, each with its quotations and references.</p><div data-ui="writeup" class="rs-writeup"></div>
<div class="rs-pills"><button type="button" data-ui="dl-md">Write-up (Markdown)</button><button type="button" data-ui="dl-csv">Evidence table (CSV)</button><button type="button" data-ui="dl-bib">BibTeX</button><button type="button" data-ui="dl-ris">RIS</button></div>
<div class="rs-pills"><button type="button" data-ui="dl-project">Download project</button><label class="rs-file">Open a project<input data-ui="open-project" type="file" accept=".json,application/json"></label><span data-ui="save-state" class="rs-hint"></span></div>
<div class="rs-pills"><button type="button" data-ui="empty" class="rs-quiet">Start an empty desk</button><button type="button" data-ui="restart" class="rs-quiet">Back to the example</button></div></div></div>
<p class="rs-foot">Searches go from your browser to <a href="https://openalex.org/" target="_blank" rel="noopener">OpenAlex</a> and <a href="https://europepmc.org/" target="_blank" rel="noopener">Europe PMC</a>. OpenAlex shares a small free daily allowance per network. The example opens on a search retained on ${h.esc(new Date(s.sample.retrieved[0].fetched_at).toLocaleDateString('en-GB', { day: 'numeric', month: 'long', year: 'numeric' }))}, with a few records already screened and quoted.</p>
</div><noscript>JavaScript is needed to search and screen.</noscript>
<script type="module" src="${h.esc(s.data + s.asset_prefix)}synthesis-client.mjs"></script></section>`;
}
