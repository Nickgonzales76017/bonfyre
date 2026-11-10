// The research desk. Search two open indexes, screen what comes back with a
// reason for every exclusion, select a passage in an abstract to file it as
// evidence for a claim, and write up only what the passages support. Work is
// kept in this browser; a project can be downloaded and opened again.
import * as syn from './synthesis.mjs';
import { attachInstrumentStore, openInstrumentScratch } from './instrument-store.mjs';
import { openComposer, resetComposer, refreshComposer } from './campaign-composer-client.mjs';
import { campaignFor, campaignSourceStatus, validateCampaigns, prepareCampaignSource, applyCampaignSource } from './campaign-composer.mjs';

const root = document.querySelector('[data-synthesis]');
const $ = name => root.querySelector(`[data-ui="${name}"]`);
const here = new URL('.', import.meta.url);
const KEY = 'aurekai-research-desk-v1';
const esc = t => String(t ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
const say = (text, bad) => { const m = $('message'); m.textContent = text; m.dataset.tone = bad ? 'bad' : ''; };
const store = await openInstrumentScratch(KEY, null);
const prepareProject = value => { const project = syn.prepareReviewProject(value); validateCampaigns(project); return project; };
let importAttempt = 0;

let editEpoch = 0;
let P = null, view = { filter: 'todo', sort: 'order', current: null, pending: null, query: '' };
const rec = id => P.records.find(r => r.id === id);
const decision = id => P.decisions[id];

function persist() { editEpoch++; const ok = store.set(P); $('save-state').textContent = ok ? store.description : 'This browser is not keeping changes; download the project to keep it.'; }
function touch() { persist(); renderAll(); }

// ------------------------------------------------------------ searching

let searchAttempt = 0, searchAbort = null;
function cancelSearch() { searchAttempt++; searchAbort?.abort(); searchAbort = null; $('search-state').textContent = ''; }
window.addEventListener('bonfyre:account-changed', () => { importAttempt++; cancelSearch(); });
window.addEventListener('pagehide', () => { importAttempt++; cancelSearch(); });

async function search(e) {
  e?.preventDefault();
  const query = $('query').value.trim(), from = +$('from').value || null, to = +$('to').value || null;
  if (!query) { say('Type what to search for.'); return; }
  const want = { openalex: $('src-openalex').checked, epmc: $('src-epmc').checked };
  if (!want.openalex && !want.epmc) { say('Choose at least one index.'); return; }
  cancelSearch(); const project = P, attempt = searchAttempt, controller = new AbortController(); searchAbort = controller;
  const signal = AbortSignal.any([controller.signal, AbortSignal.timeout(30000)]);
  $('search-state').textContent = 'Searching…';
  const got = [], receipts = [], failed = [];
  await Promise.all([
    want.openalex && fetch(syn.openAlexUrl({ query, from, to, perPage: 50 }), { signal }).then(async r => { if (!r.ok) throw Error(r.status === 429 ? 'daily free allowance used up from this network' : `answered ${r.status}`); const j = await r.json(); got.push(...j.results.map(syn.fromOpenAlex)); receipts.push({ url: r.url, fetched_at: new Date().toISOString(), status: r.status }); }).catch(err => failed.push(`OpenAlex ${err.message}`)),
    want.epmc && fetch(syn.europePmcUrl({ query, from, to, pageSize: 50 }), { signal }).then(async r => { if (!r.ok) throw Error(`answered ${r.status}`); const j = await r.json(); got.push(...(j.resultList?.result ?? []).map(syn.fromEuropePmc)); receipts.push({ url: r.url, fetched_at: new Date().toISOString(), status: r.status }); }).catch(err => failed.push(`Europe PMC ${err.message}`)),
  ].filter(Boolean));
  if (P !== project || attempt !== searchAttempt || controller.signal.aborted) return;
  searchAbort = null;
  const usable = got.filter(r => r.abstract.length > 80 && r.title);
  const before = P.records.length, merged = syn.merge([...P.records, ...usable]);
  const added = merged.records.length - before;
  P.records = merged.records; P.identified = (P.identified ?? before) + usable.length; P.duplicates = [...(P.duplicates ?? []), ...merged.duplicates.filter(d => !P.duplicates?.some(x => x.id === d.id))];
  P.retrieved = [...(P.retrieved ?? []), ...receipts]; P.search = { query, from, to };
  $('search-state').textContent = `${usable.length} found with abstracts; ${added} new, ${usable.length - added} already here or the same work in both indexes.${failed.length ? ' ' + failed.join('; ') + '.' : ''}`;
  view.filter = 'todo'; touch();
}

// ------------------------------------------------------------ the list

function listed() {
  const q = view.query.toLowerCase();
  let rows = P.records.filter(r => { const d = decision(r.id)?.status; return view.filter === 'all' || (view.filter === 'todo' ? !d : d === view.filter); });
  if (q) rows = rows.filter(r => (r.title + ' ' + r.abstract + ' ' + r.authors.join(' ')).toLowerCase().includes(q));
  if (view.sort === 'year') rows = [...rows].sort((a, b) => (b.year ?? 0) - (a.year ?? 0));
  if (view.sort === 'cited') rows = [...rows].sort((a, b) => b.cited - a.cited);
  return rows;
}
function renderList() {
  const f = syn.flow(P), counts = { todo: f.unscreened, include: f.included, maybe: f.maybe, exclude: f.excludedTotal, all: P.records.length };
  $('filters').innerHTML = [['todo', 'To screen'], ['include', 'Included'], ['maybe', 'Maybe'], ['exclude', 'Excluded'], ['all', 'All']].map(([k, t]) => `<button type="button" data-filter="${k}" aria-pressed="${view.filter === k}">${t} <span>${counts[k]}</span></button>`).join('');
  const rows = listed();
  $('list').innerHTML = rows.length ? rows.map(r => { const d = decision(r.id); return `<li><button type="button" data-open="${esc(r.id)}" class="${view.current === r.id ? 'is-current' : ''}" aria-current="${view.current === r.id}">
    <span class="rs-mark rs-${d?.status ?? 'todo'}" aria-hidden="true"></span><span class="rs-li-title">${esc(r.title)}</span><span class="rs-li-meta">${esc(syn.cite(r))}${r.cited ? ` · cited ${r.cited.toLocaleString()}` : ''}${P.excerpts.some(e => e.record === r.id) ? ` · ${P.excerpts.filter(e => e.record === r.id).length} quoted` : ''}</span></button></li>`; }).join('')
    : `<li class="rs-hint">${view.filter === 'todo' ? 'Everything is screened. Search again to find more.' : 'Nothing here.'}</li>`;
}
$('filters').addEventListener('click', e => { const b = e.target.closest('[data-filter]'); if (b) { view.filter = b.dataset.filter; renderList(); } });
$('list').addEventListener('click', e => { const b = e.target.closest('[data-open]'); if (b) openRecord(b.dataset.open); });
$('find').addEventListener('input', () => { view.query = $('find').value; renderList(); });
$('sort').addEventListener('change', () => { view.sort = $('sort').value; renderList(); });

// ------------------------------------------------------------ reading

function openRecord(id) { view.current = id; view.pending = null; renderReader(); renderList(); $('reader').focus({ preventScroll: true }); }
function renderReader() {
  const r = rec(view.current), box = $('reader');
  if (!r) { box.innerHTML = '<p class="rs-hint">Choose a record on the left. Keys: <kbd>I</kbd> include, <kbd>X</kbd> exclude, <kbd>M</kbd> maybe, <kbd>J</kbd>/<kbd>K</kbd> next and previous.</p>'; return; }
  const d = decision(r.id) ?? {}, guessDesign = syn.design(r.abstract), sizes = syn.sampleSizes(r.abstract);
  const marks = syn.termsIn(r.abstract, syn.questionTerms(P.question)), quoted = P.excerpts.filter(e => e.record === r.id);
  box.innerHTML = `<p class="rs-eyebrow">${esc(r.year ?? 'Undated')}${r.venue ? ' · ' + esc(r.venue) : ''}</p><h2>${esc(r.title)}</h2>
  <p class="rs-authors">${esc(r.authors.slice(0, 8).join(', '))}${r.authors.length > 8 ? ` and ${r.authors.length - 8} more` : ''}</p>
  <p class="rs-links"><a href="${esc(r.url)}" target="_blank" rel="noopener">Read the paper ↗</a>${r.sources.map(s => ` <a href="${esc(s.url)}" target="_blank" rel="noopener">${esc(s.name)} ↗</a>`).join('')}</p>
  <div class="rs-decide" role="group" aria-label="Screening"><button type="button" data-decide="include" aria-pressed="${d.status === 'include'}">Include <kbd>I</kbd></button><button type="button" data-decide="maybe" aria-pressed="${d.status === 'maybe'}">Maybe <kbd>M</kbd></button>
  <label class="rs-exclude">Exclude because <select data-ui="reason"><option value="">choose a reason <kbd>X</kbd></option>${syn.REASONS.map(x => `<option${d.reason === x ? ' selected' : ''}>${esc(x)}</option>`).join('')}</select></label></div>
  <div class="rs-pair"><label>Kind of study<input data-ui="design" value="${esc(d.design ?? '')}" placeholder="${esc(guessDesign || 'not stated')}" list="rs-designs"></label><label>How many studied<input data-ui="n" type="number" min="1" value="${d.n ?? ''}" placeholder="${sizes[0] ? sizes[0].n.toLocaleString() : 'not stated'}"></label></div>
  ${guessDesign || sizes.length ? `<p class="rs-hint">The abstract itself mentions ${[guessDesign && `a ${guessDesign}`, sizes.length && sizes.map(s => `“${esc(s.text)}”`).join(', ')].filter(Boolean).join(' and ')}. Confirm before relying on it; empty fields stay empty.</p>` : ''}
  <p class="rs-abstract" data-ui="abstract" tabindex="0" aria-label="Abstract. Select a passage to file it as evidence.">${paint(r.abstract, marks, quoted)}</p>
  <div data-ui="file" class="rs-filebox" hidden></div>
  <p class="rs-hint">${r.abstractFrom ? `Abstract as published in ${esc(r.abstractFrom)}. ` : ''}Select words in the abstract to quote them. Words of your question are underlined.</p>`;
}
function paint(text, marks, quoted) {
  // Underlines and quoted spans are drawn over the exact characters; the visible text equals the abstract.
  const cuts = new Set([0, text.length]); for (const [a, b] of marks) { cuts.add(a); cuts.add(b); } for (const e of quoted) { cuts.add(e.start); cuts.add(e.end); }
  const at = [...cuts].sort((a, b) => a - b); let html = '';
  for (let i = 0; i < at.length - 1; i++) { const [a, b] = [at[i], at[i + 1]], piece = esc(text.slice(a, b));
    const q = quoted.find(e => e.start <= a && b <= e.end), m = marks.some(([x, y]) => x <= a && b <= y);
    html += q ? `<mark class="rs-q rs-${q.direction}" data-ex="${esc(q.id)}">${m ? `<u>${piece}</u>` : piece}</mark>` : m ? `<u>${piece}</u>` : piece; }
  return html;
}
function offsetIn(container, node, offset) {
  const walk = document.createTreeWalker(container, NodeFilter.SHOW_TEXT); let total = 0;
  for (let n = walk.nextNode(); n; n = walk.nextNode()) { if (n === node) return total + offset; total += n.length; }
  return node === container ? [...container.childNodes].slice(0, offset).reduce((t, c) => t + c.textContent.length, 0) : -1;
}
document.addEventListener('selectionchange', () => {
  const abstract = $('abstract'), sel = getSelection(); if (!abstract || !sel.rangeCount || sel.isCollapsed) return;
  const range = sel.getRangeAt(0); if (!abstract.contains(range.startContainer) || !abstract.contains(range.endContainer)) return;
  const a = offsetIn(abstract, range.startContainer, range.startOffset), b = offsetIn(abstract, range.endContainer, range.endOffset);
  if (a < 0 || b <= a) return;
  try { view.pending = syn.excerpt(rec(view.current), a, b); } catch { return; }
  if (view.pending.text.length < 12) return;
  const box = $('file'); box.hidden = false;
  box.innerHTML = `<blockquote>“${esc(view.pending.text)}”</blockquote><div class="rs-pair"><label>File under<select data-ui="file-claim">${P.claims.map(c => `<option value="${esc(c.id)}">${esc(c.text)}</option>`).join('')}<option value="__new">A new claim…</option></select></label>
  <label>This passage<select data-ui="file-direction"><option value="supports">supports it</option><option value="against">goes against it</option><option value="mixed">is mixed</option></select></label></div>
  <label data-ui="file-new-wrap" ${P.claims.length ? 'hidden' : ''}>New claim<input data-ui="file-new" maxlength="160" placeholder="State it as something the evidence could support or not"></label>
  <label>Note (optional)<input data-ui="file-note" maxlength="300"></label><div class="rs-pills"><button type="button" class="rs-primary" data-ui="file-save">File this quotation</button><button type="button" data-ui="file-cancel">Cancel</button></div>`;
  if (!P.claims.length) box.querySelector('[data-ui="file-claim"]').value = '__new';
});
$('reader').addEventListener('change', e => {
  const r = rec(view.current); if (!r) return;
  if (e.target.dataset.ui === 'reason') { if (e.target.value) decide(r.id, 'exclude', e.target.value); }
  if (e.target.dataset.ui === 'design') { P.decisions[r.id] = { ...(decision(r.id) ?? {}), design: e.target.value.trim() }; persist(); renderClaims(); }
  if (e.target.dataset.ui === 'n') { P.decisions[r.id] = { ...(decision(r.id) ?? {}), n: +e.target.value || undefined }; persist(); renderClaims(); }
  if (e.target.dataset.ui === 'file-claim') root.querySelector('[data-ui="file-new-wrap"]').hidden = e.target.value !== '__new';
});
$('reader').addEventListener('click', e => {
  const r = rec(view.current); if (!r) return;
  const d = e.target.closest('[data-decide]'); if (d) decide(r.id, d.dataset.decide);
  if (e.target.dataset.ui === 'file-cancel') { view.pending = null; $('file').hidden = true; getSelection().removeAllRanges(); }
  if (e.target.dataset.ui === 'file-save') fileQuote();
  const m = e.target.closest('[data-ex]'); if (m && getSelection().isCollapsed) root.querySelector(`[data-claim-ex="${CSS.escape(m.dataset.ex)}"]`)?.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
});
function fileQuote() {
  const box = $('file'), choice = box.querySelector('[data-ui="file-claim"]').value; let claim = choice;
  if (choice === '__new') { const text = box.querySelector('[data-ui="file-new"]').value.trim(); if (!text) { say('Write the claim first.'); return; }
    claim = 'c' + Date.now().toString(36); P.claims.push({ id: claim, text }); }
  const ex = { ...view.pending, claim, direction: box.querySelector('[data-ui="file-direction"]').value, note: box.querySelector('[data-ui="file-note"]').value.trim() };
  if (P.excerpts.some(x => x.id === ex.id && x.claim === claim)) { say('That passage is already filed under that claim.'); return; }
  if (!syn.verify(P.records, ex)) { say('That passage does not match the abstract; nothing was filed.', true); return; }
  P.excerpts.push(ex); view.pending = null;
  if (!decision(view.current)) P.decisions[view.current] = { status: 'include' };
  getSelection().removeAllRanges(); touch(); say(decision(view.current).status === 'include' ? 'Filed. Quoting a record includes it, unless you change that.' : 'Filed. It counts once the record is included.');
}
function decide(id, status, reason) {
  P.decisions[id] = { ...(decision(id) ?? {}), status, reason: status === 'exclude' ? reason : undefined };
  if (status === 'exclude' && !reason) { say('Choose why it is excluded.'); return; }
  persist(); const rows = listed(), at = rows.findIndex(r => r.id === id);
  renderAll();
  if (view.filter === 'todo') { const next = listed()[Math.max(0, Math.min(at, listed().length - 1))]; if (next) openRecord(next.id); }
}
root.addEventListener('keydown', e => {
  if (e.target.matches('input, select, textarea') || e.metaKey || e.ctrlKey || e.altKey) return;
  const rows = listed(), at = rows.findIndex(r => r.id === view.current), key = e.key.toLowerCase();
  if (key === 'j' || key === 'k') { const next = rows[Math.max(0, Math.min(rows.length - 1, at + (key === 'j' ? 1 : -1)))]; if (next) openRecord(next.id); e.preventDefault(); }
  if (!view.current) return;
  if (key === 'i') { decide(view.current, 'include'); e.preventDefault(); }
  if (key === 'm') { decide(view.current, 'maybe'); e.preventDefault(); }
  if (key === 'x') { root.querySelector('[data-ui="reason"]')?.focus(); e.preventDefault(); }
});

// ------------------------------------------------------------ claims

let dragged = null;
function renderClaims() {
  $('claims').innerHTML = P.claims.map(c => { const st = syn.standing(c, P.excerpts, P.records, P.decisions), mine = P.excerpts.filter(e => e.claim === c.id);
    const tone = !st.studies ? 'none' : st.against > st.supports ? 'against' : st.supports && !st.against && !st.mixed ? 'supports' : 'mixed';
    return `<section class="rs-claim" data-claim="${esc(c.id)}"><div class="rs-claim-head"><input value="${esc(c.text)}" data-claim-text="${esc(c.id)}" aria-label="Claim" maxlength="160"><button type="button" class="rs-quiet" data-claim-remove="${esc(c.id)}" aria-label="Remove claim">✕</button></div>
    <p class="rs-standing rs-${tone}">${esc(st.words)}${st.years ? ` · ${st.years[0] === st.years[1] ? st.years[0] : st.years.join('–')}` : ''}${st.people ? ` · ${st.people.toLocaleString()} studied` : ''}${st.designs.length ? ` · ${esc(st.designs.join(', '))}` : ''}${st.invalid ? ` · ${st.invalid} quotation${st.invalid === 1 ? ' no longer matches the source and is' : 's no longer match their sources and are'} not counted` : ''}</p>
    <ol>${mine.map(e => { const r = rec(e.record), d = decision(e.record)?.status; return `<li draggable="true" data-claim-ex="${esc(e.id)}" data-claim-of="${esc(c.id)}" class="rs-${e.direction}${d !== 'include' ? ' is-out' : ''}">
      <button type="button" data-goto="${esc(e.record)}">“${esc(e.text.length > 220 ? e.text.slice(0, 217) + '…' : e.text)}”</button><span>${esc(r ? syn.cite(r) : 'Source removed')}${d !== 'include' ? ' · not counted while the record is not included' : ''}${syn.verify(P.records, e) ? '' : ' · does not match its source'}</span>
      <select data-ex-direction="${esc(e.id)}" data-ex-claim="${esc(c.id)}" aria-label="Direction"><option value="supports"${e.direction === 'supports' ? ' selected' : ''}>supports</option><option value="against"${e.direction === 'against' ? ' selected' : ''}>against</option><option value="mixed"${e.direction === 'mixed' ? ' selected' : ''}>mixed</option></select>
      <button type="button" class="rs-quiet" data-post="${esc(e.id)}" data-post-claim="${esc(c.id)}">Make a post</button><button type="button" class="rs-quiet" data-unfile="${esc(e.id)}" data-unfile-claim="${esc(c.id)}" aria-label="Remove this quotation">✕</button></li>`; }).join('') || '<li class="rs-hint">Select a passage in an abstract to file evidence here, or drag a quotation from another claim.</li>'}</ol></section>`; }).join('');
}
const claimsBox = $('claims');
claimsBox.addEventListener('click', e => {
  const t = e.target.closest('button'); if (!t) return;
  if (t.dataset.goto) { view.filter = 'all'; openRecord(t.dataset.goto); }
  if (t.dataset.post) {
    try {
      const project = P, x = P.excerpts.find(y => y.id === t.dataset.post && y.claim === t.dataset.postClaim);
      if (!x) throw Error('The quotation is no longer in this review.');
      const draft = campaignFor(project, x.id, x.claim, syn.cite(rec(x.record))); persist(); renderCampaigns();
      showCampaign(project, draft);
    } catch (error) { say(error.message, true); }
  }
  if (t.dataset.unfile) { P.excerpts = P.excerpts.filter(x => !(x.id === t.dataset.unfile && x.claim === t.dataset.unfileClaim)); touch(); }
  if (t.dataset.claimRemove) { const n = P.excerpts.filter(x => x.claim === t.dataset.claimRemove).length;
    if (n && !t.dataset.sure) { t.dataset.sure = '1'; t.textContent = `Remove with ${n} quotation${n > 1 ? 's' : ''}?`; return; }
    P.claims = P.claims.filter(c => c.id !== t.dataset.claimRemove); P.excerpts = P.excerpts.filter(x => x.claim !== t.dataset.claimRemove); touch(); }
});
claimsBox.addEventListener('change', e => {
  if (e.target.dataset.claimText) { const c = P.claims.find(x => x.id === e.target.dataset.claimText); c.text = e.target.value.trim() || c.text; touch(); }
  if (e.target.dataset.exDirection) { const x = P.excerpts.find(y => y.id === e.target.dataset.exDirection && y.claim === e.target.dataset.exClaim); x.direction = e.target.value; touch(); }
});
claimsBox.addEventListener('dragstart', e => { const li = e.target.closest('[data-claim-ex]'); if (!li) return; dragged = { id: li.dataset.claimEx, from: li.dataset.claimOf }; e.dataTransfer.effectAllowed = 'move'; e.dataTransfer.setData('text/plain', li.dataset.claimEx); });
claimsBox.addEventListener('dragover', e => { const c = e.target.closest('[data-claim]'); if (c && dragged) { e.preventDefault(); c.classList.add('is-target'); } });
claimsBox.addEventListener('dragleave', e => e.target.closest('[data-claim]')?.classList.remove('is-target'));
claimsBox.addEventListener('drop', e => {
  const c = e.target.closest('[data-claim]'); if (!c || !dragged) return; e.preventDefault();
  const to = c.dataset.claim, x = P.excerpts.find(y => y.id === dragged.id && y.claim === dragged.from); dragged = null;
  if (!x || to === x.claim) { renderClaims(); return; }
  if (P.excerpts.some(y => y.id === x.id && y.claim === to)) { say('That quotation is already under that claim.'); renderClaims(); return; }
  x.claim = to; touch(); say('Moved the quotation.');
});
$('add-claim').addEventListener('submit', e => { e.preventDefault(); const text = $('claim-text').value.trim(); if (!text) return; P.claims.push({ id: 'c' + Date.now().toString(36), text }); $('claim-text').value = ''; touch(); });

// Saved outputs remain reachable when their originating evidence is unfiled.
let sourceReview = null;
function showCampaign(project, draft) {
  void openComposer({ draft, onChange: () => { if (P === project) { persist(); renderCampaigns(); } }, getSourceStatus: () => campaignSourceStatus(project, draft) });
}
function renderCampaigns() {
  $('campaigns').innerHTML = (P.campaigns ?? []).map((draft, index) => `<article>
    <p>${esc(draft.content.cite)} · ${esc(draft.content.quote.slice(0, 160))}</p>
    <p class="rs-hint">${esc(campaignSourceStatus(P, draft))}</p>
    <div class="rs-pills"><button type="button" data-campaign-open="${index}">Open artboard ${index + 1}</button>
    <button type="button" data-campaign-source="${index}">Read source</button>
    <button type="button" data-campaign-review="${index}">Review source update</button></div></article>`).join('')
    || '<p class="rs-hint">Make a post from a filed quotation to keep its artboards here.</p>';
  if (sourceReview && sourceReview.project !== P) sourceReview.close();
}
$('campaigns').addEventListener('click', e => {
  const button = e.target.closest('button'); if (!button) return;
  const key = ['campaignOpen', 'campaignSource', 'campaignReview'].find(k => button.dataset[k] !== undefined);
  const draft = P.campaigns?.[Number(button.dataset[key])]; if (!draft) return;
  if (key === 'campaignOpen') showCampaign(P, draft);
  if (key === 'campaignSource') {
    const ref = draft.sourceRef ?? draft.origin;
    if (!rec(ref.record)) { say('The source record was removed. The artboard retains its saved source words.', true); return; }
    view.filter = 'all'; openRecord(ref.record); $('reader').scrollIntoView({ block: 'nearest' });
  }
  if (key === 'campaignReview') reviewCampaign(P, draft);
});
function reviewCampaign(project, draft) {
  sourceReview?.close();
  const focus = document.activeElement, dialog = document.createElement('dialog'); dialog.className = 'instrument-discard';
  dialog.setAttribute('aria-label', 'Review artboard source');
  const options = project.excerpts.filter(x => project.decisions[x.record]?.status === 'include' && syn.verify(project.records, x));
  dialog.innerHTML = `<h2>Review artboard source</h2><p>Layout and claim-line edits stay as they are. Choose the source and words to retain in this post.</p>
    <p>Saved source</p><blockquote>${esc(draft.source)}</blockquote>
    <label>Replacement quotation<select data-source-choice style="max-width:100%">${options.map((x, index) => `<option value="${index}">${esc(syn.cite(project.records.find(r => r.id === x.record)) + ' · ' + x.text.slice(0, 120))}</option>`).join('')}</select></label>
    <label>Post words<select data-source-wording><option value="keep">Keep my post words</option><option value="source">Use the source words</option></select></label>
    <p>Reviewed source</p><blockquote data-source-preview></blockquote><p data-source-citation></p>
    <p>Post words after the update</p><blockquote data-wording-preview></blockquote>
    <p data-source-message role="status"></p><button type="button" data-source-apply>Apply source update</button><button type="button" data-source-cancel>Cancel</button>`;
  const q = s => dialog.querySelector(s), choice = q('[data-source-choice]'), wording = q('[data-source-wording]');
  const ref = draft.sourceRef ?? draft.origin, selected = options.findIndex(x => x.id === ref.excerpt && x.claim === ref.claim);
  if (selected >= 0) choice.value = String(selected);
  let proposal;
  function prepare() {
    try {
      const target = options[Number(choice.value)]; if (!target) throw Error('File a verified quotation from an included source before updating this artboard.');
      proposal = prepareCampaignSource(project, draft, target.id, target.claim, syn.cite(project.records.find(r => r.id === target.record)), wording.value);
      q('[data-source-preview]').textContent = proposal.target.text;
      q('[data-source-citation]').textContent = proposal.target.citation;
      q('[data-wording-preview]').textContent = proposal.wording === 'source' ? proposal.target.text : draft.content.quote;
      q('[data-source-message]').textContent = ''; q('[data-source-apply]').disabled = false;
    } catch (error) { proposal = null; q('[data-source-message]').textContent = error.message; q('[data-source-apply]').disabled = true; }
  }
  const close = () => { dialog.close(); dialog.remove(); sourceReview = null; if (focus?.isConnected) focus.focus(); };
  sourceReview = { project, close }; root.append(dialog);
  choice.onchange = wording.onchange = prepare;
  q('[data-source-cancel]').onclick = close;
  dialog.addEventListener('cancel', e => { e.preventDefault(); close(); });
  q('[data-source-apply]').onclick = () => {
    try {
      if (P !== project || !proposal) throw Error('The project changed. Open source review again.');
      const current = project.records.find(r => r.id === proposal.target.ref.record);
      applyCampaignSource(project, draft, proposal, current ? syn.cite(current) : '');
      close(); touch(); refreshComposer({ syncInputs: true });
      $('campaigns').dispatchEvent(new Event('change', { bubbles: true }));
      say('Source updated. Save the review to retain this version.');
    } catch (error) { q('[data-source-message]').textContent = error.message; q('[data-source-apply]').disabled = true; }
  };
  prepare(); dialog.showModal(); choice.focus();
}

// ------------------------------------------------------------ the record of the review

function renderFlow() {
  const f = syn.flow(P), reasons = Object.entries(f.excluded).sort((a, b) => b[1] - a[1]);
  const box = (x, y, w, title, n, cls = '') => `<g class="rs-flow-box ${cls}"><rect x="${x}" y="${y}" width="${w}" height="56" rx="28"/><text x="${x + 24}" y="${y + 24}" class="rs-flow-n">${n.toLocaleString()}</text><text x="${x + 24}" y="${y + 44}">${esc(title)}</text></g>`;
  const h = 300 + reasons.length * 22;
  $('flow').setAttribute('viewBox', `0 0 640 ${h}`);
  $('flow').innerHTML = box(0, 0, 300, 'found in OpenAlex and Europe PMC', f.identified) + `<path d="M150 56V84" class="rs-flow-line"/>`
    + box(0, 84, 300, 'after merging the same work', f.screened) + `<path d="M300 112H340" class="rs-flow-line"/>` + box(340, 84, 300, 'duplicates merged', f.duplicates, 'is-side')
    + `<path d="M150 140V168" class="rs-flow-line"/>` + box(0, 168, 300, `screened (${f.unscreened.toLocaleString()} still to go)`, f.screened - f.unscreened) + `<path d="M300 196H340" class="rs-flow-line"/>`
    + box(340, 168, 300, `excluded${f.maybe ? `, ${f.maybe} maybe` : ''}`, f.excludedTotal, 'is-side')
    + reasons.map(([r, n], i) => `<text x="364" y="${248 + i * 22}" class="rs-flow-reason">${n} · ${esc(r)}</text>`).join('')
    + `<path d="M150 224V${236}" class="rs-flow-line"/>` + box(0, 236, 300, 'included', f.included, 'is-lime');
}
function renderWriteup() {
  const md = syn.report(P), html = md.split('\n').map(l => l.startsWith('# ') ? `<h3>${esc(l.slice(2))}</h3>` : l.startsWith('## ') ? `<h4>${esc(l.slice(3))}</h4>` : l.startsWith('> ') ? `<blockquote>${esc(l.slice(2))}</blockquote>` : l.startsWith('- ') ? `<p class="rs-ref">${esc(l.slice(2))}</p>` : l ? `<p>${esc(l)}</p>` : '').join('');
  $('writeup').innerHTML = html;
}
const download = (name, type, text) => { const a = document.createElement('a'); a.href = URL.createObjectURL(new Blob([text], { type })); a.download = name; a.click(); setTimeout(() => URL.revokeObjectURL(a.href), 1000); };
const slug = () => P.question.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '').slice(0, 50) || 'review';
const included = () => P.records.filter(r => decision(r.id)?.status === 'include');
$('dl-md').addEventListener('click', () => download(slug() + '.md', 'text/markdown', syn.report(P)));
$('dl-csv').addEventListener('click', () => download(slug() + '-evidence.csv', 'text/csv', syn.evidenceCsv(P)));
$('dl-bib').addEventListener('click', () => download(slug() + '.bib', 'application/x-bibtex', syn.bibtex(included())));
$('dl-ris').addEventListener('click', () => download(slug() + '.ris', 'application/x-research-info-systems', syn.ris(included())));
$('dl-project').addEventListener('click', () => download(slug() + '-project.json', 'application/json', JSON.stringify(P)));
$('open-project').addEventListener('change', async e => { const file = e.target.files[0]; if (!file) return;
  try { const project = P, epoch = editEpoch, attempt = ++importAttempt, body = await file.text();
    if (attempt !== importAttempt) return;
    if (P !== project || epoch !== editEpoch) { say('The review changed while the file was opening. Open the file again when ready.'); return; }
    const p = prepareProject(JSON.parse(body));
    const bad = p.excerpts.filter(x => !syn.verify(p.records, x)).length; cancelSearch(); resetComposer(); P = p; view.current = null; touch(); say(`Opened ${file.name}.${bad ? ` ${bad} quotation${bad > 1 ? 's do' : ' does'} not match the abstracts in it and ${bad > 1 ? 'are' : 'is'} marked.` : ''}`); }
  catch (error) { say(`${file.name} could not be opened: ${error.message}.`, true); } });
$('question').addEventListener('change', () => { P.question = $('question').value.trim() || P.question; touch(); });
$('search').addEventListener('submit', search);
$('restart').addEventListener('click', async () => { if (!$('restart').dataset.sure) { $('restart').dataset.sure = '1'; $('restart').textContent = 'This replaces your work here. Again to confirm'; return; } const project = await sample(); cancelSearch(); resetComposer(); P = project; view.current = null; touch(); $('restart').textContent = 'Back to the example'; delete $('restart').dataset.sure; });
$('empty').addEventListener('click', () => { cancelSearch(); resetComposer(); P = { question: 'Your question', records: [], decisions: {}, claims: [], excerpts: [], identified: 0, duplicates: [], retrieved: [] }; view.current = null; touch(); $('question').focus(); say('An empty desk. Write your question, then search.'); });

function renderAll() { $('question').value = P.question; renderList(); renderReader(); renderClaims(); renderCampaigns(); renderFlow(); renderWriteup(); refreshComposer(); }
async function sample() { return await (await fetch(new URL('sample-project.json', here))).json(); }

try {
  P = prepareProject(store.get() ?? await sample());
  if (P.search) { $('query').value = P.search.query ?? ''; $('from').value = P.search.from ?? ''; $('to').value = P.search.to ?? ''; }
  view.current = P.records.find(r => !decision(r.id))?.id ?? P.records[0]?.id ?? null;
  renderAll(); persist(); $('loading').hidden = true; $('ready').hidden = false;
  await attachInstrumentStore({ prefix: 'review-project:', label: 'review', mount: $('ready'), get: () => P, set: project => { const prepared = prepareProject(project); cancelSearch(); resetComposer(); P = prepared; view.current = P.records[0]?.id ?? null; touch(); } });
} catch (error) { $('loading').textContent = `The research desk could not start: ${error.message}`; }
