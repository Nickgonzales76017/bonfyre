// The opportunity desk. Four views over one set of requirements read from
// buyers' documents: the requirement grid (with each sentence shown in its
// source), the weighted comparison, the proof of concept, and discovery with a
// RICE ranking and roadmap. Work is kept in this browser, or in the Aurekai
// destination the page is installed in.
import * as O from './opportunity-desk.mjs';
import {openSourceReview} from './opportunity-source-review.mjs';
import * as P from './plan-network.mjs';
import { attachInstrumentStore, openInstrumentScratch } from './instrument-store.mjs';

const root = document.querySelector('[data-opportunity]');
const $ = n => root.querySelector(`[data-ui="${n}"]`);
const here = new URL('.', import.meta.url), KEY = 'aurekai-opportunity-desk-v1';
const esc = t => String(t ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
const say = (text, bad) => { const m = $('message'); m.textContent = text; m.dataset.tone = bad ? 'bad' : ''; };
const store = await openInstrumentScratch(KEY, null);
let SRC = null, baseSRC = null, W = null, namedStore = null, view = 'grid', filter = { source: '', theme: '', priority: '', coverage: '' }, focus = null;
let docEpoch=0, importing=false;const selected=new Set(),bulkUndo=[];
const persist = () => store.set(W);
const COVER = [['', 'Not assessed'], ['covered', 'Covered'], ['partly', 'Partly'], ['planned', 'Planned'], ['no', 'Not offered']];

function fresh() {
  const reqs = Object.entries(SRC.texts).flatMap(([id, text]) => O.requirements(text, id));
  const themes = [...new Set(reqs.map(r => r.theme))];
  return { reqs, options: [{ name: 'Our offer', scores: Object.fromEntries(themes.map(t => [t, null])) }, { name: 'Incumbent agency', scores: Object.fromEntries(themes.map(t => [t, null])) }], weightOverrides: {},
    poc: [], ops: Object.fromEntries(O.opportunities(reqs).map(o => [o.theme, { impact: null, confidence: null, effort: null, lane: '' }])) };
}

// ------------------------------------------------------------ requirements

function grid() {
  const rows = W.reqs.filter(r => (!filter.source || r.source === filter.source) && (!filter.theme || r.theme === filter.theme) && (!filter.priority || r.priority === filter.priority) && (filter.coverage === '' || filter.coverage === 'none' ? (filter.coverage === '' || !r.coverage) : r.coverage === filter.coverage));
  const c = O.coverage(W.reqs), sel = (k, opts) => `<select data-filter="${k}"><option value="">${{ source: 'Every buyer', theme: 'Every theme', priority: 'Any priority', coverage: 'Any coverage' }[k]}</option>${opts.map(([v, l]) => `<option value="${esc(v)}"${filter[k] === v ? ' selected' : ''}>${esc(l)}</option>`).join('')}</select>`;
  $('view').innerHTML = `<p class="od-sum">${c.assessed} of ${c.of} requirements assessed; coverage of assessed requirements ${Math.round(c.score * 100)}%. Unassessed requirements are not scored.${c.gaps.length ? ` <strong>${c.gaps.length} required item${c.gaps.length > 1 ? 's are' : ' is'} planned or not offered.</strong>` : ''}${c.unassessedMust ? ` ${c.unassessedMust} required items not yet assessed.` : ''}</p>
  <div class="od-filters">${sel('source', Object.entries(SRC.sources).map(([id, s]) => [id, s.title]))}${sel('theme', [...new Set(W.reqs.map(r => r.theme))].sort().map(t => [t, t]))}${sel('priority', [['must', 'Must'], ['should', 'Should'], ['could', 'Could']])}${sel('coverage', [['none', 'Not assessed'], ...COVER.slice(1)])}</div>
  <div class="od-bulk"><label><input type="checkbox" data-select-visible ${rows.length&&rows.every(r=>selected.has(r.id))?'checked':''}> Select visible requirements</label><span data-selection-count>${selected.size} selected</span><label>Coverage<select data-bulk-coverage><option value="covered">Covered</option><option value="partly">Partly covered</option><option value="planned">Planned, not yet available</option><option value="no">Not offered</option><option value="">Not assessed</option></select></label><button type="button" data-review-selected ${selected.size?'':'disabled'}>Review selected changes</button><button type="button" data-undo-bulk ${bulkUndo.length?'':'disabled'}>Undo last bulk review</button><button type="button" data-prepare-plan ${selected.size?'':'disabled'}>Prepare delivery plan</button></div>
  <table class="od-grid"><thead><tr><th>Select</th><th>Requirement, in the buyer's words</th><th>Priority</th><th>Theme</th><th>Our coverage</th><th>Note</th></tr></thead><tbody>${rows.map(r => `<tr class="${focus === r.id ? 'is-focus' : ''}">
    <td><input type="checkbox" data-select-req="${esc(r.id)}" ${selected.has(r.id)?'checked':''} aria-label="Select requirement ${esc(r.text.slice(0,100))}"></td><td><button type="button" class="od-req" data-focus="${esc(r.id)}">${esc(r.text)}</button><small>${esc(SRC.sources[r.source].title)}</small></td>
    <td><select data-set="priority" data-id="${esc(r.id)}">${['must', 'should', 'could'].map(p => `<option${r.priority === p ? ' selected' : ''}>${p}</option>`).join('')}</select></td>
    <td><select data-set="theme" data-id="${esc(r.id)}">${[...Object.keys(O.THEMES), 'Other'].map(t => `<option${r.theme === t ? ' selected' : ''}>${esc(t)}</option>`).join('')}</select></td>
    <td><select data-set="coverage" data-id="${esc(r.id)}" class="od-cov od-cov-${r.coverage || 'none'}">${COVER.map(([v, l]) => `<option value="${v}"${r.coverage === v ? ' selected' : ''}>${l}</option>`).join('')}</select></td>
    <td><input data-set="note" data-id="${esc(r.id)}" value="${esc(r.note)}" maxlength="300" placeholder="How, or why not"></td></tr>`).join('')}</tbody></table>
  <p class="od-hint">${rows.length} shown. Select a requirement to read it where the buyer wrote it.</p>`;
  context();
}
function context() {
  const r = W.reqs.find(x => x.id === focus), box = $('context');
  if (!r) { box.innerHTML = '<p class="od-hint">Select a requirement to read it in its document.</p>'; return; }
  const text = SRC.texts[r.source], a = Math.max(0, r.start - 420), b = Math.min(text.length, r.end + 420), ok = O.verify(SRC.texts, r);
  box.innerHTML = `<h3>${esc(SRC.sources[r.source].title)}</h3><p class="od-hint"><a href="${esc(SRC.sources[r.source].url)}" target="_blank" rel="noopener">Open the buyer's page ↗</a> · read ${esc(new Date(SRC.sources[r.source].observed_at).toLocaleDateString())}${ok ? '' : ' · <strong>this text no longer matches the retained document</strong>'}</p>
  <p class="od-passage">${a ? '… ' : ''}${esc(text.slice(a, r.start))}<mark>${esc(text.slice(r.start, r.end))}</mark>${esc(text.slice(r.end, b))}${b < text.length ? ' …' : ''}</p>
  <div class="od-pills"><button type="button" data-ui="to-poc" data-id="${esc(r.id)}">${W.poc.some(c => c.id === r.id) ? 'In the proof of concept' : 'Add to the proof of concept'}</button></div>`;
}

// ------------------------------------------------------------ comparison

function compareView() {
  const weights = O.themeWeights(W.reqs, W.weightOverrides), cmp = O.compare(W.options, weights), themes = Object.keys(weights), summary = !cmp.complete ? "Enter every active score before choosing a leader. Blank cells are unknown, not zero." : cmp.rows.length > 1 && cmp.margin === 0 ? "The options are tied on the entered scores." : cmp.rows.length > 1 ? `${esc(cmp.rows[0].name)} leads at ${cmp.rows[0].total.toFixed(2)} of 5, ${cmp.margin.toFixed(2)} ahead.` : "One scored option; add another to compare.";
  $("view").innerHTML = `<p class="od-sum">${summary}</p><p class="od-hint">The comparison uses a 0\u20135 scale and the displayed priority weights. Range bars show possible totals while scores are missing; they are not confidence intervals.</p><table class="od-grid od-matrix"><thead><tr><th>Theme</th><th>Weight</th>${W.options.map((o, i) => `<th><input data-option="${i}" value="${esc(o.name)}" maxlength="40" aria-label="Option ${i + 1} name"></th>`).join("")}</tr></thead><tbody>${themes.map((t) => `<tr><td>${esc(t)}</td><td><input type="number" min="0" max="1000000" step="any" data-weight="${esc(t)}" value="${weights[t]}" aria-label="Weight for ${esc(t)}"></td>${W.options.map((o, i) => `<td><input type="number" min="0" max="5" step="any" data-review-score="${i}" data-theme="${esc(t)}" value="${o.scores[t] ?? ""}" placeholder="Unknown" aria-label="${esc(o.name)} on ${esc(t)}"></td>`).join("")}</tr>`).join("")}<tr class="od-total"><td>Total</td><td></td>${W.options.map((o, i) => {
    const r = cmp.rows.find((r) => r.index === i);
    return `<td>${r.total === null ? `${r.lower === null ? "No active weights" : r.lower.toFixed(2) + "\u2013" + r.upper.toFixed(2)}<small>${r.missing} unscored</small>` : r.total.toFixed(2)}<span class="od-score-range" role="img" aria-label="${esc(o.name)} weighted score range" style="--lo:${(r.lower ?? 0) / 5 * 100}%;--hi:${(r.upper ?? 0) / 5 * 100}%"></span></td>`;
  }).join("")}</tr></tbody></table>${cmp.complete && cmp.flips.length ? `<details><summary>Inspect weight sensitivity</summary><p>Holding other weights fixed, a tie with the runner-up occurs when:</p><ul>${cmp.flips.map((f) => `<li>${esc(f.theme)} changes from ${f.from} to ${f.to.toFixed(2)}.</li>`).join("")}</ul></details>` : ""}<div class="od-pills"><button type="button" data-ui="add-option" ${W.options.length >= 20 ? "disabled" : ""}>Add an option</button>${Object.keys(W.weightOverrides).length ? '<button type="button" data-ui="reset-weights">Restore requirement weights</button>' : ""}</div>`;
}

// ------------------------------------------------------------ proof of concept

function pocView() {
  const out = O.pocOutcome(W.poc);
  $('view').innerHTML = `<p class="od-sum"><strong>${esc(out.words)}.</strong> ${W.poc.length ? '' : 'Add requirements from the grid: select one and choose "Add to the proof of concept".'}</p>
  <ol class="od-poc">${W.poc.map((c, i) => `<li class="od-${c.result || 'open'}"><p>${esc(c.text)}</p><small>${esc(c.priority)} · ${esc(SRC.sources[c.source]?.title)}</small>
  <label>How it will be shown<input data-poc="${i}" data-k="test" value="${esc(c.test)}" maxlength="200" placeholder="The check that would show it working"></label>
  <div class="od-pills">${[['pass', 'Passed'], ['fail', 'Failed'], ['', 'Not tried']].map(([v, l]) => `<button type="button" data-poc-result="${i}" data-v="${v}" aria-pressed="${(c.result || '') === v}">${l}</button>`).join('')}<button type="button" class="od-quiet" data-poc-drop="${i}">Remove</button></div>
  <label>What happened<input data-poc="${i}" data-k="observed" value="${esc(c.observed)}" maxlength="300"></label></li>`).join('')}</ol>`;
}

// ------------------------------------------------------------ discovery

function discoveryView() {
  const ops = O.opportunities(W.reqs), items = ops.map((o) => ({ ...o, ...W.ops[o.theme] ??= { impact: null, confidence: null, effort: null, lane: "" } })), ranked = O.ranking(items), complete = ranked.every((o) => o.place !== null);
  $("view").innerHTML = `<p class="od-sum">${ops.length} needs across ${Object.keys(SRC.sources).length} retained buyer documents. ${complete ? "Ranks use your entered estimates." : "Enter impact, confidence and effort for every need before ranking."}</p><p class="od-hint">Reach here counts distinct retained buyer documents, not market size. Sensitivity changes one confidence estimate at a time by 20%; it is not a forecast.</p><table class="od-grid"><thead><tr><th>Rank</th><th>Need</th><th>Buyer documents</th><th>Impact (0\u20133)</th><th>Confidence (0\u20131)</th><th>Effort (weeks)</th><th>Score</th><th>Plan</th></tr></thead><tbody>${ranked.map((o) => `<tr><td>${o.place ?? "\u2014"}${o.place !== null && !o.stable ? `<small>Range ${o.best}\u2013${o.worst}</small>` : ""}</td><td><button type="button" class="od-req" data-need="${esc(o.theme)}">${esc(o.theme)}</button><small>${o.mentions} mentions \xB7 ${o.must} required</small></td><td>${o.reach}</td>${[["impact", 0, 3], ["confidence", 0, 1], ["effort", 0.01, 1e9]].map(([k, min, max]) => `<td><input type="number" min="${min}" max="${max}" step="any" data-op="${esc(o.theme)}" data-k="${k}" value="${o[k] ?? ""}" placeholder="Unknown" aria-label="${k} for ${esc(o.theme)}"></td>`).join("")}<td>${o.score === null ? "Needs estimates" : o.score.toFixed(2)}</td><td><select data-op-lane="${esc(o.theme)}" aria-label="Roadmap placement for ${esc(o.theme)}">${[["", "Unplanned"], ["now", "Now"], ["next", "Next"], ["later", "Later"]].map(([v, label]) => `<option value="${v}" ${W.ops[o.theme].lane === v ? "selected" : ""}>${label}</option>`).join("")}</select></td></tr>`).join("")}</tbody></table><div class="od-lanes">${["now", "next", "later"].map((l) => `<section data-lane="${l}"><h3>${l === "now" ? "Now" : l === "next" ? "Next" : "Later"}</h3>${ranked.filter((o) => W.ops[o.theme].lane === l).map((o) => `<p draggable="true" data-drag="${esc(o.theme)}">${esc(o.theme)}<small>${o.score === null ? "Not estimated" : o.score.toFixed(2)}</small></p>`).join("") || '<p class="od-hint">Use the placement field or drag a planned need here.</p>'}</section>`).join("")}</div><div data-ui="need-passages"></div>`;
}

let dragged = null;
root.addEventListener('dragstart', e => { const d = e.target.closest('[data-drag]'); if (d) { dragged = d.dataset.drag; e.dataTransfer.setData('text/plain', dragged); } });
root.addEventListener('dragover', e => { if (dragged && e.target.closest('[data-lane]')) e.preventDefault(); });
root.addEventListener('drop', e => { const lane = e.target.closest('[data-lane]'); if (!lane || !dragged) return; e.preventDefault(); W.ops[dragged].lane = lane.dataset.lane; dragged = null; persist(); discoveryView(); });

// ------------------------------------------------------------ events

root.addEventListener('change', e => {
  const t = e.target, d = t.dataset;
  if(d.selectReq){if(t.checked)selected.add(d.selectReq);else selected.delete(d.selectReq);grid();return;}
  if(d.selectVisible!==undefined){for(const r of visibleRequirements())if(t.checked)selected.add(r.id);else selected.delete(r.id);grid();return;}
  if(d.reviewScore!==undefined){if(!t.validity.valid){say('Scores must be between 0 and 5.',true);compareView();return;}W.options[Number(d.reviewScore)].scores[d.theme]=t.value===''?null:t.valueAsNumber;persist();compareView();return;}
  if(d.opLane){W.ops[d.opLane].lane=t.value;persist();discoveryView();return;}
  if (d.filter) { filter[d.filter] = t.value; grid(); return; }
  if (d.set) { const r = W.reqs.find(x => x.id === d.id); r[d.set] = t.value; persist(); grid(); return; }
  if(d.weight){if(!t.validity.valid||t.value===''){say('Enter a non-negative weight.',true);compareView();return;}W.weightOverrides[d.weight]=t.valueAsNumber;persist();compareView();return;}
  if (d.option != null) { W.options[+d.option].name = t.value.trim() || W.options[+d.option].name; persist(); compareView(); return; }
  if (d.poc != null) { W.poc[+d.poc][d.k] = t.value; persist(); return; }
  if(d.op){if(!t.validity.valid){say('Review the estimate range.',true);discoveryView();return;}W.ops[d.op][d.k]=t.value===''?null:t.valueAsNumber;persist();discoveryView();}
});
root.addEventListener('input', e => { const d = e.target.dataset; if (d.score != null) { W.options[+d.score].scores[d.theme] = +e.target.value; e.target.nextElementSibling.textContent = e.target.value; persist(); clearTimeout(root._t); root._t = setTimeout(compareView, 150); } });
root.addEventListener('click', e => {
  const t = e.target.closest('button'); if (!t) return; const d = t.dataset;
  if(d.reviewSelected!==undefined){reviewSelected();return;}
  if(d.undoBulk!==undefined){try{const r=bulkUndo.at(-1);if(!r)return;W.reqs=O.applyRequirementChanges(W.reqs,r,SRC.texts);bulkUndo.pop();persist();grid();say('Undid the last bulk review. Save to retain it.');}catch(error){say(error.message,true);}return;}
  if(d.preparePlan!==undefined){void preparePlan();return;}
  if (d.tab) { view = d.tab; render(); return; }
  if (d.focus) { focus = d.focus; grid(); return; }
  if (d.ui === 'to-poc') { const r = W.reqs.find(x => x.id === d.id); if (!W.poc.some(c => c.id === r.id)) { W.poc.push({ id: r.id, text: r.text, source: r.source, priority: r.priority, test: '', result: '', observed: '' }); persist(); say('Added to the proof of concept.'); } context(); return; }
  if (d.ui === 'add-option') { if(W.options.length>=20)return;W.options.push({ name: `Option ${W.options.length + 1}`, scores: {} }); persist(); compareView(); return; }
  if (d.ui === 'reset-weights') { W.weightOverrides = {}; persist(); compareView(); return; }
  if(d.pocResult!=null){const c=W.poc[+d.pocResult];if(d.v&&(!c.test.trim()||!c.observed.trim())){say('Describe the check and what happened before recording a result.',true);return;}c.result=d.v;persist();pocView();return;}
  if (d.pocDrop != null) { W.poc.splice(+d.pocDrop, 1); persist(); pocView(); return; }
  if (d.need) { const o = O.opportunities(W.reqs).find(x => x.theme === d.need); $('need-passages').innerHTML = `<h3>${esc(o.theme)}: in the buyers' words</h3>${o.passages.map(p => `<blockquote>${esc(p.text)}<small>${esc(SRC.sources[p.source].title)} · ${p.priority}</small></blockquote>`).join('')}`; }
});
const download = (name, type, text) => { const a = document.createElement('a'); a.href = URL.createObjectURL(new Blob([text], { type })); a.download = name; a.click(); setTimeout(() => URL.revokeObjectURL(a.href), 1000); };
$('dl-compliance').addEventListener('click', () => download('requirements-and-coverage.csv', 'text/csv', O.complianceCsv(W.reqs, SRC.sources)));
$('dl-outline').addEventListener('click', () => download('response-outline.md', 'text/markdown', O.proposalOutline(W.reqs, SRC.sources, O.compare(W.options, O.themeWeights(W.reqs, W.weightOverrides)), O.pocOutcome(W.poc))));
$('dl-roadmap').addEventListener('click',()=>{const ranked=O.ranking(O.opportunities(W.reqs).map(o=>({...o,...W.ops[o.theme]})));if(ranked.some(o=>o.place===null)){say('Complete the estimates before exporting a ranked roadmap.',true);return;}download('roadmap.csv','text/csv',O.roadmapCsv(ranked));});
$('add-doc').addEventListener('change',importDocument);
$('reset').addEventListener('click', () => { docEpoch++;SRC=structuredClone(baseSRC);W=fresh();selected.clear();bulkUndo.length=0;persist();render();say('Back to the four public requests for proposals.'); });
function render() { $('view').setAttribute('aria-labelledby','od-tab-'+view); for (const b of root.querySelectorAll('[data-tab]')) b.setAttribute('aria-pressed', String(b.dataset.tab === view)); $('context').hidden=view!=='grid';root.querySelector('.od-separator').hidden=view!=='grid';root.querySelector('.od-work').dataset.inspector=String(view==='grid');for(const b of root.querySelectorAll('[data-tab]')){b.setAttribute('aria-selected',String(b.dataset.tab===view));b.tabIndex=b.dataset.tab===view?0:-1;} ({ grid, compare: compareView, poc: pocView, discovery: discoveryView })[view](); }

try {
  SRC = await (await fetch(new URL('sample-rfps.json', here))).json();baseSRC=structuredClone(SRC);
  W = store.get() ?? fresh();
  for (const [id, o] of Object.entries(W.own ?? {})) { SRC.texts[id] = o.text; SRC.sources[id] = { title: o.title, url: '', observed_at: '' }; }
  render(); $('loading').hidden = true; $('ready').hidden = false;
  namedStore = await attachInstrumentStore({ prefix: 'opportunity:', label: 'opportunity', mount: $('ready'), get: () => W, set: w => { docEpoch++;SRC=structuredClone(baseSRC);selected.clear();bulkUndo.length=0;W=structuredClone(w); for (const [id, o] of Object.entries(W.own ?? {})) { SRC.texts[id] = o.text; SRC.sources[id] = { title: o.title, url: '', observed_at: '' }; } persist(); render(); } });
} catch (e) { $('loading').textContent = `The opportunity desk could not start: ${e.message}`; }

function visibleRequirements() {
  return W.reqs.filter((r) => (!filter.source || r.source === filter.source) && (!filter.theme || r.theme === filter.theme) && (!filter.priority || r.priority === filter.priority) && (filter.coverage === "" || filter.coverage === "none" ? !filter.coverage || !r.coverage : r.coverage === filter.coverage));
}
function reviewDialog(title, markup) {
  const dialog = document.createElement("dialog"), previous = document.activeElement;
  dialog.className = "od-review";
  dialog.setAttribute("aria-label", title);
  dialog.innerHTML = "<h2>" + esc(title) + "</h2>" + markup;
  root.append(dialog);
  dialog.addEventListener("close", () => previous?.isConnected && previous.focus());
  dialog.showModal();
  return dialog;
}
function reviewSelected() {
  try {
    const review = O.previewRequirementChanges(W.reqs, [...selected], { coverage: root.querySelector("[data-bulk-coverage]").value }, SRC.texts), dialog = reviewDialog("Review selected coverage", `<p>${review.length} exact requirements will change in this draft. Unselected requirements and source text stay unchanged.</p><div class="od-dialog-scroll"><table class="od-grid"><thead><tr><th>Requirement</th><th>Before</th><th>After</th></tr></thead><tbody>${review.map((r) => `<tr><td>${esc(r.text)}</td><td>${esc(r.before.coverage || "Not assessed")}</td><td>${esc(r.after.coverage || "Not assessed")}</td></tr>`).join("")}</tbody></table></div><p role="status" data-review-status></p><div class="od-pills"><button type="button" data-apply-reviewed>Apply reviewed changes</button><button type="button" data-close-review>Cancel</button></div>`);
    dialog.querySelector("[data-close-review]").onclick = () => {
      dialog.close();
      dialog.remove();
    };
    dialog.querySelector("[data-apply-reviewed]").onclick = () => {
      try {
        W.reqs = O.applyRequirementChanges(W.reqs, review, SRC.texts);
        bulkUndo.push(review.map((r) => ({ ...r, before: r.after, after: r.before })));
        if (bulkUndo.length > 10)
          bulkUndo.shift();
        persist();
        root.dispatchEvent(new Event("change", { bubbles: !0 }));
        grid();
        dialog.close();
        dialog.remove();
        say("Reviewed changes applied to this draft. Save to retain them.");
      } catch (e) {
        dialog.querySelector("[data-review-status]").textContent = e.message;
      }
    };
  } catch (e) {
    say(e.message, !0);
  }
}
async function importDocument(e) {
  const input = e.target, file = input.files[0];
  if (!file || importing)
    return;
  const epoch = docEpoch;
  importing = !0;
  input.disabled = !0;
  try {
    if (file.size > 2097152)
      throw Error("Import at most 2 MiB per document.");
    const raw = await file.text(), text = /\.html?$/i.test(file.name) ? O.plainText(raw) : raw, imported = await O.importRequirementDocument({ name: file.name, text }, SRC);
    if (epoch !== docEpoch)
      throw Error("The open review changed during import. Import into the intended review again.");
    if (imported.duplicate) {
      say("This exact document is already retained. No duplicate was added.");
      return;
    }
    if (W.reqs.length + imported.requirements.length > 2000)
      throw Error("Keep at most 2,000 requirements in one review.");
    SRC.texts[imported.id] = text;
    SRC.sources[imported.id] = { title: file.name, url: "", observed_at: new Date().toISOString(), sha256: imported.sha256 };
    W.own = { ...W.own ?? {}, [imported.id]: { text, title: file.name, sha256: imported.sha256 } };
    W.reqs.push(...imported.requirements);
    persist();
    render();
    say(imported.requirements.length ? "Retained " + imported.requirements.length + " exact requirements. Same-named revisions stay separate." : "Retained the document without inventing requirements. Select passages with Read source documents.");
  } catch (error) {
    say(error.message, !0);
  } finally {
    importing = !1;
    input.disabled = !1;
    input.value = "";
  }
}
async function preparePlan() {
  try {
    if (root.querySelector("[data-delivery-plan]")) {
      root.querySelector("[data-delivery-plan]").showModal();
      return;
    }
    if (!namedStore)
      throw Error("The saved-work owner is not ready.");
    const checkpoint = await namedStore.checkpoint(), ids = [...selected];
    if (!ids.length || ids.length > 100)
      throw Error("Select 1\u2013100 requirements for a delivery plan.");
    const sourceTexts = structuredClone(SRC.texts), reqs = structuredClone(W.reqs), chosen = ids.map((id) => reqs.find((r) => r.id === id));
    if (chosen.some((r) => !r || !O.verify(sourceTexts, r)))
      throw Error("Review the source of every selected requirement.");
    const dialog = reviewDialog("Prepare delivery plan", `<p>Source: ${esc(checkpoint.name)}, saved version ${checkpoint.revision}. Enter working-day estimates; no duration or person is inferred. This creates a proposed plan, not a booking.</p><div class="od-plan-fields"><label>Plan name<input data-plan-name maxlength="80" value="${esc((checkpoint.name + " delivery").slice(0, 80))}"></label><label>Start date<input type="date" data-plan-start></label></div><div class="od-dialog-scroll"><table class="od-grid"><thead><tr><th>Exact requirement</th><th>Best</th><th>Likely</th><th>Worst</th><th>Owner</th><th>Depends on</th></tr></thead><tbody>${chosen.map((r, i) => `<tr data-plan-task="${i}"><td>${i + 1}. ${esc(r.text)}</td>${["optimistic", "likely", "pessimistic"].map((k) => `<td><input data-duration="${k}" type="number" min="0" max="10000" step="any" aria-label="${k} working days for task ${i + 1}"></td>`).join("")}<td><input data-plan-owner maxlength="80" aria-label="Owner for task ${i + 1}"></td><td><select multiple data-plan-deps aria-label="Dependencies for task ${i + 1}">${chosen.map((other, j) => j === i ? "" : `<option value="${esc(other.id)}">${j + 1}. ${esc(other.text.slice(0, 70))}</option>`).join("")}</select></td></tr>`).join("")}</tbody></table></div><section data-plan-summary hidden></section><p role="status" data-plan-status></p><div class="od-pills"><button type="button" data-plan-review>Review schedule</button><button type="button" data-plan-commit disabled>Create reviewed plan</button><button type="button" data-plan-check hidden>Check saved plan</button><button type="button" data-plan-retry hidden>Retry reviewed plan</button><button type="button" data-plan-close>Close</button></div>`);
    dialog.dataset.deliveryPlan = "";
    const status = dialog.querySelector("[data-plan-status]"), summary = dialog.querySelector("[data-plan-summary]"), reviewButton = dialog.querySelector("[data-plan-review]"), commitButton = dialog.querySelector("[data-plan-commit]"), checkButton = dialog.querySelector("[data-plan-check]"), retryButton = dialog.querySelector("[data-plan-retry]"), endpoint = document.querySelector("meta[name=bonfyre-workspace-store]")?.content, records = endpoint ? (await import(new URL("../bridge/workspace-store.mjs", import.meta.url))).openDestinationStore(endpoint) : await import("./instrument-local-store.mjs");
    let plan = null, operation = null, busy = !1, committed = !1;
    const readExact = (key) => records.readVersion ? records.readVersion(key) : records.readLocal(key).then((rows) => rows.find((r) => r.key === key) || null), controls = () => {
      for (const n of dialog.querySelectorAll("input,select"))
        n.disabled = busy || !!operation || committed;
      reviewButton.disabled = busy || !!operation || committed;
      commitButton.disabled = busy || !plan || !!operation || committed;
      checkButton.hidden = !operation || committed;
      checkButton.disabled = busy;
      retryButton.hidden = !operation?.retryable || committed;
      retryButton.disabled = busy;
    }, sourceCurrent = async () => {
      const now = await namedStore.checkpoint();
      if (now.key !== checkpoint.key || now.revision !== checkpoint.revision)
        throw Error("The source review changed. Close and prepare a new plan.");
    }, saved = (row) => {
      if (!row || row.deleted || row.revision !== 1 || row.value?.save_id !== operation.value.save_id || JSON.stringify(row.value) !== JSON.stringify(operation.value))
        throw Error("The saved plan differs from this reviewed snapshot. It was not overwritten.");
      committed = !0;
      dialog.dataset.state = "saved";
      status.textContent = "Saved the reviewed plan. The source review is unchanged.";
      const url = new URL("../plan/", location.href);
      url.search = "";
      url.hash = "";
      url.searchParams.set("record", operation.key);
      const link = document.createElement("a");
      link.href = url.href;
      link.dataset.openDeliveryPlan = "";
      link.textContent = "Open this plan in the dependency planner";
      summary.append(link);
      controls();
    };
    reviewButton.onclick = async () => {
      try {
        await sourceCurrent();
        const estimates = {};
        chosen.forEach((r, i) => {
          const row = dialog.querySelector('[data-plan-task="' + i + '"]'), values = Object.fromEntries([...row.querySelectorAll("[data-duration]")].map((n) => [n.dataset.duration, n.value]));
          estimates[r.id] = { ...values, owner: row.querySelector("[data-plan-owner]").value, deps: [...row.querySelector("[data-plan-deps]").selectedOptions].map((n) => n.value) };
        });
        plan = O.deliveryPlan({ requirements: reqs, ids, estimates, start: dialog.querySelector("[data-plan-start]").value, name: dialog.querySelector("[data-plan-name]").value, texts: sourceTexts, checkpoint });
        const problems = P.problems(plan.tasks);
        if (problems.length)
          throw Error(problems.join(" "));
        const schedule = P.cpm(plan.tasks), levelled = P.levelled(plan.tasks);
        summary.hidden = !1;
        summary.textContent = `${plan.tasks.length} tasks. Dependency-only duration: ${schedule.end.toFixed(2)} working days; with one task per named owner at a time: ${levelled.end.toFixed(2)} working days. ${schedule.critical.length} tasks lie on the critical path. Save creates one editable plan in the same destination.`;
        status.textContent = "Review complete. Nothing has been written.";
        dialog.dataset.state = "reviewed";
        controls();
      } catch (e) {
        plan = null;
        status.textContent = e.message;
        controls();
      }
    };
    dialog.addEventListener("input", () => {
      if (operation)
        return;
      plan = null;
      summary.hidden = !0;
      status.textContent = "Settings changed. Review the schedule again.";
      controls();
    });
    dialog.addEventListener("change", () => {
      if (operation)
        return;
      plan = null;
      summary.hidden = !0;
      controls();
    });
    const commit = async () => {
      if (busy || committed)
        return;
      busy = !0;
      controls();
      try {
        await sourceCurrent();
        await records.saveLocalBatch([{ key: checkpoint.key, expectedRevision: checkpoint.revision, checkOnly: !0 }, { key: operation.key, expectedRevision: 0, value: operation.value }]);
        saved(await readExact(operation.key));
      } catch (e) {
        dialog.dataset.state = "uncertain";
        status.textContent = "The plan could not be confirmed. Check the saved plan before retrying. " + e.message;
      } finally {
        busy = !1;
        controls();
      }
    };
    commitButton.onclick = () => {
      if (busy || operation || !plan)
        return;
      operation = { key: "plan:" + crypto.randomUUID(), value: { name: plan.name, saved_at: new Date().toISOString(), save_id: crypto.randomUUID(), state: structuredClone(plan) }, retryable: !1 };
      commit();
    };
    checkButton.onclick = async () => {
      if (busy || !operation)
        return;
      busy = !0;
      controls();
      try {
        const row = await readExact(operation.key);
        if (row)
          saved(row);
        else {
          await sourceCurrent();
          const current = await readExact(checkpoint.key);
          if (!current || current.revision !== checkpoint.revision)
            throw Error("The saved source review changed. Prepare a new plan rather than overwriting it.");
          operation.retryable = !0;
          status.textContent = "This exact plan is not present. Retry this reviewed snapshot explicitly.";
        }
      } catch (e) {
        status.textContent = e.message;
      } finally {
        busy = !1;
        controls();
      }
    };
    retryButton.onclick = () => {
      if (operation?.retryable) {
        operation.retryable = !1;
        commit();
      }
    };
    dialog.querySelector("[data-plan-close]").onclick = () => {
      dialog.close();
      if (!operation || committed)
        dialog.remove();
    };
    dialog.addEventListener("cancel", (e) => {
      e.preventDefault();
      dialog.close();
      if (!operation || committed)
        dialog.remove();
    });
    window.addEventListener("beforeunload", (e) => {
      if (operation && !committed) {
        e.preventDefault();
        e.returnValue = "";
      }
    });
  } catch (e) {
    say(e.message, !0);
  }
}
function installDeskControls() {
  const tabs = [...root.querySelectorAll("[data-tab]")];
  root.querySelector(".od-tabs").addEventListener("keydown", (e) => {
    const index = tabs.indexOf(e.target);
    if (index < 0)
      return;
    let next;
    if (e.key === "ArrowRight")
      next = (index + 1) % tabs.length;
    else if (e.key === "ArrowLeft")
      next = (index + tabs.length - 1) % tabs.length;
    else if (e.key === "Home")
      next = 0;
    else if (e.key === "End")
      next = tabs.length - 1;
    else
      return;
    e.preventDefault();
    view = tabs[next].dataset.tab;
    render();
    tabs[next].focus();
  });
  const separator = root.querySelector(".od-separator");
  let drag = null;
  const resize = (value) => {
    const width = Math.max(240, Math.min(600, value));
    root.style.setProperty("--od-inspector", width + "px");
    separator.setAttribute("aria-valuenow", String(Math.round(width)));
  };
  separator.addEventListener("keydown", (e) => {
    const width = Number(separator.getAttribute("aria-valuenow"));
    if (!["ArrowLeft", "ArrowRight", "Home", "End"].includes(e.key))
      return;
    e.preventDefault();
    resize(e.key === "Home" ? 240 : e.key === "End" ? 600 : width + (e.key === "ArrowLeft" ? 20 : -20));
  });
  separator.addEventListener("pointerdown", (e) => {
    drag = { x: e.clientX, width: Number(separator.getAttribute("aria-valuenow")) };
    separator.setPointerCapture(e.pointerId);
    e.preventDefault();
  });
  separator.addEventListener("pointermove", (e) => {
    if (drag)
      resize(drag.width + drag.x - e.clientX);
  });
  for (const event of ["pointerup", "pointercancel", "lostpointercapture"])
    separator.addEventListener(event, () => {
      drag = null;
    });
}
installDeskControls();

function sameReviewedValue(a, b) {
  if (Object.is(a, b))
    return !0;
  if (!a || !b || typeof a !== "object" || typeof b !== "object" || Array.isArray(a) !== Array.isArray(b))
    return !1;
  const keys = Object.keys(a);
  return keys.length === Object.keys(b).length && keys.every((k) => Object.hasOwn(b, k) && sameReviewedValue(a[k], b[k]));
}

function readSourceDocuments(initialSource) {
  const epoch=docEpoch;
  return openSourceReview({
    root, sources:SRC, initialSource, current:()=>docEpoch===epoch,
    getRequirements:()=>W.reqs,
    apply:result=>{
      W.reqs=result.requirements;
      selected.clear();for(const id of result.added)selected.add(id);
      focus=result.added[0]??focus;view="grid";
      filter={source:"",theme:"",priority:"",coverage:""};
      persist();render();root.dispatchEvent(new Event("change",{bubbles:true}));
      say(`Added ${result.added.length} reviewed requirements; ${result.duplicates} already existed. Save to retain these changes.`);
    }
  });
}
$("read-sources").addEventListener("click",()=>readSourceDocuments());
