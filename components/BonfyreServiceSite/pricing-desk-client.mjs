// The pricing desk. Drag the price along the curve and see volume, revenue,
// profit and break-even move; change the cost and demand assumptions beside
// it; try tiers against a spread of customer usage; load a price survey to see
// the range buyers call acceptable. The result is a proposal to download. It
// never sets a price.
import * as D from './pricing-desk.mjs';
import { attachInstrumentStore, openInstrumentScratch } from './instrument-store.mjs';

const root = document.querySelector('[data-pricing]');
const $ = n => root.querySelector(`[data-ui="${n}"]`);
const here = new URL('.', import.meta.url), NS = 'http://www.w3.org/2000/svg', KEY = 'aurekai-pricing-desk-v1';
const esc = t => String(t ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
const money = (v, d = v < 1 ? 3 : v < 100 ? 2 : 0) => Number.isFinite(v) ? '$' + v.toLocaleString(undefined, { minimumFractionDigits: d, maximumFractionDigits: d }) : '—';
const say = (text, bad) => { const m = $('message'); m.textContent = text; m.dataset.tone = bad ? 'bad' : ''; };
const store = await openInstrumentScratch(KEY, null);

const START = () => ({ cost: { computeSeconds: 4, computeRate: 0.0004, dataCost: 0.002, supportMinutes: 0.05, supportRate: 60, fixedMonthly: 250 },
  demand: { refPrice: 0.5, refVolume: 2000, shape: 'elastic', elasticity: 1.6, choke: 3 }, price: 0.5,
  tiers: [{ name: 'Starter', fee: 29, included: 100, overage: 0.35 }, { name: 'Team', fee: 149, included: 1000, overage: 0.2 }],
  spread: { customers: 200, median: 60, p90: 900, seed: 1 }, perUse: true });
let sample = null, W = null, current = null, survey = null;
const item = () => W.items[current];

// ------------------------------------------------------------ the curve

function chart() {
  const it = item(), cv = D.curve(it.cost, it.demand), svg = $('curve'); svg.replaceChildren();
  const w = 720, h = 300, L = 60, R = 20, T = 20, B = 36, lo = cv.points[0].price, hi = cv.points.at(-1).price;
  const maxP = Math.max(...cv.points.map(p => p.profit), 1), minP = Math.min(0, ...cv.points.map(p => p.profit)), maxV = Math.max(...cv.points.map(p => p.volume), 1);
  const x = p => L + (p - lo) / (hi - lo) * (w - L - R), y = v => T + (maxP - v) / (maxP - minP) * (h - T - B), yv = v => T + (1 - v / maxV) * (h - T - B);
  const el = (tag, a) => { const n = document.createElementNS(NS, tag); for (const [k, v] of Object.entries(a)) n.setAttribute(k, v); svg.append(n); return n; };
  if (survey?.low && survey?.high) el('rect', { x: x(Math.max(lo, survey.low)), y: T, width: Math.max(0, x(Math.min(hi, survey.high)) - x(Math.max(lo, survey.low))), height: h - T - B, class: 'pd-band' });
  el('line', { x1: L, x2: w - R, y1: y(0), y2: y(0), class: 'pd-zero' });
  el('path', { d: cv.points.map((p, i) => `${i ? 'L' : 'M'}${x(p.price)} ${yv(p.volume)}`).join(''), class: 'pd-volume' });
  el('path', { d: cv.points.map((p, i) => `${i ? 'L' : 'M'}${x(p.price)} ${y(p.profit)}`).join(''), class: 'pd-profit' });
  if (cv.unit >= lo && cv.unit <= hi) { el('line', { x1: x(cv.unit), x2: x(cv.unit), y1: T, y2: h - B, class: 'pd-floor' }); el('text', { x: x(cv.unit) + 4, y: T + 12, class: 'pd-axis' }).textContent = 'cost to serve'; }
  el('circle', { cx: x(cv.best.price), cy: y(cv.best.profit), r: 5, class: 'pd-best' });
  const px = x(it.price); el('line', { x1: px, x2: px, y1: T, y2: h - B, class: 'pd-price' });
  el('rect', { x: px - 14, y: h - B - 4, width: 28, height: 22, rx: 11, class: 'pd-knob', 'data-knob': 1 });
  for (let i = 0; i <= 4; i++) { const p = lo + (hi - lo) * i / 4; el('text', { x: x(p), y: h - 8, class: 'pd-axis', 'text-anchor': 'middle' }).textContent = money(p); }
  el('text', { x: 4, y: y(maxP) + 4, class: 'pd-axis' }).textContent = money(maxP, 0); el('text', { x: 4, y: y(0) + 4, class: 'pd-axis' }).textContent = '$0';
  const at = { volume: D.demand(it.price, it.demand) }; at.revenue = at.volume * it.price; at.profit = (it.price - cv.unit) * at.volume - (+it.cost.fixedMonthly || 0);
  $('readout').innerHTML = `<div><dt>Price per use</dt><dd>${money(it.price)}</dd></div><div><dt>Uses a month</dt><dd>${Math.round(at.volume).toLocaleString()}</dd></div><div><dt>Revenue</dt><dd>${money(at.revenue, 0)}</dd></div><div><dt>Profit after fixed</dt><dd class="${at.profit < 0 ? 'is-loss' : ''}">${money(at.profit, 0)}</dd></div>
  <div><dt>Margin per use</dt><dd>${money(it.price - cv.unit)}</dd></div><div><dt>Break-even uses</dt><dd>${Number.isFinite(D.breakEven(it.price, it.cost)) ? D.breakEven(it.price, it.cost).toLocaleString() : 'never'}</dd></div>`;
  $('verdict').textContent = `By this demand model, profit is highest at ${money(cv.best.price)}${cv.exact ? ` (the formula gives ${money(cv.exact)}: cost × elasticity ÷ (elasticity − 1))` : ''}. ${survey?.low ? `The survey's acceptable range runs ${money(survey.low)} to ${money(survey.high)}.` : 'Load a price survey to see what buyers call acceptable.'}`;
  $('legend').innerHTML = '<span class="pd-k pd-k-profit"></span>Profit after fixed costs <span class="pd-k pd-k-volume"></span>Uses a month <span class="pd-k pd-k-best"></span>Most profit' + (survey?.low ? ' <span class="pd-k pd-k-band"></span>Acceptable to buyers' : '');
}
let dragging = false;
const priceAt = e => { const svg = $('curve'), r = svg.getBoundingClientRect(), vb = svg.viewBox.baseVal, cv = D.curve(item().cost, item().demand), lo = cv.points[0].price, hi = cv.points.at(-1).price;
  const xx = (e.clientX - r.left) / r.width * vb.width; return Math.max(lo, Math.min(hi, lo + (xx - 60) / (720 - 80) * (hi - lo))); };
$('curve').addEventListener('pointerdown', e => { dragging = true; $('curve').setPointerCapture(e.pointerId); item().price = +priceAt(e).toFixed(4); chart(); tiersView(); });
$('curve').addEventListener('pointermove', e => { if (!dragging) return; item().price = +priceAt(e).toFixed(4); chart(); });
$('curve').addEventListener('pointerup', () => { if (dragging) { dragging = false; persist(); tiersView(); } });
$('curve').addEventListener('keydown', e => { const step = { ArrowLeft: -1, ArrowRight: 1 }[e.key]; if (!step) return; item().price = Math.max(0.0001, +(item().price * (1 + step * 0.02)).toFixed(4)); chart(); tiersView(); persist(); e.preventDefault(); });

// ------------------------------------------------------------ assumptions

const FIELDS = [['cost', 'computeSeconds', 'Compute seconds per use'], ['cost', 'computeRate', 'Compute cost per second ($)'], ['cost', 'dataCost', 'Data cost per use ($)'], ['cost', 'supportMinutes', 'Support minutes per use'], ['cost', 'supportRate', 'Support cost per hour ($)'], ['cost', 'fixedMonthly', 'Fixed cost a month ($)'],
  ['demand', 'refPrice', 'At this price ($)'], ['demand', 'refVolume', '…this many uses a month'], ['demand', 'elasticity', 'Price sensitivity (elasticity)']];
function assumptions() {
  const it = item();
  $('assumptions').innerHTML = `<h2>${esc(it.title)}</h2><p class="pd-hint">${esc(it.what)}</p>${it.answer ? `<p class="pd-hint">Its address answers today: <code>${esc(it.answer)}</code>.</p>` : ''}
  <fieldset><legend>Cost to serve one use</legend>${FIELDS.filter(f => f[0] === 'cost').map(([g, k, l]) => `<label>${l}<input type="number" min="0" step="any" data-g="${g}" data-k="${k}" value="${it[g][k]}"></label>`).join('')}<p class="pd-hint">= ${money(D.unitCost(it.cost), 4)} a use.</p></fieldset>
  <fieldset><legend>Demand</legend>${FIELDS.filter(f => f[0] === 'demand').map(([g, k, l]) => `<label>${l}<input type="number" min="0" step="any" data-g="${g}" data-k="${k}" value="${it[g][k]}"${k === 'elasticity' && it.demand.shape === 'linear' ? ' disabled' : ''}></label>`).join('')}
  <label>Shape<select data-g="demand" data-k="shape"><option value="elastic"${it.demand.shape !== 'linear' ? ' selected' : ''}>Falls off proportionally</option><option value="linear"${it.demand.shape === 'linear' ? ' selected' : ''}>Straight line to nobody</option></select></label>
  ${it.demand.shape === 'linear' ? `<label>Nobody buys at this many times the price<input type="number" min="1.1" step="0.1" data-g="demand" data-k="choke" value="${it.demand.choke}"></label>` : ''}</fieldset>
  <p class="pd-hint">These are assumptions. Replace them with measured costs and a price you have actually sold at.</p>`;
}
$('assumptions').addEventListener('change', e => { const g = e.target.dataset.g, k = e.target.dataset.k; if (!g) return; item()[g][k] = k === 'shape' ? e.target.value : Math.max(0, +e.target.value || 0); persist(); render(); });

// ------------------------------------------------------------ tiers

function tiersView() {
  const it = item(), uses = D.usage(it.spread), out = D.tierOutcome(uses, it.tiers, { perUse: it.perUse ? it.price : null, unit: D.unitCost(it.cost) });
  $('tiers').innerHTML = `<table class="pd-table"><thead><tr><th>Plan</th><th>Monthly fee</th><th>Uses included</th><th>Each extra use</th><th></th></tr></thead><tbody>${it.tiers.map((t, i) => `<tr>
    <td><input data-t="${i}" data-k="name" value="${esc(t.name)}" maxlength="30"></td><td><input data-t="${i}" data-k="fee" type="number" min="0" step="any" value="${t.fee}"></td><td><input data-t="${i}" data-k="included" type="number" min="0" step="1" value="${t.included}"></td><td><input data-t="${i}" data-k="overage" type="number" min="0" step="any" value="${t.overage}"></td>
    <td><button type="button" class="pd-quiet" data-drop-tier="${i}" aria-label="Remove ${esc(t.name)}">✕</button></td></tr>`).join('')}</tbody></table>
  <div class="pd-pills"><button type="button" data-ui="add-tier">Add a plan</button><label class="pd-check"><input type="checkbox" data-ui="per-use" ${it.perUse ? 'checked' : ''}> Also sell per use at ${money(it.price)}</label></div>
  <div class="pd-three"><label>Customers<input type="number" min="1" max="20000" step="1" data-s="customers" value="${it.spread.customers}"></label><label>Typical customer's uses a month<input type="number" min="1" step="1" data-s="median" value="${it.spread.median}"></label><label>Busiest tenth use over<input type="number" min="1" step="1" data-s="p90" value="${it.spread.p90}"></label></div>
  <table class="pd-table pd-out"><thead><tr><th>Plan</th><th>Customers</th><th>Revenue a month</th><th>Share</th></tr></thead><tbody>${out.rows.map(r => `<tr><td>${esc(r.name)}</td><td>${r.customers.toLocaleString()}</td><td>${money(r.revenue, 0)}</td><td><span class="pd-share" style="--w:${out.revenue ? r.revenue / out.revenue * 100 : 0}%"></span></td></tr>`).join('')}</tbody></table>
  <p>${out.customers.toLocaleString()} customers each choose their cheapest option: ${money(out.revenue, 0)} a month, ${money(out.perUnit)} per use on average, ${money(out.margin - (+it.cost.fixedMonthly || 0), 0)} after the cost of serving and fixed costs.</p>`;
}
$('tiers').addEventListener('change', e => { const it = item(), t = e.target.dataset.t, s = e.target.dataset.s;
  if (t != null) it.tiers[+t][e.target.dataset.k] = e.target.dataset.k === 'name' ? e.target.value : Math.max(0, +e.target.value || 0);
  if (s) it.spread[s] = Math.max(1, +e.target.value || 1);
  if (e.target.dataset.ui === 'per-use') it.perUse = e.target.checked;
  if (it.spread.p90 <= it.spread.median) { it.spread.p90 = it.spread.median * 1.5; say('The busiest tenth must use more than the typical customer; adjusted.'); }
  persist(); tiersView(); });
$('tiers').addEventListener('click', e => { const it = item(); if (e.target.dataset.ui === 'add-tier') { it.tiers.push({ name: 'New plan', fee: 0, included: 0, overage: it.price }); persist(); tiersView(); }
  if (e.target.dataset.dropTier != null) { it.tiers.splice(+e.target.dataset.dropTier, 1); persist(); tiersView(); } });

// ------------------------------------------------------------ the list, survey, proposal

function list() {
  $('list').innerHTML = W.items.map((it, i) => `<li><button type="button" data-pick="${i}" aria-current="${i === current}">${esc(it.title)}<small>${it.answer ? 'no asking price today' : 'your own'}</small></button></li>`).join('');
}
$('list').addEventListener('click', e => { const b = e.target.closest('[data-pick]'); if (b) { current = +b.dataset.pick; W.current = current; persist(); render(); } });
$('add-item').addEventListener('submit', e => { e.preventDefault(); const title = new FormData(e.target).get('title').trim(); if (!title) return; W.items.push({ id: 'own-' + Date.now().toString(36), title, what: 'Your product.', ...START() }); current = W.items.length - 1; e.target.reset(); persist(); render(); });
$('survey').addEventListener('change', async e => { const f = e.target.files[0]; if (!f) return;
  try { const vw = D.vanWestendorp(D.readSurvey(await f.text())); if (vw.usable < 5) throw Error(`only ${vw.usable} usable answers; at least five are needed`); survey = vw; render(); say(`${vw.usable} answers read${vw.dropped ? `; ${vw.dropped} were left out because their four prices were not in order` : ''}.`); }
  catch (err) { say(`${f.name} could not be used: ${err.message}.`, true); } });
$('propose').addEventListener('click', () => { const it = item(), p = D.proposal({ id: it.id }, it.cost, it.demand, it.price, it.tiers);
  const a = document.createElement('a'); a.href = URL.createObjectURL(new Blob([JSON.stringify(p, null, 1)], { type: 'application/json' })); a.download = `${it.id}-price-proposal.json`; a.click();
  say(`Proposal for ${it.title} at ${money(it.price)} a use downloaded. It sets nothing; a price is set on the desk by a person who can.`); });
$('reset').addEventListener('click', () => { W = fresh(); current = 0; survey = null; persist(); render(); say('Back to the starting assumptions.'); });

function fresh() { return { current: 0, items: sample.capabilities.map(c => ({ id: c.id, title: c.id.replace(/-/g, ' ').replace(/^./, x => x.toUpperCase()), what: c.what, answer: c.answer?.body?.error ?? String(c.answer?.status ?? ''), ...START() })) }; }
function persist() { store.set(W); }
function render() { list(); assumptions(); chart(); tiersView(); }

try {
  sample = await (await fetch(new URL('sample-capabilities.json', here))).json();
  const kept = store.get(); W = kept?.items?.length ? kept : fresh(); current = Math.min(W.current ?? 0, W.items.length - 1);
  render(); $('loading').hidden = true; $('ready').hidden = false;
  await attachInstrumentStore({ prefix: 'price-work:', label: 'pricing', mount: $('ready'), get: () => W, set: w => { W = structuredClone(w); current = Math.min(W.current ?? 0, W.items.length - 1); persist(); render(); } });
} catch (e) { $('loading').textContent = `The pricing desk could not start: ${e.message}`; }
