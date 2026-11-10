// The process simulator page. The map and the day strip are edited directly;
// every edit is worked out at once by formula, and the event simulation runs
// in a worker shortly after. A day of the simulation can be played back on
// the map. Work is kept in this browser and can be shared as a link.
import * as sim from './process-sim.mjs';
import { attachInstrumentStore, openInstrumentScratch } from './instrument-store.mjs';

const root = document.querySelector('[data-simulation]');
const config = JSON.parse(root.querySelector('[data-simulation-config]').textContent);
const $ = name => root.querySelector(`[data-ui="${name}"]`);
const here = new URL('.', import.meta.url);
const NS = 'http://www.w3.org/2000/svg';
const KEY = 'aurekai-process-sim-v1';
const store = await openInstrumentScratch(KEY, {saved:[]});
const esc = t => String(t ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
const fmtMin = m => !Number.isFinite(m) ? 'grows without limit' : m < 1 ? `${Math.round(m * 60)} s` : m < 90 ? `${m.toFixed(1)} min` : `${(m / 60).toFixed(1)} h`;
const pct = x => Number.isFinite(x) ? `${Math.round(x * 100)}%` : '—';
const el = (tag, attrs = {}, parent) => { const n = document.createElementNS(NS, tag); for (const [k, v] of Object.entries(attrs)) n.setAttribute(k, v); parent?.appendChild(n); return n; };
const say = (text, bad) => { const m = $('message'); m.textContent = text; m.dataset.tone = bad ? 'bad' : ''; };

let state = { model: null, selected: null, result: null, kept: null, clock: 9 * 60, playing: false, dirty: false, runId: 0 };
const options = () => ({ replications: +$('reps').value, days: state.model.arrivals.kind === 'trace' ? 1 : +$('days').value, warmup: state.model.arrivals.kind === 'trace' ? 0 : 60, seed: +$('seed').value || 1 });

// ------------------------------------------------------------ starting points

async function sample() {
  const trace = await (await fetch(new URL('sample-311.json', here))).json();
  const day = Array.from({ length: 24 }, (_, h) => h >= 8 && h < 18 ? 12 : h >= 6 && h < 22 ? 8 : 5);
  return {
    name: `NYC 311 phone requests, ${trace.weekday} ${trace.day}`,
    entry: 'answer',
    arrivals: { kind: 'trace', times: trace.minutes, source: trace.source, label: `${trace.minutes.length.toLocaleString()} real requests created from phone calls to NYC 311 on ${trace.weekday} ${new Date(trace.day + 'T12:00').toLocaleDateString(undefined, { day: 'numeric', month: 'long', year: 'numeric' })}` },
    stations: [
      { id: 'answer', name: 'Answer the call', servers: day, service: { kind: 'lognormal', mean: 6.5, cv: 0.7 }, patience: { kind: 'exponential', mean: 4 }, target: 0.5, goal: 0.8, routes: [{ to: 'specialist', p: 0.22 }, { to: 'exit', p: 0.78 }] },
      { id: 'specialist', name: 'Specialist follow-up', servers: 3, service: { kind: 'lognormal', mean: 14, cv: 0.8 }, target: 5, goal: 0.8, routes: [{ to: 'answer', p: 0.05 }, { to: 'exit', p: 0.95 }] },
    ],
    layout: { answer: { x: 300, y: 300 }, specialist: { x: 640, y: 160 } },
    assumed: 'Arrivals are the city\'s published records. Handling times, staff and patience are starting assumptions: change them, or load your own.',
  };
}

const blank = () => ({ name: 'New process', entry: 's1', arrivals: { kind: 'profile', perHour: Array.from({ length: 24 }, (_, h) => h >= 8 && h < 18 ? 40 : 6) },
  stations: [{ id: 's1', name: 'First step', servers: 4, service: { kind: 'exponential', mean: 5 }, target: 1, goal: 0.8, routes: [{ to: 'exit', p: 1 }] }], layout: { s1: { x: 400, y: 250 } } });

// ------------------------------------------------------------ the map

const map = $('map');
let drag = null;
function drawMap(live) {
  const m = state.model, svg = map; svg.replaceChildren();
  const defs = el('defs', {}, svg), mk = el('marker', { id: 'ps-arrow', viewBox: '0 0 10 10', refX: 9, refY: 5, markerWidth: 7, markerHeight: 7, orient: 'auto-start-reverse' }, defs);
  el('path', { d: 'M0 0L10 5L0 10z', class: 'ps-arrowhead' }, mk);
  const pos = id => m.layout[id] ?? { x: 500, y: 260 }, R = 64, exit = { x: 960, y: 260 };
  const entry = pos(m.entry);
  el('path', { d: `M20 ${entry.y}L${entry.x - R - 30} ${entry.y}`, class: 'ps-edge ps-in', 'marker-end': 'url(#ps-arrow)' }, svg);
  const inLabel = el('text', { x: 24, y: entry.y - 14, class: 'ps-edge-label ps-click', tabindex: 0, role: 'button' }, svg); inLabel.textContent = 'Arrivals ›';
  inLabel.addEventListener('click', () => select('arrivals'));
  el('text', { x: exit.x, y: exit.y + 4, class: 'ps-exit', 'text-anchor': 'middle' }, svg).textContent = 'Done';
  for (const s of m.stations) for (const r of s.routes) {
    if (!(r.p > 0)) continue;
    const a = pos(s.id), b = r.to === 'exit' ? exit : pos(r.to), self = r.to === s.id;
    let d, lx, ly;
    if (self) { d = `M${a.x + 20} ${a.y - R}C${a.x + 90} ${a.y - R - 110} ${a.x - 90} ${a.y - R - 110} ${a.x - 20} ${a.y - R}`; lx = a.x; ly = a.y - R - 88; }
    else { const dx = b.x - a.x, dy = b.y - a.y, len = Math.hypot(dx, dy) || 1, ux = dx / len, uy = dy / len, bend = r.to === 'exit' ? 0 : 36;
      const sx = a.x + ux * R, sy = a.y + uy * R, ex = b.x - ux * (r.to === 'exit' ? 34 : R + 8), ey = b.y - uy * (r.to === 'exit' ? 34 : R + 8), cx = (sx + ex) / 2 - uy * bend, cy = (sy + ey) / 2 + ux * bend;
      d = `M${sx} ${sy}Q${cx} ${cy} ${ex} ${ey}`; lx = (sx + 2 * cx + ex) / 4; ly = (sy + 2 * cy + ey) / 4 - 8; }
    el('path', { d, class: 'ps-edge' + (r.to === 'exit' ? ' ps-out' : ''), 'marker-end': 'url(#ps-arrow)' }, svg);
    el('text', { x: lx, y: ly, class: 'ps-edge-label', 'text-anchor': 'middle' }, svg).textContent = `${Math.round(r.p * 100)}%`;
  }
  const hour = Math.floor(state.clock / 60) % 24, formula = sim.formulaByHour(m)[hour];
  for (const s of m.stations) {
    const p = pos(s.id), g = el('g', { class: 'ps-node' + (state.selected === s.id ? ' is-selected' : ''), transform: `translate(${p.x} ${p.y})`, tabindex: 0, role: 'button', 'aria-label': `${s.name}. Drag to move; select to edit.`, 'data-id': s.id }, svg);
    const f = formula[s.id], tone = !f.stable ? 'bad' : f.serviceLevel >= (s.goal ?? 0.8) ? 'good' : 'warn';
    el('circle', { r: R, class: `ps-disc ps-${tone}` }, g);
    const staff = sim.serversAt(s, state.clock), busy = live?.[s.id]?.busy ?? null, shown = Math.min(staff, 36);
    for (let k = 0; k < shown; k++) { const t = -Math.PI / 2 + k / shown * Math.PI * 2; el('circle', { cx: Math.cos(t) * (R + 12), cy: Math.sin(t) * (R + 12), r: shown > 24 ? 3 : 4.5, class: 'ps-seat' + (busy != null && k < busy ? ' is-busy' : '') }, g); }
    const t1 = el('text', { y: -10, class: 'ps-node-title', 'text-anchor': 'middle' }, g); t1.textContent = s.name.length > 15 ? s.name.slice(0, 14) + '…' : s.name;
    const t2 = el('text', { y: 12, class: 'ps-node-sub', 'text-anchor': 'middle' }, g); t2.textContent = `${staff} staff · ${fmtMin(sim.moments(s.service).mean)}`;
    const t3 = el('text', { y: 30, class: 'ps-node-sub', 'text-anchor': 'middle' }, g);
    t3.textContent = live ? `${live[s.id]?.queue ?? 0} waiting` : f.stable ? `wait ${fmtMin(f.wq)}` : 'overloaded';
    if (live?.[s.id]?.queue) { const q = Math.min(live[s.id].queue, 40); for (let k = 0; k < q; k++) el('circle', { cx: -R - 22 - (k % 10) * 11, cy: -18 + Math.floor(k / 10) * 11, r: 4, class: 'ps-waiting' }, g); }
    const handle = el('circle', { cx: R, cy: 0, r: 9, class: 'ps-handle', 'aria-hidden': 'true' }, g); handle.dataset.handle = s.id;
    if (s.id === m.entry) el('text', { y: -R - 22, class: 'ps-node-sub', 'text-anchor': 'middle' }, g).textContent = 'work arrives here';
  }
  if (drag?.kind === 'link') el('line', { x1: drag.from.x, y1: drag.from.y, x2: drag.x, y2: drag.y, class: 'ps-edge ps-drawing' }, svg);
}

const point = e => { const r = map.getBoundingClientRect(), vb = map.viewBox.baseVal; return { x: (e.clientX - r.left) / r.width * vb.width, y: (e.clientY - r.top) / r.height * vb.height }; };
map.addEventListener('pointerdown', e => {
  const handle = e.target.dataset?.handle, node = e.target.closest('.ps-node');
  if (handle) { drag = { kind: 'link', id: handle, from: state.model.layout[handle], ...point(e) }; map.setPointerCapture(e.pointerId); e.preventDefault(); return; }
  if (node) { const p = point(e), at = state.model.layout[node.dataset.id]; drag = { kind: 'move', id: node.dataset.id, dx: p.x - at.x, dy: p.y - at.y, moved: false }; map.setPointerCapture(e.pointerId); select(node.dataset.id); }
});
map.addEventListener('pointermove', e => {
  if (!drag) return; const p = point(e);
  if (drag.kind === 'move') { drag.moved = true; state.model.layout[drag.id] = { x: Math.max(70, Math.min(900, p.x - drag.dx)), y: Math.max(70, Math.min(450, p.y - drag.dy)) }; drawMap(); }
  else { drag.x = p.x; drag.y = p.y; drawMap(); }
});
map.addEventListener('pointerup', e => {
  if (drag?.kind === 'link') { const p = point(e), target = state.model.stations.find(s => Math.hypot(state.model.layout[s.id].x - p.x, state.model.layout[s.id].y - p.y) < 60);
    if (target) addRoute(drag.id, target.id); }
  if (drag?.moved) changed('layout');
  drag = null; drawMap();
});
map.addEventListener('keydown', e => {
  const id = e.target.closest?.('.ps-node')?.dataset.id; if (!id) return;
  if (e.key === 'Enter' || e.key === ' ') { select(id); e.preventDefault(); }
  if (e.key === 'Delete' || e.key === 'Backspace') { removeStation(id); e.preventDefault(); }
  const step = { ArrowLeft: [-10, 0], ArrowRight: [10, 0], ArrowUp: [0, -10], ArrowDown: [0, 10] }[e.key];
  if (step) { const p = state.model.layout[id]; state.model.layout[id] = { x: p.x + step[0], y: p.y + step[1] }; drawMap(); root.querySelector(`.ps-node[data-id="${id}"]`)?.focus(); e.preventDefault(); }
});

function addRoute(from, to) {
  const s = state.model.stations.find(x => x.id === from), exit = s.routes.find(r => r.to === 'exit');
  if (s.routes.some(r => r.to === to)) { say(`${s.name} already sends work there. Change its share on the right.`); select(from); return; }
  const share = Math.min(0.2, exit?.p ?? 0);
  if (!(share > 0)) { say(`All of ${s.name}'s work already goes elsewhere. Lower another share first.`, true); return; }
  exit.p = +(exit.p - share).toFixed(4); s.routes.splice(s.routes.indexOf(exit), 0, { to, p: share });
  select(from); changed('route'); say(`Now ${Math.round(share * 100)}% of ${s.name} goes to ${to === from ? 'itself again' : state.model.stations.find(x => x.id === to).name}.`);
}
function removeStation(id) {
  const m = state.model; if (m.stations.length === 1) { say('A process needs at least one station.', true); return; }
  const gone = m.stations.find(s => s.id === id);
  m.stations = m.stations.filter(s => s.id !== id); delete m.layout[id];
  for (const s of m.stations) { const lost = s.routes.filter(r => r.to === id).reduce((a, r) => a + r.p, 0); s.routes = s.routes.filter(r => r.to !== id); const exit = s.routes.find(r => r.to === 'exit') ?? (s.routes.push({ to: 'exit', p: 0 }), s.routes.at(-1)); exit.p = +(exit.p + lost).toFixed(4); }
  if (m.entry === id) m.entry = m.stations[0].id;
  select(m.stations[0].id); changed('remove'); say(`Removed ${gone.name}. Work that went there now finishes instead.`);
}
$('add-station').addEventListener('click', () => {
  const m = state.model; let n = m.stations.length + 1; while (m.stations.some(s => s.id === 's' + n)) n++;
  const id = 's' + n; m.stations.push({ id, name: `Step ${n}`, servers: 2, service: { kind: 'exponential', mean: 5 }, target: 1, goal: 0.8, routes: [{ to: 'exit', p: 1 }] });
  m.layout[id] = { x: 480 + (n % 3) * 60, y: 380 }; select(id); changed('add');
  say('Added a step. Drag the small dot on another step\'s edge onto it to send work here.');
});

// ------------------------------------------------------------ the day strip

const strip = $('strip');
let paint = null;
function hourly() {
  const a = state.model.arrivals;
  if (a.kind === 'trace') { const days = Math.max(1, Math.ceil((a.times.at(-1) ?? 0) / sim.DAY)), out = Array(24).fill(0); for (const t of a.times) out[Math.floor(t / 60) % 24] += 1 / days; return out; }
  return a.kind === 'constant' ? Array(24).fill(a.perHour) : a.perHour;
}
function drawStrip() {
  const svg = strip; svg.replaceChildren();
  const W = 960, H = 220, top = 18, base = 176, colW = W / 24, rates = hourly(), maxRate = Math.max(1, ...rates) * 1.1;
  const s = state.model.stations.find(x => x.id === state.selected) ?? state.model.stations.find(x => x.id === state.model.entry);
  const staff = Array.from({ length: 24 }, (_, h) => sim.serversAt(s, h * 60)), maxStaff = Math.max(4, ...staff, ...(state.suggest ?? [])) * 1.15;
  const formula = sim.formulaByHour(state.model);
  for (let h = 0; h < 24; h++) {
    const f = formula[h][s.id], tone = !f.stable ? 'bad' : f.serviceLevel >= (s.goal ?? 0.8) ? 'good' : 'warn';
    el('rect', { x: h * colW + 1, y: base + 6, width: colW - 2, height: 6, class: `ps-band ps-${tone}` }, svg);
    const bh = rates[h] / maxRate * (base - top);
    el('rect', { x: h * colW + 4, y: base - bh, width: colW - 8, height: bh, rx: 3, class: 'ps-bar' + (state.model.arrivals.kind === 'profile' ? ' is-editable' : ''), 'data-hour': h }, svg);
    if (h % 3 === 0) el('text', { x: h * colW + 2, y: H - 8, class: 'ps-axis' }, svg).textContent = `${String(h).padStart(2, '0')}:00`;
  }
  const y = c => base - c / maxStaff * (base - top);
  el('path', { d: staff.map((c, h) => `${h ? 'L' : 'M'}${h * colW} ${y(c)}H${(h + 1) * colW}`).join(''), class: 'ps-staff' }, svg);
  if (state.suggest) el('path', { d: state.suggest.map((c, h) => `${h ? 'L' : 'M'}${h * colW} ${y(c)}H${(h + 1) * colW}`).join(''), class: 'ps-staff ps-suggest' }, svg);
  staff.forEach((c, h) => { const t = el('text', { x: h * colW + colW / 2, y: y(c) - 5, class: 'ps-staff-label', 'text-anchor': 'middle' }, svg); t.textContent = c; });
  if (state.byHour?.[s.id]) { const w = state.byHour[s.id], top2 = Math.max(1, ...w.filter(Number.isFinite));
    w.forEach((v, h) => { if (v == null) return; el('circle', { cx: h * colW + colW / 2, cy: base - Math.min(1, v / top2) * (base - top) * 0.9, r: 3.5, class: 'ps-simwait' }, svg); });
    const lab = el('text', { x: W - 4, y: top + 4, class: 'ps-axis ps-axis-wait', 'text-anchor': 'end' }, svg); lab.textContent = `highest dot: ${fmtMin(top2)} average wait`; }
  el('line', { x1: state.clock / 60 * colW, x2: state.clock / 60 * colW, y1: 4, y2: base + 12, class: 'ps-clock' }, svg);
  el('rect', { x: 0, y: 0, width: W, height: H, class: 'ps-hit' }, svg);
  $('strip-legend').innerHTML = `<span class="ps-key ps-key-bar"></span>${state.model.arrivals.kind === 'profile' ? 'Arrivals per hour (drag bars)' : 'Arrivals per hour'} <span class="ps-key ps-key-staff"></span>Staff at ${esc(s.name)} (drag the line)${state.suggest ? ' <span class="ps-key ps-key-suggest"></span>Suggested' : ''}${state.byHour ? ' <span class="ps-key ps-key-dot"></span>Simulated average wait' : ''} <span class="ps-key ps-key-band"></span>Goal met / short / overloaded`;
}
const stripHour = e => { const r = strip.getBoundingClientRect(); return Math.max(0, Math.min(23, Math.floor((e.clientX - r.left) / r.width * 24))); };
const stripValue = (e, max) => { const r = strip.getBoundingClientRect(), yy = (e.clientY - r.top) / r.height * 220; return Math.max(0, (176 - yy) / (176 - 18) * max); };
strip.addEventListener('pointerdown', e => {
  const s = state.model.stations.find(x => x.id === state.selected) ?? state.model.stations.find(x => x.id === state.model.entry), h = stripHour(e);
  const staff = Array.from({ length: 24 }, (_, k) => sim.serversAt(s, k * 60)), maxStaff = Math.max(4, ...staff, ...(state.suggest ?? [])) * 1.15, r = strip.getBoundingClientRect();
  const yy = (e.clientY - r.top) / r.height * 220, staffY = 176 - staff[h] / maxStaff * (176 - 18);
  if (Math.abs(yy - staffY) < 16) paint = { kind: 'staff', s, max: maxStaff };
  else if (state.model.arrivals.kind === 'profile' && e.target.classList.contains('ps-bar')) paint = { kind: 'rate', max: Math.max(1, ...hourly()) * 1.1 };
  else { state.clock = h * 60 + 30; stopPlay(); showClock(); drawMap(state.result ? liveAt(state.clock) : null); drawStrip(); return; }
  strip.setPointerCapture(e.pointerId); applyPaint(e);
});
strip.addEventListener('pointermove', e => { if (paint) applyPaint(e); });
strip.addEventListener('pointerup', () => { if (paint) { paint = null; changed('strip'); renderInspector(); } });
function applyPaint(e) {
  // Fast drags skip hours between pointer samples; every hour passed over takes the value.
  const h = stripHour(e), from = paint.last ?? h, lo = Math.min(from, h), hi = Math.max(from, h); paint.last = h;
  for (let k = lo; k <= hi; k++) {
    if (paint.kind === 'staff') { const s = paint.s; if (!Array.isArray(s.servers)) s.servers = Array(24).fill(s.servers); s.servers[k] = Math.min(500, Math.round(stripValue(e, paint.max))); }
    else state.model.arrivals.perHour[k] = Math.round(stripValue(e, paint.max) * 10) / 10;
  }
  drawStrip(); drawMap(); renderFormula();
}

// ------------------------------------------------------------ the inspector

function select(id) { state.selected = id; state.suggest = null; renderInspector(); drawMap(); drawStrip(); renderResults(); }
function renderInspector() {
  const box = $('inspector'), m = state.model;
  if (state.selected === 'arrivals') { renderArrivals(box); return; }
  const s = m.stations.find(x => x.id === state.selected); if (!s) { box.innerHTML = ''; return; }
  const mo = sim.moments(s.service), byHour = Array.isArray(s.servers);
  const others = s.routes.filter(r => r.to !== 'exit'), exit = s.routes.find(r => r.to === 'exit')?.p ?? 0;
  box.innerHTML = `<h2>${esc(s.name)}</h2>
  <label>Name<input data-f="name" value="${esc(s.name)}" maxlength="40"></label>
  <fieldset><legend>Staff</legend><div class="ps-pills"><button type="button" data-f="staff-flat" aria-pressed="${!byHour}">Same all day</button><button type="button" data-f="staff-hourly" aria-pressed="${byHour}">By the hour</button></div>
  ${byHour ? '<p class="ps-hint">Drag the line on the day strip below the map.</p>' : `<label>People working<input data-f="servers" type="number" min="0" max="500" step="1" value="${s.servers}"></label>`}</fieldset>
  <fieldset><legend>Handling time</legend><label>Shape<select data-f="kind">${[['exponential', 'Varies a lot (exponential)'], ['lognormal', 'Varies, long tail'], ['fixed', 'Always the same'], ['empirical', 'From my own times']].map(([k, t]) => `<option value="${k}"${s.service.kind === k ? ' selected' : ''}${k === 'empirical' && !s.service.samples ? ' disabled' : ''}>${t}</option>`).join('')}</select></label>
  ${s.service.kind === 'empirical' ? `<p class="ps-hint">${s.service.samples.length.toLocaleString()} recorded times, average ${fmtMin(mo.mean)}, spread ${mo.cv.toFixed(2)}.</p>` : `<div class="ps-pair"><label>Average (min)<input data-f="mean" type="number" min="0.05" step="0.1" value="${+mo.mean.toFixed(2)}"></label>${s.service.kind === 'lognormal' ? `<label>Spread<input data-f="cv" type="number" min="0" max="4" step="0.05" value="${s.service.cv}"></label>` : ''}</div>`}
  <label class="ps-file">Load recorded handling times (CSV)<input data-f="durations" type="file" accept=".csv,.txt,text/csv"></label></fieldset>
  <fieldset><legend>Waiting</legend><div class="ps-pair"><label>Answer within (s)<input data-f="target" type="number" min="0" step="5" value="${Math.round((s.target ?? 0) * 60)}"></label><label>For this share (%)<input data-f="goal" type="number" min="1" max="99" step="1" value="${Math.round((s.goal ?? 0.8) * 100)}"></label></div>
  <div class="ps-pair"><label>Give up after (min, avg)<input data-f="patience" type="number" min="0" step="0.5" value="${s.patience?.mean ?? ''}" placeholder="never"></label><label>Waiting room limit<input data-f="limit" type="number" min="0" step="1" value="${s.limit ?? ''}" placeholder="none"></label></div>
  <button type="button" data-f="find" class="ps-primary">Find staff for each hour</button><p class="ps-hint" data-ui="find-note"></p></fieldset>
  <fieldset><legend>Where work goes next</legend>${others.map(r => `<label>${r.to === s.id ? 'Back to this step' : esc(m.stations.find(x => x.id === r.to)?.name)} (%)<span class="ps-route"><input data-route="${esc(r.to)}" type="number" min="0" max="100" step="1" value="${Math.round(r.p * 100)}"><button type="button" data-unroute="${esc(r.to)}" aria-label="Stop sending work there">✕</button></span></label>`).join('')}
  <p class="ps-hint">${Math.round(exit * 100)}% is finished here. Drag the dot on the step's edge onto another step to add a route.</p></fieldset>
  <div class="ps-pills">${m.entry !== s.id ? '<button type="button" data-f="entry">Make work arrive here</button>' : ''}<button type="button" data-f="remove">Remove step</button></div>`;
}
function renderArrivals(box) {
  const a = state.model.arrivals, rates = hourly(), total = rates.reduce((x, y) => x + y, 0);
  box.innerHTML = `<h2>Arrivals</h2><p>${a.kind === 'trace' ? esc(a.label ?? `${a.times.length.toLocaleString()} recorded arrivals`) + '. Each one is replayed at its own minute.' : `${Math.round(total).toLocaleString()} a day by the hourly rates. Drag the bars to change them.`}</p>
  ${a.source ? `<p class="ps-hint">From <a href="${esc(a.source.dataset)}" target="_blank" rel="noopener">${esc(a.source.name)}</a>, ${esc(a.source.publisher)}.</p>` : ''}
  <div class="ps-pills"><button type="button" data-f="to-profile">Edit as hourly rates</button></div>
  <fieldset><legend>Your own arrivals</legend><label class="ps-file">Load timestamps (CSV, one row per arrival)<input data-f="timestamps" type="file" accept=".csv,.txt,text/csv"></label></fieldset>
  <fieldset><legend>Another day from NYC 311</legend><div class="ps-pair"><label>Day<input data-f="nyc-day" type="date" value="${esc(config.nycDay)}" max="${new Date().toISOString().slice(0, 10)}"></label><label>Channel<select data-f="nyc-channel"><option value="PHONE">Phone</option><option value="ONLINE">Online</option><option value="MOBILE">App</option><option value="">All</option></select></label></div>
  <label>Agency (optional)<select data-f="nyc-agency"><option value="">Every agency</option>${['HPD', 'NYPD', 'DSNY', 'DOT', 'DEP', 'DOB', 'DPR', 'DOHMH', 'TLC', 'DHS'].map(x => `<option>${x}</option>`).join('')}</select></label><button type="button" data-f="nyc-load">Load that day</button><p class="ps-hint" data-ui="nyc-note">Read directly from NYC Open Data in your browser.</p></fieldset>`;
}
const inspector = $('inspector');
inspector.addEventListener('change', async e => {
  const f = e.target.dataset.f, s = state.model.stations.find(x => x.id === state.selected), v = e.target.value;
  if (e.target.dataset.route != null) { setRoute(s, e.target.dataset.route, +v / 100); return; }
  if (f === 'name') s.name = v.trim() || s.name;
  else if (f === 'servers') s.servers = Math.max(0, Math.min(500, Math.round(+v || 0)));
  else if (f === 'kind') s.service = v === 'empirical' ? { kind: 'empirical', samples: s.service.samples, mean: sim.moments(s.service).mean } : { kind: v, mean: sim.moments(s.service).mean || 5, cv: v === 'lognormal' ? (s.service.cv || 0.6) : undefined, samples: s.service.samples };
  else if (f === 'mean') s.service.mean = Math.max(0.05, +v || 0.05);
  else if (f === 'cv') s.service.cv = Math.max(0, Math.min(4, +v || 0));
  else if (f === 'target') s.target = Math.max(0, +v || 0) / 60;
  else if (f === 'goal') s.goal = Math.max(0.01, Math.min(0.99, (+v || 80) / 100));
  else if (f === 'patience') s.patience = +v > 0 ? { kind: 'exponential', mean: +v } : undefined;
  else if (f === 'limit') s.limit = v === '' ? undefined : Math.max(0, Math.round(+v));
  else if (f === 'durations') { const file = e.target.files[0]; if (!file) return; const d = sim.readDurations(await file.text());
    if (!d.samples.length) { say(`No handling times found in ${file.name}. Use minutes (4.5) or h:mm:ss.`, true); return; }
    s.service = { kind: 'empirical', samples: d.samples, mean: sim.moments({ kind: 'empirical', samples: d.samples }).mean }; say(`Using ${d.samples.length.toLocaleString()} handling times from ${file.name}${d.skipped ? `; ${d.skipped} rows were not times` : ''}.`); }
  else if (f === 'timestamps') { const file = e.target.files[0]; if (!file) return; const t = sim.readTimestamps(await file.text());
    if (!t.times.length) { say(`No dates or times found in ${file.name}.`, true); return; }
    state.model.arrivals = { kind: 'trace', times: t.times, label: `${t.times.length.toLocaleString()} arrivals from ${file.name} over ${t.days} day${t.days > 1 ? 's' : ''}` }; say(`Loaded ${t.times.length.toLocaleString()} arrivals${t.skipped ? `; ${t.skipped} rows had no readable time` : ''}.`); }
  else return;
  changed(f); renderInspector();
});
inspector.addEventListener('click', async e => {
  const f = e.target.dataset.f, s = state.model.stations.find(x => x.id === state.selected);
  if (e.target.dataset.unroute != null) { setRoute(s, e.target.dataset.unroute, 0, true); return; }
  if (f === 'staff-flat') { s.servers = Math.max(...[].concat(s.servers)); changed(f); renderInspector(); }
  else if (f === 'staff-hourly') { if (!Array.isArray(s.servers)) s.servers = Array(24).fill(s.servers); changed(f); renderInspector(); }
  else if (f === 'find') { const goal = s.goal ?? 0.8, staff = sim.staffingFor(state.model, s.id, goal); state.suggest = staff; drawStrip();
    const now = Array.from({ length: 24 }, (_, h) => sim.serversAt(s, h * 60)), hours = (a) => a.reduce((x, y) => x + y, 0);
    $('find-note').innerHTML = `${hours(staff).toLocaleString()} staff-hours to answer ${Math.round(goal * 100)}% within ${fmtMin(s.target ?? 0)} by formula (now ${hours(now).toLocaleString()}). <button type="button" data-f="use-suggest">Use these</button>`; }
  else if (f === 'use-suggest' && state.suggest) { s.servers = state.suggest.slice(); state.suggest = null; changed(f); renderInspector(); say('Staff set from the formula. The simulation checks it now.'); }
  else if (f === 'entry') { state.model.entry = s.id; changed(f); renderInspector(); }
  else if (f === 'remove') removeStation(s.id);
  else if (f === 'to-profile') { state.model.arrivals = { kind: 'profile', perHour: hourly().map(x => Math.round(x * 10) / 10) }; changed(f); renderInspector(); }
  else if (f === 'nyc-load') await loadNyc();
});
function setRoute(s, to, p, drop) {
  const exit = s.routes.find(r => r.to === 'exit') ?? (s.routes.push({ to: 'exit', p: 0 }), s.routes.at(-1)), r = s.routes.find(x => x.to === to);
  const others = s.routes.filter(x => x !== exit && x !== r).reduce((a, x) => a + x.p, 0), next = Math.max(0, Math.min(1 - others, p || 0));
  if (drop) s.routes = s.routes.filter(x => x !== r); else r.p = +next.toFixed(4);
  exit.p = +(1 - s.routes.filter(x => x !== exit).reduce((a, x) => a + x.p, 0)).toFixed(4);
  if (!drop && Math.abs(next - p) > 1e-9) say(`Capped at ${Math.round(next * 100)}%: the other routes already take the rest.`);
  changed('route'); renderInspector();
}
async function loadNyc() {
  const day = inspector.querySelector('[data-f="nyc-day"]').value, channel = inspector.querySelector('[data-f="nyc-channel"]').value, agency = inspector.querySelector('[data-f="nyc-agency"]').value, note = $('nyc-note');
  if (!/^\d{4}-\d{2}-\d{2}$/.test(day)) { note.textContent = 'Choose a day.'; return; }
  const where = [`created_date between '${day}T00:00:00' and '${day}T23:59:59'`, channel && `open_data_channel_type='${channel}'`, agency && `agency='${agency}'`].filter(Boolean).join(' and ');
  const url = `https://data.cityofnewyork.us/resource/erm2-nwe9.json?$select=created_date&$where=${encodeURIComponent(where)}&$order=created_date&$limit=50000`;
  note.textContent = 'Reading NYC Open Data…';
  try {
    const rows = await (await fetch(url)).json(); if (!Array.isArray(rows)) throw Error(rows.message || 'unexpected answer');
    if (!rows.length) { note.textContent = 'No requests recorded for that choice yet. Recent days fill in over a few days.'; return; }
    const start = Date.parse(day + 'T00:00:00Z'), times = rows.map(r => (Date.parse(r.created_date + 'Z') - start) / 60000).filter(t => t >= 0 && t < sim.DAY);
    const label = `${times.length.toLocaleString()} ${channel ? { PHONE: 'phone', ONLINE: 'online', MOBILE: 'app' }[channel] + ' ' : ''}requests to NYC 311${agency ? ` for ${agency}` : ''} on ${new Date(day + 'T12:00').toLocaleDateString(undefined, { weekday: 'long', day: 'numeric', month: 'long', year: 'numeric' })}`;
    state.model.arrivals = { kind: 'trace', times, label, source: { name: 'NYC 311 Service Requests from 2010 to Present', publisher: 'City of New York, NYC Open Data', dataset: 'https://data.cityofnewyork.us/Public-Safety/311-Service-Requests-from-2010-to-Present/erm2-nwe9', query: url, fetched_at: new Date().toISOString() } };
    if (rows.length === 50000) say('That day has more than 50,000 requests; the first 50,000 are used.');
    changed('arrivals'); renderInspector(); say(`Loaded ${label}.`);
  } catch (error) { note.textContent = `NYC Open Data did not answer (${error.message}). Try again in a moment.`; }
}

// ------------------------------------------------------------ formula, simulation, results

function dayFormula() {
  const byHour = sim.formulaByHour(state.model), rates = hourly();
  return Object.fromEntries(state.model.stations.map(s => { let lam = 0, wq = 0, sl = 0, busy = 0, cap = 0, unstable = [];
    byHour.forEach((h, k) => { const f = h[s.id], l = f.lambda * 60; lam += l; if (!f.stable) unstable.push(k); else { wq += l * f.wq; sl += l * f.serviceLevel; } busy += Math.min(f.lambda * sim.moments(s.service).mean, sim.serversAt(s, k * 60)); cap += sim.serversAt(s, k * 60); });
    const stableLam = lam - unstable.reduce((a, k) => a + byHour[k][s.id].lambda * 60, 0);
    return [s.id, { perDay: lam, wait: unstable.length ? Infinity : stableLam ? wq / stableLam : 0, serviceLevel: stableLam ? sl / lam : 0, utilization: cap ? busy / cap : 0, unstable, exact: byHour.every(h => h[s.id].exact) }]; }));
}
function renderFormula() { state.formula = dayFormula(); renderResults(); }

let worker = null, timer = null;
function changed(why) {
  state.dirty = true; state.result = state.result && { ...state.result, stale: true }; state.byHour = null;
  const found = sim.problems(state.model); $('problems').textContent = found.join(' ');
  renderFormula(); drawMap(); drawStrip(); save('Working copy');
  clearTimeout(timer); if (!found.length) timer = setTimeout(runSim, 500);
}
function runSim() {
  const found = sim.problems(state.model); if (found.length) { say(found[0], true); return; }
  worker ??= new Worker(new URL('process-sim-worker.mjs', here), { type: 'module' });
  const id = ++state.runId, opts = options(), model = JSON.parse(JSON.stringify(state.model));
  $('run-state').textContent = `Simulating ${opts.replications} runs…`; stopPlay();
  worker.onmessage = ({ data }) => {
    if (data.id !== state.runId) return;
    if (data.error) { $('run-state').textContent = ''; say(data.error, true); return; }
    state.result = data.result; state.byHour = waitsByHour(data.result.log); state.dirty = false;
    $('run-state').textContent = `${opts.replications} runs of ${opts.days === 1 ? 'one day' : opts.days + ' days'} in ${data.ms < 100 ? "under 0.1" : (data.ms / 1000).toFixed(1)} s`;
    renderResults(); drawStrip(); drawMap(liveAt(state.clock));
  };
  worker.postMessage({ id, model, options: opts });
}
function waitsByHour(log) {
  const out = {}; if (!log) return out;
  for (const s of state.model.stations) out[s.id] = Array(24).fill(null);
  const sums = state.model.stations.map(() => Array.from({ length: 24 }, () => [0, 0]));
  for (const [, i, a, st] of log) { const h = Math.floor(a / 60) % 24; sums[i][h][0] += st - a; sums[i][h][1]++; }
  state.model.stations.forEach((s, i) => sums[i].forEach(([w, n], h) => { if (n) out[s.id][h] = w / n; }));
  return out;
}
function renderResults() {
  const box = $('results'), m = state.model, r = state.result, F = state.formula ?? dayFormula(), kept = state.kept;
  const ci = (d, fmt = x => Math.round(x).toLocaleString()) => d && Number.isFinite(d.half) ? ` <small>± ${fmt(d.half)}</small>` : '';
  const verdicts = r && kept && !r.stale ? sim.compare(kept.result, r) : [];
  const word = v => v === 'better' ? '<em class="ps-better">better</em>' : v === 'worse' ? '<em class="ps-worse">worse</em>' : '<em>no clear change</em>';
  const rows = m.stations.map(s => { const f = F[s.id], x = r?.stations.find(y => y.id === s.id), k = kept?.result.stations.find(y => y.id === s.id), v = verdicts.find(y => y.id === s.id);
    const cell = (sv, fmt) => x ? `${fmt(sv.mean)}${ci(sv, fmt === pct ? v => `${(v * 100).toFixed(1)} pts` : fmt)}` : '…';
    return `<tbody class="${state.selected === s.id ? 'is-selected' : ''}"><tr><th colspan="${kept ? 4 : 3}" scope="rowgroup"><button type="button" data-pick="${esc(s.id)}">${esc(s.name)}</button> <small>${Math.round(f.perDay).toLocaleString()} a day reach it${f.unstable.length ? ` · more work than staff can handle in ${f.unstable.length} hour${f.unstable.length > 1 ? 's' : ''}` : ''}</small></th></tr>
    <tr><td>Average wait</td><td>${fmtMin(f.wait)}</td><td>${cell(x?.wait, fmtMin)}</td>${kept ? `<td>${k ? fmtMin(k.wait.mean) : '—'} ${v ? word(v.wait) : ''}</td>` : ''}</tr>
    <tr><td>Answered within ${fmtMin(s.target ?? 0)}</td><td>${pct(f.serviceLevel)}</td><td>${cell(x?.serviceLevel, pct)}</td>${kept ? `<td>${k ? pct(k.serviceLevel.mean) : '—'} ${v ? word(v.serviceLevel) : ''}</td>` : ''}</tr>
    <tr><td>Staff busy</td><td>${pct(f.utilization)}</td><td>${cell(x?.utilization, pct)}</td>${kept ? `<td>${k ? pct(k.utilization.mean) : '—'}</td>` : ''}</tr>
    ${s.patience || x?.abandoned.mean ? `<tr><td>Gave up waiting</td><td>—</td><td>${x ? Math.round(x.abandoned.mean).toLocaleString() + ci(x.abandoned) : '…'}</td>${kept ? `<td>${k ? Math.round(k.abandoned.mean).toLocaleString() : '—'} ${v ? word(v.abandoned) : ''}</td>` : ''}</tr>` : ''}
    ${s.limit != null ? `<tr><td>Turned away (room full)</td><td>—</td><td>${x ? Math.round(x.blocked.mean).toLocaleString() : '…'}</td>${kept ? '<td></td>' : ''}</tr>` : ''}
    ${x ? `<tr><td>1 in 10 waits longer than</td><td>—</td><td>${fmtMin(x.wait90.mean)}</td>${kept ? `<td>${k ? fmtMin(k.wait90.mean) : ''}</td>` : ''}</tr>` : ''}</tbody>`; }).join('');
  const sys = r?.system;
  box.innerHTML = `<table class="ps-table"><thead><tr><th scope="col"></th><th scope="col">By formula</th><th scope="col">Simulated${r?.stale ? ' <small>(updating)</small>' : ''}</th>${kept ? `<th scope="col">${esc(kept.name)}</th>` : ''}</tr></thead>${rows}
  ${sys ? `<tbody><tr><th colspan="${kept ? 4 : 3}" scope="rowgroup">Whole process</th></tr><tr><td>Finished</td><td>—</td><td>${Math.round(sys.done.mean).toLocaleString()}${ci(sys.done)}</td>${kept ? `<td>${Math.round(kept.result.system.done.mean).toLocaleString()}</td>` : ''}</tr><tr><td>Time from arrival to done</td><td>—</td><td>${fmtMin(sys.cycle.mean)}${ci(sys.cycle, fmtMin)}</td>${kept ? `<td>${fmtMin(kept.result.system.cycle.mean)}</td>` : ''}</tr></tbody>` : ''}</table>
  <p class="ps-hint">${explain(F, r)}</p>`;
  drawHistogram(); renderHiring();
}
function explain(F, r) {
  const notes = [];
  if (m().stations.some(s => !F[s.id].exact)) notes.push('Where handling times are not the "varies a lot" shape, the formula column is an approximation (Allen–Cunneen).');
  notes.push('The formula settles each hour on its own; the simulation carries queues from one hour into the next and lets people give up, so it is the one to trust when they differ.');
  if (r) notes.push(`Intervals are 95% across ${r.replications} independent runs.`);
  return notes.join(' ');
}
const m = () => state.model;
root.querySelector('[data-ui="results"]').addEventListener('click', e => { const id = e.target.dataset.pick; if (id) select(id); });
function drawHistogram() {
  const c = $('histogram'), x = state.result?.stations.find(s => s.id === state.selected), ctx = c.getContext('2d'), dpr = devicePixelRatio || 1, w = c.clientWidth, h = c.clientHeight;
  c.width = w * dpr; c.height = h * dpr; ctx.scale(dpr, dpr); ctx.clearRect(0, 0, w, h);
  $('histogram-label').textContent = x ? `How long people waited at ${x.name}, across all runs` : 'Run the simulation to see how waits spread out.';
  if (!x?.histogram) return; const { bins, top } = x.histogram, max = Math.max(1, ...bins), bw = w / bins.length, cs = getComputedStyle(root);
  bins.forEach((n, i) => { ctx.fillStyle = cs.getPropertyValue('--ps-accent'); const bh = n / max * (h - 22); ctx.fillRect(i * bw + 1, h - 18 - bh, bw - 2, bh); });
  ctx.fillStyle = cs.getPropertyValue('--ps-muted'); ctx.font = '11px "DM Mono", monospace'; ctx.fillText('0', 2, h - 4); const t = fmtMin(top); ctx.fillText(t, w - ctx.measureText(t).width - 2, h - 4);
}

// ------------------------------------------------------------ hiring

// The selected step's staff-hours a day become people, and people become a hiring sequence.
const hireInputs = () => Object.fromEntries([...root.querySelectorAll('[data-hire]')].map(i => [i.dataset.hire, +i.value]));
function renderHiring() {
  const s = state.model.stations.find(x => x.id === state.selected) ?? state.model.stations[0]; if (!s) return;
  const hours = Array.from({ length: 24 }, (_, h) => sim.serversAt(s, h * 60)).reduce((a, b) => a + b, 0), v = hireInputs();
  const need = sim.peopleFor(hours, { hoursPerPerson: v.hours || 7.5, shrinkage: (v.shrinkage || 0) / 100 }), cur = root.querySelector('[data-hire="current"]');
  if (cur.value === '' || cur.dataset.for !== s.id) { cur.value = Math.round(need); cur.dataset.for = s.id; }
  const plan = sim.hiringPlan({ need, current: +cur.value || 0, months: 12, growth: (v.growth || 0) / 100, attrition: (v.attrition || 0) / 100, ramp: v.ramp, lead: v.lead });
  $('hire-need').textContent = `${s.name} is staffed for ${hours.toLocaleString()} staff-hours a day, which takes about ${need.toFixed(1)} people at ${v.hours} hours each with ${v.shrinkage}% of time away.`;
  const svg = $('hire-chart'); svg.replaceChildren(); const W = 520, H = 170, top = Math.max(1, ...plan.rows.map(r => Math.max(r.need, r.capacity))) * 1.1, x = m => 30 + m * (W - 40) / 11, y = n => H - 22 - n / top * (H - 34);
  const mk = (tag, a) => { const n = document.createElementNS('http://www.w3.org/2000/svg', tag); for (const [k, val] of Object.entries(a)) n.setAttribute(k, val); svg.append(n); return n; };
  const month = m => { const d = new Date(); d.setDate(1); d.setMonth(d.getMonth() + m); return d.toLocaleDateString(undefined, { month: 'short' }); };
  for (const r of plan.rows) { if (r.starts) { mk('rect', { x: x(r.month) - 6, y: y(r.starts), width: 12, height: H - 22 - y(r.starts), class: 'ps-hire-bar' }); }
    if (r.short > 0.05) mk('rect', { x: x(r.month) - 10, y: 4, width: 20, height: H - 26, class: 'ps-hire-short' });
    mk('text', { x: x(r.month), y: H - 6, class: 'ps-axis', 'text-anchor': 'middle' }).textContent = month(r.month); }
  mk('path', { d: plan.rows.map((r, i) => `${i ? 'L' : 'M'}${x(r.month)} ${y(r.need)}`).join(''), class: 'ps-hire-need' });
  mk('path', { d: plan.rows.map((r, i) => `${i ? 'L' : 'M'}${x(r.month)} ${y(r.capacity)}`).join(''), class: 'ps-hire-cap' });
  const steps = plan.rows.filter(r => r.decide).map(r => `<li>In ${month(r.month)}, agree ${r.decide} hire${r.decide > 1 ? 's' : ''} to start in ${month(r.month + (v.lead || 0))}.</li>`);
  $('hire-steps').innerHTML = (steps.join('') || '<li>No hires needed over the next twelve months.</li>') + (plan.shortMonths.length ? `<li class="ps-warn-text">Short of people in ${plan.shortMonths.map(month).join(', ')}: new hires cannot be up to speed sooner.</li>` : '') + '<li class="ps-hint">Lines: people needed (pale) and the output of people working, counting time to full speed (lime). Bars: people starting.</li>';
}
root.querySelector('.ps-hire').addEventListener('input', renderHiring);

// ------------------------------------------------------------ playback

let raf = null, lastFrame = 0;
function liveAt(t) {
  const log = state.result?.log; if (!log) return null; const out = Object.fromEntries(state.model.stations.map(s => [s.id, { queue: 0, busy: 0 }]));
  for (const [, i, a, s, f, o] of log) { const id = state.model.stations[i]?.id; if (!id) continue; if (a <= t && t < s) out[id].queue++; else if (!o && s <= t && t < f) out[id].busy++; }
  return out;
}
function showClock() { const t = state.clock % sim.DAY; $('clock').textContent = `${String(Math.floor(t / 60)).padStart(2, '0')}:${String(Math.floor(t % 60)).padStart(2, '0')}`; }
function frame(now) {
  const dt = Math.min(100, now - lastFrame); lastFrame = now;
  state.clock = (state.clock + dt / 1000 * +$('speed').value) % sim.DAY; showClock();
  drawMap(liveAt(state.clock)); if (Math.floor(now / 250) !== Math.floor((now - dt) / 250)) drawStrip();
  raf = requestAnimationFrame(frame);
}
function stopPlay() { state.playing = false; cancelAnimationFrame(raf); $('play').textContent = 'Play the day'; }
$('play').addEventListener('click', () => {
  if (!state.result) { say('The simulation is still running. Play starts once it has finished.'); return; }
  if (state.playing) { stopPlay(); return; }
  state.playing = true; $('play').textContent = 'Pause'; lastFrame = performance.now(); raf = requestAnimationFrame(frame);
});

// ------------------------------------------------------------ keeping work

async function packed(model) {
  const bytes = new TextEncoder().encode(JSON.stringify(model)), stream = new Blob([bytes]).stream().pipeThrough(new CompressionStream('deflate-raw'));
  const out = new Uint8Array(await new Response(stream).arrayBuffer()); let s = ''; for (const b of out) s += String.fromCharCode(b);
  return btoa(s).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
}
async function unpacked(text) {
  const bin = atob(text.replace(/-/g, '+').replace(/_/g, '/')), bytes = Uint8Array.from(bin, c => c.charCodeAt(0));
  return JSON.parse(await new Response(new Blob([bytes]).stream().pipeThrough(new DecompressionStream('deflate-raw'))).text());
}
function save(name) {
  const db = store.get(), entry = { name, savedAt: new Date().toISOString(), model: state.model };
  db.saved = [entry, ...db.saved.filter(x => x.name !== name)].slice(0, 12);
  if (!store.set(db)) $('save-state').textContent = 'This browser is not keeping changes.';
  else if (name === 'Working copy') $('save-state').textContent = store.description;
  renderSaved();
}
function renderSaved() {
  const list = store.get().saved.filter(x => x.name !== 'Working copy');
  $('saved').innerHTML = list.length ? list.map((x, i) => `<li><button type="button" data-open="${i}">${esc(x.name)}</button> <small>${new Date(x.savedAt).toLocaleString()}</small> <button type="button" class="ps-quiet" data-forget="${i}" aria-label="Forget ${esc(x.name)}">✕</button></li>`).join('') : '<li class="ps-hint">Nothing saved under a name yet.</li>';
}
$('saved').addEventListener('click', e => {
  const list = store.get().saved.filter(x => x.name !== 'Working copy');
  if (e.target.dataset.open != null) { open(structuredClone(list[+e.target.dataset.open].model)); say(`Opened ${list[+e.target.dataset.open].name}.`); }
  if (e.target.dataset.forget != null) { const gone = list[+e.target.dataset.forget].name, db = store.get(); db.saved = db.saved.filter(x => x.name !== gone); store.set(db); renderSaved(); }
});
$('save').addEventListener('click', () => { const name = $('name').value.trim() || state.model.name; state.model.name = name; save(name); say(`Saved "${name}" in this browser.`); });
$('name').addEventListener('change', () => { state.model.name = $('name').value.trim() || state.model.name; save('Working copy'); });
$('keep').addEventListener('click', () => {
  if (!state.result || state.result.stale) { say('Wait for the simulation to finish, then keep it for comparison.'); return; }
  state.kept = { name: 'Kept: ' + (state.model.name.length > 24 ? state.model.name.slice(0, 23) + '…' : state.model.name), result: state.result }; renderResults();
  say('Kept for comparison. Change something; the next run is compared with it, using the same random draws.');
});
$('share').addEventListener('click', async () => {
  const url = new URL(location.href); url.hash = 'm=' + await packed(state.model);
  try { await navigator.clipboard.writeText(url.href); say('Link copied. It carries the whole process, including loaded arrivals.'); } catch { $('share-url').hidden = false; $('share-url').value = url.href; $('share-url').select(); }
});
const download = (name, type, text) => { const a = document.createElement('a'); a.href = URL.createObjectURL(new Blob([text], { type })); a.download = name; a.click(); setTimeout(() => URL.revokeObjectURL(a.href), 1000); };
const slug = () => state.model.name.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '').slice(0, 50) || 'process';
$('download-model').addEventListener('click', () => download(slug() + '.json', 'application/json', JSON.stringify(state.model, null, 1)));
$('download-results').addEventListener('click', () => {
  const r = state.result; if (!r) { say('Nothing simulated yet.'); return; }
  const F = dayFormula(), lines = ['station,measure,formula,simulated_mean,simulated_95_half_width,runs'];
  for (const s of r.stations) { const f = F[s.id]; for (const [k, label, fv] of [['wait', 'average wait (min)', f.wait], ['serviceLevel', 'answered within target (share)', f.serviceLevel], ['utilization', 'staff busy (share)', f.utilization], ['abandoned', 'gave up', ''], ['blocked', 'turned away', ''], ['wait90', '90th percentile wait (min)', '']])
    lines.push([JSON.stringify(s.name), JSON.stringify(label), Number.isFinite(fv) ? fv : fv === '' ? '' : 'unbounded', s[k].mean, Number.isFinite(s[k].half) ? s[k].half : '', r.replications].join(',')); }
  download(slug() + '-results.csv', 'text/csv', lines.join('\n') + '\n');
});
$('download-log').addEventListener('click', () => { const r = state.result; if (!r?.log) { say('Nothing simulated yet.'); return; } download(slug() + '-visits.csv', 'text/csv', sim.logCsv(state.model, r.log, r.warmup)); });
$('import-model').addEventListener('change', async e => { const file = e.target.files[0]; if (!file) return;
  try { const model = JSON.parse(await file.text()); const found = sim.problems(model); if (found.length) throw Error(found[0]); open(model); say(`Opened ${file.name}.`); } catch (error) { say(`${file.name} is not a process this page can open: ${error.message}`, true); } });
$('reset').addEventListener('click', async () => { open(await sample()); say('Back to the NYC 311 example.'); });
$('new').addEventListener('click', () => { open(blank()); say('A new process with one step. Add steps and drag routes between them.'); });
for (const id of ['reps', 'days', 'seed']) $(id).addEventListener('change', () => { state.result = null; renderResults(); runSim(); });

function open(model) {
  model.layout ??= {}; model.stations.forEach((s, i) => { model.layout[s.id] ??= { x: 200 + i * 160, y: 260 }; });
  state = { ...state, model, selected: model.entry, result: null, kept: null, byHour: null, suggest: null };
  $('name').value = model.name ?? ''; $('assumed').textContent = model.assumed ?? '';
  $('days-wrap').hidden = model.arrivals.kind === 'trace';
  changed('open'); renderInspector();
}

try {
  const hash = location.hash.match(/^#m=(.+)$/), saved = store.get().saved.find(x => x.name === 'Working copy');
  const model = hash ? await unpacked(hash[1]) : saved?.model ?? await sample();
  open(model); renderSaved(); showClock(); root.querySelector('[data-ui="ready"]').hidden = false; $('loading').hidden = true;
  if (hash) say('Opened the shared process. Your own working copy is kept separately once you save it under a name.');
  await attachInstrumentStore({ prefix: 'process-model:', label: 'process', mount: root.querySelector('[data-ui="ready"]'), get: () => state.model, set: model => open(structuredClone(model)) });
} catch (error) { $('loading').textContent = `The simulator could not start: ${error.message}`; }
