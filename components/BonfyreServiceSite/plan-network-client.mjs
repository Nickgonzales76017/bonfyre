// The dependency planner. The timeline is edited directly: drag a bar's end to
// change its likely length, drag the dot at its end onto another task to make
// that task wait on it. Every change reworks the critical path at once and
// reruns the finish-date simulation; the people view shows what one person
// doing one thing at a time does to the end date.
import * as P from './plan-network.mjs';
import { attachInstrumentStore, openInstrumentScratch } from './instrument-store.mjs';

const root = document.querySelector('[data-plan]');
const $ = n => root.querySelector(`[data-ui="${n}"]`);
const here = new URL('.', import.meta.url), NS = 'http://www.w3.org/2000/svg', KEY = 'aurekai-dependency-planner-v1';
const esc = t => String(t ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
const say = (text, bad) => { const m = $('message'); m.textContent = text; m.dataset.tone = bad ? 'bad' : ''; };
const store = await openInstrumentScratch(KEY, null);
const weekday = iso => { const d = new Date(iso + 'T12:00:00Z'); while ([0, 6].includes(d.getUTCDay())) d.setUTCDate(d.getUTCDate() + 1); return d.toISOString().slice(0, 10); };
const nice = iso => new Date(iso + 'T12:00:00Z').toLocaleDateString(undefined, { weekday: 'short', day: 'numeric', month: 'short' });
const dateAt = days => P.addWorkdays(weekday(S.start), days);
const finishDate = (es, ef) => P.addWorkdays(weekday(S.start), Math.max(es, Math.ceil(ef - 1e-9) - 1));

let S = null, cpm = null, sim = null, lev = null, selected = null, people = false, target = null;
function persist() { const ok = store.set(S); $('save-state').textContent = ok ? store.description : 'This browser is not keeping changes.'; }
function recompute() {
  const found = P.problems(S.tasks); $('problems').textContent = found.join(' ');
  if (found.length) { cpm = null; sim = null; lev = null; render(); return; }
  cpm = P.cpm(S.tasks); lev = P.levelled(S.tasks); sim = P.simulate(S.tasks, { runs: 3000, seed: S.seed ?? 1 });
  target ??= Math.ceil(sim.p80); persist(); render();
}
function render() { gantt(); inspector(); outlook(); }

// ------------------------------------------------------------ the timeline

const ROW = 34, LEFT = 230, TOP = 34;
let scale = 30, drag = null;
function gantt() {
  const svg = $('gantt'); svg.replaceChildren(); if (!cpm) return;
  const el = (tag, a, parent = svg) => { const n = document.createElementNS(NS, tag); for (const [k, v] of Object.entries(a)) n.setAttribute(k, v); parent.append(n); return n; };
  const rows = [...S.tasks].sort((a, b) => cpm.rows[a.id].es - cpm.rows[b.id].es || a.name.localeCompare(b.name));
  const span = Math.max(cpm.end, lev?.end ?? 0, sim?.p95 ?? 0) + 2, width = LEFT + span * scale + 40, height = TOP + rows.length * ROW + 20;
  svg.setAttribute('viewBox', `0 0 ${width} ${height}`); svg.style.width = width + 'px'; svg.style.height = height + 'px';
  for (let d = 0; d <= span; d++) { const x = LEFT + d * scale, iso = dateAt(d), monday = new Date(iso + 'T12:00:00Z').getUTCDay() === 1;
    el('line', { x1: x, x2: x, y1: TOP - 8, y2: height - 10, class: monday ? 'dp-week' : 'dp-day' });
    if (monday || d === 0) el('text', { x: x + 3, y: 20, class: 'dp-axis' }).textContent = nice(iso); }
  if (sim) for (const [p, label] of [[sim.p50, 'half the runs'], [sim.p80, '8 in 10'], [sim.p95, '19 in 20']]) { const x = LEFT + p * scale; el('line', { x1: x, x2: x, y1: TOP - 6, y2: height - 10, class: 'dp-pct' }); el('text', { x: x + 4, y: height - 2, class: 'dp-pct-label' }).textContent = label; }
  const y = id => TOP + rows.findIndex(t => t.id === id) * ROW + ROW / 2;
  for (const t of rows) for (const d of t.deps ?? []) { const a = cpm.rows[d], b = cpm.rows[t.id], x1 = LEFT + a.ef * scale, x2 = LEFT + b.es * scale, y1 = y(d), y2 = y(t.id);
    el('path', { d: `M${x1} ${y1}C${x1 + 18} ${y1} ${x2 - 18} ${y2} ${x2 - 2} ${y2}`, class: 'dp-link' + (a.critical && b.critical && Math.abs(a.ef - b.es) < 1e-9 ? ' is-critical' : '') }); }
  rows.forEach((t, i) => {
    const r = cpm.rows[t.id], yy = TOP + i * ROW, g = el('g', { class: 'dp-row' + (selected === t.id ? ' is-selected' : ''), 'data-task': t.id });
    el('rect', { x: 0, y: yy + 2, width, height: ROW - 4, class: 'dp-row-hit' }, g);
    const name = el('text', { x: 10, y: yy + ROW / 2 + 5, class: 'dp-name' }, g); name.textContent = t.name.length > (t.owner ? 21 : 27) ? t.name.slice(0, t.owner ? 20 : 26) + '…' : t.name;
    if (t.owner) el('text', { x: LEFT - 10, y: yy + ROW / 2 + 5, class: 'dp-owner', 'text-anchor': 'end' }, g).textContent = t.owner;
    const crit = sim?.criticality[t.id] ?? (r.critical ? 1 : 0);
    el('line', { x1: LEFT + (r.es + (+t.optimistic)) * scale, x2: LEFT + (r.es + (+t.pessimistic)) * scale, y1: yy + ROW / 2, y2: yy + ROW / 2, class: 'dp-range' }, g);
    if (people && lev) { const l = lev.rows[t.id]; el('rect', { x: LEFT + l.start * scale, y: yy + 9, width: Math.max(3, (l.finish - l.start) * scale), height: ROW - 18, rx: 4, class: 'dp-ghost' }, g); }
    el('rect', { x: LEFT + r.es * scale, y: yy + 7, width: Math.max(4, (r.ef - r.es) * scale), height: ROW - 14, rx: 6, class: 'dp-bar', style: `--crit:${crit.toFixed(3)}`, 'data-bar': t.id }, g);
    if (r.slack > 0) el('rect', { x: LEFT + r.ef * scale, y: yy + ROW / 2 - 1, width: r.slack * scale, height: 2, class: 'dp-slack' }, g);
    el('rect', { x: LEFT + r.ef * scale - 6, y: yy + 7, width: 8, height: ROW - 14, class: 'dp-resize', 'data-resize': t.id }, g);
    el('circle', { cx: LEFT + r.ef * scale + 8, cy: yy + ROW / 2, r: 5, class: 'dp-handle', 'data-link': t.id }, g);
  });
  if (drag?.kind === 'link') el('line', { x1: drag.x0, y1: drag.y0, x2: drag.x, y2: drag.y, class: 'dp-link is-drawing' });
  $('legend').innerHTML = `Bars are likely lengths, whiskers best to worst; brighter bars lie on the critical path in more of ${sim?.runs.toLocaleString() ?? ''} simulated runs.${people ? ' Pale bars: when each task can start with one person doing one thing at a time.' : ''}`;
}
const pt = e => { const svg = $('gantt'), r = svg.getBoundingClientRect(), vb = svg.viewBox.baseVal; return { x: (e.clientX - r.left) / r.width * vb.width, y: (e.clientY - r.top) / r.height * vb.height }; };
$('gantt').addEventListener('pointerdown', e => {
  const p = pt(e), link = e.target.dataset.link, resize = e.target.dataset.resize, row = e.target.closest('[data-task]');
  if (link) { drag = { kind: 'link', from: link, x0: p.x, y0: p.y, ...p }; }
  else if (resize) { const t = S.tasks.find(x => x.id === resize); drag = { kind: 'resize', id: resize, x0: p.x, m0: +t.likely }; }
  else if (row) { selected = row.dataset.task; render(); return; }
  else return;
  $('gantt').setPointerCapture(e.pointerId); e.preventDefault();
});
$('gantt').addEventListener('pointermove', e => {
  if (!drag) return; const p = pt(e);
  if (drag.kind === 'link') { drag.x = p.x; drag.y = p.y; gantt(); return; }
  const t = S.tasks.find(x => x.id === drag.id), m = Math.max(0.5, Math.round((drag.m0 + (p.x - drag.x0) / scale) * 2) / 2);
  t.likely = m; if (+t.optimistic > m) t.optimistic = m; if (+t.pessimistic < m) t.pessimistic = m;
  cpm = P.cpm(S.tasks); gantt();
});
$('gantt').addEventListener('pointerup', e => {
  if (drag?.kind === 'link') { const rows = [...S.tasks].sort((a, b) => cpm.rows[a.id].es - cpm.rows[b.id].es || a.name.localeCompare(b.name)), i = Math.floor((pt(e).y - TOP) / ROW), to = rows[i];
    if (to && to.id !== drag.from) { if ((to.deps ?? []).includes(drag.from)) say(`${to.name} already waits on that.`); else { to.deps = [...(to.deps ?? []), drag.from];
      if (P.findCycle(S.tasks)) { to.deps = to.deps.filter(d => d !== drag.from); say('That would make the tasks wait on each other in a loop.', true); } else say(`${to.name} now waits on ${S.tasks.find(x => x.id === drag.from).name}.`); } } }
  if (drag) { drag = null; recompute(); }
});
$('zoom').addEventListener('input', () => { scale = +$('zoom').value; gantt(); });

// ------------------------------------------------------------ a task

function inspector() {
  const box = $('inspector'), t = S.tasks.find(x => x.id === selected);
  if (!t) { box.innerHTML = '<p class="dp-hint">Select a task on the timeline. Drag the end of a bar to change its length; drag the dot after it onto another task to make that one wait.</p>'; return; }
  const r = cpm?.rows[t.id], owners = [...new Set(S.tasks.map(x => x.owner).filter(Boolean))];
  box.innerHTML = `<h2>${esc(t.name)}</h2>
  ${r ? `<p class="dp-hint">Starts ${nice(dateAt(r.es))}, done ${nice(finishDate(r.es, r.ef))}. ${r.critical ? 'On the critical path: any delay moves the end.' : `Can slip ${r.slack} working day${r.slack === 1 ? '' : 's'} without moving the end.`} On the critical path in ${Math.round((sim?.criticality[t.id] ?? 0) * 100)}% of runs.${people && lev?.rows[t.id].waited > 0 ? ` With people's time it waits ${lev.rows[t.id].waited} more day${lev.rows[t.id].waited === 1 ? '' : 's'} for ${esc(t.owner)}.` : ''}</p>` : ''}
  <label>Task<input data-f="name" value="${esc(t.name)}" maxlength="80"></label>
  <div class="dp-three"><label>Best (days)<input data-f="optimistic" type="number" min="0" step="0.5" value="${t.optimistic}"></label><label>Likely<input data-f="likely" type="number" min="0" step="0.5" value="${t.likely}"></label><label>Worst<input data-f="pessimistic" type="number" min="0" step="0.5" value="${t.pessimistic}"></label></div>
  <label>Who does it<input data-f="owner" value="${esc(t.owner ?? '')}" list="dp-owners" maxlength="40" placeholder="nobody in particular"></label><datalist id="dp-owners">${owners.map(o => `<option>${esc(o)}</option>`).join('')}</datalist>
  <fieldset><legend>Waits on</legend>${S.tasks.filter(x => x.id !== t.id).map(x => `<label class="dp-check"><input type="checkbox" data-dep="${esc(x.id)}" ${(t.deps ?? []).includes(x.id) ? 'checked' : ''}> ${esc(x.name)}</label>`).join('')}</fieldset>
  ${t.note ? `<p class="dp-hint">${esc(t.note)}</p>` : ''}
  <button type="button" data-f="remove" class="dp-quiet">Remove task</button>`;
}
$('inspector').addEventListener('change', e => {
  const t = S.tasks.find(x => x.id === selected), f = e.target.dataset.f; if (!t) return;
  if (e.target.dataset.dep) { const d = e.target.dataset.dep; t.deps = e.target.checked ? [...(t.deps ?? []), d] : (t.deps ?? []).filter(x => x !== d); if (P.findCycle(S.tasks)) { t.deps = t.deps.filter(x => x !== d); say('That would make the tasks wait on each other in a loop.', true); } }
  else if (['optimistic', 'likely', 'pessimistic'].includes(f)) t[f] = Math.max(0, +e.target.value || 0);
  else if (f === 'name') t.name = e.target.value.trim() || t.name; else if (f === 'owner') t.owner = e.target.value.trim();
  recompute();
});
$('inspector').addEventListener('click', e => { if (e.target.dataset.f !== 'remove') return; const gone = selected; S.tasks = S.tasks.filter(x => x.id !== gone); for (const x of S.tasks) x.deps = (x.deps ?? []).filter(d => d !== gone); selected = null; recompute(); say('Removed the task; anything that waited on it no longer does.'); });
$('add').addEventListener('submit', e => { e.preventDefault(); const name = new FormData(e.target).get('name').trim(); if (!name) return; let n = S.tasks.length + 1; while (S.tasks.some(x => x.id === 't' + n)) n++;
  S.tasks.push({ id: 't' + n, name, optimistic: 1, likely: 2, pessimistic: 4, deps: selected ? [selected] : [], owner: '' }); selected = 't' + n; e.target.reset(); recompute(); });

// ------------------------------------------------------------ when it ends

function outlook() {
  const box = $('outlook'); if (!sim) { box.innerHTML = ''; return; }
  const chance = sim.chanceBy(target), people2 = lev.delay > 0;
  box.innerHTML = `<p class="dp-big">${nice(finishDate(0, sim.p80))}</p><p>is when 8 in 10 simulated runs are done. Half are done by ${nice(finishDate(0, sim.p50))}; 19 in 20 by ${nice(finishDate(0, sim.p95))}. The plan with every task at its likely length ends ${nice(finishDate(0, cpm.end))}, which ${Math.round(sim.chanceBy(cpm.end) * 100)}% of runs manage.</p>
  <p>${people2 ? `With one person doing one thing at a time, the likely plan ends ${lev.delay} working day${lev.delay === 1 ? '' : 's'} later, ${nice(finishDate(0, lev.end))}.` : 'Nobody is asked to do two things at once in the likely plan.'}</p>
  <p class="dp-target">By <strong>${nice(finishDate(0, target))}</strong>: ${Math.round(chance * 100)}% of runs are done. <span class="dp-hint">Drag on the chart to choose a date.</span></p>`;
  const c = $('spread'), ctx = c.getContext('2d'), dpr = devicePixelRatio || 1, w = c.clientWidth, h = c.clientHeight, cs = getComputedStyle(root);
  c.width = w * dpr; c.height = h * dpr; ctx.scale(dpr, dpr); ctx.clearRect(0, 0, w, h);
  const max = Math.max(...sim.histogram), bw = w / sim.histogram.length, at = d => (d - sim.lo) / (sim.hi - sim.lo || 1) * w;
  sim.histogram.forEach((n, i) => { const mid = sim.lo + (i + 0.5) * (sim.hi - sim.lo) / sim.histogram.length; ctx.fillStyle = mid <= target ? cs.getPropertyValue('--dp-accent') : '#3c5a42'; ctx.fillRect(i * bw + 1, h - 18 - n / max * (h - 26), bw - 2, n / max * (h - 26)); });
  ctx.fillStyle = cs.getPropertyValue('--dp-muted'); ctx.font = '11px "DM Mono", monospace'; ctx.fillText(nice(finishDate(0, sim.lo)), 2, h - 4); const r = nice(finishDate(0, sim.hi)); ctx.fillText(r, w - ctx.measureText(r).width - 2, h - 4);
  ctx.strokeStyle = cs.getPropertyValue('--dp-display'); ctx.lineWidth = 2; ctx.beginPath(); ctx.moveTo(at(target), 0); ctx.lineTo(at(target), h - 18); ctx.stroke();
}
let picking = false;
const pickTarget = e => { const r = $('spread').getBoundingClientRect(); target = Math.round(sim.lo + (e.clientX - r.left) / r.width * (sim.hi - sim.lo)); outlook(); };
$('spread').addEventListener('pointerdown', e => { if (!sim) return; picking = true; $('spread').setPointerCapture(e.pointerId); pickTarget(e); });
$('spread').addEventListener('pointermove', e => { if (picking) pickTarget(e); });
$('spread').addEventListener('pointerup', () => { picking = false; });
$('people').addEventListener('click', () => { people = !people; $('people').setAttribute('aria-pressed', String(people)); render(); });

// ------------------------------------------------------------ keeping it

const download = (name, type, text) => { const a = document.createElement('a'); a.href = URL.createObjectURL(new Blob([text], { type })); a.download = name; a.click(); setTimeout(() => URL.revokeObjectURL(a.href), 1000); };
const slug = () => S.name.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '').slice(0, 50) || 'plan';
$('dl-csv').addEventListener('click', () => cpm && download(slug() + '.csv', 'text/csv', P.toCsv(S.tasks, cpm, weekday(S.start))));
$('dl-json').addEventListener('click', () => download(slug() + '.json', 'application/json', JSON.stringify(S, null, 1)));
$('open').addEventListener('change', async e => { const f = e.target.files[0]; if (!f) return; const text = await f.text();
  try { const tasks = /\.json$/i.test(f.name) ? JSON.parse(text).tasks : P.fromCsv(text); if (!Array.isArray(tasks) || !tasks.length) throw Error('no tasks in it');
    S = { name: f.name.replace(/\.\w+$/, ''), start: S.start, seed: 1, tasks }; selected = null; target = null; boot(); say(`Opened ${tasks.length} tasks from ${f.name}.`); }
  catch (err) { say(`${f.name} could not be read: ${err.message}.`, true); } });
$('start').addEventListener('change', () => { S.start = $('start').value || S.start; recompute(); });
$('name').addEventListener('change', () => { S.name = $('name').value.trim() || S.name; persist(); });
$('reset').addEventListener('click', async () => { S = await sample(); selected = null; target = null; boot(); say('Back to the example plan.'); });
async function sample() { return await (await fetch(new URL('sample-plan.json', here))).json(); }
function boot() { $('name').value = S.name; $('start').value = S.start; $('assumed').textContent = S.assumed ?? ''; recompute(); }

try { S = store.get() ?? await sample(); boot(); $('loading').hidden = true; $('ready').hidden = false;
  await attachInstrumentStore({ prefix: 'plan:', label: 'plan', mount: $('ready'), get: () => S, set: plan => { S = structuredClone(plan); selected = null; target = null; boot(); } }); }
catch (e) { $('loading').textContent = `The planner could not start: ${e.message}`; }
