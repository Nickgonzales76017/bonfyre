// A plan as a network of tasks, worked out three ways: the critical path from
// the likely durations; a seeded Monte Carlo over three-point estimates, giving
// finish-date percentiles and how often each task lies on the critical path;
// and a schedule in which one person does one task at a time, showing how much
// later the plan ends when people, not only dependencies, are the limit.
// Durations are working days; calendar dates skip Saturdays and Sundays.

export function rng(seed) { let a = seed >>> 0 || 1; return () => { a = (a + 0x6d2b79f5) >>> 0; let t = a; t = Math.imul(t ^ (t >>> 15), t | 1); t ^= t + Math.imul(t ^ (t >>> 7), t | 61); return ((t ^ (t >>> 14)) >>> 0) / 4294967296; }; }

// ------------------------------------------------------------ shape of the network

export function problems(tasks) {
  const out = [], ids = new Set();
  for (const t of tasks) { if (!t.id || ids.has(t.id)) out.push(`Task "${t.name || t.id}" needs its own id.`); ids.add(t.id); }
  for (const t of tasks) {
    for (const d of t.deps ?? []) if (!ids.has(d)) out.push(`${t.name} waits on "${d}", which is not in the plan.`);
    const [o, m, p] = [t.optimistic, t.likely, t.pessimistic].map(Number);
    if (!(o >= 0 && m >= o && p >= m)) out.push(`${t.name}: estimates must run best ≤ likely ≤ worst.`);
  }
  if (!out.length) { const cycle = findCycle(tasks); if (cycle) out.push(`These tasks wait on each other in a loop: ${cycle.map(id => tasks.find(t => t.id === id).name).join(' → ')}.`); }
  return out;
}
export function findCycle(tasks) {
  const by = new Map(tasks.map(t => [t.id, t])), state = new Map(), stack = [];
  const visit = id => { state.set(id, 1); stack.push(id);
    for (const d of by.get(id)?.deps ?? []) { if (state.get(d) === 1) return [...stack.slice(stack.indexOf(d)), d]; if (!state.get(d)) { const c = visit(d); if (c) return c; } }
    stack.pop(); state.set(id, 2); return null; };
  for (const t of tasks) if (!state.get(t.id)) { const c = visit(t.id); if (c) return c; }
  return null;
}
export function order(tasks) {
  const by = new Map(tasks.map(t => [t.id, t])), seen = new Set(), out = [];
  const visit = id => { if (seen.has(id)) return; seen.add(id); for (const d of by.get(id).deps ?? []) visit(d); out.push(by.get(id)); };
  for (const t of tasks) visit(t.id);
  return out;
}

// ------------------------------------------------------------ critical path

export function cpm(tasks, duration = t => +t.likely) {
  const seq = order(tasks), es = new Map(), ef = new Map(), succ = new Map(tasks.map(t => [t.id, []]));
  for (const t of seq) { for (const d of t.deps ?? []) succ.get(d).push(t.id); const s = Math.max(0, ...(t.deps ?? []).map(d => ef.get(d))); es.set(t.id, s); ef.set(t.id, s + duration(t)); }
  const end = Math.max(0, ...ef.values()), ls = new Map(), lf = new Map();
  for (const t of [...seq].reverse()) { const f = Math.min(end, ...succ.get(t.id).map(s => ls.get(s))); lf.set(t.id, f); ls.set(t.id, f - duration(t)); }
  const rows = tasks.map(t => ({ id: t.id, es: es.get(t.id), ef: ef.get(t.id), ls: ls.get(t.id), lf: lf.get(t.id), slack: +(ls.get(t.id) - es.get(t.id)).toFixed(6), critical: Math.abs(ls.get(t.id) - es.get(t.id)) < 1e-9 }));
  return { end, rows: Object.fromEntries(rows.map(r => [r.id, r])), critical: seq.filter(t => rows.find(r => r.id === t.id).critical).map(t => t.id) };
}

// ------------------------------------------------------------ uncertainty

// PERT-Beta draws from the three-point estimate (the usual weight of four on the likely value).
export function pertDraw(o, m, p, r) {
  if (p - o < 1e-9) return m;
  const a = 1 + 4 * (m - o) / (p - o), b = 1 + 4 * (p - m) / (p - o);
  return o + (p - o) * beta(a, b, r);
}
function gamma(k, r) { // Marsaglia–Tsang
  if (k < 1) return gamma(k + 1, r) * Math.pow(r(), 1 / k);
  const d = k - 1 / 3, c = 1 / Math.sqrt(9 * d);
  for (;;) { let x, v; do { const u1 = r(), u2 = r(); x = Math.sqrt(-2 * Math.log(u1 || 1e-12)) * Math.cos(2 * Math.PI * u2); v = 1 + c * x; } while (v <= 0); v = v * v * v; const u = r();
    if (u < 1 - 0.0331 * x ** 4 || Math.log(u) < 0.5 * x * x + d * (1 - v + Math.log(v))) return d * v; }
}
const beta = (a, b, r) => { const x = gamma(a, r), y = gamma(b, r); return x / (x + y); };
export const pertMean = t => (+t.optimistic + 4 * +t.likely + +t.pessimistic) / 6;

export function simulate(tasks, { runs = 4000, seed = 1 } = {}) {
  const r = rng(seed), ends = new Float64Array(runs), onCritical = Object.fromEntries(tasks.map(t => [t.id, 0]));
  for (let k = 0; k < runs; k++) {
    const d = new Map(tasks.map(t => [t.id, pertDraw(+t.optimistic, +t.likely, +t.pessimistic, r)]));
    const res = cpm(tasks, t => d.get(t.id)); ends[k] = res.end; for (const id of res.critical) onCritical[id]++;
  }
  const sorted = Float64Array.from(ends).sort(), pct = p => sorted[Math.min(runs - 1, Math.floor(p * runs))];
  const bins = 30, lo = sorted[0], hi = sorted[runs - 1], width = (hi - lo) / bins || 1, histogram = Array(bins).fill(0);
  for (const e of ends) histogram[Math.min(bins - 1, Math.floor((e - lo) / width))]++;
  return { runs, seed, p10: pct(0.1), p50: pct(0.5), p80: pct(0.8), p95: pct(0.95), mean: ends.reduce((a, b) => a + b, 0) / runs,
    criticality: Object.fromEntries(Object.entries(onCritical).map(([id, n]) => [id, n / runs])), histogram, lo, hi, chanceBy: days => { let n = 0; for (const e of ends) if (e <= days + 1e-9) n++; return n / runs; } };
}

// ------------------------------------------------------------ people

// Serial schedule: whenever a task's predecessors are done and its owner is free, it starts; among tasks ready at
// the same moment the one with least slack goes first. Unassigned tasks need nobody in particular.
export function levelled(tasks, duration = t => +t.likely) {
  const base = cpm(tasks, duration), done = new Map(), busy = new Map(), start = new Map(), left = new Set(tasks.map(t => t.id)), by = new Map(tasks.map(t => [t.id, t]));
  while (left.size) {
    const ready = [...left].filter(id => (by.get(id).deps ?? []).every(d => done.has(d)));
    const candidates = ready.map(id => { const t = by.get(id), after = Math.max(0, ...(t.deps ?? []).map(d => done.get(d))), free = t.owner ? busy.get(t.owner) ?? 0 : 0; return { id, at: Math.max(after, free), slack: base.rows[id].slack }; });
    candidates.sort((a, b) => a.at - b.at || a.slack - b.slack || a.id.localeCompare(b.id));
    const pick = candidates[0], t = by.get(pick.id);
    start.set(pick.id, pick.at); done.set(pick.id, pick.at + duration(t)); if (t.owner) busy.set(t.owner, pick.at + duration(t)); left.delete(pick.id);
  }
  const end = Math.max(0, ...done.values());
  return { end, delay: end - base.end, rows: Object.fromEntries(tasks.map(t => [t.id, { start: start.get(t.id), finish: done.get(t.id), waited: start.get(t.id) - base.rows[t.id].es }])) };
}

// ------------------------------------------------------------ calendar

export function addWorkdays(isoDate, days) {
  const d = new Date(isoDate + 'T12:00:00Z'); let left = Math.ceil(days - 1e-9);
  while (left > 0) { d.setUTCDate(d.getUTCDate() + 1); const w = d.getUTCDay(); if (w !== 0 && w !== 6) left--; }
  return d.toISOString().slice(0, 10);
}
export function workdaysBetween(fromIso, toIso) {
  const a = new Date(fromIso + 'T12:00:00Z'), b = new Date(toIso + 'T12:00:00Z'); let n = 0;
  for (const d = new Date(a); d < b; d.setUTCDate(d.getUTCDate() + 1)) { const w = d.getUTCDay(); if (w !== 0 && w !== 6) n++; }
  return n;
}

// ------------------------------------------------------------ files

export function fromCsv(text) {
  const lines = String(text).split(/\r?\n/).filter(l => l.trim()), head = lines[0].toLowerCase().split(',').map(s => s.trim());
  const col = (...names) => head.findIndex(h => names.some(n => h.includes(n)));
  const ci = { id: col('id', 'key'), name: col('name', 'task', 'title'), o: col('best', 'optim', 'min'), m: col('likely', 'estimate', 'duration', 'days'), p: col('worst', 'pessim', 'max'), deps: col('depend', 'after', 'pred', 'wait'), owner: col('owner', 'who', 'assignee') };
  if (ci.name < 0 || ci.m < 0) throw Error('The file needs at least a task name column and a duration (or likely) column.');
  return lines.slice(1).map((l, i) => { const c = splitCsv(l), m = +c[ci.m];
    return { id: (ci.id >= 0 ? c[ci.id] : '') || `t${i + 1}`, name: c[ci.name], optimistic: ci.o >= 0 && c[ci.o] !== '' ? +c[ci.o] : m, likely: m, pessimistic: ci.p >= 0 && c[ci.p] !== '' ? +c[ci.p] : m,
      deps: ci.deps >= 0 ? (c[ci.deps] || '').split(/[;|]/).map(s => s.trim()).filter(Boolean) : [], owner: ci.owner >= 0 ? (c[ci.owner] || '').trim() : '' }; });
}
function splitCsv(line) { const out = []; let cur = '', q = false; for (let i = 0; i < line.length; i++) { const c = line[i]; if (q) { if (c === '"' && line[i + 1] === '"') { cur += '"'; i++; } else if (c === '"') q = false; else cur += c; } else if (c === '"') q = true; else if (c === ',') { out.push(cur); cur = ''; } else cur += c; } out.push(cur); return out.map(s => s.trim()); }
export function toCsv(tasks, schedule, startIso) {
  const q = v => /[",\n]/.test(String(v)) ? `"${String(v).replace(/"/g, '""')}"` : String(v);
  return 'id,name,best,likely,worst,waits_on,owner,start,finish,slack_days,critical\n' + tasks.map(t => { const r = schedule.rows[t.id];
    return [t.id, t.name, t.optimistic, t.likely, t.pessimistic, (t.deps ?? []).join(';'), t.owner ?? '', addWorkdays(startIso, r.es), addWorkdays(startIso, r.ef), r.slack, r.critical ? 'yes' : 'no'].map(q).join(','); }).join('\n') + '\n';
}
