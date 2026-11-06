// Queued work, simulated and checked. A process is stations joined by routes;
// work arrives at an entry station by an hourly profile (or an exact trace),
// waits for one of a station's servers, is served for a drawn time, may hang
// up while waiting, then moves on by the route probabilities. The simulation
// is event by event with a seeded generator, so a run can be repeated exactly,
// and every station is also worked out by queueing formulas so the two can be
// compared. Times are minutes.

export const DAY = 1440;

// ---------------------------------------------------------------- numbers

export function rng(seed) {
  let a = seed >>> 0 || 0x9e3779b9;
  const next = () => { a = (a + 0x6d2b79f5) >>> 0; let t = a; t = Math.imul(t ^ (t >>> 15), t | 1); t ^= t + Math.imul(t ^ (t >>> 7), t | 61); return ((t ^ (t >>> 14)) >>> 0) / 4294967296; };
  let spare = null;
  return {
    next,
    exp: mean => -Math.log(1 - next()) * mean,
    normal() { if (spare !== null) { const s = spare; spare = null; return s; } let u, v, s; do { u = next() * 2 - 1; v = next() * 2 - 1; s = u * u + v * v; } while (!s || s >= 1); const m = Math.sqrt(-2 * Math.log(s) / s); spare = v * m; return u * m; },
  };
}

// A drawn service (or patience) time from a distribution described by mean and coefficient of variation.
export function draw(dist, r) {
  const mean = Math.max(0, +dist.mean || 0);
  switch (dist.kind) {
    case 'fixed': return mean;
    case 'exponential': return r.exp(mean);
    case 'lognormal': { const cv = Math.max(0, +dist.cv || 0); if (!cv) return mean; const s2 = Math.log(1 + cv * cv); return Math.exp(Math.log(mean) - s2 / 2 + Math.sqrt(s2) * r.normal()); }
    case 'empirical': { const xs = dist.samples; return xs?.length ? xs[Math.floor(r.next() * xs.length)] : mean; }
    default: throw Error(`unknown distribution ${dist.kind}`);
  }
}

export function moments(dist) {
  if (dist.kind === 'empirical' && dist.samples?.length) {
    const xs = dist.samples, mean = xs.reduce((a, b) => a + b, 0) / xs.length;
    const sd = Math.sqrt(xs.reduce((a, b) => a + (b - mean) ** 2, 0) / Math.max(1, xs.length - 1));
    return { mean, cv: mean ? sd / mean : 0 };
  }
  return { mean: +dist.mean || 0, cv: dist.kind === 'fixed' ? 0 : dist.kind === 'exponential' ? 1 : Math.max(0, +dist.cv || 0) };
}

const T95 = [12.706, 4.303, 3.182, 2.776, 2.571, 2.447, 2.365, 2.306, 2.262, 2.228, 2.201, 2.179, 2.160, 2.145, 2.131, 2.120, 2.110, 2.101, 2.093, 2.086, 2.080, 2.074, 2.069, 2.064, 2.060, 2.056, 2.052, 2.048, 2.045, 2.042];
export function interval(xs) {
  const n = xs.length, mean = n ? xs.reduce((a, b) => a + b, 0) / n : NaN;
  if (n < 2) return { mean, half: NaN, n };
  const sd = Math.sqrt(xs.reduce((a, b) => a + (b - mean) ** 2, 0) / (n - 1));
  return { mean, half: (n - 1 <= 30 ? T95[n - 2] : 1.96) * sd / Math.sqrt(n), n };
}

// ---------------------------------------------------------------- formulas

// Probability an arrival waits in an M/M/c queue offered a erlangs (Erlang C), by the stable Erlang B recursion.
export function erlangC(c, a) {
  if (c <= 0 || a >= c) return 1;
  let b = 1;
  for (let k = 1; k <= c; k++) b = a * b / (k + a * b);
  return b / (1 - (a / c) * (1 - b));
}

// One station's steady state at a given arrival rate (per minute). Exponential arrivals and service give the exact
// M/M/c answer; other variability uses the Allen–Cunneen correction, an approximation that is named as one.
export function stationFormula({ lambda, mean, cv, servers, ca = 1, target = 0 }) {
  const c = Math.max(0, Math.floor(servers)), a = lambda * mean, rho = c ? a / c : Infinity;
  if (!(lambda > 0) || !(mean > 0)) return { rho: 0, pWait: 0, wq: 0, lq: 0, w: mean || 0, serviceLevel: 1, exact: true, stable: true };
  if (rho >= 1) return { rho, pWait: 1, wq: Infinity, lq: Infinity, w: Infinity, serviceLevel: 0, exact: false, stable: false };
  const pWait = erlangC(c, a), wqM = pWait * mean / (c * (1 - rho)), factor = (ca * ca + cv * cv) / 2, wq = wqM * factor;
  // Service level: P(wait ≤ target). Exact for M/M/c; for the approximation the tail is scaled by the same factor.
  const decay = (c - a) / mean / Math.max(factor, 1e-9);
  return { rho, pWait, wq, lq: lambda * wq, w: wq + mean, serviceLevel: 1 - pWait * Math.exp(-decay * target), exact: Math.abs(factor - 1) < 1e-9, stable: true };
}

// Flow into each station per minute: entry arrivals plus everything routed in, solved by repeated substitution.
// A loop whose routes return all work forever has no steady state and is reported, not hidden.
export function flows(model, entryRate) {
  const ids = model.stations.map(s => s.id), into = Object.fromEntries(ids.map(id => [id, 0]));
  let rate = { ...into };
  for (let i = 0; i < 2000; i++) {
    const next = { ...into }; next[model.entry] += entryRate;
    for (const s of model.stations) for (const r of s.routes ?? []) if (r.to !== 'exit') next[r.to] += rate[s.id] * r.p * (1 - (s.lostShare ?? 0));
    const delta = ids.reduce((m, id) => Math.max(m, Math.abs(next[id] - rate[id])), 0);
    rate = next;
    if (delta < 1e-12) return { rate, converged: true };
  }
  return { rate, converged: false };
}

export const serversAt = (s, minute) => Array.isArray(s.servers) ? s.servers[Math.floor(minute / 60) % 24] : s.servers;
export const arrivalsPerHour = (model, hour) => model.arrivals.kind === 'profile' ? model.arrivals.perHour[hour % 24] : model.arrivals.kind === 'constant' ? model.arrivals.perHour : NaN;

// The formula view by hour: each hour treated as its own steady state (a good guide when an hour is long
// against waiting times, which is what staffing by the hour already assumes).
export function formulaByHour(model) {
  const ca = model.arrivals.kind === 'trace' ? 1 : 1;
  return Array.from({ length: 24 }, (_, hour) => {
    const lam = (model.arrivals.kind === 'trace' ? traceRate(model.arrivals.times, hour) : arrivalsPerHour(model, hour)) / 60;
    const { rate } = flows(model, lam);
    return Object.fromEntries(model.stations.map(s => { const m = moments(s.service); return [s.id, { lambda: rate[s.id], ...stationFormula({ lambda: rate[s.id], mean: m.mean, cv: m.cv, servers: serversAt(s, hour * 60), ca, target: s.target ?? 0 }) }]; }));
  });
}
function traceRate(times, hour) { const days = Math.max(1, Math.ceil((times.at(-1) ?? 0) / DAY)); return times.filter(t => Math.floor(t / 60) % 24 === hour).length / days; }

// Smallest staff each hour that meets a station's target share answered within its target wait, by formula.
export function staffingFor(model, stationId, share) {
  const s = model.stations.find(x => x.id === stationId), m = moments(s.service), byHour = formulaByHour(model);
  return byHour.map(h => { const lambda = h[stationId].lambda; if (!(lambda > 0)) return 0; let c = Math.max(1, Math.ceil(lambda * m.mean));
    while (c < 500 && stationFormula({ lambda, mean: m.mean, cv: m.cv, servers: c, target: s.target ?? 0 }).serviceLevel < share) c++; return c; });
}

// ---------------------------------------------------------------- checking a model

export function problems(model) {
  const out = [], ids = new Set(model.stations?.map(s => s.id));
  if (!model.stations?.length) out.push('Add at least one station.');
  if (!ids.has(model.entry)) out.push('Choose where work arrives.');
  for (const s of model.stations ?? []) {
    const servers = Array.isArray(s.servers) ? s.servers : [s.servers];
    if (servers.some(c => !Number.isInteger(c) || c < 0 || c > 500) || (Array.isArray(s.servers) && s.servers.length !== 24)) out.push(`${s.name}: staff must be whole numbers from 0 to 500${Array.isArray(s.servers) ? ' for each of 24 hours' : ''}.`);
    if (!(moments(s.service).mean > 0)) out.push(`${s.name}: handling time must be above zero.`);
    const total = (s.routes ?? []).reduce((a, r) => a + r.p, 0);
    if ((s.routes ?? []).some(r => !(r.p >= 0) || (r.to !== 'exit' && !ids.has(r.to)))) out.push(`${s.name}: a route goes nowhere.`);
    if (Math.abs(total - 1) > 1e-6) out.push(`${s.name}: routes out add to ${Math.round(total * 100)}%, not 100%.`);
  }
  const a = model.arrivals;
  if (a?.kind === 'profile' && (a.perHour?.length !== 24 || a.perHour.some(x => !(x >= 0)))) out.push('Arrivals need a rate for each of 24 hours.');
  if (a?.kind === 'trace' && !a.times?.length) out.push('The arrival trace is empty.');
  if (!out.length && !flows(model, 1).converged) out.push('Some work loops forever: a cycle of routes never reaches the exit.');
  return out;
}

// ---------------------------------------------------------------- simulation

class Heap {
  constructor() { this.a = []; }
  get size() { return this.a.length; }
  push(e) { const a = this.a; a.push(e); let i = a.length - 1; while (i) { const p = (i - 1) >> 1; if (a[p].t < e.t || (a[p].t === e.t && a[p].k <= e.k)) break; a[i] = a[p]; i = p; } a[i] = e; }
  pop() { const a = this.a, top = a[0], last = a.pop(); if (a.length) { let i = 0; for (;;) { let c = 2 * i + 1; if (c >= a.length) break; if (c + 1 < a.length && (a[c + 1].t < a[c].t || (a[c + 1].t === a[c].t && a[c + 1].k < a[c].k))) c++; if (a[c].t > last.t || (a[c].t === last.t && a[c].k >= last.k)) break; a[i] = a[c]; i = c; } a[i] = last; } return top; }
}

function arrivalTimes(model, horizon, r) {
  const a = model.arrivals;
  if (a.kind === 'trace') return a.times.filter(t => t < horizon);
  const rate = m => (a.kind === 'constant' ? a.perHour : a.perHour[Math.floor(m / 60) % 24]) / 60, peak = Math.max(...(a.kind === 'constant' ? [a.perHour] : a.perHour)) / 60, out = [];
  if (!(peak > 0)) return out;
  // Thinning: candidates at the peak rate, each kept with probability rate(t)/peak.
  for (let t = r.exp(1 / peak); t < horizon; t += r.exp(1 / peak)) if (r.next() * peak < rate(t)) out.push(t);
  return out;
}

// One run. Returns per-station tallies counted after the warm-up, and (when asked) the record of each visit.
export function run(model, { seed = 1, days = 1, warmup = 0, keepLog = false, maxLog = 40000 } = {}) {
  const r = rng(seed), horizon = days * DAY + warmup, byId = new Map(model.stations.map((s, i) => [s.id, i])), n = model.stations.length;
  const st = model.stations.map(s => ({ s, busy: 0, queue: [], head: 0, waiting: 0, arrivals: 0, served: 0, abandoned: 0, blocked: 0, waits: [], within: 0, busyArea: 0, capArea: 0, qArea: 0, maxQ: 0, last: warmup }));
  const sys = { done: 0, cycle: [], wipArea: 0, wip: 0, last: warmup, entered: 0 }, log = keepLog ? [] : null, heap = new Heap();
  let seq = 0; const at = (t, k, x) => heap.push({ t, k: seq++, ...x });
  const area = (q, t) => { const from = Math.max(q.last, warmup); if (t > from) { const c = serversAt(q.s, from); q.busyArea += Math.min(q.busy, Math.max(c, q.busy)) * (t - from); q.capArea += Math.max(c, q.busy) * (t - from); q.qArea += q.waiting * (t - from); } q.last = Math.max(q.last, t); };
  const sysArea = t => { const from = Math.max(sys.last, warmup); if (t > from) sys.wipArea += sys.wip * (t - from); sys.last = Math.max(sys.last, t); };
  for (const t of arrivalTimes(model, horizon, r)) at(t, 0, { type: 'enter' });
  for (let h = 60; h < horizon; h += 60) at(h, 0, { type: 'shift' });
  let job = 0;
  const startFrom = (i, t) => {
    const q = st[i];
    while (q.waiting && q.busy < serversAt(q.s, t)) {
      const w = q.queue[q.head++]; if (w.gone) continue;
      w.started = true; q.waiting--; q.busy++;
      const wait = t - w.arrived;
      if (w.arrived >= warmup) { q.waits.push(wait); if (wait <= (q.s.target ?? 0) + 1e-9) q.within++; }
      const service = draw(q.s.service, r);
      if (log && log.length < maxLog) w.entry = log.push([w.id, i, w.arrived, t, t + service, 0]) - 1;
      at(t + service, 1, { type: 'done', i, w });
    }
    if (q.head > 1024 && q.head * 2 > q.queue.length) { q.queue = q.queue.slice(q.head); q.head = 0; }
  };
  const arrive = (i, t, w) => {
    const q = st[i]; area(q, t);
    if (t >= warmup) q.arrivals++;
    if (q.s.limit != null && q.waiting >= q.s.limit && q.busy >= serversAt(q.s, t)) { if (t >= warmup) q.blocked++; if (log && log.length < maxLog) log.push([w.id, i, t, t, t, 2]); leave(t, w); return; }
    const visit = { id: w.id, born: w.born, arrived: t, started: false, gone: false };
    q.queue.push(visit); q.waiting++; q.maxQ = Math.max(q.maxQ, t >= warmup ? q.waiting : 0);
    if (q.s.patience?.mean > 0) at(t + draw(q.s.patience, r), 2, { type: 'abandon', i, w: visit });
    startFrom(i, t);
  };
  const leave = (t, w) => { sysArea(t); sys.wip--; if (w.born >= warmup) { sys.done++; sys.cycle.push(t - w.born); } };
  while (heap.size) {
    const e = heap.pop(); if (e.t >= horizon) break;
    if (e.type === 'enter') { sysArea(e.t); sys.wip++; if (e.t >= warmup) sys.entered++; arrive(byId.get(model.entry), e.t, { id: job++, born: e.t }); }
    else if (e.type === 'shift') { for (let i = 0; i < n; i++) { area(st[i], e.t); startFrom(i, e.t); } }
    else if (e.type === 'abandon') { const q = st[e.i]; if (e.w.started || e.w.gone) continue; area(q, e.t); e.w.gone = true; q.waiting--; if (e.w.arrived >= warmup) { q.abandoned++; q.waits.push(e.t - e.w.arrived); } if (log && log.length < maxLog) log.push([e.w.id, e.i, e.w.arrived, e.t, e.t, 1]); leave(e.t, e.w); }
    else if (e.type === 'done') {
      const q = st[e.i]; area(q, e.t); q.busy--; if (e.w.arrived >= warmup) q.served++;
      startFrom(e.i, e.t);
      let u = r.next(), to = 'exit';
      for (const route of q.s.routes ?? []) { u -= route.p; if (u < 0) { to = route.to; break; } }
      if (to === 'exit') leave(e.t, e.w); else arrive(byId.get(to), e.t, e.w);
    }
  }
  for (const q of st) area(q, horizon); sysArea(horizon);
  const span = horizon - warmup, pct = (xs, p) => { if (!xs.length) return NaN; const s = Float64Array.from(xs).sort(); return s[Math.min(s.length - 1, Math.floor(p * s.length))]; };
  return {
    stations: st.map(q => ({ id: q.s.id, arrivals: q.arrivals, served: q.served, abandoned: q.abandoned, blocked: q.blocked,
      wait: q.waits.length ? q.waits.reduce((a, b) => a + b, 0) / q.waits.length : 0, wait90: pct(q.waits, 0.9) || 0,
      serviceLevel: q.waits.length ? q.within / q.waits.length : 1, utilization: q.capArea ? q.busyArea / q.capArea : 0, queue: q.qArea / span, maxQueue: q.maxQ, waits: q.waits })),
    system: { entered: sys.entered, done: sys.done, perHour: sys.done / span * 60, cycle: sys.cycle.length ? sys.cycle.reduce((a, b) => a + b, 0) / sys.cycle.length : 0, cycle90: pct(sys.cycle, 0.9) || 0, wip: sys.wipArea / span },
    log,
  };
}

// Several independent runs, summarised with 95% intervals. The first run's visit record is kept for playback.
export function simulate(model, { seed = 1, days = 1, warmup = 0, replications = 10, keepLog = true } = {}) {
  const found = problems(model); if (found.length) throw Error(found[0]);
  const runs = Array.from({ length: replications }, (_, k) => run(model, { seed: seed + k * 7919, days, warmup, keepLog: keepLog && k === 0 }));
  const sum = pick => interval(runs.map(pick));
  const keys = ['arrivals', 'served', 'abandoned', 'blocked', 'wait', 'wait90', 'serviceLevel', 'utilization', 'queue', 'maxQueue'];
  return {
    replications, days, warmup, seed,
    stations: model.stations.map((s, i) => ({ id: s.id, name: s.name, ...Object.fromEntries(keys.map(k => [k, sum(r => r.stations[i][k])])), waits: runs.flatMap(r => r.stations[i].waits) })),
    system: Object.fromEntries(['entered', 'done', 'perHour', 'cycle', 'cycle90', 'wip'].map(k => [k, sum(r => r.system[k])])),
    log: runs[0].log,
  };
}

// Two runs of the same seeds compared measure by measure: a difference counts only when the intervals do not overlap.
export function compare(a, b) {
  const verdict = (x, y, lowerIsBetter) => { const lo = d => d.mean - (d.half || 0), hi = d => d.mean + (d.half || 0);
    if (hi(y) < lo(x)) return lowerIsBetter ? 'better' : 'worse'; if (lo(y) > hi(x)) return lowerIsBetter ? 'worse' : 'better'; return 'same'; };
  return a.stations.map((s, i) => { const t = b.stations[i]; if (!t || t.id !== s.id) return null;
    return { id: s.id, name: s.name, wait: verdict(s.wait, t.wait, true), serviceLevel: verdict(s.serviceLevel, t.serviceLevel, false), abandoned: verdict(s.abandoned, t.abandoned, true), utilization: verdict(s.utilization, t.utilization, true) }; }).filter(Boolean);
}

// ---------------------------------------------------------------- reading people's files

// A column of timestamps (ISO or anything Date reads) becomes minutes from the first midnight, plus the hourly
// average across the days covered. Rows that are not dates are counted, not guessed at.
export function readTimestamps(text) {
  const rows = String(text).split(/\r?\n/).filter(Boolean), head = rows[0]?.split(',').map(x => x.trim().toLowerCase()) ?? [];
  const col = Math.max(0, head.findIndex(h => /time|date|created|arriv|start|open/.test(h)));
  const parsed = [], skipped = [];
  for (const row of rows.slice(head.some(h => isNaN(Date.parse(h))) ? 1 : 0)) {
    const cell = splitCsv(row)[col]?.trim(); const ms = Date.parse(cell);
    if (Number.isFinite(ms)) parsed.push(ms); else skipped.push(cell);
  }
  parsed.sort((x, y) => x - y);
  if (!parsed.length) return { times: [], perHour: Array(24).fill(0), days: 0, skipped: skipped.length };
  const first = new Date(parsed[0]); first.setHours(0, 0, 0, 0);
  const times = parsed.map(ms => (ms - first.getTime()) / 60000), days = Math.max(1, Math.ceil((times.at(-1) + 1e-9) / DAY));
  const perHour = Array(24).fill(0); for (const t of times) perHour[Math.floor(t / 60) % 24] += 1 / days;
  return { times, perHour, days, skipped: skipped.length };
}

// A column of durations (minutes, or h:mm:ss) becomes an empirical distribution.
export function readDurations(text) {
  const rows = String(text).split(/\r?\n/).filter(Boolean), out = []; let skipped = 0;
  for (const row of rows) {
    const cell = splitCsv(row).find(c => /\d/.test(c))?.trim() ?? '';
    const hms = cell.match(/^(\d+):(\d{1,2})(?::(\d{1,2}(?:\.\d+)?))?$/);
    const v = hms ? (hms[3] != null ? +hms[1] * 60 + +hms[2] + +hms[3] / 60 : +hms[1] + +hms[2] / 60) : Number(cell);
    if (Number.isFinite(v) && v > 0) out.push(v); else skipped++;
  }
  return { samples: out, skipped: Math.max(0, skipped - (rows.length && !/\d/.test(rows[0]) ? 1 : 0)) };
}

export function splitCsv(line) {
  const out = []; let cur = '', quoted = false;
  for (let i = 0; i < line.length; i++) { const c = line[i];
    if (quoted) { if (c === '"' && line[i + 1] === '"') { cur += '"'; i++; } else if (c === '"') quoted = false; else cur += c; }
    else if (c === '"') quoted = true; else if (c === ',') { out.push(cur); cur = ''; } else cur += c; }
  out.push(cur); return out;
}

// The visit record as CSV, in clock time from the start of day one.
export function logCsv(model, log, warmup = 0) {
  const clock = m => { const d = Math.floor(m / DAY), x = m - d * DAY; return `${d + 1} ${String(Math.floor(x / 60)).padStart(2, '0')}:${String(Math.floor(x % 60)).padStart(2, '0')}:${String(Math.floor((x * 60) % 60)).padStart(2, '0')}`; };
  const outcome = ['served', 'hung up', 'turned away'];
  return 'item,station,arrived,started,finished,wait_minutes,outcome\n' + log.map(([id, i, a, s, f, o]) => [id, JSON.stringify(model.stations[i].name), clock(a - warmup), o ? '' : clock(s - warmup), clock(f - warmup), (s - a).toFixed(2), outcome[o]].join(',')).join('\n') + '\n';
}

// ---------------------------------------------------------------- hiring

// Staff-hours a day become people: each person gives so many productive hours, less the share lost to leave,
// training and meetings.
export const peopleFor = (staffHours, { hoursPerPerson = 7.5, shrinkage = 0.3 } = {}) => staffHours / (hoursPerPerson * (1 - shrinkage));

// A month-by-month hiring sequence. Today's people leave at the monthly rate; a new hire starts `lead` months
// after the decision and reaches full output linearly over `ramp` months; need grows by `growth` a month. Each
// month's starters are sized so that, once ramped, capacity meets the need of that later month. Months in which
// capacity still falls short (a hire cannot be ready in time) are reported, not hidden.
export function hiringPlan({ need, current, months = 12, growth = 0, attrition = 0.02, ramp = 3, lead = 1 }) {
  const rampAt = age => age < 0 ? 0 : ramp <= 0 ? 1 : Math.min(1, (age + 1) / ramp), alive = age => Math.pow(1 - attrition, Math.max(0, age));
  const needAt = m => need * Math.pow(1 + growth, m), cohorts = [], capacity = m => current * alive(m) + cohorts.reduce((s, c) => s + c.n * alive(m - c.start) * rampAt(m - c.start), 0);
  for (let s = lead; s < months; s++) {
    const ready = Math.min(months - 1, s + Math.max(0, Math.ceil(ramp) - 1)), short = needAt(ready) - capacity(ready);
    if (short > 1e-9) cohorts.push({ start: s, n: Math.ceil(short / (alive(ready - s) * rampAt(ready - s)) - 1e-9) });
  }
  const rows = Array.from({ length: months }, (_, m) => { const cap = capacity(m), n = needAt(m); return { month: m, need: n, capacity: cap, starts: cohorts.find(c => c.start === m)?.n ?? 0, decide: cohorts.find(c => c.start === m + lead)?.n ?? 0, short: Math.max(0, n - cap) }; });
  return { rows, hires: cohorts.reduce((s, c) => s + c.n, 0), shortMonths: rows.filter(r => r.short > 0.05).map(r => r.month) };
}
