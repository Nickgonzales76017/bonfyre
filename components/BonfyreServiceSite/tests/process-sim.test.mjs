import { test, expect } from 'bun:test';
import { peopleFor, hiringPlan, erlangC, stationFormula, flows, simulate, run, problems, staffingFor, readTimestamps, readDurations, compare, logCsv, rng, draw, moments } from '../process-sim.mjs';

const single = (over = {}) => ({ entry: 'desk', arrivals: { kind: 'constant', perHour: 50 },
  stations: [{ id: 'desk', name: 'Desk', servers: 5, service: { kind: 'exponential', mean: 5 }, routes: [{ to: 'exit', p: 1 }], target: 1, ...over }] });

test('Erlang C matches published values', () => {
  // 10 erlangs on 11 agents: P(wait) = 0.6821 (standard tables).
  expect(erlangC(11, 10)).toBeCloseTo(0.6821, 3);
  expect(erlangC(2, 1)).toBeCloseTo(1 / 3, 6);
  expect(erlangC(1, 0.5)).toBeCloseTo(0.5, 6);
  expect(erlangC(3, 3)).toBe(1);
});

test('the simulation agrees with M/M/c inside its interval', () => {
  const m = single(), f = stationFormula({ lambda: 50 / 60, mean: 5, cv: 1, servers: 5, target: 1 });
  const r = simulate(m, { days: 3, warmup: 240, replications: 12, seed: 3 });
  const s = r.stations[0];
  expect(Math.abs(s.wait.mean - f.wq)).toBeLessThan(Math.max(s.wait.half * 1.5, 0.05 * f.wq));
  expect(Math.abs(s.utilization.mean - f.rho)).toBeLessThan(0.02);
  expect(Math.abs(s.serviceLevel.mean - f.serviceLevel)).toBeLessThan(0.03);
});

test('a fixed seed repeats exactly and a different seed does not', () => {
  const a = run(single(), { seed: 9 }), b = run(single(), { seed: 9 }), c = run(single(), { seed: 10 });
  expect(a.stations[0].wait).toBe(b.stations[0].wait);
  expect(a.stations[0].wait).not.toBe(c.stations[0].wait);
});

test('routes split and loop work in the right proportions', () => {
  const m = { entry: 'a', arrivals: { kind: 'constant', perHour: 30 }, stations: [
    { id: 'a', name: 'Check', servers: 4, service: { kind: 'exponential', mean: 2 }, routes: [{ to: 'b', p: 0.25 }, { to: 'exit', p: 0.75 }] },
    { id: 'b', name: 'Fix', servers: 3, service: { kind: 'exponential', mean: 4 }, routes: [{ to: 'a', p: 0.2 }, { to: 'exit', p: 0.8 }] }] };
  const { rate } = flows(m, 0.5); // λa = 0.5 + 0.2 λb, λb = 0.25 λa
  expect(rate.a).toBeCloseTo(0.5 / (1 - 0.05), 6);
  expect(rate.b).toBeCloseTo(0.25 * rate.a, 6);
  const r = simulate(m, { days: 4, replications: 6 });
  expect(r.stations[1].arrivals.mean / r.stations[0].arrivals.mean).toBeCloseTo(0.25, 1);
});

test('impatient callers hang up and full queues turn work away', () => {
  const m = single({ servers: 2, patience: { kind: 'exponential', mean: 3 } });
  const r = simulate(m, { days: 1, replications: 4 });
  expect(r.stations[0].abandoned.mean).toBeGreaterThan(50);
  const full = simulate(single({ servers: 2, limit: 3 }), { days: 1, replications: 4 });
  expect(full.stations[0].blocked.mean).toBeGreaterThan(50);
  expect(full.stations[0].maxQueue.mean).toBeLessThanOrEqual(3);
});

test('staffing by hour follows the arrival profile and meets the target by formula', () => {
  const perHour = Array.from({ length: 24 }, (_, h) => (h >= 8 && h < 18 ? 120 : 20));
  const m = { entry: 'desk', arrivals: { kind: 'profile', perHour }, stations: [{ id: 'desk', name: 'Desk', servers: 1, service: { kind: 'exponential', mean: 4 }, routes: [{ to: 'exit', p: 1 }], target: 0.5 }] };
  const staff = staffingFor(m, 'desk', 0.8);
  expect(staff[10]).toBeGreaterThan(staff[3]);
  expect(stationFormula({ lambda: 2, mean: 4, cv: 1, servers: staff[10], target: 0.5 }).serviceLevel).toBeGreaterThanOrEqual(0.8);
  expect(stationFormula({ lambda: 2, mean: 4, cv: 1, servers: staff[10] - 1, target: 0.5 }).serviceLevel).toBeLessThan(0.8);
  const sim = simulate({ ...m, stations: [{ ...m.stations[0], servers: staff }] }, { days: 2, replications: 6 });
  expect(sim.stations[0].serviceLevel.mean).toBeGreaterThan(0.75);
});

test('a broken model is described, not run', () => {
  expect(problems(single({ routes: [{ to: 'exit', p: 0.6 }] }))[0]).toMatch(/60%/);
  expect(problems({ ...single(), entry: 'nowhere' })[0]).toMatch(/arrives/);
  const loop = { entry: 'a', arrivals: { kind: 'constant', perHour: 1 }, stations: [{ id: 'a', name: 'A', servers: 1, service: { kind: 'fixed', mean: 1 }, routes: [{ to: 'a', p: 1 }] }] };
  expect(problems(loop)[0]).toMatch(/loops forever/);
  expect(() => simulate(loop)).toThrow();
});

test('lognormal draws keep their mean and spread', () => {
  const r = rng(4), xs = Array.from({ length: 40000 }, () => draw({ kind: 'lognormal', mean: 6, cv: 0.5 }, r));
  const m = moments({ kind: 'empirical', samples: xs });
  expect(m.mean).toBeCloseTo(6, 1); expect(m.cv).toBeCloseTo(0.5, 1);
});

test('people\'s files: timestamps become an hourly profile, durations a distribution', () => {
  const csv = 'created_at,ref\n2026-10-06T08:05:00,a\n2026-10-06T08:40:00,b\n2026-10-06T09:10:00,c\nnot a date,d\n2026-10-07T08:30:00,e\n';
  const t = readTimestamps(csv);
  expect(t.days).toBe(2); expect(t.skipped).toBe(1); expect(t.perHour[8]).toBe(1.5); expect(t.perHour[9]).toBe(0.5);
  const d = readDurations('handle\n4.5\n0:06:30\nx\n3');
  expect(d.samples).toEqual([4.5, 6.5, 3]); expect(d.skipped).toBe(1);
});

test('comparison counts only differences beyond the intervals', () => {
  const a = simulate(single(), { replications: 8 }), b = simulate(single({ servers: 6 }), { replications: 8 }), same = simulate(single(), { replications: 8 });
  expect(compare(a, b)[0].wait).toBe('better');
  expect(compare(a, same)[0].wait).toBe('same');
  const csv = logCsv(single(), a.log.slice(0, 3));
  expect(csv.split('\n')[0]).toBe('item,station,arrived,started,finished,wait_minutes,outcome');
});

test('staff-hours become people, and a hiring plan covers growth through ramp and attrition', () => {
  expect(peopleFor(331, { hoursPerPerson: 7.5, shrinkage: 0.3 })).toBeCloseTo(63.05, 1);
  const flat = hiringPlan({ need: 50, current: 50, months: 12, attrition: 0, growth: 0 });
  expect(flat.hires).toBe(0); expect(flat.shortMonths).toEqual([]);
  const p = hiringPlan({ need: 60, current: 50, months: 12, attrition: 0.02, growth: 0.01, ramp: 3, lead: 1 });
  // A hire decided now starts next month and is only fully productive two months later, so the first months fall short.
  expect(p.shortMonths.slice(0, 3)).toEqual([0, 1, 2]);
  expect(p.rows.slice(4).every(r => r.short < 0.05)).toBe(true);
  expect(p.rows[0].starts).toBe(0); expect(p.rows[1].starts).toBeGreaterThan(0); expect(p.rows[0].decide).toBe(p.rows[1].starts);
  expect(p.hires).toBeGreaterThan(10);
});
