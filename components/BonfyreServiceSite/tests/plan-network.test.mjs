import { test, expect } from 'bun:test';
import { problems, cpm, simulate, levelled, pertMean, addWorkdays, workdaysBetween, fromCsv, toCsv, findCycle, rng, pertDraw } from '../plan-network.mjs';

const t = (id, o, m, p, deps = [], owner = '') => ({ id, name: id.toUpperCase(), optimistic: o, likely: m, pessimistic: p, deps, owner });
// The textbook network: A(3) → B(4), A → C(2) → D(5), B,D → E(1). Critical path A C D E = 11.
const plan = [t('a', 2, 3, 6), t('b', 3, 4, 5, ['a']), t('c', 1, 2, 3, ['a']), t('d', 4, 5, 9, ['c']), t('e', 1, 1, 1, ['b', 'd'])];

test('the critical path, slack and late dates match a hand calculation', () => {
  const r = cpm(plan);
  expect(r.end).toBe(11); expect(r.critical).toEqual(['a', 'c', 'd', 'e']);
  expect(r.rows.b).toMatchObject({ es: 3, ef: 7, ls: 6, lf: 10, slack: 3, critical: false });
  expect(r.rows.e).toMatchObject({ es: 10, ef: 11, slack: 0 });
});

test('a loop and a missing dependency are named, not scheduled', () => {
  expect(problems([t('a', 1, 1, 1, ['b']), t('b', 1, 1, 1, ['a'])])[0]).toMatch(/loop: A → B → A|loop: B → A → B/);
  expect(problems([t('a', 1, 1, 1, ['zz'])])[0]).toMatch(/not in the plan/);
  expect(problems([t('a', 3, 2, 1)])[0]).toMatch(/best ≤ likely ≤ worst/);
  expect(findCycle(plan)).toBeNull();
});

test('PERT draws stay in range with the PERT mean', () => {
  const r = rng(5), xs = Array.from({ length: 30000 }, () => pertDraw(2, 3, 10, r));
  expect(Math.min(...xs)).toBeGreaterThanOrEqual(2); expect(Math.max(...xs)).toBeLessThanOrEqual(10);
  expect(xs.reduce((a, b) => a + b, 0) / xs.length).toBeCloseTo(pertMean({ optimistic: 2, likely: 3, pessimistic: 10 }), 1);
});

test('simulation: finish dates spread past the likely plan, the long path dominates, and a seed repeats', () => {
  const s = simulate(plan, { runs: 3000, seed: 2 }), again = simulate(plan, { runs: 3000, seed: 2 });
  expect(s.p50).toBe(again.p50);
  expect(s.p80).toBeGreaterThan(cpm(plan).end); expect(s.p10).toBeLessThan(s.p95);
  expect(s.criticality.d).toBeGreaterThan(0.85); expect(s.criticality.b).toBeLessThan(0.2);
  expect(s.chanceBy(cpm(plan).end)).toBeLessThan(0.5);
  expect(s.histogram.reduce((a, b) => a + b, 0)).toBe(3000);
});

test('one person doing two parallel tasks pushes the end out by the shorter one', () => {
  const shared = [t('a', 1, 1, 1), t('b', 4, 4, 4, ['a'], 'sam'), t('c', 2, 2, 2, ['a'], 'sam'), t('d', 1, 1, 1, ['b', 'c'])];
  expect(cpm(shared).end).toBe(6);
  const l = levelled(shared);
  expect(l.end).toBe(8); expect(l.delay).toBe(2);
  expect(l.rows.c.start).toBe(5); expect(l.rows.c.waited).toBe(4);
});

test('working days skip weekends; files round-trip', () => {
  expect(addWorkdays('2026-10-16', 1)).toBe('2026-10-19'); // Friday + 1 = Monday
  expect(addWorkdays('2026-10-19', 5)).toBe('2026-10-26');
  expect(workdaysBetween('2026-10-19', '2026-10-26')).toBe(5);
  const csv = 'id,task,best,likely,worst,depends on,owner\nx,Design,1,2,4,,Ana\ny,Build,"3",5,9,x,Ben\nz,"Test, then ship",1,1,2,x;y,Ana\n';
  const tasks = fromCsv(csv);
  expect(tasks.map(x => [x.id, x.deps, x.owner])).toEqual([['x', [], 'Ana'], ['y', ['x'], 'Ben'], ['z', ['x', 'y'], 'Ana']]);
  expect(problems(tasks)).toEqual([]);
  const out = toCsv(tasks, cpm(tasks), '2026-10-19');
  expect(out.split('\n')[3]).toContain('"Test, then ship"'); expect(out.split('\n')[3]).toContain(',yes');
  expect(() => fromCsv('a,b\n1,2')).toThrow(/task name/);
});
