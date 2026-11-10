import { test, expect } from 'bun:test';
import { unitCost, demand, curve, breakEven, usage, bill, tierOutcome, vanWestendorp, readSurvey, proposal } from '../pricing-desk.mjs';

const cost = { computeSeconds: 2, computeRate: 0.0005, dataCost: 0.004, supportMinutes: 0.1, supportRate: 60, fixedMonthly: 300 };

test('cost to serve adds compute, data and support time', () => {
  expect(unitCost(cost)).toBeCloseTo(0.001 + 0.004 + 0.1, 9);
  expect(breakEven(0.205, cost)).toBe(3000);
  expect(breakEven(0.1, cost)).toBe(Infinity);
});

test('demand passes through its reference point; the best price matches the closed form', () => {
  const m = { refPrice: 1, refVolume: 1000, elasticity: 2 };
  expect(demand(1, m)).toBe(1000); expect(demand(2, m)).toBe(250);
  const c = curve({ ...cost, fixedMonthly: 0 }, m, { from: 0.11, to: 1, steps: 4000 });
  expect(c.exact).toBeCloseTo(0.21, 6); expect(c.best.price).toBeCloseTo(0.21, 2);
  expect(demand(1.5, { ...m, shape: 'linear', choke: 3 })).toBe(750);
});

test('customers take the cheapest tier for their use; pay-per-use catches the light users', () => {
  const tiers = [{ name: 'Starter', fee: 20, included: 100, overage: 0.25 }, { name: 'Team', fee: 120, included: 1000, overage: 0.12 }];
  expect(bill(250, tiers[0])).toBe(57.5); expect(bill(250, tiers[1])).toBe(120);
  const out = tierOutcome([10, 90, 250, 900, 4000], tiers, { perUse: 0.3, unit: 0.1 });
  expect(out.rows.map(r => r.customers)).toEqual([2, 2, 1]);
  expect(out.rows[2].revenue).toBeCloseTo(3, 9);
  expect(out.revenue).toBeCloseTo(3 + 20 + 57.5 + 120 + (120 + 3000 * 0.12), 6);
  const u = usage({ customers: 4000, median: 100, p90: 600, seed: 3 }), s = [...u].sort((a, b) => a - b);
  expect(s[2000]).toBeGreaterThan(85); expect(s[2000]).toBeLessThan(115); expect(s[3600]).toBeGreaterThan(500); expect(s[3600]).toBeLessThan(700);
});

test('Van Westendorp finds the acceptable range and refuses inconsistent answers', () => {
  const csv = 'too cheap,cheap,expensive,too expensive\n' + [[5, 10, 20, 30], [6, 12, 22, 35], [4, 9, 18, 28], [8, 14, 25, 40], [5, 11, 21, 33], [7, 13, 24, 36], [30, 10, 5, 1]].map(r => r.join(',')).join('\n');
  const vw = vanWestendorp(readSurvey(csv));
  expect(vw.usable).toBe(6); expect(vw.dropped).toBe(1);
  expect(vw.low).toBeLessThan(vw.high); expect(vw.low).toBeGreaterThan(8); expect(vw.high).toBeLessThan(26);
  expect(vanWestendorp(readSurvey(csv).slice(0, 3)).usable).toBe(3);
  expect(() => readSurvey('a,b\n1,2')).toThrow(/four columns/);
});

test('a proposal names its arithmetic and sets nothing', () => {
  const p = proposal({ id: 'route-frontier' }, cost, { refPrice: 1, refVolume: 1000, elasticity: 2 }, 0.5, []);
  expect(p).toMatchObject({ capability: 'route-frontier', proposed_unit_price: 0.5, set_by: null });
  expect(p.margin_per_unit).toBeCloseTo(0.395, 6); expect(p.cannot_infer).toMatch(/not a price/);
});
