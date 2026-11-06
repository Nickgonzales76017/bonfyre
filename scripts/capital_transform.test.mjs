import { test, expect } from 'bun:test';
import { applyStep, compileCapital, evaluate, openingState, pureJob, search } from './capital_transform.mjs';

const base = () => openingState([], []);

test('an unknown cash step never manufactures cash; vendor terms move only schedule', () => {
  const state = base();
  const [vendor] = applyStep(state, { transform: 'vendor-terms', cash_delta: '0', monthly_obligation_delta: '40', provides: 'schedule-relief' });
  expect(vendor.cash).toBe(0);
  expect(vendor.monthly_obligation).toBe(40);
  expect(vendor.tokens.has('schedule-relief')).toBe(true);
  const [unknown] = applyStep(vendor, { transform: 'grant', cash_delta: 'UNKNOWN' });
  expect(unknown.cash_known).toBe(false);
  expect(evaluate([{ transform: 'grant', cash_delta: 'UNKNOWN' }], state).end_cash).toBeNull();
});

test('rights conflicts and consumed tokens make order material', () => {
  const state = base(), transforms = [
    { transform: 'lease', requires: 'collateral-free', consumes: 'collateral-free', grants_rights: 'first-lien-equipment', cash_delta: '0', cost_usd: '0', reversibility: 'costly' },
    { transform: 'debt', requires: 'collateral-free', conflicts_with_rights: 'first-lien-equipment', cash_delta: 'UNKNOWN', cost_usd: '10', reversibility: 'irreversible' },
  ];
  expect(evaluate(transforms, state).admissible).toBe(false);
  expect(evaluate([...transforms].reverse(), state).admissible).toBe(true);
  const [admitted, refused] = search(transforms, state, 2);
  expect(admitted.length).toBe(3);
  expect(refused.length).toBe(1);
});

test('capital receipt separates structural counterfactual from reachable and exposes declaration fingerprint', () => {
  const transforms = [
    { transform: 'grant', status: 'published', requires: 'approved', cash_delta: 'UNKNOWN', cost_usd: '1', reversibility: 'reversible', grants_rights: 'restricted-use' },
    { transform: 'approve', status: 'published', provides: 'approved', cash_delta: '0', cost_usd: '0', reversibility: 'reversible' },
    { transform: 'debt', status: 'published', requires: 'collateral-free', conflicts_with_rights: 'restricted-use', cash_delta: 'UNKNOWN', cost_usd: '2', reversibility: 'costly' },
  ];
  const receipt = compileCapital({ institution: 'x', transforms, requirements: [], fundingRoutes: [], budget: [{ amount: '100' }], depth: 2, now: new Date('2026-09-30T00:00:00Z') });
  expect(receipt.effect_class).toBe('PURE');
  expect(receipt.declared.transformations).toBe(3);
  expect(receipt.declared.known_cash_deltas).toEqual([0]);
  expect(receipt.target.amount).toBe(100);
  expect(receipt.known_to_reach_target).toBe(0);
  expect(receipt.reach.find(r => r.transform === 'grant').via).toEqual(['approve', 'grant']);
  expect(receipt.order_sensitive.some(r => r.admissible.join('>') === 'debt>grant')).toBe(false);
  const [admissible] = search(transforms, base(), 2);
  expect(pureJob(admissible).effect_class).toBe('PURE');
});

test('bounded enumeration refuses accidental factorial explosion', () => {
  expect(() => search(Array.from({ length: 6 }, (_, i) => ({ transform: String(i), cash_delta: 0 })), base(), 6, 10)).toThrow('path limit');
});
