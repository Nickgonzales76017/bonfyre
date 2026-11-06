import { test, expect } from 'bun:test';
import { decide } from '../closure-spend.mjs';

const state = { domain: 'd', subject: 's', resolver_leverage: [
  { resolver: 'free', class: '', closure_contribution: 3, cost_usd: 0, success_probability: 1, free: true, priced: true },
  { resolver: 'unknown', class: '', closure_contribution: 3, cost_usd: null, success_probability: 1, free: false, priced: false },
  { resolver: 'dear', class: '', closure_contribution: 2, cost_usd: 20, success_probability: 1, free: false, priced: true },
  { resolver: 'cheap', class: '', closure_contribution: 2, cost_usd: 5, success_probability: 0.5, free: false, priced: true },
  { resolver: 'idle', class: '', closure_contribution: 0, cost_usd: 1, success_probability: 1, free: false, priced: true },
] };

test('spend policy refuses free, unknown, idle, and over-ceiling without moving money', () => {
  const result = decide(state, { ceiling: 10, worldValue: 20, now: 'now' });
  expect(result.admitted).toEqual(['cheap']);
  expect(result.admitted_total_usd).toBe(5);
  expect(result.referred_to_a_person).toEqual(['dear']);
  expect(result.money_moved).toBe(false);
  expect(result.decisions.find(row => row.resolver === 'unknown').because).toContain('no declared cost');
});
