import { test, expect } from 'bun:test';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { record, run, score } from '../closure-outcome.mjs';

const before = { domain: 'd', subject: 's', closed_at_fixed_point: ['a'], resolvers_admitted: 1, closure_hypotheses: [{ condition: 'b', gain: 2 }] };

test('recorded claim is scored only against same subject and a changed closure', () => {
  const claim = record(before, 't0');
  expect(claim.predictions).toEqual({ b: 2 });
  const unchanged = score(claim, before, 't1');
  expect(unchanged.nothing_changed).toBe(true);
  expect(unchanged.verdict).toContain('not evidence');
  const changed = score(claim, { ...before, closed_at_fixed_point: ['a', 'b'], resolvers_admitted: 3 }, 't2');
  expect(changed.scored).toEqual([{ condition: 'b', predicted_gain: 2, was_predicted: true, note: 'closed, and the prediction said it would unlock 2 resolver(s)' }]);
  expect(changed.resolver_delta).toBe(2);
  expect(() => score(claim, { ...before, subject: 'other' })).toThrow('different subject');
  const ledger = mkdtempSync(join(tmpdir(), 'bonfyre-outcome-'));
  const saved = run({ state: before, ledger });
  expect(saved.path).toEndWith('.json');
  expect(run({ state: before, ledger, scoring: true }).result.nothing_changed).toBe(true);
});
