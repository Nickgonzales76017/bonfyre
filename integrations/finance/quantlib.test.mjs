import { test } from 'bun:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { comparePaymentOptions } from './quantlib.mjs';

const source = resolve(import.meta.dir, '../../packs/aurekai-work-samples/twc-quantlib-input-20261009.json');

test('QuantLib reprices the retained TWC supplier alternatives and preserves their sources', () => {
  const input = JSON.parse(readFileSync(source, 'utf8'));
  const output = comparePaymentOptions(input);
  assert.equal(output.provider, 'QuantLib');
  assert.equal(output.version, '1.43');
  assert.equal(output.results.length, 4);
  for (const result of output.results) {
    const supplied = input.alternatives.find(row => row.id === result.id);
    assert.ok(Math.abs(Number(result.repriced_upfront_amount) - Number(supplied.upfront_amount)) < 0.000001);
    assert.deepEqual(result.source_refs, supplied.source_refs);
    assert.equal(result.payment_dates.length, 12);
    assert.ok(result.modified_duration_years > 0);
    assert.ok(result.local_pv_change_per_basis_point < 0);
  }
  const by = Object.fromEntries(output.results.map(row => [row.id, row]));
  assert.ok(by['gold-advance'].effective_annual_indifference_rate > by['gold-arrears'].effective_annual_indifference_rate);
  assert.ok(by['diamond-advance'].effective_annual_indifference_rate > by['diamond-arrears'].effective_annual_indifference_rate);
  assert.equal(output.effect, 'analysis_only');
  assert.equal(output.payment_sent, false);
});

test('missing price or provenance does not become a computed financing offer', () => {
  const input = JSON.parse(readFileSync(source, 'utf8'));
  const absentPrice = structuredClone(input); absentPrice.alternatives[0].upfront_amount = null;
  assert.throws(() => comparePaymentOptions(absentPrice), /source price/);
  const absentSource = structuredClone(input); absentSource.alternatives[0].source_refs = [];
  assert.throws(() => comparePaymentOptions(absentSource), /price evidence/);
});
