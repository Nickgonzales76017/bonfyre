import { test, expect } from 'bun:test';
import { canonical, deformations, loadPolicy, measure } from '../deformation.mjs';

test('policy folds attribution and host case without folding path case or non-root slash', () => {
  const policy = loadPolicy();
  expect(canonical('HTTPS://EXAMPLE.COM:443/Feed?utm_source=x&b=2&a=1#section', policy)).toBe('https://example.com/Feed?a=1&b=2');
  expect(canonical('https://example.com/Feed', policy)).not.toBe(canonical('https://example.com/feed', policy));
  expect(canonical('https://example.com/Feed', policy)).not.toBe(canonical('https://example.com/Feed/', policy));
  const family = deformations('https://example.com/Feed', policy);
  expect(family.find(([name]) => name === 'decoy_trailing_slash')[1]).toBe('https://example.com/Feed/');
  expect(family.find(([name]) => name === 'decoy_other_path')[1]).toBe('https://example.com/Feed/definitely-not-the-same-resource');
});

test('false merges and misses are never netted against closure', async () => {
  const policy = loadPolicy();
  const result = await measure([{ subject_id: 'one', url: 'https://example.com/Feed' }], async address => address.includes('definitely-not') ? 1 : address === 'https://example.com/Feed' ? 1 : 0, { policy });
  expect(result.false_merges).toBe(1);
  expect(result.closed).toBe(1);
  expect(result.missed).toBeGreaterThan(0);
  expect(result.rows.find(row => row.deformation === 'decoy_other_path').verdict).toBe('false_merge');
});
