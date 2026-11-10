import { expect, test } from 'bun:test';
import { createHash } from 'node:crypto';
import { canonicalJson, digestValue, jsonFloat, parsedDocument } from '../canonical_json.mjs';
const hash = text => createHash('sha256').update(text).digest('hex');

test('historical Unicode policies are explicit and retain sorted codepoint keys', () => {
  const value = { '\u{10000}': '雪', '\ue000': 'é', timestamp: jsonFloat(1000) };
  expect(canonicalJson(value)).toBe('{"timestamp":1000.0,"\\ue000":"\\u00e9","\\ud800\\udc00":"\\u96ea"}');
  expect(canonicalJson(value, { ascii: false })).toBe('{"timestamp":1000.0,"\ue000":"é","\u{10000}":"雪"}');
  expect(digestValue(value)).not.toBe(digestValue(value, { ascii: false }));
  expect(JSON.parse(JSON.stringify(value)).timestamp).toBe(1000);
});
test('raw numeric spellings and owner-specific digest fields survive verification', () => {
  const source = '{"x":1.0,"雪":1e-07,"manifest_digest":"not-part-of-body"}';
  expect(parsedDocument(source, { ascii: false, omitKey: 'manifest_digest' }).calculated).toBe(hash('{"x":1.0,"雪":1e-07}'));
  expect(parsedDocument('{"digest":"ordinary","x":-0.0}', { omitKey: null }).calculated).toBe(hash('{"digest":"ordinary","x":-0.0}'));
});
test('strict Unicode and bounded JSON refuse malformed or excessive input', () => {
  expect(() => canonicalJson('\ud800', { ascii: false })).toThrow('surrogate');
  expect(canonicalJson('\ud800')).toBe('"\\ud800"');
  expect(() => parsedDocument(' '.repeat(8 * 1024 * 1024 + 1))).toThrow('budget');
  expect(() => canonicalJson(Infinity)).toThrow('non-finite');
  expect(() => parsedDocument('[]')).toThrow('object');
  expect(() => parsedDocument('{"overflow":1e400}')).toThrow('non-finite');
});
test('Python default-space canonical mode preserves float exponents and raw historical spellings', () => {
  const value = { z: [jsonFloat(1e-7), 'é'], a: jsonFloat(1e16) };
  const expected = '{"a": 1e+16, "z": [1e-07, "\\u00e9"]}';
  expect(canonicalJson(value, { spaced: true })).toBe(expected);
  expect(parsedDocument(`{"z":[1e-07,"é"],"a":1e+16,"digest":"x"}`, { spaced: true }).calculated).toBe(hash(expected));
  expect(() => canonicalJson(value, { spaced: 'yes' })).toThrow('mode');
});
