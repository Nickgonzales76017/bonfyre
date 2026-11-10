import { test, expect } from 'bun:test';
import { loadVocabularies, phrase, target } from '../residual-target.mjs';

test('one authored vocabulary feeds typed targets without repeating condition words', () => {
  const vocabularies = loadVocabularies();
  expect(phrase('building-permit', vocabularies.voices.find(row => row.kind === 'authority').phrasings)).not.toContain('building permit permit');
  const state = {
    domain: 'd', subject: 's', closed_at_fixed_point: ['a'], unknown_not_counted_as_blocked: ['permit'],
    resolvers_blocked: [{ resolver: 'r', waiting_on: ['permit'] }],
    closure_hypotheses: [{ condition: 'permit', gain: 2 }, { condition: 'a', gain: 0 }],
    condition_kinds: { permit: 'authority' }, condition_resolvable_by: {},
  };
  const result = target(state, vocabularies, 'now');
  expect(result.targets).toHaveLength(1);
  expect(result.targets[0].capability_that_would_answer).toBe('authority.lookup');
  expect(result.targets[0].blocks_resolvers).toEqual(['r']);
  expect(result.rung_reached).toBe('named');
});
