import { test, expect } from 'bun:test';
import { publisherGrammar, publisherGrammarPolicy } from '../tools/publisher_grammar_policy.mjs';
import { channelsFromBody } from '../tools/expand_publisher_lattice.mjs';
import { grammarOf } from '../tools/sweep_root_grammars.mjs';

test('one authored grammar supplies both sweep and retained profiles', () => {
  const policy = publisherGrammarPolicy();
  const wordpress = publisherGrammar('wordpress');
  expect(policy.schema).toBe('bonfyre.publisher-grammar-policy.v1');
  expect(wordpress.sweep.map(route => route.path)).toContain('/feed/');
  expect(wordpress.retained.map(route => route.path)).toContain('/feed/');
  expect(wordpress.retained.some(route => route.material === 'photo')).toBe(true);
  expect(grammarOf('<meta name="generator" content="WordPress">')[0]).toBe(wordpress.id);
  const result = channelsFromBody(Buffer.from('<meta name="generator" content="WordPress">'),
    'source:one', 'https://publisher.test/');
  expect(result.rows.find(row => row.address === 'https://publisher.test/feed/'))
    .toMatchObject({ protocol: 'rss', material: 'publication', truth_level: 'protocol_derived_candidate' });
  expect(result.rows.every(row => row.state === 'shadow')).toBe(true);
});

test('grammar routes remain unique and only candidate-producing', () => {
  const policy = publisherGrammarPolicy();
  expect(new Set(policy.grammars.map(grammar => grammar.id)).size).toBe(policy.grammars.length);
  for (const grammar of policy.grammars) {
    for (const profile of ['sweep', 'retained']) {
      const routes = grammar[profile];
      expect(new Set(routes.map(route => route.path)).size).toBe(routes.length);
      expect(routes.every(route => route.path && route.protocol)).toBe(true);
    }
  }
  expect(policy.truth).toContain('not_observed_endpoints_or_admission');
});
