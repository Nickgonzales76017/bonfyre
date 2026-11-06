import { test, expect } from 'bun:test';
import { expandPublishers, publisherHost } from '../tools/expand_publishers_wikidata.mjs';

test('official-site expansion preserves host identity and candidate truth', async () => {
  const calls = [];
  const fetcher = async (url, options) => {
    calls.push({ url, agent: options.headers['User-Agent'] });
    return { ok: true, json: async () => ({ results: { bindings: [
      { site: { value: 'https://www.Example.org/news' }, item: { value: 'https://www.wikidata.org/entity/Q1' },
        itemLabel: { value: 'Example' } },
      { site: { value: 'https://example.org/other' }, item: { value: 'https://www.wikidata.org/entity/Q2' } },
      { site: { value: 'https://invalid-host' }, item: { value: 'https://www.wikidata.org/entity/Q3' } },
    ] } }) };
  };
  const result = await expandPublishers({ classes: ['newspaper', 'museum', 'not-a-class'], pauseMs: 0 },
    undefined, fetcher);
  expect(result.summary).toMatchObject({ publishers: 1, classes_asked: 3,
    classes_refused: { 'not-a-class': 'not a class this tool knows' } });
  expect(result.rows).toEqual([{ root: 'example.org', name: 'Example', identity: 'Q1',
    discovered_from: 'Wikidata records https://www.Example.org/news as the official website of this newspaper' }]);
  expect(calls).toHaveLength(2);
  expect(new URL(calls[0].url).searchParams.get('query')).toContain('wd:Q11032');
  expect(publisherHost('https://user:pass@WWW.Example.org:444/data')).toBe('example.org');
  expect(publisherHost('not a url')).toBe('');
});
