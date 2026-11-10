import { test, expect } from 'bun:test';
import { commonCrawlPolicy, discoverCommonCrawl } from '../tools/dork_common_crawl.mjs';

test('index dork emits only exact observed-index candidates', async () => {
  const policy = { ...commonCrawlPolicy(), dorks: [{ domain: '*.org/*', wants: ['feed'] }], default_pause_ms: 0 };
  const queried = [];
  const fetcher = async url => {
    queried.push(url);
    if (url.endsWith('collinfo.json')) return { status: 200, text: async () => '[{"id":"CC-MAIN-TEST"}]' };
    return { status: 200, text: async () => [
      { url: 'https://www.example.org/feed.xml', status: '200', 'mime-detected': 'application/rss+xml' },
      { url: 'https://www.example.org/feed.xml', status: '200' },
      { url: 'https://example.org/photo.jpg', status: '200' },
      { url: 'https://other.org/feed', status: '404' },
    ].map(row => JSON.stringify(row)).join('\n') };
  };
  const result = await discoverCommonCrawl({}, policy, fetcher);
  expect(result.summary).toMatchObject({ dorks_asked: 1, collection: 'CC-MAIN-TEST',
    addresses_listed: 1, publishers: 1 });
  expect(result.rows).toEqual([{ root: 'example.org', address: 'https://www.example.org/feed.xml',
    protocol: 'rss', title: '', truth_level: 'observed_index_candidate',
    discovered_from: 'listed in the CC-MAIN-TEST web index as application/rss+xml' }]);
  expect(queried).toHaveLength(2);
  expect(queried[1]).toContain('CC-MAIN-TEST-index');
});

test('missing collection leaves no output or inferred source', async () => {
  const result = await discoverCommonCrawl({}, commonCrawlPolicy(), async () => ({ status: 503 }));
  expect(result).toMatchObject({ rows: [], failed: true, summary: { asked: 0 } });
});
