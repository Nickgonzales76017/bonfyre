import { test, expect } from 'bun:test';
import { analyzeReadership } from '../machine-readership.mjs';

test('access-log representation demand remains observational, not bot identity', () => {
  const lines = [
    '127.0.0.1 - - [29/Sep/2026:12:00:00 +0000] "GET /robots.txt HTTP/1.1" 404 20 "-" "Mozilla/5.0 (compatible; Googlebot/2.1)"',
    '127.0.0.1 - - [29/Sep/2026:12:00:01 +0000] "GET /feed.xml?x=1 HTTP/1.1" 404 20 "-" "Feedly/1.0"',
    '127.0.0.1 - - [29/Sep/2026:12:00:02 +0000] "GET /story HTTP/1.1" 200 400 "-" "Mozilla/5.0"',
  ];
  const result = analyzeReadership([lines.join('\n')]);
  expect(result).toMatchObject({ requests_read: 3, declared_machine_requests: 2,
    discovery_misses: [{ path: '/feed.xml', count: 1 }, { path: '/robots.txt', count: 1 }] });
  expect(result.by_family).toEqual([{ family: 'feed', count: 1 }, { family: 'search', count: 1 }]);
  expect(result.by_agent.find(row => row.agent === 'googlebot')).toMatchObject({ count: 1,
    statuses: [{ status: '404', count: 1 }] });
  expect(result.cannot_infer).toContain('not a credential');
});
