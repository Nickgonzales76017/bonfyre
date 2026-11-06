// What this does not prove:
// - a dependency is replaced by an object built in this test, so the real one is not exercised.
import { test, expect } from 'bun:test';
import { createHash } from 'node:crypto';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { discoverForeign, headDeclarations } from '../foreign-discover.mjs';
import { membraneDescriptors, refreshMembrane } from '../membrane-store.mjs';

function mockObserver(directory, replies, seen) {
  let index = 0;
  return async address => {
    seen.push(address);
    const reply = replies[new URL(address).pathname];
    if (reply === undefined) return { receipt: { policy_decision: 'allow', http_status: 404 } };
    if (reply === false) return { receipt: { policy_decision: 'robots_disallow', http_status: 0 } };
    const body = Buffer.from(reply), bodyPath = join(directory, `${index++}.body`);
    writeFileSync(bodyPath, body);
    return { bodyPath, receiptPath: `${bodyPath}.receipt.json`, receipt: {
      policy_decision: 'allow', http_status: 200, body_bytes: body.length,
      content_sha256: createHash('sha256').update(body).digest('hex'), etag: '"latest"' } };
  };
}

test('public descriptor observations remain candidate-only, with no implicit authority', async () => {
  const directory = mkdtempSync(join(tmpdir(), 'bonfyre-discover.'));
  try {
    const seen = [];
    const observe = mockObserver(directory, {
      '/robots.txt': 'User-agent: *\nAllow: /\n',
      '/': '<!doctype html><title>Publisher</title>',
      '/.well-known/mcp.json': '{"name":"fixture MCP"}',
      '/.well-known/agent.json': '{"name":"fixture agent"}',
      '/.well-known/oauth-authorization-server': '{"issuer":"fixture"}',
      '/.well-known/assetlinks.json': '[]',
      '/auth.md': '# Registration guide\n',
    }, seen);
    const passport = await discoverForeign('https://publisher.test', {
      observe, clock: () => new Date('2026-09-29T12:00:00Z') });
    const families = new Set(passport.protocol_descriptors.map(row => row.family));
    for (const family of ['mcp-descriptor-candidate', 'a2a-agent-card-candidate',
      'oauth-server-candidate', 'android-app-link-candidate', 'auth-md-candidate'])
      expect(families.has(family)).toBe(true);
    expect(passport.protocol_descriptors.every(row => row.truth_level === 'declared_descriptor'))
      .toBe(true);
    expect(passport.best_carrier).toBe('conditional-http');
    expect(passport.conditional.etag).toBe('"latest"');
    expect(passport).not.toHaveProperty('provider');
    expect(seen[0]).toBe('https://publisher.test/robots.txt');
  } finally { rmSync(directory, { recursive: true, force: true }); }
});

test('robots refusal prevents all downstream probes', async () => {
  const directory = mkdtempSync(join(tmpdir(), 'bonfyre-discover-refusal.'));
  try {
    const seen = [], observe = mockObserver(directory, { '/robots.txt': false }, seen);
    const passport = await discoverForeign('https://publisher.test', { observe });
    expect(passport).toMatchObject({ robots_allows_us: false, best_carrier: 'none',
      protocol_descriptors: [] });
    expect(seen).toEqual(['https://publisher.test/robots.txt']);
  } finally { rmSync(directory, { recursive: true, force: true }); }
});

test('feed and sitemap candidates require verified carrier bodies; markup declarations retain source order', async () => {
  const directory = mkdtempSync(join(tmpdir(), 'bonfyre-discover-feed.'));
  try {
    const seen = [];
    const observe = mockObserver(directory, {
      '/robots.txt': 'User-agent: *\nAllow: /\nSitemap: https://publisher.test/map.xml\n',
      '/': '<link rel="hub" href="https://hub.test/"><link rel="alternate" type="application/rss+xml" href="/feed.xml">',
      '/feed.xml': '<rss><channel><item><guid>x</guid></item></channel></rss>',
      '/sitemap.xml': '<html>challenge</html>',
    }, seen);
    const passport = await discoverForeign('https://publisher.test', { observe,
      includeObservation: true });
    expect(passport.best_carrier).toBe('websub');
    expect(passport.feeds).toEqual([{ url: 'https://publisher.test/feed.xml', type: 'application/rss+xml' }]);
    expect(passport.sitemaps).toEqual(['https://publisher.test/map.xml']);
    expect(passport.notes.some(note => note.includes('not a sitemap'))).toBe(true);
    expect(passport.observed_url).toBe('https://publisher.test/feed.xml');
    expect(passport.observation.content_sha256).toHaveLength(64);
    expect(seen.filter(address => address.endsWith('/feed.xml'))).toHaveLength(1);
    expect(headDeclarations('<link rel="canonical" href="/story">', 'https://publisher.test/').canonical)
      .toBe('https://publisher.test/story');
  } finally { rmSync(directory, { recursive: true, force: true }); }
});

test('descriptor candidates cross discovery-to-membrane storage without promotion', async () => {
  const directory = mkdtempSync(join(tmpdir(), 'bonfyre-discover-store.'));
  try {
    const observe = mockObserver(directory, {
      '/robots.txt': 'User-agent: *\nAllow: /\n',
      '/': '<!doctype html><title>Publisher</title>',
      '/.well-known/mcp.json': '{"name":"fixture MCP"}',
      '/auth.md': '# Registration guide\n',
    }, []);
    const dbPath = join(directory, 'membrane.db');
    const stored = await refreshMembrane(['publisher.test'], { dbPath,
      discover: host => discoverForeign(host, { observe }) });
    expect(stored).toMatchObject([{ root: 'https://publisher.test/', carrier: 'conditional-http' }]);
    const retained = membraneDescriptors(dbPath);
    expect(retained.descriptors.map(row => row.family).toSorted())
      .toEqual(['auth-md-candidate', 'mcp-descriptor-candidate']);
    expect(retained.descriptors.every(row => row.truth_level === 'declared_descriptor')).toBe(true);
    expect(retained.forbidden_inference).toContain('not a live endpoint');
  } finally { rmSync(directory, { recursive: true, force: true }); }
});
