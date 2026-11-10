import { test, expect } from 'bun:test';
import { createHash } from 'node:crypto';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { checkSitemap, decodeSitemap, fromRobots, sitemapDatabase } from '../sitemap-delta.mjs';

const namespace = 'http://www.sitemaps.org/schemas/sitemap/0.9';
const a = `<?xml version="1.0"?><urlset xmlns="${namespace}">
  <url><loc>https://site.test/a</loc><lastmod>2026-09-01</lastmod></url>
  <url><loc>https://site.test/b</loc><lastmod>2026-09-01</lastmod></url>
  </urlset>`;
const b = `\uFEFF<?xml version="1.0"?><urlset xmlns="${namespace}">
  <url><loc>https://site.test/a</loc><lastmod>2026-09-29</lastmod></url>
  <url><loc>https://site.test/c</loc></url></urlset>`;
const index = `<?xml version="1.0"?><sitemapindex xmlns="${namespace}">
  <sitemap><loc>https://site.test/pages.xml</loc><lastmod>2026-09-29</lastmod></sitemap>
  </sitemapindex>`;
const fixture = (directory, name, body) => {
  const bodyPath = join(directory, name);
  writeFileSync(bodyPath, body);
  return { bodyPath, receiptPath: join(directory, `${name}.receipt.json`), receipt: {
    content_sha256: createHash('sha256').update(body).digest('hex'), policy_decision: 'allow',
    http_status: 200, body_bytes: Buffer.byteLength(body) } };
};

test('native sitemap decoder distinguishes URL sets from indexes and accepts BOM', () => {
  expect(decodeSitemap(Buffer.from(b))).toEqual({ kind: 'urlset', entries: [
    { loc: 'https://site.test/a', lastmod: '2026-09-29' },
    { loc: 'https://site.test/c', lastmod: '' } ] });
  expect(decodeSitemap(Buffer.from(index))).toEqual({ kind: 'sitemapindex', entries: [
    { loc: 'https://site.test/pages.xml', lastmod: '2026-09-29' } ] });
  expect(() => decodeSitemap(Buffer.from('<html>challenge</html>'))).toThrow();
});

test('receipted sitemap delta captures additions, change, withdrawal, then unchanged', async () => {
  const directory = mkdtempSync(join(tmpdir(), 'bonfyre-sitemap-delta.'));
  try {
    const dbPath = join(directory, 'sitemap.db');
    const observations = [fixture(directory, 'a.xml', a), fixture(directory, 'b.xml', b)];
    const url = 'https://site.test/sitemap.xml';
    let next = 0;
    const observe = async () => observations[Math.min(next++, 1)];
    const options = { dbPath, observe };
    const first = await checkSitemap(url, options);
    expect(first).toMatchObject({ kind: 'urlset', first_run: true, added: 2, changed: 0,
      removed: 0, urls: 2 });
    const second = await checkSitemap(url, options);
    expect(second).toMatchObject({ first_run: false, added: 1, changed: 1, removed: 1, urls: 2 });
    expect(second.sample_removed).toEqual(['https://site.test/b']);
    const third = await checkSitemap(url, options);
    expect(third).toMatchObject({ unchanged: true, digest: observations[1].receipt.content_sha256.slice(0, 16) });
    const db = sitemapDatabase(dbPath);
    try {
      expect(db.query('SELECT checks,unchanged,url_count FROM sitemap_state WHERE sitemap_url=?').get(url))
        .toMatchObject({ checks: 3, unchanged: 1, url_count: 2 });
      expect(db.query('SELECT loc FROM sitemap_urls WHERE sitemap_url=? ORDER BY loc').all(url)
        .map(row => row.loc)).toEqual(['https://site.test/a', 'https://site.test/c']);
    } finally { db.close(); }
  } finally { rmSync(directory, { recursive: true, force: true }); }
});

test('refused, drifted and challenged bodies do not mutate sitemap state', async () => {
  const directory = mkdtempSync(join(tmpdir(), 'bonfyre-sitemap-refusal.'));
  try {
    const dbPath = join(directory, 'sitemap.db'), url = 'https://site.test/sitemap.xml';
    const refused = await checkSitemap(url, { dbPath,
      observe: async () => ({ receipt: { policy_decision: 'refuse', http_status: 403 } }) });
    expect(refused.error).toBe('refuse');
    const drifted = fixture(directory, 'drift.xml', a);
    drifted.receipt.content_sha256 = '0'.repeat(64);
    expect((await checkSitemap(url, { dbPath, observe: async () => drifted })).error)
      .toBe('body_does_not_match_receipt');
    const challenged = fixture(directory, 'challenge.html', '<html>Enable JavaScript</html>');
    expect((await checkSitemap(url, { dbPath, observe: async () => challenged })).error)
      .toBe('challenged');
    const db = sitemapDatabase(dbPath);
    try { expect(db.query('SELECT COUNT(*) AS n FROM sitemap_state').get().n).toBe(0); }
    finally { db.close(); }
  } finally { rmSync(directory, { recursive: true, force: true }); }
});

test('robots sitemap discovery uses the same admitted, hashed observation path', async () => {
  const directory = mkdtempSync(join(tmpdir(), 'bonfyre-sitemap-robots.'));
  try {
    const robots = fixture(directory, 'robots.txt', 'User-agent: *\nSitemap: https://site.test/sitemap.xml\n');
    const sitemap = fixture(directory, 'site.xml', a);
    const calls = [];
    const result = await fromRobots('site.test', { dbPath: join(directory, 'state.db'),
      observe: async url => { calls.push(url); return url.endsWith('robots.txt') ? robots : sitemap; } });
    expect(calls).toEqual(['https://site.test/robots.txt', 'https://site.test/sitemap.xml']);
    expect(result).toMatchObject({ declared_sitemaps: 1,
      results: [{ sitemap: 'https://site.test/sitemap.xml', added: 2 }] });
  } finally { rmSync(directory, { recursive: true, force: true }); }
});
