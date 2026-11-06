import { test, expect } from 'bun:test';
import { Database } from 'bun:sqlite';
import { createHash } from 'node:crypto';
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { compileRecursiveChannels } from '../tools/compile_recursive_channels.mjs';

test('retained body compiles observed links and distinct WordPress candidate routes', () => {
  const dir = mkdtempSync(join(tmpdir(), 'bonfyre-recursive-channels.'));
  let db;
  try {
    const foreign = join(dir, 'foreign'), bodies = join(foreign, 'bodies');
    mkdirSync(bodies, { recursive: true });
    const body = '<meta content="wp-content/"><link rel="alternate" type="application/rss+xml" href="/feed.xml">'
      + '<link rel="hub" href="https://hub.test/subscribe"><a href="/category/news/">News</a>'
      + '<sitemapindex><loc>https://pub.test/wp-sitemap.xml</loc></sitemapindex>';
    writeFileSync(join(bodies, 'twin-1.body'), body);
    const path = join(dir, 'cms.sqlite');
    db = new Database(path);
    db.exec('CREATE TABLE internet_source (id INTEGER, source TEXT, twin TEXT, url TEXT, family TEXT, status TEXT)');
    db.query('INSERT INTO internet_source VALUES (?, ?, ?, ?, ?, ?)').run(
      1, 'source:one', 'twin-1', 'https://pub.test/', 'publisher', 'published');
    const result = compileRecursiveChannels(db, foreign);
    const rss = result.rows.find(row => row.url === 'https://pub.test/feed.xml');
    expect(rss).toMatchObject({ channel_family: 'RSS', state: 'SHADOW',
      source_kind: 'recursive_channel_candidate', truth_level: 'observed_link_candidate' });
    expect(rss.discovered_from[0].body_sha256).toBe(createHash('sha256').update(body).digest('hex'));
    expect(result.rows.find(row => row.url === 'https://hub.test/subscribe').channel_family).toBe('WEBSUB');
    expect(result.rows.find(row => row.url === 'https://pub.test/category/news/feed/'))
      .toMatchObject({ source_kind: 'wordpress_archive_feed_candidate',
        parent_archive: 'https://pub.test/category/news/' });
    expect(result.rows.find(row => row.url === 'https://pub.test/wp-json/wp/v2/posts'))
      .toMatchObject({ channel_role: 'posts_collection', truth_level: 'protocol_derived_candidate' });
    expect(result.rows.find(row => row.url === 'https://pub.test/wp-sitemap.xml'))
      .toMatchObject({ truth_level: 'observed_xml_index_candidate' });
    expect(result.summary).toMatchObject({ retained_bodies_read: 1,
      candidates: result.rows.length, truth: 'observed_link_candidates_only' });
    expect(result.rows.every(row => row.cannot_infer.includes('channel candidate -> admission or current content'))).toBe(true);
    const out = join(dir, 'recursive.jsonl');
    const child = Bun.spawnSync(['bun', new URL('../tools/compile_recursive_channels.mjs', import.meta.url).pathname,
      '--cms-db', path, '--foreign-root', foreign, '--out', out]);
    expect(child.exitCode).toBe(0);
    expect(readFileSync(out, 'utf8').trim().split('\n').map(JSON.parse)).toEqual(result.rows);
  } finally { db?.close(); rmSync(dir, { recursive: true, force: true }); }
});
