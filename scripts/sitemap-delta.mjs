#!/usr/bin/env bun
// Sitemap listings are receipted publisher statements, not facts about pages.
import { createHash } from 'node:crypto';
import { existsSync, mkdirSync, readFileSync, statSync } from 'node:fs';
import { dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { Database } from 'bun:sqlite';
import { observePublic, successfulPublicObservation } from '../cmd/BonfyreSourceFrontier/tools/foreign_observation.mjs';

let cachedPolicy;
export function sitemapPolicy() {
  if (!cachedPolicy) {
    const path = fileURLToPath(new URL('./sitemap-delta.cue', import.meta.url));
    const result = Bun.spawnSync(['cue', 'export', path, '-e', 'sitemapDelta']);
    if (result.exitCode !== 0) throw new Error(new TextDecoder().decode(result.stderr).trim());
    cachedPolicy = JSON.parse(new TextDecoder().decode(result.stdout));
  }
  return cachedPolicy;
}

export function sitemapDatabase(path) {
  mkdirSync(dirname(path), { recursive: true });
  const db = new Database(path, { create: true });
  db.exec(`CREATE TABLE IF NOT EXISTS sitemap_state(
    sitemap_url TEXT PRIMARY KEY, etag TEXT, last_modified TEXT,
    digest TEXT, checked_at TEXT, url_count INTEGER DEFAULT 0,
    checks INTEGER DEFAULT 0, unchanged INTEGER DEFAULT 0);
    CREATE TABLE IF NOT EXISTS sitemap_urls(
    sitemap_url TEXT, loc TEXT, lastmod TEXT, first_seen TEXT, last_seen TEXT,
    PRIMARY KEY(sitemap_url, loc));`);
  return db;
}

const digest = raw => createHash('sha256').update(raw).digest('hex');
export function verifiedObservation(observation, maxBytes) {
  const receipt = observation.receipt ?? {};
  if (!successfulPublicObservation(receipt) || !receipt.content_sha256)
    return { error: receipt.policy_decision || observation.error || 'not observed' };
  if (!observation.bodyPath || !existsSync(observation.bodyPath)) return { error: 'body_missing' };
  if (statSync(observation.bodyPath).size > maxBytes) return { error: 'body_exceeds_parser_limit' };
  const raw = readFileSync(observation.bodyPath);
  if (raw.length > maxBytes) return { error: 'body_exceeds_parser_limit' };
  if (digest(raw) !== receipt.content_sha256) return { error: 'body_does_not_match_receipt' };
  return { raw };
}

export function decodeSitemap(raw, options = {}) {
  const binary = options.xmlBinary ?? fileURLToPath(new URL('./foreign-xml', import.meta.url));
  const result = Bun.spawnSync([binary, '--sitemap'], { stdin: raw });
  if (result.exitCode !== 0) throw new Error('XML carrier decode failed');
  return JSON.parse(new TextDecoder().decode(result.stdout));
}

export async function checkSitemap(url, options = {}, policy = sitemapPolicy()) {
  const foreign = options.foreign ?? process.env.BONFYRE_FOREIGN_ROOT ?? '/var/lib/bonfyre/foreign';
  const project = options.project ?? process.env.BONFYRE_PROJECT_BIN ?? '/usr/local/bin/bonfyre-project';
  const dbPath = options.dbPath ?? process.env.BONFYRE_SITEMAP_DB ?? '/var/lib/bonfyre/state/sitemap.db';
  const observe = options.observe ?? ((address) => observePublic(project, foreign, 'sitemap', address,
    { maxBytes: policy.max_body_bytes, timeout: 180000, readText: false }));
  const observation = await observe(url);
  const receipt = observation.receipt ?? {};
  const verified = verifiedObservation(observation, policy.max_body_bytes);
  if (verified.error) return { sitemap: url, error: verified.error,
    policy_decision: receipt.policy_decision, robots_http_status: receipt.robots_http_status,
    http_status: receipt.http_status, detail: policy.observation_refusal };
  const raw = verified.raw;
  const db = sitemapDatabase(dbPath);
  try {
    const prior = db.query('SELECT digest FROM sitemap_state WHERE sitemap_url=?').get(url);
    if (prior?.digest === receipt.content_sha256) {
      db.query(`UPDATE sitemap_state SET checked_at=datetime('now'), checks=checks+1,
        unchanged=unchanged+1 WHERE sitemap_url=?`).run(url);
      return { sitemap: url, unchanged: true, digest: receipt.content_sha256.slice(0, 16),
        note: policy.unchanged_note };
    }
    let decoded;
    try { decoded = decodeSitemap(raw, options); }
    catch {
      const head = raw.subarray(0, policy.challenge_sample_bytes).toString('utf8').toLowerCase();
      const challenged = policy.challenge_markers.some(marker => head.includes(marker));
      return { sitemap: url, error: challenged ? 'challenged' : 'not_a_sitemap',
        detail: challenged ? policy.challenge_detail : 'receipted body is not a sitemap',
        http_status: receipt.http_status, bytes: receipt.body_bytes };
    }
    const known = new Map(db.query('SELECT loc,lastmod FROM sitemap_urls WHERE sitemap_url=?')
      .all(url).map(row => [row.loc, row.lastmod]));
    const seen = new Map(decoded.entries.map(row => [row.loc, row.lastmod]));
    const added = [...seen.keys()].filter(loc => !known.has(loc));
    const removed = [...known.keys()].filter(loc => !seen.has(loc));
    const changed = [...seen].filter(([loc, lastmod]) => known.has(loc) && lastmod && lastmod !== known.get(loc))
      .map(([loc]) => loc);
    const firstRun = known.size === 0;
    db.transaction(() => {
      const put = db.query(`INSERT INTO sitemap_urls(sitemap_url,loc,lastmod,first_seen,last_seen)
        VALUES(?,?,?,datetime('now'),datetime('now')) ON CONFLICT(sitemap_url,loc)
        DO UPDATE SET lastmod=excluded.lastmod,last_seen=datetime('now')`);
      const drop = db.query('DELETE FROM sitemap_urls WHERE sitemap_url=? AND loc=?');
      for (const [loc, lastmod] of seen) put.run(url, loc, lastmod);
      for (const loc of removed) drop.run(url, loc);
      db.query(`INSERT INTO sitemap_state(sitemap_url,etag,last_modified,digest,checked_at,url_count,checks,unchanged)
        VALUES(?,?,?,?,datetime('now'),?,1,0) ON CONFLICT(sitemap_url) DO UPDATE SET
        etag=excluded.etag,last_modified=excluded.last_modified,digest=excluded.digest,
        checked_at=datetime('now'),url_count=excluded.url_count,checks=checks+1`)
        .run(url, receipt.etag ?? '', receipt.last_modified ?? '', receipt.content_sha256, seen.size);
    })();
    return { sitemap: url, kind: decoded.kind, urls: seen.size, first_run: firstRun,
      added: added.length, changed: changed.length, removed: removed.length,
      sample_added: added.slice(0, policy.sample_urls),
      sample_changed: changed.slice(0, policy.sample_urls),
      sample_removed: removed.slice(0, policy.sample_urls),
      receipt: { sha256: receipt.content_sha256.slice(0, 16), http_status: receipt.http_status,
        bytes: receipt.body_bytes }, note: firstRun ? policy.baseline_note : policy.withdrawal_note };
  } finally { db.close(); }
}

export async function fromRobots(host, options = {}, policy = sitemapPolicy()) {
  const root = new URL(host.startsWith('http') ? host : `https://${host}`);
  const address = new URL('/robots.txt', root).toString();
  const foreign = options.foreign ?? process.env.BONFYRE_FOREIGN_ROOT ?? '/var/lib/bonfyre/foreign';
  const project = options.project ?? process.env.BONFYRE_PROJECT_BIN ?? '/usr/local/bin/bonfyre-project';
  const observe = options.observe ?? ((url) => observePublic(project, foreign, 'robots-sitemap', url,
    { maxBytes: policy.max_robots_bytes, timeout: 25000, readText: false }));
  const verified = verifiedObservation(await observe(address), policy.max_robots_bytes);
  if (verified.error) return { host: root.origin, declared_sitemaps: 0,
    error: verified.error, note: 'robots was not admitted; sitemap discovery remains unknown' };
  const declared = [...verified.raw.toString('utf8').matchAll(/^\s*sitemap:\s*(\S+)/gim)]
    .map(match => match[1]);
  if (!declared.length) return { host: root.origin, declared_sitemaps: 0,
    note: 'robots names no sitemap; fall to the next rung' };
  const results = [];
  for (const url of declared) results.push(await checkSitemap(url, options, policy));
  return { host: root.origin, declared_sitemaps: declared.length, results };
}

function state(path, url) {
  const db = sitemapDatabase(path);
  try { return url ? db.query('SELECT * FROM sitemap_state WHERE sitemap_url=?').all(url)
    : db.query('SELECT * FROM sitemap_state').all(); }
  finally { db.close(); }
}

if (import.meta.main) {
  const [command, ...args] = process.argv.slice(2);
  try {
    let result;
    if (command === 'check' && args.length) result = await Promise.all(args.map(url => checkSitemap(url)));
    else if (command === 'from-robots' && args.length) result = await Promise.all(args.map(host => fromRobots(host)));
    else if (command === 'state') result = state(process.env.BONFYRE_SITEMAP_DB ?? '/var/lib/bonfyre/state/sitemap.db', args[0]);
    else throw new Error('usage: sitemap-delta.mjs check <url>... | from-robots <host>... | state [url]');
    process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
  } catch (error) { process.stderr.write(`${error.message}\n`); process.exitCode = 1; }
}
