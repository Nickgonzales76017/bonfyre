#!/usr/bin/env bun
// Recursive candidates from retained CMS source bodies; no admission or effects.
import { Database } from 'bun:sqlite';
import { createHash } from 'node:crypto';
import { existsSync, openSync, closeSync, readSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { publisherGrammarPolicy } from '../../BonfyreSourceFrontier/tools/publisher_grammar_policy.mjs';
import { linkTokens } from '../../BonfyreSourceFrontier/tools/expand_retained_channels.mjs';
import { canonicalUrl, channelUrl, loadRows } from './source_identity.mjs';

let compiled;
export function recursivePolicy() {
  if (!compiled) {
    const source = publisherGrammarPolicy().recursive_profile;
    compiled = { ...source,
      linkHints: source.link_hints.map(({ pattern, family }) => ({ pattern: new RegExp(pattern, 'i'), family })),
      jsonApiPath: new RegExp(source.json_api_path, 'i'),
      sitemapPath: new RegExp(source.sitemap_path, 'i'),
      archivePath: new RegExp(source.wordpress_archive_path, 'i'),
      sitemapLocation: new RegExp(source.sitemap_location, 'gi') };
  }
  return compiled;
}

export function classifyRecursiveLink(tag, url, attrs, policy = recursivePolicy()) {
  const evidence = `${tag} ${url}`;
  for (const hint of policy.linkHints) if (hint.pattern.test(evidence)) return hint.family;
  if (policy.jsonApiPath.test(url)) return 'JSON_API';
  if (policy.sitemapPath.test(url)) return 'SITEMAP';
  if (attrs.xmlurl) return String(attrs.type || '').toLowerCase() === 'atom' ? 'ATOM' : 'RSS';
  return '';
}

const string = value => value == null ? '' : String(value);
const jsonl = rows => rows.map(row => JSON.stringify(row)).join('\n') + (rows.length ? '\n' : '');
const canonical = value => {
  try { return canonicalUrl(value); } catch { return ''; }
};

export function compileRecursiveChannels(db, foreignRoot, knownRows = [], maxBody = 2_000_000,
  policy = recursivePolicy()) {
  const table = db.query("SELECT 1 FROM sqlite_master WHERE type='table' AND name='internet_source'").get();
  const sources = table ? db.query("SELECT source,twin,url,COALESCE(family,'') AS family FROM internet_source "
    + "WHERE status='published' AND COALESCE(twin,'')<>'' AND COALESCE(url,'')<>'' ORDER BY id").all() : [];
  const known = new Set(knownRows.flatMap(row => {
    const value = row && typeof row === 'object' ? channelUrl(row) : null;
    const address = value && canonical(value);
    return address ? [address] : [];
  }));
  for (const source of sources) {
    const address = canonical(string(source.url));
    if (address) known.add(address);
  }
  const found = new Map();
  let bodies = 0;
  for (const source of sources) {
    const path = join(foreignRoot, 'bodies', `${source.twin}.body`);
    if (!existsSync(path)) continue;
    const fd = openSync(path, 'r'), buffer = Buffer.alloc(maxBody);
    let size;
    try { size = readSync(fd, buffer, 0, buffer.length, 0); } finally { closeSync(fd); }
    const raw = buffer.subarray(0, size), text = raw.toString('utf8');
    const digest = createHash('sha256').update(raw).digest('hex');
    bodies++;
    const provenance = { source: string(source.source), twin: string(source.twin),
      base_url: string(source.url), family: string(source.family), body_sha256: digest };
    const add = (address, family, sourceKind, truthLevel, extra = {}) => {
      const url = canonical(address);
      if (!url || !family) return;
      const key = `${url}\0${family}`;
      let row = found.get(key);
      if (!row) {
        row = { url, channel_family: family, state: 'SHADOW', source_kind: sourceKind,
          capabilities: ['observe'], discovered_from: [], already_known: known.has(url),
          truth_level: truthLevel, ...extra };
        found.set(key, row);
      }
      row.discovered_from.push(provenance);
    };
    const tokens = linkTokens(text);
    for (const { raw: tag, attrs } of tokens) {
      const href = attrs.href || attrs.xmlurl;
      if (!href) continue;
      let address;
      try { address = new URL(href.replaceAll('&amp;', '&'), string(source.url)).href; } catch { continue; }
      add(address, classifyRecursiveLink(tag, address, attrs, policy),
        'recursive_channel_candidate', 'observed_link_candidate');
    }
    if (text.toLowerCase().includes(policy.sitemap_index_marker)) {
      for (const match of text.matchAll(policy.sitemapLocation)) {
        const address = match[1].replaceAll('&amp;', '&');
        if (policy.sitemapPath.test(address))
          add(address, 'SITEMAP', 'recursive_channel_candidate', 'observed_xml_index_candidate');
      }
    }
    const lower = text.toLowerCase();
    if (policy.wordpress_markers.some(marker => lower.includes(marker.toLowerCase()))) {
      let base;
      try { base = new URL(string(source.url)); } catch { continue; }
      for (const route of policy.wordpress_routes)
        add(new URL(route.path, base.origin + '/').href, route.family, 'wordpress_channel_candidate',
          'protocol_derived_candidate', { channel_role: route.role });
      for (const { attrs } of tokens) {
        if (!attrs.href) continue;
        let archive;
        try { archive = new URL(attrs.href.replaceAll('&amp;', '&'), base); } catch { continue; }
        if (archive.host.toLowerCase() !== base.host.toLowerCase() || !policy.archivePath.test(archive.pathname)) continue;
        const parent = canonical(archive.href);
        if (parent) add(new URL(`${archive.pathname.replace(/\/$/, '')}/feed/`, base.origin).href,
          'RSS', 'wordpress_archive_feed_candidate', 'protocol_derived_candidate', { parent_archive: parent });
      }
    }
  }
  const rows = [...found.values()].toSorted((a, b) => a.url.localeCompare(b.url) ||
    a.channel_family.localeCompare(b.channel_family));
  for (const row of rows) row.cannot_infer = policy.cannot_infer;
  const families = {};
  for (const row of rows) families[row.channel_family] = (families[row.channel_family] || 0) + 1;
  return { rows, summary: { schema: 'bonfyre.recursive-channel-candidates.v1',
    retained_bodies_read: bodies, candidates: rows.length,
    already_known: rows.filter(row => row.already_known).length,
    families: Object.fromEntries(Object.entries(families).toSorted(([a], [b]) => a.localeCompare(b))),
    truth: 'observed_link_candidates_only' } };
}

if (import.meta.main) {
  let db;
  try {
    const args = {}, argv = process.argv.slice(2);
    for (let i = 0; i < argv.length; i += 2) {
      if (!argv[i]?.startsWith('--') || !argv[i + 1]) throw new Error('expected --key value arguments');
      args[argv[i].slice(2)] = argv[i + 1];
    }
    if (!args['cms-db'] || !args['foreign-root'] || !args.out) throw new Error('CMS DB, ForeignTwin root and output are required');
    const maxBody = Number(args['max-body'] || 2_000_000);
    if (!Number.isSafeInteger(maxBody) || maxBody < 1 || maxBody > 64_000_000) throw new Error('invalid max body');
    db = new Database(args['cms-db'], { readonly: true });
    const result = compileRecursiveChannels(db, args['foreign-root'], args.known ? loadRows(args.known) : [], maxBody);
    writeFileSync(args.out, jsonl(result.rows));
    console.log(JSON.stringify(result.summary));
  } catch (error) { console.error(`recursive channels: ${error.message}`); process.exitCode = 2; }
  finally { db?.close(); }
}
