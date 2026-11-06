import { test, expect } from 'bun:test';
import { mkdtempSync, readFileSync, rmSync, writeFileSync, chmodSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { collect, normalizeCarrier, normalizeProviderRecord, eventFromHtml } from '../tools/collect_public_carrier.mjs';

const args = { 'source-ref': 'source:fixture', 'observed-at': '2026-09-28T00:00:00Z', provider: 'meetup' };
test('five public carriers narrow retained responses without private authority', () => {
  const cases = [
    ['github', { id: 42, full_name: 'org/repo', html_url: 'https://github.com/org/repo' }, 'github_repository'],
    ['reddit', { data: { children: [{ data: { subreddit: 'bonfyre', subreddit_url: 'https://reddit.com/r/bonfyre/' } }] } }, 'reddit_community'],
    ['discord', { id: '42', name: 'Operators', instant_invite: 'https://discord.gg/abc' }, 'discord_server'],
    ['event', '<script type="application/ld+json">{"@type":"Event","name":"Meet","url":"https://events.test/m"}</script>', 'meetup_event'],
    ['211', { id: 'r', name: 'Food', url: 'https://211.test/r', private_enrollment: 'discarded' }, '211_resource'],
  ];
  for (const [carrier, doc, kind] of cases) {
    const row = normalizeCarrier(carrier, doc, args, 'fixture', 'https://events.test/m');
    expect(row.kind).toBe(kind);
    expect(row.receipt_ref).toBe('foreign:fixture');
    expect(JSON.stringify(row)).not.toContain('private_enrollment');
  }
  expect(() => normalizeCarrier('github', {}, args, 'x', '')).toThrow();
  expect(() => normalizeCarrier('reddit', {}, args, 'x', '')).toThrow();
  expect(() => normalizeCarrier('discord', { id: '42', name: 'Private' }, args, 'x', '')).toThrow();
  expect(() => eventFromHtml('<html></html>', 'https://events.test/m')).toThrow();
});

test('provider phenotypes exclude private enrollment, tenancy, membership and payment', () => {
  const cases = {
    '211': { id: 'r', name: 'Food', url: 'https://r.test', private_enrollment: 'no' },
    clever: { data: { id: 's', name: 'School', url: 'https://s.test', enrollment: 'no' } },
    buildium: { Id: 1, Name: 'Home', PublicUrl: 'https://p.test', tenant: 'no' },
    nextdoor: { id: 'n', name: 'Place', url: 'https://n.test', members: 'no' },
    patreon: { data: { id: 'p', attributes: { full_name: 'Creator', url: 'https://p.test', payment: 'no' } } },
  };
  for (const [provider, doc] of Object.entries(cases)) {
    const row = normalizeProviderRecord(provider, doc, 'source:x', 'receipt:x', '2026');
    expect(row.payload.title).toBeTruthy();
    expect(JSON.stringify(row)).not.toContain('"no"');
  }
  expect(() => normalizeProviderRecord('clever', { id: 's', name: 'Private' }, 's', 'r', 't')).toThrow();
});

test('collector requires both retained body and receipt before emitting an envelope', () => {
  const dir = mkdtempSync(join(tmpdir(), 'bonfyre-public-carrier-'));
  try {
    const observer = join(dir, 'observer');
    writeFileSync(observer, '#!/bin/sh\nwhile [ "$#" -gt 0 ]; do case "$1" in --body) body=$2; shift 2;; --receipt) receipt=$2; shift 2;; *) shift;; esac; done\nprintf "%s\\n" \'{"id":"food","name":"Food","url":"https://211.test/food"}\' > "$body"\nprintf "%s\\n" \'{"receipt":"fixture"}\' > "$receipt"\n');
    chmodSync(observer, 0o755);
    const out = join(dir, 'record.json');
    const result = collect('211', { ...args, url: 'https://211.test/food', project: observer,
      'foreign-root': join(dir, 'foreign'), out });
    expect(result.kind).toBe('211_resource');
    expect(JSON.parse(readFileSync(out, 'utf8')).receipt_ref).toBe(result.receipt_ref);
  } finally { rmSync(dir, { recursive: true, force: true }); }
});
