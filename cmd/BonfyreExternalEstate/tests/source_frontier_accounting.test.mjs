import { test, expect } from 'bun:test';
import { createHash } from 'node:crypto';
import { mkdtempSync, rmSync, writeFileSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { compileSourceFrontier } from '../tools/compile_source_frontier.mjs';

const digest = (text, length) => createHash('sha256').update(text).digest('hex').slice(0, length);

test('frontier accounting preserves exact channels, descriptor roots, evidence and gaps', () => {
  const dir = mkdtempSync(join(tmpdir(), 'bonfyre-frontier-accounting.'));
  try {
    const input = join(dir, 'sources.jsonl');
    writeFileSync(input, [
      { url: 'https://www.example.org/feed.xml?utm_source=test', source_kind: 'rss', state: 'allow',
        publisher_ident: 'publisher:one', licence: 'CC-BY', virtual_channel_count: 3 },
      { url: 'https://example.org/feed.xml', source_kind: 'rss', status: 'live',
        capabilities: ['buy'], credentials: 'required_pending' },
      { source: 'Street Directory', kind: 'directory', terms: 'book,quote' },
      { source: 'Excluded Directory', status: 'refused', reuse: { unknown: true } },
    ].map(row => JSON.stringify(row)).join('\n') + '\n');
    const result = compileSourceFrontier([`PUBLIC=${input}`]);
    const webRootId = `root-${digest('https://example.org/', 20)}`;
    const webChannelId = `channel-${digest('https://example.org/feed.xml', 20)}`;
    expect(result.manifest.cardinality).toEqual({ input_occurrences: 4, source_roots: 3,
      channel_families: 2, channels: 3, eligible_channels: 2, admitted_channels: 1,
      active_channels: 1, declared_virtual_channels: 3, open_gaps: 12 });
    expect(result.manifest.truth_level).toBe('compiled_accounting_not_admission');
    expect(result.channels.find(row => row.channel_id === webChannelId)).toMatchObject({
      root_id: webRootId, address: 'https://example.org/feed.xml', family: 'RSS',
      states: ['LIVE', 'OBSERVED'], provenance: ['PUBLIC:sources.jsonl:1', 'PUBLIC:sources.jsonl:2'] });
    expect(result.roots.find(row => row.root_id === webRootId)).toMatchObject({
      channels: 1, admitted_channels: 1, active_channels: 1, transaction_like: true,
      rights_states: ['DECLARED', 'UNKNOWN'] });
    expect(result.roots.find(row => row.address === 'urn:bonfyre:source:street-directory'))
      .toMatchObject({ channels: 1, admitted_channels: 0, publisher_identities: [] });
    expect(result.gaps.filter(row => row.root_id === webRootId).map(row => row.kind))
      .toEqual(['CREDENTIAL', 'ECONOMIC_INTERFACE', 'RIGHTS']);
    const manifest = join(dir, 'frontier.json'), roots = join(dir, 'roots.jsonl');
    const child = Bun.spawnSync(['bun', new URL('../tools/compile_source_frontier.mjs', import.meta.url).pathname,
      '--input', `PUBLIC=${input}`, '--out', manifest, '--roots', roots]);
    expect(child.exitCode).toBe(0);
    expect(JSON.parse(readFileSync(manifest, 'utf8'))).toEqual(result.manifest);
    expect(readFileSync(roots, 'utf8').trim().split('\n').map(JSON.parse)).toEqual(result.roots);
  } finally { rmSync(dir, { recursive: true, force: true }); }
});
