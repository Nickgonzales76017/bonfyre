import { test, expect } from 'bun:test';
import { readFileSync, writeFileSync, mkdtempSync, rmSync, existsSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { tmpdir } from 'node:os';
import { loadConfig, parseMaster, parseProtocols, project, termMatches, compileCorpus } from '../compile-external-estate.mjs';

const root = resolve(import.meta.dir, '../..');
const config = loadConfig();

test('CUE composition vocabulary is complete and short acronyms are token-matched', () => {
  expect(config.domain_terms.length).toBe(61);
  expect(config.worlds.length).toBe(30);
  expect(config.spatial_terms.length).toBe(39);
  expect(termMatches('DER resource', 'der')).toBe(true);
  expect(termMatches('Genderize profile', 'der')).toBe(false);
  expect(termMatches('weather forecast', 'weather')).toBe(true);
  expect(termMatches('business forecast', 'weather')).toBe(false);
});

test('source parser preserves occurrence and documented continuation identity', () => {
  const directory = mkdtempSync(join(tmpdir(), 'bonfyre-external-compiler-'));
  try {
    const master = join(directory, 'master.md'), protocols = join(directory, 'protocols.md');
    writeFileSync(master, '- `EXT-00001` **[Foundation](https://example.test/foundation)**\n' +
      '## 3. Lossless source-organized master list\n### People\n' +
      '- `EXT-00002` **[Town School](https://example.test/school)**\n');
    writeFileSync(protocols, '## Protocols\n| **[Protocol](https://example.test/protocol)** | a | b | c | d | [source](https://example.test/spec) |\n');
    const fixture = { ...config, source_ranges: [{ max: 1, kind: 'x402_spec' }, { max: 2, kind: 'rss' }],
      expected_counts: { master: 2, protocols: 1 } };
    const source = parseMaster(master, fixture), continuation = parseProtocols(protocols, fixture);
    expect(source.map((row) => row.source_ref)).toEqual(['EXT-00001', 'EXT-00002']);
    expect(source[1].raw_source_text).toContain('Town School');
    expect(source[1].section).toBe('People');
    expect(continuation[0].source_ref).toBe('EXT-00003');
    expect(continuation[0].source_ref_assignment).toBe('documented_continuation_range');
    const { records, summary } = compileCorpus(master, protocols, fixture);
    expect(records).toHaveLength(3);
    expect(summary.counts.external_occurrences).toBe(3);
    expect(records[1].projection.topic_domains).toContain('education');
    expect(records[1].projection.classification_level).toBe('candidate_heuristic_not_provider_admission');
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test('portable golden occurrences from every source kind replay from CUE', () => {
  const rows = JSON.parse(readFileSync(join(import.meta.dir, 'external-estate-golden.json'), 'utf8'));
  expect(new Set(rows.map((row) => row.source_kind)).size).toBe(6);
  expect(rows.some((row) => row.projection.spatial_candidate === 'bearing')).toBe(true);
  for (const row of rows) expect(project(row, config), row.id).toEqual(row.projection);
});

if (existsSync(join(root, 'estate/generated/external-estate-v4/observed-external-estate.jsonl'))) test('all local occurrence projections replay from the CUE rules', () => {
  const corpus = join(root, 'estate/generated/external-estate-v4/observed-external-estate.jsonl');
  const rows = readFileSync(corpus, 'utf8').trimEnd().split('\n').map((line) => JSON.parse(line));
  expect(rows).toHaveLength(4891);
  for (const row of rows) expect(project(row, config), row.id).toEqual(row.projection);
  expect(rows.filter((row) => row.projection.spatial_candidate === 'bearing')).toHaveLength(146);
  expect(rows.every((row) => row.truth_level === 'observed_candidate')).toBe(true);
});
