import { test, expect } from 'bun:test';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { compileSourcePressure, sourceIndex, sourcePressurePolicy } from '../tools/compile_source_pressure.mjs';

test('Frontier occurrence projection carries source state and family into candidate pressure', () => {
  const dir = mkdtempSync(join(tmpdir(), 'bonfyre-source-pressure.'));
  try {
    const input = join(dir, 'sources.jsonl'), closure = join(dir, 'closure.json');
    writeFileSync(input, [
      { id: 'ext-1', publisher_ident: 'shared', url: 'https://one.test/feed' },
      { id: 'ext-2', publisher_ident: 'shared', url: 'https://two.test/feed' },
    ].map(JSON.stringify).join('\n') + '\n');
    writeFileSync(closure, JSON.stringify({ subject: 'subject:1', scope: 'public',
      pack_demands: [{ identity: 'pack:1', candidate_groups: [{ source: 'search',
        candidates: [{ identity: 'ext-1' }, { identity: 'shared' }, { identity: 'missing' }] }] }],
      world_demands: [{ identity: 'world:1', dependencies: [{ dependency: 'learning',
        candidates: [{ identity: 'shared' }] }] }] }));
    const roots = [
      { root_id: 'root-1', address: 'https://one.test/', provenance: ['TEST'], states: ['OBSERVED'], channel_families: ['RSS'] },
      { root_id: 'root-2', address: 'https://two.test/', provenance: ['TEST'], states: ['CANDIDATE'], channel_families: ['ATOM'] },
    ];
    const channels = [
      { root_id: 'root-1', provenance: ['TEST:sources.jsonl:1'] },
      { root_id: 'root-2', provenance: ['TEST:sources.jsonl:2'] },
    ];
    const result = compileSourcePressure([`TEST=${input}`], channels, roots, [closure]);
    expect(result.summary).toMatchObject({ closures_read: 1, roots_under_pressure: 2,
      candidate_occurrences_mapped: 5, unmapped_candidate_identities: 1,
      unmapped_candidate_occurrences: 1 });
    expect(result.rows[0]).toMatchObject({ root_id: 'root-1', candidate_occurrences: 3,
      pack_candidate_occurrences: 2, world_candidate_occurrences: 1,
      ambiguous_identity_matches: 2, states: ['OBSERVED'], channel_families: ['RSS'] });
    expect(result.rows[1]).toMatchObject({ root_id: 'root-2', candidate_occurrences: 2,
      ambiguous_identity_matches: 2, states: ['CANDIDATE'], channel_families: ['ATOM'] });
    expect(result.unmapped).toEqual({ missing: 1 });
    expect(result.rows[0].cannot_infer).toContain('candidate pressure -> source contribution');
    const changed = structuredClone(sourcePressurePolicy());
    changed.truth = 'test_candidate_truth';
    expect(compileSourcePressure([`TEST=${input}`], channels, roots, [closure], changed).rows[0].truth)
      .toBe('test_candidate_truth');
    expect(() => sourceIndex([`TEST=${input}`], [...channels, { root_id: 'root-2',
      provenance: ['TEST:sources.jsonl:1'] }], roots)).toThrow('ambiguous Frontier provenance');
  } finally { rmSync(dir, { recursive: true, force: true }); }
});

test('descriptor-only source identity joins through Frontier provenance without URL guessing', () => {
  const dir = mkdtempSync(join(tmpdir(), 'bonfyre-descriptor-pressure.'));
  try {
    const input = join(dir, 'descriptors.jsonl'), closure = join(dir, 'closure.json');
    writeFileSync(input, JSON.stringify({ id: 'descriptor:one', observed_name: 'Local programme' }) + '\n');
    writeFileSync(closure, JSON.stringify({ subject: 'programme:one', scope: 'public',
      pack_demands: [{ identity: 'pack:one', candidate_groups: [{ candidates: [{ identity: 'descriptor:one' }] }] }] }));
    const roots = [{ root_id: 'root-descriptor', address: 'urn:bonfyre:source:descriptor-one',
      provenance: ['DESCRIPTORS'], states: ['CANDIDATE'], channel_families: ['DESCRIPTOR'] }];
    const channels = [{ root_id: 'root-descriptor', provenance: ['DESCRIPTORS:descriptors.jsonl:1'] }];
    const result = compileSourcePressure([`DESCRIPTORS=${input}`], channels, roots, [closure]);
    expect(result.rows[0]).toMatchObject({ root_id: 'root-descriptor', candidate_occurrences: 1,
      channel_families: ['DESCRIPTOR'], root_address: 'urn:bonfyre:source:descriptor-one' });
  } finally { rmSync(dir, { recursive: true, force: true }); }
});
