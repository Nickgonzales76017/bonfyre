import { test, expect } from 'bun:test';
import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { buildWitness, inventory, itemKey } from '../tools/audio_family_witness.mjs';

const bodies = {
  'wikidata-recorded-music': { results: { bindings: [{ title: { value: 'Work' }, composer: { value: 'Composer' }, page: { value: 'https://music.test/work' }, audio: { value: 'https://music.test/work.mp3' } }] } },
  'archive-great-78': { response: { docs: [{ title: 'Record', creator: ['Singer', 'Band'], identifier: 'record-1', date: '1935-04-12' }] } },
  'archive-dallas-live-music': { response: { docs: [{ title: 'Set', creator: 'Player', identifier: 'set-1', date: '' }] } },
};
const bodyFn = (_root, twin) => bodies[twin];

test('retained-family inventory preserves distinct source addresses and collection grouping', () => {
  const rows = inventory('/foreign', 1, bodyFn);
  expect(rows).toHaveLength(3);
  expect(rows.map(row => row.group)).toEqual(['group:recorded-works', 'group:78-rpm-records', 'group:dallas-live']);
  expect(rows[1].maker).toBe('Singer, Band');
  expect(itemKey(rows[0].source, rows[0].href, rows[0].title)).toHaveLength(24);
});

test('witness carries samples into six charts, groups, refusal, mutation and reopen proof', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'bonfyre-audio-witness-'));
  const calls = [];
  try {
    const out = join(dir, 'witness.json');
    const runFn = (_atlas, args) => {
      calls.push(args);
      switch (args[0]) {
        case 'encode-text': return [0.2, 0.4];
        case 'encode-roles': return '1011';
        case 'import': return { imported: 1 };
        case 'group': return { snapshot: 's', members: 1, member_type: 'point', representation: { type: 'medoid' } };
        case 'lift': return { snapshot: 'parent', representation: { type: 'hierarchy', topology: [] } };
        case 'compare': return { refused: 'cross-chart' };
        case 'mutate': return { changed: true };
        case 'reopen': return { drifted: 0 };
        case 'refresh': return { groups: [{ logical: 'group:recorded-works@spectrum-24' }], parents_recomputed: 1, parents_held: 0 };
        default: return {};
      }
    };
    const sampleFn = async (_item, { start = 0 }) => ({ acoustic: { fingerprint: '1010', distribution: [1, 2], trajectory: [3], frames: 7 }, sample_sha256: start ? 'b'.repeat(64) : 'a'.repeat(64), sample_bytes: 2000, url: 'https://audio.test/file.mp3', receipt: { policy_decision: 'allow', http_status: 206 } });
    const result = await buildWitness({ atlas: 'atlas', project: 'project', foreign: dir, db: join(dir, 'atlas.db'), out, perSource: 1 }, { bodyFn, sampleFn, runFn, delayFn: async () => {} });
    const witness = JSON.parse(readFileSync(out, 'utf8'));
    expect(result.sampled).toBe(3);
    expect(result.refused).toBe(0);
    expect(witness.cross_chart.refused).toBe('cross-chart');
    expect(witness.mutation.from).not.toBe(witness.mutation.to);
    expect(witness.reopen_after_refresh).toHaveProperty('group:recorded-works@spectrum-24');
    expect(calls.filter(args => args[0] === 'group')).toHaveLength(9);
    expect(calls.some(args => args[0] === 'lens')).toBe(true);
  } finally { rmSync(dir, { recursive: true, force: true }); }
});
