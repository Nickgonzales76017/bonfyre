import { test, expect } from 'bun:test';
import { strict as assert } from 'node:assert';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Database } from 'bun:sqlite';

const helper = new URL('./pure_tier_witness.mjs', import.meta.url).pathname;
const invoke = (...args) => Bun.spawnSync([process.execPath, helper, ...args],
  { stdout: 'pipe', stderr: 'pipe' });
const writeJson = (path, value) => writeFileSync(path, JSON.stringify(value));

test('Bun plane witness reads SQLite owners and rejects a changed reducer frontier', () => {
  const root = mkdtempSync(join(tmpdir(), 'bonfyre-pure-tier-witness-'));
  try {
    const estatePath = join(root, 'estate.db');
    const rankPath = join(root, 'rank.db');
    const bodyPath = join(root, 'body.txt');
    const observationPath = join(root, 'observation.jsonl');
    const ids = Array.from({ length: 8 }, (_, index) => `c${index + 1}`);
    writeFileSync(bodyPath, 'retained');
    writeJson(join(root, 'plane.json'), { rejected_before_exact: 3,
      survivors: ids.map(id => ({ id })) });
    writeFileSync(observationPath, ids.slice(0, 2).map(id =>
      JSON.stringify({ twin_id: id, body_path: bodyPath, body_bytes: 8 })).join('\n') + '\nmalformed\n');

    const estate = new Database(estatePath);
    estate.exec('create table occurrence(id text, source_kind text)');
    const addOccurrence = estate.query('insert into occurrence values (?, ?)');
    for (const id of ids) addOccurrence.run(id, 'public_api');
    estate.close();
    const rank = new Database(rankPath);
    rank.exec('create table ranked_candidate_envelope(rank integer, candidate_identity text)');
    const addRank = rank.query('insert into ranked_candidate_envelope values (?, ?)');
    ids.forEach((id, index) => addRank.run(ids.length - index, id));
    rank.close();

    const generated = invoke('plane-generate', root, rankPath, estatePath, observationPath);
    assert.equal(generated.exitCode, 0, generated.stderr.toString());
    const ranked = JSON.parse(readFileSync(join(root, 'chain_rank.json'), 'utf8'));
    const reversed = JSON.parse(readFileSync(join(root, 'chain_reversed.json'), 'utf8'));
    const plane = JSON.parse(readFileSync(join(root, 'chain_plane.json'), 'utf8'));
    expect(ranked.candidate_space.map(candidate => candidate.id)).toEqual([...ids].reverse());
    expect(reversed.candidate_space.map(candidate => candidate.id)).toEqual(ids);
    expect(plane.candidate_space.map(candidate => candidate.id)).toEqual(ids);
    expect(ranked.candidate_space.filter(candidate => candidate.retained).map(candidate => candidate.id))
      .toEqual(['c2', 'c1']);

    const result = ['c1', 'c2'];
    for (const [index, order] of ['rank', 'reversed', 'plane'].entries()) {
      writeJson(join(root, `out_${order}.json`), { agreement: true, backends: [
        ...['reference', 'hvm2', 'hvm4'].map(backend => ({ backend, result })),
        { backend: 'bend', aggregate: { index_checksum: index + 10 } },
      ] });
    }
    const checked = invoke('plane-check', root);
    assert.equal(checked.exitCode, 0, checked.stderr.toString());
    expect(checked.stdout.toString()).toContain('one survivor set across three orders');
    expect(checked.stdout.toString()).not.toContain('SKIP');

    const altered = JSON.parse(readFileSync(join(root, 'out_reversed.json'), 'utf8'));
    altered.backends.find(row => row.backend === 'hvm4').result = ['c3'];
    writeJson(join(root, 'out_reversed.json'), altered);
    const rejected = invoke('plane-check', root);
    expect(rejected.exitCode).not.toBe(0);
    expect(rejected.stderr.toString()).toContain('backends returned different sets');
  } finally { rmSync(root, { recursive: true, force: true }); }
});
