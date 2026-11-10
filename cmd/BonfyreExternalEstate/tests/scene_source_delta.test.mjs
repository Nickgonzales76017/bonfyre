import { test, expect } from 'bun:test';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { resolve } from 'node:path';
import { compileSceneDelta } from '../tools/compile_scene_source_delta.mjs';

const scene = (key, digest, roots, apertures = [], lenses = [], foreign = []) => ({
  schema: 'bonfyre.scene-source-receipt.v1', scene_key: key,
  subject_address: `/s/${key}`, actor_scope: 'public', surface_scene_digest: digest,
  source_roots: roots.map(root_id => ({ root_id })), action_apertures: apertures,
  lenses, foreign_twin_dependencies: foreign.map(ref => ({ ref })),
});

test('Scene co-change is measured without attributing source causality', () => {
  const before = [scene('a', 'one', ['source:a'], ['read'], ['wall'], ['receipt:1']),
    scene('retired', 'old', ['source:c'])];
  const after = [scene('a', 'two', ['source:a', 'source:b'], ['read', 'save'], ['map'], ['receipt:2']),
    scene('born', 'new', ['source:d'])];
  const { deltas, rootRows, summary } = compileSceneDelta(before, after);
  expect(deltas.map(row => [row.scene_key, row.state])).toEqual([
    ['a', 'changed'], ['born', 'born'], ['retired', 'retired']]);
  expect(deltas[0].source_roots_added).toEqual(['source:b']);
  expect(deltas[0].foreign_dependencies_removed).toEqual(['receipt:1']);
  expect(deltas[0].action_apertures_added).toEqual(['save']);
  expect(deltas[0].lenses_removed).toEqual(['wall']);
  expect(deltas[0].truth).toBe('temporal_scene_dependency_cochange_not_source_causality');
  expect(rootRows.find(row => row.root_id === 'source:a').scenes_changed).toBe(1);
  expect(summary).toMatchObject({ before_scenes: 2, after_scenes: 2, changed_scenes: 3,
    roots_with_dependent_cochange: 4 });
  expect(compileSceneDelta(before, after, 'source:b').deltas.map(row => row.scene_key)).toEqual(['a']);
});

test('unchanged dependencies and digest produce no false delta', () => {
  const old = scene('stable', 'digest', ['source:a'], [], ['agenda']);
  expect(compileSceneDelta([old], [{ ...old }]).deltas).toEqual([]);
});

test('production CLI writes both Scene and per-root receipt streams', () => {
  const dir = mkdtempSync(resolve(tmpdir(), 'bonfyre-scene-delta-test.'));
  try {
    const before = resolve(dir, 'before.jsonl'), after = resolve(dir, 'after.jsonl');
    const out = resolve(dir, 'delta.jsonl'), roots = resolve(dir, 'roots.jsonl');
    writeFileSync(before, JSON.stringify(scene('a', 'one', ['source:a'])) + '\n');
    writeFileSync(after, JSON.stringify(scene('a', 'two', ['source:a'])) + '\n');
    const run = Bun.spawnSync(['bun', resolve(import.meta.dir, '../tools/compile_scene_source_delta.mjs'),
      '--before', before, '--after', after, '--out', out, '--root-out', roots]);
    expect(run.exitCode).toBe(0);
    expect(JSON.parse(readFileSync(out, 'utf8')).scene_digest_changed).toBe(true);
    expect(JSON.parse(readFileSync(roots, 'utf8')).root_id).toBe('source:a');
  } finally { rmSync(dir, { recursive: true, force: true }); }
});
