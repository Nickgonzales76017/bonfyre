import { test, expect } from 'bun:test';
import { Database } from 'bun:sqlite';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { compileChangeImpact, deltaWrites } from '../tools/compile_source_change_impact.mjs';
import { sourceRoots } from '../tools/source_identity.mjs';

test('FactDelta reverse dependencies measure closure and published Scene reuse, not visibility', () => {
  const dir = mkdtempSync(join(tmpdir(), 'bonfyre-source-impact.'));
  let frontier, scenes;
  try {
    const sources = join(dir, 'sources.jsonl'), delta = join(dir, 'delta.json'), wrong = join(dir, 'wrong.json');
    writeFileSync(sources, JSON.stringify({ url: 'https://www.example.org/news?utm_source=test' }) + '\n');
    writeFileSync(delta, JSON.stringify({ schema: 'bonfyre-fact-delta.v1', writes: [
      'ForeignTwin:https://example.org/news', 'ForeignTwin:https://missing.test/other',
      'ForeignTwin:https://missing.test/other', 'Subject:else' ] }));
    writeFileSync(wrong, JSON.stringify({ schema: 'another.v1', writes: ['ForeignTwin:https://example.org/news'] }));
    expect(deltaWrites(wrong)).toEqual([]);
    expect(deltaWrites(join(dir, 'absent.json'))).toEqual([]);
    const frontierPath = join(dir, 'frontier.sqlite'), scenePath = join(dir, 'scenes.sqlite');
    frontier = new Database(frontierPath);
    frontier.exec('CREATE TABLE closure_dependency (dependency_ref TEXT, subject TEXT, scope TEXT, basis TEXT)');
    const add = frontier.query('INSERT INTO closure_dependency VALUES (?, ?, ?, ?)');
    add.run('ForeignTwin:https://example.org/news', 'school:1', 'public', 'foreign_twin');
    add.run('ForeignTwin:https://example.org/news', 'school:1', 'public', 'foreign_twin');
    add.run('ForeignTwin:https://example.org/news', 'school:1', 'public', 'other_basis');
    add.run('ForeignTwin:https://example.org/news', 'club:2', 'public', 'foreign_twin');
    scenes = new Database(scenePath);
    scenes.exec('CREATE TABLE surface_scene (scene_key TEXT, subject_address TEXT, actor_scope TEXT, status TEXT)');
    const addScene = scenes.query('INSERT INTO surface_scene VALUES (?, ?, ?, ?)');
    addScene.run('scene:school', 'school:1', 'public', 'published');
    addScene.run('scene:club-draft', 'club:2', 'public', 'draft');
    const roots = sourceRoots([`TEST=${sources}`]);
    const result = compileChangeImpact(frontier, scenes, roots, [wrong, delta]);
    expect(result.summary).toMatchObject({ fact_deltas_read: 1, roots_with_measured_impact: 1,
      affected_closures: 3, affected_scenes: 1, unmapped_topics: 1, unmapped_topic_occurrences: 2 });
    expect(result.measurements[0]).toMatchObject({ root_address: 'https://example.org/', subject_reuse: 2, scene_reuse: 1 });
    expect(result.receipts[0]).toMatchObject({ affected_subjects: 2, affected_scopes: 1,
      dependency_bases: ['foreign_twin', 'other_basis'], provenance: ['TEST'],
      truth: 'reverse_dependency_impact_not_visible_material_contribution' });
    expect(result.receipts[0].cannot_infer).toContain('affected scene -> source appeared in the rendered extent');
    const incompleteScene = new Database(':memory:');
    try {
      incompleteScene.exec('CREATE TABLE surface_scene (scene_key TEXT)');
      expect(compileChangeImpact(frontier, incompleteScene, roots, [delta]).measurements[0].scene_reuse).toBe(0);
    } finally { incompleteScene.close(); }
    const out = join(dir, 'impact.jsonl'), receipts = join(dir, 'receipts.jsonl');
    const child = Bun.spawnSync(['bun', new URL('../tools/compile_source_change_impact.mjs', import.meta.url).pathname,
      '--source-input', `TEST=${sources}`, '--frontier-db', frontierPath, '--scene-db', scenePath,
      '--fact-delta', delta, '--out', out, '--receipts', receipts]);
    expect(child.exitCode).toBe(0);
    expect(JSON.parse(readFileSync(out, 'utf8'))).toEqual(result.measurements[0]);
    expect(JSON.parse(readFileSync(receipts, 'utf8'))).toEqual(result.receipts[0]);
  } finally { frontier?.close(); scenes?.close(); rmSync(dir, { recursive: true, force: true }); }
});
