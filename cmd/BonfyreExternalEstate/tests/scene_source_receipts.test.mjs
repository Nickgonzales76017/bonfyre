import { test, expect } from 'bun:test';
import { Database } from 'bun:sqlite';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { resolve } from 'node:path';
import { compileSceneReceipts } from '../tools/compile_scene_source_receipts.mjs';
import { canonicalUrl, sourceRoots, rootForTopic } from '../tools/source_identity.mjs';

test('canonical source coordinate preserves provenance and strips tracking', () => {
  expect(canonicalUrl('HTTPS://www.Tool.Test:443//catalog?utm_source=x&a=1#frag'))
    .toBe('https://tool.test/catalog?a=1');
});

test('persisted Scene joins only exact ForeignTwin roots from the closure', () => {
  const dir = mkdtempSync(resolve(tmpdir(), 'bonfyre-scene-receipts-test.'));
  const frontierPath = resolve(dir, 'frontier.sqlite'), scenesPath = resolve(dir, 'scenes.sqlite');
  const frontier = new Database(frontierPath), scenes = new Database(scenesPath);
  try {
    const inventory = resolve(dir, 'sources.jsonl');
    writeFileSync(inventory, JSON.stringify({ url: 'https://www.tool.test/feed', source: 'tool' }) + '\n');
    const roots = sourceRoots([`PUBLIC=${inventory}`]);
    expect(rootForTopic(roots, 'https://tool.test/item/1')?.provenance.has('PUBLIC')).toBe(true);
    frontier.exec('CREATE TABLE closure_dependency (subject TEXT, scope TEXT, dependency_ref TEXT, basis TEXT)');
    const put = frontier.query('INSERT INTO closure_dependency VALUES (?,?,?,?)');
    put.run('/s/tool', 'public', 'ForeignTwin:https://www.tool.test/item/1', 'retained');
    put.run('/s/tool', 'public', 'ForeignTwin:https://unknown.test/thing', 'retained');
    put.run('/s/tool', 'public', 'external:candidate', 'external_candidate');
    put.run('/s/tool', 'public', 'Actor:someone', 'owned');
    scenes.exec('CREATE TABLE surface_scene (scene_key TEXT, subject_address TEXT, actor_scope TEXT, site_id TEXT, world TEXT, lenses TEXT, operations TEXT, dependencies TEXT, source_digest TEXT, status TEXT)');
    scenes.query('INSERT INTO surface_scene VALUES (?,?,?,?,?,?,?,?,?,?)').run('scene:tool', '/s/tool', 'public',
      'site:one', 'world:one', '["map","map","agenda"]', '{"apertures":["save","read"]}', '[]', 'digest:one', 'published');
    const result = compileSceneReceipts(frontier, scenes, roots);
    expect(result.receipts).toHaveLength(1);
    const row = result.receipts[0];
    expect(row.source_roots).toHaveLength(1);
    expect(row.source_roots[0].root_address).toBe('https://tool.test/');
    expect(row.source_roots[0].provenance).toEqual(['PUBLIC']);
    expect(row.external_candidate_dependencies).toHaveLength(1);
    expect(row.other_dependency_count).toBe(1);
    expect(row.lenses).toEqual(['agenda', 'map']);
    expect(row.action_apertures).toEqual(['read', 'save']);
    expect(result.summary).toMatchObject({ scenes: 1, distinct_source_roots: 1,
      unresolved_foreign_topics: 1, max_scene_reuse: 1 });
    expect(row.truth).toBe('scene_dependency_receipt_not_render_visibility_or_source_authority');
    const out = resolve(dir, 'receipts.jsonl'), summary = resolve(dir, 'summary.json');
    const run = Bun.spawnSync(['bun', resolve(import.meta.dir, '../tools/compile_scene_source_receipts.mjs'),
      '--source-input', `PUBLIC=${inventory}`, '--frontier-db', frontierPath,
      '--scene-db', scenesPath, '--out', out, '--summary', summary]);
    expect(run.exitCode).toBe(0);
    expect(JSON.parse(readFileSync(out, 'utf8')).source_roots[0].root_id).toBe(row.source_roots[0].root_id);
    expect(JSON.parse(readFileSync(summary, 'utf8')).unresolved_foreign_topics).toBe(1);
  } finally { frontier.close(); scenes.close(); rmSync(dir, { recursive: true, force: true }); }
});
