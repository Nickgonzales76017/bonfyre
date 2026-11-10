import { test, expect } from 'bun:test';
import { Database } from 'bun:sqlite';
import { compileLegacySceneSources } from '../tools/compile_scene_legacy_sources.mjs';

test('legacy Scene source list is capped and reports every lost census ID', () => {
  const db = new Database(':memory:');
  try {
    db.exec('CREATE TABLE internet_source (id INTEGER, source TEXT, url TEXT, family TEXT, kind TEXT, tier TEXT, status TEXT)');
    const insert = db.query('INSERT INTO internet_source VALUES (?,?,?,?,?,?,?)');
    insert.run(1, 'tool-feed', 'https://tool.test/feed', 'news', 'rss', 'warm', 'published');
    insert.run(2, 'tool-api', 'https://www.tool.test/api', 'commerce', 'json', 'warm', 'published');
    insert.run(3, 'hidden', 'https://tool.test/private', 'commerce', 'json', 'warm', 'draft');
    const scenes = [{ schema: 'bonfyre.scene-source-receipt.v1', scene_key: 'scene:tool',
      subject_address: '/s/tool', actor_scope: 'public', source_roots: [
        { root_id: 'root:tool', root_address: 'https://tool.test/' },
        { root_id: 'root:missing', root_address: 'https://missing.test/' }] }];
    const result = compileLegacySceneSources(scenes, db, 1);
    expect(result.rows[0].sources).toEqual(['tool-feed']);
    expect(result.rows[0].compatibility_loss.dropped_sources).toEqual(['tool-api']);
    expect(result.rows[0].unmatched_scene_roots[0].root_id).toBe('root:missing');
    expect(result.summary).toMatchObject({ scenes: 1, matched_sources: 2, emitted_sources: 1,
      dropped_for_legacy_limit: 1, unmatched_roots: 1 });
    expect(result.rows[0].cannot_infer).toContain('dropped legacy source -> source is unimportant');
  } finally { db.close(); }
});
