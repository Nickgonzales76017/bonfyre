import { test, expect } from 'bun:test';
import { Database } from 'bun:sqlite';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fragmentRow, materializeWorldFragments, missingWorldFragments,
  worldFragmentProfiles } from './materialize_world_fragments.mjs';

test('all World grammars derive shared presentation with authored World overrides', () => {
  const profiles = worldFragmentProfiles();
  const world = { world: 'night', subject: '', template: 'ghost:night' };
  for (const grammar of Object.keys(profiles.law.profiles))
    expect(fragmentRow(world, grammar, 10, profiles).status).toBe('published');
  for (const grammar of Object.keys(profiles.law.historical))
    expect(fragmentRow(world, grammar, 10, profiles).status).toBe('published');
  expect(fragmentRow(world, 'search', 10, profiles)).toMatchObject({ surface_grammar: 'discover',
    renderer_family: 'search', renderer_variant: 'query_results', extent: 'page' });
  expect(fragmentRow(world, 'thread', 10, profiles)).toMatchObject({ surface_grammar: 'channels',
    renderer_family: 'thread', renderer_variant: 'stream_topics' });
  expect(() => fragmentRow(world, 'unknown', 10, profiles)).toThrow('unsupported World grammar');
});

test('missing fragments are namespace-scoped and a check has no CMS effect', () => {
  const dir = mkdtempSync(join(tmpdir(), 'bonfyre-world-fragments.'));
  try {
    const dbPath = join(dir, 'cms.db'), db = new Database(dbPath);
    db.exec(`CREATE TABLE world(id INTEGER PRIMARY KEY,namespace TEXT,world TEXT,subject TEXT,template TEXT,grammars TEXT,status TEXT);
      CREATE TABLE world_fragment(namespace TEXT,fragment_id TEXT,status TEXT);
      INSERT INTO world VALUES(1,'one','night','','ghost:night','discover,stage','published');
      INSERT INTO world VALUES(2,'two','night','','ghost:night','discover,stage','published');
      INSERT INTO world_fragment VALUES('one','night:discover','published');`);
    const wanted = missingWorldFragments(db);
    db.close();
    expect(wanted.map(row => `${row.namespace}:${row.item.fragment_id}`))
      .toEqual(['one:night:stage', 'two:night:discover', 'two:night:stage']);
    expect(wanted[0].item).toMatchObject({ ordinal: 11, subject_address: 'world:night',
      renderer_family: 'stage', renderer_variant: 'calendar_timeline', extent: 'page' });
    const calls = [];
    const args = { db: dbPath, cms: '/not-executed', schemas: '/not-executed', check: true };
    const check = materializeWorldFragments(args, command => { calls.push(command); return { exitCode: 0 }; });
    expect(check).toMatchObject({ result: { missing: 3 }, exitCode: 1 });
    expect(calls).toEqual([]);
    const write = materializeWorldFragments({ ...args, check: false }, command => {
      calls.push(command); return { exitCode: 0 };
    });
    expect(write).toMatchObject({ result: { materialized: 3 }, exitCode: 0 });
    expect(calls).toHaveLength(3);
    expect(calls[0].slice(0, 4)).toEqual(['/not-executed', 'entry', 'create', 'world_fragment']);
  } finally { rmSync(dir, { recursive: true, force: true }); }
});
