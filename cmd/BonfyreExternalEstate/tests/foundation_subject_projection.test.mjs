import { test, expect } from 'bun:test';
import { Database } from 'bun:sqlite';
import { mkdtempSync, rmSync, readFileSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

test('foundation reader closure feeds exact estate collections without candidate leakage', () => {
  const dir = mkdtempSync(join(tmpdir(), 'bonfyre-foundation-projection.'));
  try {
    const fabricPath = join(dir, 'fabric.db'), frontierPath = join(dir, 'frontier.db');
    const cmsPath = join(dir, 'cms.db'), foundationPath = join(dir, 'foundation.db');
    const fabric = new Database(fabricPath);
    fabric.exec(`CREATE TABLE subject_bundle(subject TEXT,scope TEXT,bundle_digest TEXT,closure_digest TEXT,
      scene_digest TEXT,compiled_at TEXT,counts_json TEXT,payload_json TEXT,PRIMARY KEY(subject,scope));
      CREATE TABLE subject_reader(subject TEXT,scope TEXT,census_source TEXT,root_id TEXT,channel_id TEXT,
      material TEXT,protocol TEXT,state TEXT,last_contributed TEXT,contributions INTEGER,first_seen TEXT);
      CREATE TABLE subject_route(subject TEXT,scope TEXT,route TEXT);
      CREATE TABLE subject_route_item(subject TEXT,scope TEXT,route TEXT);
      INSERT INTO subject_bundle VALUES('institution:publisher:kept','public','old','old','','','{}','{}');
      INSERT INTO subject_reader VALUES('institution:publisher:kept','public','kept','r0','c0','news','RSS','admitted','',0,'');`);
    fabric.close();
    const frontier = new Database(frontierPath);
    frontier.exec(`CREATE TABLE channel(channel_id TEXT,census_source TEXT,root_id TEXT,material TEXT,
      protocol TEXT,state TEXT,last_contributed TEXT,contributions INTEGER,first_seen TEXT);
      INSERT INTO channel VALUES('c-news','news-source','root-news','news','RSS','admitted','',0,'2026-01-01');
      INSERT INTO channel VALUES('c-music','music-source','root-music','music','RSS','active','',0,'2026-01-01');
      INSERT INTO channel VALUES('c-hidden','hidden-source','root-hidden','news','RSS','candidate','',0,'2026-01-01');`);
    frontier.close();
    const cms = new Database(cmsPath);
    cms.exec(`CREATE TABLE source_collection(id INTEGER PRIMARY KEY,collection_id TEXT,subject_address TEXT,
      source_selector TEXT,material_kind TEXT,refresh_policy TEXT,status TEXT)`);
    const insert = cms.query('INSERT INTO source_collection VALUES(NULL,?,?,?,?,?,?)');
    for (const [family, source] of [['news', 'news-source'], ['music', 'music-source']])
      insert.run(`estate-family-${family}`, 'estate:aurekai',
        JSON.stringify({ grouping: 'family', value: family, source_refs: [source] }),
        JSON.stringify(['publication_occurrence']), 'hourly', 'published');
    cms.close();
    const foundation = Bun.spawnSync(['bun', new URL('../tools/materialize_foundation_subjects.mjs', import.meta.url).pathname,
      '--fabric-db', fabricPath, '--frontier-db', frontierPath, '--out', foundationPath,
      '--territory', 'live', '--territory', 'life']);
    expect(foundation.exitCode).toBe(0);
    expect(JSON.parse(new TextDecoder().decode(foundation.stdout)).admitted_sources).toBe(2);
    const generated = new Database(foundationPath);
    expect(generated.query('SELECT COUNT(*) AS n FROM subject_bundle').get().n).toBe(3);
    expect(generated.query("SELECT COUNT(*) AS n FROM subject_reader WHERE subject='institution:aurekai:live'").get().n).toBe(2);
    expect(generated.query("SELECT COUNT(*) AS n FROM subject_reader WHERE census_source='hidden-source'").get().n).toBe(0);
    expect(generated.query("SELECT COUNT(*) AS n FROM subject_reader WHERE subject='institution:publisher:kept'").get().n).toBe(1);
    generated.close();
    const items = join(dir, 'items.jsonl'), page = join(dir, 'page.json'), surface = join(dir, 'surface.json');
    writeFileSync(items, [
      { source: 'news-source', family: 'news', key: 'n1', kind: 'story', title: 'Exact Estate News',
        href: 'https://news.test/n1', at: 100 },
      { source: 'music-source', family: 'music', key: 'm1', kind: 'audio', title: 'Exact Estate Music',
        href: 'https://music.test/m1', at: 200 },
      { source: 'hidden-source', family: 'news', key: 'h1', kind: 'story', title: 'Candidate Must Not Leak',
        href: 'https://hidden.test/h1', at: 300 },
    ].map(row => JSON.stringify(row)).join('\n') + '\n');
    writeFileSync(page, JSON.stringify({ regions: [
      { role: 'lead', material: 'publication', topology: 'lead', language: 'story', min_items: 1, max_items: 8 },
      { role: 'listen', material: 'audio', topology: 'playlist', language: 'tracks', min_items: 1, max_items: 8 },
    ], programs: [
      { route: '/', territory: 'live', title: 'Live', lede: 'Live route', character: 'signal',
        cannot_infer: 'fixture', families: ['news'], source_limit: 8, regions: ['lead'] },
      { route: '/listen', territory: 'any', title: 'Listen', lede: 'Listen route', character: 'sound',
        cannot_infer: 'fixture', families: ['music'], source_limit: 8, regions: ['listen'] },
    ] }));
    writeFileSync(surface, JSON.stringify({ programs: [] }));
    const projection = Bun.spawnSync(['bun', new URL('../tools/compile_subject_projection.mjs', import.meta.url).pathname,
      '--fabric-db', foundationPath, '--cms-db', cmsPath, '--items', items,
      '--page-composition', page, '--surface-programs', surface]);
    expect(projection.exitCode).toBe(0);
    const db = new Database(foundationPath, { readonly: true });
    const home = db.query("SELECT basis,collections_json,reader_count,item_count FROM subject_route WHERE subject=? AND route='/'")
      .get('institution:aurekai:live');
    expect(home).toMatchObject({ basis: 'estate_source_collection_exact', collections_json: '["estate-family-news"]',
      reader_count: 1, item_count: 1 });
    const title = db.query(`SELECT m.title FROM subject_route_item r JOIN subject_material m ON
      m.subject=r.subject AND m.scope=r.scope AND m.source=r.source AND m.item_key=r.item_key AND m.material=r.material
      WHERE r.subject=? AND r.route='/' LIMIT 1`).get('institution:aurekai:live');
    expect(title.title).toBe('Exact Estate News');
    expect(db.query("SELECT COUNT(*) AS n FROM subject_material WHERE title='Candidate Must Not Leak'").get().n).toBe(0);
    expect(db.query("SELECT collections_json,item_count FROM subject_route WHERE subject=? AND route='/listen'")
      .get('institution:aurekai:live')).toMatchObject({ collections_json: '["estate-family-music"]', item_count: 1 });
    db.close();
    const summary = JSON.parse(new TextDecoder().decode(projection.stdout));
    expect(summary.basis_counts.estate_source_collection_exact).toBe(3);
    expect(readFileSync(foundationPath).length).toBeGreaterThan(0);
  } finally { rmSync(dir, { recursive: true, force: true }); }
});
