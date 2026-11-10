import { test, expect } from 'bun:test';
import { mkdtempSync, writeFileSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { Database } from 'bun:sqlite';
import { compilePlan } from '../tools/plan_fragment_migration.mjs';
import { main as stage, prepareDrafts } from '../tools/stage_fragment_migration.mjs';
import { main as buildIndex, signature } from '../tools/build_fragment_semantic_index.mjs';
import { classify, textNodes, compileCandidates } from '../tools/ast_literal_candidates.mjs';

test('Clang JSON-sequence literals retain source identity and human text nodes', async () => {
  const source = Buffer.from('void f(){const char *s="Hello there friend";}\n');
  const begin = source.indexOf('"Hello');
  const ast = { kind: 'FunctionDecl', name: 'f', inner: [
    { kind: 'StringLiteral', value: '"Hello there friend"', range: { begin: { offset: begin }, end: { offset: begin, tokLen: 20 } } },
  ] };
  const result = await compileCandidates(`${JSON.stringify(ast)}\n${JSON.stringify({ kind: 'Ignored' })}`, source, '/tmp/fixture.c');
  expect(result.candidate_count).toBe(1);
  expect(result.candidates[0].role).toBe('copy');
  expect(result.candidates[0].text_nodes).toEqual(['Hello there friend']);
  expect(result.candidates[0].source_ref).toContain('#f@L1:C');
  expect(classify('SELECT * FROM table')).toBe('sql');
  expect(await textNodes('<p>Tea &amp; biscuits</p><script>do not include this text</script>', 'markup')).toEqual(['Tea & biscuits']);
});

test('unmapped literals remain review, mapped literals stay draft', () => {
  const candidates = { candidates: [
    { candidate_id: 'a', function: 'known', role: 'copy', line: 8, source_ref: 'source:x#known@L8:C1', source_digest: 'abc', text_nodes: ['First   phrase', 'FIRST phrase'] },
    { candidate_id: 'b', function: 'unknown', role: 'markup', line: 9, source_ref: 'source:x#unknown@L9:C1', source_digest: 'def', text_nodes: ['Needs review'] },
  ] };
  const plan = compilePlan(candidates, { mappings: { known: { page: 'home', audience: 'public', slot: 'hero' } } }, { manifest: '/tmp/literals.json', manifestText: '{}', map: '/tmp/map.json' });
  expect(plan.draft_count).toBe(2);
  expect(plan.review_count).toBe(1);
  expect(plan.duplicate_equivalence_groups).toBe(1);
  expect(plan.drafts.every(draft => draft.status === 'draft')).toBe(true);
  expect(() => prepareDrafts({ drafts: [plan.drafts[0], plan.drafts[0]] })).toThrow('duplicate coordinate');
});

test('CMS imports draft batch atomically and a repeat skips without overwriting', () => {
  const dir = mkdtempSync(join(tmpdir(), 'bonfyre-fragment-native-'));
  const root = resolve(import.meta.dir, '../../..');
  const schemas = join(root, 'cmd/BonfyreCMS/content-types');
  const dbPath = join(dir, 'cms.db'), planPath = join(dir, 'plan.json');
  try {
    const plan = compilePlan({ candidates: [
      { candidate_id: 'a', function: 'known', role: 'copy', line: 8, source_ref: 'source:x#known@L8:C1', source_digest: 'abc', text_nodes: ['First phrase', 'Second phrase'] },
    ] }, { mappings: { known: { page: 'home', audience: 'public', slot: 'hero' } } }, { manifest: join(dir, 'literals.json'), manifestText: '{}', map: join(dir, 'map.json') });
    writeFileSync(planPath, JSON.stringify(plan));
    expect(stage([planPath, '--db', dbPath, '--schemas', schemas])).toBe(0);
    expect(stage([planPath, '--db', dbPath, '--schemas', schemas, '--apply'])).toBe(0);
    expect(stage([planPath, '--db', dbPath, '--schemas', schemas, '--apply'])).toBe(0);
    const db = new Database(dbPath, { readonly: true });
    try {
      const rows = db.query("SELECT coordinate,status FROM site_fragment WHERE namespace='aurekai' ORDER BY coordinate").all();
      expect(rows).toHaveLength(2);
      expect(rows.every(row => row.status === 'draft')).toBe(true);
    } finally { db.close(); }
    const work = join(dir, 'semantic-index');
    expect(buildIndex(['--db', dbPath, '--namespace', 'aurekai', '--status', 'draft', '--work-dir', work, '--vec-db', join(dir, 'vectors.db'), '--backend', 'hash', '--dims', '16'])).toBe(0);
    const manifest = JSON.parse(readFileSync(join(work, 'manifest.json'), 'utf8'));
    expect(manifest.queried_count).toBe(2);
    expect(manifest.indexable_count).toBe(2);
    expect(() => buildIndex(['--db', dbPath, '--namespace', 'aurekai', '--status', 'draft', '--work-dir', work, '--vec-db', join(dir, 'vectors.db')])).toThrow('already exists');
  } finally { rmSync(dir, { recursive: true, force: true }); }
});

test('semantic signature excludes migration-control tags', () => {
  expect(signature({ heading: 'A  title', body: 'A title', semantic_tags: '["migration","function:f","geography"]' })).toBe('A title\ngeography');
});
