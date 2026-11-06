import { test, expect } from 'bun:test';
import { Database } from 'bun:sqlite';
import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { compileCorpus } from '../semantic_corpus.mjs';

test('corpus is ordered, bounded, source-addressed, and atomically preserved on refusal', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'bonfyre-semantic-corpus-'));
  const archive = join(dir, 'items.db');
  const output = join(dir, 'corpus.jsonl');
  try {
    const db = new Database(archive);
    db.exec('CREATE TABLE item(source TEXT, key TEXT, title TEXT, publisher TEXT, body TEXT, at INTEGER)');
    const insert = db.query('INSERT INTO item VALUES(?,?,?,?,?,?)');
    insert.run('older', 'one', 'Old', null, 'unused', 1);
    insert.run('new', 'two', 'Title', 'Publisher', 'a'.repeat(400) + 'cut', 2);
    insert.run('hidden', 'three', '', '', 'not indexable', 3);
    db.close();
    expect(await compileCorpus(archive, output, 1)).toEqual({ corpus: 1 });
    expect(JSON.parse(readFileSync(output, 'utf8').trim())).toEqual({
      id: 'o/new/two', text: `Title Publisher ${'a'.repeat(400)}` });
    expect(compileCorpus(archive, output, 0)).rejects.toThrow('positive integer');
    expect(JSON.parse(readFileSync(output, 'utf8').trim()).id).toBe('o/new/two');
    expect(await compileCorpus(archive, output, 9)).toEqual({ corpus: 2 });
    expect(readFileSync(output, 'utf8').trim().split('\n').map(JSON.parse).map(r => r.id))
      .toEqual(['o/new/two', 'o/older/one']);
  } finally { rmSync(dir, { recursive: true, force: true }); }
});
