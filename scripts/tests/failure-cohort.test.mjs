import { test, expect } from 'bun:test';
import { Database } from 'bun:sqlite';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';

const program = resolve(import.meta.dir, '..', 'failure-cohort.mjs');

test('closable work ranks separately from human-bound failures', async () => {
  const root = await mkdtemp(join(tmpdir(), 'bonfyre-failure-cohort-'));
  const path = join(root, 'operator.db');
  try {
    const db = new Database(path);
    db.exec('CREATE TABLE goals(id TEXT,title TEXT,state TEXT,attempts INTEGER,last_error TEXT,updated_at TEXT)');
    const insert = db.query('INSERT INTO goals VALUES (?,?,?,?,?,?)');
    insert.run('a', 'Credit', 'retry', 100, 'insufficient_quota', '2026-09-30');
    insert.run('b', 'Tool A', 'retry', 3, 'unknown tool', '2026-09-30');
    insert.run('c', 'Tool B', 'queued', 2, 'tool_not_found', '2026-09-30');
    insert.run('d', 'No error', 'failed', 1, '', '2026-09-30');
    db.close();
    const result = Bun.spawnSync(['bun', program, '--json'], {
      env: { ...process.env, BONFYRE_OPERATOR_DB: path },
      stdout: 'pipe', stderr: 'pipe',
    });
    expect(result.exitCode).toBe(0);
    const report = JSON.parse(result.stdout.toString());
    expect(report.goals_examined).toBe(4);
    expect(report.attempts_examined).toBe(106);
    expect(report.clusters[0].cause).toBe('tool_missing');
    expect(report.clusters[0].goals).toBe(2);
    expect(report.awaiting_person[0].cause).toBe('no_credit');
    expect(report.awaiting_person[0].attempts).toBe(100);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
