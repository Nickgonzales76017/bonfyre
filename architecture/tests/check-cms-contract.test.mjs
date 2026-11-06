import { test, expect } from 'bun:test';
import { Database } from 'bun:sqlite';
import { mkdtemp, mkdir, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';

const program = resolve(import.meta.dir, '..', 'check-cms-contract.mjs');

test('CMS contract accepts equal sets and reports both drift directions', async () => {
  const root = await mkdtemp(join(tmpdir(), 'bonfyre-cms-contract-'));
  try {
    const dbPath = join(root, 'cms.db');
    const schemas = join(root, 'content-types');
    await mkdir(schemas);
    await writeFile(join(schemas, 'alpha.json'), '{}\n');
    const db = new Database(dbPath);
    db.exec('CREATE TABLE _content_types (name TEXT PRIMARY KEY); INSERT INTO _content_types VALUES (\'alpha\')');
    const run = () => Bun.spawnSync(['bun', program], {
      env: { ...process.env, BONFYRE_CMS_DB: dbPath, BONFYRE_CMS_SCHEMAS: schemas },
      stdout: 'pipe', stderr: 'pipe',
    });
    const valid = run();
    expect(valid.exitCode).toBe(0);
    expect(valid.stdout.toString()).toContain('1 content types');
    db.exec("INSERT INTO _content_types VALUES ('orphan')");
    await writeFile(join(schemas, 'unloaded.json'), '{}\n');
    const invalid = run();
    expect(invalid.exitCode).toBe(1);
    expect(invalid.stderr.toString()).toContain('registered with no schema file: orphan');
    expect(invalid.stderr.toString()).toContain('schema file never registered: unloaded');
    db.close();
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
