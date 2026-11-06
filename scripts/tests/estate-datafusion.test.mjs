import { test, expect } from 'bun:test';
import { mkdtemp, mkdir, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';

const program = resolve(import.meta.dir, '..', 'estate-datafusion.mjs');
const run = (root, sql) => Bun.spawnSync(['bun', program, '--root', root, '--sql', sql], {
  stdout: 'pipe', stderr: 'pipe', maxBuffer: 1024 * 1024,
});

test('discovers Parquet views and queries the current bytes', async () => {
  const root = await mkdtemp(join(tmpdir(), 'bonfyre-parquet-query-'));
  try {
    const family = join(root, 'cms');
    await mkdir(family);
    const file = join(family, 'sample.parquet');
    const create = Bun.spawnSync(['duckdb', ':memory:',
      `COPY (SELECT 7 AS value) TO '${file.replaceAll("'", "''")}' (FORMAT PARQUET);`],
      { stdout: 'pipe', stderr: 'pipe' });
    expect(create.exitCode).toBe(0);
    const tables = run(root, 'SHOW TABLES');
    expect(tables.exitCode).toBe(0);
    expect(JSON.parse(tables.stdout.toString())).toEqual([{ name: 'cms__sample' }]);
    const result = run(root, 'SELECT value FROM cms__sample');
    expect(result.exitCode).toBe(0);
    expect(JSON.parse(result.stdout.toString())).toEqual([{ value: 7 }]);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test('missing representation root fails rather than inventing an empty catalogue', () => {
  const result = run('/bonfyre-missing-parquet-root', 'SHOW TABLES');
  expect(result.exitCode).not.toBe(0);
});

test('normalization collisions fail instead of silently binding the wrong table', async () => {
  const root = await mkdtemp(join(tmpdir(), 'bonfyre-parquet-collision-'));
  try {
    const create = Bun.spawnSync(['duckdb', ':memory:',
      `COPY (SELECT 1 AS value) TO '${join(root, 'a-b.parquet')}' (FORMAT PARQUET); ` +
      `COPY (SELECT 2 AS value) TO '${join(root, 'a_b.parquet')}' (FORMAT PARQUET);`],
      { stdout: 'pipe', stderr: 'pipe' });
    expect(create.exitCode).toBe(0);
    const result = run(root, 'SHOW TABLES');
    expect(result.exitCode).not.toBe(0);
    expect(result.stderr.toString()).toContain('table-name collision');
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
