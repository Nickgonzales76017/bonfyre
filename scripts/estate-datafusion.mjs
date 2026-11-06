#!/usr/bin/env bun
// Query the current Parquet representation tree without a generated path list.
import { readdir, stat } from 'node:fs/promises';
import { homedir } from 'node:os';
import { join, relative, resolve, sep } from 'node:path';

const defaultRoot = join(homedir(), '.bonfyre', 'estate-local', 'representations', 'parquet');
const args = process.argv.slice(2);
let root = defaultRoot;
let sql = 'SHOW TABLES';
for (let i = 0; i < args.length; i++) {
  if (args[i] === '--root' && args[i + 1]) root = args[++i];
  else if (args[i] === '--sql' && args[i + 1]) sql = args[++i];
  else {
    process.stderr.write('usage: bun scripts/estate-datafusion.mjs [--root parquet-directory] [--sql query]\n');
    process.exit(2);
  }
}

const rootPath = resolve(root);
const quoteName = value => `"${value.replaceAll('"', '""')}"`;
const quoteValue = value => `'${value.replaceAll("'", "''")}'`;
const tableName = path => relative(rootPath, path)
  .slice(0, -'.parquet'.length)
  .split(sep)
  .map(part => part.replace(/[^A-Za-z0-9_]/g, '_'))
  .join('__');

async function discover(dir, out) {
  for (const item of await readdir(dir, { withFileTypes: true })) {
    const path = join(dir, item.name);
    if (item.isDirectory()) await discover(path, out);
    else if (item.isFile() && item.name.endsWith('.parquet')) out.push(path);
  }
}

try {
  if (!(await stat(rootPath)).isDirectory()) throw new Error(`not a directory: ${rootPath}`);
  const paths = [];
  await discover(rootPath, paths);
  paths.sort();
  const seen = new Map();
  const views = paths.map(path => {
    const name = tableName(path);
    if (seen.has(name)) throw new Error(`table-name collision ${name}: ${seen.get(name)} and ${path}`);
    seen.set(name, path);
    return `CREATE VIEW ${quoteName(name)} AS SELECT * FROM read_parquet(${quoteValue(path)});`;
  });
  const child = Bun.spawnSync(['duckdb', '-json', ':memory:', `${views.join('\n')}\n${sql}`], {
    stdout: 'pipe', stderr: 'pipe', maxBuffer: 64 * 1024 * 1024,
  });
  process.stdout.write(child.stdout);
  process.stderr.write(child.stderr);
  process.exit(child.exitCode ?? 1);
} catch (error) {
  process.stderr.write(`estate-datafusion: ${error.message}\n`);
  process.exit(1);
}
