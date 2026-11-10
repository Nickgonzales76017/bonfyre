#!/usr/bin/env bun
// Files and registered CMS content types must name the same set.
import { Database } from 'bun:sqlite';
import { readdir, stat } from 'node:fs/promises';

const databasePath = process.env.BONFYRE_CMS_DB ?? '/var/lib/bonfyre/cms/bonfyre_cms.db';
const schemasPath = process.env.BONFYRE_CMS_SCHEMAS ?? '/var/lib/bonfyre/cms/content-types';

async function isKind(path, kind) {
  try {
    const info = await stat(path);
    return kind === 'file' ? info.isFile() : info.isDirectory();
  } catch {
    return false;
  }
}

if (!(await isKind(databasePath, 'file'))) {
  process.stderr.write(`no CMS store at ${databasePath}; set BONFYRE_CMS_DB\n`);
  process.exit(1);
}
if (!(await isKind(schemasPath, 'directory'))) {
  process.stderr.write(`no schema directory at ${schemasPath}; set BONFYRE_CMS_SCHEMAS\n`);
  process.exit(1);
}

try {
  const files = new Set((await readdir(schemasPath)).filter(name => name.endsWith('.json'))
    .map(name => name.slice(0, -'.json'.length)));
  const db = new Database(databasePath, { readonly: true, strict: true });
  let registered;
  try {
    registered = new Set(db.query('SELECT name FROM _content_types').all().map(row => row.name));
  } finally {
    db.close();
  }
  const orphanRows = [...registered].filter(name => !files.has(name)).sort();
  const unloadedFiles = [...files].filter(name => !registered.has(name)).sort();
  for (const name of orphanRows)
    process.stderr.write(`  registered with no schema file: ${name} (its only copy is inside the database)\n`);
  for (const name of unloadedFiles)
    process.stderr.write(`  schema file never registered: ${name}\n`);
  if (orphanRows.length || unloadedFiles.length) {
    process.stderr.write(`CMS contract mismatch: ${files.size} files, ${registered.size} registered\n`);
    process.exit(1);
  }
  process.stdout.write(`CMS contract agrees: ${files.size} content types, files and store identical\n`);
} catch (error) {
  process.stderr.write(`CMS contract check failed: ${error.message}\n`);
  process.exit(1);
}
