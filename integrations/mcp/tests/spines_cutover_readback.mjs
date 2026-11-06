#!/usr/bin/env bun
// Read-only recovery witness: compare the retained stopped-Python DB backup to
// a fresh live Bun snapshot. No cutover or historical effects are repeated.
import assert from 'node:assert/strict';
import { Database } from 'bun:sqlite';
import { createHash } from 'node:crypto';
import { copyFileSync, mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { homedir, tmpdir } from 'node:os';
import { join } from 'node:path';
import { collectSpinesWitness } from './spines_live_witness.mjs';
import { SPINES_TABLES } from '../spines_store.mjs';
const proof = JSON.parse(readFileSync(new URL('./spines_cutover.json', import.meta.url), 'utf8'));
const backup = proof.previous.database_backups[0];
const hash = bytes => createHash('sha256').update(bytes).digest('hex');
assert.equal(hash(readFileSync(backup.path)), backup.sha256);
const token = readFileSync(join(homedir(), '.bonfyre/mcp/token'), 'utf8').trim();
const current = await collectSpinesWitness({ token, samples: 0 });
assert.equal(current.runtime, 'bun');
// SQLite may need WAL bookkeeping even for a read-only backup. Give it an
// isolated copy; never allow those writes to touch the retained rollback body.
const temporary = mkdtempSync(join(tmpdir(), 'spines-readback-'));
let db;
try {
  const copy = join(temporary, 'snapshot.sqlite3');
  copyFileSync(backup.path, copy);
  assert.equal(hash(readFileSync(copy)), backup.sha256);
  db = new Database(copy);
  for (const name of SPINES_TABLES) {
    const digest = createHash('sha256'); let rows = 0;
    for (const row of db.query(`SELECT rowid AS _witness_rowid,* FROM ${name} ORDER BY rowid`).iterate()) {
      assert(++rows <= 100000); digest.update(JSON.stringify(row)).update('\n');
    }
    assert.deepEqual(current.snapshot.tables[name], { rows, sha256: digest.digest('hex') }, `${name} differs from stopped Python backup`);
  }
} finally { db?.close(); rmSync(temporary, { recursive: true, force: true }); }
assert.equal(hash(readFileSync(backup.path)), backup.sha256);
assert.equal(hash(readFileSync(proof.cli.path)), proof.cli.sha256);
const { files, ...snapshot } = current.snapshot;
console.log(JSON.stringify({ ...current, schema: 'bonfyre.spines-cutover-readback.v1', measured_at: new Date().toISOString(),
  backup_sha256: backup.sha256, all_24_tables_equal_to_stopped_python: true, cli_unchanged: true,
  snapshot: { ...snapshot, owned_files: Object.keys(files).length, owned_file_map_sha256: hash(JSON.stringify(files)) },
  evidence_scope: 'Fresh Bun readback and stopped-Python database comparison; current owned files and receipt chains verified. No benchmark or remote deployment inferred.' }, null, 2));
