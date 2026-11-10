#!/usr/bin/env bun
// Read-only source witness: all verification writes are confined to a disposable copy.
import { Database } from 'bun:sqlite';
import { createHash } from 'node:crypto';
import { closeSync, constants, existsSync, fstatSync, lstatSync, mkdirSync, mkdtempSync, openSync, readSync, realpathSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, isAbsolute, join, relative, sep } from 'node:path';
import { createV6Store } from '../integrations/mcp/v6_store.mjs';

const MAX_FILES = 100000, MAX_FILE_BYTES = 64 * 1024 * 1024, MAX_TOTAL_BYTES = 512 * 1024 * 1024;
const TABLES = ['receipts', 'estates', 'bfp_builds', 'cutovers', 'graph_versions', 'sparse_snapshots'];
const sha = bytes => createHash('sha256').update(bytes).digest('hex');
const inside = (root, path) => {
  const rel = relative(root, path);
  return rel === '' || (rel !== '..' && !rel.startsWith(`..${sep}`) && !isAbsolute(rel));
};
function readPinned(path, maximum) {
  const fd = openSync(path, constants.O_RDONLY | (constants.O_NOFOLLOW ?? 0) | (constants.O_NONBLOCK ?? 0));
  try {
    const first = fstatSync(fd), named = lstatSync(path);
    if (!first.isFile() || !named.isFile() || named.isSymbolicLink() || first.size > maximum || first.ino !== named.ino || first.dev !== named.dev)
      throw new Error('source file identity or budget changed');
    const bytes = Buffer.alloc(first.size); let read = 0;
    while (read < bytes.length) {
      const n = readSync(fd, bytes, read, bytes.length - read, null);
      if (!n) throw new Error('source file truncated during witness');
      read += n;
    }
    const last = fstatSync(fd), namedLast = lstatSync(path);
    if (first.size !== last.size || first.mtimeMs !== last.mtimeMs || first.ctimeMs !== last.ctimeMs ||
        namedLast.ino !== first.ino || namedLast.dev !== first.dev) throw new Error('source file changed during witness');
    return bytes;
  } finally { closeSync(fd); }
}
function fingerprint(path) { return existsSync(path) ? sha(readPinned(path, MAX_FILE_BYTES)) : null; }
function sourceSignature(dbPath) { return Object.fromEntries(['', '-wal', '-shm'].map(suffix => [suffix || 'main', fingerprint(`${dbPath}${suffix}`)])); }
const assert = (condition, message) => { if (!condition) throw new Error(message); };

export function witness({ home, root = process.cwd() }) {
  if (!home) throw new Error('pass --home PATH');
  home = realpathSync(home); root = realpathSync(root);
  const dbPath = join(home, 'v6.sqlite3');
  assert(lstatSync(dbPath).isFile(), 'V6 database missing');
  const before = sourceSignature(dbPath), scratch = realpathSync(mkdtempSync(join(tmpdir(), 'bonfyre-v6-history-witness-')));
  const copyHome = join(scratch, 'v6'); mkdirSync(copyHome, { mode: 0o700 });
  let source, copy, store;
  try {
    const copiedDb = join(copyHome, 'v6.sqlite3');
    source = new Database(dbPath, { readonly: true, strict: true });
    source.query('VACUUM INTO ?').run(copiedDb);
    source.close(); source = null;
    copy = new Database(copiedDb, { strict: true });
    const fileProof = [], dedupe = new Set(); let totalBytes = 0;
    for (const table of TABLES) {
      for (const row of copy.query(`SELECT rowid AS witness_rowid,path FROM ${table} ORDER BY rowid LIMIT ${MAX_FILES + 1}`).all()) {
        if (fileProof.length > MAX_FILES) throw new Error('V6 historical file count budget exceeded');
        const original = realpathSync(row.path);
        if (!inside(home, original) || lstatSync(original).isSymbolicLink()) throw new Error(`historical ${table} path escapes owned home`);
        const rel = relative(home, original), target = join(copyHome, rel);
        if (!dedupe.has(original)) {
          const bytes = readPinned(original, MAX_FILE_BYTES); totalBytes += bytes.length;
          if (totalBytes > MAX_TOTAL_BYTES) throw new Error('V6 historical file byte budget exceeded');
          mkdirSync(dirname(target), { recursive: true, mode: 0o700 });
          writeFileSync(target, bytes, { flag: 'wx', mode: 0o600 });
          fileProof.push({ path: original, copied: target, sha256: sha(bytes), bytes: bytes.length });
          dedupe.add(original);
        }
        copy.query(`UPDATE ${table} SET path=? WHERE rowid=?`).run(target, row.witness_rowid);
      }
    }
    copy.close(); copy = null;
    store = createV6Store({ home: copyHome, root });
    const verification = store.verifyReceipts();
    assert(verification.verified && verification.receipts === 31, 'historical V6 receipt chain did not verify all 31 records');
    const counts = Object.fromEntries(TABLES.map(table => [table, store.db.query(`SELECT count(*) AS n FROM ${table}`).get().n]));
    for (const proof of fileProof) {
      assert(fingerprint(proof.path) === proof.sha256, 'live source file changed after snapshot');
      assert(fingerprint(proof.copied) === proof.sha256, 'copied historical file changed');
    }
    const after = sourceSignature(dbPath);
    assert(JSON.stringify(before) === JSON.stringify(after), 'live V6 database changed during witness');
    const ordered = fileProof.map(item => [relative(home, item.path), item.sha256]).sort((a, b) => a[0].localeCompare(b[0]));
    return { schema: 'bonfyre.v6-history-native-witness.v1', source_database_sha256: before.main,
      source_sidecar_sha256: { wal: before['-wal'], shm: before['-shm'] },
      receipt_chain: { verified: verification.verified, receipts: verification.receipts, head: verification.head },
      counts, owned_files_copied: fileProof.length, owned_bytes_copied: totalBytes,
      owned_files_digest: sha(Buffer.from(JSON.stringify(ordered))), source_unchanged: true, disposable_copy_verified: true };
  } finally {
    try { store?.close(); } catch { /* closing disposable store */ }
    try { copy?.close(); } catch { /* closing disposable snapshot */ }
    try { source?.close(); } catch { /* closing read-only source */ }
    rmSync(scratch, { recursive: true, force: true });
  }
}

if (import.meta.main) {
  const homeIndex = process.argv.indexOf('--home'), rootIndex = process.argv.indexOf('--root');
  if (homeIndex < 0 || !process.argv[homeIndex + 1]) throw new Error('pass --home PATH');
  const result = witness({ home: process.argv[homeIndex + 1], root: rootIndex >= 0 ? process.argv[rootIndex + 1] : process.cwd() });
  process.stdout.write(`${JSON.stringify(result)}\n`);
}
