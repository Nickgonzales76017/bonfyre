#!/usr/bin/env bun
// Historical continuity proof is performed only on an owned disposable copy.
import { Database } from 'bun:sqlite';
import { createHash } from 'node:crypto';
import { copyFileSync, existsSync, lstatSync, mkdirSync, mkdtempSync, readFileSync, realpathSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { basename, join, relative, sep } from 'node:path';
import { createHypervisorStore } from '../integrations/mcp/hypervisor_store.mjs';

const hash = bytes => createHash('sha256').update(bytes).digest('hex');
const assert = (value, message) => { if (!value) throw new Error(message); };
export function witness({ home }) {
  home = realpathSync(home);
  const implementationPaths = {
    hypervisor_store: new URL('../integrations/mcp/hypervisor_store.mjs', import.meta.url).pathname,
    canonical_json: new URL('../integrations/mcp/canonical_json.mjs', import.meta.url).pathname,
    witness: new URL(import.meta.url).pathname,
  };
  const implementationHashes = Object.fromEntries(Object.entries(implementationPaths).map(([name, path]) => [name, hash(readFileSync(path))]));
  const sourceDb = join(home, 'hypervisor.sqlite3'), sourceReceipts = join(home, 'receipts');
  assert(!['-wal', '-shm', '-journal'].some(suffix => existsSync(`${sourceDb}${suffix}`)), 'source database is active; refuse main-file copy');
  const beforeDb = hash(readFileSync(sourceDb));
  const scratch = realpathSync(mkdtempSync(join(tmpdir(), 'bonfyre-hypervisor-history-')));
  let db, store;
  try {
    const copiedDb = join(scratch, 'hypervisor.sqlite3');
    copyFileSync(sourceDb, copiedDb);
    copyFileSync(join(home, 'ecosystem_registry.json'), join(scratch, 'ecosystem_registry.json'));
    mkdirSync(join(scratch, 'receipts'), { mode: 0o700 });
    db = new Database(copiedDb, { strict: true });
    const rows = db.query('SELECT id,path FROM receipts ORDER BY created_at,id LIMIT 100001').all();
    assert(rows.length === 19, 'historical receipt count changed');
    const files = [];
    for (const row of rows) {
      const source = realpathSync(row.path), rel = relative(sourceReceipts, source);
      assert(rel && rel !== '..' && !rel.startsWith(`..${sep}`) && !lstatSync(source).isSymbolicLink(), 'receipt path escapes source root');
      const contents = readFileSync(source), digest = hash(contents), target = join(scratch, 'receipts', basename(source));
      copyFileSync(source, target);
      assert(hash(readFileSync(target)) === digest, 'receipt copy differs');
      db.query('UPDATE receipts SET path=? WHERE id=?').run(target, row.id);
      files.push({ source, digest });
    }
    db.close(); db = null;
    store = createHypervisorStore({ home: scratch });
    const verification = store.verifyReceiptChain(), registry = store.registry();
    assert(verification.verified && verification.receipts === 19, 'historical receipt chain fails native verification');
    const summary = store.summary();
    assert(hash(readFileSync(sourceDb)) === beforeDb, 'source database changed during witness');
    for (const file of files) assert(hash(readFileSync(file.source)) === file.digest, 'source receipt changed during witness');
    for (const [name, path] of Object.entries(implementationPaths))
      assert(hash(readFileSync(path)) === implementationHashes[name], 'implementation changed during witness');
    const filesDigest = hash(Buffer.from(JSON.stringify(files.map(file => [basename(file.source), file.digest]).sort((a, b) => a[0].localeCompare(b[0])))));
    return { schema: 'bonfyre.hypervisor-native-history-witness.v1', database_sha256: beforeDb,
      implementation_sha256: implementationHashes,
      receipt_chain: { verified: true, receipts: verification.receipts, head: verification.head },
      receipt_files_digest: filesDigest, providers: registry.projects.length, capabilities: registry.capability_contracts.length,
      summary, source_unchanged: true, disposable_copy_verified: true };
  } finally { try { store?.close(); } catch { /* disposable owner */ } try { db?.close(); } catch { /* disposable copy */ }
    rmSync(scratch, { recursive: true, force: true }); }
}
if (import.meta.main) {
  const index = process.argv.indexOf('--home');
  if (index < 0 || !process.argv[index + 1]) throw new Error('pass --home PATH');
  process.stdout.write(`${JSON.stringify(witness({ home: process.argv[index + 1] }))}\n`);
}
