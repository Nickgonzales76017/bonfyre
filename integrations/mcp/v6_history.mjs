import { Database } from 'bun:sqlite';
import { createHash, randomUUID } from 'node:crypto';
import { closeSync, existsSync, fstatSync, lstatSync, mkdirSync, mkdtempSync, openSync, readSync, readdirSync, realpathSync, renameSync, rmSync, writeFileSync, writeSync, constants } from 'node:fs';
import { homedir, tmpdir } from 'node:os';
import { basename, dirname, extname, isAbsolute, join, relative, resolve, sep } from 'node:path';
import { createRecursiveStore } from './recursive_store.mjs';
import { createSpinesStore } from './spines_store.mjs';
import { createCanaryStore } from './canary_store.mjs';
import { createHypervisorStore } from './hypervisor_store.mjs';
import { compareUnicode } from './canonical_json.mjs';

const CATEGORIES = ['command-plane', 'applications', 'databases', 'models', 'media', 'integrations'];
const EXCLUDE = new Set(['.git', 'node_modules', '.venv', 'venv', 'target', '__pycache__', '.cache', '.next', 'dist', 'build']);
const PRIORITIES = { models: ['.gguf', '.fpq', '.onnx', '.safetensors', '.bin', '.pt', '.pth', '.json'],
  media: ['.mp4', '.webm', '.mov', '.png', '.jpg', '.jpeg', '.wav', '.flac', '.blend'], databases: ['.db', '.sqlite', '.sqlite3', '.duckdb'] };
const DB_EXT = new Set(['.db', '.sqlite', '.sqlite3', '.duckdb']);
const CHUNK = 4 * 1024 * 1024;
const MAX_RECEIPTS = 100000, MAX_RECEIPT_BYTES = 64 * 1024 * 1024;
const SHA = bytes => createHash('sha256').update(bytes).digest('hex');
const readFlags = constants.O_RDONLY | (constants.O_NOFOLLOW ?? 0) | (constants.O_NONBLOCK ?? 0);
function writeAll(fd, buffer) {
  for (let offset = 0; offset < buffer.length;) {
    const count = writeSync(fd, buffer, offset, buffer.length - offset);
    if (!count) throw new Error('short output write');
    offset += count;
  }
}
const integer = (value, name, maximum) => {
  const n = Number(value);
  if (!Number.isSafeInteger(n) || n < 0 || n > maximum) throw new Error(`invalid ${name}`);
  return n;
};
const inside = (root, path) => {
  const rel = relative(root, path);
  return rel === '' || (rel !== '..' && !rel.startsWith(`..${sep}`) && !isAbsolute(rel));
};
const checkedFile = (path, maximum) => {
  const fd = openSync(path, readFlags);
  try {
    const before = fstatSync(fd), named = lstatSync(path);
    if (!before.isFile() || !named.isFile() || named.isSymbolicLink() || before.ino !== named.ino || before.dev !== named.dev || before.size > maximum)
      throw new Error('file identity or byte budget changed');
    const chunks = []; let total = 0;
    for (;;) {
      const buffer = Buffer.allocUnsafe(Math.min(CHUNK, Math.max(1, maximum - total + 1)));
      const n = readSync(fd, buffer, 0, buffer.length, null);
      if (!n) break;
      total += n;
      if (total > maximum) throw new Error('file byte budget exceeded');
      chunks.push(buffer.subarray(0, n));
    }
    const after = fstatSync(fd), namedAfter = lstatSync(path);
    if (total !== before.size || after.size !== before.size || after.mtimeMs !== before.mtimeMs || namedAfter.ino !== before.ino || namedAfter.dev !== before.dev)
      throw new Error('file changed while reading');
    return Buffer.concat(chunks, total);
  } finally { closeSync(fd); }
};

function sourceDefinitions(userHome) {
  const base = join(userHome, '.bonfyre');
  return {
    'recursive-r1': join(base, 'recursive-substrate-v5', 'receipts'),
    'native-spines-r7': join(base, 'agent-hypervisor', 'native-spines-r7', 'receipts'),
    'fixture-canary-r4': join(base, 'agent-hypervisor', 'fixture-canary-r4', 'receipts'),
    hypervisor: join(base, 'agent-hypervisor', 'receipts'),
  };
}

const SOURCE_DB = { 'recursive-r1': 'substrate.sqlite3', 'native-spines-r7': 'spines.sqlite3', 'fixture-canary-r4': 'canary.sqlite3', hypervisor: 'hypervisor.sqlite3' };
function verifiedSource(source, receiptRoot) {
  if (!Object.hasOwn(SOURCE_DB, source)) throw new Error('unsupported receipt source');
  if (!existsSync(receiptRoot)) return { present: false, rows: [], head: null };
  if (!lstatSync(receiptRoot).isDirectory() || lstatSync(receiptRoot).isSymbolicLink()) throw new Error('invalid receipt root');
  receiptRoot = realpathSync(receiptRoot);
  const home = dirname(receiptRoot), dbPath = join(home, SOURCE_DB[source]);
  if (!existsSync(dbPath) || !lstatSync(dbPath).isFile() || lstatSync(dbPath).isSymbolicLink()) throw new Error('source receipt database missing');
  const scratch = realpathSync(mkdtempSync(join(tmpdir(), 'bonfyre-v6-history-')));
  let live, db, owner;
  try {
    const scratchDbPath = join(scratch, SOURCE_DB[source]);
    try {
      live = new Database(dbPath, { readonly: true, strict: true });
      live.query('VACUUM INTO ?').run(scratchDbPath);
      live.close(); live = null;
    } catch (error) {
      live?.close(); live = null;
      if (source !== 'hypervisor' || ['-wal', '-shm', '-journal'].some(suffix => existsSync(`${dbPath}${suffix}`))) throw error;
      // The retired hypervisor directory may deny SQLite's read-only shared
      // memory sidecar. An inactive single-file DB can be copied with identity
      // checks; never use this fallback while a journal or WAL is present.
      writeFileSync(scratchDbPath, checkedFile(dbPath, 512 * 1024 * 1024), { flag: 'wx', mode: 0o600 });
      if (['-wal', '-shm', '-journal'].some(suffix => existsSync(`${dbPath}${suffix}`))) throw new Error('legacy database became active during snapshot');
    }
    db = new Database(scratchDbPath, { strict: true });
    const rows = db.query(source === 'hypervisor' ? 'SELECT * FROM receipts ORDER BY created_at,id LIMIT 100001' :
      source === 'recursive-r1' ? 'SELECT * FROM receipts ORDER BY rowid LIMIT 100001' : 'SELECT * FROM receipts ORDER BY seq LIMIT 100001').all();
    if (rows.length > MAX_RECEIPTS) throw new Error('source receipt count budget exceeded');
    mkdirSync(join(scratch, 'receipts'), { recursive: true, mode: 0o700 });
    let bytes = 0;
    const originals = [];
    for (const row of rows) {
      const path = realpathSync(row.path);
      if (!inside(receiptRoot, path) || dirname(path) !== resolve(receiptRoot) || !path.endsWith('.json')) throw new Error('receipt path outside owner root');
      const content = checkedFile(path, 8 * 1024 * 1024);
      bytes += content.length;
      if (bytes > MAX_RECEIPT_BYTES) throw new Error('source receipt byte budget exceeded');
      const copied = join(scratch, 'receipts', basename(path));
      writeFileSync(copied, content, { flag: 'wx', mode: 0o600 });
      db.query('UPDATE receipts SET path=? WHERE id=?').run(copied, row.id);
      originals.push({ row, path, copied, content });
    }
    db.close(); db = null;
    let result;
    if (source === 'recursive-r1') owner = createRecursiveStore({ home: scratch, tokenBinary: '/nonexistent/v6-history-token' });
    else if (source === 'native-spines-r7') owner = createSpinesStore({ home: scratch, projectRoot: scratch, deltaKernel: '/nonexistent/v6-history-delta', helixVm: '/nonexistent/v6-history-helix', kernelProbe: '/nonexistent/v6-history-probe' });
    else if (source === 'fixture-canary-r4') owner = createCanaryStore({ home: scratch });
    else owner = createHypervisorStore({ home: scratch, readOnly: true });
    result = source === 'hypervisor' ? owner.verifyReceiptChain() : owner.verifyReceipts();
    if (!result.verified || result.receipts !== rows.length || result.incomplete)
      throw new Error(`source receipt verification failed: ${JSON.stringify({ receipts: result.receipts, head: result.head, failures: result.failures?.slice(0, 2), incomplete: result.incomplete })}`);
    return { present: true, rows: originals.map(({ row, path }) => ({ receipt_id: row.id, digest: row.digest, path,
      created_at: row.created_at, state: row.state, kind: row.kind ?? row.schema ?? 'receipt' })), head: result.head };
  } finally {
    try { owner?.close(); } catch { /* best-effort scratch close */ }
    try { db?.close(); } catch { /* best-effort scratch close */ }
    try { live?.close(); } catch { /* best-effort scratch close */ }
    rmSync(scratch, { recursive: true, force: true });
  }
}

function rootMap(root, userHome, env, includeAmbient) {
  const paths = key => (env[key] ?? '').split(':').filter(Boolean).map(p => p.startsWith('~/') ? join(userHome, p.slice(2)) : p);
  const map = {
    'command-plane': [join(root, 'cmd'), ...paths('BONFYRE_V61_COMMAND_ROOTS')],
    applications: [join(root, 'integrations/frappe-bench/apps'), join(root, 'integrations/frappe_bench/apps'), join(root, 'frappe-bench/apps'), ...paths('BONFYRE_V61_APPLICATION_ROOTS')],
    databases: [root, ...paths('BONFYRE_V61_DATABASE_ROOTS'), ...(includeAmbient ? [join(userHome, '.bonfyre')] : [])],
    models: [...(includeAmbient ? [join(userHome, 'BonfyreModels')] : []), join(root, 'models'), join(root, 'model'), join(root, 'fpq'), ...paths('BONFYRE_V61_MODEL_ROOTS')],
    media: [join(root, 'output'), join(root, 'renders'), join(root, 'assets'), join(root, 'scene-lab'), join(root, 'media'), ...paths('BONFYRE_V61_MEDIA_ROOTS')],
    integrations: [join(root, 'Aurekai/generated/integration-repos'), ...paths('BONFYRE_V61_INTEGRATION_ROOTS')],
  };
  return map;
}

function walk(base, { databasesOnly = false, maxVisited = 200000, maxDepth = 32 } = {}) {
  if (!existsSync(base)) return [];
  const actual = realpathSync(base), found = [], stack = [[actual, 0]];
  let visited = 0;
  while (stack.length) {
    const [path, depth] = stack.pop();
    if (++visited > maxVisited) throw new Error('sparse traversal entry budget exceeded');
    const stat = lstatSync(path);
    if (stat.isSymbolicLink()) continue;
    if (stat.isFile()) {
      if (!databasesOnly || DB_EXT.has(extname(path).toLowerCase())) found.push(path);
      continue;
    }
    if (!stat.isDirectory()) continue;
    if (depth >= maxDepth) throw new Error('sparse traversal depth budget exceeded');
    const children = readdirSync(path, { withFileTypes: true }).filter(child => !EXCLUDE.has(child.name)).sort((a, b) => compareUnicode(a.name, b.name));
    for (let i = children.length - 1; i >= 0; i--) if (!children[i].isSymbolicLink()) stack.push([join(path, children[i].name), depth + 1]);
  }
  return found;
}

function readHash(path, maxBytes) {
  const fd = openSync(path, readFlags);
  try {
    const before = fstatSync(fd), named = lstatSync(path);
    if (!before.isFile() || before.size > maxBytes || before.ino !== named.ino || before.dev !== named.dev) throw new Error('file identity or budget changed');
    const hash = createHash('sha256'), chunks = []; let offset = 0;
    for (;;) {
      const bytes = Buffer.allocUnsafe(CHUNK), n = readSync(fd, bytes, 0, CHUNK, null);
      if (!n) break;
      const part = bytes.subarray(0, n); hash.update(part);
      chunks.push({ offset, length: n, sha256: SHA(part) }); offset += n;
      if (offset > maxBytes) throw new Error('file byte budget exceeded');
    }
    const after = fstatSync(fd), namedAfter = lstatSync(path);
    if (offset !== before.size || before.size !== after.size || before.mtimeMs !== after.mtimeMs || namedAfter.ino !== before.ino || namedAfter.dev !== before.dev)
      throw new Error('file changed during hashing');
    return { sha256: hash.digest('hex'), chunks, bytes: offset };
  } finally { closeSync(fd); }
}

function sqliteHeader(path) {
  const fd = openSync(path, readFlags);
  try {
    const bytes = Buffer.alloc(16);
    return readSync(fd, bytes, 0, bytes.length, 0) === bytes.length && bytes.equals(Buffer.from('SQLite format 3\0'));
  } finally { closeSync(fd); }
}

function writeTarHeader(fd, name, size) {
  const raw = Buffer.from(name, 'utf8');
  if (raw.length > 255 || name.startsWith('/') || name.split('/').includes('..')) throw new Error('tar entry path exceeds safe USTAR bounds');
  let prefix = '', leaf = name;
  if (raw.length > 100) {
    const cut = name.lastIndexOf('/');
    prefix = name.slice(0, cut); leaf = name.slice(cut + 1);
    if (Buffer.byteLength(prefix) > 155 || Buffer.byteLength(leaf) > 100) throw new Error('tar entry path exceeds safe USTAR bounds');
  }
  const header = Buffer.alloc(512);
  header.write(leaf, 0, 100, 'utf8');
  const oct = (n, start, width) => header.write(n.toString(8).padStart(width - 1, '0') + '\0', start, width, 'ascii');
  oct(0o644, 100, 8); oct(0, 108, 8); oct(0, 116, 8); oct(size, 124, 12); oct(0, 136, 12);
  header.fill(0x20, 148, 156); header.write('0', 156); header.write('ustar\0', 257, 6, 'ascii'); header.write('00', 263, 2, 'ascii');
  header.write(prefix, 345, 155, 'utf8');
  oct(header.reduce((sum, byte) => sum + byte, 0), 148, 8);
  writeAll(fd, header);
}

function tarBundle(path, category, entries) {
  const fd = openSync(path, 'wx', 0o600);
  try {
    let written = 0;
    for (const entry of entries) {
      const archiveName = `${category}/${entry.relative_path.replace(/^\/+/, '')}`;
      writeTarHeader(fd, archiveName, entry.bytes); written += 512;
      const source = openSync(entry.pack_path, readFlags);
      try {
        const before = fstatSync(source), named = lstatSync(entry.pack_path), hash = createHash('sha256');
        if (!before.isFile() || before.size !== entry.bytes || before.ino !== named.ino || before.dev !== named.dev) throw new Error('source changed before packing');
        let copied = 0;
        while (copied < entry.bytes) {
          const bytes = Buffer.allocUnsafe(Math.min(CHUNK, entry.bytes - copied));
          const n = readSync(source, bytes, 0, bytes.length, null);
          if (!n) throw new Error('source truncated during packing');
          writeAll(fd, bytes.subarray(0, n)); hash.update(bytes.subarray(0, n)); copied += n;
        }
        const after = fstatSync(source), namedAfter = lstatSync(entry.pack_path);
        if (after.size !== before.size || after.mtimeMs !== before.mtimeMs || namedAfter.ino !== before.ino || namedAfter.dev !== before.dev || hash.digest('hex') !== entry.sha256)
          throw new Error('packed bytes do not match source hash');
        written += copied;
      } finally { closeSync(source); }
      const pad = (512 - entry.bytes % 512) % 512;
      if (pad) { writeAll(fd, Buffer.alloc(pad)); written += pad; }
    }
    writeAll(fd, Buffer.alloc(1024)); written += 1024;
    const blockPad = (10240 - written % 10240) % 10240;
    if (blockPad) writeAll(fd, Buffer.alloc(blockPad));
  } finally { closeSync(fd); }
  return readHash(path, Number.MAX_SAFE_INTEGER);
}

export function createV6History(store, { projectRoot = store.root, home = store.home, userHome = homedir(), env = process.env,
  sourceRoots = sourceDefinitions(userHome), roots = null, limits = {}, checkStop = () => {} } = {}) {
  if (!store || !('db' in store) || !store.digest || !store.writeJson || !store.receipt || !store.setMeta || !store.ownedPath) throw new Error('V6 store interface incomplete');
  const root = realpathSync(projectRoot), ownedHome = store.home;
  if (resolve(home) !== resolve(store.home) && (!existsSync(home) || realpathSync(home) !== store.home)) throw new Error('history home differs from store owner');
  const check = signal => { signal?.throwIfAborted(); checkStop(); if (existsSync(join(ownedHome, 'STOP'))) throw new Error('STOP active; history mutation refused'); };
  const clock = () => store.now?.() ?? Date.now() / 1000;
  function federate({ signal } = {}) {
    check(signal);
    const summary = {}, failures = [], verified = [];
    for (const [source, path] of Object.entries(sourceRoots)) {
      check(signal);
      try {
        const checked = verifiedSource(source, resolve(path));
        summary[source] = { receipts: checked.rows.length, head: checked.head, path: resolve(path), present: checked.present,
          verified: checked.present };
        for (const row of checked.rows) verified.push({ source, ...row });
      } catch (error) {
        check(signal);
        summary[source] = { receipts: 0, head: null, path: resolve(path), present: existsSync(path), verified: false };
        failures.push({ source, path: resolve(path), error: error.message });
      }
    }
    check(signal); const db = store.db;
    db.transaction(() => {
      check(signal);
      for (const source of Object.keys(sourceRoots)) db.query('DELETE FROM federated_receipts WHERE source=?').run(source);
      const insert = db.query('INSERT INTO federated_receipts(source,receipt_id,digest,path,created_at,state,kind) VALUES(?,?,?,?,?,?,?)');
      for (const row of verified) insert.run(row.source, row.receipt_id, row.digest, row.path, row.created_at, row.state, row.kind);
    }).immediate();
    for (const [source, value] of Object.entries(summary)) if (value.receipts)
      store.receipt('v6.1.federated-root', 'passed', { source, receipts: value.receipts, head: value.head });
    const doc = { schema: 'bonfyre.federated-history.v1', generated_at: clock(), sources: summary, total_receipts: verified.length, failures };
    doc.digest = store.digest(doc);
    store.writeJson(join(ownedHome, 'history', 'federated-history.json'), doc);
    store.setMeta('history_summary', { sources: Object.keys(summary).length, federated_receipts: verified.length, failures: failures.length });
    store.receipt('v6.1.history-federation', failures.length ? 'partial' : 'passed', { digest: doc.digest, total_receipts: verified.length, sources: summary });
    return doc;
  }
  function sparseSnapshot({ maxHashBytes = env.BONFYRE_V61_MAX_HASH_BYTES ?? 2 * 1024 ** 3,
    maxFileBytes = env.BONFYRE_V61_MAX_FILE_BYTES ?? 512 * 1024 ** 2,
    bundleBytes = env.BONFYRE_V61_BUNDLE_BYTES ?? 512 * 1024 ** 2, includeAmbient = true, signal } = {}) {
    check(signal);
    maxHashBytes = integer(maxHashBytes, 'max hash bytes', 8 * 1024 ** 3);
    maxFileBytes = integer(maxFileBytes, 'max file bytes', 2 * 1024 ** 3);
    bundleBytes = integer(bundleBytes, 'bundle bytes', 2 * 1024 ** 3);
    const maxCandidates = integer(limits.maxCandidates ?? 100000, 'max candidates', 1000000);
    const map = roots ?? rootMap(root, userHome, env, includeAmbient);
    const temporary = mkdtempSync(join(tmpdir(), 'bonfyre-v6-sparse-'));
    const entries = [], skipped = [], categories = {}, bundles = [], seen = new Set();
    let candidatesVisited = 0;
    try {
      for (const [index, category] of CATEGORIES.entries()) {
        check(signal);
        const bases = map[category] ?? [], candidates = [];
        for (const base of bases) {
          for (const path of walk(base, { databasesOnly: category === 'databases', maxVisited: maxCandidates })) {
            if (++candidatesVisited > maxCandidates) throw new Error('sparse candidate budget exceeded');
            const actual = realpathSync(path);
            if (seen.has(actual)) continue;
            seen.add(actual);
            const stat = lstatSync(actual);
            if (stat.isFile()) candidates.push({ path: actual, size: stat.size });
          }
        }
        const wanted = PRIORITIES[category] ?? [];
        candidates.sort((a, b) => Number(!wanted.includes(extname(a.path).toLowerCase())) - Number(!wanted.includes(extname(b.path).toLowerCase())) || a.size - b.size || compareUnicode(a.path, b.path));
        const baseBudget = Math.max(1, Math.floor(maxHashBytes / CATEGORIES.length));
        const budget = baseBudget + (index === 0 ? Math.max(0, maxHashBytes - baseBudget * CATEGORIES.length) : 0);
        let used = 0, eligible = 0; const selected = [];
        for (const item of candidates) {
          check(signal);
          if (item.size > maxFileBytes) { skipped.push({ category, path: item.path, bytes: item.size, reason: 'max-file-bytes' }); continue; }
          eligible++;
          if (used + item.size > budget) { skipped.push({ category, path: item.path, bytes: item.size, reason: 'category-hash-budget' }); continue; }
          try {
            let packPath = item.path, snapshotMode = 'pinned-file';
            if (category === 'databases' && extname(item.path).toLowerCase() !== '.duckdb' && sqliteHeader(item.path)) {
              const snap = join(temporary, `${randomUUID()}.sqlite3`);
              const source = new Database(item.path, { readonly: true, strict: true });
              try { source.query('VACUUM INTO ?').run(snap); } finally { source.close(); }
              packPath = snap; snapshotMode = 'sqlite-consistent-vacuum';
            }
            const hashed = readHash(packPath, maxFileBytes);
            if (used + hashed.bytes > budget) { skipped.push({ category, path: item.path, bytes: hashed.bytes, reason: 'category-hash-budget' }); continue; }
            const rel = inside(root, item.path) ? relative(root, item.path) : item.path;
            const entry = { category, path: item.path, relative_path: rel, bytes: hashed.bytes, sha256: hashed.sha256,
              chunks: hashed.chunks, snapshot_mode: snapshotMode, pack_path: packPath };
            selected.push(entry); entries.push(entry); used += hashed.bytes;
          } catch (error) { skipped.push({ category, path: item.path, bytes: item.size, reason: error.message }); }
        }
        categories[category] = { files: selected.length, bytes: used, chunks: selected.reduce((n, x) => n + x.chunks.length, 0),
          eligible_files: eligible, hash_budget: budget, hash_used: used };
      }
      const bundleBudget = Math.max(1, Math.floor(bundleBytes / CATEGORIES.length));
      for (const category of CATEGORIES) {
        check(signal);
        const wanted = PRIORITIES[category] ?? [];
        const candidates = entries.filter(entry => entry.category === category)
          .sort((a, b) => Number(!wanted.includes(extname(a.path).toLowerCase())) - Number(!wanted.includes(extname(b.path).toLowerCase())) || a.bytes - b.bytes || compareUnicode(a.path, b.path));
        const chosen = []; let used = 0;
        for (const entry of candidates) if (entry.bytes <= bundleBudget && used + entry.bytes <= bundleBudget) { chosen.push(entry); used += entry.bytes; }
        if (!chosen.length) continue;
        const tempTar = join(temporary, `${category}.tar`), hash = tarBundle(tempTar, category, chosen);
        bundles.push({ category, tempTar, path: join(ownedHome, 'sparse', 'bundles', `${category}.tar`), bytes: hash.bytes,
          sha256: hash.sha256, files: chosen.length, source_bytes: used, complete_files: true, category_budget: bundleBudget });
      }
      check(signal);
      mkdirSync(store.ownedPath(join(ownedHome, 'sparse', 'bundles')), { recursive: true, mode: 0o700 });
      for (const bundle of bundles) { check(signal); renameSync(bundle.tempTar, store.ownedPath(bundle.path)); }
      const files = entries.map(entry => ({ category: entry.category, path: entry.path, relative_path: entry.relative_path,
        bytes: entry.bytes, sha256: entry.sha256, chunks: entry.chunks, snapshot_mode: entry.snapshot_mode }));
      const publicBundles = bundles.map(bundle => ({ category: bundle.category, path: bundle.path, bytes: bundle.bytes,
        sha256: bundle.sha256, files: bundle.files, source_bytes: bundle.source_bytes, complete_files: bundle.complete_files,
        category_budget: bundle.category_budget }));
      const snapshot = { schema: 'bonfyre.sparse-artifact-snapshot.v1', version: '6.1.0', root, include_ambient: includeAmbient,
        created_at: clock(), chunk_bytes: CHUNK, files, file_count: files.length,
        total_bytes: files.reduce((n, item) => n + item.bytes, 0), chunk_count: files.reduce((n, item) => n + item.chunks.length, 0),
        categories, bundles: publicBundles, bundle_source_bytes: publicBundles.reduce((n, b) => n + b.source_bytes, 0),
        bundle_bytes: publicBundles.reduce((n, b) => n + b.bytes, 0), skipped };
      snapshot.digest = store.digest(snapshot);
      const path = join(ownedHome, 'sparse', 'snapshot.json'); store.writeJson(path, snapshot);
      check(signal);
      store.db.query('INSERT OR REPLACE INTO sparse_snapshots(id,file_count,total_bytes,chunk_count,digest,path,created_at) VALUES(?,?,?,?,?,?,?)')
        .run(snapshot.digest, snapshot.file_count, snapshot.total_bytes, snapshot.chunk_count, snapshot.digest, path, clock());
      const summary = { files: snapshot.file_count, hashed_bytes: snapshot.total_bytes, chunks: snapshot.chunk_count, bundles: publicBundles.length,
        bundle_source_bytes: snapshot.bundle_source_bytes, bundle_bytes: snapshot.bundle_bytes, skipped: skipped.length, categories };
      store.setMeta('sparse_summary', summary);
      store.receipt('v6.1.sparse-snapshot', 'passed', { digest: snapshot.digest, summary });
      return snapshot;
    } finally { rmSync(temporary, { recursive: true, force: true }); }
  }
  return Object.freeze({ federate, sparseSnapshot });
}
