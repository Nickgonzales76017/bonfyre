import { Database } from 'bun:sqlite';
import { createHash, randomUUID } from 'node:crypto';
import { constants, closeSync, existsSync, fstatSync, fsyncSync, lstatSync, mkdirSync, openSync, readFileSync,
  readSync, realpathSync, renameSync, unlinkSync, writeFileSync } from 'node:fs';
import { dirname, isAbsolute, join, relative, resolve, sep } from 'node:path';
import { homedir } from 'node:os';
import { canonicalJson, digestValue, jsonFloat, parsedDocument } from './canonical_json.mjs';
import { projectRun } from './project_runtime.mjs';

export const VERSION = '5.7.0-native-spines-r7';
export const ALLOWED_EFFECTS = Object.freeze(['PURE', 'READ_ONLY', 'CANDIDATE_WRITE', 'REVERSIBLE_INTERNAL_WRITE',
  'AUTHORITATIVE_INTERNAL_WRITE', 'EXTERNAL_REVERSIBLE_EFFECT', 'EXTERNAL_IRREVERSIBLE_EFFECT']);
export const SPINES_TABLES = Object.freeze(['metadata', 'receipts', 'flux_streams', 'flux_events', 'object_state', 'deltas',
  'dependency_edges', 'closures', 'materialized_views', 'parity_runs', 'graph_partitions', 'rewrite_rules', 'saturation_runs',
  'helix_artifacts', 'reductions', 'authority_decisions', 'crux_proofs', 'hardware_census', 'model_inventory', 'phenotype_plans',
  'kernel_parity_runs', 'placement_nodes', 'placement_plans', 'fuse_plans']);
const MAX_BYTES = 8 * 1024 * 1024, MAX_ROWS = 100000;
const uuid = () => randomUUID().replaceAll('-', '');
function nonempty(value, name) {
  if (typeof value !== 'string' || !value || value.includes('\0') || Buffer.byteLength(value) > 16384) throw new Error(`invalid ${name}`);
  return value;
}
function location(path) {
  const tail = []; let parent = resolve(nonempty(path, 'path'));
  while (!existsSync(parent)) { tail.unshift(parent.slice(dirname(parent).length + 1)); parent = dirname(parent); }
  if (lstatSync(parent).isSymbolicLink()) throw new Error('symlink store root refused');
  return join(realpathSync(parent), ...tail);
}
export function spinesOwnedPath(root, path) {
  const full = resolve(nonempty(path, 'path')), rel = relative(root, full);
  if (isAbsolute(rel) || rel === '..' || rel.startsWith(`..${sep}`)) throw new Error('path escapes owned store');
  let current = root;
  for (const part of rel.split(sep).filter(Boolean)) {
    current = join(current, part);
    try { if (lstatSync(current).isSymbolicLink()) throw new Error('owned symlink refused'); }
    catch (error) { if (error.code !== 'ENOENT') throw error; }
  }
  return full;
}
export function readSpinesBytes(path, maximum = MAX_BYTES) {
  const fd = openSync(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
  try {
    const before = fstatSync(fd, { bigint: true });
    if (!before.isFile() || before.size > BigInt(maximum)) throw new Error('not a bounded regular file');
    const bytes = Buffer.alloc(Number(before.size) + 1); let used = 0;
    while (used < bytes.length) { const n = readSync(fd, bytes, used, bytes.length - used, null); if (!n) break; used += n; }
    const after = fstatSync(fd, { bigint: true }), named = lstatSync(path, { bigint: true });
    const identity = s => [s.dev, s.ino, s.size, s.mtimeNs, s.ctimeNs].join(':');
    if (used !== Number(before.size) || identity(before) !== identity(after) || identity(after) !== identity(named) || !named.isFile()) throw new Error('file changed while reading');
    return bytes.subarray(0, used);
  } finally { closeSync(fd); }
}

export function createSpinesStore({ home, projectRoot, deltaKernel, helixVm, kernelProbe,
  env = process.env, now = () => Date.now() / 1000, run = projectRun, id = uuid } = {}) {
  home = location(home); projectRoot = realpathSync(nonempty(projectRoot, 'project root'));
  for (const [name, path] of Object.entries({ deltaKernel, helixVm, kernelProbe })) nonempty(path, name);
  const dbPath = join(home, 'spines.sqlite3');
  const roots = Object.fromEntries(['flux', 'delta', 'graph', 'helix', 'crux', 'model', 'placement', 'receipt'].map(name =>
    [`${name}Root`, join(home, name === 'graph' ? 'graf' : name === 'receipt' ? 'receipts' : name)]));
  let connection, closed = false;
  const clock = () => { const at = now(); if (!Number.isFinite(at) || at < 0) throw new Error('invalid clock'); return at; };
  function database() {
    if (closed) throw new Error('spines store is closed');
    if (!connection) {
      mkdirSync(home, { recursive: true, mode: 0o700 });
      for (const path of Object.values(roots)) mkdirSync(spinesOwnedPath(home, path), { recursive: true, mode: 0o700 });
      spinesOwnedPath(home, dbPath);
      const fresh = !existsSync(dbPath), db = new Database(dbPath, { create: true, strict: true });
      try {
        db.exec('PRAGMA busy_timeout=30000;'); db.exec(readFileSync(new URL('./spines_schema.sql', import.meta.url), 'utf8'));
        connection = db;
        if (fresh) bootstrap();
      } catch (error) { connection = null; db.close(); throw error; }
    }
    return connection;
  }
  function bootstrap() {
    database().transaction(() => {
      const at = clock(); database().query('INSERT INTO metadata VALUES(?,?,?)').run('version', VERSION, at);
      for (const [name, lhs, rhs] of [['add-zero-r', 'add(?x,0)', '?x'], ['add-zero-l', 'add(0,?x)', '?x'],
        ['mul-one-r', 'mul(?x,1)', '?x'], ['mul-one-l', 'mul(1,?x)', '?x'], ['mul-zero-r', 'mul(?x,0)', '0'],
        ['mul-zero-l', 'mul(0,?x)', '0'], ['sub-zero', 'sub(?x,0)', '?x'], ['double-neg', 'neg(neg(?x))', '?x']]) {
        const ruleId = createHash('sha256').update(`${name}\0${lhs}\0${rhs}`).digest('hex');
        database().query('INSERT INTO rewrite_rules VALUES(?,?,?,?,?,1,?,?)').run(ruleId, name, lhs, rhs, '{}', digestValue({ name, lhs, rhs, guard: {} }), at);
      }
      const defaults = [
        ['local', 'workstation', 'owner-controlled', 'candidate', { local_models: true, metal: process.platform === 'darwin' }, { network_required: false }, 'healthy'],
        ['oracle', 'remote-server', 'operator-controlled', 'candidate', { remote_execution: true, bfp_reconstruction: true }, { network_required: true }, 'unknown'],
        ['browser', 'wasm-edge', 'user-session', 'read-only', { wasm: true, webgpu: true }, { max_payload_mb: 64 }, 'candidate'],
      ];
      for (const [nodeId, kind, trust, zone, caps, constraints, health] of defaults)
        database().query('INSERT INTO placement_nodes VALUES(?,?,?,?,?,?,?,?)').run(nodeId, kind, trust, zone, JSON.stringify(caps), JSON.stringify(constraints), health, at);
    }).immediate();
  }
  function writeJson(path, value) {
    const bytes = canonicalJson(value); database(); path = spinesOwnedPath(home, path);
    mkdirSync(dirname(path), { recursive: true, mode: 0o700 });
    const temporary = `${path}.${uuid()}.tmp`; let fd;
    try {
      fd = openSync(temporary, 'wx', 0o600); writeFileSync(fd, bytes); fsyncSync(fd); closeSync(fd); fd = undefined;
      renameSync(temporary, path);
    } finally { if (fd !== undefined) closeSync(fd); if (existsSync(temporary)) unlinkSync(temporary); }
    return path;
  }
  const readText = path => new TextDecoder('utf-8', { fatal: true }).decode(readSpinesBytes(spinesOwnedPath(home, path)));
  function writeReceipt({ release, kind, subject_kind = null, subject_id = null, state, payload }) {
    for (const [name, value] of Object.entries({ release, kind, state })) nonempty(value, name);
    for (const [name, value] of Object.entries({ subject_kind, subject_id })) if (value !== null) nonempty(value, name);
    const payloadText = canonicalJson(payload);
    return database().transaction(() => {
      const previous = database().query('SELECT digest FROM receipts ORDER BY seq DESC LIMIT 1').get()?.digest ?? null;
      const receiptId = id(), at = clock();
      if (!/^[a-f0-9]{32}$/.test(receiptId)) throw new Error('invalid receipt identity');
      const body = { schema: 'bonfyre.native-spines-receipt.v1', id: receiptId, release, kind, subject_kind,
        subject_id, state, payload, previous_digest: previous, created_at: jsonFloat(at) };
      const digest = digestValue(body), path = join(roots.receiptRoot, `${Math.floor(at * 1000)}-${receiptId}.json`);
      if (existsSync(path)) throw new Error('receipt identity already exists');
      writeJson(path, { ...body, digest });
      database().query('INSERT INTO receipts(id,release,kind,subject_kind,subject_id,state,payload_json,previous_digest,digest,path,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)')
        .run(receiptId, release, kind, subject_kind, subject_id, state, payloadText, previous, digest, path, at);
      return { receipt_id: receiptId, digest, previous_digest: previous, path, state };
    }).immediate();
  }
  function verifyReceipts() {
    let previous = null, count = 0, bytes = 0; const failures = [];
    const query = database().query('SELECT * FROM receipts ORDER BY seq LIMIT 100001');
    try {
      for (const row of query.iterate()) {
        if (++count > MAX_ROWS) throw new Error('receipt verification budget exceeded');
        const fail = (reason, extra = {}) => { if (failures.length < 1000) failures.push({ id: row.id, reason, ...extra }); };
        try {
          const path = spinesOwnedPath(roots.receiptRoot, row.path);
          if (!existsSync(path)) fail('missing_file');
          else {
            const text = readText(path); bytes += Buffer.byteLength(text);
            if (bytes > 64 * 1024 * 1024) throw new Error('receipt verification byte budget exceeded');
            const { value, calculated } = parsedDocument(text);
            if (value.digest !== calculated || row.digest !== calculated) fail('digest_mismatch');
            if (['id', 'release', 'kind', 'subject_kind', 'subject_id', 'state', 'previous_digest', 'created_at'].some(key => value[key] !== row[key]) ||
              canonicalJson(value.payload) !== canonicalJson(JSON.parse(row.payload_json))) fail('row_document_mismatch');
          }
        } catch { fail('unreadable_document'); }
        if (bytes > 64 * 1024 * 1024) throw new Error('receipt verification byte budget exceeded');
        if (row.previous_digest !== previous) fail('chain_mismatch', { expected: previous, actual: row.previous_digest });
        previous = row.digest;
      }
    } finally { query.finalize(); }
    return { verified: !failures.length, receipts: count, head: previous, failures };
  }
  async function execute(argv, { cwd, timeout = 120, ...options } = {}) {
    const result = await run(argv, { cwd, env, timeoutMs: timeout * 1000, maxOutputBytes: 250000, ...options });
    return { ...result, cwd: cwd ?? null, timed_out: result.timeout === true, ...(result.exit_code === 127 ? { spawn_error: true } : {}) };
  }
  function backendStatus() {
    return Object.fromEntries(Object.entries({ feldera: 'feldera', egglog: 'egglog', hvm4: 'hvm', cubecl: 'bonfyre-cubecl-adapter', hydro: 'bonfyre-hydro-adapter' }).map(([name, fallback]) => {
      const command = env[`BONFYRE_${name.toUpperCase()}_ADAPTER`] || fallback;
      const expanded = command.startsWith('~/') ? join(homedir(), command.slice(2)) : command;
      const binary = existsSync(expanded) && lstatSync(expanded).isFile() ? realpathSync(expanded) : Bun.which(command, { PATH: env.PATH }) || null;
      return [name, { available: Boolean(binary), binary, mode: binary ? 'adapter-candidate' : 'contract-only' }];
    }));
  }
  const counts = () => Object.fromEntries(SPINES_TABLES.filter(table => !['metadata', 'flux_streams'].includes(table))
    .map(table => [table, database().query(`SELECT COUNT(*) AS count FROM ${table}`).get().count]));
  async function status() {
    const entries = Object.entries({ delta_kernel: deltaKernel, helix_vm: helixVm, kernel_probe: kernelProbe });
    const results = await Promise.all(entries.map(async ([key, path]) => [key, await execute([path, 'status'], { timeout: 10 })]));
    return { schema: 'bonfyre.native-spines-status.v1', service: 'bonfyre-native-spines-r7', version: VERSION, home, database: dbPath,
      releases: { R5: 'temporal-and-incremental', R6: 'reasoning-proof-and-authority-preflight', R7: 'model-kernel-and-placement' },
      native_binaries: Object.fromEntries(results), external_backends: backendStatus(), counts: counts(), receipt_chain: verifyReceipts(),
      authority: { automatic_authoritative_writes: false, automatic_external_effects: false, zig_authority_required: true } };
  }
  return { home, projectRoot, dbPath, ...roots, deltaKernel, helixVm, kernelProbe, env,
    get db() { return database(); }, now: clock, id, canonical: canonicalJson, digest: digestValue, run: execute,
    writeJson, readJson: path => JSON.parse(readText(path)), readText, ownedPath: path => spinesOwnedPath(home, path),
    writeReceipt, verifyReceipts, counts, status, backendStatus,
    close() { connection?.close(); connection = null; closed = true; },
  };
}
