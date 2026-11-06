import { Database } from 'bun:sqlite';
import { randomUUID } from 'node:crypto';
import { closeSync, existsSync, fsyncSync, mkdirSync, openSync, readFileSync, realpathSync, renameSync, unlinkSync, writeFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { canonicalJson, digestValue, jsonFloat, parsedDocument } from './canonical_json.mjs';
import { spinesOwnedPath as confined, readSpinesBytes as readBytes } from './spines_store.mjs';
import { projectRun } from './project_runtime.mjs';

export const V6_VERSION = '6.1.0';
export const V6_TABLES = Object.freeze(['metadata', 'receipts', 'estates', 'bfp_builds', 'cutovers', 'outcomes',
  'resources', 'backends', 'missions', 'mission_runs', 'federated_receipts', 'graph_versions', 'sparse_snapshots']);
const directories = ['receipts', 'estate', 'bfp', 'proof', 'outcomes', 'logs', 'package', 'keys', 'resources',
  'backends', 'missions', 'history', 'sparse', 'graphs', 'artifacts'];
const uuid = () => randomUUID().replaceAll('-', '');
const canonical = value => canonicalJson(value, { ascii: false });
const digest = value => digestValue(value, { ascii: false });
const parseDocument = (source, omitKey = 'digest') => parsedDocument(source, { ascii: false, omitKey });
function text(value, name) {
  if (typeof value !== 'string' || !value || value.includes('\0') || Buffer.byteLength(value) > 16384) throw new Error(`invalid ${name}`);
  return value;
}

export function createV6Store({ home, root, env = process.env, now = () => Date.now() / 1000, id = uuid, run = projectRun } = {}) {
  home = resolve(text(home, 'home')); root = realpathSync(text(root, 'root'));
  // Canonicalize existing ancestors without permitting owned path symlinks.
  let ancestor = home; const tail = [];
  while (!existsSync(ancestor)) { tail.unshift(ancestor.slice(dirname(ancestor).length + 1)); ancestor = dirname(ancestor); }
  home = join(realpathSync(ancestor), ...tail);
  const dbPath = join(home, 'v6.sqlite3'); let connection, closed = false;
  const clock = () => { const at = now(); if (!Number.isFinite(at) || at < 0) throw new Error('invalid clock'); return at; };
  const ownedPath = path => confined(home, path);
  function db() {
    if (closed) throw new Error('v6 store is closed');
    if (!connection) {
      mkdirSync(home, { recursive: true, mode: 0o700 });
      for (const name of directories) mkdirSync(ownedPath(join(home, name)), { recursive: true, mode: 0o700 });
      const candidate = new Database(ownedPath(dbPath), { create: true, strict: true });
      try { candidate.exec('PRAGMA busy_timeout=30000;'); candidate.exec(readFileSync(new URL('./v6_schema.sql', import.meta.url), 'utf8')); connection = candidate; }
      catch (error) { candidate.close(); throw error; }
    }
    return connection;
  }
  function writeJson(path, value) {
    const bytes = canonical(value); db(); path = ownedPath(path);
    mkdirSync(dirname(path), { recursive: true, mode: 0o700 });
    const temporary = `${path}.${uuid()}.tmp`; let fd;
    try { fd = openSync(temporary, 'wx', 0o600); writeFileSync(fd, `${bytes}\n`); fsyncSync(fd); closeSync(fd); fd = undefined; renameSync(temporary, path); }
    finally { if (fd !== undefined) closeSync(fd); if (existsSync(temporary)) unlinkSync(temporary); }
    return path;
  }
  const readText = path => new TextDecoder('utf-8', { fatal: true }).decode(readBytes(ownedPath(path)));
  const readJson = path => JSON.parse(readText(path));
  const setMeta = (key, value) => db().query('INSERT INTO metadata VALUES(?,?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at').run(text(key, 'metadata key'), canonical(value), clock());
  const getMeta = (key, fallback = null) => { const row = db().query('SELECT value FROM metadata WHERE key=?').get(text(key, 'metadata key')); return row ? JSON.parse(row.value) : fallback; };
  function receipt(kind, state, payload) {
    text(kind, 'receipt kind'); text(state, 'receipt state'); const payloadText = canonical(payload);
    return db().transaction(() => {
      const previous = db().query('SELECT digest FROM receipts ORDER BY seq DESC LIMIT 1').get()?.digest ?? null;
      const rid = id(), at = clock(); if (!/^[a-f0-9]{32}$/.test(rid)) throw new Error('invalid receipt identity');
      const base = { id: rid, kind, state, payload, previous_digest: previous, created_at: jsonFloat(at) };
      const dg = digest(base), path = join(home, 'receipts', `${Math.floor(at * 1000)}-${rid}.json`);
      if (existsSync(path)) throw new Error('receipt already exists');
      writeJson(path, { ...base, digest: dg });
      db().query('INSERT INTO receipts(id,kind,state,payload_json,previous_digest,digest,path,created_at) VALUES(?,?,?,?,?,?,?,?)')
        .run(rid, kind, state, payloadText, previous, dg, path, at);
      return { receipt_id: rid, digest: dg, previous_digest: previous, path, state };
    }).immediate();
  }
  function verifyReceipts() {
    let previous = null, count = 0, bytes = 0; const failures = [];
    const query = db().query('SELECT * FROM receipts ORDER BY seq LIMIT 100001');
    try { for (const row of query.iterate()) {
      if (++count > 100000) throw new Error('receipt row budget exceeded');
      const fail = () => { if (failures.length < 1000) failures.push(row.id); };
      try {
        const source = readText(confined(join(home, 'receipts'), row.path)); bytes += Buffer.byteLength(source);
        if (bytes > 64 * 1024 * 1024) throw new Error('receipt byte budget exceeded');
        const { value, calculated } = parseDocument(source);
        if (value.digest !== calculated || row.digest !== calculated || row.previous_digest !== previous ||
          ['id', 'kind', 'state', 'previous_digest', 'created_at'].some(key => value[key] !== row[key]) ||
          canonical(value.payload) !== canonical(JSON.parse(row.payload_json))) fail();
      } catch { fail(); }
      if (bytes > 64 * 1024 * 1024) throw new Error('receipt byte budget exceeded');
      previous = row.digest;
    } } finally { query.finalize(); }
    return { verified: failures.length === 0, receipts: count, head: previous, failures };
  }
  function checkDoc(doc, fields) { for (const field of fields) text(doc[field], field); if (!/^[a-f0-9]{64}$/.test(doc.digest)) throw new Error('invalid document digest'); }
  function upsertResource(doc) {
    checkDoc(doc, ['id', 'provider', 'kind', 'state']);
    db().query('INSERT INTO resources VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET provider=excluded.provider,kind=excluded.kind,state=excluded.state,capabilities_json=excluded.capabilities_json,evidence_json=excluded.evidence_json,digest=excluded.digest,updated_at=excluded.updated_at')
      .run(doc.id, doc.provider, doc.kind, doc.state, canonical(doc.capabilities ?? []), canonical(doc.evidence ?? {}), doc.digest, clock());
  }
  function upsertBackend(doc) {
    checkDoc(doc, ['id', 'state']);
    db().query('INSERT INTO backends VALUES(?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET state=excluded.state,evidence_json=excluded.evidence_json,digest=excluded.digest,updated_at=excluded.updated_at')
      .run(doc.id, doc.state, canonical(doc), doc.digest, clock());
  }
  function upsertMission(doc) {
    checkDoc(doc, ['id', 'name', 'kind', 'state', 'effect_class']);
    db().query('INSERT INTO missions VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET name=excluded.name,kind=excluded.kind,state=excluded.state,effect_class=excluded.effect_class,definition_json=excluded.definition_json,digest=excluded.digest,updated_at=excluded.updated_at')
      .run(doc.id, doc.name, doc.kind, doc.state, doc.effect_class, canonical(doc), doc.digest, clock());
  }
  function missionRun(missionId, state, outcome, started) {
    text(missionId, 'mission'); text(state, 'state'); if (!Number.isFinite(started)) throw new Error('invalid mission start');
    const rid = id(), completed = clock();
    const base = { id: rid, mission_id: missionId, state, outcome, started_at: jsonFloat(started), completed_at: jsonFloat(completed) };
    const dg = digest(base);
    return db().transaction(() => {
      db().query('INSERT INTO mission_runs VALUES(?,?,?,?,?,?,?)').run(rid, missionId, state, canonical(outcome), dg, started, completed);
      const proof = receipt('v6.1.mission-run', state, { mission_id: missionId, run_digest: dg, outcome_digest: digest(outcome) });
      return { ...JSON.parse(canonical(base)), digest: dg, receipt: proof };
    }).immediate();
  }
  function status() {
    const counts = Object.fromEntries(['resources', 'backends', 'missions', 'mission_runs', 'federated_receipts', 'graph_versions', 'sparse_snapshots']
      .map(name => [name, db().query(`SELECT count(*) AS n FROM ${name}`).get().n]));
    return { service: 'bonfyre-recursive-hypervisor-v6', version: V6_VERSION, system_version: V6_VERSION,
      cutover_complete: existsSync(join(home, 'V6_CUTOVER_COMPLETE.json')), marker: join(home, 'V6_CUTOVER_COMPLETE.json'),
      receipts: verifyReceipts(), activation_counts: counts, active_missions: db().query("SELECT count(*) AS n FROM missions WHERE state='active'").get().n,
      resource_graph: getMeta('resource_graph_summary', {}), workgraph: getMeta('workgraph_summary', {}),
      external_backends: getMeta('backend_summary', {}), sparse_fabric: getMeta('sparse_summary', {}) };
  }
  return { home, root, env, dbPath, get db() { return db(); }, now: clock, canonical, digest, parseDocument,
    run: (argv, options = {}) => run(argv, { cwd: root, env, timeoutMs: 120000, maxOutputBytes: 250000, ...options }),
    ownedPath, writeJson, readJson, readText, setMeta, getMeta, receipt, verifyReceipts,
    upsertResource, upsertBackend, upsertMission, missionRun, status,
    close() { connection?.close(); connection = null; closed = true; } };
}
