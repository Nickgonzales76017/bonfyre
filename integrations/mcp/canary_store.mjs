import { Database } from 'bun:sqlite';
import { createHash, randomUUID } from 'node:crypto';
import { constants, closeSync, existsSync, fstatSync, fsyncSync, lstatSync, mkdirSync,
  openSync, readSync, readdirSync, realpathSync, renameSync, unlinkSync, writeFileSync } from 'node:fs';
import { dirname, join, relative, resolve, sep } from 'node:path';
import { projectRun } from './project_runtime.mjs';
import { inspectCanaryProviderExecutable } from './canary_fixtures.mjs';
import { canonicalJson, digestValue, parsedDocument, compareUnicode as compare } from './canonical_json.mjs';
export { canonicalJson, digestValue } from './canonical_json.mjs';

export const VERSION = '5.4.0-fixture-canary-r4';
export const SCHEMA_VERSION = 'bonfyre.canary-control.v1';
export const CERT_SCHEMA = 'bonfyre.shadow-certificate.v1';
export const ALLOWED_EFFECTS = Object.freeze(['CANDIDATE_WRITE', 'PURE', 'READ_ONLY']);
const PROBES = Object.freeze({ rtk: ['rtk', '--version'], bernstein: ['bernstein', '--version'],
  'agent-deck': ['agent-deck', '--version'], cavekit: ['bonfyre-cavekit-provider', '--version'],
  caveman: ['bonfyre-caveman-provider', '--version'], cavemem: ['cavemem', '--version'],
  'caveman-code': ['caveman-code', '--version'], 'claude-squad': ['claude-squad', 'version'] });
const MAX_DOCUMENT = 8 * 1024 * 1024;
const hash = bytes => createHash('sha256').update(bytes).digest('hex');
const uuid = () => randomUUID().replaceAll('-', '');
const object = value => value !== null && typeof value === 'object' && !Array.isArray(value);
function text(value, name, max = 4096, nullable = false) {
  if (nullable && value === null) return value;
  if (typeof value !== 'string' || !value || value.includes('\0') || Buffer.byteLength(value) > max)
    throw new Error(`invalid ${name}`);
  return value;
}
function name(value) {
  text(value, 'provider/capability', 128);
  if (!/^[a-zA-Z0-9][a-zA-Z0-9._-]*$/.test(value) || value === '.' || value === '..') throw new Error('invalid provider/capability path');
  return value;
}
function bounded(value, min, max, integer = false) {
  if (!Number.isFinite(value) || value < min || value > max || (integer && !Number.isSafeInteger(value)))
    throw new Error('number outside bounded contract');
  return value;
}
const snapshot = value => JSON.parse(canonicalJson(value));
function safePath(path) {
  const full = resolve(path), parts = full.split(sep).filter(Boolean);
  let current = sep;
  for (const part of parts) {
    current = join(current, part);
    if (existsSync(current) && lstatSync(current).isSymbolicLink()) throw new Error('symlink path refused');
  }
  return full;
}
function location(path) {
  const tail = []; let parent = resolve(path);
  while (!existsSync(parent)) { tail.unshift(parent.slice(dirname(parent).length + 1)); parent = dirname(parent); }
  if (lstatSync(parent).isSymbolicLink()) throw new Error('symlink root refused');
  return join(realpathSync(parent), ...tail);
}
function readBounded(path) {
  safePath(path);
  const fd = openSync(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
  try {
    const before = fstatSync(fd);
    if (!before.isFile() || before.size > MAX_DOCUMENT) throw new Error('not a bounded regular document');
    const bytes = Buffer.alloc(before.size + 1);
    let length = 0;
    while (length < bytes.length) {
      const n = readSync(fd, bytes, length, bytes.length - length, null);
      if (!n) break;
      length += n;
    }
    const after = fstatSync(fd), named = lstatSync(path);
    if (length !== before.size || after.size !== before.size || after.mtimeMs !== before.mtimeMs ||
        named.ino !== before.ino || named.dev !== before.dev || !named.isFile()) throw new Error('document changed while reading');
    return bytes.subarray(0, length);
  } finally { closeSync(fd); }
}
function readDocument(path) { return new TextDecoder('utf-8', { fatal: true }).decode(readBounded(path)); }
function atomicFile(path, bytes) {
  safePath(path); mkdirSync(dirname(path), { recursive: true, mode: 0o700 }); safePath(path);
  const temporary = `${path}.${uuid()}.tmp`;
  let fd;
  try {
    fd = openSync(temporary, 'wx', 0o600); writeFileSync(fd, bytes); fsyncSync(fd); closeSync(fd); fd = undefined;
    renameSync(temporary, path);
  } finally { if (fd !== undefined) closeSync(fd); if (existsSync(temporary)) unlinkSync(temporary); }
}
export function treeDigest(root) {
  const base = safePath(location(root)), rows = []; let bytes = 0, visited = 0;
  if (!existsSync(base)) return digestValue(rows);
  function walk(directory, depth) {
    if (depth > 64) throw new Error('fixture depth budget exceeded');
    for (const entry of readdirSync(directory, { withFileTypes: true })) {
      if (++visited > 10000) throw new Error('fixture entry budget exceeded');
      if (entry.name === '.git') continue;
      const path = join(directory, entry.name);
      if (entry.isSymbolicLink()) throw new Error('fixture symlink refused');
      if (entry.isDirectory()) walk(path, depth + 1);
      else if (entry.isFile()) {
        const data = readBounded(path); bytes += data.length;
        if (bytes > 64 * 1024 * 1024) throw new Error('fixture byte budget exceeded');
        rows.push({ path: relative(base, path), sha256: hash(data), bytes: data.length });
      } else throw new Error('fixture special file refused');
    }
  }
  walk(base, 0);
  rows.sort((a, b) => {
    // pathlib sorts path components: a/file precedes a.txt, unlike flat strings.
    const left = a.path.split(sep), right = b.path.split(sep);
    for (let index = 0; index < Math.min(left.length, right.length); index++) {
      const order = compare(left[index], right[index]);
      if (order) return order;
    }
    return left.length - right.length;
  });
  return digestValue(rows);
}

const SCHEMA = `CREATE TABLE IF NOT EXISTS metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL,updated_at REAL NOT NULL);
CREATE TABLE IF NOT EXISTS policies(id TEXT PRIMARY KEY,capability TEXT NOT NULL,provider TEXT NOT NULL,
 fallback_provider TEXT NOT NULL,percentage INTEGER NOT NULL,effect_classes_json TEXT NOT NULL,state TEXT NOT NULL,
 certificate_digest TEXT NOT NULL,provider_version TEXT NOT NULL,max_consecutive_failures INTEGER NOT NULL DEFAULT 2,
 max_failure_rate REAL NOT NULL DEFAULT 0.25,minimum_outcomes INTEGER NOT NULL DEFAULT 5,
 created_at REAL NOT NULL,updated_at REAL NOT NULL,UNIQUE(capability,provider));
CREATE TABLE IF NOT EXISTS decisions(id TEXT PRIMARY KEY,mission_id TEXT NOT NULL,task_id TEXT,capability TEXT NOT NULL,
 effect_class TEXT NOT NULL,selected_provider TEXT NOT NULL,fallback_provider TEXT NOT NULL,mode TEXT NOT NULL,
 bucket INTEGER NOT NULL,percentage INTEGER NOT NULL,policy_id TEXT,certificate_digest TEXT,reason TEXT NOT NULL,created_at REAL NOT NULL);
CREATE TABLE IF NOT EXISTS outcomes(id TEXT PRIMARY KEY,decision_id TEXT NOT NULL,state TEXT NOT NULL,latency_ms REAL,
 receipt_digest TEXT,payload_json TEXT NOT NULL,created_at REAL NOT NULL);
CREATE TABLE IF NOT EXISTS receipts(seq INTEGER PRIMARY KEY AUTOINCREMENT,id TEXT UNIQUE NOT NULL,kind TEXT NOT NULL,
 subject TEXT,state TEXT NOT NULL,payload_json TEXT NOT NULL,previous_digest TEXT,digest TEXT NOT NULL,path TEXT NOT NULL,created_at REAL NOT NULL);
CREATE INDEX IF NOT EXISTS canary_policy_lookup ON policies(capability,state,percentage,provider);
CREATE INDEX IF NOT EXISTS canary_decision_policy ON decisions(policy_id,mode);
CREATE INDEX IF NOT EXISTS canary_outcome_decision ON outcomes(decision_id,created_at);
`;

export function createCanaryStore({ home, now = () => Date.now() / 1000, run = projectRun,
  inspectProvider = inspectCanaryProviderExecutable, env = process.env,
  legacyRoot = home ? join(dirname(resolve(home)), 'shadow-r3/certificates') : undefined } = {}) {
  text(home, 'canary home', 16384);
  if (typeof now !== 'function' || typeof run !== 'function') throw new Error('invalid clock/runner');
  home = location(home); legacyRoot = location(legacyRoot);
  const dbPath = join(home, 'canary.sqlite3'), certRoot = join(home, 'certificates'), runRoot = join(home, 'runs');
  const receiptRoot = join(home, 'receipts'), policyRoot = join(home, 'policies');
  let connection, closed = false;
  const clock = () => bounded(now(), 0, 8640000000000);
  function db() {
    if (closed) throw new Error('canary store is closed');
    if (!connection) {
      for (const directory of [home, certRoot, runRoot, receiptRoot, policyRoot]) {
        safePath(directory); mkdirSync(directory, { recursive: true, mode: 0o700 });
      }
      safePath(dbPath);
      const opened = new Database(dbPath, { create: true, strict: true });
      try {
        opened.exec('PRAGMA busy_timeout=30000; PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;');
        opened.transaction(() => {
          opened.exec(SCHEMA);
          opened.query("INSERT INTO metadata VALUES('version',?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at").run(VERSION, clock());
        }).immediate();
        connection = opened;
      } catch (error) { opened.close(); throw error; }
    }
    return connection;
  }
  const get = (sql, ...args) => db().query(sql).get(...args);
  const write = (sql, ...args) => db().query(sql).run(...args);
  const atomic = fn => db().transaction(fn).immediate();
  function writeReceipt({ kind, subject = null, state, payload }) {
    text(kind, 'receipt kind'); text(subject, 'receipt subject', 4096, true); text(state, 'receipt state');
    payload = snapshot(payload);
    return atomic(() => {
      const previous = get('SELECT digest FROM receipts ORDER BY seq DESC LIMIT 1')?.digest ?? null;
      const created = clock(), id = uuid();
      const body = { schema: 'bonfyre.canary-receipt.v1', id, kind, subject, state, payload,
        previous_digest: previous, created_at: created };
      const digest = digestValue(body), path = join(receiptRoot, `${Math.floor(created * 1000)}-${id}.json`);
      atomicFile(path, `${canonicalJson({ ...body, digest })}\n`);
      const inserted = write(`INSERT INTO receipts(id,kind,subject,state,payload_json,previous_digest,digest,path,created_at)
        VALUES(?,?,?,?,?,?,?,?,?)`, id, kind, subject, state, canonicalJson(payload), previous, digest, path, created);
      return { id, seq: Number(inserted.lastInsertRowid), digest, previous_digest: previous, path, state };
    });
  }
  function verifyReceipts() {
    const failures = []; let previous = null, count = 0, complete = true;
    for (const row of db().query('SELECT * FROM receipts ORDER BY seq LIMIT 100001').iterate()) {
      if (++count > 100000) { count--; complete = false; break; }
      const fail = reason => { if (failures.length < 1000) failures.push({ seq: row.seq, reason }); };
      try {
        const path = resolve(row.path), rel = relative(receiptRoot, path);
        if (rel.startsWith(`..${sep}`) || rel === '..' || resolve(receiptRoot, rel) !== path) throw new Error('outside_receipt_root');
        if (!existsSync(path)) fail('missing_file');
        else {
          const { value, calculated } = parsedDocument(readDocument(path));
          if (value.digest !== calculated || row.digest !== calculated) fail('digest_mismatch');
          if (value.id !== row.id || value.kind !== row.kind || value.subject !== row.subject || value.state !== row.state ||
              value.previous_digest !== row.previous_digest || value.created_at !== row.created_at ||
              canonicalJson(value.payload) !== canonicalJson(JSON.parse(row.payload_json))) fail('row_document_mismatch');
        }
      } catch (error) { fail(error.message === 'outside_receipt_root' ? error.message : 'unreadable_document'); }
      if (row.previous_digest !== previous) fail('chain_mismatch');
      previous = row.digest;
    }
    return { verified: complete && !failures.length, receipts: count, head: previous, failures,
      ...(complete ? {} : { incomplete: true, reason: 'receipt_verification_budget' }) };
  }
  async function providerVersion(provider) {
    const argv = PROBES[provider];
    if (!argv) return { argv: [], exit_code: 127, stdout: '', stderr: `No provider version probe for ${provider}`, duration_ms: 0, timed_out: false };
    try {
      const owner = inspectProvider(argv[0], env.PATH);
      const exception = provider === 'bernstein' && owner.kind === 'python_launcher';
      const boundary = { kind: exception ? 'third_party_python' : owner.kind,
        resolved_launcher: owner.path ?? null, sha256: owner.sha256 ?? null,
        ...(owner.interpreter ? { interpreter: owner.interpreter } : {}),
        ...(exception ? { reason: 'Exact upstream Bernstein version probe; no native provider implementation exists.' } : {}) };
      if (!['native', 'reviewed_script'].includes(owner.kind) && !exception) return { argv: [...argv], exit_code: 127, stdout: '',
        stderr: 'Provider executable is missing or its runtime is unverified', duration_ms: 0, timed_out: false, runtime_boundary: boundary };
      const result = await run([owner.path, ...argv.slice(1)], { env, timeoutMs: 20000, maxOutputBytes: 250000 });
      const probe = snapshot(result);
      if (typeof probe.stdout !== 'string' || typeof probe.stderr !== 'string' || !Number.isInteger(probe.exit_code)) throw new Error('invalid probe receipt');
      const after = inspectProvider(argv[0], env.PATH);
      if (after.path !== owner.path || after.sha256 !== owner.sha256 || after.kind !== owner.kind) throw new Error('provider changed during version probe');
      return { ...probe, timed_out: Boolean(probe.timeout || probe.timed_out), runtime_boundary: boundary };
    } catch { return { argv: [...argv], exit_code: 127, stdout: '', stderr: 'Provider version unavailable', duration_ms: 0, timed_out: false }; }
  }
  const versionString = probe => `${probe.stdout || ''}${probe.stderr || ''}`.trim();
  function certificatePath(provider, capability) {
    name(provider); name(capability);
    const preferred = join(certRoot, provider, `${capability}.json`);
    return existsSync(preferred) ? preferred : join(legacyRoot, provider, `${capability}.json`);
  }
  async function validate(provider, capability, probes) {
    const path = certificatePath(provider, capability);
    if (!existsSync(path)) return { valid: false, reason: 'missing', path };
    let certificate, calculated, source;
    try { source = readDocument(path); ({ value: certificate, calculated } = parsedDocument(source)); }
    catch { return { valid: false, reason: 'json:invalid_or_unreadable', path }; }
    const refused = reason => ({ valid: false, reason, path, certificate });
    if (certificate.digest !== calculated) return refused('digest_mismatch');
    if (certificate.schema !== CERT_SCHEMA || certificate.provider !== provider || certificate.capability !== capability) return refused('identity_mismatch');
    if (certificate.cleanup_verified !== true) return refused('cleanup_not_verified');
    if (!Number.isFinite(certificate.expires_at) || certificate.expires_at <= clock()) return refused('expired');
    let pending = probes?.get(provider);
    if (!pending) { pending = providerVersion(provider); probes?.set(provider, pending); }
    const probe = await pending, current = versionString(probe);
    // A certificate swap during the await must not validate the earlier bytes.
    try { if (readDocument(path) !== source) return refused('changed_during_validation'); }
    catch { return refused('changed_during_validation'); }
    if (certificate.expires_at <= clock()) return refused('expired');
    if (probe.exit_code !== 0 || probe.timed_out || probe.overflow || current !== certificate.provider_version)
      return { ...refused('provider_version_mismatch'), current_provider_version: current, probe };
    if (!['passed', 'partial'].includes(certificate.state)) return refused(`state:${certificate.state}`);
    return { valid: true, path, certificate, current_provider_version: current };
  }
  const validateCertificate = (provider, capability) => validate(provider, capability);
  async function sealCertificate(input) {
    const value = snapshot(input);
    const { provider, capability, state, behavior_score, fixture_root, claims, evidence,
      cleanup_verified, run_id, limitations = [], ttl_days = 30 } = value;
    name(provider); name(capability); text(run_id, 'run id'); text(fixture_root, 'fixture root', 16384);
    if (!['passed', 'partial', 'blocked', 'failed'].includes(state)) throw new Error('invalid certificate state');
    bounded(behavior_score, 0, 1); bounded(ttl_days, 1, 3650, true);
    if (typeof cleanup_verified !== 'boolean' || !Array.isArray(claims) || !Array.isArray(evidence) ||
        !Array.isArray(limitations) || limitations.some(item => typeof item !== 'string')) throw new Error('invalid certificate evidence');
    const probe = await providerVersion(provider), created = clock();
    const body = { schema: CERT_SCHEMA, r3_version: VERSION, r4_version: VERSION, run_id, provider, capability,
      state, behavior_score, provider_version: versionString(probe), provider_version_probe: probe,
      created_at: created, expires_at: created + ttl_days * 86400, fixture_root,
      fixture_digest: treeDigest(fixture_root), cleanup_verified, claims, evidence, limitations };
    const certificate = { ...body, digest: digestValue(body) }, path = join(certRoot, provider, `${capability}.json`);
    const legacy = join(legacyRoot, provider, `${capability}.json`);
    return atomic(() => {
      if (existsSync(legacy)) atomicFile(legacy.replace(/\.json$/, `.pre-r4-${Math.floor(created)}-${uuid()}.json`), readBounded(legacy));
      const bytes = `${canonicalJson(certificate)}\n`;
      atomicFile(path, bytes); atomicFile(legacy, bytes);
      const receipt = writeReceipt({ kind: 'certificate.sealed', subject: `${provider}:${capability}`, state,
        payload: { path, legacy_path: legacy, digest: certificate.digest } });
      return { ...certificate, path, legacy_path: legacy, receipt };
    });
  }
  async function listCertificates() {
    const rows = [], seen = new Set(), probes = new Map(); let visited = 0;
    for (const root of [certRoot, legacyRoot]) {
      if (!existsSync(root)) continue;
      safePath(root);
      for (const provider of readdirSync(root).sort(compare)) {
        if (++visited > 2000) throw new Error('certificate inventory budget exceeded');
        const directory = join(root, provider);
        if (!lstatSync(directory).isDirectory() || lstatSync(directory).isSymbolicLink()) continue;
        for (const file of readdirSync(directory).sort(compare)) {
          if (++visited > 2000) throw new Error('certificate inventory budget exceeded');
          if (!file.endsWith('.json') || file.includes('.pre-r4-')) continue;
          const capability = file.slice(0, -5), key = `${provider}\0${capability}`, path = join(directory, file);
          if (seen.has(key)) continue;
          seen.add(key);
          let value;
          try { value = JSON.parse(readDocument(path)); name(provider); name(capability); }
          catch { rows.push({ provider, capability, valid: false, reason: 'json:invalid_or_unreadable', path }); continue; }
          const validation = await validate(provider, capability, probes);
          rows.push({ provider, capability, state: value.state, behavior_score: value.behavior_score,
            valid: validation.valid, reason: validation.reason, digest: value.digest, provider_version: value.provider_version, path });
        }
      }
    }
    return rows;
  }
  function policy(id) {
    text(id, 'policy id');
    const row = get('SELECT * FROM policies WHERE id=?', id);
    if (!row) throw new Error('policy not found');
    const { effect_classes_json, ...rest } = row;
    return { ...rest, effect_classes: JSON.parse(effect_classes_json) };
  }
  function listPolicies() {
    const rows = db().query('SELECT * FROM policies ORDER BY capability,provider LIMIT 1001').all();
    if (rows.length > 1000) throw new Error('policy inventory budget exceeded');
    return rows.map(({ effect_classes_json, ...row }) => ({ ...row, effect_classes: JSON.parse(effect_classes_json) }));
  }
  function samePolicy(before) {
    const current = policy(before.id);
    if (canonicalJson(current) !== canonicalJson(before)) throw new Error('policy changed during validation');
  }
  function pinnedValidation(validation, current) {
    if (!validation.valid) throw new Error(`certificate invalid: ${validation.reason}`);
    if (validation.certificate.state !== 'passed') throw new Error('only passed certificates may enter canary');
    if (current && (validation.certificate.digest !== current.certificate_digest ||
        validation.certificate.provider_version !== current.provider_version)) throw new Error('certificate binding changed');
    const reread = parsedDocument(readDocument(validation.path));
    if (reread.calculated !== validation.certificate.digest || reread.value.digest !== reread.calculated) throw new Error('certificate changed after validation');
    if (reread.value.expires_at <= clock()) throw new Error('certificate expired after validation');
  }
  async function upsertPolicy(input) {
    const { capability, provider, fallback_provider, percentage, effect_classes, enable = false } = snapshot(input);
    name(capability); name(provider); text(fallback_provider, 'fallback provider');
    if (fallback_provider === provider) throw new Error('a distinct fallback provider is required');
    bounded(percentage, 1, 25, true);
    if (typeof enable !== 'boolean' || !Array.isArray(effect_classes) || !effect_classes.length ||
        effect_classes.some(effect => !ALLOWED_EFFECTS.includes(effect))) throw new Error('invalid effect classes/enable');
    const effects = [...new Set(effect_classes)].sort(compare), id = hash(`${capability}\0${provider}`);
    const previous = get('SELECT * FROM policies WHERE id=?', id);
    const validation = await validateCertificate(provider, capability);
    return atomic(() => {
      if (canonicalJson(get('SELECT * FROM policies WHERE id=?', id) ?? null) !== canonicalJson(previous ?? null)) throw new Error('policy changed during validation');
      pinnedValidation(validation);
      const current = clock(), state = enable ? 'enabled' : 'proposed', cert = validation.certificate;
      write(`INSERT INTO policies(id,capability,provider,fallback_provider,percentage,effect_classes_json,state,
        certificate_digest,provider_version,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(capability,provider) DO UPDATE SET fallback_provider=excluded.fallback_provider,
        percentage=excluded.percentage,effect_classes_json=excluded.effect_classes_json,state=excluded.state,
        certificate_digest=excluded.certificate_digest,provider_version=excluded.provider_version,updated_at=excluded.updated_at`,
      id, capability, provider, fallback_provider, percentage, canonicalJson(effects), state, cert.digest, cert.provider_version, current, current);
      const receipt = writeReceipt({ kind: 'canary.policy.upsert', subject: id, state,
        payload: { capability, provider, fallback_provider, percentage, effect_classes: effects, certificate_digest: cert.digest } });
      return { ...policy(id), receipt };
    });
  }
  function changeState(id, state) {
    write('UPDATE policies SET state=?,updated_at=? WHERE id=?', state, clock(), id);
    return writeReceipt({ kind: 'canary.policy.state', subject: id, state, payload: { policy_id: id, state } });
  }
  async function setPolicyState(id, state) {
    if (!['proposed', 'enabled', 'suspended', 'disabled'].includes(state)) throw new Error('invalid policy state');
    const before = policy(id), validation = state === 'enabled' ? await validateCertificate(before.provider, before.capability) : null;
    return atomic(() => {
      samePolicy(before);
      if (validation) pinnedValidation(validation, before);
      const receipt = changeState(id, state); return { ...policy(id), receipt };
    });
  }
  function recordDecision(value) {
    const id = digestValue({ ...value, nonce: uuid() }), created = clock();
    write(`INSERT INTO decisions(id,mission_id,task_id,capability,effect_class,selected_provider,fallback_provider,
      mode,bucket,percentage,policy_id,certificate_digest,reason,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
    id, value.mission_id, value.task_id, value.capability, value.effect_class, value.selected_provider,
    value.fallback_provider, value.mode, value.bucket, value.percentage, value.policy_id, value.certificate_digest, value.reason, created);
    const receipt = writeReceipt({ kind: 'canary.route.decision', subject: id, state: value.mode, payload: value });
    return { schema: 'bonfyre.canary-route-decision.v1', id, ...value, created_at: created, receipt };
  }
  async function routeDecide({ mission_id, capability, effect_class, fallback_provider = null, task_id = null }) {
    text(mission_id, 'mission id'); name(capability); text(effect_class, 'effect class');
    text(task_id, 'task id', 4096, true); text(fallback_provider, 'fallback provider', 4096, true);
    const row = get("SELECT id FROM policies WHERE capability=? AND state='enabled' ORDER BY percentage,provider LIMIT 1", capability);
    const before = row ? policy(row.id) : null;
    const validation = before ? await validateCertificate(before.provider, capability) : null;
    const bucket = Number.parseInt(hash(`${mission_id}\0${task_id || ''}\0${capability}`).slice(0, 8), 16) % 10000;
    return atomic(() => {
      const current = get("SELECT id FROM policies WHERE capability=? AND state='enabled' ORDER BY percentage,provider LIMIT 1", capability);
      if ((current?.id ?? null) !== (before?.id ?? null)) throw new Error('policy changed during route validation');
      if (before) samePolicy(before);
      const fallback = fallback_provider || before?.fallback_provider || 'bonfyre-native';
      if (before && fallback === before.provider) throw new Error('fallback cannot select the canary provider');
      let reason = 'no_enabled_policy', selected = false;
      if (before) {
        reason = 'eligible'; selected = true;
        if (!ALLOWED_EFFECTS.includes(effect_class) || !before.effect_classes.includes(effect_class)) { reason = 'effect_class_denied'; selected = false; }
        else if (!validation.valid) { reason = `certificate_${validation.reason}`; selected = false; changeState(before.id, 'suspended'); }
        else if (validation.certificate.state !== 'passed') { reason = 'certificate_not_passed'; selected = false; }
        else if (validation.certificate.digest !== before.certificate_digest) { reason = 'certificate_digest_changed'; selected = false; changeState(before.id, 'suspended'); }
        else if (validation.certificate.provider_version !== before.provider_version) { reason = 'provider_version_changed'; selected = false; changeState(before.id, 'suspended'); }
        else { pinnedValidation(validation, before); if (bucket >= before.percentage * 100) { reason = 'outside_canary_bucket'; selected = false; } }
      }
      return recordDecision({ mission_id, task_id, capability, effect_class, selected_provider: selected ? before.provider : fallback,
        fallback_provider: fallback, mode: selected ? 'canary' : 'fallback', bucket, percentage: before?.percentage ?? 0,
        policy_id: before?.id ?? null, certificate_digest: before?.certificate_digest ?? null, reason });
    });
  }
  function recordOutcome({ decision_id, state, latency_ms = null, receipt_digest = null, payload = {} }) {
    text(decision_id, 'decision id'); text(receipt_digest, 'receipt digest', 4096, true);
    if (!['passed', 'failed', 'fallback', 'cancelled'].includes(state)) throw new Error('invalid outcome state');
    if (latency_ms !== null) bounded(latency_ms, 0, 86400000);
    if (!object(payload)) throw new Error('outcome payload must be an object');
    payload = snapshot(payload);
    return atomic(() => {
      const decision = get('SELECT * FROM decisions WHERE id=?', decision_id);
      if (!decision) throw new Error('decision not found');
      const id = uuid(), at = clock();
      write('INSERT INTO outcomes(id,decision_id,state,latency_ms,receipt_digest,payload_json,created_at) VALUES(?,?,?,?,?,?,?)',
        id, decision_id, state, latency_ms, receipt_digest, canonicalJson(payload), at);
      let suspended = false, reason = null;
      if (decision.policy_id && decision.mode === 'canary') {
        const states = db().query(`SELECT o.state FROM outcomes o JOIN decisions d ON d.id=o.decision_id
          WHERE d.policy_id=? AND d.mode='canary' ORDER BY o.created_at DESC,o.rowid DESC LIMIT 50`).all(decision.policy_id).map(item => item.state);
        const p = policy(decision.policy_id); let consecutive = 0;
        for (const item of states) { if (item !== 'failed') break; consecutive++; }
        const rate = states.filter(item => item === 'failed').length / states.length;
        if (consecutive >= p.max_consecutive_failures) reason = 'consecutive_failures';
        else if (states.length >= p.minimum_outcomes && rate > p.max_failure_rate) reason = 'failure_rate';
        if (reason) { suspended = true; write("UPDATE policies SET state='suspended',updated_at=? WHERE id=?", at, p.id); }
      }
      const result = { schema: 'bonfyre.canary-outcome.v1', id, decision_id, state, latency_ms, receipt_digest,
        payload, policy_suspended: suspended, suspension_reason: reason };
      return { ...result, receipt: writeReceipt({ kind: 'canary.outcome', subject: id, state, payload: result }) };
    });
  }
  const limitValue = limit => Math.max(1, Math.min(bounded(limit, 0, Number.MAX_SAFE_INTEGER, true), 1000));
  const listDecisions = (limit = 100) => db().query('SELECT * FROM decisions ORDER BY created_at DESC,rowid DESC LIMIT ?').all(limitValue(limit));
  const listOutcomes = (limit = 100) => db().query('SELECT * FROM outcomes ORDER BY created_at DESC,rowid DESC LIMIT ?').all(limitValue(limit))
    .map(({ payload_json, ...row }) => ({ ...row, payload: JSON.parse(payload_json) }));
  async function status() {
    const certs = await listCertificates();
    const counts = (table, column) => Object.fromEntries(db().query(`SELECT ${column} AS state,COUNT(*) AS count FROM ${table} GROUP BY ${column}`).all().map(row => [row.state, row.count]));
    return { schema: SCHEMA_VERSION, service: 'bonfyre-fixture-canary-r4', version: VERSION, home, database: dbPath,
      certificates: { total: certs.length, valid: certs.filter(item => item.valid).length,
        ...Object.fromEntries(['passed', 'partial', 'blocked', 'failed'].map(state => [state, certs.filter(item => item.state === state).length])) },
      policies: counts('policies', 'state'), decisions: counts('decisions', 'mode'), outcomes: counts('outcomes', 'state'),
      receipt_chain: verifyReceipts(), authority: { allowed_effect_classes: [...ALLOWED_EFFECTS], max_canary_percentage: 25,
        automatic_promotion: false, automatic_suspension: true, fallback_required: true } };
  }
  function close() { if (connection) connection.close(); connection = null; closed = true; }
  return Object.freeze({ home, dbPath, certRoot, runRoot, receiptRoot, policyRoot, legacyRoot,
    writeReceipt, verifyReceipts, sealCertificate, certificatePath, validateCertificate, listCertificates,
    providerVersion, upsertPolicy, policy, listPolicies, setPolicyState, routeDecide, recordOutcome,
    listDecisions, listOutcomes, status, close });
}
