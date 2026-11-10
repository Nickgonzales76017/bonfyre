import { Database } from 'bun:sqlite';
import { createHash, randomUUID } from 'node:crypto';
import { closeSync, constants, existsSync, fstatSync, fsyncSync, lstatSync, mkdirSync, openSync, readFileSync, readSync, realpathSync, renameSync, unlinkSync, writeFileSync } from 'node:fs';
import { dirname, isAbsolute, join, relative, resolve, sep } from 'node:path';
import { canonicalJson, compareUnicode, digestValue, jsonFloat, parsedDocument } from './canonical_json.mjs';

const MAX_RECEIPTS = 100000, MAX_RECEIPT_BYTES = 64 * 1024 * 1024;
const uuid = () => randomUUID().replaceAll('-', '');
const sha = value => createHash('sha256').update(value).digest('hex');
const boundedJson = value => {
  const encoded = canonicalJson(value);
  if (Buffer.byteLength(encoded) > 8 * 1024 * 1024) throw new Error('document byte budget exceeded');
  return encoded;
};
const isObject = value => value !== null && typeof value === 'object' && !Array.isArray(value);
const valid = (value, name, max = 16384) => {
  if (typeof value !== 'string' || !value || value.includes('\0') || Buffer.byteLength(value) > max) throw new Error(`invalid ${name}`);
  return value;
};
const inside = (root, path) => { const rel = relative(root, path); return rel === '' || rel !== '..' && !rel.startsWith(`..${sep}`) && !isAbsolute(rel); };
function readPinned(path, maximum = 8 * 1024 * 1024) {
  const fd = openSync(path, constants.O_RDONLY | (constants.O_NOFOLLOW ?? 0) | (constants.O_NONBLOCK ?? 0));
  try {
    const first = fstatSync(fd), named = lstatSync(path);
    if (!first.isFile() || !named.isFile() || named.isSymbolicLink() || first.size > maximum || first.dev !== named.dev || first.ino !== named.ino)
      throw new Error('unbounded or changed file');
    const bytes = Buffer.alloc(first.size); let used = 0;
    while (used < bytes.length) { const n = readSync(fd, bytes, used, bytes.length - used, null); if (!n) throw new Error('truncated file'); used += n; }
    const after = fstatSync(fd), namedAfter = lstatSync(path);
    if (first.size !== after.size || first.mtimeMs !== after.mtimeMs || first.ctimeMs !== after.ctimeMs || namedAfter.ino !== first.ino || namedAfter.dev !== first.dev)
      throw new Error('file changed while reading');
    return new TextDecoder('utf-8', { fatal: true }).decode(bytes);
  } finally { closeSync(fd); }
}
function existingAncestor(path) {
  const tail = []; let parent = resolve(path);
  while (!existsSync(parent)) { tail.unshift(parent.slice(dirname(parent).length + 1)); parent = dirname(parent); }
  if (lstatSync(parent).isSymbolicLink()) throw new Error('symlink store ancestor refused');
  return join(realpathSync(parent), ...tail);
}
function owned(root, path) {
  path = resolve(path);
  if (!inside(root, path)) throw new Error('path escapes hypervisor home');
  let current = root;
  for (const part of relative(root, path).split(sep).filter(Boolean)) {
    current = join(current, part);
    if (existsSync(current) && lstatSync(current).isSymbolicLink()) throw new Error('symlink owned path refused');
  }
  return path;
}
const CAPABILITY_META = {
  'agent.executor': ['Execute bounded coding work', 'write'], 'agent.architect': ['Produce system architecture and plans', 'read'],
  'agent.reviewer': ['Review source or diffs', 'read'], 'agent.verifier': ['Independently verify claims', 'read'],
  'context.output_compression': ['Compress model replies without changing technical payloads', 'read'],
  'context.tool_compression': ['Filter tool output before model context', 'read'], 'context.memory_compression': ['Compress durable memory at rest', 'write'],
  'memory.capture': ['Capture lifecycle observations', 'write'], 'memory.search': ['Search prior observations', 'read'],
  'memory.timeline': ['Retrieve a session timeline progressively', 'read'], 'spec.compile': ['Compile goals into durable specs', 'write'],
  'spec.invariant_backprop': ['Feed failures back into invariants/specs', 'write'],
  'scheduler.deterministic_dag': ['Execute a deterministic task graph', 'write'],
  'scheduler.persisted_dag': ['Persist and operate a task DAG', 'write'], 'scheduler.wave_execution': ['Run dependency-ready tasks in waves', 'write'],
  'workspace.worktree': ['Create isolated Git workspaces', 'write'], 'workspace.sandbox': ['Execute within a sandbox', 'write'],
  'session.fork': ['Fork native agent sessions', 'write'], 'session.resume': ['Resume native agent sessions', 'write'],
  'session.migrate': ['Migrate a session/account while preserving history', 'write'],
  'fleet.pty': ['Supervise multiple agent PTYs', 'write'], 'fleet.status_detection': ['Detect running/waiting/idle/error state', 'read'],
  'fleet.remote_control': ['Operate a fleet remotely', 'write'], 'mcp.pool': ['Share and recover MCP server processes', 'write'],
  'mcp.proxy': ['Expose or proxy MCP tools', 'write'], 'review.diff': ['Review source diffs', 'read'],
  'review.inline': ['Attach inline review comments', 'write'], 'review.pr': ['Operate pull request review workflows', 'write'],
  'proof.lineage': ['Record lineage of steps and artifacts', 'write'], 'proof.replay': ['Replay and verify prior execution', 'read'],
  'proof.audit_chain': ['Create tamper-evident execution chains', 'write'], 'gate.human': ['Pause for explicit human disposition', 'write'],
  'worker.recovery': ['Reconcile and retry failed tasks', 'write'], 'worker.pr_maintenance': ['Operate PR maintenance workers', 'write'],
  'deployment.remote_ssh': ['Place work on SSH targets', 'write'], 'ui.terminal': ['Expose a terminal UI', 'read'],
  'ui.desktop': ['Expose a desktop UI', 'read'], 'ui.web': ['Expose a web UI', 'read'],
};

export function createHypervisorStore({ home, dbPath, registryPath, now = () => Date.now() / 1000, id = uuid,
  checkStop: stopCheck = () => {}, readOnly = false } = {}) {
  home = existingAncestor(valid(home, 'home'));
  dbPath = owned(home, existingAncestor(dbPath || join(home, 'hypervisor.sqlite3')));
  registryPath = resolve(valid(registryPath || join(home, 'ecosystem_registry.json'), 'registry path'));
  const receiptRoot = join(home, 'receipts'), missionRoot = join(home, 'missions');
  let connection, closed = false, cachedRegistry, loaded = false, routeQueries = 0;
  const checkStop = () => { if (readOnly) throw new Error('read-only hypervisor store refuses writes'); stopCheck(); };
  const clock = () => { const at = now(); if (!Number.isFinite(at) || at < 0) throw new Error('invalid time'); return at; };
  const identity = () => { const value = id(); if (!/^[a-f0-9]{32}$/.test(value)) throw new Error('invalid generated id'); return value; };
  function db() {
    if (closed) throw new Error('hypervisor store closed');
    if (!connection) {
      if (readOnly) {
        if (!existsSync(dbPath)) throw new Error('read-only hypervisor database missing');
        connection = new Database(dbPath, { readonly: true, strict: true });
        connection.exec('PRAGMA query_only=ON; PRAGMA busy_timeout=30000;');
        return connection;
      }
      checkStop();
      if (!existsSync(home)) checkStop();
      mkdirSync(home, { recursive: true, mode: 0o700 });
      for (const path of [receiptRoot, missionRoot, join(home, 'sources')]) { if (!existsSync(path)) checkStop(); mkdirSync(owned(home, path), { recursive: true, mode: 0o700 }); }
      connection = new Database(owned(home, dbPath), { create: true, strict: true });
      try { connection.exec('PRAGMA busy_timeout=30000;'); connection.exec(readFileSync(new URL('./hypervisor_schema.sql', import.meta.url), 'utf8')); }
      catch (error) { connection.close(); connection = null; throw error; }
    }
    return connection;
  }
  const sql = (query, ...args) => db().query(query).run(...args);
  const one = (query, ...args) => db().query(query).get(...args);
  function registry() {
    if (cachedRegistry) return structuredClone(cachedRegistry);
    const value = JSON.parse(readPinned(registryPath, 8 * 1024 * 1024));
    if (!isObject(value) || !Array.isArray(value.projects) || value.projects.length > 1000 || !Array.isArray(value.capability_contracts) || value.capability_contracts.length > 1000)
      throw new Error('invalid hypervisor registry');
    const providerIds = new Set(), capabilities = new Set(value.capability_contracts.map(item => valid(item, 'capability id', 256)));
    for (const project of value.projects) {
      if (!isObject(project)) throw new Error('invalid registry project');
      for (const key of ['id', 'repo', 'homepage', 'license', 'integration', 'default_mode']) valid(project[key], key, 4096);
      if (providerIds.has(project.id)) throw new Error('duplicate registry provider');
      providerIds.add(project.id);
      if (!Array.isArray(project.capabilities) || project.capabilities.length > 1000 || project.capabilities.some(cap => !capabilities.has(cap)))
        throw new Error('undeclared provider capability');
      if (project.detect !== undefined && (!Array.isArray(project.detect) || project.detect.length > 100 || project.detect.some(cmd => typeof cmd !== 'string' || !cmd || cmd.length > 512)))
        throw new Error('invalid provider detection contract');
    }
    cachedRegistry = value; return structuredClone(value);
  }
  function loadRegistry() {
    if (loaded) return;
    const source = registry(), at = clock(); checkStop();
    db().transaction(() => {
      checkStop();
      for (const capability of source.capability_contracts) {
        const [description, effect] = Object.hasOwn(CAPABILITY_META, capability) ? CAPABILITY_META[capability] : ['', 'read'];
        sql('INSERT INTO capabilities(id,description,effect_class) VALUES(?,?,?) ON CONFLICT(id) DO UPDATE SET description=excluded.description,effect_class=excluded.effect_class', capability, description, effect);
      }
      for (const project of source.projects) {
        const metadata = { install: project.install ?? {}, detect: project.detect ?? [], notes: project.notes ?? '' };
        sql(`INSERT INTO providers(id,repo,homepage,license,integration,mode,metadata_json,updated_at) VALUES(?,?,?,?,?,?,?,?)
          ON CONFLICT(id) DO UPDATE SET repo=excluded.repo,homepage=excluded.homepage,license=excluded.license,
          integration=excluded.integration,metadata_json=excluded.metadata_json,updated_at=excluded.updated_at`,
        project.id, project.repo, project.homepage, project.license, project.integration, project.default_mode, JSON.stringify(metadata), at);
        for (const capability of project.capabilities)
          sql("INSERT INTO provider_capabilities(provider_id,capability_id,state,score,evidence_json,updated_at) VALUES(?,?,'declared',0,'[]',?) ON CONFLICT(provider_id,capability_id) DO NOTHING", project.id, capability, at);
      }
      checkStop();
    }).immediate();
    loaded = true;
  }
  function event(source, kind, payload, missionId = null) {
    checkStop(); valid(source, 'event source'); valid(kind, 'event kind');
    return sql('INSERT INTO events(ts,source,kind,mission_id,payload_json) VALUES(?,?,?,?,?)', clock(), source, kind, missionId, boundedJson(payload)).lastInsertRowid;
  }
  function events({ after = 0, limit = 500 } = {}) {
    if (!Number.isSafeInteger(after) || after < 0 || !Number.isSafeInteger(limit) || limit < 1 || limit > 2000) throw new Error('invalid event cursor');
    const result = []; let bytes = 0;
    // Bound before SQLite exposes a legacy oversized payload to the JS heap.
    for (const { payload_json, payload_bytes, ...row } of db().query(`SELECT seq,ts,source,kind,mission_id,
      length(CAST(payload_json AS BLOB)) AS payload_bytes,
      CASE WHEN length(CAST(payload_json AS BLOB))<=8388608 THEN payload_json ELSE NULL END AS payload_json
      FROM events WHERE seq>? ORDER BY seq LIMIT ?`).iterate(after, limit)) {
      bytes += payload_bytes;
      if (payload_json === null || bytes > 16 * 1024 * 1024) throw new Error('event read byte budget exceeded');
      result.push({ ...row, payload: JSON.parse(payload_json) });
    }
    return { events: result };
  }
  function capabilityRows(providerId = null) {
    const rows = db().query(`SELECT pc.*,c.description,c.effect_class FROM provider_capabilities pc
      JOIN capabilities c ON c.id=pc.capability_id ${providerId ? 'WHERE pc.provider_id=?' : ''}
      ORDER BY pc.provider_id,pc.capability_id`).all(...(providerId ? [providerId] : []));
    return rows.map(({ evidence_json, ...row }) => ({ ...row, evidence: JSON.parse(evidence_json) }));
  }
  function providers() {
    if (!readOnly) loadRegistry();
    const rows = db().query('SELECT * FROM providers ORDER BY id LIMIT 1001').all();
    if (rows.length > 1000) throw new Error('provider read budget exceeded');
    const grouped = new Map(); for (const cap of capabilityRows()) {
      if (!grouped.has(cap.provider_id)) grouped.set(cap.provider_id, []);
      grouped.get(cap.provider_id).push(cap);
    }
    return rows.map(({ metadata_json, ...row }) => ({ ...row, metadata: JSON.parse(metadata_json), capabilities: grouped.get(row.id) ?? [] }));
  }
  function provider(providerId) {
    if (!readOnly) loadRegistry(); valid(providerId, 'provider id');
    const row = one('SELECT * FROM providers WHERE id=?', providerId);
    if (!row) return null;
    const { metadata_json, ...fields } = row;
    return { ...fields, metadata: JSON.parse(metadata_json), capabilities: capabilityRows(providerId) };
  }
  function observe(method, ...args) {
    if (closed) throw new Error('hypervisor store closed');
    const reader = createHypervisorStore({ home, dbPath, registryPath, readOnly: true });
    try { return reader[method](...args); } finally { reader.close(); }
  }
  function routeMany(capabilities, { scope = 'global', include_shadow = false } = {}) {
    if (!readOnly) loadRegistry(); valid(scope, 'route scope');
    if (!Array.isArray(capabilities) || capabilities.length > 1000 || capabilities.some(item => typeof item !== 'string' || !item || item.length > 256))
      throw new Error('invalid route capability set');
    const unique = [...new Set(capabilities)], result = Object.assign(Object.create(null), Object.fromEntries(unique.map(cap => [cap, []])));
    if (!unique.length) return result;
    const modes = include_shadow ? ['active', 'candidate', 'shadow'] : ['active', 'candidate'];
    const marks = unique.map(() => '?').join(',');
    routeQueries++;
    const rows = db().query(`SELECT pc.capability_id,p.id AS provider_id,p.mode,p.installed,p.health,p.license,p.license_approved,
      pc.score,pc.state,pc.evidence_json,COALESCE(r.weight,1) AS route_weight,COALESCE(r.state,'unrouted') AS route_state
      FROM provider_capabilities pc JOIN providers p ON p.id=pc.provider_id
      LEFT JOIN routes r ON r.provider_id=p.id AND r.capability_id=pc.capability_id AND r.scope=?
      WHERE pc.capability_id IN (${marks}) AND p.mode IN (${modes.map(() => '?').join(',')})
      ORDER BY pc.capability_id,CASE WHEN r.state='promoted' THEN 0 ELSE 1 END,p.installed DESC,
        CASE p.health WHEN 'verified' THEN 0 WHEN 'reachable' THEN 1 WHEN 'degraded' THEN 2 ELSE 3 END,
        (pc.score * COALESCE(r.weight,1)) DESC,p.id LIMIT 100001`).all(scope, ...unique, ...modes);
    if (rows.length > 100000) throw new Error('route read budget exceeded');
    for (const { capability_id, evidence_json, ...row } of rows) {
      const eligible = !!row.installed && ['reachable', 'verified'].includes(row.health) &&
        (['MIT', 'Apache-2.0', 'vendor', 'private'].includes(row.license) || !!row.license_approved);
      result[capability_id].push({ ...row, evidence: JSON.parse(evidence_json), eligible });
    }
    return result;
  }
  function route(capability, options = {}) { valid(capability, 'capability'); return routeMany([capability], options)[capability]; }
  function promote(providerId, capability, { scope = 'global', weight = 1 } = {}) {
    checkStop(); valid(scope, 'route scope');
    if (!Number.isFinite(weight) || weight < 0 || weight > 1000) throw new Error('invalid route weight');
    const value = provider(providerId); if (!value) throw new Error('provider not found');
    if (['retired', 'quarantined'].includes(value.mode)) throw new Error(`provider mode blocks promotion: ${value.mode}`);
    if (!value.installed || !['reachable', 'verified'].includes(value.health)) throw new Error('provider is not installed and reachable');
    if (!['MIT', 'Apache-2.0', 'vendor', 'private'].includes(value.license) && !value.license_approved) throw new Error('provider license is not approved');
    if (!value.repo.startsWith('local/') && value.license !== 'vendor' && (!value.source_ref || !value.source_digest))
      throw new Error('provider source is not absorbed and immutably pinned; run source_absorb before promotion');
    const cap = one('SELECT * FROM provider_capabilities WHERE provider_id=? AND capability_id=?', providerId, capability);
    if (!cap) throw new Error('provider does not declare capability');
    if (cap.score < 0.5) throw new Error('provider capability lacks sufficient evaluation score');
    const result = { provider: providerId, capability, scope, state: 'promoted', score: cap.score };
    db().transaction(() => { checkStop(); sql(`INSERT INTO routes(capability_id,scope,provider_id,state,weight,policy_json,updated_at)
      VALUES(?,?,?,'promoted',?,'{}',?) ON CONFLICT(capability_id,scope,provider_id)
      DO UPDATE SET state='promoted',weight=excluded.weight,updated_at=excluded.updated_at`, capability, scope, providerId, weight, clock());
    event('hypervisor', 'provider.promoted', result); }).immediate();
    return result;
  }
  function quarantine(providerId, reason) {
    checkStop(); valid(providerId, 'provider id'); valid(reason, 'quarantine reason');
    if (!provider(providerId)) throw new Error('provider not found');
    const result = { provider: providerId, state: 'quarantined', reason };
    db().transaction(() => { checkStop(); const at = clock();
      sql("UPDATE providers SET mode='quarantined',health='blocked',updated_at=? WHERE id=?", at, providerId);
      sql("UPDATE routes SET state='quarantined',updated_at=? WHERE provider_id=?", at, providerId);
      event('hypervisor', 'provider.quarantined', result); }).immediate();
    return result;
  }
  function approveLicense(providerId, note) {
    checkStop(); valid(providerId, 'provider id'); valid(note, 'license approval note');
    const value = provider(providerId); if (!value) throw new Error('provider not found');
    const at = clock(), metadata = { ...value.metadata, license_approval_note: note, license_approved_at: at };
    const result = { provider: providerId, license_approved: true, note };
    db().transaction(() => { checkStop(); sql('UPDATE providers SET license_approved=1,updated_at=?,metadata_json=? WHERE id=?', at, JSON.stringify(metadata), providerId);
      event('hypervisor', 'provider.license_approved', result); }).immediate();
    return result;
  }
  function recordDiscovery({ providers: findings, observed_at = clock() } = {}) {
    checkStop(); const source = registry();
    if (!Array.isArray(findings) || findings.length > source.projects.length) throw new Error('invalid discovery result');
    const allowed = new Set(source.projects.map(project => project.id)), seen = new Set();
    for (const item of findings) {
      if (!isObject(item) || !allowed.has(item.provider) || seen.has(item.provider) || typeof item.installed !== 'boolean' ||
          !['missing', 'reachable', 'degraded', 'verified'].includes(item.health) || !Array.isArray(item.commands) || item.commands.length > 100 ||
          item.commands.some(pair => !Array.isArray(pair) || pair.length !== 2 || pair.some(v => typeof v !== 'string' || v.length > 4096)) ||
          !Array.isArray(item.evidence) || item.evidence.length > 100) throw new Error('invalid discovery result');
      seen.add(item.provider);
    }
    if (!Number.isFinite(observed_at) || observed_at < 0) throw new Error('invalid discovery time');
    loadRegistry();
    db().transaction(() => { checkStop();
      for (const item of findings) sql('UPDATE providers SET installed=?,command_path=?,version=?,health=?,discovered_at=?,updated_at=? WHERE id=?',
        item.installed ? 1 : 0, item.commands[0]?.[1] ?? null, item.version ?? null, item.health, observed_at, observed_at, item.provider);
      event('hypervisor', 'ecosystem.discovered', { providers: findings });
      checkStop();
    }).immediate();
    return { providers: findings };
  }
  function output(path, bytes) {
    checkStop(); path = owned(home, path); mkdirSync(dirname(path), { recursive: true, mode: 0o700 });
    const temporary = `${path}.${identity()}.tmp`; let fd;
    try { fd = openSync(temporary, 'wx', 0o600); writeFileSync(fd, bytes); fsyncSync(fd); closeSync(fd); fd = undefined;
      checkStop(); renameSync(temporary, path); }
    finally { if (fd !== undefined) closeSync(fd); if (existsSync(temporary)) unlinkSync(temporary); }
    return path;
  }
  function writeReceipt({ mission_id = null, task_id = null, provider_id = null, capability_id = null, state, payload }) {
    checkStop(); valid(state, 'receipt state'); const rid = identity();
    return db().transaction(() => {
      checkStop();
      const previousRow = one('SELECT digest,created_at FROM receipts ORDER BY created_at DESC,id DESC LIMIT 1');
      const at = Math.max(clock(), previousRow ? previousRow.created_at + 0.000001 : 0);
      const body = { id: rid, mission_id, task_id, provider_id, capability_id, state, payload,
        previous_digest: previousRow?.digest ?? null, created_at: jsonFloat(at) };
      const digest = digestValue(body, { spaced: true }), path = join(receiptRoot, `${Math.floor(at)}-${rid}.json`);
      output(path, `${canonicalJson({ ...body, digest }, { spaced: true })}\n`);
      try { checkStop(); sql('INSERT INTO receipts(id,mission_id,task_id,provider_id,capability_id,state,path,digest,previous_digest,payload_json,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)',
        rid, mission_id, task_id, provider_id, capability_id, state, path, digest, previousRow?.digest ?? null, canonicalJson(payload), at); }
      catch (error) { throw new Error(`receipt file may exist without database row; reconcile ${rid}: ${error.message}`); }
      return { receipt_id: rid, path, digest, previous_digest: previousRow?.digest ?? null };
    }).immediate();
  }
  function verifyReceiptChain() {
    const rows = db().query('SELECT * FROM receipts ORDER BY created_at,id LIMIT 100001').all();
    if (rows.length > MAX_RECEIPTS) throw new Error('receipt verification row budget exceeded');
    const failures = []; let previous = null, bytes = 0;
    for (const row of rows) {
      const fail = reason => { if (failures.length < 1000) failures.push({ id: row.id, reason }); };
      try {
        const path = owned(receiptRoot, row.path), source = readPinned(path);
        bytes += Buffer.byteLength(source); if (bytes > MAX_RECEIPT_BYTES) throw new Error('receipt verification byte budget exceeded');
        const { value: doc, calculated } = parsedDocument(source, { spaced: true });
        if (doc.digest !== calculated || row.digest !== calculated) fail('digest_mismatch');
        if (doc.id !== row.id || doc.state !== row.state || doc.created_at !== row.created_at ||
            ['mission_id', 'task_id', 'provider_id', 'capability_id'].some(key => doc[key] !== row[key]) ||
            canonicalJson(doc.payload) !== canonicalJson(JSON.parse(row.payload_json))) fail('row_document_mismatch');
      } catch { fail('unreadable_document'); }
      if (row.previous_digest !== previous) fail('chain_mismatch');
      previous = row.digest;
    }
    return { verified: failures.length === 0, receipts: rows.length, head: previous, failures };
  }
  function summary() {
    const groups = (table, column) => Object.fromEntries(db().query(`SELECT ${column} AS state,count(*) AS count FROM ${table} GROUP BY ${column}`).all()
      .map(row => [row.state, row.count]));
    return { providers: groups('providers', 'mode'), installed_providers: one('SELECT count(*) AS n FROM providers WHERE installed=1').n,
      capabilities: one('SELECT count(*) AS n FROM capabilities').n, missions: groups('missions', 'state'),
      evaluations: one('SELECT count(*) AS n FROM evaluations').n, receipts: one('SELECT count(*) AS n FROM receipts').n,
      sources: one('SELECT count(*) AS n FROM sources').n };
  }
  function recentReceipts({ limit = 12 } = {}) {
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 1000) throw new Error('invalid receipt limit');
    return { scope: 'recent-owner-observations-not-full-chain-verification', receipts: db().query(
      'SELECT id,mission_id,task_id,provider_id,capability_id,state,digest,previous_digest,created_at FROM receipts ORDER BY created_at DESC,id DESC LIMIT ?').all(limit) };
  }
  const missionPath = missionId => owned(missionRoot, join(missionRoot, valid(missionId, 'mission id', 256)));
  function intent(kind, payload, work) {
    const started = writeReceipt({ state: 'started', payload: { kind, ...payload } });
    try { const value = work(started); writeReceipt({ state: 'completed', payload: { kind, started_receipt_id: started.receipt_id } }); return value; }
    catch (error) { try { writeReceipt({ state: 'uncertain', payload: { kind, started_receipt_id: started.receipt_id, reconcile_before_retry: true } }); } catch { /* started remains */ }
      throw new Error(`${kind} outcome uncertain; reconcile receipt ${started.receipt_id}: ${error.message}`); }
  }
  function createMission({ mission, spec, graph }) {
    if (!isObject(mission) || !Array.isArray(mission.tasks) || mission.tasks.length > 10000) throw new Error('invalid mission');
    valid(mission.id, 'mission id', 256); valid(mission.goal, 'mission goal', 100000);
    if (typeof spec !== 'string' || Buffer.byteLength(spec) > 8 * 1024 * 1024) throw new Error('invalid spec');
    const dir = missionPath(mission.id), specPath = join(dir, 'SPEC.md'), graphPath = join(dir, 'task-graph.json');
    if (existsSync(dir) || one('SELECT id FROM missions WHERE id=?', mission.id)) throw new Error('mission already exists');
    return intent('mission.compiled', { mission_id: mission.id }, () => {
      checkStop(); mkdirSync(dir, { mode: 0o700 });
      output(join(dir, 'mission.json'), `${canonicalJson(mission)}\n`); output(specPath, spec); output(graphPath, `${canonicalJson(graph)}\n`);
      db().transaction(() => {
        checkStop(); const at = clock();
        sql("INSERT INTO missions(id,goal,state,spec_path,mission_json,created_at,updated_at) VALUES(?,?,'compiled',?,?,?,?)",
          mission.id, mission.goal, specPath, JSON.stringify(mission), at, at);
        for (const task of mission.tasks) sql("INSERT INTO mission_tasks(id,mission_id,title,state,dependencies_json,capability,created_at,updated_at) VALUES(?,?,?,'planned',?,?,?,?)",
          task.id, mission.id, task.title, JSON.stringify(task.dependencies ?? []), task.capability ?? null, at, at);
        event('mission-compiler', 'mission.compiled', { mission_id: mission.id, spec_path: specPath,
          capabilities: [...new Set(mission.requested_capabilities ?? [])].sort(compareUnicode) }, mission.id);
      }).immediate();
      return { mission_dir: dir, spec_path: specPath, graph_path: graphPath };
    });
  }
  function getMission(missionId) {
    const row = one('SELECT mission_json FROM missions WHERE id=?', valid(missionId, 'mission id', 256));
    if (!row) throw new Error('mission not found'); return JSON.parse(row.mission_json);
  }
  function saveMission({ mission, spec, graph }) {
    if (!isObject(mission)) throw new Error('invalid mission');
    const dir = missionPath(mission.id), old = one('SELECT mission_json FROM missions WHERE id=?', mission.id);
    if (!old) throw new Error('mission not found');
    if (typeof spec !== 'string' || Buffer.byteLength(spec) > 8 * 1024 * 1024) throw new Error('invalid spec');
    return intent('mission.saved', { mission_id: mission.id }, () => {
      output(join(dir, 'mission.json'), `${canonicalJson(mission)}\n`); output(join(dir, 'SPEC.md'), spec); output(join(dir, 'task-graph.json'), `${canonicalJson(graph)}\n`);
      checkStop(); sql('UPDATE missions SET mission_json=?,spec_path=?,updated_at=MAX(?,updated_at+0.000001) WHERE id=?', JSON.stringify(mission), join(dir, 'SPEC.md'), clock(), mission.id);
      return { mission_id: mission.id, spec_path: join(dir, 'SPEC.md') };
    });
  }
  function saveRoute({ mission_id, payload }) {
    valid(mission_id, 'mission id', 256); if (!one('SELECT id FROM missions WHERE id=?', mission_id)) throw new Error('mission not found');
    return intent('mission.routed', { mission_id }, () => {
      db().transaction(() => { checkStop(); sql('UPDATE missions SET route_json=?,updated_at=MAX(?,updated_at+0.000001) WHERE id=?', JSON.stringify(payload), clock(), mission_id);
        event('mission-compiler', 'mission.routed', payload, mission_id); }).immediate();
      return payload;
    });
  }
  function upsertBug({ mission_id, bug }) {
    valid(mission_id, 'mission id', 256); if (!isObject(bug)) throw new Error('invalid bug');
    for (const key of ['id', 'symptom', 'root_cause', 'regression_test']) valid(bug[key], key, 100000);
    return intent('failure.backpropagated', { mission_id, bug_id: bug.id }, () => {
      db().transaction(() => { checkStop(); const at = clock();
        sql(`INSERT INTO bugs(id,mission_id,invariant_id,symptom,root_cause,regression_test,state,evidence_json,created_at,updated_at)
          VALUES(?,?,?,?,?,?,'open',?,?,?) ON CONFLICT(id) DO UPDATE SET root_cause=excluded.root_cause,
          regression_test=excluded.regression_test,invariant_id=excluded.invariant_id,evidence_json=excluded.evidence_json,updated_at=excluded.updated_at`,
        bug.id, mission_id, bug.invariant_id ?? null, bug.symptom, bug.root_cause, bug.regression_test, JSON.stringify(bug.evidence ?? []), at, at);
        event('mission-compiler', 'failure.backpropagated', { bug, new_invariant: bug.invariant_id ?? null }, mission_id);
      }).immediate();
      return { bug, new_invariant: bug.invariant_id ?? null };
    });
  }
  function saveBackpropagation({ mission, spec, graph, bug }) {
    if (!isObject(mission) || !isObject(bug)) throw new Error('invalid backpropagation');
    const dir = missionPath(mission.id), previous = one('SELECT mission_json FROM missions WHERE id=?', mission.id);
    if (!previous) throw new Error('mission not found');
    if (typeof spec !== 'string' || Buffer.byteLength(spec) > 8 * 1024 * 1024 ||
        !Array.isArray(mission.bugs) || !mission.bugs.some(item => item.id === bug.id)) throw new Error('invalid backpropagation files');
    for (const key of ['id', 'symptom', 'root_cause', 'regression_test']) valid(bug[key], key, 100000);
    return intent('failure.backpropagated', { mission_id: mission.id, bug_id: bug.id }, () => {
      const documents = [[join(dir, 'mission.json'), `${canonicalJson(mission)}\n`],
        [join(dir, 'SPEC.md'), spec], [join(dir, 'task-graph.json'), `${canonicalJson(graph)}\n`]];
      const stages = [];
      try {
        checkStop();
        for (const [target, content] of documents) {
          owned(home, target);
          const temporary = `${target}.${identity()}.tmp`, fd = openSync(temporary, 'wx', 0o600);
          try { writeFileSync(fd, content); fsyncSync(fd); } finally { closeSync(fd); }
          stages.push([temporary, target]);
        }
        db().transaction(() => {
          checkStop();
          if (one('SELECT mission_json FROM missions WHERE id=?', mission.id)?.mission_json !== previous.mission_json)
            throw new Error('mission changed during backpropagation');
          for (const [temporary, target] of stages) { checkStop(); renameSync(temporary, target); }
          const at = clock();
          sql('UPDATE missions SET mission_json=?,spec_path=?,updated_at=MAX(?,updated_at+0.000001) WHERE id=?', JSON.stringify(mission), join(dir, 'SPEC.md'), at, mission.id);
          sql(`INSERT INTO bugs(id,mission_id,invariant_id,symptom,root_cause,regression_test,state,evidence_json,created_at,updated_at)
            VALUES(?,?,?,?,?,?,'open',?,?,?) ON CONFLICT(id) DO UPDATE SET root_cause=excluded.root_cause,
            regression_test=excluded.regression_test,invariant_id=excluded.invariant_id,evidence_json=excluded.evidence_json,updated_at=excluded.updated_at`,
          bug.id, mission.id, bug.invariant_id ?? null, bug.symptom, bug.root_cause, bug.regression_test, JSON.stringify(bug.evidence ?? []), at, at);
          event('mission-compiler', 'failure.backpropagated', { bug, new_invariant: bug.invariant_id ?? null }, mission.id);
          checkStop();
        }).immediate();
        return { bug, new_invariant: bug.invariant_id ?? null };
      } finally { for (const [temporary] of stages) if (existsSync(temporary)) unlinkSync(temporary); }
    });
  }
  function registerSession({ provider_id, native_session_id, mission_id = null, cwd = null,
    parent_session_id = null, state = 'active', metadata = {} }) {
    checkStop();
    if (!provider(provider_id)) throw new Error('provider not found');
    valid(native_session_id, 'native session id', 4096); valid(state, 'session state', 128);
    for (const [key, value] of Object.entries({ mission_id, cwd, parent_session_id })) if (value !== null) valid(value, key, 4096);
    const sid = `session-${sha(`${provider_id}\0${native_session_id}`).slice(0, 24)}`, encoded = boundedJson(metadata);
    return db().transaction(() => {
      checkStop(); const at = clock();
      sql(`INSERT INTO sessions VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(provider_id,native_session_id)
        DO UPDATE SET mission_id=COALESCE(excluded.mission_id,sessions.mission_id),cwd=COALESCE(excluded.cwd,sessions.cwd),
        parent_session_id=COALESCE(excluded.parent_session_id,sessions.parent_session_id),state=excluded.state,
        metadata_json=excluded.metadata_json,updated_at=excluded.updated_at`,
      sid, provider_id, native_session_id, mission_id, cwd, parent_session_id, state, encoded, at, at);
      const result = getSession(sid); event('hypervisor', 'session.registered', result, mission_id); checkStop(); return result;
    }).immediate();
  }
  const sessionRow = row => { if (!row) return null; const { metadata_json, ...value } = row; return { ...value, metadata: JSON.parse(metadata_json) }; };
  function getSession(sid) { return sessionRow(one('SELECT * FROM sessions WHERE id=?', valid(sid, 'session id', 256))); }
  function listSessions({ mission_id = null, provider_id = null, limit = 100 } = {}) {
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 1000) throw new Error('invalid session limit');
    const clauses = [], args = [];
    for (const [key, value] of Object.entries({ mission_id, provider_id })) if (value !== null) { valid(value, key); clauses.push(`${key}=?`); args.push(value); }
    return db().query(`SELECT * FROM sessions ${clauses.length ? `WHERE ${clauses.join(' AND ')}` : ''} ORDER BY updated_at DESC,id LIMIT ?`).all(...args, limit).map(sessionRow);
  }
  function persistEvaluation({ id: eid, provider_id, started_at, completed_at, capabilities, provider_health, document }) {
    checkStop(); valid(eid, 'evaluation id', 256);
    if (!/^[A-Za-z0-9_-]+$/.test(eid)) throw new Error('invalid evaluation id');
    const source = provider(provider_id); if (!source) throw new Error('provider not found');
    if (!Array.isArray(capabilities) || !capabilities.length || capabilities.length > 1000 ||
        !Number.isFinite(started_at) || started_at < 0 || !Number.isFinite(completed_at) || completed_at < started_at ||
        !['verified', 'reachable', 'degraded'].includes(provider_health)) throw new Error('invalid evaluation');
    const declared = new Set(source.capabilities.map(cap => cap.capability_id)), seen = new Set();
    for (const cap of capabilities) {
      if (!declared.has(cap.capability) || seen.has(cap.capability) || !['passed', 'partial', 'failed', 'blocked', 'unavailable'].includes(cap.state) ||
          !Number.isFinite(cap.score) || cap.score < 0 || cap.score > 1) throw new Error('invalid capability evaluation');
      seen.add(cap.capability); boundedJson(cap.metrics); boundedJson(cap.evidence);
    }
    if (provider_health === 'verified' && capabilities.some(cap => cap.state !== 'passed')) throw new Error('verified health requires passing capability evidence');
    const bytes = `${boundedJson(document)}\n`, path = join(home, 'evaluations', `${eid}.json`);
    if (existsSync(path) || one('SELECT id FROM evaluations WHERE id LIKE ? LIMIT 1', `${eid}:%`)) throw new Error('evaluation already exists');
    return intent('provider.evaluated', { evaluation_id: eid, provider_id }, () => {
      output(path, bytes);
      db().transaction(() => {
        checkStop();
        for (const cap of capabilities) {
          sql('INSERT INTO evaluations VALUES(?,?,?,?,?,?,?,?,?)', `${eid}:${cap.capability}`, provider_id, cap.capability,
            cap.state, cap.score, boundedJson(cap.metrics), boundedJson(cap.evidence), started_at, completed_at);
          sql('UPDATE provider_capabilities SET state=?,score=?,evidence_json=?,updated_at=? WHERE provider_id=? AND capability_id=?',
            cap.state === 'passed' ? 'evaluated' : cap.state === 'partial' ? 'candidate' : 'failed', cap.score,
            boundedJson([{ evaluation_id: eid, capability: cap.capability, score: cap.score, state: cap.state }]), completed_at, provider_id, cap.capability);
        }
        sql('UPDATE providers SET health=?,updated_at=? WHERE id=?', provider_health, completed_at, provider_id);
        event('hypervisor', 'provider.evaluated', { evaluation_id: eid, provider_id, path, digest: sha(bytes) }); checkStop();
      }).immediate();
      return { ...document, path };
    });
  }
  function latestEvaluations(capability, { providers: ids } = {}) {
    valid(capability, 'capability', 256);
    if (ids !== undefined && (!Array.isArray(ids) || ids.length > 1000 || ids.some(value => typeof value !== 'string' || !value || value.length > 256))) throw new Error('invalid provider filter');
    if (ids?.length === 0) return [];
    const rows = db().query(`SELECT * FROM (SELECT *,ROW_NUMBER() OVER (PARTITION BY provider_id ORDER BY completed_at DESC,id DESC) AS ordinal
      FROM evaluations WHERE capability_id=? ${ids ? `AND provider_id IN (${ids.map(() => '?').join(',')})` : ''}) WHERE ordinal=1 LIMIT 1001`).all(capability, ...(ids ?? []));
    if (rows.length > 1000) throw new Error('evaluation read budget exceeded');
    return rows.map(({ metrics_json, evidence_json, ordinal: _ordinal, ...row }) => ({ ...row, metrics: JSON.parse(metrics_json), evidence: JSON.parse(evidence_json) }));
  }
  // Content-addressed bodies are representations, never evidence or authority.
  function putContextBody(text) {
    if (typeof text !== 'string' || Buffer.byteLength(text) > 8 * 1024 * 1024) throw new Error('invalid context body');
    checkStop(); const digest = sha(text), path = owned(home, join(home, 'context', 'bodies', `${digest}.txt`));
    const reused = existsSync(path);
    if (reused) { if (sha(readPinned(path)) !== digest) throw new Error('context body digest mismatch'); }
    else output(path, text);
    return { path, digest, bytes: Buffer.byteLength(text), reused };
  }
  function readContextBody({ path, digest }) {
    const expected = owned(home, join(home, 'context', 'bodies', `${valid(digest, 'body digest', 64)}.txt`));
    if (!/^[a-f0-9]{64}$/.test(digest) || resolve(path) !== expected) throw new Error('invalid context body address');
    const text = readPinned(expected); if (sha(text) !== digest) throw new Error('context body digest mismatch'); return text;
  }
  function persistContext(packet) {
    checkStop(); getMission(packet.mission_id);
    if (!/^[a-f0-9]{32}$/.test(packet.id) || !Number.isSafeInteger(packet.token_estimate) || packet.token_estimate < 0) throw new Error('invalid context packet');
    const bytes = `${boundedJson(packet)}\n`, digest = sha(bytes), path = join(home, 'context', `${packet.id}.json`);
    if (existsSync(path) || one('SELECT id FROM context_packets WHERE id=?', packet.id)) throw new Error('context packet already exists');
    return intent('context.compiled', { mission_id: packet.mission_id, packet_id: packet.id, digest }, () => {
      output(path, bytes);
      db().transaction(() => {
        checkStop(); sql('INSERT INTO context_packets VALUES(?,?,?,?,?,?,?)', packet.id, packet.mission_id, path, digest, packet.token_estimate,
          boundedJson({ sections: packet.sections, representation_digest: packet.representation_digest }), clock());
        event('hypervisor', 'context.compiled', { packet_id: packet.id, digest }, packet.mission_id); checkStop();
      }).immediate();
      return { ...packet, path, digest };
    });
  }
  function getContext(packetId) {
    const row = one('SELECT * FROM context_packets WHERE id=?', valid(packetId, 'context packet id', 256));
    if (!row) throw new Error('context packet not found');
    const source = readPinned(owned(home, row.path));
    if (sha(source) !== row.digest) throw new Error('context packet digest mismatch');
    const packet = JSON.parse(source);
    if (packet.id !== row.id || packet.mission_id !== row.mission_id || packet.token_estimate !== row.token_estimate) throw new Error('context packet row mismatch');
    return { ...packet, path: row.path, digest: row.digest };
  }
  function getMissionRuntime(missionId) {
    const mission = getMission(missionId), row = one('SELECT * FROM missions WHERE id=?', missionId);
    const tasks = db().query('SELECT * FROM mission_tasks WHERE mission_id=? ORDER BY id LIMIT 10001').all(missionId);
    if (tasks.length > 10000) throw new Error('task read budget exceeded');
    const packet = row.context_packet_id ? getContext(row.context_packet_id) : null;
    const taskRows = tasks.map(({ dependencies_json, ...task }) => ({ ...task, dependencies: JSON.parse(dependencies_json) }));
    return { mission, state: row.state, mission_revision: row.updated_at, spec_path: row.spec_path, route: JSON.parse(row.route_json), context_packet_id: row.context_packet_id,
      context: packet, packet, tasks: taskRows, taskRows };
  }
  function readMissionSpec(missionId) {
    getMission(missionId);
    const row = one('SELECT spec_path FROM missions WHERE id=?', missionId), path = owned(missionPath(missionId), row.spec_path);
    const text = readPinned(path); return { path, text, digest: sha(text) };
  }
  function beginMission({ mission_id, context_packet_id, plan_digest = null, route_digest = null }) {
    checkStop(); const packet = getContext(context_packet_id);
    if (packet.mission_id !== mission_id) throw new Error('context belongs to another mission');
    return db().transaction(() => {
      checkStop(); const current = getMissionRuntime(mission_id);
      if (['running', 'uncertain', 'resolved', 'passed'].includes(current.state) || current.tasks.some(task => ['running', 'started', 'uncertain'].includes(task.state)))
        throw new Error('mission has an outstanding attempt; reconcile before retry');
      sql("UPDATE missions SET state='running',context_packet_id=?,updated_at=MAX(?,updated_at+0.000001) WHERE id=?", context_packet_id, clock(), mission_id);
      event('hypervisor', 'mission.started', { context_packet_id, plan_digest, route_digest }, mission_id); checkStop();
      return { mission_id, state: 'running', context_packet_id };
    }).immediate();
  }
  function taskRow(missionId, taskId) {
    const row = one('SELECT * FROM mission_tasks WHERE mission_id=? AND id=?', valid(missionId, 'mission id', 256), valid(taskId, 'task id', 256));
    if (!row) throw new Error('mission task not found'); return row;
  }
  function taskStates(missionId, taskIds) {
    valid(missionId, 'mission id', 256);
    if (!Array.isArray(taskIds) || taskIds.length > 10000) throw new Error('invalid task state set');
    const ids = [...new Set(taskIds.map(value => valid(value, 'task id', 256)))];
    if (!ids.length) return {};
    return Object.fromEntries(db().query(`SELECT id,state FROM mission_tasks WHERE mission_id=? AND id IN (${ids.map(() => '?').join(',')})`)
      .all(missionId, ...ids).map(row => [row.id, row.state]));
  }
  function taskReceipt(rid) {
    const row = one('SELECT * FROM receipts WHERE id=?', rid); if (!row) throw new Error('task receipt missing');
    const { value: doc, calculated } = parsedDocument(readPinned(owned(receiptRoot, row.path)), { spaced: true });
    if (calculated !== row.digest || doc.digest !== row.digest || doc.id !== row.id || doc.state !== row.state || doc.created_at !== row.created_at ||
        doc.previous_digest !== row.previous_digest || ['mission_id', 'task_id', 'provider_id', 'capability_id'].some(key => doc[key] !== row[key]) ||
        canonicalJson(doc.payload) !== canonicalJson(JSON.parse(row.payload_json))) throw new Error('task receipt integrity mismatch');
    return { ...doc, receipt_id: row.id };
  }
  function beginTask({ mission_id, task_id, input_digest, provider_id = null, capability = null,
    mission_revision, plan_digest, route_digest, context_packet_id }) {
    checkStop(); if (!/^[a-f0-9]{64}$/.test(input_digest)) throw new Error('invalid task input digest');
    // FULL commits the intent to the WAL before an external effect may start.
    db().exec('PRAGMA synchronous=FULL');
    return db().transaction(() => {
      checkStop(); const task = taskRow(mission_id, task_id);
      if (plan_digest !== undefined || route_digest !== undefined || context_packet_id !== undefined || mission_revision !== undefined) {
        if (!Number.isFinite(mission_revision) || !/^[a-f0-9]{64}$/.test(plan_digest) || !/^[a-f0-9]{64}$/.test(route_digest)) throw new Error('invalid execution cut');
        const current = one('SELECT state,updated_at,context_packet_id FROM missions WHERE id=?', mission_id);
        if (current.state !== 'running' || current.updated_at !== mission_revision || current.context_packet_id !== context_packet_id)
          throw new Error('execution cut changed; reconcile before dispatch');
        const owner = one(`SELECT p.mode,p.installed,p.health,p.license,p.license_approved,COALESCE(r.state,'unrouted') AS route_state
          FROM providers p JOIN provider_capabilities pc ON pc.provider_id=p.id
          LEFT JOIN routes r ON r.provider_id=p.id AND r.capability_id=pc.capability_id AND r.scope='global'
          WHERE p.id=? AND pc.capability_id=?`, provider_id, capability);
        if (!owner || !owner.installed || !['active', 'candidate'].includes(owner.mode) ||
            !(owner.mode === 'active' || owner.route_state === 'promoted') || !['reachable', 'verified'].includes(owner.health) ||
            !(['MIT', 'Apache-2.0', 'vendor', 'private'].includes(owner.license) || owner.license_approved))
          throw new Error('provider eligibility changed; reconcile before dispatch');
      }
      if (['running', 'started', 'uncertain'].includes(task.state)) throw new Error('task outcome uncertain; reconcile before retry');
      const previous = task.receipt_id ? taskReceipt(task.receipt_id) : null;
      if (previous && (previous.mission_id !== mission_id || previous.task_id !== task_id)) throw new Error('task receipt ownership mismatch');
      if (task.state === 'passed') {
        if (!previous || previous.state !== 'passed') throw new Error('passed task lacks verified completion receipt');
        if (previous.payload.input_digest === input_digest && previous.provider_id === provider_id && previous.capability_id === capability)
          return { ...previous.payload, receipt_id: previous.receipt_id, state: 'passed', reusable: true };
        // Reusing a prior effect against changed input requires an explicit new
        // operation identity/reconciliation, not implicit replay on a new hash.
        throw new Error('completed task input changed; reconcile before retry');
      }
      for (const parent of JSON.parse(task.dependencies_json)) if (taskRow(mission_id, parent).state !== 'passed') throw new Error('task dependencies not passed');
      const attempt_id = identity(), receipt = writeReceipt({ mission_id, task_id, provider_id, capability_id: capability, state: 'started',
        payload: { kind: 'task.attempt', attempt_id, input_digest, reconcile_before_retry: true,
          ...(mission_revision === undefined ? {} : { mission_revision, plan_digest, route_digest, context_packet_id }) } });
      sql("UPDATE mission_tasks SET state='running',provider_id=?,receipt_id=?,attempts=attempts+1,error='',updated_at=? WHERE mission_id=? AND id=?",
        provider_id, receipt.receipt_id, clock(), mission_id, task_id);
      event('hypervisor', 'task.started', { task_id, attempt_id, input_digest }, mission_id); checkStop();
      return { ...receipt, attempt_id, input_digest, state: 'started', reusable: false };
    }).immediate();
  }
  function completeTask({ mission_id, task_id, attempt_id, input_digest, state, result = {}, evidence = [] }) {
    if (!['passed', 'failed', 'blocked', 'uncertain', 'partial'].includes(state)) throw new Error('invalid task outcome');
    const payload = { kind: 'task.outcome', attempt_id, input_digest, result, evidence }; boundedJson(payload); checkStop();
    return db().transaction(() => {
      checkStop(); const task = taskRow(mission_id, task_id), started = task.receipt_id ? taskReceipt(task.receipt_id) : null;
      if (task.state !== 'running' || started?.state !== 'started' || started.mission_id !== mission_id || started.task_id !== task_id ||
          started.payload.attempt_id !== attempt_id || started.payload.input_digest !== input_digest) throw new Error('task attempt no longer current');
      if (state === 'passed' && started.payload.mission_revision !== undefined) {
        const current = one('SELECT state,updated_at,context_packet_id FROM missions WHERE id=?', mission_id);
        if (current.state !== 'running' || current.updated_at !== started.payload.mission_revision ||
            current.context_packet_id !== started.payload.context_packet_id) throw new Error('execution cut changed during effect; record uncertain outcome');
      }
      const receipt = writeReceipt({ mission_id, task_id, provider_id: started.provider_id, capability_id: started.capability_id, state,
        payload: { ...payload, started_receipt_id: started.receipt_id } });
      sql('UPDATE mission_tasks SET state=?,receipt_id=?,error=?,updated_at=? WHERE mission_id=? AND id=?', state, receipt.receipt_id,
        state === 'passed' ? '' : String(result.reason ?? state).slice(0, 4096), clock(), mission_id, task_id);
      event('hypervisor', 'task.completed', { task_id, attempt_id, state, receipt_id: receipt.receipt_id }, mission_id); checkStop();
      return { ...receipt, ...payload, state };
    }).immediate();
  }
  function blockTask({ mission_id, task_id, reason }) {
    checkStop(); valid(reason, 'block reason', 4096);
    return db().transaction(() => {
      checkStop(); const task = taskRow(mission_id, task_id);
      if (['running', 'started', 'uncertain', 'passed'].includes(task.state)) throw new Error('cannot overwrite task effect state');
      sql("UPDATE mission_tasks SET state='blocked',error=?,updated_at=? WHERE mission_id=? AND id=?", reason, clock(), mission_id, task_id);
      event('hypervisor', 'task.blocked', { task_id, reason }, mission_id); checkStop(); return { mission_id, task_id, state: 'blocked', reason };
    }).immediate();
  }
  function finishMission({ mission_id, state, evidence = [], mission_revision }) {
    checkStop(); if (!['passed', 'resolved', 'blocked', 'failed', 'partial', 'uncertain'].includes(state)) throw new Error('invalid mission outcome');
    return db().transaction(() => {
      checkStop(); const runtime = getMissionRuntime(mission_id);
      if (mission_revision !== undefined && runtime.mission_revision !== mission_revision) throw new Error('mission changed before finalization');
      if (['passed', 'resolved'].includes(state) && (!runtime.tasks.length || runtime.tasks.some(task => task.state !== 'passed') ||
          !Array.isArray(evidence) || !evidence.length)) throw new Error('mission completion requires passed tasks and verification evidence');
      const receipt = writeReceipt({ mission_id, state, payload: { kind: 'mission.outcome', evidence } });
      sql('UPDATE missions SET state=?,updated_at=MAX(?,updated_at+0.000001) WHERE id=?', state, clock(), mission_id);
      event('hypervisor', 'mission.completed', { state, receipt_id: receipt.receipt_id }, mission_id); checkStop(); return { mission_id, state, ...receipt };
    }).immediate();
  }
  function missionStatus(missionId) { const runtime = getMissionRuntime(missionId); return { mission_id: missionId, ...runtime }; }
  function getSource(sourceId) {
    valid(sourceId, 'source id', 64);
    const row = one('SELECT * FROM sources WHERE id=?', sourceId);
    if (!row) return null;
    const { manifest_json, ...fields } = row;
    return { ...fields, manifest: JSON.parse(manifest_json) };
  }
  function beginSource({ sourceId, repo, requestedRef = null, checkoutPath }) {
    checkStop(); valid(sourceId, 'source id', 64); valid(repo, 'source repository', 256);
    if (sourceId !== `source-${sha([repo, requestedRef ?? 'HEAD'].join('\0')).slice(0, 24)}` ||
        !registry().projects.some(project => project.repo.toLowerCase() === repo.toLowerCase()) ||
        requestedRef !== null && (typeof requestedRef !== 'string' || !requestedRef || requestedRef.length > 256))
      throw new Error('source is not declared in registry');
    const expected = owned(home, join(home, 'sources', sourceId));
    if (checkoutPath !== expected || getSource(sourceId) || existsSync(expected))
      throw new Error('source already exists; reconcile before retry');
    return db().transaction(() => {
      checkStop();
      const started = writeReceipt({ state: 'started', payload: { kind: 'source.ingest', source_id: sourceId, repo,
        requested_ref: requestedRef, checkout: expected } });
      const at = clock();
      sql("INSERT INTO sources(id,repo,requested_ref,checkout_path,state,created_at,updated_at) VALUES(?,?,?,?,'cloning',?,?)",
        sourceId, repo, requestedRef, expected, at, at);
      event('hypervisor', 'source.started', { source_id: sourceId, repo, started_receipt_id: started.receipt_id });
      checkStop(); return { source_id: sourceId, state: 'cloning', started_receipt_id: started.receipt_id, checkout: expected };
    }).immediate();
  }
  function finishSource({ sourceId, state, resolvedRef = null, license = null, digest = null, manifest = {} }) {
    checkStop(); valid(sourceId, 'source id', 64);
    if (!['inspected', 'failed'].includes(state) || state === 'inspected' &&
        (!/^(?:[a-f0-9]{40}|[a-f0-9]{64})$/.test(resolvedRef ?? '') || !/^[a-f0-9]{64}$/.test(digest ?? '') ||
          typeof license !== 'string' || !license)) throw new Error('invalid source completion');
    const encoded = boundedJson(manifest);
    return db().transaction(() => {
      checkStop(); const source = getSource(sourceId);
      if (!source || source.state !== 'cloning') throw new Error('source attempt no longer current');
      const at = clock(), receipt = writeReceipt({ state, payload: { kind: 'source.ingest', source_id: sourceId,
        repo: source.repo, resolved_ref: resolvedRef, digest, manifest_sha256: sha(encoded) } });
      sql('UPDATE sources SET state=?,resolved_ref=?,license=?,digest=?,manifest_json=?,updated_at=? WHERE id=?',
        state, resolvedRef, license, digest, encoded, at, sourceId);
      if (state === 'inspected') sql('UPDATE providers SET source_ref=?,source_digest=?,updated_at=? WHERE lower(repo)=lower(?)',
        resolvedRef, digest, at, source.repo);
      event('hypervisor', state === 'inspected' ? 'source.ingested' : 'source.failed',
        { source_id: sourceId, repo: source.repo, state, digest, receipt_id: receipt.receipt_id });
      checkStop(); return { ...getSource(sourceId), receipt_id: receipt.receipt_id };
    }).immediate();
  }
  function recordSourceUpdate({ id: updateId, providerId, oldRef, newRef, evidence }) {
    checkStop(); valid(updateId, 'update id', 64); valid(providerId, 'provider id', 256);
    if (updateId !== `update-${sha([providerId, oldRef ?? '', newRef].join('\0')).slice(0, 24)}` ||
        !/^(?:[a-f0-9]{40}|[a-f0-9]{64})$/.test(newRef) ||
        oldRef !== null && !/^(?:[a-f0-9]{40}|[a-f0-9]{64})$/.test(oldRef)) throw new Error('invalid source update');
    const encoded = boundedJson(evidence);
    return db().transaction(() => {
      checkStop(); const current = provider(providerId);
      if (!current || current.source_ref !== oldRef || current.repo.startsWith('local/')) throw new Error('source authority changed');
      const existing = one('SELECT * FROM source_updates WHERE id=?', updateId);
      if (existing) {
        if (existing.provider_id !== providerId || existing.old_ref !== oldRef || existing.new_ref !== newRef)
          throw new Error('source update identity collision');
        return { id: updateId, provider_id: providerId, old_ref: oldRef, new_ref: newRef, state: existing.state, reused: true };
      }
      const at = clock(), receipt = writeReceipt({ state: 'candidate', payload: { kind: 'source.update_candidate',
        update_id: updateId, provider_id: providerId, old_ref: oldRef, new_ref: newRef, evidence_sha256: sha(encoded) } });
      sql("INSERT INTO source_updates(id,provider_id,old_ref,new_ref,state,compatibility_json,created_at,updated_at) VALUES(?,?,?,?,'candidate',?,?,?)",
        updateId, providerId, oldRef, newRef, encoded, at, at);
      event('hypervisor', 'source.update_candidate', { update_id: updateId, provider_id: providerId, receipt_id: receipt.receipt_id });
      checkStop(); return { id: updateId, provider_id: providerId, old_ref: oldRef, new_ref: newRef, state: 'candidate', receipt_id: receipt.receipt_id };
    }).immediate();
  }
  return Object.freeze({ home, dbPath, registryPath, receiptRoot, missionRoot, now: clock, id: identity, registry, loadRegistry,
    event, events, observe, providers, provider, observedProviders: () => observe('providers'), observedProvider: pid => observe('provider', pid),
    observedSummary: () => observe('summary'), observedReceiptChain: () => observe('verifyReceiptChain'),
    observedRoute: (capability, options) => observe('route', capability, options),
    providerCapabilities: capabilityRows, route, routeMany, routeQueryCount: () => routeQueries,
    promote, quarantine, approveLicense, taskStates,
    recordDiscovery, writeReceipt, verifyReceiptChain, summary, recentReceipts, createMission, getMission, saveMission, saveRoute, upsertBug,
    saveBackpropagation, registerSession, getSession, listSessions, persistEvaluation, latestEvaluations,
    getSource, beginSource, finishSource, recordSourceUpdate,
    putContextBody, readContextBody, persistContext, getContext, getMissionRuntime, readMissionSpec, beginMission, beginTask, completeTask, blockTask, finishMission, missionStatus,
    close() { connection?.close(); connection = null; closed = true; } });
}
