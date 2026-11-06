#!/usr/bin/env bun
// Read-only cutover witness. No discovery, compilation, provider probe, or
// effectful endpoint is called; the SQLite online backup writes only to temp.
import assert from 'node:assert/strict';
import { Database } from 'bun:sqlite';
import { createHash, randomUUID } from 'node:crypto';
import { existsSync, lstatSync, mkdtempSync, readdirSync, readFileSync, realpathSync, rmSync } from 'node:fs';
import { homedir, tmpdir } from 'node:os';
import { dirname, isAbsolute, join, relative, resolve, sep } from 'node:path';
import { canonicalJson } from '../canonical_json.mjs';
import { readRequest } from '../http_boundary.mjs';
import { HYPERVISOR_TOOLS } from '../hypervisor_catalog.mjs';
import { readSpinesBytes, spinesOwnedPath } from '../spines_store.mjs';

const TABLES = Object.freeze(['providers', 'capabilities', 'provider_capabilities', 'sources', 'missions',
  'mission_tasks', 'sessions', 'context_packets', 'evaluations', 'routes', 'receipts', 'events', 'bugs', 'source_updates']);
const hash = bytes => createHash('sha256').update(bytes).digest('hex');
const digest = value => hash(canonicalJson(value));
const READ_HINT = new Map(HYPERVISOR_TOOLS.map(tool => [tool.name, tool.annotations?.readOnlyHint === true]));
const owned = (home, path, alias = home) => {
  assert.equal(typeof path, 'string', 'owned file path is not text');
  assert(isAbsolute(path), 'owned file path is not absolute');
  let full = resolve(path);
  const aliasRel = relative(alias, full);
  if (alias !== home && aliasRel !== '..' && !aliasRel.startsWith(`..${sep}`) && !isAbsolute(aliasRel))
    full = join(home, aliasRel);
  const rel = relative(home, full);
  assert(rel !== '..' && !rel.startsWith(`..${sep}`) && !isAbsolute(rel), 'file path escapes hypervisor home');
  return spinesOwnedPath(home, full);
};
function sourceFile(path) {
  try { const bytes = readSpinesBytes(path, 256 * 1024 * 1024); return { state: 'present', sha256: hash(bytes), bytes: bytes.length }; }
  catch (error) { if (error.code === 'ENOENT') return { state: 'missing' }; throw error; }
}
function inventoryContext(home, files) {
  const root = join(home, 'context');
  if (!existsSync(root)) { files[root] = { state: 'missing_directory' }; return; }
  const pending = [[root, 0]]; let entries = 0, bytes = 0;
  while (pending.length) {
    const [path, depth] = pending.pop();
    assert(depth <= 32 && ++entries <= 10000, 'context file inventory budget exceeded');
    const stat = lstatSync(owned(home, path));
    assert(!stat.isSymbolicLink(), 'context symlink refused');
    if (stat.isDirectory()) for (const name of readdirSync(path)) pending.push([join(path, name), depth + 1]);
    else {
      assert(stat.isFile(), 'context entry is not a regular file');
      bytes += stat.size; assert(bytes <= 512 * 1024 * 1024, 'context byte inventory budget exceeded');
      files[path] = sourceFile(path);
    }
  }
}
function sourceFingerprint(dbPath) {
  const shared = `${dbPath}-shm`;
  return { main: sourceFile(dbPath), '-wal': sourceFile(`${dbPath}-wal`),
    // SQLite readers change transient lock bytes in -shm. Only database and
    // WAL payload hashes are durable source identity; SHM presence is checked.
    '-shm': existsSync(shared) ? { state: 'present', bytes: lstatSync(shared).size } : { state: 'missing' } };
}
export function snapshotHypervisorState(inputHome) {
  const home = realpathSync(inputHome), alias = resolve(inputHome), source = join(home, 'hypervisor.sqlite3');
  if (!existsSync(source)) return { database: { state: 'missing' }, tables: Object.fromEntries(TABLES.map(name => [name, { state: 'missing_database' }])),
    files: {}, sample: { mission_id: null, capability: null }, source_unchanged: true };
  const before = sourceFingerprint(source), temporary = mkdtempSync(join(tmpdir(), 'bonfyre-hypervisor-live-witness-'));
  assert(before['-wal'].state !== 'present' || before['-shm'].state === 'present',
    'live WAL has no SHM; refuse a read that could create source bookkeeping');
  let db;
  try {
    const copied = join(temporary, 'hypervisor.sqlite3');
    const backup = Bun.spawnSync(['/usr/bin/sqlite3', '-readonly', source, `.backup '${copied}'`],
      { stdout: 'pipe', stderr: 'pipe', timeout: 30000 });
    assert.equal(backup.exitCode, 0, 'read-only SQLite online backup failed');
    // The backup retains WAL mode; only the disposable copy may create -shm.
    db = new Database(copied, { strict: true });
    const names = new Set(db.query("SELECT name FROM sqlite_master WHERE type='table'").all().map(row => row.name));
    const tables = {}, files = {};
    for (const name of TABLES) {
      if (!names.has(name)) { tables[name] = { state: 'missing_table' }; continue; }
      const checksum = createHash('sha256'); let count = 0;
      for (const row of db.query(`SELECT rowid AS _witness_rowid,* FROM ${name} ORDER BY rowid`).iterate()) {
        assert(++count <= 100000, 'SQLite row inventory budget exceeded');
        checksum.update(JSON.stringify(row)).update('\n');
        if (name === 'receipts' && row.path) { const path = owned(home, row.path, alias); files[path] ??= sourceFile(path); }
        if (name === 'missions' && row.spec_path) {
          const spec = owned(home, row.spec_path, alias);
          for (const path of [spec, join(dirname(spec), 'mission.json'), join(dirname(spec), 'task-graph.json')])
            files[owned(home, path)] ??= sourceFile(path);
        }
        if (name === 'context_packets' && row.path) { const path = owned(home, row.path, alias); files[path] ??= sourceFile(path); }
      }
      tables[name] = { state: 'present', rows: count, sha256: checksum.digest('hex') };
    }
    inventoryContext(home, files);
    const sampleMission = names.has('missions') ? db.query('SELECT id FROM missions ORDER BY created_at,id LIMIT 1').get()?.id ?? null : null;
    const sampleCapability = names.has('provider_capabilities') ?
      db.query('SELECT capability_id FROM provider_capabilities ORDER BY capability_id LIMIT 1').get()?.capability_id ?? null : null;
    const after = sourceFingerprint(source);
    assert.deepEqual(after, before, 'source database or WAL changed during snapshot; retry before cutover');
    return { database: { state: 'present', source: before, snapshot_sha256: hash(readSpinesBytes(copied, 256 * 1024 * 1024)) },
      tables, files, sample: { mission_id: sampleMission, capability: sampleCapability }, source_unchanged: true };
  } finally { db?.close(); rmSync(temporary, { recursive: true, force: true }); }
}
function normalizedCatalog(tools, prompts) {
  assert(Array.isArray(tools) && Array.isArray(prompts), 'invalid MCP catalog');
  const expectedNames = [...READ_HINT.keys()].sort(), actualNames = tools.map(tool => tool.name).sort();
  assert.deepEqual(actualNames, expectedNames, 'hypervisor operation catalog changed');
  const corrections = [], normalized = tools.map(tool => {
    assert(tool.inputSchema && typeof tool.inputSchema === 'object', `missing input schema: ${tool.name}`);
    const original = tool.annotations?.readOnlyHint;
    const expected = READ_HINT.get(tool.name);
    if (original !== expected) corrections.push({ tool: tool.name, from: original ?? null, to: expected });
    return { ...tool, annotations: { ...tool.annotations, readOnlyHint: expected } };
  });
  return { tool_count: tools.length, prompt_count: prompts.length, raw_sha256: digest({ tools, prompts }),
    normalized_sha256: digest({ tools: normalized, prompts }), input_schema_sha256: digest(tools.map(tool => [tool.name, tool.inputSchema])),
    read_annotation_corrections: corrections };
}
function projection(name, result) {
  if (name === 'ecosystem_providers') {
    assert(Array.isArray(result.providers), 'providers read shape changed');
    return { providers: result.providers };
  }
  if (name === 'session_list') {
    assert(Array.isArray(result.sessions), 'sessions read shape changed');
    return { sessions: result.sessions };
  }
  if (name === 'provider_compare') {
    assert(Array.isArray(result.ranking), 'comparison read shape changed');
    return { capability: result.capability, ranking: result.ranking, winner: result.winner };
  }
  if (name === 'mission_status') {
    assert(Array.isArray(result.tasks) && Array.isArray(result.receipts), 'mission status lost tasks or receipts');
    return { id: result.id ?? result.mission_id, goal: result.goal ?? result.mission?.goal,
      state: result.state, spec_path: result.spec_path, mission: result.mission, route: result.route,
      tasks: result.tasks.toSorted((a, b) => String(a.id).localeCompare(String(b.id))),
      receipts: result.receipts.toSorted((a, b) => String(a.id).localeCompare(String(b.id))), active: result.active };
  }
  if (name === 'replay_verify') {
    assert(typeof result.verified === 'boolean', 'replay verification shape changed');
    return { verified: result.verified, receipts: result.receipts, head: result.head, failures: result.failures };
  }
  throw new Error(`unreviewed read projection: ${name}`);
}
export async function collectHypervisorWitness({ token, home = join(homedir(), '.bonfyre/agent-hypervisor'),
  url = 'http://127.0.0.1:3054', samples = 0, request = fetch } = {}) {
  assert(typeof token === 'string' && token.length > 0, 'hypervisor bearer token required');
  assert(Number.isSafeInteger(samples) && samples >= 0 && samples <= 100, 'invalid read sample count');
  const endpoint = new URL(url);
  assert(endpoint.protocol === 'http:' && ['127.0.0.1', 'localhost', '[::1]'].includes(endpoint.hostname) &&
    endpoint.username === '' && endpoint.password === '' && endpoint.search === '' && endpoint.hash === '',
  'cutover witness accepts only an exact local HTTP endpoint');
  const snapshot = snapshotHypervisorState(home), base = endpoint.origin;
  async function rpc(method, params = {}) {
    const id = randomUUID(), response = await request(`${base}/mcp`, { method: 'POST', redirect: 'error',
      signal: AbortSignal.timeout(60000), headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' },
      body: JSON.stringify({ jsonrpc: '2.0', id, method, params }) });
    assert.equal(response.status, 200, `${method} HTTP response changed`);
    const document = await readRequest(response, { maxBytes: 20_000_000 });
    assert.equal(document.jsonrpc, '2.0'); assert.equal(document.id, id); assert(!document.error, `${method} RPC refused`);
    return document.result;
  }
  async function call(name, args = {}) {
    const value = await rpc('tools/call', { name, arguments: args });
    assert.equal(value?.isError, false, `${name} returned MCP error`);
    assert(value.structuredContent !== undefined, `${name} has no structured result`);
    return value.structuredContent;
  }
  const initialized = await rpc('initialize'), tools = (await rpc('tools/list')).tools,
    prompts = (await rpc('prompts/list')).prompts;
  assert.equal(initialized.serverInfo?.name, 'bonfyre-agent-hypervisor');
  const catalog = normalizedCatalog(tools, prompts), reads = {};
  for (const [name, args] of [['ecosystem_providers', {}], ['session_list', {}],
    ['provider_compare', snapshot.sample.capability ? { capability: snapshot.sample.capability } : null],
    ['mission_status', snapshot.sample.mission_id ? { mission_id: snapshot.sample.mission_id } : null],
    ['replay_verify', {}]]) {
    if (args === null) { reads[name] = { state: 'absent_sample' }; continue; }
    const result = projection(name, await call(name, args));
    reads[name] = { state: 'present', sha256: digest(result), projection: result };
  }
  const timings = [];
  if (samples && snapshot.sample.capability) for (let index = 0; index < samples; index++) {
    const start = performance.now();
    const result = projection('provider_compare', await call('provider_compare', { capability: snapshot.sample.capability }));
    assert.equal(digest(result), reads.provider_compare.sha256, 'read sample changed');
    timings.push(performance.now() - start);
  }
  const ordered = timings.toSorted((a, b) => a - b);
  const afterReads = snapshotHypervisorState(home);
  assert.deepEqual(afterReads.tables, snapshot.tables, 'SQLite rows changed during read-only RPC witness');
  assert.deepEqual(afterReads.files, snapshot.files, 'owned files changed during read-only RPC witness');
  assert.deepEqual(afterReads.database.source, snapshot.database.source, 'SQLite source changed during read-only RPC witness');
  return { schema: 'bonfyre.hypervisor.live-witness.v1', observed_at: new Date().toISOString(), endpoint: base,
    runtime: initialized.serverInfo.version === '5.0.0' ? 'python' : 'bun', version: initialized.serverInfo.version,
    catalog, reads, snapshot,
    ...(timings.length ? { comparison_http: { samples: timings.length, milliseconds: timings,
      p50_ms: ordered[Math.floor(timings.length * .5)], p95_ms: ordered[Math.ceil(timings.length * .95) - 1],
      scope: 'Warm serial local provider comparison reads; no capability evaluation or distributed throughput claim.' } } : {}) };
}
if (import.meta.main) {
  const token = readFileSync(process.env.BONFYRE_MCP_TOKEN_FILE || join(homedir(), '.bonfyre/mcp/token'), 'utf8').trim();
  process.stdout.write(`${JSON.stringify(await collectHypervisorWitness({ token, samples: process.argv.includes('--benchmark') ? 25 : 0 }), null, 2)}\n`);
}
