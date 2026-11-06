#!/usr/bin/env bun
// Observes the existing broker; never queues tasks, cancels work, or calls agents.
import assert from 'node:assert/strict';
import { Database } from 'bun:sqlite';
import { readFileSync, existsSync, copyFileSync, mkdtempSync, rmSync, lstatSync } from 'node:fs';
import { homedir } from 'node:os';
import { join, relative, resolve, sep } from 'node:path';
import { createHash, randomUUID } from 'node:crypto';
import { readRequest } from '../http_boundary.mjs';

const catalog = JSON.parse(readFileSync(new URL('../fabric_catalog.json', import.meta.url), 'utf8'));
const hash = bytes => createHash('sha256').update(bytes).digest('hex');
const TABLES = ['tasks', 'runs', 'events', 'locks', 'sessions', 'receipts'];

export function snapshotFabricState(home) {
  const source = join(home, 'fabric.sqlite3');
  // Do not silently ignore an active WAL. This proof uses a quiescent copy,
  // checked on both sides; an active writer requires a different snapshot path.
  const original = hash(readFileSync(source)), temporary = mkdtempSync('/private/tmp/bonfyre-fabric-witness-');
  let db;
  try {
    const path = join(temporary, 'fabric.sqlite3');
    if (existsSync(`${source}-wal`)) {
      const backup = Bun.spawnSync(['/usr/bin/sqlite3', '-readonly', source, `.backup '${path}'`], { stdout: 'pipe', stderr: 'pipe', timeout: 30_000 });
      assert.equal(backup.exitCode, 0, 'online read-only SQLite backup failed');
    } else {
      copyFileSync(source, path);
      assert(!existsSync(`${source}-wal`)); assert.equal(hash(readFileSync(source)), original); assert.equal(hash(readFileSync(path)), original);
    }
    db = new Database(path);
    const tables = {};
    for (const name of TABLES) {
      const digest = createHash('sha256'); let rows = 0;
      for (const row of db.query(`SELECT rowid AS _witness_rowid,* FROM ${name} ORDER BY rowid`).iterate()) {
        assert(++rows <= 1_000_000, 'row budget exceeded');
        digest.update(JSON.stringify(row)).update('\n');
      }
      tables[name] = { rows, sha256: digest.digest('hex') };
    }
    const receiptFailures = [];
    for (const row of db.query('SELECT id,path,digest FROM receipts').all()) {
      try {
        const rel = relative(resolve(home, 'receipts'), resolve(row.path));
        assert(rel && rel !== '..' && !rel.startsWith(`..${sep}`));
        const stat = lstatSync(row.path); assert(stat.isFile() && !stat.isSymbolicLink() && stat.size <= 32 * 1024 * 1024);
        assert.equal(hash(readFileSync(row.path)), row.digest);
      } catch { receiptFailures.push(row.id); }
    }
    return { source_db_sha256: original, tables, receipts: { verified: !receiptFailures.length, failures: receiptFailures },
      task_states: db.query('SELECT state,count(*) AS count FROM tasks GROUP BY state ORDER BY state').all(),
      run_states: db.query('SELECT state,count(*) AS count FROM runs GROUP BY state ORDER BY state').all(),
      recorded_nonterminal_tasks: db.query("SELECT id,kind,state,started_at FROM tasks WHERE state IN ('queued','running') ORDER BY id").all(),
      recorded_nonterminal_runs: db.query("SELECT id,task_id,agent,role,state,started_at FROM runs WHERE state='running' ORDER BY id").all() };
  } finally { db?.close(); rmSync(temporary, { recursive: true, force: true }); }
}

export async function collectFabricWitness({ url = 'http://127.0.0.1:3053', token, home = join(homedir(), '.bonfyre/dev-fabric'), samples = 0, request = fetch } = {}) {
  assert(token); assert(Number.isSafeInteger(samples) && samples >= 0 && samples <= 100);
  const healthResponse = await request(`${url}/health`, { redirect: 'error', signal: AbortSignal.timeout(15_000) });
  assert.equal(healthResponse.status, 200); const health = await readRequest(healthResponse);
  assert.equal(health.service, 'bonfyre-dual-agent-fabric'); assert.equal(health.tools, 14);
  const anonymous = await request(`${url}/events`, { redirect: 'error', signal: AbortSignal.timeout(15_000) });
  assert.equal(anonymous.status, 401); await anonymous.body?.cancel();
  async function rpc(method, params = {}) {
    const id = randomUUID(), res = await request(`${url}/mcp`, { method: 'POST', redirect: 'error', signal: AbortSignal.timeout(60_000),
      headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' }, body: JSON.stringify({ jsonrpc: '2.0', id, method, params }) });
    assert.equal(res.status, 200); const value = await readRequest(res, { maxBytes: 20_000_000 });
    assert.equal(value.id, id); assert.equal(value.jsonrpc, '2.0'); assert(!value.error); return value.result;
  }
  async function call(name, args) {
    const result = await rpc('tools/call', { name, arguments: args });
    assert.equal(result.isError, false); return result.structuredContent;
  }
  assert.equal((await rpc('initialize')).serverInfo.name, health.service);
  assert.deepEqual((await rpc('tools/list')).tools, catalog.tools);
  assert.deepEqual((await rpc('prompts/list')).prompts, catalog.prompts);
  const receipts = await call('recent_receipts', { limit: 500 }), receiptDigest = hash(JSON.stringify(receipts));
  const eventsResponse = await request(`${url}/events`, { headers: { authorization: `Bearer ${token}` }, redirect: 'error', signal: AbortSignal.timeout(30_000) });
  assert.equal(eventsResponse.status, 200); const events = await readRequest(eventsResponse, { maxBytes: 20_000_000 });
  assert(Array.isArray(events.events));
  const taskIds = [...new Set(receipts.receipts.map(row => row.task_id))], taskDigests = {};
  for (const taskId of taskIds) taskDigests[taskId] = hash(JSON.stringify(await call('task_status', { task_id: taskId })));
  const timings = [];
  for (let index = 0; index < samples; index++) {
    const start = performance.now(), result = await call('recent_receipts', { limit: 50 }); timings.push(performance.now() - start);
    assert.equal(hash(JSON.stringify(result)), hash(JSON.stringify({ receipts: receipts.receipts.slice(0, 50) })));
  }
  const ordered = timings.toSorted((a, b) => a - b);
  return { schema: 'bonfyre.fabric.live-witness.v1', measured_at: new Date().toISOString(), runtime: health.runtime || 'python', endpoint: url,
    tool_count: 14, prompt_count: 4, catalog_sha256: hash(JSON.stringify(catalog)), unauthorized_events_refused: true,
    receipt_metadata_sha256: receiptDigest, task_detail_sha256: taskDigests, events_sha256: hash(JSON.stringify(events)),
    snapshot: snapshotFabricState(home),
    ...(samples ? { recent_receipts_http: { samples, milliseconds: timings, p50_ms: ordered[Math.floor(samples * .5)],
      p95_ms: ordered[Math.min(samples - 1, Math.ceil(samples * .95) - 1)], scope: 'Warm serial loopback receipt reads only; not agent throughput.' } } : {}) };
}

if (import.meta.main) {
  const token = readFileSync(process.env.BONFYRE_MCP_TOKEN_FILE || join(homedir(), '.bonfyre/mcp/token'), 'utf8').trim();
  console.log(JSON.stringify(await collectFabricWitness({ token, samples: process.argv.includes('--benchmark') ? 25 : 0 }), null, 2));
}
