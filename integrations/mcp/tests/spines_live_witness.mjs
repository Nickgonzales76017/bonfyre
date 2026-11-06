#!/usr/bin/env bun
// Observation only: parity/closure endpoints persist evidence despite readOnlyHint.
import assert from 'node:assert/strict';
import { Database } from 'bun:sqlite';
import { createHash, randomUUID } from 'node:crypto';
import { copyFileSync, existsSync, mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { homedir, tmpdir } from 'node:os';
import { join } from 'node:path';
import { SPINES_TABLES, readSpinesBytes, spinesOwnedPath } from '../spines_store.mjs';
import { canonicalJson, parsedDocument } from '../canonical_json.mjs';
import { readRequest } from '../http_boundary.mjs';
const hash = bytes => createHash('sha256').update(bytes).digest('hex');
const catalog = JSON.parse(readFileSync(new URL('../spines_catalog.json', import.meta.url), 'utf8'));
export function snapshotSpinesState(home) {
  const source = join(home, 'spines.sqlite3'), original = hash(readSpinesBytes(source, 256 * 1024 * 1024));
  const temporary = mkdtempSync(join(tmpdir(), 'bonfyre-spines-witness-')); let db;
  try {
    const path = join(temporary, 'spines.sqlite3');
    if (existsSync(`${source}-wal`)) {
      const backup = Bun.spawnSync(['/usr/bin/sqlite3', '-readonly', source, `.backup '${path}'`], { stdout: 'pipe', stderr: 'pipe', timeout: 30000 });
      assert.equal(backup.exitCode, 0, 'online SQLite snapshot failed');
    } else {
      copyFileSync(source, path); assert(!existsSync(`${source}-wal`));
      assert.equal(hash(readSpinesBytes(source, 256 * 1024 * 1024)), original);
      assert.equal(hash(readSpinesBytes(path, 256 * 1024 * 1024)), original);
    }
    // The owned snapshot may create WAL bookkeeping; the source remains untouched.
    db = new Database(path);
    const tables = {}, files = {};
    for (const name of SPINES_TABLES) {
      const digest = createHash('sha256'); let count = 0;
      for (const row of db.query(`SELECT rowid AS _witness_rowid,* FROM ${name} ORDER BY rowid`).iterate()) {
        assert(++count <= 100000, 'snapshot row budget'); digest.update(JSON.stringify(row)).update('\n');
        if (name !== 'model_inventory') for (const [key, file] of Object.entries(row)) if ((key === 'path' || key.endsWith('_path')) && file) {
          spinesOwnedPath(home, file);
          files[file] = existsSync(file) ? hash(readSpinesBytes(file, 32 * 1024 * 1024)) : null;
        }
      }
      tables[name] = { rows: count, sha256: digest.digest('hex') };
    }
    let previous = null, receipts = 0;
    for (const row of db.query('SELECT * FROM receipts ORDER BY seq').iterate()) {
      const source = readSpinesBytes(spinesOwnedPath(home, row.path)).toString('utf8'), { value, calculated } = parsedDocument(source);
      assert.equal(value.digest, calculated); assert.equal(row.digest, calculated); assert.equal(row.previous_digest, previous);
      for (const key of ['id', 'release', 'kind', 'subject_kind', 'subject_id', 'state', 'previous_digest', 'created_at']) assert.deepEqual(value[key], row[key], `receipt ${key} mismatch`);
      assert.equal(canonicalJson(value.payload), canonicalJson(JSON.parse(row.payload_json)));
      previous = row.digest; receipts++;
    }
    return { source_db_sha256: original, tables, files, receipt_chain: { verified: true, receipts, head: previous } };
  } finally { db?.close(); rmSync(temporary, { recursive: true, force: true }); }
}
export async function collectSpinesWitness({ token, home = join(homedir(), '.bonfyre/agent-hypervisor/native-spines-r7'),
  url = 'http://127.0.0.1:3057', samples = 0, request = fetch } = {}) {
  assert(token); assert(Number.isSafeInteger(samples) && samples >= 0 && samples <= 100);
  const healthResponse = await request(`${url}/health`, { redirect: 'error', signal: AbortSignal.timeout(30000) });
  assert.equal(healthResponse.status, 200); const health = await readRequest(healthResponse);
  assert.equal(health.service, 'bonfyre-native-spines-r7'); assert.equal(health.tools, 29); assert.equal(health.receipts_verified, true);
  const anonymous = await request(`${url}/status`, { redirect: 'error', signal: AbortSignal.timeout(15000) });
  assert.equal(anonymous.status, 401); await anonymous.body?.cancel();
  async function rpc(method, params = {}) {
    const id = randomUUID(), response = await request(`${url}/mcp`, { method: 'POST', redirect: 'error', signal: AbortSignal.timeout(60000),
      headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' }, body: JSON.stringify({ jsonrpc: '2.0', id, method, params }) });
    assert.equal(response.status, 200); const value = await readRequest(response, { maxBytes: 30000000 });
    assert.equal(value.id, id); assert.equal(value.jsonrpc, '2.0'); assert(!value.error); return value.result;
  }
  async function call(name) {
    const result = await rpc('tools/call', { name, arguments: {} }); assert.equal(result.isError, false); return result.structuredContent;
  }
  assert.equal((await rpc('initialize')).serverInfo.name, health.service);
  assert.deepEqual((await rpc('tools/list')).tools, catalog.tools);
  const status = await call('spines_status'), rules = await call('r6_rules_list'), models = await call('r7_model_inventory'), nodes = await call('r7_nodes_list');
  const native = Object.fromEntries(Object.entries(status.native_binaries).map(([key, result]) => {
    assert.equal(result.exit_code, 0); assert.equal(result.timed_out, false);
    return [key, { exit_code: result.exit_code, stdout: result.stdout, stderr: result.stderr }];
  }));
  const timings = [];
  for (let index = 0; index < samples; index++) {
    const start = performance.now(), result = await call('r6_rules_list'); timings.push(performance.now() - start); assert.deepEqual(result, rules);
  }
  const ordered = timings.toSorted((a, b) => a - b);
  return { schema: 'bonfyre.spines.live-witness.v1', measured_at: new Date().toISOString(), endpoint: url, runtime: health.runtime || 'python',
    tool_count: 29, catalog_sha256: hash(JSON.stringify(catalog)), unauthorized_status_refused: true,
    read_models_sha256: hash(canonicalJson({ rules, models, nodes })), counts: status.counts, authority: status.authority,
    native_binaries: native, receipt_chain: status.receipt_chain, snapshot: snapshotSpinesState(home),
    ...(samples ? { rules_http: { samples, milliseconds: timings, p50_ms: ordered[Math.floor(samples * .5)], p95_ms: ordered[Math.ceil(samples * .95) - 1],
      scope: 'Warm serial loopback rule inventory reads only; not saturation, inference, concurrency, or system-wide throughput.' } } : {}) };
}
if (import.meta.main) {
  const token = readFileSync(process.env.BONFYRE_MCP_TOKEN_FILE || join(homedir(), '.bonfyre/mcp/token'), 'utf8').trim();
  console.log(JSON.stringify(await collectSpinesWitness({ token, samples: process.argv.includes('--benchmark') ? 25 : 0 }), null, 2));
}
