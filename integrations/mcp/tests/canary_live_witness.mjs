#!/usr/bin/env bun
// Read-only live proof. Never run fixtures, choose a route, or mutate a policy.
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { homedir } from 'node:os';
import { join } from 'node:path';
import { createHash, randomUUID } from 'node:crypto';
import { CANARY_TOOLS } from '../canary_server.mjs';
import { readRequest } from '../http_boundary.mjs';

export async function collectCanaryWitness({ url = 'http://127.0.0.1:3056', token, samples = 0, request = fetch } = {}) {
  assert(token, 'missing canary token');
  assert(Number.isSafeInteger(samples) && samples >= 0 && samples <= 100);
  const get = async (path, headers = {}) => request(`${url}${path}`, { headers, redirect: 'error', signal: AbortSignal.timeout(15_000) });
  const health = await (await get('/health')).json();
  assert.equal(health.service, 'bonfyre-fixture-canary-r4'); assert.equal(health.tools, 12);
  const anonymous = await get('/status'); assert.equal(anonymous.status, 401); await anonymous.body?.cancel();
  async function rpc(method, params = {}) {
    const id = randomUUID();
    const res = await request(`${url}/mcp`, { method: 'POST', redirect: 'error', signal: AbortSignal.timeout(30_000),
      headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' },
      body: JSON.stringify({ jsonrpc: '2.0', id, method, params }) });
    assert.equal(res.status, 200);
    const value = await readRequest(res, { maxBytes: 10_000_000 });
    assert.equal(value.id, id); assert.equal(value.jsonrpc, '2.0'); assert(!value.error, 'canary RPC error');
    return value.result;
  }
  const initialized = await rpc('initialize');
  assert.equal(initialized.serverInfo.name, 'bonfyre-fixture-canary-r4');
  const catalog = await rpc('tools/list'); assert.deepEqual(catalog.tools, CANARY_TOOLS);
  const receipt = async () => {
    const value = await rpc('tools/call', { name: 'canary_receipt_verify', arguments: {} });
    assert.equal(value.isError, false);
    const proof = value.structuredContent;
    assert.equal(proof.verified, true, 'existing receipt chain does not verify');
    assert.deepEqual(proof.failures, []);
    return proof;
  };
  const chain = await receipt(), timings = [];
  for (let index = 0; index < samples; index++) {
    const start = performance.now(), current = await receipt();
    timings.push(performance.now() - start);
    assert.deepEqual(current, chain, 'receipt chain changed during read benchmark');
  }
  const ordered = timings.toSorted((a, b) => a - b);
  return { schema: 'bonfyre.canary.live-witness.v1', measured_at: new Date().toISOString(),
    endpoint: url, version: health.version, runtime: health.runtime || 'python', tool_count: catalog.tools.length,
    catalog_sha256: createHash('sha256').update(JSON.stringify(catalog.tools)).digest('hex'),
    unauthorized_status_refused: true, receipt_chain: chain,
    ...(samples ? { receipt_verification_http: { samples, milliseconds: timings,
      p50_ms: ordered[Math.floor(samples * .5)], p95_ms: ordered[Math.min(samples - 1, Math.ceil(samples * .95) - 1)],
      scope: 'Warm serial loopback HTTP verification of the existing hash-linked receipt chain; not fixture execution or estate scaling.' } } : {}) };
}

if (import.meta.main) {
  const token = readFileSync(process.env.BONFYRE_MCP_TOKEN_FILE || join(homedir(), '.bonfyre/mcp/token'), 'utf8').trim();
  const proof = await collectCanaryWitness({ token, samples: process.argv.includes('--benchmark') ? 25 : 0 });
  console.log(JSON.stringify(proof, null, 2));
}
