#!/usr/bin/env bun
// Read-only live contract/chain proof; never ingest or finalize live missions.
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { homedir } from 'node:os';
import { join } from 'node:path';
import { createHash, randomUUID } from 'node:crypto';
import { RECURSIVE_TOOLS, RECURSIVE_PROMPTS } from '../recursive_server.mjs';
import { readRequest } from '../http_boundary.mjs';

export async function collectRecursiveWitness({ url = 'http://127.0.0.1:3055', token, samples = 0, request = fetch } = {}) {
  assert(token, 'missing recursive token');
  assert(Number.isSafeInteger(samples) && samples >= 0 && samples <= 100);
  const healthResponse = await request(`${url}/health`, { redirect: 'error', signal: AbortSignal.timeout(30_000) });
  assert.equal(healthResponse.status, 200);
  const health = await readRequest(healthResponse);
  assert.equal(health.service, 'bonfyre-recursive-substrate-v5'); assert.equal(health.tools, 22); assert.equal(health.token_binary, true);
  const anonymous = await request(`${url}/status`, { redirect: 'error', signal: AbortSignal.timeout(15_000) });
  assert.equal(anonymous.status, 401); await anonymous.body?.cancel();
  async function rpc(method, params = {}) {
    const id = randomUUID();
    const res = await request(`${url}/mcp`, { method: 'POST', redirect: 'error', signal: AbortSignal.timeout(30_000),
      headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' },
      body: JSON.stringify({ jsonrpc: '2.0', id, method, params }) });
    assert.equal(res.status, 200);
    const value = await readRequest(res, { maxBytes: 30_000_000 });
    assert.equal(value.id, id); assert.equal(value.jsonrpc, '2.0'); assert(!value.error, 'recursive RPC error');
    return value.result;
  }
  async function call(name, args = {}) {
    const value = await rpc('tools/call', { name, arguments: args });
    assert.equal(value.isError, false); assert(value.structuredContent);
    return value.structuredContent;
  }
  assert.equal((await rpc('initialize')).serverInfo.name, health.service);
  const catalog = await rpc('tools/list'), prompts = await rpc('prompts/list');
  assert.deepEqual(catalog.tools, RECURSIVE_TOOLS); assert.deepEqual(prompts.prompts, RECURSIVE_PROMPTS);
  const status = await call('recursive_status'), chain = await call('substrate_replay_verify');
  assert.equal(chain.verified, true); assert.equal(chain.receipt_chain.verified, true); assert.equal(chain.flux_chains.verified, true);
  assert.deepEqual(chain.receipt_chain.failures, []); assert.deepEqual(chain.flux_chains.failures, []);
  assert.deepEqual(status.receipt_chain, chain.receipt_chain); assert.deepEqual(status.flux_chains, chain.flux_chains);
  const tokenizerResult = await call('tokenizer_list'); assert.deepEqual(tokenizerResult.tokenizers, status.tokenizers);
  const timings = [];
  for (let index = 0; index < samples; index++) {
    const start = performance.now(), current = await call('substrate_replay_verify');
    timings.push(performance.now() - start); assert.deepEqual(current, chain, 'chain changed during read benchmark');
  }
  const ordered = timings.toSorted((a, b) => a - b);
  return { schema: 'bonfyre.recursive.live-witness.v1', measured_at: new Date().toISOString(), endpoint: url,
    version: health.version, runtime: health.runtime || 'python', tool_count: catalog.tools.length, prompt_count: prompts.prompts.length,
    catalog_sha256: createHash('sha256').update(JSON.stringify(catalog)).digest('hex'), unauthorized_status_refused: true,
    token_binary: status.token_binary, token_binary_status: status.token_binary_status, counts: status.counts,
    tokenizers: status.tokenizers.map(({ created_at: _created, updated_at: _updated, ...descriptor }) => descriptor), chains: chain,
    ...(samples ? { replay_verification_http: { samples, milliseconds: timings, p50_ms: ordered[Math.floor(samples * .5)],
      p95_ms: ordered[Math.min(samples - 1, Math.ceil(samples * .95) - 1)], scope: 'Warm serial loopback receipt/flux verification only, not tokenization throughput or estate scaling.' } } : {}) };
}

if (import.meta.main) {
  const token = readFileSync(process.env.BONFYRE_MCP_TOKEN_FILE || join(homedir(), '.bonfyre/mcp/token'), 'utf8').trim();
  console.log(JSON.stringify(await collectRecursiveWitness({ token, samples: process.argv.includes('--benchmark') ? 25 : 0 }), null, 2));
}
