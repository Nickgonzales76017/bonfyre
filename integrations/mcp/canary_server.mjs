#!/usr/bin/env bun
// Version-bound fixture evidence and deterministic canaries; no promotion by discovery.
import { readFileSync, realpathSync } from 'node:fs';
import { homedir } from 'node:os';
import { join, resolve } from 'node:path';
import { randomUUID } from 'node:crypto';
import { json, object, readRequest, createMcpTransport } from './http_boundary.mjs';

export const CANARY_TOOLS = JSON.parse(readFileSync(new URL('./canary_catalog.json', import.meta.url), 'utf8'));
const VERSION = '5.4.0-fixture-canary-r4';
const contract = 'Use passed version-bound capability certificates for bounded safe canaries only. Partial, blocked, failed, stale, cleanup-failed, digest-invalid or version-mismatched certificates remain ineligible. No automatic promotion.';
const MAX_BYTES = 10_000_000;
function endpoint(value) {
  const url = new URL(value);
  if (!['http:', 'https:'].includes(url.protocol) || url.username || url.password || url.hash || url.search
      || url.protocol === 'http:' && !['127.0.0.1', 'localhost', '[::1]'].includes(url.hostname))
    throw new Error('recursive MCP endpoint requires loopback or HTTPS without URL credentials');
  return url.href;
}
export function canaryConfig(env = process.env, userHome = homedir()) {
  const token = readFileSync(env.BONFYRE_MCP_TOKEN_FILE || join(userHome, '.bonfyre/mcp/token'), 'utf8').trim();
  const port = Number(env.BONFYRE_CANARY_PORT || 3056);
  if (!token) throw new Error('Bonfyre MCP token missing');
  if (!Number.isSafeInteger(port) || port < 1 || port > 65535) throw new Error('invalid canary port');
  return { token, port, env, host: env.BONFYRE_CANARY_HOST || '127.0.0.1',
    home: resolve(env.BONFYRE_CANARY_HOME || join(userHome, '.bonfyre/agent-hypervisor/canary-r4')),
    root: realpathSync(env.BONFYRE_ROOT || join(userHome, 'Developer/Bonfyre')),
    recursiveEndpoint: endpoint(env.BONFYRE_RECURSIVE_MCP || 'http://127.0.0.1:3055/mcp') };
}

export function createCanaryRecursiveCall({ endpoint: address, token, request = fetch }) {
  address = endpoint(address);
  if (!token) throw new Error('recursive MCP token missing');
  return async (name, args, { timeoutMs = 300_000 } = {}) => {
    if (!['artifact_ingest', 'token_artifact_compile', 'dependency_register'].includes(name)) throw new Error('unknown certificate projection owner');
    if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 600_000) throw new Error('invalid recursive owner timeout');
    const id = randomUUID();
    try {
      const res = await request(address, { method: 'POST', redirect: 'error', signal: AbortSignal.timeout(timeoutMs),
        headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json', 'MCP-Protocol-Version': '2025-06-18' },
        body: json({ jsonrpc: '2.0', id, method: 'tools/call', params: { name, arguments: args } }) });
      if (!res.ok) { await res.body?.cancel(); throw new Error(`recursive owner HTTP ${res.status}`); }
      const value = await readRequest(res, { maxBytes: MAX_BYTES, timeoutMs });
      if (!object(value) || value.jsonrpc !== '2.0' || value.id !== id || value.error || !object(value.result) || value.result.isError)
        throw new Error('recursive owner refused or returned invalid response identity');
      return value.result.structuredContent ?? value.result;
    } catch (error) { throw new Error(String(error.message || error).replaceAll(token, '[redacted]').slice(0, 2000)); }
  };
}

export async function createCanaryServer({ token, home, root, env = process.env, store, fixtures,
  recursiveEndpoint = 'http://127.0.0.1:3055/mcp', request = fetch, maxConcurrent = 16 } = {}) {
  if (!token) throw new Error('canary requires an authentication token');
  if (!Number.isSafeInteger(maxConcurrent) || maxConcurrent < 1 || maxConcurrent > 64) throw new Error('invalid concurrency bound');
  store ||= (await import('./canary_store.mjs')).createCanaryStore({ home });
  fixtures ||= (await import('./canary_fixtures.mjs')).createCanaryFixtures({ store, projectRoot: root, env,
    recursiveCall: createCanaryRecursiveCall({ endpoint: recursiveEndpoint, token, request }) });
  const dispatch = {
    canary_status: () => store.status(),
    fixture_correction_run: args => fixtures.runSelected(args.providers || [], { max_agent_turns: args.max_agent_turns ?? 4 }),
    canary_certificate_list: async () => ({ certificates: await store.listCertificates() }),
    canary_certificate_verify: args => store.validateCertificate(args.provider, args.capability),
    canary_policy_upsert: args => store.upsertPolicy({ ...args, enable: args.enable ?? false }),
    canary_policy_list: async () => ({ policies: await store.listPolicies() }),
    canary_policy_state: args => store.setPolicyState(args.policy_id, args.state),
    canary_route_decide: args => store.routeDecide({ ...args, task_id: args.task_id || null, fallback_provider: args.fallback_provider || null }),
    canary_outcome_record: args => store.recordOutcome({ ...args, latency_ms: args.latency_ms ?? null, receipt_digest: args.receipt_digest || null, payload: args.payload || {} }),
    canary_decision_list: async args => ({ decisions: await store.listDecisions(args.limit ?? 100) }),
    canary_outcome_list: async args => ({ outcomes: await store.listOutcomes(args.limit ?? 100) }),
    canary_receipt_verify: () => store.verifyReceipts(),
  };
  const transport = createMcpTransport({ token, label: 'canary', service: 'bonfyre-fixture-canary-r4', version: VERSION,
    instructions: contract, tools: CANARY_TOOLS, dispatch, statusTool: 'canary_status', maxBytes: MAX_BYTES, maxConcurrent });
  return { ...transport, close: () => store.close?.() };
}

if (import.meta.main) {
  const config = canaryConfig();
  const node = await createCanaryServer(config);
  const server = Bun.serve({ hostname: config.host, port: config.port, fetch: node.handle, maxRequestBodySize: MAX_BYTES, idleTimeout: 0 });
  console.log(`canary MCP listening on ${server.hostname}:${server.port} runtime=bun`);
}
