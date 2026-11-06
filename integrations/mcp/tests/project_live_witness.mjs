#!/usr/bin/env bun
// Read-only loopback witness; neither calls operator mutations nor model generation.
// --baseline compares an ephemeral concrete Bun owner with the configured live owner.
// Without it, verifies the configured live endpoint after a separately approved cutover.
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { homedir } from 'node:os';
import { join } from 'node:path';
import { createHash } from 'node:crypto';
import { CATALOG, createProjectNode, projectConfig } from '../project_node.mjs';

const plist = join(homedir(), 'Library/LaunchAgents/com.bonfyre.project-mcp.plist');
const decoded = Bun.spawnSync(['/usr/bin/plutil', '-convert', 'json', '-o', '-', plist]);
assert.equal(decoded.exitCode, 0, 'read service configuration');
const configuration = JSON.parse(decoded.stdout.toString());
const env = { ...process.env, ...configuration.EnvironmentVariables };
const config = projectConfig(env);
assert.equal(config.host, '127.0.0.1', 'witness restricted to loopback');
const live = `http://${config.host}:${config.port}`;
const baseline = process.argv.includes('--baseline');
const server = baseline ? Bun.serve({ hostname: '127.0.0.1', port: 0,
  fetch: (await createProjectNode(config)).handle, idleTimeout: 0 }) : null;
const candidate = server ? `http://127.0.0.1:${server.port}` : live;
let id = 0;
const digest = value => createHash('sha256').update(JSON.stringify(value)).digest('hex');
async function rpc(endpoint, method, params = {}) {
  const response = await fetch(`${endpoint}/mcp`, { method: 'POST',
    headers: { authorization: `Bearer ${config.token}`, 'content-type': 'application/json' },
    body: JSON.stringify({ jsonrpc: '2.0', id: ++id, method, params }), signal: AbortSignal.timeout(30_000) });
  assert.equal(response.status, 200);
  const result = await response.json();
  assert.equal(result.error, undefined, 'JSON-RPC transport error');
  assert.notEqual(result.result?.isError, true, `${params.name || method} refused`);
  return result.result;
}
async function call(endpoint, name, args = {}) {
  const result = await rpc(endpoint, 'tools/call', { name, arguments: args });
  return result.structuredContent ?? JSON.parse(result.content[0].text);
}
async function listTools(endpoint) {
  const tools = [];
  let cursor = null;
  do {
    const page = await rpc(endpoint, 'tools/list', cursor ? { cursor } : {});
    tools.push(...page.tools);
    cursor = page.nextCursor ?? null;
  } while (cursor);
  return tools;
}
function statistics(values) {
  const sorted = [...values].sort((a, b) => a - b);
  return { samples: sorted.length, median_ms: sorted[Math.floor(sorted.length / 2)],
    p95_ms: sorted[Math.ceil(sorted.length * 0.95) - 1], min_ms: sorted[0], max_ms: sorted.at(-1) };
}
try {
  const health = await (await fetch(`${candidate}/health`)).json();
  assert.equal(health.runtime, 'bun');
  assert.equal((await fetch(`${candidate}/mcp`, { method: 'POST', body: 'invalid' })).status, 401);
  const initialized = await rpc(candidate, 'initialize');
  assert.equal(initialized.serverInfo.name, 'bonfyre-project-node');
  const catalogTools = await listTools(candidate);
  assert.deepEqual(catalogTools.map(t => t.name).sort(), CATALOG.tools.map(t => t.name).sort());
  let previousToolCount = null, addedTools = [];
  if (baseline) {
    const previousTools = await listTools(live);
    const previousNames = previousTools.map(t => t.name).sort(), candidateNames = catalogTools.map(t => t.name).sort();
    const candidateSet = new Set(candidateNames);
    assert(previousNames.every(name => candidateSet.has(name)), 'candidate removed a live tool');
    previousToolCount = previousNames.length;
    addedTools = candidateNames.filter(name => !previousNames.includes(name));
  }
  const resources = await rpc(candidate, 'resources/list');
  const templates = await rpc(candidate, 'resources/templates/list');
  const prompts = await rpc(candidate, 'prompts/list');
  assert.equal(resources.resources.length, CATALOG.resources.length);
  assert.equal(templates.resourceTemplates.length, CATALOG.resourceTemplates.length);
  assert.equal(prompts.prompts.length, CATALOG.prompts.length);
  if (!baseline) assert.equal(health.runtime_identity?.state, 'current', 'live Project MCP source identity is not current');
  await rpc(candidate, 'resources/read', { uri: 'bonfyre://mcp/contract' });
  const inventory = await call(candidate, 'native_tool_inventory');
  const atlas = await call(candidate, 'architecture_atlas', { op: 'summary' });
  const operations = [
    ['project_read', { path: 'AGENTS.md', start_line: 1, end_line: 30 }],
    ['project_tree', { path: 'integrations/mcp/tests', depth: 1, limit: 500 }],
    ['project_status', {}],
  ];
  const measurements = [];
  for (const [name, args] of operations) {
    const current = await call(candidate, name, args);
    let parity = 'not_compared';
    if (baseline) {
      const previous = await call(live, name, args);
      // Status has timestamps/runtime-sensitive counts; compare identity, not timing.
      const normalize = value => name === 'project_status'
        ? Object.fromEntries(['root', 'branch', 'head', 'upstream', 'ahead', 'behind', 'clean'].map(k => [k, value[k]]))
        : name === 'project_tree' ? { ...value, items: [...value.items].sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b))) } : value;
      assert.deepEqual(normalize(current), normalize(previous), `${name} parity`);
      parity = name === 'project_status' ? 'identity_and_clean_state_equal' : 'equal';
    }
    const before = [], after = [];
    for (let i = 0; i < 25; i++) {
      const timed = async (endpoint, samples) => {
        const start = performance.now(); await call(endpoint, name, args); samples.push(performance.now() - start);
      };
      // Alternate order to reduce cache/order bias; these are warmed end-to-end RPCs.
      if (baseline && i % 2 === 0) await timed(live, before);
      await timed(candidate, after);
      if (baseline && i % 2 !== 0) await timed(live, before);
    }
    measurements.push({ name, arguments: args, parity, candidate: statistics(after),
      ...(baseline ? { previous: statistics(before), median_speedup: statistics(before).median_ms / statistics(after).median_ms } : {}) });
  }
  console.log(JSON.stringify({ schema: 'bonfyre.project-mcp.http-witness.v1', measured_at: new Date().toISOString(),
    mode: baseline ? 'ephemeral_candidate_vs_live_owner' : 'configured_live_owner',
    runtime: health.runtime, root: config.root, tool_count: catalogTools.length,
    previous_tool_count: previousToolCount, added_tools: addedTools,
    resources: resources.resources.length, templates: templates.resourceTemplates.length, prompts: prompts.prompts.length,
    native_inventory_sha256: digest(inventory), atlas_summary_sha256: digest(atlas), measurements,
    source_sha256: createHash('sha256').update(readFileSync(new URL('../project_node.mjs', import.meta.url))).digest('hex'),
    limitation: 'Local warmed read RPCs only; not a load, durability, native generation, remote Matrix or system-wide scaling benchmark. No operator mutation or generation invoked.' }, null, 2));
} finally { server?.stop(true); }
