#!/usr/bin/env bun
import { readFileSync } from 'node:fs';
import { RICH_CATALOG, LOCAL_TOOLS } from '../recursive_overlay.mjs';

const [tokenPath, host, port] = process.argv.slice(2);
if (!tokenPath || !host || !port) throw new Error('usage: recursive_overlay_smoke.mjs TOKEN_FILE HOST PORT');
const token = readFileSync(tokenPath, 'utf8').trim();
const endpoint = `http://${host}:${Number(port)}/mcp`;
async function call(method, params = {}) {
  const response = await fetch(endpoint, { method: 'POST', headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' }, body: JSON.stringify({ jsonrpc: '2.0', id: method, method, params }), signal: AbortSignal.timeout(30_000) });
  if (!response.ok) throw new Error(`${method} HTTP ${response.status}`);
  const value = await response.json();
  if (value.error) throw new Error(`${method}: ${JSON.stringify(value.error)}`);
  return value.result || {};
}
const init = await call('initialize');
if (init.serverInfo?.name !== 'bonfyre-mcp-mesh') throw new Error(`wrong native owner: ${init.serverInfo?.name}`);
const tools = new Set((await call('tools/list')).tools.map(item => item.name));
const needed = [...RICH_CATALOG.tools.map(item => item.name), ...LOCAL_TOOLS.map(item => item.name)];
for (const name of needed) if (!tools.has(name)) throw new Error(`missing tool: ${name}`);
const resources = new Set((await call('resources/list')).resources.map(item => item.uri));
for (const uri of [...RICH_CATALOG.resources.map(item => item.uri), 'bonfyre://closure/frontier', 'bonfyre://closure/operating-contract']) if (!resources.has(uri)) throw new Error(`missing resource: ${uri}`);
const prompts = new Set((await call('prompts/list')).prompts.map(item => item.name));
for (const prompt of RICH_CATALOG.prompts) {
  if (!prompts.has(prompt.name)) throw new Error(`missing prompt: ${prompt.name}`);
  const rendered = await call('prompts/get', { name: prompt.name, arguments: { focus: 'MCP', request: 'smoke' } });
  if (!rendered.messages?.[0]?.content?.text) throw new Error(`empty prompt: ${prompt.name}`);
}
const contract = await call('resources/read', { uri: 'bonfyre://closure/operating-contract' });
if (!JSON.stringify(contract).toLowerCase().includes('authority')) throw new Error('operating contract did not resolve');
for (const uri of ['bonfyre://project/status', 'bonfyre://mcp/calls']) {
  const result = await call('resources/read', { uri });
  if (!result.contents?.[0]?.text) throw new Error(`resource did not resolve: ${uri}`);
}
const denied = await call('tools/call', { name: 'estate_query', arguments: { sql: 'DROP TABLE observation' } });
if (!denied.structuredContent?.error || denied.isError) throw new Error('native estate admission gate missing');
const ledger = await call('tools/call', { name: 'mcp_call_history', arguments: { limit: 1 } });
if (!Array.isArray(ledger.structuredContent?.calls)) throw new Error('native call ledger missing');
for (const path of ['/metrics', '/events?limit=1']) {
  const response = await fetch(new URL(path, endpoint), { headers: { Authorization: `Bearer ${token}` }, signal: AbortSignal.timeout(10_000) });
  if (!response.ok) throw new Error(`${path} HTTP ${response.status}`);
}
console.log(JSON.stringify({ status: 'passed', native_owner: init.serverInfo.name, tools: tools.size, resources: resources.size, prompts: prompts.size }));
