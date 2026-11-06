#!/usr/bin/env bun
// Read-only MCP aperture over the existing mesh call ledger; no deploy swap.
import http from 'node:http';
import { timingSafeEqual } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { Database } from 'bun:sqlite';

function authoredRegistry() {
  const result = Bun.spawnSync(['cue', 'export', fileURLToPath(new URL('./mesh-ledger.cue', import.meta.url)), '-e', 'registry'], { stdout: 'pipe', stderr: 'pipe' });
  if (result.exitCode !== 0) throw new Error(result.stderr.toString());
  return Object.freeze(JSON.parse(result.stdout.toString()));
}
export const REGISTRY = authoredRegistry();
const names = new Set(REGISTRY.tools.map(tool => tool.name));
const uris = new Set(REGISTRY.resources.map(resource => resource.uri));
const fields = REGISTRY.tools.find(tool => tool.name === 'mcp_call_history').inputSchema.properties;

const limitOf = value => {
  const { minimum, maximum, default: fallback } = fields.limit;
  const limit = value === undefined ? fallback : Number(value);
  if (!Number.isInteger(limit) || limit < minimum || limit > maximum) throw new Error(`limit must be an integer in ${minimum}..${maximum}`);
  return limit;
};
const stringOf = (value, field) => {
  if (value === undefined) return '';
  if (typeof value !== 'string' || value.length > fields[field].maxLength) throw new Error(`${field} must be a bounded string`);
  return value;
};

export function callHistory(db, args = {}) {
  if (!args || typeof args !== 'object' || Array.isArray(args)) throw new Error('arguments must be an object');
  if (Object.keys(args).some(key => !(key in fields))) throw new Error('unknown call-history argument');
  const tool = stringOf(args.tool, 'tool'), state = stringOf(args.state, 'state'), limit = limitOf(args.limit);
  const clauses = [], values = [];
  if (tool) { clauses.push('tool=?'); values.push(tool); }
  if (state) { clauses.push('state=?'); values.push(state); }
  const sql = `SELECT * FROM calls${clauses.length ? ` WHERE ${clauses.join(' AND ')}` : ''} ORDER BY ts DESC LIMIT ?`;
  const rows = db.query(sql).all(...values, limit).map(row => ({ ...row, args: JSON.parse(row.args_json), result: JSON.parse(row.result_json) }));
  for (const row of rows) { delete row.args_json; delete row.result_json; }
  return { calls: rows };
}

export function dispatch(db, method, params = {}) {
  switch (method) {
    case 'initialize': return { protocolVersion: '2025-06-18', capabilities: { tools: { listChanged: false }, resources: { subscribe: false, listChanged: false } }, serverInfo: { name: 'bonfyre-mesh-ledger-aperture', version: '1.0.0' }, instructions: 'Read-only mesh call history. A call receipt is not evidence that its result was true or that an effect was authorized.' };
    case 'ping': return {};
    case 'tools/list': return { tools: REGISTRY.tools };
    case 'tools/call': {
      if (!names.has(params.name)) throw new Error(`unknown tool: ${params.name}`);
      const value = callHistory(db, params.arguments ?? {});
      return { content: [{ type: 'text', text: JSON.stringify(value, null, 2) }], structuredContent: value, isError: false };
    }
    case 'resources/list': return { resources: REGISTRY.resources };
    case 'resources/read': {
      if (!uris.has(params.uri)) throw new Error(`unknown resource: ${params.uri}`);
      const value = callHistory(db, { limit: 300 });
      return { contents: [{ uri: params.uri, mimeType: 'application/json', text: JSON.stringify(value, null, 2) }] };
    }
    default: throw new Error(`unsupported method: ${method}`);
  }
}

const equalToken = (left, right) => {
  const a = Buffer.from(left), b = Buffer.from(right);
  return a.length === b.length && timingSafeEqual(a, b);
};
const respond = (res, status, body, headers = {}) => {
  const data = Buffer.from(JSON.stringify(body));
  res.writeHead(status, { 'Content-Type': 'application/json', 'Content-Length': data.length, 'Cache-Control': 'no-store', 'X-Content-Type-Options': 'nosniff', ...headers });
  res.end(data);
};
async function bodyOf(req) {
  const chunks = []; let size = 0;
  for await (const chunk of req) {
    size += chunk.length;
    if (size > 1_000_000) throw new Error('body too large');
    chunks.push(chunk);
  }
  return JSON.parse(Buffer.concat(chunks).toString());
}

export function createMeshLedgerServer({ dbPath, token, origins = ['https://chatgpt.com', 'https://chat.openai.com', 'http://127.0.0.1', 'http://localhost'] }) {
  if (!token || !dbPath) throw new Error('mesh ledger requires token and existing database path');
  const db = new Database(dbPath, { readonly: true, strict: true });
  const allowed = new Set(origins);
  const server = http.createServer(async (req, res) => {
    if (req.method === 'GET' && req.url === '/health') return respond(res, 200, { status: 'ok', service: 'bonfyre-mesh-ledger-aperture', tools: REGISTRY.tools.length });
    if (req.headers.origin && !allowed.has(req.headers.origin)) return respond(res, 403, { error: 'origin not allowed' });
    if (!equalToken(String(req.headers.authorization ?? ''), `Bearer ${token}`)) return respond(res, 401, { error: 'unauthorized' });
    if (req.url !== '/mcp') return respond(res, 404, { error: 'not found' });
    if (req.method !== 'POST') return respond(res, 405, { error: 'use POST' });
    let request;
    try { request = await bodyOf(req); } catch (error) { return respond(res, 400, { error: error.message }); }
    const { id, method, params = {} } = request ?? {};
    if (method?.startsWith('notifications/')) { res.writeHead(202); res.end(); return; }
    const headers = { 'MCP-Protocol-Version': '2025-06-18', 'Mcp-Method': String(method ?? '') };
    try { return respond(res, 200, { jsonrpc: '2.0', id, result: dispatch(db, method, params) }, headers); }
    catch (error) { return respond(res, 200, { jsonrpc: '2.0', id, error: { code: -32000, message: error.message } }, headers); }
  });
  server.on('close', () => db.close());
  return server;
}

if (import.meta.main) {
  const token = process.env.BONFYRE_MCP_TOKEN || (process.env.BONFYRE_MCP_TOKEN_FILE ? readFileSync(process.env.BONFYRE_MCP_TOKEN_FILE, 'utf8').trim() : '');
  const server = createMeshLedgerServer({ dbPath: process.env.BONFYRE_MCP_DB || '/var/lib/bonfyre/mcp/mesh.sqlite3', token, origins: (process.env.BONFYRE_MCP_ALLOWED_ORIGINS || 'https://chatgpt.com,https://chat.openai.com,http://127.0.0.1,http://localhost').split(',').map(value => value.trim()).filter(Boolean) });
  const port = Number(process.env.BONFYRE_MESH_LEDGER_PORT || 3054), host = process.env.BONFYRE_MESH_LEDGER_HOST || '127.0.0.1';
  server.listen(port, host, () => console.log(`Mesh ledger aperture listening on ${host}:${port}`));
}
