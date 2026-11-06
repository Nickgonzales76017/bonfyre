#!/usr/bin/env bun
// Native recursive MCP overlay. The rich mesh remains the owner of its existing
// tools; this edge adds explicit owner-backed operations without copying them.
import http from 'node:http';
import { createHash, randomUUID, timingSafeEqual } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { Database } from 'bun:sqlite';
import { TOOLS as MATRIX_TOOLS } from './matrix_aperture.mjs';
import { createRecursiveTools } from './recursive_overlay_tools.mjs';
import { createNativeRichTools } from '../../mcp/rich-native-tools.mjs';
import { knightIcons, withMcpIcons } from './brand.mjs';
import { mcpToolCase } from '../../components/BonfyreDecision/frontier-adapters.mjs';
import { assessDecision, decisionProposal } from '../../components/BonfyreDecision/decision-plane.mjs';

function authoredRegistry() {
  const compiled = process.env.BONFYRE_RECURSIVE_REGISTRY_JSON;
  if (compiled) return JSON.parse(readFileSync(compiled, 'utf8'));
  const cue = Bun.spawnSync(['cue', 'export', fileURLToPath(new URL('./recursive_overlay.cue', import.meta.url)), '-e', 'registry'], { stdout: 'pipe', stderr: 'pipe' });
  if (cue.exitCode !== 0) throw new Error(cue.stderr.toString());
  return JSON.parse(cue.stdout.toString());
}
export const REGISTRY = Object.freeze(authoredRegistry());
export const RICH_CATALOG = Object.freeze(JSON.parse(readFileSync(fileURLToPath(new URL('../../mcp/rich-catalog.json', import.meta.url)), 'utf8')));
const MCP_ICONS = knightIcons(process.env.BONFYRE_MCP_PUBLIC_ORIGIN || 'https://aurekai.work');
if (Object.keys(REGISTRY.promptBodies).sort().join('\0') !== RICH_CATALOG.prompts.map(prompt => prompt.name).sort().join('\0')) throw new Error('native prompt bodies differ from rich catalogue');

const schema = (properties = {}, required = []) => ({ type: 'object', properties, required, additionalProperties: false });
export const LOCAL_TOOLS = Object.freeze([
  ...MATRIX_TOOLS,
  { name: 'recursive_closure_frontier', description: 'Compile a five-plane owner-backed preparation frontier. No joined authority.', inputSchema: schema({ host: { type: 'string' }, format: { type: 'string', enum: ['ro-crate', 'zarr'] } }), annotations: { readOnlyHint: true, destructiveHint: false } },
  { name: 'foreign_twin_observe', description: 'Observe one public HTTPS source into the deployment foreign root through BonfyreProject with a receipt and robots policy.', inputSchema: schema({ twin_id: { type: 'string' }, url: { type: 'string' }, timeout_seconds: { type: 'integer' } }, ['twin_id', 'url']), annotations: { readOnlyHint: false, destructiveHint: false } },
  { name: 'foreign_resolution', description: 'Ask BonfyreProject for current foreign observation, history or conflicts.', inputSchema: schema({ action: { type: 'string', enum: ['resolve', 'history', 'conflicts'] }, source_url: { type: 'string' } }, ['action']), annotations: { readOnlyHint: true, destructiveHint: false } },
  { name: 'surface_project', description: 'Read declared public surface projections from the running WebAddress owner.', inputSchema: schema({ action: { type: 'string', enum: ['catalogue', 'read'] }, pattern: { type: 'string' }, selector: { type: 'string' } }, ['action']), annotations: { readOnlyHint: true, destructiveHint: false } },
  { name: 'cms_record', description: 'Read or write this deployment CMS store; a save is not publication.', inputSchema: schema({ action: { type: 'string' }, content_type: { type: 'string' }, namespace: { type: 'string' }, entry_id: { type: 'string' }, attributes: { type: 'object' }, limit: { type: 'integer' } }, ['action']), annotations: { readOnlyHint: false, destructiveHint: true } },
  { name: 'capability_circuit_compile', description: 'Compile a priced-capability demand into a lawful owner-backed route; does not authorize or execute.', inputSchema: schema({ demand: { type: 'string' }, budget_usd: { type: 'number' } }, ['demand']), annotations: { readOnlyHint: true, destructiveHint: false } },
  { name: 'decision_tool_shortlist', description: 'Assess an exact shortlist of read-only MCP tools with typed model questions. Returns a non-effecting proposal, never invokes a suggested tool. Planning does not load a model. execute=true uses the installed pinned local Clef runtime by default; a hosted provider must be selected explicitly and accepts public state only. Missing local artifacts refuse without remote fallback.', inputSchema: schema({ intent: { type: 'string' }, shortlist: { type: 'array', items: { type: 'string' } }, information_policy_class: { type: 'string' }, execute: { type: 'boolean' }, provider: { type: 'string', enum: ['local', 'clef-flash', 'clef', 'jev'], default: 'local' } }, ['intent', 'shortlist', 'information_policy_class']), annotations: { readOnlyHint: false, destructiveHint: false } },
].map(tool => {
  const authored = REGISTRY.localTools[tool.name];
  return authored ? { ...tool, annotations: { ...tool.annotations, readOnlyHint: authored.readOnly } } : tool;
}));
if (Object.keys(REGISTRY.localTools).some(name => !LOCAL_TOOLS.some(tool => tool.name === name))) throw new Error('CUE registry names an unbound local tool');
const localNames = new Set(LOCAL_TOOLS.map(tool => tool.name));
const resourceUris = new Set(REGISTRY.resources.map(resource => resource.uri));
const projectNames = new Set(REGISTRY.forwardedProjectTools);
const nativeRichNames = new Set(REGISTRY.nativeRichTools);
const richNames = new Set(RICH_CATALOG.tools.map(tool => tool.name));
if ([...richNames].some(name => !nativeRichNames.has(name) && !projectNames.has(name))) throw new Error('rich catalogue has an unbound tool');
if (RICH_CATALOG.resources.map(item => item.uri).sort().join('\0') !== Object.keys(REGISTRY.richResourceTools).sort().join('\0')) throw new Error('rich resources differ from CUE owner bindings');
const equal = (left, right) => { const a = Buffer.from(left), b = Buffer.from(right); return a.length === b.length && timingSafeEqual(a, b); };
const respond = (res, status, value, headers = {}) => {
  const body = Buffer.from(JSON.stringify(value));
  res.writeHead(status, { 'Content-Type': 'application/json', 'Content-Length': body.length, 'Cache-Control': 'no-store', 'X-Content-Type-Options': 'nosniff', ...headers });
  res.end(body);
};
async function body(req) {
  let length = 0; const chunks = [];
  for await (const chunk of req) { length += chunk.length; if (length > 20_000_000) throw new Error('invalid body size'); chunks.push(chunk); }
  if (!length) throw new Error('invalid body size');
  return JSON.parse(Buffer.concat(chunks).toString());
}
const wrap = value => ({ content: [{ type: 'text', text: JSON.stringify(value, null, 2) }], structuredContent: value, isError: false });
const redact = value => {
  if (Array.isArray(value)) return value.map(item => typeof item === 'string' && /^[0-9a-f]{64}$/i.test(item) ? '[redacted]' : redact(item));
  if (!value || typeof value !== 'object') return value;
  return Object.fromEntries(Object.entries(value).map(([name, item]) => [name,
    /(?:token|secret|password|credential|authorization|api_key)/i.test(name) ? '[redacted]'
      : /intent/i.test(name) && typeof item === 'string' ? { sha256: createHash('sha256').update(item).digest('hex'), bytes: Buffer.byteLength(item) }
      : /patch/i.test(name) && typeof item === 'string' ? { sha256: createHash('sha256').update(item).digest('hex'), bytes: Buffer.byteLength(item) }
        : redact(item)]));
};
const unwrap = value => {
  if (value?.structuredContent !== undefined) return value.structuredContent;
  const content = value?.content;
  if (Array.isArray(content) && content.length === 1 && content[0]?.type === 'text') {
    try { return JSON.parse(content[0].text); } catch { return content[0].text; }
  }
  return value;
};

export function openLedger(dbPath) {
  const db = new Database(dbPath, { create: true });
  db.exec('PRAGMA journal_mode=WAL');
  db.exec('PRAGMA synchronous=NORMAL');
  db.exec('CREATE TABLE IF NOT EXISTS calls (id TEXT PRIMARY KEY, ts REAL NOT NULL, trace_id TEXT, tool TEXT NOT NULL, args_json TEXT NOT NULL, state TEXT NOT NULL, duration_ms INTEGER NOT NULL, result_json TEXT NOT NULL)');
  db.exec('CREATE INDEX IF NOT EXISTS calls_ts_idx ON calls(ts DESC)');
  db.exec('CREATE INDEX IF NOT EXISTS calls_tool_idx ON calls(tool,ts DESC)');
  db.exec('CREATE TABLE IF NOT EXISTS deltas (seq INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL NOT NULL, source TEXT NOT NULL, head TEXT, status_hash TEXT, payload_json TEXT NOT NULL)');
  db.exec('CREATE INDEX IF NOT EXISTS deltas_ts_idx ON deltas(ts DESC)');
  db.exec('CREATE TABLE IF NOT EXISTS events (seq INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL NOT NULL, kind TEXT NOT NULL, trace_id TEXT, payload_json TEXT NOT NULL)');
  db.exec('CREATE INDEX IF NOT EXISTS events_ts_idx ON events(ts DESC)');
  db.exec('CREATE TABLE IF NOT EXISTS tasks (id TEXT PRIMARY KEY, ts REAL NOT NULL, kind TEXT NOT NULL, name TEXT NOT NULL, unit_name TEXT NOT NULL, state TEXT NOT NULL, log_path TEXT NOT NULL)');
  const call = db.query('INSERT INTO calls(id,ts,trace_id,tool,args_json,state,duration_ms,result_json) VALUES(?,?,?,?,?,?,?,?)');
  const event = db.query('INSERT INTO events(ts,kind,trace_id,payload_json) VALUES(?,?,?,?)');
  const delta = db.query('INSERT INTO deltas(ts,source,head,status_hash,payload_json) VALUES(?,?,?,?,?)');
  const task = db.query('INSERT INTO tasks(id,ts,kind,name,unit_name,state,log_path) VALUES(?,?,?,?,?,?,?)');
  return {
    record(trace, tool, args, state, duration, value) {
      db.transaction(() => {
        const id = randomUUID().replaceAll('-', '');
        const ts = Date.now() / 1000;
        call.run(id, ts, trace, tool, JSON.stringify(redact(args)), state, duration, JSON.stringify(redact(value)));
        event.run(ts, 'mcp.call', trace, JSON.stringify({ call_id: id, tool, state }));
      })();
    },
    appendDelta(payload) {
      const source = String(payload.source ?? 'local-project'), head = payload.head ?? null, statusHash = payload.status_hash ?? null;
      return db.transaction(() => {
        const ts = Date.now() / 1000;
        const result = delta.run(ts, source, head, statusHash, JSON.stringify(payload));
        const seq = Number(result.lastInsertRowid);
        event.run(ts, 'project.delta', null, JSON.stringify({ seq, source, head, status_hash: statusHash }));
        return seq;
      })();
    },
    createTask(row, trace) {
      db.transaction(() => {
        const ts = Date.now() / 1000;
        task.run(row.id, ts, row.kind, row.name, row.unit_name, row.state, row.log_path);
        event.run(ts, 'task.started', trace, JSON.stringify({ task_id: row.id, kind: row.kind, name: row.name, state: row.state }));
      })();
    },
    task(id) { return db.query('SELECT * FROM tasks WHERE id=?').get(id) ?? null; },
    events(after = 0, limit = 500) {
      const rows = db.query('SELECT * FROM events WHERE seq>? ORDER BY seq LIMIT ?').all(after, limit).map(({ payload_json, ...row }) => ({ ...row, payload: JSON.parse(payload_json) }));
      return { events: rows, next_after: rows.at(-1)?.seq ?? after };
    },
    metrics() {
      const count = table => db.query(`SELECT COUNT(*) AS n FROM ${table}`).get().n;
      return { calls: count('calls'), call_failures: db.query("SELECT COUNT(*) AS n FROM calls WHERE state!='passed'").get().n, deltas: count('deltas'), events: count('events'), tasks: count('tasks') };
    },
    close() { db.close(); },
  };
}

export function createRecursiveOverlay({
  token,
  projectUrl,
  runtimeUrl = process.env.BONFYRE_RUNTIME_URL || 'http://127.0.0.1:3045',
  projectToken = token,
  origins = ['https://chatgpt.com', 'https://chat.openai.com', 'http://127.0.0.1', 'http://localhost'],
  ledger,
  request = fetch,
  operations,
  richOperations,
  decisionOptions = {},
} = {}) {
  if (!token || !projectUrl) throw new Error('native MCP gateway needs token and projectUrl');
  const allowed = new Set(origins);
  async function rpc(target, credential, method, params = {}, signalMs = 60_000) {
    const response = await request(target, { method: 'POST', headers: { 'Content-Type': 'application/json', Accept: 'application/json', Authorization: `Bearer ${credential}`, 'MCP-Protocol-Version': '2025-06-18' }, body: JSON.stringify({ jsonrpc: '2.0', id: randomUUID(), method, params }), signal: AbortSignal.timeout(signalMs) });
    if (!response.ok) throw new Error(`MCP owner HTTP ${response.status}`);
    const answer = await response.json();
    if (answer.error) throw new Error(JSON.stringify(answer.error));
    return answer.result;
  }
  const projectRpc = (method, params) => rpc(projectUrl, projectToken, method, params);
  const projectCall = async (name, args) => {
    if (projectNames.has(name) && !listedNames().has(name)) throw new Error(`project owner unavailable for ${name}`);
    return unwrap(await projectRpc('tools/call', { name, arguments: args }));
  };
  const richCall = richOperations || createNativeRichTools({ request, projectCall, ledger, catalogueToolCount: RICH_CATALOG.tools.length, previewPaths: REGISTRY.previewPaths, protocolVariants: REGISTRY.protocolVariants, excludedCarriers: REGISTRY.excludedCarrierNames, oraclePolicy: REGISTRY.oracle, flemingProjections: REGISTRY.flemingProjections });
  const ops = operations || createRecursiveTools({ request, baseCall: richCall, projectCall });
  let projectSchemas = [];
  async function refreshProjectSchemas() {
    try {
      const listed = await projectRpc('tools/list');
      projectSchemas = (listed.tools || []).filter(item => projectNames.has(item.name));
    } catch { projectSchemas = []; }
    return projectSchemas;
  }
  const ready = refreshProjectSchemas();
  const listedNames = () => new Set(projectSchemas.map(tool => tool.name));
  async function decideToolShortlist(args) {
    const catalogue = new Map(RICH_CATALOG.tools.map(tool => [tool.name, tool]));
    for (const tool of LOCAL_TOOLS) if (tool.name !== 'decision_tool_shortlist') catalogue.set(tool.name, tool);
    for (const tool of projectSchemas) catalogue.set(tool.name, tool);
    const compiled = mcpToolCase({ tools: [...catalogue.values()] }, args.shortlist,
      args.intent, args.information_policy_class);
    const receipt = await assessDecision(compiled, { ...decisionOptions,
      provider: args.provider, execute: args.execute === true,
      fetcher: decisionOptions.fetcher || request });
    return { schema: 'bonfyre.mcp-tool-decision.v1', receipt,
      proposal: receipt.state === 'diagnostic' ? decisionProposal(receipt) : null,
      invoked_tool: null, action: 'none' };
  }
  const server = http.createServer(async (req, res) => {
    if (req.method === 'GET' && req.url === '/health') {
      try {
        const health = await request(new URL('/health', runtimeUrl), { signal: AbortSignal.timeout(5000) });
        const value = await health.json();
        const names = new Set(RICH_CATALOG.tools.map(item => item.name));
        for (const item of LOCAL_TOOLS) names.add(item.name);
        for (const item of projectSchemas) names.add(item.name);
        return respond(res, health.ok ? 200 : 503, { status: health.ok ? 'ok' : 'degraded', service: 'bonfyre-mcp-gateway', runtime: value, tools: names.size, recursive_overlay: 'native' });
      }
      catch { return respond(res, 503, { status: 'degraded', service: 'bonfyre-recursive-overlay' }); }
    }
    if (req.headers.origin && !allowed.has(req.headers.origin)) return respond(res, 403, { error: 'origin not allowed' });
    if (!equal(String(req.headers.authorization ?? ''), `Bearer ${token}`)) return respond(res, 401, { error: 'unauthorized' });
    const route = new URL(req.url || '/', 'http://localhost');
    if (req.method === 'GET' && route.pathname === '/events') {
      const after = Number(route.searchParams.get('after') ?? 0), count = Number(route.searchParams.get('limit') ?? 500);
      if (!Number.isSafeInteger(after) || after < 0 || !Number.isSafeInteger(count) || count < 1) return respond(res, 400, { error: 'invalid event cursor or limit' });
      return respond(res, 200, ledger.events(after, Math.min(count, 2000)));
    }
    if (req.method === 'GET' && route.pathname === '/metrics') return respond(res, 200, ledger.metrics());
    if (req.method === 'POST' && route.pathname === '/internal/delta') {
      try { const payload = await body(req); return respond(res, 202, { accepted: true, seq: ledger.appendDelta(payload) }); }
      catch (error) { return respond(res, 400, { error: String(error) }); }
    }
    if (req.url !== '/mcp') {
      return respond(res, 404, { error: 'not found' });
    }
    if (req.method === 'GET') return respond(res, 405, { error: 'standalone GET stream disabled; use POST' });
    if (req.method !== 'POST') return respond(res, 405, { error: 'use POST' });
    let payload;
    try { payload = await body(req); } catch (error) { return respond(res, 400, { error: error.message }); }
    const { id, method, params = {} } = payload ?? {};
    if (method?.startsWith('notifications/')) { res.writeHead(202); res.end(); return; }
    const headers = { 'MCP-Protocol-Version': '2025-06-18', 'Mcp-Method': String(method || '') };
    try {
      let result;
      if (method === 'tools/list') {
        const byName = new Map(RICH_CATALOG.tools.map(item => [item.name, item]));
        for (const item of LOCAL_TOOLS) if (!byName.has(item.name)) byName.set(item.name, item);
        for (const item of projectSchemas) byName.set(item.name, item);
        result = { tools: withMcpIcons([...byName.values()], MCP_ICONS).sort((a, b) => a.name.localeCompare(b.name)) };
      } else if (method === 'resources/list') {
        const byUri = new Map(RICH_CATALOG.resources.map(item => [item.uri, item]));
        for (const item of REGISTRY.resources) if (!byUri.has(item.uri)) byUri.set(item.uri, item);
        result = { resources: withMcpIcons([...byUri.values()], MCP_ICONS) };
      } else if (method === 'prompts/list') {
        result = { prompts: withMcpIcons(RICH_CATALOG.prompts, MCP_ICONS) };
      } else if (method === 'prompts/get') {
        const name = String(params.name || '');
        if (!Object.hasOwn(REGISTRY.promptBodies, name)) throw new Error(`unknown prompt: ${name}`);
        const values = { focus: 'entire mesh', request: '', ...(params.arguments || {}) };
        const message = REGISTRY.promptBodies[name].replaceAll(/\{(focus|request)\}/g, (_, field) => String(values[field]));
        result = { description: name, messages: [{ role: 'user', content: { type: 'text', text: message } }] };
      } else if (method === 'tools/call' && (localNames.has(params.name) || nativeRichNames.has(params.name) || (projectNames.has(params.name) && (richNames.has(params.name) || listedNames().has(params.name))))) {
        const name = String(params.name), args = params.arguments || {};
        if (!args || typeof args !== 'object' || Array.isArray(args)) throw new Error('arguments must be an object');
        const trace = String(params?._meta?.traceparent || req.headers.traceparent || randomUUID().replaceAll('-', ''));
        const started = performance.now();
        try {
          const value = name === 'decision_tool_shortlist' ? await decideToolShortlist(args)
            : localNames.has(name) ? await ops.call(name, args, trace)
              : nativeRichNames.has(name) ? await richCall(name, args, trace) : await projectCall(name, args);
          ledger?.record(trace, name, args, 'passed', Math.round(performance.now() - started), value);
          result = wrap(value);
        } catch (error) {
          const value = { error: String(error), tool: name, trace_id: trace };
          ledger?.record(trace, name, args, 'failed', Math.round(performance.now() - started), value);
          result = { ...wrap(value), isError: true };
        }
      } else if (method === 'resources/read' && resourceUris.has(params.uri)) {
        const value = await ops.read(params.uri);
        result = { contents: [{ uri: params.uri, mimeType: 'application/json', text: JSON.stringify(value, null, 2) }] };
      } else if (method === 'resources/read' && Object.hasOwn(REGISTRY.richResourceTools, params.uri)) {
        const binding = REGISTRY.richResourceTools[params.uri];
        const value = binding.owner === 'project' ? await projectCall(binding.tool, binding.args || {}) : await richCall(binding.tool, binding.args || {});
        result = { contents: [{ uri: params.uri, mimeType: 'application/json', text: JSON.stringify(value, null, 2) }] };
      }
      else if (method === 'initialize') result = {
        protocolVersion: '2025-06-18',
        capabilities: { tools: { listChanged: false }, resources: { subscribe: false, listChanged: false }, prompts: { listChanged: false } },
        serverInfo: { name: 'bonfyre-mcp-mesh', version: '2.0.0', ...(MCP_ICONS.length ? { icons: MCP_ICONS } : {}) },
        instructions: 'Use project tools for live Mac source, runtime tools for Bonfyre APIs, Oracle tools for commands/services/profiles, explicit task handles for long work, and candidate patch tools before any approved write.',
      };
      else if (method === 'ping') result = {};
      else throw new Error(`unsupported method or name: ${method}`);
      respond(res, 200, { jsonrpc: '2.0', id, result }, headers);
    } catch (error) { respond(res, 200, { jsonrpc: '2.0', id, error: { code: -32000, message: String(error) } }, headers); }
  });
  server.on('close', () => ledger?.close());
  return { server, ready, refreshProjectSchemas };
}

if (import.meta.main) {
  const token = process.env.BONFYRE_MCP_TOKEN || (process.env.BONFYRE_MCP_TOKEN_FILE ? readFileSync(process.env.BONFYRE_MCP_TOKEN_FILE, 'utf8').trim() : '');
  const projectToken = process.env.BONFYRE_PROJECT_MCP_TOKEN_FILE ? readFileSync(process.env.BONFYRE_PROJECT_MCP_TOKEN_FILE, 'utf8').trim() : (process.env.BONFYRE_PROJECT_MCP_TOKEN || token);
  const ledger = openLedger(process.env.BONFYRE_MCP_DB || '/var/lib/bonfyre/mcp/mesh.sqlite3');
  const { server, ready } = createRecursiveOverlay({ token, projectUrl: process.env.BONFYRE_PROJECT_MCP_URL || 'http://127.0.0.1:13051/mcp', projectToken, ledger, origins: (process.env.BONFYRE_MCP_ALLOWED_ORIGINS || 'https://chatgpt.com,https://chat.openai.com,http://127.0.0.1,http://localhost').split(',').map(x => x.trim()).filter(Boolean) });
  await ready;
  server.listen(Number(process.env.BONFYRE_MCP_PORT || 3052), process.env.BONFYRE_MCP_HOST || '127.0.0.1');
}
