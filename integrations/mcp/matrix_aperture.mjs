#!/usr/bin/env bun
import http from 'node:http';
import { timingSafeEqual } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { boundedPipe } from './bounded_pipe.mjs';

const registry = JSON.parse(readFileSync(new URL('./matrix_tools.json', import.meta.url), 'utf8'));
export const TOOLS = Object.freeze(registry.tools);
const toolNames = new Set(TOOLS.map(tool => tool.name));
const hostPattern = /^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)+$/;
const coordinatePattern = /^[A-Za-z0-9:_.-]+$/;
const bounded = (value, label, max = 48) => { const str = String(value ?? '').trim(); if (!str || str.length > max || !coordinatePattern.test(str)) throw new Error(`invalid Matrix ${label}`); return str; };
export function host(value) { const result = String(value ?? '').trim().toLowerCase().replace(/\.$/, ''); if (result.length > 253 || !hostPattern.test(result)) throw new Error('host must be a normalized DNS hostname'); return result; }
export function path(value = '/') { const result = String(value); if (!result.startsWith('/') || result.length > 2048 || result.includes('://') || result.includes('\0')) throw new Error('path must be a bounded local Matrix path'); return result; }
export function nativeRunner(binary, argv, timeoutMs) {
  const started = performance.now();
  const result = Bun.spawnSync([binary, ...argv], { stdout: 'pipe', stderr: 'pipe', timeout: timeoutMs });
  return { exit_code: result.exitCode ?? 124, stdout: result.stdout.toString(), stderr: result.stderr.toString().slice(-100_000), duration_ms: Math.round(performance.now() - started) };
}

export function matrixInvocation(name, args = {}, db = process.env.BONFYRE_MATRIX_DB || '/var/lib/bonfyre/matrix.db') {
  if (!toolNames.has(name)) throw new Error(`unknown tool: ${name}`);
  if (!args || typeof args !== 'object' || Array.isArray(args)) throw new Error('arguments must be an object');
  let argv, envelope, timeoutMs = 30_000;
  switch (name) {
    case 'matrix_address_resolve': {
      argv = ['resolve', '--db', db, '--host', host(args.host), '--path', path(args.path ?? '/')];
      for (const [key, flag] of [['world', '--world'], ['audience', '--audience'], ['representation', '--repr']]) if (args[key] !== undefined && args[key] !== null) argv.push(flag, bounded(args[key], key));
      envelope = { schema_version: 'bonfyre.matrix.address.mcp.v1', native_owner: 'BonfyreMatrix/AddressPlane', forbidden_inference: 'an address resolution is not a viewer closure, evidence frontier, provider route, payment state, publication decision, or effect authority' };
      break;
    }
    case 'matrix_subject_constellation': {
      const subject = bounded(args.subject, 'subject identifier', 256);
      argv = ['constellation', '--db', db, '--subject', subject];
      envelope = { schema_version: 'bonfyre.matrix.constellation.mcp.v1', native_owner: 'BonfyreMatrix/AddressConstellation', subject, forbidden_inference: 'an address constellation locates one semantic subject; it neither copies the subject nor grants disclosure, provider, financial, or effect authority' };
      break;
    }
    case 'matrix_closure_compile': {
      const targetHost = host(args.host), format = String(args.format ?? 'ro-crate').trim().toLowerCase();
      if (!['ro-crate', 'zarr'].includes(format)) throw new Error('format must be ro-crate or zarr');
      const seconds = Number(args.timeout_seconds ?? 30);
      if (!Number.isInteger(seconds) || seconds < 5 || seconds > 120) throw new Error('timeout_seconds must be 5..120');
      timeoutMs = seconds * 1000;
      argv = ['closure', '--db', db, '--host', targetHost, '--format', format];
      envelope = { schema_version: 'bonfyre.matrix.closure.receipt.v1', status: 'available', host: targetHost, format, native_owner: 'BonfyreMatrix/RecursiveMatrixClosure', unresolved_closures: ['actor/session', 'delegation envelope', 'information policy', 'evidence frontier', 'material body', 'latent passport', 'provider/resource state', 'work/effect authority', 'economic commitment'], forbidden_inference: 'a sparse Matrix closure is not a complete viewer closure, material body, model route, financial state, or effect authority' };
      break;
    }
  }
  return { argv, envelope, timeoutMs, field: name === 'matrix_address_resolve' ? 'resolution' : name === 'matrix_subject_constellation' ? 'constellation' : 'closure' };
}
function matrixResult(invocation, result) {
  if (result.exit_code !== 0) throw new Error(result.stderr || result.stdout || 'BonfyreWebAddress failed');
  let value;
  try { value = JSON.parse(result.stdout); } catch { throw new Error('BonfyreWebAddress returned non-JSON output'); }
  const { stdout, ...receipt } = result;
  return { ...invocation.envelope, [invocation.field]: value, native_receipt: receipt };
}
export function callMatrixTool(name, args = {}, { binary = process.env.BONFYRE_WEB_ADDRESS_BINARY || '/usr/local/bin/bonfyre-web-address', db = process.env.BONFYRE_MATRIX_DB || '/var/lib/bonfyre/matrix.db', runner = nativeRunner } = {}) {
  const invocation = matrixInvocation(name, args, db);
  return matrixResult(invocation, runner(binary, invocation.argv, invocation.timeoutMs));
}
export async function callMatrixToolAsync(name, args = {}, { binary = process.env.BONFYRE_WEB_ADDRESS_BINARY || '/usr/local/bin/bonfyre-web-address', db = process.env.BONFYRE_MATRIX_DB || '/var/lib/bonfyre/matrix.db', runner } = {}) {
  const invocation = matrixInvocation(name, args, db);
  const perform = runner || (async (program, argv, timeoutMs) => {
    const started = performance.now();
    const child = Bun.spawn([program, ...argv], { stdout: 'pipe', stderr: 'pipe' });
    let timedOut = false, hardStop;
    const timer = setTimeout(() => { timedOut = true; child.kill(); hardStop = setTimeout(() => child.kill('SIGKILL'), 250); }, timeoutMs);
    try {
      const [stdout, stderr, exit_code] = await Promise.all([boundedPipe(child.stdout, 200_000), boundedPipe(child.stderr, 100_000), child.exited]);
      return { exit_code: timedOut ? 124 : exit_code, stdout, stderr, duration_ms: Math.round(performance.now() - started), ...(timedOut ? { timeout: true } : {}) };
    } finally { clearTimeout(timer); clearTimeout(hardStop); }
  });
  return matrixResult(invocation, await perform(binary, invocation.argv, invocation.timeoutMs));
}

const jsonReply = (res, status, value, headers = {}) => {
  const body = Buffer.from(JSON.stringify(value));
  res.writeHead(status, { 'Content-Type': 'application/json', 'Content-Length': body.length, 'Cache-Control': 'no-store', 'X-Content-Type-Options': 'nosniff', ...headers });
  res.end(body);
};
const equalToken = (left, right) => { const a = Buffer.from(left), b = Buffer.from(right); return a.length === b.length && timingSafeEqual(a, b); };
async function readJson(req) {
  const chunks = []; let bytes = 0;
  for await (const chunk of req) { bytes += chunk.length; if (bytes > 1_000_000) throw new Error('body too large'); chunks.push(chunk); }
  return JSON.parse(Buffer.concat(chunks).toString('utf8'));
}
export function createMatrixMcpServer({ token, origins = ['https://chatgpt.com', 'https://chat.openai.com', 'http://127.0.0.1', 'http://localhost'], call = callMatrixTool } = {}) {
  if (!token) throw new Error('Matrix MCP token missing');
  const allowed = new Set(origins);
  return http.createServer(async (req, res) => {
    if (req.method === 'GET' && req.url === '/health') return jsonReply(res, 200, { status: 'ok', service: 'bonfyre-matrix-mcp-aperture', tools: TOOLS.length });
    if (req.headers.origin && !allowed.has(req.headers.origin)) return jsonReply(res, 403, { error: 'origin not allowed' });
    if (!equalToken(String(req.headers.authorization ?? ''), `Bearer ${token}`)) return jsonReply(res, 401, { error: 'unauthorized' });
    if (req.url !== '/mcp') return jsonReply(res, 404, { error: 'not found' });
    if (req.method !== 'POST') return jsonReply(res, 405, { error: 'use POST' });
    let payload;
    try { payload = await readJson(req); } catch (error) { return jsonReply(res, 400, { error: error.message }); }
    const { id, method, params = {} } = payload ?? {};
    if (method?.startsWith('notifications/')) { res.writeHead(202); res.end(); return; }
    const headers = { 'MCP-Protocol-Version': '2025-06-18', 'Mcp-Method': String(method ?? '') };
    try {
      let result;
      switch (method) {
        case 'initialize': result = { protocolVersion: '2025-06-18', capabilities: { tools: { listChanged: false } }, serverInfo: { name: 'bonfyre-matrix-mcp-aperture', version: '1.0.0' }, instructions: 'Read-only Matrix address and sparse closure aperture. No viewer, publication, effect, or financial authority is granted.' }; break;
        case 'ping': result = {}; break;
        case 'tools/list': result = { tools: TOOLS }; break;
        case 'tools/call': {
          const value = call(String(params.name ?? ''), params.arguments ?? {});
          result = { content: [{ type: 'text', text: JSON.stringify(value, null, 2) }], structuredContent: value, isError: false };
          break;
        }
        default: throw new Error(`unsupported method: ${method}`);
      }
      jsonReply(res, 200, { jsonrpc: '2.0', id, result }, headers);
    } catch (error) { jsonReply(res, 200, { jsonrpc: '2.0', id, error: { code: -32000, message: error.message } }, headers); }
  });
}

if (import.meta.main) {
  const token = process.env.BONFYRE_MCP_TOKEN || (process.env.BONFYRE_MCP_TOKEN_FILE ? readFileSync(process.env.BONFYRE_MCP_TOKEN_FILE, 'utf8').trim() : '');
  const server = createMatrixMcpServer({ token, origins: (process.env.BONFYRE_MCP_ALLOWED_ORIGINS || 'https://chatgpt.com,https://chat.openai.com,http://127.0.0.1,http://localhost').split(',').map(s => s.trim()).filter(Boolean) });
  const port = Number(process.env.BONFYRE_MATRIX_MCP_PORT || 3053), bind = process.env.BONFYRE_MATRIX_MCP_HOST || '127.0.0.1';
  server.listen(port, bind, () => console.log(`Matrix MCP aperture listening on ${bind}:${port}`));
}
