// Shared bounded MCP wire boundary; semantic owners remain outside transport.
import { timingSafeEqual } from 'node:crypto';
import { isDeepStrictEqual } from 'node:util';
export const json = value => JSON.stringify(value, (_, item) => typeof item === 'bigint' ? String(item) : item);
export const response = (status, value) => new Response(value === undefined ? null : json(value), {
  status, headers: { 'Content-Type': 'application/json', 'Cache-Control': 'no-store', 'X-Content-Type-Options': 'nosniff' },
});
export const equal = (a, b) => { const left = Buffer.from(a), right = Buffer.from(b); return left.length === right.length && timingSafeEqual(left, right); };
export const object = value => value !== null && typeof value === 'object' && !Array.isArray(value);

// This catalog uses only the structural/type/enum/range subset below. Reject
// malformed arguments before they reach an owner, without coercing strings
// into booleans or silently granting defaults the owner did not declare.
export function validateArguments(value, schema, path = 'arguments') {
  if (Array.isArray(schema.type)) {
    for (const type of schema.type) {
      try { validateArguments(value,{...schema,type},path); return; } catch { /* Try the next declared JSON type. */ }
    }
    throw new Error(`${path} must match one of ${schema.type.join(', ')}`);
  }
  const valid = schema.type === 'object' ? object(value) : schema.type === 'array' ? Array.isArray(value)
    : schema.type === 'integer' ? Number.isSafeInteger(value)
      : schema.type === 'number' ? typeof value === 'number' && Number.isFinite(value)
        : schema.type === 'null' ? value === null : !schema.type || typeof value === schema.type;
  if (!valid) throw new Error(`${path} must be ${schema.type}`);
  if (Object.hasOwn(schema, 'const') && !isDeepStrictEqual(value, schema.const)) throw new Error(`${path} must equal its declared constant`);
  if (schema.enum && !schema.enum.includes(value)) throw new Error(`${path} is not an allowed value`);
  if (schema.minimum !== undefined && value < schema.minimum || schema.maximum !== undefined && value > schema.maximum)
    throw new Error(`${path} is outside its allowed range`);
  if (schema.type === 'object') {
    for (const key of schema.required || []) if (!Object.hasOwn(value, key)) throw new Error(`${path}.${key} is required`);
    for (const [key, item] of Object.entries(value)) {
      if (Object.hasOwn(schema.properties || {}, key)) validateArguments(item, schema.properties[key], `${path}.${key}`);
      else if (schema.additionalProperties === false) throw new Error(`${path}.${key} is not allowed`);
    }
  } else if (schema.type === 'array' && schema.items) {
    for (let i = 0; i < value.length; i++) validateArguments(value[i], schema.items, `${path}[${i}]`);
  }
}

export async function readRequest(request, { maxBytes = 20_000_000, timeoutMs = 30_000 } = {}) {
  const length = request.headers.get('content-length');
  if (length !== null && (!/^\d+$/.test(length) || Number(length) > maxBytes)) throw new Error('invalid body size');
  if (!request.body) throw new Error('invalid body size');
  const reader = request.body.getReader(), chunks = [];
  let size = 0, timer;
  const deadline = new Promise((_, reject) => { timer = setTimeout(() => reject(new Error('request body timed out')), timeoutMs); });
  try {
    while (true) {
      const { value, done } = await Promise.race([reader.read(), deadline]);
      if (done) break;
      size += value.byteLength;
      if (size > maxBytes) throw new Error('invalid body size');
      chunks.push(value);
    }
    if (!size) throw new Error('invalid body size');
    return JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(Buffer.concat(chunks)));
  } finally {
    clearTimeout(timer);
    reader.cancel().catch(() => {});
    reader.releaseLock();
  }
}

// One bounded transport for catalog-driven owners. The owner supplies its
// operations and may serialize a legacy "read" that actually persists data.
export function createMcpTransport({ token, label, service, version, instructions, tools, dispatch,
  statusTool, health = () => ({}), prompts = [], getPrompt, serialize = [],
  routes = [], maxBytes = 10_000_000, maxConcurrent = 16 }) {
  if (!token) throw new Error(`${label} requires an authentication token`);
  if (!Number.isSafeInteger(maxConcurrent) || maxConcurrent < 1 || maxConcurrent > 64) throw new Error('invalid concurrency bound');
  const definitions = new Map(tools.map(tool => [tool.name, tool])), serial = new Set(serialize);
  const routeMap = new Map();
  for (const route of routes) {
    const key = `${route.method} ${route.path}`;
    if (!['GET', 'POST'].includes(route.method) || !/^(?:\/|\/[a-z][a-z0-9_/-]*)$/.test(route.path)
        || ['/health', '/status', '/mcp'].includes(route.path) || routeMap.has(key)
        || typeof route.run !== 'function' || route.method === 'POST' && !route.schema
        || route.render !== undefined && (route.method !== 'GET' || typeof route.render !== 'function'))
      throw new Error('invalid owner route');
    routeMap.set(key, route);
  }
  if (definitions.size !== tools.length || tools.some(tool => !Object.hasOwn(dispatch, tool.name) || typeof dispatch[tool.name] !== 'function')
      || Object.keys(dispatch).length !== tools.length) throw new Error(`unbound ${label} catalog`);
  let active = 0, effectTail = Promise.resolve();
  async function invoke(fn, args, serialized) {
    if (!serialized) return await fn(args);
    const pending = effectTail.then(() => fn(args));
    effectTail = pending.catch(() => {});
    return await pending;
  }
  async function call(name, args = {}) {
    const definition = definitions.get(name);
    if (!definition) throw new Error(`unknown ${label} tool`);
    validateArguments(args, definition.inputSchema);
    return await invoke(dispatch[name], args, definition.annotations.readOnlyHint !== true || serial.has(name));
  }
  const errorText = error => String(error?.message || `${label} operation failed`).replaceAll(token, '[redacted]').slice(0, 2000);
  const send = (status, value) => {
    const res = response(status, value);
    res.headers.set('MCP-Protocol-Version', '2025-06-18');
    return res;
  };
  async function handle(req) {
    const path = new URL(req.url).pathname, isHealth = req.method === 'GET' && path === '/health', route = routeMap.get(`${req.method} ${path}`);
    if (!isHealth && !equal(req.headers.get('authorization') || '', `Bearer ${token}`)) return send(401, { error: 'unauthorized' });
    if (!(route || isHealth || req.method === 'GET' && path === '/status' || req.method === 'POST' && path === '/mcp')) return send(404, { error: 'not found' });
    if (active >= maxConcurrent) return send(503, { error: `${label} owner concurrency limit` });
    active++;
    let id = null;
    try {
      if (route) {
        let args = {};
        if (req.method === 'POST') {
          try { args = await readRequest(req, { maxBytes }); validateArguments(args, route.schema); }
          catch (error) { return send(400, { error: errorText(error) }); }
        }
        const value = await invoke(route.run, args, route.serialize === true);
        if (route.render) {
          const body = route.render(value);
          if (typeof body !== 'string' || Buffer.byteLength(body) > maxBytes) throw new Error(`${label} rendered output exceeds budget`);
          return new Response(body, { status: 200, headers: { 'Content-Type': 'text/html; charset=utf-8',
            'Cache-Control': 'no-store', 'X-Content-Type-Options': 'nosniff',
            'Content-Security-Policy': "default-src 'none'; style-src 'unsafe-inline'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'" } });
        }
        if (Buffer.byteLength(json(value)) > maxBytes) throw new Error(`${label} route output exceeds budget; reconcile before retrying effects`);
        return send(route.status ?? 200, value);
      }
      if (isHealth) return send(200, { status: 'ok', service, version, tools: tools.length, runtime: 'bun', ...await health() });
      if (req.method === 'GET') {
        const result = await call(statusTool, {});
        if (Buffer.byteLength(json(result)) > maxBytes) throw new Error(`${label} status exceeds output budget`);
        return send(200, result);
      }
      let message;
      try { message = await readRequest(req, { maxBytes }); }
      catch (error) { return send(400, { error: errorText(error) }); }
      if (!object(message) || message.jsonrpc !== '2.0' || typeof message.method !== 'string') throw new Error('invalid JSON-RPC request');
      const incomingId = message.id ?? null;
      if (!(incomingId === null || typeof incomingId === 'string' || typeof incomingId === 'number' && Number.isFinite(incomingId))) throw new Error('invalid request id');
      id = incomingId;
      const params = message.params ?? {};
      if (!object(params)) throw new Error('params must be an object');
      let result;
      switch (message.method) {
        case 'initialize': result = { protocolVersion: '2025-06-18', capabilities: { tools: { listChanged: false }, ...(prompts.length ? { prompts: { listChanged: false } } : {}) }, serverInfo: { name: service, version }, instructions }; break;
        case 'notifications/initialized': case 'notifications/cancelled': return send(202);
        case 'ping': result = {}; break;
        case 'tools/list': result = { tools }; break;
        case 'tools/call': {
          try {
            const value = await call(String(params.name || ''), params.arguments ?? {});
            result = { content: [{ type: 'text', text: json(value) }], structuredContent: value, isError: false };
          } catch (error) {
            const value = { tool: String(params.name || ''), error: errorText(error) };
            result = { content: [{ type: 'text', text: json(value) }], structuredContent: value, isError: true };
          }
          break;
        }
        case 'prompts/list':
          if (!prompts.length) throw new Error('unsupported method');
          result = { prompts }; break;
        case 'prompts/get': {
          const prompt = prompts.find(item => item.name === params.name), args = params.arguments ?? {};
          if (!prompt || !getPrompt || !object(args)) throw new Error('invalid prompt');
          for (const argument of prompt.arguments || []) if (argument.required && typeof args[argument.name] !== 'string') throw new Error(`prompt ${argument.name} is required`);
          result = await getPrompt(prompt.name, args); break;
        }
        default: throw new Error('unsupported method');
      }
      const payload = { jsonrpc: '2.0', id, result };
      if (Buffer.byteLength(json(payload)) > maxBytes) throw new Error(`${label} output exceeds budget; reconcile owner receipts before retrying effects`);
      return send(200, payload);
    } catch (error) { return send(req.method === 'GET' || route ? 503 : 200, { jsonrpc: '2.0', id, error: { code: -32000, message: errorText(error) } }); }
    finally { active--; }
  }
  return { handle, call };
}
