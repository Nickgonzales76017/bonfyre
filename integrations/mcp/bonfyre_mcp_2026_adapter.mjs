#!/usr/bin/env node
/*
 * bonfyre_mcp_2026_adapter.mjs -- stateless MCP 2026 ingress.
 *
 * This is deliberately an additive edge, rather than an edit to the staged
 * OAuth gateway.  It owns the 2026-07-28 HTTP contract and compiles a fresh
 * native McpApertureIR for every authenticated closure cache miss.  The
 * existing OAuth gateway remains the exact owner-facing lane for calls: this
 * adapter never turns an advertised candidate into authority or an effect.
 *
 * A deployment can consequently move clients to stateless/header-routed MCP
 * without swapping the concurrently-owned OAuth gateway in place.  Once an
 * owner gets a native lowerer it can replace its upstream forwarding lane;
 * the ApertureIR and its invalidation semantics do not change.
 */
import http from 'node:http';
import { createHash } from 'node:crypto';
import { spawn } from 'node:child_process';
import { knightIcons, withMcpIcons } from './brand.mjs';

const host = process.env.BONFYRE_MCP_2026_HOST || '127.0.0.1';
const port = Number(process.env.BONFYRE_MCP_2026_PORT || 8792);
const protocolVersion = '2026-07-28';
const publicOrigin = (process.env.BONFYRE_MCP_2026_PUBLIC_ORIGIN || 'https://aurekai.work').replace(/\/$/, '');
const MCP_ICONS = knightIcons(publicOrigin);
const authBinary = process.env.BONFYRE_AUTH_BINARY || '/usr/local/bin/bonfyre-auth';
const authDb = process.env.BONFYRE_AUTH_DB || '/var/lib/bonfyre/auth.db';
const addressBinary = process.env.BONFYRE_ADDRESSPLANE_BINARY || '/usr/local/bin/bonfyre-external-estate';
const addressDb = process.env.BONFYRE_ADDRESSPLANE_DB || '/var/lib/bonfyre/address-plane.db';
const mediaBinary = process.env.BONFYRE_MEDIA_LIBRARY_BINARY || '';
const mediaDb = process.env.BONFYRE_MEDIA_LIBRARY_DB || '';
const mediaCmsDb = process.env.BONFYRE_MEDIA_LIBRARY_CMS_DB || '';
const mediaDeliveryBinary = process.env.BONFYRE_MEDIA_DELIVERY_BINARY || '';
const mediaDeliveryDb = process.env.BONFYRE_MEDIA_DELIVERY_DB || '';
const channelBinary = process.env.BONFYRE_CHANNEL_BINARY || '';
const channelDb = process.env.BONFYRE_CHANNEL_DB || '';
const channelCmsDb = process.env.BONFYRE_CHANNEL_CMS_DB || '';
const upstream = new URL(process.env.BONFYRE_MCP_2026_UPSTREAM || 'http://127.0.0.1:8791/mcp');
const apertureTtlMs = Math.max(100, Number(process.env.BONFYRE_MCP_2026_APERTURE_TTL_MS || 1000));
const apertures = new Map();

function json(res, status, value, extra = {}) {
  const body = Buffer.from(JSON.stringify(value));
  res.writeHead(status, { 'content-type': 'application/json; charset=utf-8', 'content-length': body.length, 'cache-control': 'no-store', ...extra });
  res.end(body);
}
function reply(res, id, result) { json(res, 200, { jsonrpc: '2.0', id, result }); }
function error(res, id, code, message, data) { json(res, 200, { jsonrpc: '2.0', id, error: { code, message, ...(data === undefined ? {} : { data }) } }); }
function sha(text) { return `sha256:${createHash('sha256').update(String(text)).digest('hex')}`; }
function accessToken(req) {
  const value = String(req.headers.authorization || '');
  return value.startsWith('Bearer ') ? value.slice(7).trim() : '';
}
function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = []; let bytes = 0;
    req.on('data', chunk => {
      bytes += chunk.length;
      if (bytes > 1024 * 1024) reject(new Error('body_too_large'));
      else chunks.push(chunk);
    });
    req.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
    req.on('error', reject);
  });
}
function run(argv, stdin = '', extraEnv = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(argv[0], argv.slice(1), { stdio: ['pipe', 'pipe', 'pipe'], env: { ...process.env, ...extraEnv } });
    let out = '', err = '';
    child.stdout.on('data', value => { out += value; });
    child.stderr.on('data', value => { err += value; });
    child.on('error', reject);
    child.on('close', code => code === 0 ? resolve(out) : reject(new Error(err.trim() || `native_exit_${code}`)));
    child.stdin.end(stdin);
  });
}
async function verifiedActor(token) {
  /* An OAuth session is a transport principal, not an Actor coordinate. Auth
   * emits the only ingress closure that may bind one to the other. The bearer
   * travels over the owner pipe rather than this child argv. */
  const result = JSON.parse(await run([authBinary, '--db', authDb, 'actor-closure-stdin'], `${token}\n`));
  if (result.authenticated !== true || typeof result.actor_subject !== 'string' || !result.actor_subject) throw new Error('oauth_required');
  return result.actor_subject;
}
async function authorizesRead(token) {
  for (const scope of ['read', 'write', 'self']) {
    try {
      const result = JSON.parse(await run([authBinary, '--db', authDb, 'authorize', '--token', token, '--scope', scope]));
      if (result.authorized === true) return true;
    } catch { /* fail closed after all known read-capable scopes */ }
  }
  return false;
}
async function authorizesWrite(token) {
  try {
    const result = JSON.parse(await run([authBinary, '--db', authDb, 'authorize', '--token', token, '--scope', 'write']));
    return result.authorized === true;
  } catch { return false; }
}
function requireSelfDescription(req, request) {
  if (String(req.headers['mcp-protocol-version'] || '') !== protocolVersion) return 'unsupported_protocol_version';
  if (!request || request.jsonrpc !== '2.0' || typeof request.method !== 'string') return 'invalid_jsonrpc';
  const meta = request.params?._meta?.['io.modelcontextprotocol/clientInfo'];
  if (!meta || typeof meta.name !== 'string' || !meta.name) return 'client_info_required';
  if (String(req.headers['mcp-method'] || '') !== request.method) return 'method_header_mismatch';
  const name = String(req.headers['mcp-name'] || '');
  if (request.method === 'tools/call') return name === String(request.params?.name || '') && name ? '' : 'name_header_mismatch';
  if (request.method === 'resources/read') return name === String(request.params?.uri || '') && name ? '' : 'name_header_mismatch';
  return name === request.method ? '' : 'name_header_mismatch';
}
function requestScope(request) {
  if (request.method === 'tools/call' && ['bonfyre_aperture_refresh', 'media_library_catalog', 'media_playback_plan', 'creator_channel_surface'].includes(request.params?.name)) return 'read';
  if (request.method === 'tools/call') return 'write';
  return 'read';
}
async function nativeAperture(actor, token, forceRefresh = false) {
  const actorClosure = sha(`actor:${actor}`);
  /* These coordinates are produced only after native token verification.
   * They are deliberately opaque stable addresses, not client-controlled
   * assertions about membership, target proof, or authority. */
  const addressClosure = sha(`address:actor:${actor}`);
  const targetFrontier = sha(`target-frontier:actor:${actor}`);
  const policyGeneration = sha(`policy:transport-admission:${actor}`);
  const cacheKey = `${actorClosure}:${addressClosure}:${targetFrontier}:${policyGeneration}`;
  const cached = apertures.get(cacheKey);
  if (!forceRefresh && cached && cached.expiresAt > Date.now()) return cached.value;
  const text = await run([
    addressBinary, 'mcp-aperture', '--db', addressDb,
    '--actor-closure', actorClosure,
    '--address-closure', addressClosure,
    '--target-frontier', targetFrontier,
    '--policy-generation', policyGeneration,
  ]);
  const value = JSON.parse(text);
  if (value.mcp_protocol_version !== protocolVersion || value.truth !== 'advertisement_only_exact_owner_recheck_required') {
    throw new Error('native_aperture_contract_invalid');
  }
  /* The token is intentionally not part of the cache key/value and is never
   * passed to AddressPlane.  Owner forwarding revalidates it below. */
  void token;
  apertures.set(cacheKey, { value, expiresAt: Date.now() + apertureTtlMs });
  return value;
}
function closureResource() {
  return {
    uri: 'bonfyre://closure/current',
    name: 'Current addressed machine aperture',
    description: 'Derived current closure advertisement. It is not authority, proof, a provider admission, or an effect right.',
    mimeType: 'application/json',
  };
}
function closureTool() {
  return {
    name: 'bonfyre_aperture_refresh',
    description: 'Recompile the current addressed MCP aperture. This only reports advertised candidates and does not authorize, prove, reserve, commit, pay, or execute an effect.',
    inputSchema: { type: 'object', properties: {}, additionalProperties: false },
    annotations: { readOnlyHint: true },
  };
}
function apertureOperation(aperture, identity, verifier = 'MediaLibrary:access') {
  return Array.isArray(aperture.operations) && aperture.operations.some(item => item?.identity === identity && item?.exact_verifier === verifier);
}
function advertisedOwnerTool(aperture, name) {
  /* The staged gateway may know a much larger command estate.  It is an
   * exact-owner lane, not this caller's catalog: only a tool explicitly
   * retained in the current native aperture can cross this edge. */
  return Boolean(
    typeof name === 'string' && name &&
    Array.isArray(aperture.operations) &&
    aperture.operations.some(item => item?.identity === `mcp:tool:${name}`),
  );
}
function mediaAvailable(aperture, identity) {
  return Boolean(mediaBinary && mediaDb && mediaCmsDb && apertureOperation(aperture, identity));
}
function mediaDeliveryAvailable(aperture) {
  return Boolean(mediaDeliveryBinary && mediaDeliveryDb && mediaBinary && mediaDb && mediaCmsDb && apertureOperation(aperture, 'media_transport_lease', 'MediaDelivery:right'));
}
function channelAvailable(aperture) {
  return Boolean(channelBinary && channelDb && channelCmsDb && apertureOperation(aperture, 'creator_channel_surface', 'CreatorChannel:visibility'));
}
function mediaHandle(value) {
  return typeof value === 'string' && /^[A-Za-z0-9:_-]{1,180}$/.test(value) ? value : '';
}
function mediaTools(aperture) {
  const tools = [];
  if (mediaAvailable(aperture, 'media_library_catalog')) tools.push({
    name: 'media_library_catalog',
    description: 'Read one accessible personal/community media-library catalog. The authenticated MediaLibrary owner rechecks access; this does not grant media rights, delivery, or watch proof.',
    inputSchema: { type: 'object', properties: { library: { type: 'string', minLength: 1, maxLength: 180 } }, required: ['library'], additionalProperties: false },
    annotations: { readOnlyHint: true },
  });
  if (mediaAvailable(aperture, 'media_playback_plan')) tools.push({
    name: 'media_playback_plan',
    description: 'Choose an already-declared directly playable representation for an accessible library item. It never delivers bytes, enacts a transcode, opens an HLS/DASH/MOQT lease, or proves a watch.',
    inputSchema: { type: 'object', properties: { library: { type: 'string', minLength: 1, maxLength: 180 }, subject: { type: 'string', minLength: 1, maxLength: 180 }, capabilities: { type: 'object' } }, required: ['library', 'subject', 'capabilities'], additionalProperties: false },
    annotations: { readOnlyHint: true },
  });
  if (mediaDeliveryAvailable(aperture)) tools.push({
    name: 'media_transport_lease',
    description: 'Request a short-lived HLS, DASH, or MOQT transport lease for a current directly playable representation. MediaDelivery rechecks Auth, MediaLibrary, and an explicit protocol-scoped right. A lease is not delivered media, transcode, playback, or watch proof.',
    inputSchema: { type: 'object', properties: { library: { type: 'string', minLength: 1, maxLength: 180 }, subject: { type: 'string', minLength: 1, maxLength: 180 }, capabilities: { type: 'object' }, protocol: { type: 'string', enum: ['hls', 'dash', 'moqt'] }, ttl_seconds: { type: 'integer', minimum: 1, maximum: 3600 } }, required: ['library', 'subject', 'capabilities', 'protocol'], additionalProperties: false },
  });
  return tools;
}
function channelTools(aperture) {
  if (!channelAvailable(aperture)) return [];
  return [{
    name: 'creator_channel_surface',
    description: 'Read a current creator-channel surface. The native Channel owner derives the actor from the authenticated ActorClosure and rechecks current channel visibility and CMS publication membership. It is not delivery, a subscription mutation, view analytics, or watch proof.',
    inputSchema: { type: 'object', properties: { channel: { type: 'string', minLength: 1, maxLength: 180 } }, required: ['channel'], additionalProperties: false },
    annotations: { readOnlyHint: true },
  }];
}
function mediaTemplate(aperture) {
  const templates = [];
  if (mediaAvailable(aperture, 'media_library_catalog')) templates.push({ uriTemplate: 'bonfyre://media/library/{library}/catalog', name: 'Accessible media-library catalog', description: 'A current authenticated MediaLibrary projection; exact access is rechecked on every read.', mimeType: 'application/json' });
  if (channelAvailable(aperture)) templates.push({ uriTemplate: 'bonfyre://channel/{channel}/surface', name: 'Accessible creator-channel surface', description: 'A current authenticated CreatorChannel projection; exact visibility and CMS publication membership are rechecked on every read.', mimeType: 'application/json' });
  return templates;
}
async function mediaCatalog(library, token) {
  if (!mediaHandle(library)) throw new Error('invalid_media_library');
  const text = await run([mediaBinary, 'authenticated-catalog', '--db', mediaDb, '--auth-db', authDb, '--auth-token-stdin', '--library', library], `${token}\n`, { BONFYRE_AUTH_BIN: authBinary });
  return JSON.parse(text);
}
async function mediaPlaybackPlan(args, token) {
  const library = mediaHandle(args?.library);
  const subject = mediaHandle(args?.subject);
  if (!library || !subject || !args?.capabilities || typeof args.capabilities !== 'object' || Array.isArray(args.capabilities)) throw new Error('invalid_media_playback_input');
  const text = await run([mediaBinary, 'authenticated-playback-plan', '--db', mediaDb, '--cms-db', mediaCmsDb, '--auth-db', authDb, '--auth-token-stdin', '--library', library, '--subject', subject, '--capabilities', JSON.stringify(args.capabilities)], `${token}\n`, { BONFYRE_AUTH_BIN: authBinary });
  return JSON.parse(text);
}
async function mediaTransportLease(args, token) {
  const library = mediaHandle(args?.library);
  const subject = mediaHandle(args?.subject);
  const protocol = String(args?.protocol || '');
  const ttl = args?.ttl_seconds === undefined ? 300 : Number(args.ttl_seconds);
  if (!library || !subject || !args?.capabilities || typeof args.capabilities !== 'object' || Array.isArray(args.capabilities) || !['hls', 'dash', 'moqt'].includes(protocol) || !Number.isInteger(ttl) || ttl < 1 || ttl > 3600) throw new Error('invalid_media_transport_lease_input');
  const text = await run([mediaDeliveryBinary, 'authenticated-lease', '--db', mediaDeliveryDb, '--media-library-db', mediaDb, '--cms-db', mediaCmsDb, '--auth-db', authDb, '--library', library, '--subject', subject, '--capabilities', JSON.stringify(args.capabilities), '--protocol', protocol, '--ttl-seconds', String(ttl)], `${token}\n`, { BONFYRE_AUTH_BIN: authBinary, BONFYRE_MEDIA_LIBRARY_BIN: mediaBinary });
  return JSON.parse(text);
}
async function channelSurface(channel, token) {
  if (!mediaHandle(channel)) throw new Error('invalid_creator_channel');
  const text = await run([channelBinary, 'authenticated-surface', '--db', channelDb, '--cms-db', channelCmsDb, '--auth-db', authDb, '--channel', channel], `${token}\n`, { BONFYRE_AUTH_BIN: authBinary });
  return JSON.parse(text);
}
function textResult(value, isError = false) {
  return { content: [{ type: 'text', text: JSON.stringify(value, null, 2) }], structuredContent: value, isError };
}
function forwardUpstream(req, raw) {
  return new Promise((resolve, reject) => {
    const headers = {
      'content-type': 'application/json',
      accept: 'application/json, text/event-stream',
      authorization: req.headers.authorization,
      'content-length': Buffer.byteLength(raw),
    };
    const call = http.request({ protocol: upstream.protocol, hostname: upstream.hostname, port: upstream.port, path: upstream.pathname, method: 'POST', headers }, response => {
      const chunks = [];
      response.on('data', chunk => chunks.push(chunk));
      response.on('end', () => resolve({ status: response.statusCode || 502, body: Buffer.concat(chunks).toString('utf8') }));
    });
    call.on('error', reject); call.end(raw);
  });
}
function publicMetadata() {
  return { resource: `${publicOrigin}/mcp`, authorization_servers: [publicOrigin], scopes_supported: ['read', 'write'], bearer_methods_supported: ['header'] };
}
async function mcp(req, res, raw) {
  let request;
  try { request = JSON.parse(raw); } catch { return json(res, 400, { error: 'invalid_request' }); }
  const violation = requireSelfDescription(req, request);
  if (violation) return json(res, 400, { error: violation });
  const token = accessToken(req);
  if (!token) return json(res, 401, { error: 'oauth_required' }, { 'www-authenticate': `Bearer resource_metadata="${publicOrigin}/.well-known/oauth-protected-resource/mcp"` });
  let actor;
  try { actor = await verifiedActor(token); } catch { return json(res, 401, { error: 'oauth_required' }); }
  if (requestScope(request) === 'read' && !await authorizesRead(token)) return json(res, 403, { error: 'read_scope_required' });
  if (requestScope(request) === 'write' && !await authorizesWrite(token)) return json(res, 403, { error: 'write_scope_required' });
  let aperture;
  try { aperture = await nativeAperture(actor, token); }
  catch { return json(res, 503, { error: 'address_aperture_unavailable' }); }

  const id = request.id ?? null;
  if (request.method === 'initialize') {
    return reply(res, id, {
      protocolVersion,
      capabilities: { tools: { listChanged: true }, resources: { listChanged: true, subscribe: false }, prompts: { listChanged: true }, extensions: {} },
      serverInfo: { name: 'bonfyre-mcp-2026-aperture', version: '1.0.0', ...(MCP_ICONS.length ? { icons: MCP_ICONS } : {}) },
      instructions: 'This is a stateless, addressed MCP aperture. Tool and resource advertisements are not authority, evidence, proof, provider admission, commitment, payment approval, or effect permission. Exact owners recheck every forwarded operation.',
      _meta: { 'io.bonfyre/apertureDigest': aperture.aperture_digest },
    });
  }
  if (request.method === 'ping') return reply(res, id, {});
  if (request.method === 'resources/list') return reply(res, id, { resources: withMcpIcons([closureResource()], MCP_ICONS) });
  if (request.method === 'resources/templates/list') return reply(res, id, { resourceTemplates: mediaTemplate(aperture) });
  if (request.method === 'prompts/list') return reply(res, id, { prompts: [] });
  if (request.method === 'resources/read') {
    if (request.params?.uri === 'bonfyre://closure/current') return reply(res, id, { contents: [{ uri: 'bonfyre://closure/current', mimeType: 'application/json', text: JSON.stringify(aperture) }] });
    const match = /^bonfyre:\/\/media\/library\/([A-Za-z0-9:_-]{1,180})\/catalog$/.exec(String(request.params?.uri || ''));
    if (match && mediaAvailable(aperture, 'media_library_catalog')) {
      try { const catalog = await mediaCatalog(match[1], token); return reply(res, id, { contents: [{ uri: request.params.uri, mimeType: 'application/json', text: JSON.stringify(catalog) }] }); }
      catch { return error(res, id, -32003, 'media_library_access_refused'); }
    }
    const channelMatch = /^bonfyre:\/\/channel\/([A-Za-z0-9:_-]{1,180})\/surface$/.exec(String(request.params?.uri || ''));
    if (channelMatch && channelAvailable(aperture)) {
      try { const surface = await channelSurface(channelMatch[1], token); return reply(res, id, { contents: [{ uri: request.params.uri, mimeType: 'application/json', text: JSON.stringify(surface) }] }); }
      catch { return error(res, id, -32003, 'creator_channel_visibility_refused'); }
    }
    return error(res, id, -32002, 'resource_not_in_current_aperture');
  }
  if (request.method === 'tools/call' && request.params?.name === 'bonfyre_aperture_refresh') {
    try { return reply(res, id, textResult(await nativeAperture(actor, token, true))); }
    catch { return error(res, id, -32004, 'address_aperture_refresh_failed'); }
  }
  if (request.method === 'tools/list') {
    try {
      const forwarded = await forwardUpstream(req, raw);
      const answer = JSON.parse(forwarded.body);
      if (forwarded.status !== 200 || !answer.result || !Array.isArray(answer.result.tools)) throw new Error('owner_invalid_tool_list');
      answer.result.tools = [
        closureTool(),
        ...mediaTools(aperture),
        ...channelTools(aperture),
        ...answer.result.tools.filter(tool => advertisedOwnerTool(aperture, tool?.name)),
      ];
      answer.result.tools = withMcpIcons(answer.result.tools, MCP_ICONS);
      answer.result._meta = { ...(answer.result._meta || {}), 'io.bonfyre/apertureDigest': aperture.aperture_digest, 'io.bonfyre/advertisementTruth': aperture.truth };
      return json(res, 200, answer);
    } catch { return json(res, 502, { error: 'owner_tool_list_unavailable' }); }
  }
  if (request.method === 'tools/call' && request.params?.name === 'media_library_catalog') {
    if (!mediaAvailable(aperture, 'media_library_catalog')) return error(res, id, -32002, 'tool_not_in_current_aperture');
    try { return reply(res, id, textResult(await mediaCatalog(request.params?.arguments?.library, token))); }
    catch { return error(res, id, -32003, 'media_library_access_refused'); }
  }
  if (request.method === 'tools/call' && request.params?.name === 'media_playback_plan') {
    if (!mediaAvailable(aperture, 'media_playback_plan')) return error(res, id, -32002, 'tool_not_in_current_aperture');
    try { return reply(res, id, textResult(await mediaPlaybackPlan(request.params?.arguments, token))); }
    catch { return error(res, id, -32003, 'media_playback_plan_refused'); }
  }
  if (request.method === 'tools/call' && request.params?.name === 'media_transport_lease') {
    if (!mediaDeliveryAvailable(aperture)) return error(res, id, -32002, 'tool_not_in_current_aperture');
    try { return reply(res, id, textResult(await mediaTransportLease(request.params?.arguments, token))); }
    catch { return error(res, id, -32003, 'media_transport_lease_refused'); }
  }
  if (request.method === 'tools/call' && request.params?.name === 'creator_channel_surface') {
    if (!channelAvailable(aperture)) return error(res, id, -32002, 'tool_not_in_current_aperture');
    try { return reply(res, id, textResult(await channelSurface(request.params?.arguments?.channel, token))); }
    catch { return error(res, id, -32003, 'creator_channel_visibility_refused'); }
  }
  if (request.method === 'tools/call' && !advertisedOwnerTool(aperture, request.params?.name)) {
    return error(res, id, -32002, 'tool_not_in_current_aperture');
  }
  /* Calls remain owned by the existing authenticated gateway.  It re-evaluates
   * the current self/institution/admin closure and the receiving native owner
   * rechecks its own law.  This prevents this new routing plane from becoming
   * a shadow authority cache. */
  try {
    const forwarded = await forwardUpstream(req, raw);
    res.writeHead(forwarded.status, { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' });
    return res.end(forwarded.body);
  } catch { return json(res, 502, { error: 'owner_unreachable' }); }
}

http.createServer(async (req, res) => {
  try {
    const url = new URL(req.url || '/', publicOrigin);
    if (req.method === 'GET' && url.pathname === '/.well-known/oauth-protected-resource/mcp') return json(res, 200, publicMetadata());
    if (req.method === 'POST' && url.pathname === '/mcp') return mcp(req, res, await readBody(req));
    return json(res, 404, { error: 'not_found' });
  } catch { return json(res, 400, { error: 'invalid_request' }); }
}).listen(port, host, () => console.log(`bonfyre MCP 2026 aperture ${host}:${port}`));
