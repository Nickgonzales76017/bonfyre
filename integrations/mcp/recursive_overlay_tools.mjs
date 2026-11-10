#!/usr/bin/env bun
// Owner-backed recursive MCP operations. This module owns no semantic store.
import { createHash, randomUUID } from 'node:crypto';
import { mkdir, readFile, readdir, stat, unlink } from 'node:fs/promises';
import { join } from 'node:path';
import { Database } from 'bun:sqlite';
import { callMatrixToolAsync, host as matrixHost } from './matrix_aperture.mjs';
import { OPERATING_CONTRACT } from './recursive_closure_contract.mjs';
import { boundedPipe } from './bounded_pipe.mjs';

const ident = (value, what, max = 128) => {
  const text = String(value ?? '').trim();
  if (!text || text.length > max || !/^[A-Za-z0-9][A-Za-z0-9._:-]*$/.test(text)) throw new Error(`${what} must be a short identifier`);
  return text;
};
const url = (value, httpsOnly = false) => {
  const source = String(value ?? '').trim();
  let parsed;
  try { parsed = new URL(source); } catch { throw new Error('invalid source URL'); }
  if (!['http:', 'https:'].includes(parsed.protocol) || (httpsOnly && parsed.protocol !== 'https:') || !parsed.hostname || parsed.username || parsed.password) throw new Error(httpsOnly ? 'url must be https' : 'source_url must be an http(s) URL');
  return source;
};
const digest = value => createHash('sha256').update(value).digest('hex');
const parse = text => { try { return JSON.parse(text); } catch { return text; } };
export async function nativeRun(argv, { timeoutMs = 30_000, env = process.env, cwd } = {}) {
  const started = performance.now();
  const child = Bun.spawn(argv, { stdout: 'pipe', stderr: 'pipe', env, ...(cwd ? { cwd } : {}) });
  let timedOut = false, hardStop;
  const timer = setTimeout(() => { timedOut = true; child.kill(); hardStop = setTimeout(() => child.kill('SIGKILL'), 250); }, timeoutMs);
  try {
    const [stdout, stderr, exit_code] = await Promise.all([boundedPipe(child.stdout, 200_000), boundedPipe(child.stderr, 100_000), child.exited]);
    return { exit_code: timedOut ? 124 : exit_code, stdout, stderr, duration_ms: Math.round(performance.now() - started), ...(timedOut ? { timeout: true } : {}) };
  } finally { clearTimeout(timer); clearTimeout(hardStop); }
}

export function createRecursiveTools({
  env = process.env,
  run = nativeRun,
  request = fetch,
  matrix = callMatrixToolAsync,
  projectCall = async () => { throw new Error('project owner unavailable'); },
  baseCall = async () => { throw new Error('rich mesh owner unavailable'); },
} = {}) {
  const config = {
    foreignRoot: env.BONFYRE_FOREIGN_ROOT || '/var/lib/bonfyre/foreign',
    projectBinary: env.BONFYRE_PROJECT_BINARY || '/usr/local/bin/bonfyre-project',
    cmsBinary: env.BONFYRE_CMS_BINARY || '/usr/local/bin/bonfyre-cms',
    cmsDb: env.BONFYRE_CMS_DB || '/var/lib/bonfyre/cms/bonfyre_cms.db',
    cmsSchemas: env.BONFYRE_CMS_SCHEMAS || '/var/lib/bonfyre/cms/content-types',
    surfaceOrigin: env.BONFYRE_SURFACE_ORIGIN || 'http://127.0.0.1:8787',
    surfaceHost: env.BONFYRE_SURFACE_HOST || 'aurekai.life',
    matrixDb: env.BONFYRE_MATRIX_DB || '/var/lib/bonfyre/matrix.db',
    matrixBinary: env.BONFYRE_WEB_ADDRESS_BINARY || '/usr/local/bin/bonfyre-web-address',
    financeDb: env.BONFYRE_FINANCE_DB || '/var/lib/bonfyre/state/finance.db',
    capabilityUrl: env.BONFYRE_CAPABILITY_URL || 'http://127.0.0.1:8787/capability',
    organsDir: env.BONFYRE_ORGANS_DIR || '/usr/local/bin',
    publicOrigin: env.BONFYRE_PUBLIC_ORIGIN || 'https://aurekai.work',
  };
  const native = (name, args) => matrix(name, args, { binary: config.matrixBinary, db: config.matrixDb });

  async function ownerRun(argv, timeoutMs = 30_000) {
    const result = await run(argv, { timeoutMs });
    return result;
  }
  async function foreignTwinObserve(args) {
    const twin = ident(args.twin_id, 'twin_id');
    if (twin.length > 128 || !/^[A-Za-z0-9][A-Za-z0-9._-]*$/.test(twin)) throw new Error('twin_id must be a short slug');
    const source = url(args.url, true);
    const requestedTimeout = Number(args.timeout_seconds || 30);
    if (!Number.isInteger(requestedTimeout)) throw new Error('timeout_seconds must be an integer');
    const timeoutMs = Math.max(5, Math.min(requestedTimeout, 120)) * 1000;
    const stamp = new Date().toISOString().replace(/[-:]/g, '').replace(/\.\d{3}/, '');
    const tag = digest(`${twin}${source}${stamp}`).slice(0, 10);
    const receipts = join(config.foreignRoot, 'receipts'), bodies = join(config.foreignRoot, 'bodies');
    await Promise.all([mkdir(receipts, { recursive: true }), mkdir(bodies, { recursive: true })]);
    const receiptPath = join(receipts, `${twin}-${stamp}-${tag}.json`);
    const bodyPath = join(bodies, `${twin}-${stamp}-${tag}.body`);
    const result = await ownerRun([config.projectBinary, 'foreign', 'observe', twin, source, '--receipt', receiptPath, '--body', bodyPath, '--observe-right', 'public'], timeoutMs);
    let observation = null;
    try {
      const raw = await readFile(receiptPath, 'utf8');
      const chunks = raw.split('\n{');
      for (let i = chunks.length - 1; i >= 0; i--) {
        const candidate = chunks[i].startsWith('{') ? chunks[i] : `{${chunks[i]}`;
        try { observation = JSON.parse(candidate); break; } catch { /* earlier receipt */ }
      }
    } catch { /* native owner may refuse before writing */ }
    let size = null;
    try { size = (await stat(bodyPath)).size; } catch { /* no body */ }
    const status = Number(observation?.http_status || 0);
    const retention = { http_status: status, retained: size !== null };
    if (size !== null && (status >= 400 || size > 2_000_000)) {
      await unlink(bodyPath);
      retention.bytes_dropped = size;
      retention.retained = false;
      retention.reason = status >= 400 ? 'origin answered an error status; its body is not evidence of the resource' : 'body over 2MB';
    }
    return { schema_version: 'bonfyre.foreign.observe.mcp.v1', native_owner: 'BonfyreProject/ForeignTwin', retention, foreign_root: config.foreignRoot, receipt_path: receiptPath, body_path: retention.retained ? bodyPath : null, observation, native_exit: result.exit_code, forbidden_inference: 'a retained body is not remote truth, availability, a licence to republish, or a relationship with the observed origin; a robots refusal is a recorded observation, not a failed one' };
  }
  async function foreignResolution(args) {
    const action = ident(args.action, 'action', 24).toLowerCase();
    if (!['resolve', 'history', 'conflicts'].includes(action)) throw new Error('action must be resolve, history or conflicts');
    const argv = [config.projectBinary, 'foreign', action];
    if (action !== 'conflicts') argv.push(url(args.source_url));
    const result = await ownerRun(argv, 45_000);
    return { schema_version: 'bonfyre.foreign.resolution.mcp.v1', native_owner: 'BonfyreProject', action, result: parse(result.stdout), native_exit: result.exit_code, forbidden_inference: 'a current observation is not a true statement about the world, and a superseded one is not a deleted one: both remain recorded' };
  }
  async function cmsRecord(args) {
    const action = ident(args.action, 'action', 24).toLowerCase();
    let argv = [config.cmsBinary];
    if (action === 'schemas') argv.push('schema', 'list');
    else if (['list', 'get', 'create', 'update', 'delete'].includes(action)) {
      argv.push('entry', action, ident(args.content_type, 'content_type'));
      if (['get', 'update', 'delete'].includes(action)) argv.push(ident(args.entry_id, 'entry_id', 64));
      if (['create', 'update'].includes(action)) {
        if (!args.attributes || typeof args.attributes !== 'object' || Array.isArray(args.attributes) || !Object.keys(args.attributes).length) throw new Error('attributes object required to create or update');
        argv.push(JSON.stringify(args.attributes));
      }
      if (args.namespace) argv.push('--ns', ident(args.namespace, 'namespace', 64));
      if (action === 'list') {
        const requested = Number(args.limit || 20);
        if (!Number.isInteger(requested)) throw new Error('limit must be an integer');
        argv.push('--limit', String(Math.max(1, Math.min(requested, 200))));
      }
    } else throw new Error('action must be one of schemas, list, get, create, update, delete');
    argv.push('--db', config.cmsDb, '--schemas', config.cmsSchemas);
    const result = await ownerRun(argv, 60_000);
    return { schema_version: 'bonfyre.cms.record.mcp.v1', native_owner: 'BonfyreCMS', store: config.cmsDb, action, result: parse(result.stdout), native_exit: result.exit_code, stderr: result.stderr.slice(0, 400), forbidden_inference: 'a created or updated record is not published, publicly readable, or an institutional commitment' };
  }
  async function surfaceProject(args) {
    const action = ident(args.action, 'action', 24).toLowerCase();
    let path = '/_surface/';
    if (action === 'read') {
      path += ident(args.pattern, 'pattern', 64);
      if (args.selector) path += `?page=${encodeURIComponent(ident(args.selector, 'selector', 64))}`;
    } else if (action !== 'catalogue') throw new Error('action must be catalogue or read');
    const address = config.surfaceOrigin.replace(/\/$/, '') + path;
    try {
      const response = await request(address, { headers: { Accept: 'application/json', Host: config.surfaceHost }, signal: AbortSignal.timeout(20_000) });
      const body = await response.text();
      return { schema_version: 'bonfyre.surface.project.mcp.v1', native_owner: 'BonfyreWebAddress', surface: address, action, http_status: response.status, result: body.trim() ? parse(body) : null, forbidden_inference: 'a projected surface is not an authorized effect: these are the records a reader may see, and Authority still gates at effect time. A pattern absent from the catalogue is undeclared, not hidden.' };
    } catch (error) { return { schema_version: 'bonfyre.surface.project.mcp.v1', native_owner: 'BonfyreWebAddress', action, error: `surface unreachable: ${String(error).slice(0, 120)}` }; }
  }
  async function recursiveFrontier(args, traceId) {
    const target = matrixHost(args.host || new URL(config.publicOrigin).hostname);
    const exact = await native('matrix_closure_compile', { host: target, format: args.format || 'ro-crate' });
    const safe = async (name, parameters = {}) => {
      try { return await baseCall(name, parameters, traceId); }
      catch (error) { return { status: 'unavailable', owner_tool: name, reason: String(error) }; }
    };
    const [material, latent, execution, economic, atlas, carrier] = await Promise.all([
      safe('membrane_state'), safe('model_local_inventory'), safe('desk_open_decisions'), safe('finance_book'), safe('architecture_atlas', { op: 'summary' }), safe('protocol_fetch', { carrier: 'a2a' }),
    ]);
    return { schema_version: 'bonfyre.recursive-closure.frontier.v1', host: target, planes: { exact_semantic: exact, material_external: material, latent_model: latent, execution, economic }, architecture: atlas, protocol_lattice: { carrier_state: carrier, rule: 'protocols project the same owned state; they do not become independent truth stores' }, operating_contract: { joined_read_is_not_joined_authority: true, human_submit_remains_effect_boundary: true, observation_is_not_truth: true, cms_save_is_not_publication: true, quote_is_not_payment_or_settlement: true, approximation_cannot_satisfy_exact_constraint: true, recurse_only_from_admitted_delta: true }, forbidden_inference: 'this frontier is preparation context. Each plane remains independently owned and every write/effect still requires its native admission boundary' };
  }

  async function capabilityCircuit(args) {
    const demand = String(args.demand ?? '').trim();
    if (!demand || demand.length > 500) throw new Error('demand must be a short statement of what is wanted');
    const budget = args.budget_usd === undefined ? null : Number(args.budget_usd);
    if (budget !== null && (!Number.isFinite(budget) || budget < 0)) throw new Error('budget_usd must be nonnegative');
    const stop = new Set('the a an of for to and or with by from in on i we need want get find please can you how what show me my our is are be per much have has many most some any all this that these those'.split(' '));
    const terms = (demand.toLowerCase().match(/[a-z0-9]+/g) || []).filter(word => word.length > 2 && !stop.has(word));
    const stem = word => { for (const suffix of ['ies', 'es', 's']) if (word.length > 4 && word.endsWith(suffix)) return word.slice(0, -suffix.length); return word; };
    let catalogue = [];
    try {
      const db = new Database(config.financeDb, { readonly: true, strict: true });
      try { catalogue = db.query('SELECT name, category, sell_price, source FROM services').all().map(row => ({ name: row.name, category: row.category, price_usd: Number(row.sell_price || 0), source: row.source, priced: Number(row.sell_price || 0) > 0 })); }
      finally { db.close(); }
    } catch (error) { catalogue = [{ __error__: String(error).slice(0, 120) }]; }
    let descriptions = {};
    try {
      const response = await request(config.capabilityUrl, { headers: { Accept: 'application/json', Host: 'aurekai.life' }, signal: AbortSignal.timeout(8000) });
      if (response.ok) for (const item of (await response.json()).capabilities || []) if (item.name) descriptions[item.name] = String(item.description || '');
    } catch { /* description sharpens lexical match, not authority */ }
    let organs = [];
    try {
      const names = await readdir(config.organsDir);
      const candidates = names.filter(name => name.startsWith('bonfyre-') && !name.slice(8).includes('.'));
      organs = (await Promise.all(candidates.map(async name => {
        try { const info = await stat(join(config.organsDir, name)); return info.isFile() && (info.mode & 0o111) ? name : null; }
        catch { return null; }
      }))).filter(Boolean).sort();
    } catch { /* no local organs */ }
    const refused = [], matched = [];
    for (const entry of catalogue) {
      if (entry.__error__) { refused.push({ candidate: 'service book', reason: entry.__error__ }); continue; }
      const haystack = [entry.name, entry.category || '', descriptions[entry.name] || ''].join(' ').toLowerCase();
      const stems = new Set((haystack.match(/[a-z0-9]+/g) || []).map(stem));
      const hits = terms.filter(term => haystack.includes(term) || stems.has(stem(term)) || [...stems].some(s => term.length >= 4 && s.startsWith(stem(term))));
      if (!hits.length) continue;
      if (!entry.priced) { refused.push({ candidate: entry.name, reason: 'carries no asking price, so it is not for sale; unpriced is not free' }); continue; }
      if (budget !== null && entry.price_usd > budget) { refused.push({ candidate: entry.name, reason: `costs $${entry.price_usd.toFixed(2)}, over the stated budget of $${budget.toFixed(2)}` }); continue; }
      const strong = [...new Set(hits.filter(term => term.length >= 4))].sort();
      if (!strong.length) continue;
      matched.push({ stage: 'priced_capability', capability: entry.name, price_usd: entry.price_usd, payment: 'x402', resource: `/capability/${entry.name}`, also_reachable_over: 'A2A message/send', matched_terms: strong, score: strong.reduce((n, term) => n + term.length, 0), authority_required: 'a settled x402 payment; discovery is free' });
    }
    const organHits = [];
    for (const name of organs) {
      const root = name.slice(8);
      const hits = terms.filter(term => term === root || (root.length >= 4 && term.length >= 4 && (term.startsWith(root) || root.startsWith(term))));
      if (hits.length) organHits.push({ stage: 'native_organ', organ: name, binary: join(config.organsDir, name), matched_terms: [...new Set(hits)].sort(), authority_required: "the organ's own effect boundary" });
    }
    organHits.sort((a, b) => b.matched_terms.length - a.matched_terms.length || a.organ.localeCompare(b.organ));
    for (const candidate of matched) {
      try {
        const answer = await projectCall('capability_fulfillment', { capability: candidate.capability, exchange: `circuit-${digest(candidate.capability).slice(0, 12)}`, action: 'open_quote' });
        const authority = answer?.authority || {}, granted = new Set(authority.allowed || []), withheld = new Set(authority.not_granted || []);
        const state = String(answer?.state?.stdout || '').split('\n').find(line => line.startsWith('state='))?.slice(6).trim() || 'unknown';
        const route = String(answer?.financial_membrane?.route?.stdout || '').split('\n').filter(line => line.includes('recommended model') || line.includes('route basis')).join('; ');
        const executable = granted.has('execute') && granted.has('deliver');
        candidate.closure = { reachable: true, state, protocol_definition: answer?.protocol_definition, carrier_coordinates: answer?.carrier_coordinates, authority, economy_route: route || null, quote_sha256: answer?.public_mpp_quote?.quote_sha256, executable, blocked_by: executable ? null : `the fulfillment machine grants ${[...granted].sort().join(', ') || 'nothing'} and withholds ${[...withheld].sort().join(', ') || 'nothing'}` };
      } catch (error) { candidate.closure = { reachable: false, state: 'fulfillment_unreachable', detail: String(error).slice(0, 120) }; }
      candidate.executable = Boolean(candidate.closure.executable);
    }
    matched.sort((a, b) => Number(b.executable) - Number(a.executable) || b.score - a.score || a.price_usd - b.price_usd);
    const chosen = matched.slice(0, 1), circuit = [...chosen, ...organHits.slice(0, 4)];
    const blockers = chosen.filter(item => !item.executable).map(item => ({ stage: item.capability, state: item.closure.state, blocked_by: item.closure.blocked_by }));
    const total = Number(chosen.reduce((sum, item) => sum + item.price_usd, 0).toFixed(2));
    return { schema_version: 'bonfyre.capability.circuit.v1', demand, terms, circuit, cost: { total_usd: total, settleable: !blockers.length, budget_usd: budget, within_budget: budget === null ? null : total <= budget }, executable: !blockers.length, blocked_by: blockers, alternatives: matched.slice(1, 4).map(item => ({ capability: item.capability, price_usd: item.price_usd, matched_terms: item.matched_terms, resource: item.resource })), refused, unmatched_terms: terms.filter(term => !circuit.some(stage => JSON.stringify(stage).toLowerCase().includes(term))), surveyed: { priced_capabilities: catalogue.filter(item => item.priced).length, book_entries: catalogue.length, executable_organs: organs.length }, forbidden_inference: 'a compiled circuit is a route, not an authorization, a quote that has been paid, a reservation of the organ, or a claim that running it will succeed; admission is decided by the authority bridge and each effect by the organ that owns it' };
  }

  return {
    async call(name, args = {}, traceId = '') {
      if (name.startsWith('matrix_')) return native(name, args);
      switch (name) {
        case 'recursive_closure_frontier': return recursiveFrontier(args, traceId);
        case 'foreign_twin_observe': return foreignTwinObserve(args);
        case 'foreign_resolution': return foreignResolution(args);
        case 'cms_record': return cmsRecord(args);
        case 'surface_project': return surfaceProject(args);
        case 'capability_circuit_compile': return capabilityCircuit(args);
        default: throw new Error(`unknown recursive overlay tool: ${name}`);
      }
    },
    async read(uri) {
      if (uri === 'bonfyre://closure/frontier') return recursiveFrontier({}, `resource-${randomUUID()}`);
      if (uri === 'bonfyre://closure/operating-contract') return { schema_version: OPERATING_CONTRACT.schema_version, laws: OPERATING_CONTRACT.laws };
      throw new Error(`unknown recursive overlay resource: ${uri}`);
    },
  };
}
