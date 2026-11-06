#!/usr/bin/env bun
import { existsSync, readFileSync, readdirSync, realpathSync } from 'node:fs';
import { homedir } from 'node:os';
import { join, resolve, sep } from 'node:path';
import { createMcpTransport } from './http_boundary.mjs';
import { createV6Store, V6_VERSION } from './v6_store.mjs';
import { createOwnedWorkerPool } from './owned_worker_pool.mjs';
import { createV6HypervisorBridge } from './v6_hypervisor_bridge.mjs';
import { V6_ACTIONS, V6_READ_MODELS, V6_TOOLS } from './v6_catalog.mjs';
export { V6_TOOLS } from './v6_catalog.mjs';
const MAX_BYTES = 20_000_000;
const escape = value => String(value ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[c]);

export function v6Config(env = process.env, userHome = homedir()) {
  const token = readFileSync(env.BONFYRE_MCP_TOKEN_FILE || join(userHome, '.bonfyre/mcp/token'), 'utf8').trim();
  const port = Number(env.BONFYRE_V6_PORT || 3058), host = env.BONFYRE_V6_HOST || '127.0.0.1';
  if (!token || !Number.isSafeInteger(port) || port < 1 || port > 65535 || !['127.0.0.1', '::1'].includes(host)) throw new Error('invalid V6 listener configuration');
  return { token, port, host, env, userHome, home: resolve(env.BONFYRE_V6_HOME || join(userHome, '.bonfyre/agent-hypervisor/recursive-hypervisor-v6')),
    hypervisorHome: resolve(env.BONFYRE_HYPERVISOR_HOME || join(userHome, '.bonfyre/agent-hypervisor')),
    registryPath: env.BONFYRE_ECOSYSTEM_REGISTRY, hypervisorDbPath: env.BONFYRE_HYPERVISOR_DB,
    root: realpathSync(env.BONFYRE_PROJECT_ROOT || env.BONFYRE_ROOT || join(userHome, 'Documents/Bonfyre')) };
}

export async function createV6Server({ token, store, resources, history, missions, bfp, stopPaths, maxConcurrent = 16, ...options } = {}) {
  store ||= createV6Store(options);
  stopPaths ||= [join(store.home, 'STOP'), join(options.userHome || homedir(), '.bonfyre/operator/STOP')];
  const stopped = () => stopPaths.some(existsSync);
  const checkStop = () => { if (stopped()) throw new Error('STOP active; V6 mutation refused'); };
  // Engines share one store. All durability transitions recheck STOP, including
  // synchronous work between async boundaries; constructor/import is effect-free.
  const writes = new Set(['writeJson', 'setMeta', 'receipt', 'upsertResource', 'upsertBackend', 'upsertMission', 'missionRun']);
  const guarded = new Proxy(store, { get(target, key) {
    const value = target[key];
    return writes.has(key) ? (...args) => { checkStop(); return value(...args); } : value;
  } });
  let verificationPool;
  if (!bfp) {
    verificationPool = createOwnedWorkerPool({ workerUrl: new URL('./v6_bfp_worker.mjs', import.meta.url), maxWorkers: 2, maxBytes: MAX_BYTES });
    bfp = { verify: path => verificationPool.run({ operation: 'verify', root: store.root, home: store.home, path: resolve(path) }) };
  }
  resources ||= (await import('./v6_resources.mjs')).createV6Resources({ store: guarded, root: store.root, home: store.home, env: store.env, run: store.run, now: store.now });
  history ||= (await import('./v6_history.mjs')).createV6History(guarded, { projectRoot: store.root, home: store.home, userHome: options.userHome, checkStop });
  let compilerBridge;
  if (!missions) {
    compilerBridge = createV6HypervisorBridge({ v6Store: guarded, checkStop,
      hypervisorHome: options.hypervisorHome || store.env?.BONFYRE_HYPERVISOR_HOME || join(options.userHome || homedir(), '.bonfyre/agent-hypervisor'),
      registryPath: options.registryPath || store.env?.BONFYRE_ECOSYSTEM_REGISTRY,
      hypervisorDbPath: options.hypervisorDbPath || store.env?.BONFYRE_HYPERVISOR_DB });
    missions = (await import('./v6_missions.mjs')).createV6Missions({ store: guarded, bfp, checkStop, compileMission: compilerBridge.compileMission });
  }
  const engines = { resources, history, missions };
  const read = relative => {
    const path = store.ownedPath(join(store.home, relative));
    return existsSync(path) ? store.readJson(path) : { state: 'missing', path };
  };
  const latest = () => {
    const path = store.ownedPath(join(store.home, 'proof'));
    if (!existsSync(path)) return { state: 'missing' };
    const names = readdirSync(path); if (names.length > 10000) throw new Error('proof directory budget exceeded');
    const selected = names.filter(name => /^v6\.1-activation-[0-9]+\.json$/.test(name)).sort().at(-1);
    return selected ? read(`proof/${selected}`) : { state: 'missing' };
  };
  async function verify() {
    const receipts = store.verifyReceipts(), failures = [];
    let activationMarker = null, proof = null, archive = null;
    try {
      activationMarker = read('V61_ACTIVATION_COMPLETE.json');
      if (activationMarker.state === 'missing') throw new Error('activation marker missing');
      if (activationMarker.version !== V6_VERSION || typeof activationMarker.proof !== 'string') throw new Error('activation marker contract');
      const proofPath = store.ownedPath(activationMarker.proof);
      if (!proofPath.startsWith(join(store.home, 'proof') + sep)) throw new Error('activation proof outside proof owner');
      const document = store.parseDocument(store.readText(proofPath), ['final_digest', 'receipt']); proof = document.value;
      if (proof.schema !== 'bonfyre.v6.1-activation-proof.v1' || proof.version !== V6_VERSION || proof.state !== 'passed'
          || document.calculated !== proof.final_digest || document.calculated !== activationMarker.proof_digest) throw new Error('activation proof identity');
      if (!Array.isArray(proof.mandatory_checks) || !proof.mandatory_checks.length || proof.mandatory_checks.some(key => typeof key !== 'string' || proof.checks?.[key] !== true)) throw new Error('activation mandatory checks');
      const receipt = store.db.query('SELECT * FROM receipts WHERE id=?').get(proof.receipt?.receipt_id ?? '');
      const payload = receipt ? JSON.parse(receipt.payload_json) : null;
      if (!receipt || receipt.state !== 'passed' || receipt.kind !== 'v6.1.capability-activation' || receipt.digest !== proof.receipt?.digest
          || payload.final_digest !== proof.final_digest || payload.bfp_sha256 !== activationMarker.bfp_sha256) throw new Error('activation receipt binding');
      archive = await bfp.verify(join(store.home, 'bfp/bonfyre-v6.1.bfp'));
      if (archive.verified !== true || archive.sha256 !== activationMarker.bfp_sha256) throw new Error('activation BFP binding');
    } catch (error) { failures.push(String(error.message).slice(0, 1000)); }
    return { verified: receipts.verified && failures.length === 0, version: V6_VERSION, receipts,
      activation_marker: activationMarker, proof_digest: proof?.final_digest ?? null, bfp: archive, failures,
      evidence_scope: 'Bound historical activation records and resident archive integrity, not current provider availability, remote deployment health, or mission re-execution.' };
  }
  const dispatch = {
    ...Object.fromEntries(Object.entries(V6_READ_MODELS).map(([name, path]) => [`v6_${name}`, () => read(path)])),
    v6_status: () => store.status(), v6_verify: verify, v6_proof: latest,
    v6_outcomes: () => ({ status: store.status(), ...Object.fromEntries([
      ['mission_summary', 'mission_summary'], ['resource_summary', 'resource_graph_summary'], ['provider_activation', 'provider_activation_summary'],
      ['backend_summary', 'backend_summary'], ['sparse_summary', 'sparse_summary'], ['history', 'history_summary'],
    ].map(([name, key]) => [name, store.getMeta(key, {})])) }),
    r8_bfp_verify: args => bfp.verify(args.path || join(store.home, 'bfp/bonfyre-v6.1.bfp')),
  };
  let observation;
  for (const [name, [engine, method]] of Object.entries(V6_ACTIONS)) dispatch[name] = async args => {
    checkStop(); if (args.confirm !== true) throw new Error('explicit confirm=true required');
    const { confirm: _confirm, ...parameters } = args;
    const attempt = store.receipt(`${name}.attempt`, 'started', { arguments_digest: store.digest(parameters), automatic_retry: false });
    const controller = new AbortController(), timer = setInterval(() => { if (stopped()) controller.abort(new Error('STOP active')); }, 100);
    timer.unref();
    try {
      const result = await engines[engine][method]({ ...parameters, signal: controller.signal }); checkStop();
      store.receipt(`${name}.complete`, 'completed', { attempt_receipt_id: attempt.receipt_id, result_digest: store.digest(result) });
      return result;
    } catch (error) {
      // Recording an uncertain attempt is bookkeeping, not another execution.
      try { store.receipt(`${name}.uncertain`, 'uncertain', { attempt_receipt_id: attempt.receipt_id, reconcile_before_retry: true }); } catch { /* Started record remains. */ }
      throw new Error(`reconcile V6 attempt ${attempt.receipt_id} before retry: ${error.message}`);
    } finally { clearInterval(timer); observation = null; }
  };
  const dashboardData = () => ({ status: store.status(), missions: read(V6_READ_MODELS.missions), backends: read(V6_READ_MODELS.backends) });
  const render = data => {
    const table = (rows, fields) => `<table><tr>${fields.map(key => `<th>${escape(key)}</th>`).join('')}</tr>${rows.map(row => `<tr>${fields.map(key => `<td>${escape(row[key])}</td>`).join('')}</tr>`).join('')}</table>`;
    return `<!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width"><title>Bonfyre V6</title><style>body{font:16px system-ui;margin:2rem}td,th{padding:.5rem;text-align:left}table{border-collapse:collapse}td{border-bottom:1px solid #888}</style><h1>Bonfyre ${V6_VERSION} Capability Fabric</h1><p>Historical state and candidate evidence; no automatic authority.</p><p>Receipts: ${escape(data.status.receipts.receipts)}; verified: ${escape(data.status.receipts.verified)}</p><h2>Missions</h2>${table(data.missions.missions || [], ['name', 'state', 'effect_class'])}<h2>Backends</h2>${table(data.backends.backends || [], ['id', 'state', 'execution_mode'])}`;
  };
  const transport = createMcpTransport({ token, label: 'v6', service: 'bonfyre-recursive-hypervisor-v6', version: V6_VERSION,
    tools: V6_TOOLS, dispatch, statusTool: 'v6_status', maxConcurrent, maxBytes: MAX_BYTES,
    instructions: 'Representations and historical activation are evidence, not current remote truth or effect authority. Explicitly confirmed writes remain STOP-gated; reconcile uncertain attempts before retry.',
    routes: ['/', '/dashboard'].map(path => ({ method: 'GET', path, run: dashboardData, render })),
    health: () => {
      if (!observation || Date.now() - observation.checked_at_ms >= 1000) observation = { receipts_verified: store.verifyReceipts().verified, checked_at_ms: Date.now() };
      return { ...observation, integrity_observation_max_age_ms: 1000, paused: stopped() };
    } });
  return { ...transport, close: async () => { await verificationPool?.close(); compilerBridge?.close(); store.close(); } };
}
if (import.meta.main) {
  const config = v6Config(), owner = await createV6Server(config);
  const server = Bun.serve({ hostname: config.host, port: config.port, fetch: owner.handle, maxRequestBodySize: MAX_BYTES, idleTimeout: 0 });
  console.log(`V6 MCP listening on ${server.hostname}:${server.port} runtime=bun`);
  for (const signal of ['SIGINT', 'SIGTERM']) process.once(signal, async () => { await server.stop(); await owner.close(); process.exit(0); });
}
