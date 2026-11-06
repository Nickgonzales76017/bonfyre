import { createHash } from 'node:crypto';
import { closeSync, constants, fstatSync, lstatSync, mkdtempSync, openSync, readSync, realpathSync, rmSync, statSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { isAbsolute, join, relative, sep } from 'node:path';
import { Database } from 'bun:sqlite';
import { jsonFloat } from './canonical_json.mjs';

// The installed mission_fabric.py catalog is data, not executable authority.
export const MISSION_TEMPLATES = Object.freeze([
  { id: 'production.lead-intelligence', name: 'Lead Intelligence to Opportunity', kind: 'production', effect_class: 'CANDIDATE_WRITE', apps: ['crm', 'erpnext', 'insights', 'wiki', 'drive'], capabilities: ['web.crawl', 'model.generate', 'graph.compute', 'content.cms'], providers: ['firecrawl', 'openai', 'anthropic', 'google-ai'] },
  { id: 'production.loss-audit', name: 'Loss Database Duplicate and Payment Audit', kind: 'production', effect_class: 'READ_ONLY', apps: ['erpnext', 'insights', 'drive', 'wiki'], capabilities: ['database.postgres', 'database.libsql', 'graph.compute', 'proof'], providers: ['supabase', 'neon', 'turso'] },
  { id: 'production.support-resolution', name: 'Evidence-Grounded Support Resolution', kind: 'production', effect_class: 'CANDIDATE_WRITE', apps: ['helpdesk', 'wiki', 'drive', 'crm'], capabilities: ['model.generate', 'search', 'workflow.execute'], providers: ['openai', 'anthropic', 'cohere', 'typesense'] },
  { id: 'production.opportunity-delivery', name: 'Opportunity to Delivery Capacity Closure', kind: 'production', effect_class: 'CANDIDATE_WRITE', apps: ['crm', 'erpnext', 'hrms', 'lms', 'wiki', 'drive', 'insights'], capabilities: ['graph.compute', 'workflow.execute', 'model.generate'], providers: ['openai', 'anthropic', 'google-ai'] },
  { id: 'production.shoutout', name: 'SHOUTOUT Live Campaign and Media Dispatch', kind: 'production', effect_class: 'EXTERNAL_REVERSIBLE_EFFECT', apps: ['crm', 'erpnext', 'drive', 'insights'], capabilities: ['media.process', 'communication.chat', 'deployment'], providers: ['pexels', 'pixabay', 'giphy', 'twilio', 'sendgrid', 'cloudflare'] },
  { id: 'production.model-router', name: 'Measured Model and Kernel Auction', kind: 'production', effect_class: 'READ_ONLY', apps: ['insights'], capabilities: ['model.generate', 'model.embed', 'model.registry', 'gpu.compute'], providers: ['openai', 'anthropic', 'google-ai', 'mistral', 'groq', 'cohere', 'hugging-face', 'runpod'] },
  { id: 'production.cloud-drift', name: 'Cloud Fleet Drift and Cost Boundary', kind: 'production', effect_class: 'READ_ONLY', apps: ['insights', 'helpdesk', 'wiki'], capabilities: ['cloud.aws', 'cloud.oracle', 'cloud.cloudflare', 'cloud.fly', 'deployment'], providers: ['aws', 'oracle', 'cloudflare', 'fly', 'vercel', 'railway', 'render'] },
  { id: 'production.knowledge-successor', name: 'Institutional Knowledge Successor', kind: 'production', effect_class: 'CANDIDATE_WRITE', apps: ['wiki', 'drive', 'lms', 'helpdesk'], capabilities: ['web.crawl', 'model.generate', 'vector.search', 'proof'], providers: ['firecrawl', 'openai', 'anthropic', 'cohere', 'hugging-face'] },
  { id: 'production.sparse-estate-deploy', name: 'Sparse Estate Deployment', kind: 'production', effect_class: 'AUTHORITATIVE_INTERNAL_WRITE', apps: ['frappe', 'erpnext', 'crm', 'hrms', 'lms', 'helpdesk', 'insights', 'wiki', 'drive'], capabilities: ['deployment.bfp', 'deployment.delta', 'deployment.oracle'], providers: ['oracle', 'cloudflare', 'aws'] },
  { id: 'production.provider-credit', name: 'Provider Credit and Quota Reconciliation', kind: 'production', effect_class: 'READ_ONLY', apps: ['insights', 'erpnext'], capabilities: ['provider.credit', 'telemetry.product'], providers: ['aws', 'oracle', 'cloudflare', 'fly', 'vercel', 'render', 'railway', 'supabase', 'neon', 'turso'] },
].map(value => Object.freeze({ ...value, apps: Object.freeze(value.apps), capabilities: Object.freeze(value.capabilities), providers: Object.freeze(value.providers) })));

const defaults = Object.freeze({
  estate: { apps: [], commands: [], doctypes: [] }, resources: { resources: [], summary: {} },
  providers: { results: [], summary: {} }, backends: { backends: [], summary: {} },
  graph: { nodes: [], edges: [] }, sparse: { files: [], bundles: [], categories: {} },
  history: { total_receipts: 0, sources: {} },
});
const MAX_DATABASE_BYTES = 128 * 1024 * 1024;
function object(value, label) { if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error(`invalid ${label}`); return value; }
function array(value, label) { if (!Array.isArray(value)) throw new Error(`invalid ${label}`); return value; }
function readOptional(store, path, fallback) {
  try { return object(store.readJson(join(store.home, path)), path); }
  catch (error) { if (error?.code === 'ENOENT') return fallback; throw error; }
}
function loadInputs(store) {
  return {
    estate: readOptional(store, 'estate/estate.json', defaults.estate),
    resources: readOptional(store, 'resources/resource-graph.json', defaults.resources),
    providers: readOptional(store, 'resources/provider-activation.json', defaults.providers),
    backends: readOptional(store, 'backends/backend-fabric.json', defaults.backends),
    graph: readOptional(store, 'graphs/workgraph-v61.json', defaults.graph),
    sparse: readOptional(store, 'sparse/snapshot.json', defaults.sparse),
    history: readOptional(store, 'history/federated-history.json', defaults.history),
  };
}
function fileDigest(path) {
  const fd = openSync(path, 'r');
  try {
    const digest = createHash('sha256'), buffer = Buffer.allocUnsafe(4 * 1024 * 1024);
    let position = 0;
    for (;;) { const read = readSync(fd, buffer, 0, buffer.length, position); if (!read) break; digest.update(buffer.subarray(0, read)); position += read; }
    if (fstatSync(fd).size !== position) throw new Error('database changed during digest');
    return digest.digest('hex');
  } finally { closeSync(fd); }
}
function within(root, path) {
  const rel = relative(root, path);
  return rel === '' || (rel !== '..' && !rel.startsWith(`..${sep}`) && !isAbsolute(rel));
}
function sqliteIntegrity(path, roots) {
  const start = performance.now(); let db, fd, temporary;
  try {
    const actual = realpathSync(path);
    if (!roots.some(root => within(root, actual))) throw new Error('database path outside admitted roots');
    fd = openSync(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
    const stats = fstatSync(fd);
    if (!stats.isFile() || !Number.isSafeInteger(stats.size) || stats.size > MAX_DATABASE_BYTES)
      throw new Error('database is not a bounded regular file');
    let journalBytes = 0;
    for (const suffix of ['-wal', '-shm']) {
      let journal;
      try { journal = lstatSync(`${actual}${suffix}`); }
      catch (error) { if (error?.code !== 'ENOENT') throw error; }
      if (journal) {
        if (!journal.isFile() || journal.isSymbolicLink()) throw new Error('database journal is not a regular file');
        journalBytes += journal.size;
      }
    }
    if (stats.size + journalBytes > MAX_DATABASE_BYTES) throw new Error('database and journal budget exceeded');
    db = new Database(actual, { readonly: true, strict: true });
    db.exec('PRAGMA busy_timeout=5000;');
    const pages = Object.values(db.query('PRAGMA page_count').get())[0];
    const pageSize = Object.values(db.query('PRAGMA page_size').get())[0];
    if (!Number.isSafeInteger(pages) || !Number.isSafeInteger(pageSize) ||
      !Number.isSafeInteger(pages * pageSize) || pages * pageSize > MAX_DATABASE_BYTES)
      throw new Error('database logical size budget exceeded');
    temporary = mkdtempSync(join(tmpdir(), 'bonfyre-v6-sqlite-check-'));
    const snapshot = join(temporary, 'snapshot.sqlite3');
    db.exec(`VACUUM INTO '${snapshot.replaceAll("'", "''")}'`);
    db.close(); db = undefined;
    const after = statSync(path), checked = statSync(snapshot);
    if (after.dev !== stats.dev || after.ino !== stats.ino || after.size !== stats.size ||
      after.mtimeMs !== stats.mtimeMs || after.ctimeMs !== stats.ctimeMs) throw new Error('database source changed during check');
    if (!checked.isFile() || checked.size > MAX_DATABASE_BYTES) throw new Error('database snapshot budget exceeded');
    db = new Database(snapshot, { readonly: true, strict: true });
    const checks = db.query('PRAGMA quick_check(100)').all().map(row => Object.values(row)[0]);
    const quickCheck = checks.length === 1 ? checks[0] : checks;
    const views = db.query("SELECT COUNT(*) AS count FROM sqlite_master WHERE type IN ('table','view')").get().count;
    db.close(); db = undefined;
    return { path, ok: quickCheck === 'ok', quick_check: quickCheck ?? null, tables_and_views: views,
      bytes: checked.size, sha256: fileDigest(snapshot), digest_basis: 'sqlite_vacuum_snapshot', source_bytes: stats.size,
      duration_ms: Math.round(performance.now() - start) };
  } catch (error) { return { path, ok: false, error: `${error.name}: ${error.message}`, duration_ms: Math.round(performance.now() - start) }; }
  finally { db?.close(); if (fd !== undefined) closeSync(fd); if (temporary) rmSync(temporary, { recursive: true, force: true }); }
}
function countedBytes(items) {
  return items.reduce((sum, item) => {
    if (!Number.isSafeInteger(item?.bytes) || item.bytes < 0 || !Number.isSafeInteger(sum + item.bytes)) throw new Error('invalid sparse byte count');
    return sum + item.bytes;
  }, 0);
}
const evidence = (raw, states) => Object.fromEntries(raw.providers.map(name => [name, states.get(name)?.state ?? null]));

export async function missionWorkload(raw, store, inputs, available, bfp) {
  const { resources, providers, backends, graph, sparse } = inputs;
  const nodes = array(graph.nodes, 'graph nodes'), edges = array(graph.edges, 'graph edges');
  const files = array(sparse.files, 'sparse files'), bundles = array(sparse.bundles, 'sparse bundles');
  const providerStates = new Map(array(providers.results, 'provider results').map(item => [object(item, 'provider result').provider, item]));
  const common = { mission_id: raw.id, effect_class: raw.effect_class, available_providers: available,
    app_count: raw.apps.length, generated_at: jsonFloat(store.now()), external_effect_executed: false };
  let output;
  switch (raw.id) {
    case 'production.lead-intelligence': {
      const connectors = array(resources.resources, 'resources').filter(item => item?.kind === 'aurekai-integration' &&
        ['firecrawl', 'posthog', 'typesense', 'webhook'].some(token => String(item.provider ?? '').includes(token)))
        .map(item => item.provider).sort();
      output = { ...common, artifact: 'LeadOpportunityCandidateSet', source_connectors: connectors,
        provider_evidence: evidence(raw, providerStates), application_projections: ['crm:Lead', 'crm:Opportunity', 'insights:LeadQuality', 'wiki:SourcePolicy', 'drive:EvidenceBundle'], candidate_only: true,
        passed: Boolean(available.length || connectors.length) }; break;
    }
    case 'production.loss-audit': {
      const paths = files.filter(item => item?.category === 'databases' && typeof item.path === 'string' && /\.(sqlite|sqlite3|db)$/i.test(item.path)).map(item => item.path);
      const roots = [store.root, store.home].map(root => realpathSync(root));
      const checks = paths.slice(0, 32).map(path => sqliteIntegrity(path, roots));
      output = { ...common, artifact: 'DatabaseAuditSnapshot', databases_discovered: paths.length,
        integrity_checks: checks, configured_database_providers: available, read_only: true,
        passed: paths.length > 0 && paths.length <= 32 && checks.length === paths.length && checks.every(item => item.ok) }; break;
    }
    case 'production.support-resolution': {
      const relevant = new Set(raw.apps), selected = nodes.filter(item => relevant.has(item?.app) || relevant.has(String(item?.id ?? '').replace(/^app:/, '')));
      const ids = new Set(selected.map(item => item.id));
      const closure = edges.filter(item => ids.has(item?.source) || ids.has(item?.target));
      output = { ...common, artifact: 'SupportResolutionContext', knowledge_nodes: selected.length,
        dependency_edges: closure.length, provider_evidence: evidence(raw, providerStates), candidate_reply_only: true,
        passed: Boolean(selected.length && closure.length && available.length) }; break;
    }
    case 'production.opportunity-delivery': {
      const apps = [...raw.apps].sort(), ids = new Set(apps.map(name => `app:${name}`));
      const cross = edges.filter(item => ids.has(item?.source) || ids.has(item?.target) || ['LINKS_TO_DOCTYPE', 'CROSS_APP_DEPENDENCY'].includes(item?.kind));
      output = { ...common, artifact: 'OpportunityDeliveryClosure', applications: apps,
        cross_application_edges: cross.length, provider_evidence: evidence(raw, providerStates), candidate_plan_only: true,
        passed: cross.length > 0 }; break;
    }
    case 'production.shoutout': {
      const mediaFiles = files.filter(item => item?.category === 'media'), mediaBundles = bundles.filter(item => item?.category === 'media');
      const media = available.filter(name => ['pexels', 'pixabay', 'giphy', 'cloudflare'].includes(name));
      const dispatch = available.filter(name => ['twilio', 'sendgrid', 'cloudflare'].includes(name));
      output = { ...common, artifact: 'ShoutoutCampaignCandidate', media_files: mediaFiles.length,
        media_bundle_bytes: countedBytes(mediaBundles), media_providers: media, dispatch_providers: dispatch,
        dispatch_executed: false, candidate_only: true, passed: Boolean(mediaFiles.length && media.length) }; break;
    }
    case 'production.model-router': {
      const models = files.filter(item => item?.category === 'models');
      const active = array(backends.backends, 'backends').filter(item => item?.state === 'active').map(item => item.id);
      const usable = backends.backends.filter(item => ['active', 'source-backed'].includes(item?.state)).map(item => item.id);
      output = { ...common, artifact: 'ModelKernelAuctionSnapshot', model_files: models.length,
        model_bytes: countedBytes(models), active_backends: active, usable_backends: usable,
        configured_model_providers: available, selection_executed: false,
        passed: Boolean(usable.length && (models.length || available.length)) }; break;
    }
    case 'production.cloud-drift': {
      const drift = raw.providers.map(name => { const value = providerStates.get(name) ?? {}; return { provider: name,
        state: value.state ?? 'missing', http_status: value.http_probe?.status ?? null, cli_probe_ok: value.cli_probe_ok ?? false }; });
      output = { ...common, artifact: 'CloudFleetDriftSnapshot', providers: drift,
        configured_or_reachable: available, mutations_executed: false, passed: available.length > 0 }; break;
    }
    case 'production.knowledge-successor': {
      const knowledge = new Set(['wiki', 'drive', 'lms', 'helpdesk']);
      const selected = nodes.filter(item => knowledge.has(item?.app) || knowledge.has(String(item?.id ?? '').replace(/^app:/, '')) ||
        ['integration', 'resource', 'aurekai-integration', 'external-provider'].includes(item?.kind));
      const lineage = selected.map(item => {
        if (typeof item.id !== 'string' || typeof item.kind !== 'string') throw new Error('knowledge node identity missing');
        return [item.id, item.kind];
      }).sort((a, b) => a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : a[1] < b[1] ? -1 : a[1] > b[1] ? 1 : 0);
      output = { ...common, artifact: 'KnowledgeSuccessorCandidate', source_nodes: selected.length,
        source_digest: store.digest(lineage), provider_evidence: evidence(raw, providerStates), promotion_state: 'candidate',
        passed: Boolean(selected.length && available.length) }; break;
    }
    case 'production.sparse-estate-deploy': {
      const candidate = join(store.home, 'bfp/bonfyre-v6.bfp'); let verified = false, sectionCount = 0;
      try { const result = await bfp.verify(candidate); verified = result.verified === true; sectionCount = result.reconstructed?.length ?? 0; }
      catch (error) { if (error?.code !== 'ENOENT') throw error; }
      output = { ...common, artifact: 'SparseDeploymentPreflight', base_bfp: candidate,
        base_bfp_verified: verified, base_bfp_sections: sectionCount, payload_categories: Object.keys(object(sparse.categories, 'sparse categories')).sort(),
        oracle_configured: available.includes('oracle'), activation_executed: false,
        passed: verified && available.includes('oracle') }; break;
    }
    case 'production.provider-credit': {
      const facts = available.map(name => ({ provider: name, state: providerStates.get(name)?.state ?? 'configured',
        measured_credit: null, measured_quota: null, synthetic_value: false }));
      output = { ...common, artifact: 'ProviderCreditFactSet', provider_facts: facts,
        financial_value_accepted: false, source_documented_only: true, passed: facts.length > 0 }; break;
    }
    default: throw new Error(`unknown mission: ${raw.id}`);
  }
  return { ...output, digest: store.digest(output) };
}

function validatedProbe(value, kind, mission, intent, store, workload = null, compileProof = null) {
  if (!value || typeof value !== 'object' || value.executed !== true || value.verified !== true ||
    value.external_effect_executed !== false || (kind === 'canary' && value.read_only_local !== true) ||
    value.mission_id !== mission.id || value.effect_class !== mission.effect_class ||
    value.intent_digest !== intent.digest || !/^[a-f0-9]{64}$/.test(value.evidence_digest) ||
    !value.evidence || typeof value.evidence !== 'object' || Array.isArray(value.evidence) ||
    value.evidence.external_effect_executed !== false ||
    store.digest(value.evidence) !== value.evidence_digest ||
    (workload && (value.workload_digest !== workload.digest || value.compile_evidence_digest !== compileProof?.evidence_digest)))
    throw new Error(`invalid ${kind} proof for ${mission.id}`);
  return value;
}
const unavailable = (kind, reason) => ({ state: 'not_executed', kind, reason, executed: false, verified: false });

export function createV6Missions({ store, bfp, compileMission = null, canaryMission = null, checkStop = () => {} } = {}) {
  if (!store?.home || !store?.digest || !store?.receipt || !store?.missionRun || !store?.upsertMission || !bfp?.verify)
    throw new Error('V6 missions need owned store and BFP verifier');
  const stopped = async () => { await checkStop(); };
  async function activate({ compile = true } = {}) {
    if (typeof compile !== 'boolean') throw new Error('compile must be boolean');
    await stopped();
    const inputs = loadInputs(store), apps = new Set(array(inputs.estate.apps, 'estate apps'));
    const providerStates = new Map(array(inputs.resources.resources, 'resources').filter(item => item?.kind === 'external-provider').map(item => [item.provider, item.state]));
    const compiled = [], runs = [], catalogMissions = [];
    for (const raw of MISSION_TEMPLATES) {
      await stopped();
      const available = raw.providers.filter(name => ['configured', 'reachable', 'validated'].includes(providerStates.get(name)));
      const prompt = `${raw.name}. Compile as a governed ${raw.effect_class} production mission across ${raw.apps.join(', ')}; preserve proof, authority, rollback and measured outcome boundaries; execute only the read-only/local canary now and leave external effects gated.`;
      const intent = store.receipt('v6.1.mission-activation.intent', 'started', { mission_id: raw.id,
        effect_class: raw.effect_class, compile_requested: compile, prompt_digest: store.digest(prompt) });
      let compileProbe = unavailable('compile', compile ? 'native_compiler_unavailable' : 'compile_disabled');
      if (compile && compileMission) {
        await stopped();
        try { compileProbe = validatedProbe(await compileMission({ mission: raw, prompt, intent }), 'compile', raw, intent, store); }
        catch (error) { compileProbe = { state: 'outcome_uncertain', kind: 'compile', reason: `${error.name}: ${error.message}`,
          executed: true, verified: false }; }
      }
      store.receipt('v6.1.mission-compile.result', compileProbe.state === 'outcome_uncertain' ? 'outcome_uncertain' :
        compileProbe.verified ? 'passed' : 'not_executed', { mission_id: raw.id, intent_digest: intent.digest,
        evidence_digest: compileProbe.evidence_digest ?? null, reason: compileProbe.reason ?? null });
      if (compileProbe.state === 'outcome_uncertain') throw new Error(`mission ${raw.id} has an uncertain compiler outcome; reconcile before retry`);
      await stopped();
      const workload = await missionWorkload(raw, store, inputs, available, bfp);
      await stopped();
      const outputPath = join(store.home, 'missions/outputs', `${raw.id.replaceAll('.', '-')}.json`);
      store.writeJson(outputPath, workload);
      let canary = unavailable('canary', compile ? 'native_canary_unavailable' : 'compile_disabled');
      if (compile && compileProbe.verified && canaryMission) {
        await stopped();
        try { canary = validatedProbe(await canaryMission({ mission: raw, workload, compileProof: compileProbe, intent }),
          'canary', raw, intent, store, workload, compileProbe); }
        catch (error) { canary = { state: 'outcome_uncertain', kind: 'canary', reason: `${error.name}: ${error.message}`,
          executed: true, verified: false }; }
      }
      store.receipt('v6.1.mission-canary.result', canary.state === 'outcome_uncertain' ? 'outcome_uncertain' :
        canary.verified ? 'passed' : 'not_executed', { mission_id: raw.id, intent_digest: intent.digest,
        workload_digest: workload.digest, evidence_digest: canary.evidence_digest ?? null, reason: canary.reason ?? null });
      if (canary.state === 'outcome_uncertain') throw new Error(`mission ${raw.id} has an uncertain canary outcome; reconcile before retry`);
      await stopped();
      const appsReady = raw.apps.every(app => apps.has(app));
      const passed = compileProbe.verified === true && appsReady && workload.passed === true && canary.verified === true;
      const body = { ...raw, state: passed ? 'active' : 'candidate', version: '6.1.0', created_at: jsonFloat(store.now()),
        available_providers: available, output_artifact: outputPath, output_digest: workload.digest,
        activation: { compiled: compileProbe.verified === true, apps_ready: appsReady, provider_ready: available.length > 0,
          workload_passed: workload.passed, canary_passed: canary.verified === true, external_effect_executed: false } };
      const document = { ...body, digest: store.digest(body) };
      store.upsertMission(document); catalogMissions.push(document);
      compiled.push({ mission_id: raw.id, probe: compileProbe, compiled: compileProbe.verified === true });
      runs.push(store.missionRun(raw.id, passed ? 'passed' : 'failed', { command_canary: canary, workload,
        available_providers: available, apps_ready: appsReady, activation_intent: intent.digest }, store.now()));
      store.receipt('v6.1.mission-activation.result', passed ? 'passed' : 'candidate', { mission_id: raw.id,
        intent_digest: intent.digest, document_digest: document.digest, run_digest: runs.at(-1).digest });
    }
    await stopped();
    const body = { schema: 'bonfyre.mission-catalog.v2', generated_at: jsonFloat(store.now()), missions: catalogMissions,
      hypervisor_compilation: compiled, canary_runs: runs, output_artifacts: catalogMissions.map(item => item.output_artifact) };
    const catalog = { ...body, digest: store.digest(body) };
    store.writeJson(join(store.home, 'missions/catalog.json'), catalog);
    const summary = { cataloged: MISSION_TEMPLATES.length, active: catalogMissions.filter(item => item.state === 'active').length,
      candidate: catalogMissions.filter(item => item.state === 'candidate').length,
      hypervisor_compiled: compiled.filter(item => item.compiled).length, canary_runs_passed: runs.filter(item => item.state === 'passed').length,
      workload_artifacts: catalogMissions.length };
    store.setMeta('mission_summary', summary);
    store.receipt('v6.1.mission-activation', summary.active === MISSION_TEMPLATES.length ? 'passed' : 'failed', summary);
    return { catalog, safe_runs: runs, summary };
  }
  return { activate, templates: MISSION_TEMPLATES };
}
