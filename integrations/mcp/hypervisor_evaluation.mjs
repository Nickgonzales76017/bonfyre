import { createHash } from 'node:crypto';
import { existsSync, mkdtempSync, realpathSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { canonicalJson, compareUnicode, parsedDocument } from './canonical_json.mjs';
import { inspectCanaryProviderExecutable } from './canary_fixtures.mjs';
import { CERT_SCHEMA } from './canary_store.mjs';
import { createNativeProbe, findProbeExecutable, nativeExecutionPlan, nativeProbePassed, nativeProbeReceipt } from './native_probe.mjs';
import { projectRun } from './project_runtime.mjs';
import { readSpinesBytes, spinesOwnedPath } from './spines_store.mjs';

const SHA = bytes => createHash('sha256').update(bytes).digest('hex');
const PROBES = Object.freeze({
  'bonfyre-recursive-hypervisor-v6': [['bonfyre-v6', '--version'], ['bonfyre-v6', 'status'], ['bonfyre-v6', 'verify']],
  'bonfyre-native-spines-r7': [['bonfyre-spines', '--version'], ['bonfyre-spines', 'status'], ['bonfyre-spines', 'verify']],
  caveman: [['bonfyre-caveman-provider', 'status'], ['bonfyre-caveman-provider', '--version']],
  cavekit: [['bonfyre-cavekit-provider', 'status'], ['bonfyre-cavekit-provider', '--version']],
  'bonfyre-native': [['bonfyre-dev', 'status'], ['bonfyre-estate', 'status']],
  'bonfyre-recursive-substrate': [['bonfyre-recursive', '--version'], ['bonfyre-recursive', 'status'], ['bonfyre-token-fabric', 'status']],
  cavemem: [['cavemem', 'status'], ['cavemem', 'doctor']], rtk: [['rtk', '--version'], ['rtk', 'gain']],
  bernstein: [['bonfyre-bernstein-provider', 'status'], ['bernstein', '--version'], ['bernstein', 'workflow', 'list']],
  'agent-deck': [['bonfyre-agent-deck-provider', 'status'], ['agent-deck', '--version']],
  'claude-squad': [['bonfyre-claude-squad-provider', 'status'], ['claude-squad', 'version'], ['cs', 'version']],
  'caveman-code': [['caveman-code', '--version']], 'claude-code': [['claude', '--version']],
  codex: [['codex', '--version'], ['codex', 'mcp', 'list']], invoker: [['invoker-cli', '--version']],
});
const WEIGHTS = Object.freeze({ reachable: .15, self_check: .10, behavior: .25, native_state: .10,
  isolation: .10, replay: .10, evidence: .08, performance: .07, license: .05 });
const TRAITS = Object.freeze({
  'bonfyre-native': ['isolation', 'replay', 'evidence', 'native_state'],
  'bonfyre-recursive-substrate': ['isolation', 'replay', 'evidence', 'native_state'],
  cavemem: ['replay', 'native_state'], cavekit: ['evidence'], 'caveman-code': ['isolation', 'replay', 'native_state'],
  bernstein: ['isolation', 'replay', 'evidence', 'native_state'], invoker: ['isolation', 'evidence', 'native_state'],
  'agent-deck': ['isolation', 'native_state'], 'claude-squad': ['isolation', 'native_state'],
  'claude-code': ['native_state'], codex: ['native_state'],
});
const ISOLATION_EXACT = new Set(['agent.executor', 'workspace.worktree', 'workspace.sandbox', 'scheduler.deterministic_dag',
  'scheduler.persisted_dag', 'scheduler.wave_execution', 'fleet.pty', 'session.fork']);
const REPLAY_EXACT = new Set(['proof.replay', 'proof.lineage', 'proof.audit_chain', 'scheduler.deterministic_dag',
  'scheduler.persisted_dag', 'memory.timeline', 'session.resume']);
const SPINES = {
  temporal: new Set(['spines.temporal.bootstrap', 'spines.temporal.parity', 'temporal.flux.append', 'temporal.delta.derive',
    'temporal.affected_closure', 'temporal.graph.partition', 'proof.incremental_parity']),
  reasoning: new Set(['spines.reasoning.compile', 'spines.authority.preflight', 'reasoning.equivalence.saturate',
    'reasoning.helix.compile', 'reasoning.helix.reduce', 'authority.preflight']),
  model: new Set(['spines.model.plan', 'spines.kernel.verify', 'spines.finalize', 'model.phenotype.compile',
    'kernel.parity', 'placement.compile', 'fuse.compile']),
};
const SHADOW_VERSION = {
  caveman: ['bonfyre-caveman-provider', '--version'], cavemem: ['cavemem', '--version'],
  cavekit: ['bonfyre-cavekit-provider', '--version'], 'caveman-code': ['caveman-code', '--version'],
  rtk: ['rtk', '--version'], bernstein: ['bernstein', '--version'], 'agent-deck': ['agent-deck', '--version'],
  'claude-squad': ['claude-squad', 'version'],
};
const PROFILES = {
  lean: ['caveman', 'cavemem', 'cavekit', 'rtk'], deterministic: ['cavemem', 'cavekit', 'rtk', 'bernstein'],
  fleet: ['cavemem', 'rtk', 'agent-deck'],
  full: ['caveman', 'cavemem', 'cavekit', 'caveman-code', 'rtk', 'bernstein', 'agent-deck', 'claude-squad'],
  recommended: ['cavemem', 'cavekit', 'rtk', 'bernstein', 'agent-deck'],
};
const passed = receipt => nativeProbePassed(receipt);
const safeId = (text, name) => { if (typeof text !== 'string' || !/^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/.test(text)) throw new Error(`invalid ${name}`); return text; };
const safeJson = source => { try { return JSON.parse(source); } catch { return null; } };
function trait(provider, capability, name) {
  if (!(TRAITS[provider] ?? []).includes(name)) return 0;
  if (name === 'isolation' && !ISOLATION_EXACT.has(capability)) return .5;
  if (name === 'replay' && !REPLAY_EXACT.has(capability)) return .5;
  return 1;
}
function performance(receipts) {
  const best = Math.min(...receipts.filter(passed).map(item => item.duration_ms ?? 60000));
  return !Number.isFinite(best) ? 0 : best <= 250 ? 1 : best <= 1000 ? .8 : best <= 5000 ? .6 : best <= 15000 ? .4 : .2;
}

async function defaultSemantic(argv, { home, env, run, signal, timeoutMs = 30000, allowBernstein = false }) {
  signal?.throwIfAborted();
  const binary = findProbeExecutable(argv[0], env);
  let plan = binary && nativeExecutionPlan(binary, { env });
  let runtimeBoundary = null, bernsteinOwner = null;
  if (!plan && allowBernstein && argv[0] === 'bernstein') {
    const owner = inspectCanaryProviderExecutable('bernstein', env.PATH);
    if (binary && ['native', 'python_launcher'].includes(owner.kind) && owner.path === realpathSync(binary)) {
      plan = { argv: [owner.path] };
      bernsteinOwner = owner;
      runtimeBoundary = { kind: owner.kind === 'python_launcher' ? 'declared-third-party-python-cli' : 'native',
        launcher_sha256: owner.sha256, scope: 'upstream Bernstein fixture only' };
    }
  }
  if (!plan) return { receipt: { ...nativeProbeReceipt(argv, {}), refused: 'reviewed-native-executable-required' }, stdout: '', stderr: '' };
  const actual = [...plan.argv, ...argv.slice(1)];
  let result;
  try { result = await run(actual, { cwd: home, env, timeoutMs, maxOutputBytes: 2 * 1024 * 1024, signal }); }
  catch (error) {
    signal?.throwIfAborted();
    return { receipt: { ...nativeProbeReceipt(actual, {}), execution_state: 'unknown',
      error_class: error?.name === 'AbortError' ? 'AbortError' : 'ExecutionError' }, stdout: '', stderr: '' };
  }
  signal?.throwIfAborted();
  if (bernsteinOwner) {
    const after = inspectCanaryProviderExecutable('bernstein', env.PATH);
    if (after.kind !== bernsteinOwner.kind || after.path !== bernsteinOwner.path || after.sha256 !== bernsteinOwner.sha256)
      return { receipt: { ...nativeProbeReceipt(actual, result), refused: 'third-party-launcher-changed-during-probe',
        execution_state: 'unknown', runtime_boundary: runtimeBoundary }, stdout: '', stderr: '' };
  }
  return { receipt: { ...nativeProbeReceipt(actual, result), ...(runtimeBoundary ? { runtime_boundary: runtimeBoundary } : {}) },
    stdout: String(result.stdout || ''), stderr: String(result.stderr || '') };
}

export function createHypervisorEvaluation({ store, home = store?.home, env = process.env, run = projectRun,
  probe = createNativeProbe({ run, env, cwd: home }), semantic = defaultSemantic, now = () => Date.now() / 1000,
  checkStop = () => {} } = {}) {
  if (!store?.provider || !store?.registry || !store?.persistEvaluation || !store?.latestEvaluations || !home)
    throw new Error('hypervisor evaluation store interface incomplete');
  const execute = (argv, options = {}) => semantic(argv, { home, env, run, ...options });
  async function shadow(providerId, capability, signal, once) {
    const path = join(home, 'shadow-r3', 'certificates', providerId, `${capability}.json`);
    if (!existsSync(path)) return null;
    try {
      spinesOwnedPath(home, path);
      const bytes = new TextDecoder('utf-8', { fatal: true }).decode(readSpinesBytes(path, 8 * 1024 * 1024));
      const { value: certificate, calculated } = parsedDocument(bytes, { ascii: true });
      const basis = { certificate_digest: certificate.digest, certificate_path: path, certificate_state: certificate.state };
      if (calculated !== certificate.digest || certificate.schema !== CERT_SCHEMA ||
          certificate.provider !== providerId || certificate.capability !== capability)
        return { behavior_score: 0, note: 'Shadow certificate digest or identity mismatch.', probes: [], certificate_valid: false, ...basis };
      if (!Number.isFinite(certificate.expires_at) || certificate.expires_at <= now())
        return { behavior_score: 0, note: 'Shadow certificate expired.', probes: [], certificate_valid: false, ...basis };
      if (certificate.cleanup_verified !== true)
        return { behavior_score: 0, note: 'Shadow fixture cleanup was not verified.', probes: [], certificate_valid: false, ...basis };
      const argv = SHADOW_VERSION[providerId];
      if (!argv) return { behavior_score: 0, note: 'Shadow provider version cannot be rechecked.', probes: [], certificate_valid: false, ...basis };
      const version = await once(argv, { signal, timeoutMs: 30000, allowBernstein: providerId === 'bernstein' });
      const current = (version.stdout + version.stderr).trim();
      if (new TextDecoder('utf-8', { fatal: true }).decode(readSpinesBytes(path, 8 * 1024 * 1024)) !== bytes || certificate.expires_at <= now())
        return { behavior_score: 0, note: 'Shadow certificate changed or expired during validation.', probes: [version.receipt], certificate_valid: false, ...basis };
      if (!passed(version.receipt) || current !== certificate.provider_version)
        return { behavior_score: 0, note: 'Shadow certificate provider version no longer matches.', probes: [version.receipt], certificate_valid: false, ...basis };
      if (!['passed', 'partial'].includes(certificate.state))
        return { behavior_score: 0, note: 'Shadow certificate state cannot grant behavior.', probes: [version.receipt], certificate_valid: false, ...basis };
      const behavior = certificate.state === 'passed' ? Number(certificate.behavior_score ?? 1) :
        certificate.state === 'partial' ? Math.min(.4, Number(certificate.behavior_score ?? .4)) : 0;
      if (!Number.isFinite(behavior) || behavior < 0 || behavior > 1) throw new Error('invalid shadow behavior score');
      return { behavior_score: behavior, note: `Behavior sourced from valid R3 shadow certificate: ${certificate.state}.`,
        probes: [version.receipt], certificate_valid: true, shadow_state: certificate.state, ...basis };
    } catch (error) { return { behavior_score: 0, note: `Shadow certificate unreadable: ${error.name}`, probes: [], certificate_valid: false }; }
  }
  async function capabilityProbe(providerId, capability, { signal, extended, once }) {
    const certificate = await shadow(providerId, capability, signal, once);
    if (certificate?.certificate_valid) return { ...certificate, extended };
    let behavior = 0, note = 'No direct non-destructive capability probe is registered.';
    const probes = [], add = async (argv, options) => { const item = await once(argv, { signal, ...options }); probes.push(item.receipt); return item; };
    if (providerId === 'bonfyre-native') note = 'Native contract smoke is not independently bound here; no capability pass from identity alone.';
    else if (providerId === 'bonfyre-recursive-substrate') {
      const status = await add(['bonfyre-recursive', 'status']);
      const token = await add(['bonfyre-token-fabric', 'status']);
      behavior = passed(status.receipt) && passed(token.receipt) ? .4 : 0;
      note = 'Both status owners are reachable; a token/receipt semantic witness is still required for a full pass.';
    } else if (providerId === 'bonfyre-native-spines-r7') {
      const verification = await add(['bonfyre-spines', 'verify'], { timeoutMs: 60000 });
      const verified = passed(verification.receipt) && safeJson(verification.stdout)?.verified === true;
      const path = join(home, 'native-spines-r7', 'logs', 'latest-full-smoke.json');
      let doc = null, evidenceDigest = null;
      try { const bytes = readSpinesBytes(spinesOwnedPath(home, path), 8 * 1024 * 1024); evidenceDigest = SHA(bytes); doc = safeJson(new TextDecoder('utf-8', { fatal: true }).decode(bytes)); }
      catch { /* absent or invalid smoke cannot become a pass */ }
      const sealed = verified && doc?.state === 'passed';
      behavior = SPINES.temporal.has(capability) ? Number(sealed && doc?.r5?.parity?.equal === true) :
        SPINES.reasoning.has(capability) ? Number(sealed && doc?.r6?.reduction?.state === 'passed' && doc?.r6?.authority?.decision === 'candidate_only') :
          SPINES.model.has(capability) ? Number(sealed && doc?.r7?.parity?.state === 'passed' && doc?.r7?.fuse?.fuse?.activation === 'candidate') : 0;
      note = evidenceDigest ? `Spines verification and source-bound smoke ${evidenceDigest}.` : 'Spines smoke evidence unavailable.';
    } else if (providerId === 'cavemem' && ['memory.search', 'memory.timeline'].includes(capability)) {
      const result = await add(['cavemem', 'search', '__bonfyre_capability_probe__', '--limit', '1', '--no-semantic'], { timeoutMs: 60000 });
      behavior = Number(passed(result.receipt)); note = 'Cavemem searched the local observation store without capture.';
    } else if (providerId === 'cavemem' && ['memory.capture', 'context.memory_compression'].includes(capability)) {
      const result = await add(['cavemem', 'status']);
      const output = result.stdout + result.stderr;
      behavior = passed(result.receipt) && ['claude-code', 'codex', 'data.db'].every(text => output.includes(text)) ? .4 : 0;
      note = 'Hook and store wiring observed; capture semantics require a disposable fixture.';
    } else if (providerId === 'rtk' && capability === 'context.tool_compression') {
      const raw = await add(['git', 'status'], { env: { ...env, GIT_OPTIONAL_LOCKS: '0' } });
      const compact = await add(['rtk', 'git', 'status'], { env: { ...env, GIT_OPTIONAL_LOCKS: '0' } });
      const sameExit = raw.receipt.exit_code === compact.receipt.exit_code, rawBytes = Buffer.byteLength(raw.stdout), compactBytes = Buffer.byteLength(compact.stdout);
      behavior = passed(raw.receipt) && passed(compact.receipt) && rawBytes > 0 && compactBytes <= rawBytes ? 1 :
        passed(raw.receipt) && passed(compact.receipt) && sameExit ? .4 : 0;
      note = `RTK exit equivalence=${sameExit}; raw_bytes=${rawBytes}; compact_bytes=${compactBytes}.`;
    } else if (providerId === 'bernstein' && ['scheduler.deterministic_dag', 'scheduler.wave_execution'].includes(capability)) {
      const temporary = mkdtempSync(join(tmpdir(), 'bonfyre-bernstein-eval-'));
      try {
        const path = join(temporary, 'probe.yaml');
        writeFileSync(path, 'name: bonfyre-probe\ndescription: Two-node dry-run probe.\nversion: "1.0.0"\nnodes:\n  - id: first\n    type: command\n    command: "true"\n  - id: second\n    type: command\n    depends_on: [first]\n    command: "true"\n');
        const validate = await add(['bernstein', 'workflow', 'validate', path], { allowBernstein: true, timeoutMs: 60000 });
        const dry = await add(['bernstein', 'workflow', 'run', path, '--dry-run'], { allowBernstein: true, timeoutMs: 60000 });
        behavior = Number(passed(validate.receipt) && passed(dry.receipt));
        note = 'Upstream Bernstein two-node manifest validated and dry-ran; no workflow execution.';
      } finally { rmSync(temporary, { recursive: true, force: true }); }
    } else if (providerId === 'agent-deck') {
      const descriptors = {
        'fleet.status_detection': [['conductor', 'list'], ['conductor', 'status']],
        'fleet.pty': [['--version']], 'session.resume': [['session', '--help']], 'session.fork': [['session', '--help']],
        'mcp.pool': [['mcp', '--help']], 'workspace.worktree': [['add', '--help']], 'workspace.sandbox': [['add', '--help']],
        'fleet.remote_control': [['remote', 'list']], 'session.migrate': [['remote', 'list']],
        'ui.terminal': [['--help']], 'ui.web': [['web', '--help']],
      };
      for (const args of descriptors[capability] ?? []) await add(['agent-deck', ...args]);
      behavior = capability === 'fleet.status_detection' && passed(probes[0]) ? 1 : probes.some(passed) ? .4 : 0;
      note = capability === 'fleet.status_detection' ? 'Conductor inventory queried without TUI launch.' : 'Command surface observed; disposable session proof still required.';
    } else if (providerId === 'claude-squad') {
      const item = await add(['claude-squad', capability === 'fleet.pty' ? 'version' : '--help']);
      if (capability === 'fleet.pty') await add(['tmux', '-V']);
      behavior = passed(item.receipt) ? .4 : 0;
      note = 'CLI/PTy substrate reachable; no session behavior inferred from version/help.';
    } else if (providerId === 'caveman-code' && ['agent.executor', 'agent.architect', 'session.resume', 'session.fork'].includes(capability)) {
      const item = await add(['caveman-code', '--help']); behavior = passed(item.receipt) ? .4 : 0;
      note = 'CLI surface reachable; isolated shadow mission still required.';
    } else if (['claude-code', 'codex'].includes(providerId) && capability.startsWith('agent.'))
      note = 'Vendor agent version is reachability only; behavior is verified per mission.';
    else if (providerId === 'cavekit' && ['spec.compile', 'spec.invariant_backprop'].includes(capability))
      note = 'Skill protocol declaration requires mission shadow evidence.';
    else if (providerId === 'caveman' && capability.startsWith('context.'))
      note = 'Skill/plugin declaration requires shadow response comparison.';
    return { behavior_score: behavior, note, probes, extended,
      ...(certificate && !certificate.certificate_valid ? { shadow_refusal: certificate.note } : {}) };
  }
  async function evaluate(providerId, { capability = null, execute_extended = false, signal } = {}) {
    safeId(providerId, 'provider'); signal?.throwIfAborted(); checkStop();
    const provider = store.provider(providerId); if (!provider) throw new Error('provider not found');
    const targets = capability ? [safeId(capability, 'capability')] : provider.capabilities.map(item => item.capability_id);
    if (!targets.length || targets.length > 1000 || targets.some(target => !provider.capabilities.some(item => item.capability_id === target)))
      throw new Error('provider does not declare requested capability');
    const started = now(), evaluationId = store.id(), cache = new Map();
    const once = (argv, options = {}) => {
      const key = canonicalJson([argv, options.allowBernstein ?? false]);
      if (!cache.has(key)) cache.set(key, execute(argv, options));
      return cache.get(key);
    };
    const generic = [];
    for (const argv of PROBES[providerId] ?? []) {
      signal?.throwIfAborted(); checkStop();
      const result = await probe(argv, { cwd: home, timeoutMs: 60000, signal }); generic.push(result);
      if (passed(result) && !execute_extended) break;
    }
    const reachable = generic.some(passed);
    const selfCheck = generic.filter(item => passed(item) && !item.argv?.some(part => ['--version', 'version', '--help'].includes(part))).length >= 2;
    const safeLicense = ['MIT', 'Apache-2.0', 'vendor', 'private'].includes(provider.license) || !!provider.license_approved;
    const results = [], persisted = []; let anyProgress = false;
    for (const target of targets) {
      signal?.throwIfAborted(); checkStop();
      const specific = await capabilityProbe(providerId, target, { signal, extended: execute_extended, once });
      const behavior = Number(specific.behavior_score) || 0;
      const metrics = { reachable: Number(reachable), self_check: Number(selfCheck), behavior,
        native_state: Number((TRAITS[providerId] ?? []).includes('native_state')),
        isolation: trait(providerId, target, 'isolation'), replay: trait(providerId, target, 'replay'),
        evidence: trait(providerId, target, 'evidence'), performance: performance([...generic, ...specific.probes]),
        license: Number(safeLicense) };
      const score = Object.entries(WEIGHTS).reduce((sum, [key, weight]) => sum + metrics[key] * weight, 0);
      const state = reachable && score >= .5 && behavior >= .5 ? 'passed' : reachable && score >= .3 ? 'partial' : 'failed';
      anyProgress ||= state !== 'failed';
      const evidence = { provider: providerId, capability: target, generic_probes: generic, capability_probe: specific,
        metrics, license: provider.license, license_approved: !!provider.license_approved,
        source_ref: provider.source_ref ?? null, source_digest: provider.source_digest ?? null };
      results.push({ capability: target, state, score: Math.round(score * 10000) / 10000, metrics, probe: specific });
      persisted.push({ capability: target, state, score, metrics, evidence });
    }
    const completed = now(), overall = results.every(item => item.state === 'passed') ? 'passed' : anyProgress ? 'partial' : 'failed';
    const average = results.reduce((sum, item) => sum + item.score, 0) / results.length;
    const document = { schema: 'bonfyre.provider-evaluation.v5', id: evaluationId, provider: providerId, state: overall,
      score: Math.round(average * 10000) / 10000, generic_probes: generic, capabilities: results,
      started_at: started, completed_at: completed };
    checkStop(); signal?.throwIfAborted();
    return store.persistEvaluation({ id: evaluationId, provider_id: providerId, started_at: started, completed_at: completed,
      capabilities: persisted, provider_health: results.every(item => item.state === 'passed') ? 'verified' : anyProgress ? 'reachable' : 'degraded', document });
  }
  function compare(capability, { providers = null } = {}) {
    safeId(capability, 'capability');
    if (providers !== null && (!Array.isArray(providers) || providers.length > 1000)) throw new Error('invalid providers filter');
    const ranking = store.latestEvaluations(capability, { providers }).map(row => ({
      provider_id: row.provider_id, capability_id: row.capability_id, state: row.state, score: row.score,
      completed_at: row.completed_at, metrics: row.metrics, evidence: row.evidence,
    })).sort((left, right) => right.score - left.score || compareUnicode(left.provider_id, right.provider_id));
    return { capability, ranking, winner: ranking[0]?.provider_id ?? null };
  }
  function installPlan(profile = 'recommended') {
    if (!Object.hasOwn(PROFILES, profile)) throw new Error('unknown profile');
    const selected = new Set(PROFILES[profile]);
    const steps = store.registry().projects.filter(project => selected.has(project.id)).map(project => ({
      provider: project.id, mode: project.default_mode, license: project.license, commands: project.install ?? {}, notes: project.notes ?? '',
    }));
    return { profile, automatic: false, steps,
      warning: 'Review and execute commands explicitly. The hypervisor never pipes unreviewed remote scripts into a shell. Installation does not imply capability promotion.' };
  }
  return Object.freeze({ evaluate, compare, installPlan });
}
