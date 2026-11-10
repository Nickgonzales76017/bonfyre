// R4 provider fixture witnesses. Every provider command is explicit and bounded;
// importing this module never runs a fixture or contacts a service.
import { createHash, randomBytes } from 'node:crypto';
import { accessSync, closeSync, copyFileSync, existsSync, fstatSync, lstatSync, mkdirSync, openSync, readFileSync, readSync, readdirSync, realpathSync, renameSync, statSync, writeFileSync, constants } from 'node:fs';
import { homedir } from 'node:os';
import { delimiter, join, resolve } from 'node:path';
import { projectRun } from './project_runtime.mjs';

export const CORRECTED_PROVIDERS = Object.freeze(['rtk', 'cavekit', 'bernstein', 'agent-deck']);
const VERSION = '5.4.0-fixture-canary-r4';
const FIXTURE_REVISION = 'bonfyre-canary-fixtures-bun-v1';
const REVIEWED_SCRIPTS = JSON.parse(readFileSync(new URL('./canary_reviewed_scripts.json', import.meta.url), 'utf8')).scripts;
const SECTION_MARKERS = ['§G', '§C', '§I', '§V', '§T', '§B'];
const object = value => value !== null && typeof value === 'object' && !Array.isArray(value);
const outputText = result => String(result?.stdout ?? '') + String(result?.stderr ?? '');
const sha256 = value => createHash('sha256').update(value).digest('hex');
const regular = path => { try { return lstatSync(path).isFile(); } catch { return false; } };
const read = path => regular(path) ? readFileSync(path, 'utf8') : '';
const json = value => JSON.stringify(value, null, 2) + '\n';
const evidence = result => Object.fromEntries(['argv', 'cwd', 'exit_code', 'stdout', 'stderr', 'duration_ms', 'timed_out', 'overflow']
  .map(key => [key, key === 'timed_out' ? result?.timed_out === true || result?.timeout === true : result?.[key] ?? null]));
function atomicJson(path, value) {
  const temporary = `${path}.${randomBytes(8).toString('hex')}.tmp`;
  writeFileSync(temporary, json(value), { flag: 'wx', mode: 0o600 });
  renameSync(temporary, path);
}
function assertResult(result) {
  if (!object(result) || !Number.isInteger(result.exit_code)) throw new Error('fixture command returned no exit code');
  return result;
}
function safeString(value) { return String(value ?? '').slice(0, 2000); }
function digestExecutable(path) {
  const handle = openSync(path, constants.O_RDONLY | (constants.O_NOFOLLOW || 0));
  try {
    const before = fstatSync(handle);
    if (!before.isFile() || before.size > 256 * 1024 * 1024) throw new Error('provider executable is not a bounded regular file');
    const hash = createHash('sha256'), block = Buffer.alloc(64 * 1024);
    let prefix = Buffer.alloc(0), size = 0, length;
    while ((length = readSync(handle, block, 0, block.length, null)) > 0) {
      if (size === 0) prefix = Buffer.from(block.subarray(0, Math.min(length, 8192)));
      size += length;
      if (size > 256 * 1024 * 1024) throw new Error('provider executable grew past byte bound');
      hash.update(block.subarray(0, length));
    }
    const after = fstatSync(handle);
    if (after.dev !== before.dev || after.ino !== before.ino || after.size !== before.size || after.mtimeMs !== before.mtimeMs || size !== before.size)
      throw new Error('provider executable changed during classification');
    return { prefix, sha256: hash.digest('hex') };
  } finally { closeSync(handle); }
}
export function inspectCanaryProviderExecutable(name, pathValue = process.env.PATH) {
  for (const directory of String(pathValue || '').split(delimiter)) {
    if (!directory) continue;
    const candidate = join(directory, name);
    try {
      if (!statSync(candidate).isFile()) continue;
      accessSync(candidate, constants.X_OK);
      const path = realpathSync(candidate), { prefix, sha256: digest } = digestExecutable(path);
      const magic = prefix.subarray(0, 4).toString('hex');
      if (magic === '7f454c46' || ['feedface', 'feedfacf', 'cefaedfe', 'cffaedfe', 'cafebabe', 'bebafeca'].includes(magic))
        return { path, kind: 'native', sha256: digest };
      const head = prefix.toString('utf8');
      const python = /(?:^#![^\n]*python|\b(?:exec|env)\b[^\n]*\bpython(?:[0-9.]*)?\b|\bfrom bernstein\.cli\b)/i.test(head);
      const interpreter = head.match(/\bexec'\s+"([^"\n]*\/python(?:[0-9.]*)?)"/)?.[1] || null;
      const reviewed = REVIEWED_SCRIPTS[name];
      return { path, kind: python ? 'python_launcher' : reviewed?.sha256 === digest ? 'reviewed_script' : 'unverified_script',
        sha256: digest, ...(reviewed?.sha256 === digest ? { runtime: reviewed.runtime, review: reviewed.review } : {}),
        ...(interpreter ? { interpreter } : {}) };
    } catch { /* unreadable candidate is not executable evidence */ }
  }
  return { path: null, kind: 'missing' };
}

export function createCanaryFixtures({ store, projectRoot, recursiveCall, run = projectRun,
  env = process.env, home = homedir(), now = () => Date.now() / 1000,
  sleep = ms => new Promise(done => setTimeout(done, ms)) } = {}) {
  if (!store || !regularStore(store) || typeof projectRoot !== 'string' || !projectRoot ||
      typeof run !== 'function' || typeof now !== 'function' || typeof sleep !== 'function')
    throw new TypeError('fixture runner needs a store, project root, bounded runner, clock, and sleeper');
  const root = resolve(projectRoot), userHome = resolve(home);
  const runRoot = resolve(store.runRoot), storeHome = resolve(store.home);
  const command = async (argv, cwd, timeoutMs = 60_000) => {
    if (!Array.isArray(argv) || !argv.length || argv.some(item => typeof item !== 'string')) throw new Error('fixture argv invalid');
    if (argv.some(item => /(?:^|\/)python(?:\d+(?:\.\d+)*)?(?:$|\s)/i.test(item))) throw new Error('Python fixture execution is retired');
    const sanitized = Object.fromEntries(Object.entries(env).filter(([key]) => !key.startsWith('GIT_')));
    const result = await run(argv, { cwd, env: { ...sanitized, GIT_CONFIG_NOSYSTEM: '1',
      GIT_CONFIG_GLOBAL: '/dev/null', GIT_CONFIG_COUNT: '2', GIT_CONFIG_KEY_0: 'core.hooksPath',
      GIT_CONFIG_VALUE_0: '/dev/null', GIT_CONFIG_KEY_1: 'core.fsmonitor', GIT_CONFIG_VALUE_1: 'false',
    }, timeoutMs, maxOutputBytes: 8 * 1024 * 1024 });
    const valid = assertResult(result);
    return { ...valid, stdout: String(valid.stdout ?? '').slice(-250_000),
      stderr: String(valid.stderr ?? '').slice(-250_000) };
  };
  const seal = async (provider, capability, state, score, fixtureRoot, data, claims, cleanup, limitations, runId) =>
    store.sealCertificate({ provider, capability, state, behavior_score: score, fixture_root: fixtureRoot,
      evidence: [{ fixture_runtime: 'bun', fixture_revision: FIXTURE_REVISION,
        historical_python_fixture_parity: false }, ...data], claims, cleanup_verified: cleanup,
      limitations: [...limitations, 'Bun fixture revision does not certify historical Python test-workload parity.'], run_id: runId });
  async function sealMany(provider, capabilities, fixtureRoot, data, cleanup, runId) {
    const sealed = [];
    for (const [capability, state, score, claims, limitations] of capabilities)
      sealed.push(await seal(provider, capability, state, score, fixtureRoot, data, claims, cleanup, limitations, runId));
    return sealed;
  }
  async function ingest(certificate) {
    if (typeof recursiveCall !== 'function') return { state: 'skipped', reason: 'recursive_owner_unavailable' };
    let invoked = false, artifactId = null;
    const project = async (stage, name, args, timeoutMs, valid) => {
      const intent = await store.writeReceipt({ kind: 'certificate.projection.attempt', subject: certificate.digest, state: 'started',
        payload: { certificate_digest: certificate.digest, provider: certificate.provider,
          capability: certificate.capability, stage, arguments: args } });
      if (!object(intent) || !Number.isSafeInteger(intent.seq) || !/^[a-f0-9]{64}$/.test(intent.digest || ''))
        throw new Error('projection intent has no durable receipt identity');
      invoked = true;
      let result;
      try {
        result = await recursiveCall(name, args, { timeoutMs });
        if (!valid(result)) throw new Error(`${stage} returned no valid owner identity`);
        await store.writeReceipt({ kind: 'certificate.projection.completion', subject: certificate.digest, state: 'passed',
          payload: { certificate_digest: certificate.digest, stage,
            ...(stage === 'artifact_ingest' ? { artifact_id: result.artifact_id } : {}),
            result_sha256: sha256(JSON.stringify(result)) } });
      } catch (error) {
        try {
          await store.writeReceipt({ kind: 'certificate.projection.completion', subject: certificate.digest,
            state: 'outcome_uncertain', payload: { certificate_digest: certificate.digest, stage,
              ...(artifactId ? { retained_artifact_id: artifactId } : {}), reason: safeString(error?.message || error) } });
        } catch { /* The durable attempt remains; never retry an uncertain external effect. */ }
        throw error;
      }
      return result;
    };
    try {
      const artifact = await project('artifact_ingest', 'artifact_ingest', {
        path: certificate.path, kind: 'provider-shadow-certificate-r4', classification: 'internal',
        schema_name: certificate.schema, provenance: { provider: certificate.provider,
          capability: certificate.capability, release: VERSION },
      }, 300_000, value => object(value) && typeof value.artifact_id === 'string' && value.artifact_id.length > 0
        && value.artifact_id.length <= 512 && ![...value.artifact_id].some(character => character.charCodeAt(0) < 32) && !value.error);
      artifactId = artifact.artifact_id;
      const tokenArtifact = await project('token_artifact_compile', 'token_artifact_compile', {
        artifact_id: artifact.artifact_id, tokenizer_id: 'lexeme-v1',
      }, 600_000, value => object(value) && !value.error);
      const dependency = await project('dependency_register', 'dependency_register', {
        source_kind: 'provider_capability', source_id: `${certificate.provider}:${certificate.capability}`,
        target_kind: 'artifact', target_id: artifact.artifact_id, edge_kind: 'certified_by',
        metadata: { certificate_path: certificate.path, release: VERSION },
      }, 120_000, value => object(value) && !value.error);
      return { state: 'passed', artifact, token_artifact: tokenArtifact, dependency };
    } catch (error) { return { state: invoked ? 'outcome_uncertain' : 'blocked',
      ...(artifactId ? { retained_artifact_id: artifactId } : {}),
      error: safeString(error?.message || error), no_automatic_retry: true }; }
  }
  async function runSelected(providers = null, { max_agent_turns = 4 } = {}) {
    const selected = providers == null || Array.isArray(providers) && providers.length === 0 ? [...CORRECTED_PROVIDERS] : providers;
    if (!Array.isArray(selected) || selected.some(item => !CORRECTED_PROVIDERS.includes(item)) ||
        new Set(selected).size !== selected.length) throw new Error('R4 correction supports four unique named providers only');
    if (!Number.isSafeInteger(max_agent_turns) || max_agent_turns < 1 || max_agent_turns > 4)
      throw new Error('max_agent_turns must be 1..4');
    const runId = new Date(now() * 1000).toISOString().replace(/[-:]/g, '').replace(/\.\d{3}Z$/, '')
      + '-' + randomBytes(4).toString('hex');
    const directory = join(runRoot, runId), fixtureRoot = join(directory, 'fixtures'), resultRoot = join(directory, 'results');
    const startedReceipt = await store.writeReceipt({ kind: 'fixture.run.started', subject: runId, state: 'started',
      payload: { providers: selected, fixture_runtime: 'bun', fixture_revision: FIXTURE_REVISION } });
    if (!object(startedReceipt) || !Number.isSafeInteger(startedReceipt.seq) || !/^[a-f0-9]{64}$/.test(startedReceipt.digest || ''))
      throw new Error('fixture run start has no durable receipt identity');
    mkdirSync(directory); mkdirSync(fixtureRoot); mkdirSync(resultRoot);
    const started = now(), providerResults = [];
    const methods = { rtk: certifyRtk, cavekit: certifyCavekit, bernstein: certifyBernstein, 'agent-deck': certifyAgentDeck };
    for (const provider of selected) providerResults.push(await methods[provider](runId,
      join(fixtureRoot, provider), join(resultRoot, provider), max_agent_turns));
    const certificates = providerResults.flatMap(item => item.certificates || []), recursive = [];
    for (const certificate of certificates) recursive.push({ provider: certificate.provider,
      capability: certificate.capability, certificate_digest: certificate.digest, result: await ingest(certificate) });
    const summary = Object.fromEntries(['passed', 'partial', 'blocked', 'failed']
      .map(state => [state, certificates.filter(item => item.state === state).length]));
    const recursiveUnfinished = recursive.filter(item => item.result.state !== 'passed').length;
    const recursiveFailures = recursive.filter(item => ['failed', 'outcome_uncertain'].includes(item.result.state)).length;
    const document = { schema: 'bonfyre.fixture-correction-r4.run.v1', version: VERSION,
      fixture_runtime: 'bun', fixture_revision: FIXTURE_REVISION, run_id: runId,
      providers: selected, started_at: started, completed_at: now(), run_root: directory,
      summary, certificates, runtime_boundaries: providerResults.map(item => item.runtime_boundary).filter(Boolean),
      recursive_ingestion: recursive, recursive_ingestion_failed: recursiveFailures,
      recursive_ingestion_unfinished: recursiveUnfinished,
      receipt_chain: store.verifyReceipts() };
    const path = join(directory, 'run.json');
    atomicJson(path, document);
    atomicJson(join(storeHome, 'latest-fixture-run.json'), document);
    document.path = path;
    document.receipt = await store.writeReceipt({ kind: 'fixture.run.completed', subject: runId,
      state: summary.failed === 0 && summary.blocked === 0 && recursiveUnfinished === 0 ? 'passed' : 'partial',
      payload: { providers: selected, summary, path, recursive_ingestion_states: recursive.map(item => item.result.state) } });
    return document;
  }

  async function certifyRtk(runId, fixtureRoot, resultRoot) {
    mkdirSync(fixtureRoot); mkdirSync(resultRoot); mkdirSync(join(fixtureRoot, 'src')); mkdirSync(join(fixtureRoot, 'tests'));
    writeFileSync(join(fixtureRoot, 'src', 'important.mjs'), "export const importantValue = () => 'R4_IMPORTANT_BASELINE';\n");
    writeFileSync(join(fixtureRoot, 'tests', 'r4.test.mjs'),
      "import { test, expect } from 'bun:test';\nimport { importantValue } from '../src/important.mjs';\n" +
      "test('many passing assertions', () => { for (let index = 0; index < 100; index++) expect(index + 1).toBe(index + 1); });\n" +
      "test('R4 failure sentinel', () => { expect(importantValue()).toBe('R4_EXPECTED_SENTINEL'); });\n");
    for (let index = 0; index < 80; index++) writeFileSync(join(fixtureRoot, 'src', `bulk_${String(index).padStart(3, '0')}.txt`),
      Array.from({ length: 80 }, (_, line) => `baseline-${String(index).padStart(3, '0')}-${String(line).padStart(4, '0')}-R4_BULK_DATA`).join('\n') + '\n');
    const init = await command(['git', 'init'], fixtureRoot);
    await command(['git', 'config', 'user.email', 'r4@bonfyre.local'], fixtureRoot);
    await command(['git', 'config', 'user.name', 'Bonfyre R4'], fixtureRoot);
    await command(['git', 'add', '.'], fixtureRoot);
    const commit = await command(['git', 'commit', '-m', 'R4 baseline'], fixtureRoot);
    writeFileSync(join(fixtureRoot, 'src', 'important.mjs'), "export const importantValue = () => 'R4_ACTUAL_FAILURE_SENTINEL';\n");
    for (let index = 0; index < 40; index++) {
      const path = join(fixtureRoot, 'src', `bulk_${String(index).padStart(3, '0')}.txt`);
      writeFileSync(path, read(path) + Array.from({ length: 60 }, (_, line) =>
        `changed-${String(index).padStart(3, '0')}-${String(line).padStart(4, '0')}-R4_CHANGED_DATA`).join('\n') + '\n');
    }
    for (let index = 0; index < 30; index++) writeFileSync(join(fixtureRoot, `untracked_${String(index).padStart(3, '0')}.txt`), 'R4_UNTRACKED_SENTINEL\n'.repeat(20));
    const tests = ['bun', 'test', './tests/r4.test.mjs'];
    const pairs = [
      ['status', await command(['git', 'status'], fixtureRoot), await command(['rtk', 'git', 'status'], fixtureRoot)],
      ['diff', await command(['git', 'diff'], fixtureRoot), await command(['rtk', 'git', 'diff'], fixtureRoot)],
      ['test', await command(tests, fixtureRoot, 120_000), await command(['rtk', 'test', ...tests], fixtureRoot, 120_000)],
    ];
    const rawBytes = pairs.reduce((sum, [, raw]) => sum + Buffer.byteLength(outputText(raw)), 0);
    const compactBytes = pairs.reduce((sum, [, , compact]) => sum + Buffer.byteLength(outputText(compact)), 0);
    const reduction = rawBytes ? 1 - compactBytes / rawBytes : 0;
    const exitParity = pairs.every(([, raw, compact]) => raw.exit_code === compact.exit_code);
    const compactText = pairs.map(([, , compact]) => outputText(compact)).join('\n');
    const changedIdentity = ['important.mjs', 'important', 'src/'].some(token => compactText.includes(token));
    const rawTest = pairs[2][1], compactTest = pairs[2][2];
    const sentinel = ['R4 failure sentinel', 'R4_ACTUAL_FAILURE_SENTINEL', 'R4_EXPECTED_SENTINEL'];
    const expectedTestFailure = rawTest.exit_code === 1 && compactTest.exit_code === 1;
    const failureIdentity = expectedTestFailure && sentinel.some(token => outputText(rawTest).includes(token))
      && sentinel.some(token => outputText(compactTest).includes(token));
    const meaningfulReduction = reduction >= 0.15 && compactBytes < rawBytes;
    const passed = init.exit_code === 0 && commit.exit_code === 0 && exitParity && changedIdentity && failureIdentity && meaningfulReduction;
    const data = [{ phase: 'git-init', result: evidence(init) }, { phase: 'git-commit', result: evidence(commit) },
      ...pairs.map(([name, raw, compact]) => ({ pair: name, raw: evidence(raw), rtk: evidence(compact) })),
      { raw_bytes: rawBytes, compact_bytes: compactBytes, reduction_ratio: reduction }];
    const claims = [
      { claim: 'RTK preserves exit behavior.', passed: exitParity },
      { claim: 'RTK preserves changed-file identity.', passed: changedIdentity },
      { claim: 'Both test runners observed the intended assertion failure, not a launch error.', passed: expectedTestFailure },
      { claim: 'RTK preserves failing-test identity.', passed: failureIdentity },
      { claim: 'RTK reduces aggregate meaningful output by at least 15%.', passed: meaningfulReduction },
    ];
    return { provider: 'rtk', certificates: [await seal('rtk', 'context.tool_compression', passed ? 'passed' : 'failed',
      passed ? 1 : 0, fixtureRoot, data, claims, true,
      passed ? [] : ['One or more semantic-parity or reduction assertions failed.'], runId)] };
  }

  async function certifyBernstein(runId, fixtureRoot, resultRoot) {
    mkdirSync(fixtureRoot); mkdirSync(resultRoot);
    const owner = inspectCanaryProviderExecutable('bernstein', env.PATH);
    if (!['native', 'python_launcher'].includes(owner.kind)) {
      const limitation = `Bernstein provider launcher is ${owner.kind}; Python-backed or unverified scripts are retired and were not executed.`;
      const data = [{ executable: owner, executed: false, reason: limitation }];
      return { provider: 'bernstein', certificates: await sealMany('bernstein', [
        ['scheduler.deterministic_dag', 'blocked', 0, [{ claim: 'Native Bernstein provider executable exists.', passed: false }], [limitation]],
        ['scheduler.persisted_dag', 'blocked', 0, [], [limitation]],
        ['scheduler.wave_execution', 'blocked', 0, [], [limitation]],
      ], fixtureRoot, data, true, runId) };
    }
    // Bernstein is a third-party Python CLI on this installation. Its actual
    // scheduler must be measured; substituting a Bonfyre DAG would falsely
    // certify Bernstein. This exception is exact and declared, never a Python
    // fallback for Bonfyre's own fixture/runtime code.
    const runtimeBoundary = owner.kind === 'python_launcher' ? {
      provider: 'bernstein', kind: 'declared-third-party-python-cli', launcher: owner.path,
      launcher_sha256: owner.sha256,
      interpreter: owner.interpreter || 'python interpreter selected by upstream launcher',
      reason: 'Certification measures the real upstream Bernstein scheduler; no native Bernstein implementation is present.',
    } : null;
    const boundaryLimitation = runtimeBoundary ? [runtimeBoundary.reason + ' The upstream CLI is a declared Python dependency.'] : [];
    const bernsteinCommand = async (args, timeoutMs = 60_000) => {
      if (digestExecutable(owner.path).sha256 !== owner.sha256) throw new Error('Bernstein launcher changed after admission');
      return command([owner.path, ...args], fixtureRoot, timeoutMs);
    };
    const markerA = join(fixtureRoot, 'marker-a.txt'), markerB = join(fixtureRoot, 'marker-b.txt');
    writeFileSync(join(fixtureRoot, 'first.mjs'), `import { writeFileSync } from 'node:fs';\nwriteFileSync(${JSON.stringify(markerA)}, 'R4-MARKER-A');\n`);
    writeFileSync(join(fixtureRoot, 'second.mjs'), `import { readFileSync, writeFileSync } from 'node:fs';\n` +
      `if (readFileSync(${JSON.stringify(markerA)}, 'utf8') !== 'R4-MARKER-A') throw new Error('dependency order');\n` +
      `writeFileSync(${JSON.stringify(markerB)}, 'R4-MARKER-B');\n`);
    const manifest = join(fixtureRoot, 'shadow-r4.yaml');
    const manifestText = ['name: bonfyre-r4', 'description: Bonfyre R4 deterministic two-node command DAG.', 'version: "1.0.0"',
      'nodes:', '  - id: first', '    command: "bun first.mjs"', '  - id: second', '    depends_on: [first]', '    command: "bun second.mjs"', ''].join('\n');
    writeFileSync(manifest, manifestText);
    const validate = await bernsteinCommand(['workflow', 'validate', manifest], 120_000);
    const dry = await bernsteinCommand(['workflow', 'run', manifest, '--dry-run'], 120_000);
    const execute = await bernsteinCommand(['workflow', 'run', manifest], 240_000);
    const replay = await bernsteinCommand(['replay', 'list']);
    const lineage = await bernsteinCommand(['lineage', '--help']);
    const audit = await bernsteinCommand(['audit', '--help']);
    const markerAOk = read(markerA) === 'R4-MARKER-A', markerBOk = read(markerB) === 'R4-MARKER-B';
    const commandPass = [validate, dry, execute].every(item => item.exit_code === 0) && markerAOk && markerBOk;
    const data = [{ executable: owner, runtime_boundary: runtimeBoundary },
      { manifest, manifest_text: manifestText, manifest_sha256: sha256(manifestText) },
      ...[validate, dry, execute, replay, lineage, audit].map(evidence), { marker_a: markerAOk, marker_b: markerBOk }];
    const persistedPass = commandPass && replay.exit_code === 0;
    return { provider: 'bernstein', runtime_boundary: runtimeBoundary, certificates: await sealMany('bernstein', [
      ['scheduler.deterministic_dag', commandPass ? 'passed' : 'failed', commandPass ? 1 : 0,
        [{ claim: 'Workflow validates.', passed: validate.exit_code === 0 }, { claim: 'Dry-run succeeds.', passed: dry.exit_code === 0 },
          { claim: 'Dependency order produced both markers.', passed: markerAOk && markerBOk }],
        [...boundaryLimitation, ...(commandPass ? [] : ['Installed Bernstein schema or execution did not accept the corrected command-only DAG.'])]],
      ['scheduler.persisted_dag', persistedPass ? 'passed' : 'partial', persistedPass ? 0.8 : 0.4,
        [{ claim: 'Executed workflow persisted observable run state.', passed: execute.exit_code === 0 && replay.exit_code === 0 }],
        [...boundaryLimitation, 'Agent-node scheduling remains outside this command-only fixture.']],
      ['scheduler.wave_execution', commandPass ? 'passed' : 'partial', commandPass ? 0.8 : 0.4,
        [{ claim: 'Second node executed only after first marker existed.', passed: markerBOk }],
        [...boundaryLimitation, 'This fixture certifies a two-node dependency wave, not multi-agent fanout.']],
    ], fixtureRoot, data, true, runId) };
  }

  function findSkill(keyword) {
    const candidates = [];
    for (const directory of [join(root, '.claude/skills'), join(root, '.agents/skills'), join(userHome, '.claude/skills'), join(userHome, '.agents/skills')]) {
      if (!existsSync(directory)) continue;
      for (const item of readdirSync(directory, { withFileTypes: true })) {
        if (!item.isDirectory()) continue;
        const path = join(directory, item.name, 'SKILL.md');
        if (!regular(path)) continue;
        const text = read(path).slice(0, 1_000_000), lowered = (item.name + '\n' + text).toLowerCase();
        const score = (lowered.includes(keyword.toLowerCase()) ? 10 : 0)
          + (lowered.includes('§g') && lowered.includes('§b') ? 4 : 0) + (lowered.includes('cavekit') ? 3 : 0);
        if (score) candidates.push({ score, path });
      }
    }
    candidates.sort((a, b) => b.score - a.score || a.path.localeCompare(b.path));
    return candidates[0]?.path ?? null;
  }
  async function claudePrint(prompt, cwd, turns) {
    // A nonzero or timed-out agent may already have edited the fixture. An
    // alternate syntax retry would replay that uncertain work.
    const item = await command(['claude', '-p', prompt, '--output-format', 'json', '--max-turns', String(turns),
      '--allowedTools', 'Read,Write,Edit'], cwd, 900_000);
    return { ...item, attempts: [evidence(item)] };
  }
  async function certifyCavekit(runId, fixtureRoot, resultRoot, turns) {
    mkdirSync(fixtureRoot); mkdirSync(resultRoot); mkdirSync(join(fixtureRoot, 'skills'));
    const specSkill = findSkill('spec') || findSkill('build'), backpropSkill = findSkill('backprop') || findSkill('bug');
    const copied = {};
    for (const [name, source] of [['spec', specSkill], ['backprop', backpropSkill]]) if (source) {
      const destination = join(fixtureRoot, 'skills', `${name}-SKILL.md`);
      copyFileSync(source, destination); copied[name] = destination;
    }
    writeFileSync(join(fixtureRoot, 'calculator.mjs'), 'export const add = (a, b) => a - b; // R4 known bug\n');
    writeFileSync(join(fixtureRoot, 'test_calculator.test.mjs'),
      "import { test, expect } from 'bun:test';\nimport { add } from './calculator.mjs';\n" +
      "test('test_addition_r4', () => expect(add(2, 3)).toBe(5));\n");
    const specPath = join(fixtureRoot, 'SPEC.md'), check = 'bun test ./test_calculator.test.mjs';
    const missing = (reason) => ({ argv: [], cwd: fixtureRoot, exit_code: 127, stdout: '', stderr: reason, duration_ms: 0, timed_out: false });
    const specPrompt = copied.spec ? `You are running the Bonfyre R4 disposable Cavekit fixture.\nRead and follow the installed Cavekit skill exactly from:\n${copied.spec}\n\nWork only inside ${fixtureRoot}.\nCreate ${specPath}. It must contain these exact top-level markers:\n${SECTION_MARKERS.join('\n')}\n\nGoal: repair calculator.add while preserving a durable executable specification.\nConstraint: do not edit calculator.mjs yet.\nInvariant: add(2,3) must equal 5.\nVerification command: ${check}\nRecord the known failing test as an unresolved bug entry.\nDo not touch files outside the fixture.` : '';
    const specCall = copied.spec ? await claudePrint(specPrompt, fixtureRoot, turns) : missing('Cavekit spec skill not found');
    const sectionsOk = SECTION_MARKERS.every(marker => read(specPath).includes(marker));
    const failure = await command(['bun', 'test', './test_calculator.test.mjs'], fixtureRoot);
    const failureObserved = failure.exit_code !== 0 && outputText(failure).includes('test_addition_r4');
    const backpropPrompt = copied.backprop ? `You are running the Bonfyre R4 disposable Cavekit failure-backprop fixture.\nRead and follow this installed Cavekit skill:\n${copied.backprop}\n\nWork only inside ${fixtureRoot}.\nThe verification command failed:\n${check}\n\nUpdate ${specPath} without deleting ${SECTION_MARKERS.join(' ')}.\nUnder §B add bug ID B-R4-001 with:\n- observed symptom\n- root cause\n- exact regression command\n- affected invariant\n\nUnder §V add generalized invariant ID V-R4-002 stating that arithmetic operators\nmust preserve the operation named by the public function.\nDo not edit calculator.mjs. Do not touch files outside the fixture.` : '';
    const backpropCall = copied.backprop ? await claudePrint(backpropPrompt, fixtureRoot, turns) : missing('Cavekit backprop skill not found');
    const finalText = read(specPath), bugOk = ['B-R4-001', 'test_calculator.test.mjs', 'root'].every(token => finalText.includes(token));
    const invariantOk = finalText.includes('V-R4-002'), specPass = specCall.exit_code === 0 && sectionsOk;
    const backpropPass = backpropCall.exit_code === 0 && failureObserved && bugOk && invariantOk;
    const data = [{ skills: copied }, { spec_call: evidence(specCall) }, { verification_failure: evidence(failure) },
      { backprop_call: evidence(backpropCall) }, { spec_path: specPath, spec_text: finalText, sections_ok: sectionsOk, bug_ok: bugOk, invariant_ok: invariantOk }];
    return { provider: 'cavekit', certificates: await sealMany('cavekit', [
      ['spec.compile', specPass ? 'passed' : 'failed', specPass ? 1 : 0,
        [{ claim: 'Cavekit produced durable SPEC.md.', passed: regular(specPath) },
          { claim: 'All six fixed sections are present.', passed: sectionsOk }],
        specPass ? [] : ['Skill discovery, Claude execution, or fixed-section assertion failed.']],
      ['spec.invariant_backprop', backpropPass ? 'passed' : 'failed', backpropPass ? 1 : 0,
        [{ claim: 'Known regression was executed and observed.', passed: failureObserved },
          { claim: 'B-R4-001 records root cause and regression command.', passed: bugOk },
          { claim: 'V-R4-002 was generalized.', passed: invariantOk }],
        backpropPass ? [] : ['Failure-to-bug/invariant backpropagation was incomplete.']],
      ['review.diff', 'partial', 0.4, [{ claim: 'SPEC artifact is available for review.', passed: regular(specPath) }],
        ['R4 does not certify Cavekit as a standalone diff engine.']],
      ['scheduler.wave_execution', 'partial', 0.4, [], ['Cavekit v4 durable-spec discipline is not a parallel scheduler.']],
    ], fixtureRoot, data, true, runId) };
  }

  async function tmuxSessions() {
    const result = await command(['tmux', 'list-sessions', '-F', '#{session_name}'], root, 15_000);
    return result.exit_code === 0 ? new Set(result.stdout.split(/\r?\n/).map(item => item.trim()).filter(Boolean)) : new Set();
  }
  async function captureTmux(sessions, marker, seconds) {
    const deadline = now() + seconds, probes = [];
    let bestSession = null, bestOutput = '';
    while (now() < deadline) {
      for (const session of [...sessions].sort()) {
        const capture = await command(['tmux', 'capture-pane', '-p', '-S', '-500', '-t', session], root, 10_000);
        probes.push(evidence(capture));
        const text = outputText(capture);
        if (text.length > bestOutput.length) { bestSession = session; bestOutput = text; }
        if (text.includes(marker)) return { session, output: text, probes };
      }
      await sleep(2000);
    }
    return { session: bestSession, output: bestOutput, probes };
  }
  async function certifyAgentDeck(runId, fixtureRoot, resultRoot) {
    mkdirSync(fixtureRoot); mkdirSync(resultRoot);
    const suffix = runId.split('-').at(-1), profile = `bonfyre-r4-${suffix}`, title = `r4-${suffix}`;
    const forkTitle = `${title}-fork`, marker = `AGENT-DECK-R4-READY-${suffix}`;
    const before = await tmuxSessions();
    const launch = await command(['agent-deck', '-p', profile, 'launch', fixtureRoot, '-t', title,
      '-c', 'claude', '-m', `Reply exactly ${marker}. Do not use tools.`], root, 180_000);
    await sleep(3000);
    const afterLaunch = await tmuxSessions(), created = new Set([...afterLaunch].filter(item => !before.has(item)));
    const firstCapture = await captureTmux(created, marker, 90);
    const fork = await command(['agent-deck', '-p', profile, 'session', 'fork', title, '-t', forkTitle], root, 120_000);
    await sleep(2000);
    const afterFork = await tmuxSessions(), forkCreated = new Set([...afterFork].filter(item => !afterLaunch.has(item)));
    const secondCapture = await captureTmux(forkCreated, marker, 30);
    const cleanup = [];
    for (const target of [forkTitle, title]) {
      cleanup.push(await command(['agent-deck', '-p', profile, 'session', 'stop', target], root, 30_000));
      cleanup.push(await command(['agent-deck', '-p', profile, 'session', 'remove', target], root, 30_000));
    }
    // Never kill an unrelated tmux session merely because it appeared while
    // the fixture ran. A failed provider cleanup remains visible in the result.
    const finalSessions = await tmuxSessions();
    const fixtureSessions = new Set([...created, ...forkCreated]);
    const cleanupVerified = ![...fixtureSessions].some(item => finalSessions.has(item));
    const launchOk = launch.exit_code === 0, markerOk = firstCapture.output.includes(marker);
    const observable = created.size > 0 && firstCapture.session !== null;
    const forkOk = fork.exit_code === 0 && (forkCreated.size > 0 || secondCapture.session !== null);
    const data = [{ profile, title, fork_title: forkTitle }, { tmux_before: [...before].sort() }, { launch: evidence(launch) },
      { tmux_after_launch: [...afterLaunch].sort(), created: [...created].sort() },
      { capture_session: firstCapture.session, captured: firstCapture.output, capture_probes: firstCapture.probes },
      { fork: evidence(fork) }, { tmux_after_fork: [...afterFork].sort(), fork_created: [...forkCreated].sort() },
      { fork_capture_session: secondCapture.session, fork_capture: secondCapture.output, fork_capture_probes: secondCapture.probes },
      { cleanup: cleanup.map(evidence) }, { tmux_final: [...finalSessions].sort() }];
    return { provider: 'agent-deck', certificates: await sealMany('agent-deck', [
      ['fleet.pty', launchOk && markerOk && cleanupVerified ? 'passed' : 'failed', launchOk && markerOk && cleanupVerified ? 1 : 0,
        [{ claim: 'Agent Deck launched the disposable Claude PTY.', passed: launchOk },
          { claim: 'The PTY produced the requested marker.', passed: markerOk },
          { claim: 'Fixture PTYs were cleaned.', passed: cleanupVerified }],
        markerOk ? [] : ['Claude output marker was not observable through tmux capture.']],
      ['fleet.status_detection', observable && cleanupVerified ? 'passed' : 'failed', observable && cleanupVerified ? 1 : 0,
        [{ claim: 'New provider-owned tmux session was observable.', passed: observable },
          { claim: 'Registry cleanup is externally observable.', passed: cleanupVerified }],
        observable ? [] : ['Agent Deck launch did not create an observable tmux session.']],
      ['session.fork', forkOk && cleanupVerified ? 'passed' : 'partial', forkOk && cleanupVerified ? 1 : 0.4,
        [{ claim: 'Native Agent Deck fork command succeeded.', passed: fork.exit_code === 0 },
          { claim: 'Fork created an independently observable session.', passed: forkCreated.size > 0 || secondCapture.session !== null },
          { claim: 'Fork cleanup completed.', passed: cleanupVerified }],
        forkOk ? [] : ['Native fork returned but an independent PTY was not conclusively observed.']],
      ['session.resume', 'partial', 0.4, [], ['A provider restart/resume continuity fixture remains required.']],
    ], fixtureRoot, data, cleanupVerified, runId) };
  }
  return Object.freeze({ runSelected });
}

function regularStore(store) {
  return typeof store.home === 'string' && typeof store.runRoot === 'string' &&
    ['sealCertificate', 'verifyReceipts', 'writeReceipt'].every(name => typeof store[name] === 'function');
}
