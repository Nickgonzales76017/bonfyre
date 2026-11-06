import { test, expect } from 'bun:test';
import { createHash } from 'node:crypto';
import { chmodSync, existsSync, mkdirSync, mkdtempSync, readFileSync, realpathSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createCanaryFixtures, inspectCanaryProviderExecutable } from '../canary_fixtures.mjs';

const sha = value => createHash('sha256').update(value).digest('hex');
function fixture() {
  const base = mkdtempSync(join(tmpdir(), 'bonfyre-canary-fixtures-'));
  const projectRoot = join(base, 'project'), home = join(base, 'home'), runRoot = join(home, 'runs');
  mkdirSync(projectRoot); mkdirSync(home); mkdirSync(runRoot);
  const receipts = [], certificates = [];
  const store = { home, runRoot,
    async sealCertificate(body) {
      const path = join(home, `${body.provider}-${body.capability}.json`);
      const certificate = { ...body, schema: 'bonfyre.provider-shadow-certificate.v1',
        digest: sha(JSON.stringify(body)), path };
      writeFileSync(path, JSON.stringify(certificate)); certificates.push(certificate);
      return certificate;
    },
    verifyReceipts: () => ({ verified: true, receipts: receipts.length }),
    async writeReceipt(body) {
      const receipt = { ...body, seq: receipts.length + 1, digest: sha(JSON.stringify(body)) };
      receipts.push(receipt); return receipt;
    },
  };
  return { base, projectRoot, home, store, receipts, certificates,
    cleanup: () => rmSync(base, { recursive: true, force: true }) };
}
function result(argv, options, stdout = '', exit_code = 0) {
  return { argv, cwd: options.cwd, stdout, stderr: '', exit_code, duration_ms: 1 };
}
function rtkRunner({ testExit = 1, compactTestExit = 1, compactTestOutput = 'R4 failure sentinel\n' } = {}) {
  const calls = [];
  const run = async (argv, options) => {
    calls.push({ argv, options });
    const joined = argv.join(' '), compact = argv[0] === 'rtk';
    if (joined.includes('status')) return result(argv, options,
      compact ? 'src/important.mjs changed\n' : 'src/important.mjs changed\n' + 'S'.repeat(10_000));
    if (joined.includes('diff')) return result(argv, options,
      compact ? 'src/important.mjs\n' : 'src/important.mjs R4 failure sentinel\n' + 'D'.repeat(10_000));
    if (joined.includes('test')) return result(argv, options,
      compact ? compactTestOutput : 'R4 failure sentinel R4_EXPECTED_SENTINEL\n',
      compact ? compactTestExit : testExit);
    return result(argv, options);
  };
  return { run, calls };
}

test('RTK Bun fixture certifies real parity predicates, sanitizes Git env, and marks skipped recursion partial', async () => {
  const f = fixture();
  try {
    const { run, calls } = rtkRunner();
    const runner = createCanaryFixtures({ store: f.store, projectRoot: f.projectRoot, run,
      env: { ...process.env, GIT_DIR: '/forbidden/other/repo', GIT_EXTERNAL_DIFF: '/forbidden/hook',
        GIT_CONFIG_COUNT: '1', GIT_CONFIG_KEY_0: 'core.fsmonitor', GIT_CONFIG_VALUE_0: '/forbidden/hook' } });
    const document = await runner.runSelected(['rtk']);
    expect(document.summary.passed).toBe(1);
    expect(document.certificates[0].state).toBe('passed');
    expect(document.fixture_revision).toBe('bonfyre-canary-fixtures-bun-v1');
    expect(document.certificates[0].evidence[0].historical_python_fixture_parity).toBe(false);
    expect(document.recursive_ingestion[0].result.state).toBe('skipped');
    expect(document.receipt.state).toBe('partial');
    expect(existsSync(document.path)).toBe(true);
    expect(JSON.parse(readFileSync(document.path, 'utf8')).schema).toBe('bonfyre.fixture-correction-r4.run.v1');
    expect(calls.every(item => item.options.env.GIT_DIR === undefined && item.options.env.GIT_EXTERNAL_DIFF === undefined)).toBe(true);
    expect(calls.every(item => item.options.env.GIT_CONFIG_KEY_0 === 'core.hooksPath' && item.options.env.GIT_CONFIG_VALUE_1 === 'false')).toBe(true);
    expect(calls.every(item => !item.argv.includes('python3'))).toBe(true);
  } finally { f.cleanup(); }
});

test('RTK summary-only test output fails the identity claim even with exit parity and strong reduction', async () => {
  const f = fixture();
  try {
    const { run } = rtkRunner({ compactTestOutput: '1 pass\n1 fail\n101 expect() calls\n' });
    const document = await createCanaryFixtures({ store: f.store, projectRoot: f.projectRoot, run }).runSelected(['rtk']);
    const certificate = document.certificates[0];
    expect(certificate.state).toBe('failed');
    expect(certificate.claims.find(item => item.claim === 'RTK preserves exit behavior.').passed).toBe(true);
    expect(certificate.claims.find(item => item.claim === 'RTK preserves failing-test identity.').passed).toBe(false);
  } finally { f.cleanup(); }
});

test('RTK cannot pass when both test launchers fail even if diff mentions the sentinel', async () => {
  const f = fixture();
  try {
    const { run } = rtkRunner({ testExit: 127, compactTestExit: 127 });
    const document = await createCanaryFixtures({ store: f.store, projectRoot: f.projectRoot, run }).runSelected(['rtk']);
    expect(document.certificates[0].state).toBe('failed');
    expect(document.certificates[0].claims.find(item => item.claim.includes('intended assertion failure')).passed).toBe(false);
  } finally { f.cleanup(); }
});

test('recursive projection journals intent before each effect and retains artifact identity on uncertainty', async () => {
  const f = fixture();
  try {
    const { run } = rtkRunner(), calls = [];
    const runner = createCanaryFixtures({ store: f.store, projectRoot: f.projectRoot, run,
      recursiveCall: async (name, args) => {
        expect(f.receipts.at(-1).kind).toBe('certificate.projection.attempt');
        calls.push({ name, args });
        if (name === 'artifact_ingest') return { artifact_id: 'artifact-1' };
        throw new Error('transport dropped after send');
      } });
    const document = await runner.runSelected(['rtk']);
    expect(calls.map(item => item.name)).toEqual(['artifact_ingest', 'token_artifact_compile']);
    expect(document.recursive_ingestion[0].result).toMatchObject({ state: 'outcome_uncertain',
      retained_artifact_id: 'artifact-1', no_automatic_retry: true });
    expect(document.receipt.state).toBe('partial');
    expect(f.receipts.map(item => [item.kind, item.state])).toEqual([
      ['fixture.run.started', 'started'],
      ['certificate.projection.attempt', 'started'], ['certificate.projection.completion', 'passed'],
      ['certificate.projection.attempt', 'started'], ['certificate.projection.completion', 'outcome_uncertain'],
      ['fixture.run.completed', 'partial'],
    ]);
    expect(f.receipts[2].payload.artifact_id).toBe('artifact-1');
  } finally { f.cleanup(); }
});

test('no durable projection intent means no remote call', async () => {
  const f = fixture();
  try {
    const { run } = rtkRunner(); let invoked = 0;
    f.store.writeReceipt = async () => { throw new Error('receipt store unavailable'); };
    const runner = createCanaryFixtures({ store: f.store, projectRoot: f.projectRoot, run,
      recursiveCall: async () => { invoked++; return { artifact_id: 'must-not-happen' }; } });
    await expect(runner.runSelected(['rtk'])).rejects.toThrow('receipt store unavailable');
    expect(invoked).toBe(0);
  } finally { f.cleanup(); }
});

test('Bernstein Python launcher is explicitly classified; actual CLI path and boundary are recorded', async () => {
  const f = fixture();
  try {
    const bin = join(f.base, 'bin'); mkdirSync(bin);
    const launcher = join(bin, 'bernstein');
    writeFileSync(launcher, '#!/bin/sh\n\'\'\'exec\' "/opt/provider/bin/python" "$0" "$@"\n\' \'\'\nimport sys\nfrom bernstein.cli.main import cli\n');
    chmodSync(launcher, 0o755);
    expect(inspectCanaryProviderExecutable('bernstein', bin)).toMatchObject({ path: realpathSync(launcher),
      kind: 'python_launcher', interpreter: '/opt/provider/bin/python', sha256: sha(readFileSync(launcher)) });
    const calls = [];
    const run = async (argv, options) => {
      calls.push(argv);
      return result(argv, options, argv.includes('replay') ? '' : 'fixture', argv.includes('replay') ? 1 : 0);
    };
    const document = await createCanaryFixtures({ store: f.store, projectRoot: f.projectRoot,
      env: { ...process.env, PATH: bin }, run }).runSelected(['bernstein']);
    expect(calls.every(argv => argv[0] === realpathSync(launcher))).toBe(true);
    expect(document.runtime_boundaries[0]).toMatchObject({ kind: 'declared-third-party-python-cli',
      launcher: realpathSync(launcher), launcher_sha256: sha(readFileSync(launcher)) });
    expect(document.certificates.find(item => item.capability === 'scheduler.persisted_dag').state).toBe('partial');
    expect(document.certificates.every(item => item.limitations.some(value => value.includes('declared Python dependency')))).toBe(true);
  } finally { f.cleanup(); }
});

test('missing Cavekit skills never yield fabricated spec/backprop success', async () => {
  const f = fixture();
  try {
    const calls = [];
    const run = async (argv, options) => { calls.push(argv); return result(argv, options, 'test_addition_r4 failure', 1); };
    const document = await createCanaryFixtures({ store: f.store, projectRoot: f.projectRoot,
      home: f.home, run }).runSelected(['cavekit']);
    expect(document.certificates.map(item => item.state)).toEqual(['failed', 'failed', 'partial', 'partial']);
    expect(calls).toEqual([['bun', 'test', './test_calculator.test.mjs']]);
  } finally { f.cleanup(); }
});

test('Cavekit does not replay an uncertain Claude attempt under alternate CLI syntax', async () => {
  const f = fixture();
  try {
    for (const name of ['spec', 'backprop']) {
      const directory = join(f.projectRoot, '.agents', 'skills', name);
      mkdirSync(directory, { recursive: true });
      writeFileSync(join(directory, 'SKILL.md'), `# Cavekit ${name}\n§G §B\n`);
    }
    const calls = [];
    const run = async (argv, options) => {
      if (argv[0] === 'claude') calls.push(argv);
      return result(argv, options, argv[0] === 'bun' ? 'test_addition_r4 failure' : '', 1);
    };
    const document = await createCanaryFixtures({ store: f.store, projectRoot: f.projectRoot,
      home: f.home, run }).runSelected(['cavekit']);
    expect(calls).toHaveLength(2); // one spec attempt and one distinct backprop stage
    expect(calls.every(argv => argv[1] === '-p' && !argv.includes('--print'))).toBe(true);
    expect(document.certificates.slice(0, 2).map(item => item.state)).toEqual(['failed', 'failed']);
  } finally { f.cleanup(); }
});

test('Agent Deck missing PTY marker cannot pass and never kills unrelated tmux sessions', async () => {
  const f = fixture();
  try {
    let clock = 1_760_000_000;
    const calls = [];
    const run = async (argv, options) => {
      calls.push(argv);
      if (argv[0] === 'tmux' && argv[1] === 'list-sessions') return result(argv, options, 'unrelated-session\n');
      if (argv[0] === 'tmux' && argv[1] === 'capture-pane') return result(argv, options, 'no fixture marker');
      if (argv[0] === 'agent-deck' && ['launch', 'fork'].some(word => argv.includes(word))) return result(argv, options, '', 1);
      return result(argv, options);
    };
    const document = await createCanaryFixtures({ store: f.store, projectRoot: f.projectRoot, run,
      now: () => clock, sleep: async ms => { clock += ms / 1000; } }).runSelected(['agent-deck']);
    expect(document.certificates.map(item => item.state)).toEqual(['failed', 'failed', 'partial', 'partial']);
    expect(calls.some(argv => argv[0] === 'tmux' && argv[1] === 'kill-session')).toBe(false);
    expect(document.certificates[0].cleanup_verified).toBe(true);
  } finally { f.cleanup(); }
});
