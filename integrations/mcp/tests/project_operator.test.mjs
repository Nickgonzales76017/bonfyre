import { test, expect } from 'bun:test';
import { createHash } from 'node:crypto';
import { Database } from 'bun:sqlite';
import { chmodSync, mkdtempSync, mkdirSync, readFileSync, readdirSync, realpathSync, rmSync, symlinkSync, writeFileSync, existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { createProjectOperator } from '../project_operator.mjs';
import { projectRun } from '../project_runtime.mjs';
function fixture(env = {}) {
  const base = mkdtempSync(join(tmpdir(), 'bonfyre-project-operator-'));
  const root = join(base, 'repo'), home = join(base, 'home');
  mkdirSync(root); mkdirSync(home);
  const init = Bun.spawnSync(['git', 'init', '-q', root], { stdout: 'pipe', stderr: 'pipe' });
  if (init.exitCode !== 0) throw new Error(init.stderr.toString());
  mkdirSync(join(root, 'cmd', 'BonfyreFixture'), { recursive: true });
  const approval = join(home, 'approval.token'); writeFileSync(approval, 'fixture-approval\n');
  const invocations = [];
  const run = async (argv, options) => {
    invocations.push({ argv, options });
    return projectRun(argv, options);
  };
  const operator = createProjectOperator({ root, home, env: { ...process.env, BONFYRE_OPERATOR_HOME: join(home, 'operator'),
    BONFYRE_MCP_APPROVAL_FILE: approval, BONFYRE_MCP_PROJECT_WRITE_ENABLED: '1', BONFYRE_MCP_AUTONOMOUS_WRITE_ENABLED: '1',
    ...env }, run, status: async () => ({ root, fixture: true }) });
  return { base, root, home, approval, invocations, run, operator, cleanup: () => rmSync(base, { recursive: true, force: true }) };
}

function addPatch(path, text = 'fixture') {
  return `diff --git a/${path} b/${path}\nnew file mode 100644\n--- /dev/null\n+++ b/${path}\n@@ -0,0 +1 @@\n+${text}\n`;
}
function editPatch(path, before = 'old', after = 'new') {
  return `diff --git a/${path} b/${path}\n--- a/${path}\n+++ b/${path}\n@@ -1 +1 @@\n-${before}\n+${after}\n`;
}

test('source retirement archives exact reviewed scripts after native owner tests', async () => {
  const f = fixture();
  try {
    const nativeDir = join(f.root, 'cmd/BonfyreFixture/src');
    const scripts = join(f.root, 'scripts');
    mkdirSync(nativeDir, { recursive: true });
    mkdirSync(scripts, { recursive: true });
    const nativePath = 'cmd/BonfyreFixture/src/owner.c';
    const sourcePath = 'cmd/BonfyreFixture/legacy-source.mjs';
    const otherPath = 'cmd/BonfyreFixture/a-old-owner.mjs';
    writeFileSync(join(f.root, nativePath), 'int main(void){return 0;}\n');
    writeFileSync(join(f.root, sourcePath), 'export const oldOwner = true;\n');
    writeFileSync(join(f.root, otherPath), 'export const olderOwner = true;\n');
    writeFileSync(join(f.root, 'cmd/BonfyreFixture/Makefile'),
      'test:\n\t@cc src/owner.c -o native-fixture\n\t@./native-fixture\n');
    writeFileSync(join(scripts, 'retire.sh'),
      readFileSync(new URL('../../../scripts/retire.sh', import.meta.url)));
    chmodSync(join(scripts, 'retire.sh'), 0o755);
    const add = Bun.spawnSync(['git', '-C', f.root, 'add', '--', '.']);
    expect(add.exitCode).toBe(0);
    const commit = Bun.spawnSync(['git', '-C', f.root, '-c', 'user.name=Fixture',
      '-c', 'user.email=fixture@example.invalid', 'commit', '-qm', 'reviewed baseline']);
    expect(commit.exitCode).toBe(0);
    const hash = path => createHash('sha256').update(readFileSync(join(f.root, path))).digest('hex');
    const sourceHash = hash(sourcePath), otherHash = hash(otherPath), nativeHash = hash(nativePath);
    const args = { confirm: true, reason: 'Native owner has replaced the reviewed legacy source',
      native_source: nativePath, native_sha256: nativeHash,
      // Reverse-path order exercises canonical manifest sorting across files.
      entries: [{ path: sourcePath, sha256: sourceHash }, { path: otherPath, sha256: otherHash }] };
    await expect(f.operator.call('project_retire_sources',
      { ...args, entries: [{ path: 'cmd/BonfyreFixture/absent-source.mjs', sha256: sourceHash }] }))
      .rejects.toThrow('retirement source missing or changed');
    await expect(f.operator.call('project_retire_sources',
      { ...args, entries: [{ path: sourcePath, sha256: 'f'.repeat(64) }] }))
      .rejects.toThrow('missing or changed');
    await expect(f.operator.call('project_retire_sources',
      { ...args, entries: [{ path: 'cmd/BonfyreFixture/tests/legacy.test.mjs', sha256: sourceHash }] }))
      .rejects.toThrow('retiring tests');
    expect(existsSync(join(f.root, sourcePath))).toBe(true);
    const result = await f.operator.call('project_retire_sources', args);
    expect(result.retired).toBe(true);
    expect(result.receipt.archive_verified).toBe(true);
    expect(result.receipt.manifest_sha256).toMatch(/^[a-f0-9]{64}$/);
    expect(result.receipt.files).toHaveLength(2);
    expect(result.receipt.native_tests_after).toBe(0);
    expect(existsSync(result.receipt.archive)).toBe(true);
    expect(existsSync(join(f.root, sourcePath))).toBe(false);
    expect(existsSync(join(f.root, otherPath))).toBe(false);
    expect(hash(nativePath)).toBe(nativeHash);
    expect(existsSync(join(f.home, 'operator', 'patch-receipts',
      result.receipt.id + '.retirement.json'))).toBe(true);
    const second = 'cmd/BonfyreFixture/requires-presence.mjs';
    writeFileSync(join(f.root, second), 'export const stillRequired = true;\n');
    writeFileSync(join(f.root, 'cmd/BonfyreFixture/Makefile'),
      'test:\n\t@test -f requires-presence.mjs\n\t@cc src/owner.c -o native-fixture\n\t@./native-fixture\n');
    expect(Bun.spawnSync(['git', '-C', f.root, 'add', '--', second,
      'cmd/BonfyreFixture/Makefile']).exitCode).toBe(0);
    expect(Bun.spawnSync(['git', '-C', f.root, '-c', 'user.name=Fixture',
      '-c', 'user.email=fixture@example.invalid', 'commit', '-qm', 'reviewed dependent source']).exitCode).toBe(0);
    const denied = await f.operator.call('project_retire_sources',
      { ...args, entries: [{ path: second, sha256: hash(second) }] });
    expect(denied.retired).toBe(false);
    expect(denied.reason).toBe('posttest_failed');
    expect(denied.restored).toBe(true);
    expect(existsSync(join(f.root, second))).toBe(true);
  } finally { f.cleanup(); }
});

test('path preflight reports write constraints and exact preimages before preparing a patch without creating state', async () => {
  const f = fixture({ BONFYRE_MCP_PROJECT_WRITE_ENABLED: '0' });
  try {
    writeFileSync(join(f.root, 'cmd/BonfyreFixture/dirty.txt'), 'uncommitted');
    const result = await f.operator.call('project_patch_preview', { paths: ['cmd/BonfyreFixture/dirty.txt', 'architecture/new.yaff'] });
    expect(result).toMatchObject({ kind: 'path_preflight', low_risk_paths_eligible: false,
      manual_write_enabled: false, manual_approval_required: true, patch_validation_required: true });
    expect(result.metadata.files[0]).toMatchObject({ exists: true, dirty_before: true });
    expect(result.metadata.files[0].sha256).toMatch(/^[a-f0-9]{64}$/);
    expect(result.metadata.files[1]).toMatchObject({ exists: false, sha256: null });
    expect(result.metadata.dirty_conflicts).toContain('cmd/BonfyreFixture/dirty.txt');
    expect(result.policy_errors.some(x => x.includes('allowed roots'))).toBe(true);
    expect(existsSync(join(f.home, 'operator'))).toBe(false);
    expect(JSON.stringify(result)).not.toContain('fixture-approval');
    expect(f.invocations.every(({ argv }) => argv.includes('--literal-pathspecs') && argv.includes('architecture/new.yaff'))).toBe(true);
    const clean = await f.operator.call('project_patch_preview', { paths: ['cmd/BonfyreFixture/new.txt'] });
    expect(clean.low_risk_paths_eligible).toBe(true);
    for (const args of [{}, { paths: [] }, { paths: ['../escape'] }, { paths: ['cmd/a', 'cmd/a'] },
      { paths: Array(17).fill('cmd/a') }, { paths: ['cmd/a'], patch: '' }, { paths: ['cmd/a'], extra: true }])
      await expect(f.operator.call('project_patch_preview', args)).rejects.toThrow();
  } finally { f.cleanup(); }
});

test('preview and low-risk apply use Git affected-path proof, policy, receipts and exact preimage rollback', async () => {
  const f = fixture();
  try {
    const path = 'cmd/BonfyreFixture/new.txt', patch = addPatch(path);
    const preview = await f.operator.call('project_patch_preview', { patch });
    expect(preview.valid).toBe(true);
    expect(preview.low_risk_eligible).toBe(true);
    expect(preview.metadata.files.map(item => item.path)).toEqual([path]);
    expect(existsSync(join(f.root, path))).toBe(false);
    expect(f.invocations.some(item => item.argv.join(' ').includes('apply --numstat -z -'))).toBe(true);
    const applied = await f.operator.call('project_apply_low_risk_patch', { patch });
    expect(applied.applied).toBe(true);
    expect(applied.receipt.mode).toBe('autonomous-low-risk');
    expect(applied.receipt.post_hashes[path]).toMatch(/^[a-f0-9]{64}$/);
    expect(readFileSync(join(f.root, path), 'utf8')).toBe('fixture\n');
    const reverted = await f.operator.call('project_revert_receipt', { receipt_id: applied.receipt_id });
    expect(reverted.reverted).toBe(true);
    expect(existsSync(join(f.root, path))).toBe(false);
    expect(reverted.restoration.files[0]).toMatchObject({ path, sha256: null, expected_sha256: null });
  } finally { f.cleanup(); }
});

test('manual gate requires token; postimage CAS refuses another writer and receipts reject traversal', async () => {
  const f = fixture();
  try {
    const path = 'cmd/BonfyreFixture/manual.txt', patch = addPatch(path);
    await expect(f.operator.call('project_apply_patch', { patch, approval_token: 'wrong' })).rejects.toThrow('approval');
    const applied = await f.operator.call('project_apply_patch', { patch, approval_token: 'fixture-approval' });
    expect(applied.applied).toBe(true);
    const receiptDir = join(f.home, 'operator', 'patch-receipts');
    const receipt = JSON.parse(readFileSync(join(receiptDir, `${applied.receipt_id}.json`), 'utf8'));
    expect(receipt.post_hashes[path]).toMatch(/^[a-f0-9]{64}$/);
    writeFileSync(join(f.root, path), 'another writer\n');
    const blocked = await f.operator.call('project_revert_receipt', { receipt_id: applied.receipt_id });
    expect(blocked.reverted).toBe(false);
    expect(readFileSync(join(f.root, path), 'utf8')).toBe('another writer\n');
    await expect(f.operator.call('project_revert_receipt', { receipt_id: '../escape' })).rejects.toThrow('invalid patch receipt id');
    await expect(f.operator.call('project_commit_receipt', { receipt_id: '../escape' })).rejects.toThrow('automatic operator commits are disabled');
    const disabled = createProjectOperator({ root: f.root, home: f.home, env: { BONFYRE_OPERATOR_HOME: join(f.home, 'operator') }, run: f.run, status: async () => ({}) });
    await expect(disabled.call('project_apply_patch', { patch, approval_token: 'fixture-approval' })).rejects.toThrow('write gate');
  } finally { f.cleanup(); }
});

test('existing-file preimage is restored; malformed legacy receipts cannot fake a revert', async () => {
  const f = fixture();
  try {
    const path = 'cmd/BonfyreFixture/existing.txt';
    writeFileSync(join(f.root, path), 'old\n');
    const applied = await f.operator.call('project_apply_patch', { patch: editPatch(path), approval_token: 'fixture-approval' });
    expect(applied.applied).toBe(true);
    expect(readFileSync(join(f.root, path), 'utf8')).toBe('new\n');
    const restored = await f.operator.call('project_revert_receipt', { receipt_id: applied.receipt_id });
    expect(restored.reverted).toBe(true);
    expect(readFileSync(join(f.root, path), 'utf8')).toBe('old\n');
    const another = await f.operator.call('project_apply_patch', { patch: editPatch(path), approval_token: 'fixture-approval' });
    const metaPath = join(f.home, 'operator', 'patch-receipts', `${another.receipt_id}.json`);
    const record = JSON.parse(readFileSync(metaPath, 'utf8'));
    delete record.post_hashes;
    writeFileSync(metaPath, JSON.stringify(record));
    await expect(f.operator.call('project_revert_receipt', { receipt_id: another.receipt_id })).rejects.toThrow('complete postimages');
    expect(readFileSync(join(f.root, path), 'utf8')).toBe('new\n');
  } finally { f.cleanup(); }
});

test('legacy sorted post-hash keys compare by path; corrupt later preimage blocks all restoration', async () => {
  const f = fixture();
  try {
    const first = 'cmd/BonfyreFixture/z.txt', second = 'cmd/BonfyreFixture/a.txt';
    writeFileSync(join(f.root, first), 'old\n'); writeFileSync(join(f.root, second), 'old\n');
    const result = await f.operator.call('project_apply_patch', {
      patch: editPatch(first) + editPatch(second), approval_token: 'fixture-approval',
    });
    expect(result.applied).toBe(true);
    const meta = join(f.home, 'operator', 'patch-receipts', `${result.receipt_id}.json`);
    const receipt = JSON.parse(readFileSync(meta, 'utf8'));
    receipt.post_hashes = Object.fromEntries(Object.entries(receipt.post_hashes).sort(([a], [b]) => a.localeCompare(b)));
    writeFileSync(meta, JSON.stringify(receipt));
    writeFileSync(receipt.preimages[1].snapshot_path, 'corrupt\n');
    await expect(f.operator.call('project_revert_receipt', { receipt_id: result.receipt_id })).rejects.toThrow('preimage missing or corrupt');
    expect(readFileSync(join(f.root, first), 'utf8')).toBe('new\n');
    expect(readFileSync(join(f.root, second), 'utf8')).toBe('new\n');
    writeFileSync(receipt.preimages[1].snapshot_path, 'old\n');
    const reverted = await f.operator.call('project_revert_receipt', { receipt_id: result.receipt_id });
    expect(reverted.reverted).toBe(true);
    expect(readFileSync(join(f.root, first), 'utf8')).toBe('old\n');
    expect(readFileSync(join(f.root, second), 'utf8')).toBe('old\n');
  } finally { f.cleanup(); }
});

test('Git observation disables a configured fsmonitor hook and strips hostile Git environment', async () => {
  const f = fixture();
  try {
    const marker = join(f.base, 'fsmonitor-ran');
    const hook = join(f.base, 'fsmonitor.sh');
    writeFileSync(hook, `#!/bin/sh\nprintf hook > "${marker}"\n`);
    chmodSync(hook, 0o755);
    const config = Bun.spawnSync(['git', 'config', 'core.fsmonitor', hook], { cwd: f.root, stdout: 'pipe', stderr: 'pipe' });
    expect(config.exitCode).toBe(0);
    const invocations = [];
    const env = { ...process.env, BONFYRE_OPERATOR_HOME: join(f.home, 'operator'),
      BONFYRE_MCP_AUTONOMOUS_WRITE_ENABLED: '1', GIT_DIR: join(f.base, 'outside.git'),
      GIT_EXTERNAL_DIFF: hook, GIT_CONFIG_COUNT: '1', GIT_CONFIG_KEY_0: 'core.fsmonitor', GIT_CONFIG_VALUE_0: hook };
    const run = async (argv, options) => { invocations.push({ argv, options }); return projectRun(argv, options); };
    const owner = createProjectOperator({ root: f.root, home: f.home, env, run, status: async () => ({}) });
    const preview = await owner.call('project_patch_preview', { patch: addPatch('cmd/BonfyreFixture/safe.txt') });
    expect(preview.valid).toBe(true);
    expect(existsSync(marker)).toBe(false);
    const gitReads = invocations.filter(item => item.argv.includes('diff') || item.argv.includes('ls-files'));
    expect(gitReads).toHaveLength(3);
    expect(gitReads.every(item => item.argv.includes('core.fsmonitor=false'))).toBe(true);
    expect(gitReads.every(item => item.options.env.GIT_DIR === undefined && item.options.env.GIT_CONFIG_COUNT === undefined && item.options.env.GIT_EXTERNAL_DIFF === undefined)).toBe(true);
    expect(gitReads.every(item => item.options.env.GIT_OPTIONAL_LOCKS === '0')).toBe(true);
  } finally { f.cleanup(); }
});

test('mode-changing and file-deletion patches refuse until mode-preserving rollback has an owner', async () => {
  const f = fixture();
  try {
    const path = 'cmd/BonfyreFixture/mode.txt';
    writeFileSync(join(f.root, path), 'old\n');
    const modePatch = `diff --git a/${path} b/${path}\nold mode 100644\nnew mode 100755\n`;
    const deletePatch = `diff --git a/${path} b/${path}\ndeleted file mode 100644\n--- a/${path}\n+++ /dev/null\n@@ -1 +0,0 @@\n-old\n`;
    await expect(f.operator.call('project_patch_preview', { patch: modePatch })).rejects.toThrow('mode');
    await expect(f.operator.call('project_apply_patch', { patch: deletePatch, approval_token: 'fixture-approval' })).rejects.toThrow('mode');
    expect(readFileSync(join(f.root, path), 'utf8')).toBe('old\n');
  } finally { f.cleanup(); }
});

test('rollback verifies modes as well as bytes and refuses legacy receipts without mode evidence', async () => {
  const f = fixture();
  try {
    const path = 'cmd/BonfyreFixture/chmod.txt', absolute = join(f.root, path);
    writeFileSync(absolute, 'old\n'); chmodSync(absolute, 0o644);
    const applied = await f.operator.call('project_apply_patch', { patch: editPatch(path), approval_token: 'fixture-approval' });
    expect(applied.applied).toBe(true);
    chmodSync(absolute, applied.post_modes[path] ^ 0o100);
    const blocked = await f.operator.call('project_revert_receipt', { receipt_id: applied.receipt_id });
    expect(blocked.reverted).toBe(false);
    expect(readFileSync(absolute, 'utf8')).toBe('new\n');
    chmodSync(absolute, applied.post_modes[path]);
    const meta = join(f.home, 'operator', 'patch-receipts', `${applied.receipt_id}.json`);
    const receipt = JSON.parse(readFileSync(meta, 'utf8'));
    delete receipt.post_modes;
    writeFileSync(meta, JSON.stringify(receipt));
    await expect(f.operator.call('project_revert_receipt', { receipt_id: applied.receipt_id })).rejects.toThrow('mode evidence');
    receipt.post_modes = applied.post_modes;
    writeFileSync(meta, JSON.stringify(receipt));
    expect((await f.operator.call('project_revert_receipt', { receipt_id: applied.receipt_id })).reverted).toBe(true);
    expect(readFileSync(absolute, 'utf8')).toBe('old\n');
  } finally { f.cleanup(); }
});

test('second hash check blocks a simulated external write; no apply command follows', async () => {
  const f = fixture();
  try {
    const path = 'cmd/BonfyreFixture/raced.txt', patch = addPatch(path);
    let injected = false;
    const run = async (argv, options) => {
      const result = await f.run(argv, options);
      if (!injected && argv.join(' ').includes('apply --check') && result.exit_code === 0) {
        writeFileSync(join(f.root, path), 'external\n'); injected = true;
      }
      return result;
    };
    const owner = createProjectOperator({ root: f.root, home: f.home,
      env: { BONFYRE_OPERATOR_HOME: join(f.home, 'operator'), BONFYRE_MCP_APPROVAL_FILE: f.approval,
        BONFYRE_MCP_PROJECT_WRITE_ENABLED: '1' }, run, status: async () => ({}) });
    const blocked = await owner.call('project_apply_patch', { patch, approval_token: 'fixture-approval' });
    expect(blocked.applied).toBe(false);
    expect(blocked.reason).toContain('after git apply --check');
    expect(readFileSync(join(f.root, path), 'utf8')).toBe('external\n');
    expect(f.invocations.some(item => item.argv.includes('--whitespace=fix'))).toBe(false);
  } finally { f.cleanup(); }
});

test('bounded runner timeout during apply records uncertain effect, never applied success', async () => {
  const f = fixture();
  try {
    const path = 'cmd/BonfyreFixture/timeout.txt';
    const run = async (argv, options) => argv.includes('--whitespace=fix')
      ? { argv, exit_code: 124, timeout: true, stdout: '', stderr: '', duration_ms: 120_000 }
      : f.run(argv, options);
    const owner = createProjectOperator({ root: f.root, home: f.home,
      env: { BONFYRE_OPERATOR_HOME: join(f.home, 'operator'), BONFYRE_MCP_APPROVAL_FILE: f.approval,
        BONFYRE_MCP_PROJECT_WRITE_ENABLED: '1' }, run, status: async () => ({}) });
    const result = await owner.call('project_apply_patch', { patch: addPatch(path), approval_token: 'fixture-approval' });
    expect(result.applied).toBe(false);
    expect(result.receipt.state).toBe('apply_outcome_uncertain');
    expect(result.apply_error).toContain('timed out');
    expect(existsSync(join(f.root, path))).toBe(false);
    await expect(owner.call('project_revert_receipt', { receipt_id: result.receipt_id })).rejects.toThrow('not an applied patch');
  } finally { f.cleanup(); }
});

test('commit admission uses applied receipt and postimage CAS without a real fixture commit', async () => {
  const f = fixture();
  try {
    const path = 'cmd/BonfyreFixture/commit.txt';
    const applied = await f.operator.call('project_apply_low_risk_patch', { patch: addPatch(path) });
    expect(applied.applied).toBe(true);
    const effects = [];
    const run = async (argv, options) => {
      if (argv[0] === 'git' && ['add', 'commit'].includes(argv[1])) {
        effects.push(argv);
        return { argv, exit_code: 0, stdout: 'fixture only', stderr: '', duration_ms: 0 };
      }
      return f.run(argv, options);
    };
    const owner = createProjectOperator({ root: f.root, home: f.home,
      env: { BONFYRE_OPERATOR_HOME: join(f.home, 'operator'), BONFYRE_MCP_AUTO_COMMIT_ENABLED: '1' },
      run, status: async () => ({}) });
    await expect(owner.call('project_commit_receipt', { receipt_id: '../escape' })).rejects.toThrow('invalid patch receipt id');
    const committed = await owner.call('project_commit_receipt', { receipt_id: applied.receipt_id, message: 'fixture commit' });
    expect(committed.committed).toBe(true);
    expect(effects).toEqual([
      ['git', 'add', '--', path],
      ['git', 'commit', '-m', 'fixture commit', '--', path],
    ]);
    expect(JSON.parse(readFileSync(join(f.home, 'operator', 'patch-receipts', `${applied.receipt_id}.json`), 'utf8')).state).toBe('committed');
  } finally { f.cleanup(); }
});

test('policy and symlink confinement refuse sensitive, dirty and escaping patch effects', async () => {
  const f = fixture();
  try {
    const outside = join(f.base, 'outside.txt'); writeFileSync(outside, 'keep\n');
    symlinkSync(outside, join(f.root, 'cmd', 'BonfyreFixture', 'link.txt'));
    await expect(f.operator.call('project_patch_preview', { patch: addPatch('cmd/BonfyreFixture/link.txt') })).rejects.toThrow('symlink refused');
    await expect(f.operator.call('project_patch_preview', { patch: addPatch('../outside.txt') })).rejects.toThrow('unsafe project path');
    const secret = await f.operator.call('project_apply_low_risk_patch', { patch: addPatch('cmd/BonfyreFixture/secrets.txt') });
    expect(secret.applied).toBe(false);
    expect(secret.policy_errors.some(item => item.includes('sensitive'))).toBe(true);
    expect(readFileSync(outside, 'utf8')).toBe('keep\n');
    const receipts = join(f.home, 'operator', 'patch-receipts');
    expect(existsSync(receipts) ? readdirSync(receipts).length : 0).toBe(0);
  } finally { f.cleanup(); }
});

test('existing operator SQLite store keeps goal dedupe, pause and resume semantics', async () => {
  const f = fixture();
  try {
    const first = await f.operator.call('operator_goal_add', { title: 'Fixture goal', details: 'exact', priority: 4, dedupe_key: 'fixture:goal' });
    const second = await f.operator.call('operator_goal_add', { title: 'Updated', details: 'exact', priority: 3, dedupe_key: 'fixture:goal' });
    expect(second.id).toBe(first.id);
    expect(second.priority).toBe(4);
    expect(second.title).toBe('Updated');
    const paused = await f.operator.call('operator_pause', { reason: 'fixture' });
    expect(paused.paused).toBe(true);
    expect(paused.goals.queued).toBe(1);
    const resumed = await f.operator.call('operator_resume');
    expect(resumed.paused).toBe(false);
    const db = new Database(join(f.home, 'operator', 'operator.sqlite3'));
    expect(db.query('SELECT COUNT(*) AS n FROM goals').get().n).toBe(1);
    db.query("UPDATE goals SET state='in_progress' WHERE id=?").run(first.id);
    const active = await f.operator.call('operator_goal_add', { title: 'Keep current ownership', dedupe_key: 'fixture:goal' });
    expect(active.state).toBe('in_progress');
    db.close();
  } finally { f.cleanup(); }
});

test('backlog audit is read-only and STOP-gated supersession retains exact never-started goal history', async () => {
  const f = fixture();
  try {
    const duplicateA = await f.operator.call('operator_goal_add', { title: 'Repeated cleanup target', details: 'a', dedupe_key: 'fixture:cleanup-a' });
    const duplicateB = await f.operator.call('operator_goal_add', { title: 'Repeated cleanup target', details: 'b', dedupe_key: 'fixture:cleanup-b' });
    const attempted = await f.operator.call('operator_goal_add', { title: 'Attempted work', details: 'must retain', dedupe_key: 'fixture:attempted' });
    const recurring = await f.operator.call('operator_goal_add', { title: 'Recurring work', details: 'must retain', dedupe_key: 'fixture:recurring', recurring: true, interval_seconds: 600 });
    const dbPath = join(f.home, 'operator', 'operator.sqlite3');
    const db = new Database(dbPath);
    try {
      db.query("INSERT INTO runs(id,goal_id,started_at,completed_at,state,response_id,summary,usage_json) VALUES(?,?,1,2,'failed',NULL,'fixture','{}')")
        .run('fixture-run', attempted.id);
    } finally { db.close(); }

    const beforeBytes = readFileSync(dbPath);
    const audit = await f.operator.call('operator_backlog_audit', { state: 'queued', limit: 20 });
    expect(audit).toMatchObject({ schema_version: 'bonfyre.operator.backlog-audit.v1', state: 'queued', paused: false });
    expect(audit.cohorts.repeated_exact_titles).toEqual(expect.arrayContaining([
      expect.objectContaining({ title_key: 'repeated cleanup target', count: 2 }),
    ]));
    expect(audit.compaction.eligible_returned_goal_ids).toEqual(expect.arrayContaining([duplicateA.id, duplicateB.id]));
    expect(audit.compaction.refused_with_run_history).toContain(attempted.id);
    expect(readFileSync(dbPath)).toEqual(beforeBytes);

    await expect(f.operator.call('operator_goal_supersede', { goal_ids: [duplicateA.id], reason: 'fixture cleanup' }))
      .rejects.toThrow('STOP must be active');
    await f.operator.call('operator_pause', { reason: 'reviewed backlog cleanup' });
    const stopped = readFileSync(join(f.home, 'operator', 'STOP'), 'utf8');

    await expect(f.operator.call('operator_goal_supersede', {
      goal_ids: [duplicateA.id, attempted.id], reason: 'must be atomic',
    })).rejects.toThrow('backlog compaction refused');
    let check = new Database(dbPath);
    try { expect(check.query('SELECT state FROM goals WHERE id=?').get(duplicateA.id).state).toBe('queued'); }
    finally { check.close(); }

    await expect(f.operator.call('operator_goal_supersede', {
      goal_ids: [recurring.id], reason: 'recurring must remain',
    })).rejects.toThrow('backlog compaction refused');

    const result = await f.operator.call('operator_goal_supersede', {
      goal_ids: [duplicateA.id, duplicateB.id], reason: 'reviewed exact duplicate-title cohort',
    });
    expect(result).toMatchObject({
      schema_version: 'bonfyre.operator.goal-supersede.v1',
      superseded: 2,
      retained_history: true,
      stopped: true,
    });
    expect(result.goal_ids).toEqual([duplicateA.id, duplicateB.id]);
    expect(readFileSync(join(f.home, 'operator', 'STOP'), 'utf8')).toBe(stopped);

    check = new Database(dbPath);
    try {
      const rows = check.query('SELECT id,state,last_error FROM goals WHERE id IN (?,?) ORDER BY id').all(duplicateA.id, duplicateB.id);
      expect(rows.every((row) => row.state === 'superseded')).toBe(true);
      expect(rows.every((row) => row.last_error.includes('reviewed exact duplicate-title cohort'))).toBe(true);
      expect(check.query('SELECT COUNT(*) AS n FROM runs WHERE goal_id=?').get(attempted.id).n).toBe(1);
      expect(check.query('SELECT state FROM goals WHERE id=?').get(attempted.id).state).toBe('queued');
    } finally { check.close(); }

    const after = await f.operator.call('operator_backlog_audit', { state: 'superseded', limit: 20 });
    expect(after.rows.map((row) => row.id)).toEqual(expect.arrayContaining([duplicateA.id, duplicateB.id]));
  } finally { f.cleanup(); }
});

test('maturation ledger pages historical promises and atomically records independently witnessed verdicts under STOP', async()=>{
  const f=fixture();
  try {
    const dir=join(f.home,'.bonfyre','backlog');mkdirSync(dir,{recursive:true});
    const path=join(dir,'items.tsv');
    const baseline='id\tpromise\tstatus\tevidence\nplan:1\tPreserve source identity\topen\tunverified\nplan:2\tBuild existing reader\topen\tunverified\n';
    writeFileSync(path,baseline);
    const first=await f.operator.call('operator_maturation_ledger',{action:'read',limit:1});
    expect(first).toMatchObject({schema_version:'bonfyre.backlog.maturation.v1',total:2,returned:1,has_more:true});
    expect(first.rows[0].promise).toBe('Preserve source identity');
    expect(readFileSync(path,'utf8')).toBe(baseline);
    const valid={id:'plan:1',verdict:'partial',evidence:['cmd/BonfyreFixture/x.txt: current owner inspected','native test: fixture refused stale selection']};
    const update=payload=>f.operator.call('operator_maturation_ledger',{action:'record',expected_sha256:first.sha256,updates:[payload]});
    await expect(update(valid)).rejects.toThrow('STOP');
    await f.operator.call('operator_pause',{reason:'ledger fixture review'});
    await expect(f.operator.call('operator_maturation_ledger',{action:'record',expected_sha256:'0'.repeat(64),updates:[valid]})).rejects.toThrow('changed');
    await expect(update({...valid,id:'invented'})).rejects.toThrow('existing exact item');
    await expect(update({...valid,verdict:'done'})).rejects.toThrow('source hashes');
    const result=await update(valid);
    expect(result).toMatchObject({updated:1,operator_resumed:false,previous_sha256:first.sha256});
    expect((await f.operator.call('operator_status')).paused).toBe(true);
    const next=await f.operator.call('operator_maturation_ledger',{action:'read',limit:20});
    expect(next.rows[0].status).toBe('partial');
    expect(next.rows[1].status).toBe('open');
    expect(next.columns).toContain('checked_at');
    expect(next.rows[0].evidence).toContain('native test');
    expect(readFileSync(join(dir,'receipts',first.sha256+'.tsv'),'utf8')).toBe(baseline);
    await expect(f.operator.call('operator_maturation_ledger',{action:'record',expected_sha256:first.sha256,updates:[valid]})).rejects.toThrow('changed');
    const literal=join(f.root,'cmd/BonfyreFixture/native.c');writeFileSync(literal,'int main(void){return 0;}\n');
    const crypto=await import('node:crypto');
    const proof={path:'cmd/BonfyreFixture/native.c',sha256:crypto.createHash('sha256').update(readFileSync(literal)).digest('hex')};
    const finished=await f.operator.call('operator_maturation_ledger',{action:'record',expected_sha256:next.sha256,updates:[{
      id:'plan:1',verdict:'done',evidence:['source verified cmd/BonfyreFixture/native.c sha256','native make test PASS fixtures'],
      source_proofs:[proof],test_witnesses:['native-test-passed exit0 with exact fixture input']
    }]});
    expect(finished.updated).toBe(1);
    const again=await f.operator.call('operator_maturation_ledger',{action:'read'});
    expect(again.rows[0].status).toBe('done');
    expect(again.rows[1].status).toBe('open');
    expect(again.rows[1].evidence).toBe('unverified');
  }finally{f.cleanup();}
});

test('maturation ledger refuses symlinks, duplicate IDs and updates that partially validate',async()=>{
  const f=fixture();
  try{
    const dir=join(f.home,'.bonfyre','backlog');mkdirSync(dir,{recursive:true});
    const path=join(dir,'items.tsv');
    writeFileSync(path,'id\tverdict\tevidence\none\topen\tinitial\none\topen\tinitial\n');
    await expect(f.operator.call('operator_maturation_ledger',{action:'read'})).rejects.toThrow('repeated');
    writeFileSync(path,'id\tverdict\tevidence\none\topen\tinitial\ntwo\topen\tinitial\n');
    await f.operator.call('operator_pause',{reason:'fixture'});
    const initial=await f.operator.call('operator_maturation_ledger',{action:'read'});
    await expect(f.operator.call('operator_maturation_ledger',{action:'record',expected_sha256:initial.sha256,updates:[
      {id:'one',verdict:'partial',evidence:['source verified with real owner']},
      {id:'not-there',verdict:'done',evidence:['unexpected generated evidence']},
    ]})).rejects.toThrow('existing exact');
    expect((await f.operator.call('operator_maturation_ledger',{action:'read'})).sha256).toBe(initial.sha256);
    const outside=join(f.base,'outside.tsv');writeFileSync(outside,'do not change');
    rmSync(path);symlinkSync(outside,path);
    await expect(f.operator.call('operator_maturation_ledger',{action:'read'})).rejects.toThrow('symlink');
    expect(readFileSync(outside,'utf8')).toBe('do not change');
  }finally{f.cleanup();}
});

test('one corpus-wide seeding pass preserves all Atlas gaps, handoff rows and plan markers without fake completion',async()=>{
  const f=fixture();
  try{
    const root=f.root,ledger=join(f.home,'.bonfyre','backlog');
    mkdirSync(ledger,{recursive:true});
    writeFileSync(join(ledger,'items.tsv'),'id\tpromise\tverdict\tevidence\nexisting:1\tAlready planned work\tpartial\texisting source review\n');
    mkdirSync(join(root,'architecture'),{recursive:true});
    writeFileSync(join(root,'architecture/atlas.index.json'),JSON.stringify({
      index:{declaration_digest:'fixture'},
      architectures:{
        one:{known_gaps:['Implement the source verifier using its existing owner','Never assert publication from a saved draft']},
        two:{known_gaps:['Prove downstream account readback']},
      }
    }));
    for(const rel of ['docs/engineering','docs/engineering/handoff-backfill-20261019',
      'docs/engineering/stitch-aurekai-20261104'])mkdirSync(join(root,rel),{recursive:true});
    writeFileSync(join(root,'docs/engineering/plan-20261015.md'),
      '# Exact promise\n- [ ] Finish client source review end to end\n');
    writeFileSync(join(root,'docs/engineering/handoff-backfill-20261019/skills-native-demo-scope.tsv'),
      'source_path\tacceptance\tstatus\nskill/a\tOriginal skill needs native continuation\tpending\n');
    writeFileSync(join(root,'docs/engineering/stitch-aurekai-20261104/COVERAGE.tsv'),
      'screen_id\tacceptance\tstatus\nscreen/a\tProve real source composition and browser behavior\tpending\n');
    mkdirSync(join(root,'cmd/BonfyreExperienceStage'),{recursive:true});
    writeFileSync(join(root,'cmd/BonfyreExperienceStage/experience-maturities.yaff'),
      'pack fixture\nmaturity 01 example-one\nmaturity 02 example-two\n');
    const before=await f.operator.call('operator_maturation_ledger',{action:'read'});
    const coverage=await f.operator.call('operator_maturation_ledger',{action:'coverage'});
    expect(coverage.source_scope).toMatchObject({atlas_gaps:3,handoff_rows:2,plan_markers:1});
    expect(coverage.source_candidates).toBe(6);
    expect(coverage.unregistered).toBe(6);
    expect(coverage.verdicts_are_source_checked).toBe(false);
    await expect(f.operator.call('operator_maturation_ledger',{action:'seed',expected_sha256:before.sha256})).rejects.toThrow('STOP');
    await f.operator.call('operator_pause',{reason:'source intake fixture'});
    const seeded=await f.operator.call('operator_maturation_ledger',{action:'seed',expected_sha256:before.sha256});
    expect(seeded).toMatchObject({new_candidates:6,operator_resumed:false,independent_verdicts:false});
    const after=await f.operator.call('operator_maturation_ledger',{action:'read',limit:100});
    expect(after.total).toBe(7);
    expect(after.rows[0]).toMatchObject({id:'existing:1',verdict:'partial',evidence:'existing source review'});
    expect(after.rows.slice(1).every(row=>row.verdict==='unverified')).toBe(true);
    expect(after.rows.slice(1).some(row=>row.promise==='Never assert publication from a saved draft')).toBe(true);
    expect(after.rows.slice(1).some(row=>row.source_ref.includes('stitch-aurekai'))).toBe(true);
    const second=await f.operator.call('operator_maturation_ledger',{action:'seed',expected_sha256:after.sha256});
    expect(second.seeded).toBe(0);
    expect((await f.operator.call('operator_maturation_ledger',{action:'read'})).sha256).toBe(after.sha256);
    expect((await f.operator.call('operator_maturation_ledger',{action:'coverage'})).unregistered).toBe(0);
  }finally{f.cleanup();}
});

test('actual 91 Stitch / 388 source-skills / 30 programmes and Atlas are inventoried without false completion',async()=>{
  const home=mkdtempSync(join(tmpdir(),'bonfyre-maturation-readonly-')),root=resolve(import.meta.dir,'../../..');
  try{
    const dir=join(home,'.bonfyre','backlog');mkdirSync(dir,{recursive:true});
    writeFileSync(join(dir,'items.tsv'),'id\tverdict\tevidence\npre-existing\tunverified\tunverified\n');
    const owner=createProjectOperator({root,home,env:{...process.env,
      BONFYRE_OPERATOR_HOME:join(home,'operator'),BONFYRE_MCP_PROJECT_WRITE_ENABLED:'0'},
      run:projectRun,status:async()=>({root})});
    const coverage=await owner.call('operator_maturation_ledger',{action:'coverage'});
    expect(coverage.denominators.stitch.items).toBe(91);
    expect(coverage.denominators.stitch.stages.live_release_readback.pending).toBe(91);
    expect(coverage.denominators.skills.items).toBe(388);
    expect(coverage.denominators.skills.stages.ui_complete.blank).toBe(388);
    expect(coverage.denominators.experiences.items).toBe(30);
    expect(coverage.source_scope.atlas_gaps).toBeGreaterThan(1000);
    expect(coverage.source_scope.handoff_rows).toBeGreaterThan(400);
    expect(coverage.source_scope.plan_review_rows+coverage.source_scope.plan_markers).toBeGreaterThan(20);
    expect(coverage.source_candidates).toBeGreaterThan(1500);
    expect(coverage.verdicts_are_source_checked).toBe(false);
    expect((await owner.call('operator_maturation_ledger',{action:'read'})).total).toBe(1);
  }finally{rmSync(home,{recursive:true,force:true});}
});

test('MCP dedupe replay cannot resurrect superseded catalogue content', async () => {
  const f = fixture();
  try {
    const original = await f.operator.call('operator_goal_add', { title: 'Catalogue source part', details: 'Original catalogue URLs', priority: 92, dedupe_key: 'catalogue:part:1' });
    const db = new Database(join(f.home, 'operator', 'operator.sqlite3'));
    db.query("UPDATE goals SET state='superseded',last_error='Moved to canonical source collection' WHERE id=?").run(original.id);
    const before = db.query('SELECT * FROM goals WHERE id=?').get(original.id);
    db.close();
    const replay = await f.operator.call('operator_goal_add', { title: 'Requeued inventory', details: 'Replaced body', priority: 100, dedupe_key: 'catalogue:part:1' });
    expect(replay).toEqual(before);
    expect((await f.operator.call('operator_status')).goals.queued ?? 0).toBe(0);
  } finally { f.cleanup(); }
});


test('organ drift sync refuses under STOP before any subprocess invocation', async () => {
  const f = fixture();
  try {
    mkdirSync(join(f.root, 'scripts'), { recursive: true });
    const script = join(f.root, 'scripts', 'organ_drift.sh');
    writeFileSync(script, '#!/bin/sh\necho should-not-run\n'); chmodSync(script, 0o755);
    await f.operator.call('operator_pause', { reason: 'fixture STOP' });
    const before = f.invocations.length;
    await expect(f.operator.call('organ_drift_sync', { target: 'bonfyre-fixture' })).rejects.toThrow('STOP active');
    expect(f.invocations).toHaveLength(before);
    await expect(f.operator.call('organ_drift_sync', { target: '../bad' })).rejects.toThrow('exact bonfyre target');
    expect(f.invocations).toHaveLength(before);
  } finally { f.cleanup(); }
});

test('test profiles refuse Python and parse Bun source without execution or output', async () => {
  const f = fixture();
  try {
    const source = join(f.root, 'cmd', 'BonfyreFixture', 'example.ts');
    writeFileSync(source, 'const count: number = 1;\n');
    await expect(f.operator.call('project_test_profile', { profile: 'python-compile', paths: ['cmd/BonfyreFixture/example.ts'] })).rejects.toThrow('retired');
    const result = await f.operator.call('project_test_profile', { profile: 'bun-syntax', paths: ['cmd/BonfyreFixture/example.ts'] });
    expect(result.result.exit_code).toBe(0);
    expect(result.result.note).toContain('Syntax parse only');
    writeFileSync(source, 'const count: number = ;\n');
    const invalid = await f.operator.call('project_test_profile', { profile: 'bun-syntax', paths: ['cmd/BonfyreFixture/example.ts'] });
    expect(invalid.result.exit_code).toBe(1);
    expect(f.invocations).toHaveLength(0);
    await expect(f.operator.call('project_test_profile', { profile: 'cmd-make', target: '../outside' })).rejects.toThrow('Bonfyre command family');
  } finally { f.cleanup(); }
});
test('scoped WebAddress make witnesses never run default all/test or arbitrary caller goals', async () => {
  const f = fixture();
  try {
    const dir = join(f.root, 'cmd/BonfyreWebAddress');
    mkdirSync(dir, { recursive: true });
    writeFileSync(join(dir, 'Makefile'),
      'all:\n\t@echo ALL\nbonfyre-web-address:\n\t@echo COMPILED\ntest:\n\t@echo ALL-TESTS\n');
    for (const bad of [
      [], ['clean'], ['../outside'], ['bonfyre-web-address', 'clean'],
      ['-f'], ['build/web_main.o;echo unsafe'],
    ]) await expect(f.operator.call('project_test_profile',
      { profile: 'cmd-make', target: 'BonfyreWebAddress', paths: bad }))
        .rejects.toThrow('approved native witness');
    expect(f.invocations).toHaveLength(0);
    const narrow = await f.operator.call('project_test_profile',
      { profile: 'cmd-make', target: 'BonfyreWebAddress', paths: ['bonfyre-web-address'] });
    expect(narrow.scope).toBe('bonfyre-web-address');
    expect(narrow.result.exit_code).toBe(0);
    expect(narrow.result.stdout).toContain('COMPILED');
    expect(narrow.result.stdout).not.toContain('ALL-TESTS');
    expect(f.invocations.map(call => call.argv)).toEqual([['make', '-j2', 'bonfyre-web-address']]);
  } finally { f.cleanup(); }
});
test('bounded Bun tests require exact tests under the repo, never an arbitrary execution target', async () => {
  const f = fixture();
  try {
    const dir = join(f.root, 'components/BonfyreServiceSite/tests');
    mkdirSync(dir, { recursive: true });
    writeFileSync(join(dir, 'planner.test.mjs'), "import { test } from 'bun:test'; test('safe', () => {});\n");
    mkdirSync(join(f.home, '.bun/bin'), { recursive: true });
    symlinkSync(process.execPath, join(f.home, '.bun/bin/bun'));
    const path = 'components/BonfyreServiceSite/tests/planner.test.mjs';
    for (const bad of [
      { profile: 'bun-tests', paths: ['../outside.test.mjs'] },
      { profile: 'bun-tests', paths: ['components/BonfyreServiceSite/planner-client.mjs'] },
      { profile: 'bun-tests', paths: [path, path] },
      { profile: 'bun-tests', paths: [path], target: 'sh' },
    ]) await expect(f.operator.call('project_test_profile', bad)).rejects.toThrow();
    expect(f.invocations).toHaveLength(0);
    const result = await f.operator.call('project_test_profile', { profile: 'bun-tests', paths: [path] });
    expect(result.result.exit_code).toBe(0);
    expect(f.invocations).toHaveLength(1);
    expect(f.invocations[0].argv.slice(1)).toEqual(['test', join(realpathSync(f.root), path)]);
    await expect(f.operator.call('project_test_profile', { profile: 'service-site-planner', paths: [path] })).rejects.toThrow('fixed test scope');
  } finally { f.cleanup(); }
});

test('planner verification profile owns exact test, build and local browser commands with fail-fast gates', async () => {
  const f = fixture();
  try {
    const calls=[];
    let failedAt=-1;
    const owner=createProjectOperator({root:f.root,home:f.home,env:{...process.env},
      status:async()=>({}),run:async (argv,options)=>{
        calls.push({argv,options});
        return {exit_code:calls.length===failedAt?7:0,stdout:'',stderr:''};
      }});
    for (const bad of [{profile:'service-site-planner',target:'sh'},
      {profile:'service-site-planner',paths:['../anything.test.mjs']}])
      await expect(owner.call('project_test_profile',bad)).rejects.toThrow('fixed test scope');
    expect(calls).toHaveLength(0);
    failedAt=1;
    const brokenTests=await owner.call('project_test_profile',{profile:'service-site-planner'});
    expect(brokenTests.result.exit_code).toBe(7);
    expect(calls).toHaveLength(1);
    expect(calls[0].argv.slice(0,2)).toEqual([join(f.home,'.bun/bin/bun'),'test']);
    expect(calls[0].argv.slice(2)).toHaveLength(4);
    calls.length=0;failedAt=2;
    const brokenBuild=await owner.call('project_test_profile',{profile:'service-site-planner'});
    expect(brokenBuild.result.exit_code).toBe(7);
    expect(calls).toHaveLength(2);
    expect(calls[1].argv[1]).toBe(join(realpathSync(f.root),'components/BonfyreServiceSite/build.mjs'));
    calls.length=0;failedAt=-1;
    const success=await owner.call('project_test_profile',{profile:'service-site-planner'});
    expect(success.result.exit_code).toBe(0);
    expect(calls).toHaveLength(3);
    expect(calls[2].argv[0]).toBe('/usr/bin/env');
    expect(calls[2].argv.some(x=>x.startsWith('PLANNER_PROOF='))).toBe(true);
    expect(calls[2].argv.at(-1)).toBe(join(realpathSync(f.root),'components/BonfyreServiceSite/tests/planner-browser.test.mjs'));
  } finally { f.cleanup(); }
});

test('Cloudflare profile builds the native owner and propagates failures with a fixed scope', async () => {
  const f = fixture();
  try {
    mkdirSync(join(f.root, 'cmd/BonfyreQueue'), { recursive: true });
    mkdirSync(join(f.root, 'integrations/cloudflare'), { recursive: true });
    const calls = [];
    let buildCode = 0;
    const operator = createProjectOperator({ root: f.root, home: f.home, env: {}, status: async () => ({}),
      run: async (argv, options) => { calls.push({ argv, options }); return { exit_code: argv[0] === 'make' ? buildCode : 1, stdout: '', stderr: 'fixture failure' }; } });
    await expect(operator.call('project_test_profile', { profile: 'cloudflare-bridge', paths: ['arbitrary.mjs'] })).rejects.toThrow('fixed test scope');
    await expect(operator.call('project_test_profile', { profile: 'cloudflare-bridge', target: 'arbitrary' })).rejects.toThrow('fixed test scope');
    expect(calls).toHaveLength(0);
    const failedTest = await operator.call('project_test_profile', { profile: 'cloudflare-bridge' });
    expect(failedTest.result.exit_code).toBe(1);
    expect(calls).toHaveLength(2);
    expect(calls[0].argv).toEqual(['make', '-j2']);
    expect(calls[0].options.cwd).toBe(join(realpathSync(f.root), 'cmd/BonfyreQueue'));
    expect(calls[1].argv.slice(0,2)).toEqual([join(f.home, '.bun/bin/bun'), 'test']);
    expect(calls[1].argv.slice(2)).toHaveLength(6);
    expect(calls[1].argv.slice(2).every(path => path.startsWith(join(realpathSync(f.root), 'integrations/cloudflare/')))).toBe(true);
    calls.length = 0; buildCode = 2;
    const failedBuild = await operator.call('project_test_profile', { profile: 'cloudflare-bridge' });
    expect(failedBuild.result.exit_code).toBe(2);
    expect(calls).toHaveLength(1);
  } finally { f.cleanup(); }
});

test('bulk source-to-application sweep preserves source, semantic glyph and recipient gates', async () => {
  const {planProductSweep}=await import('../../../scripts/release/downstream_product_sweep.mjs');
  const glyphs=['board','list','book','group','headphones','play','world','link','coins','tool','apps'].map(x=>'BF_ICON_'+x.toUpperCase()).join(' ');
  const base={collections:[{collection_id:'published-feed',status:'published'},{collection_id:'held-draft',status:'draft'}],
    bindings:[{binding_id:'existing',site_id:'world-a',application_kind:'Newsroom',source_collection:'published-feed'}],
    programs:[{site_id:'world-a',source_collections:'["published-feed"]',application_bindings:'["existing"]'}],glyphs};
  const ready=planProductSweep(base)[0];
  expect(ready).toMatchObject({site:'world-a',declared:1,slots:1,ready:true,missing:[],unbound:[]});
  expect(ready.material).toEqual([{kind:'Newsroom',id:'existing',collection:'published-feed'}]);
  const held=planProductSweep({...base,programs:[{...base.programs[0],source_collections:'["published-feed","held-draft","unknown"]'}]})[0];
  expect(held.ready).toBe(false);expect(held.missing).toEqual(['held-draft','unknown']);expect(held.unbound).toEqual(['held-draft','unknown']);
  expect(planProductSweep({...base,bindings:[{...base.bindings[0],site_id:'wrong-world'}]})[0].missing).toContain('binding:existing');
  expect(planProductSweep({...base,bindings:[]})[0].missing).toContain('binding:existing');
  expect(()=>planProductSweep({...base,collections:[...base.collections,base.collections[0]]})).toThrow('Ambiguous source collection');
  expect(()=>planProductSweep({...base,glyphs:'BF_ICON_BOARD'})).toThrow('Missing compiled native glyph');
  expect(()=>planProductSweep({...base,programs:[...base.programs,base.programs[0]]})).toThrow('Ambiguous published site');
});
