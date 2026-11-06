import { test, expect } from 'bun:test';
import { Database } from 'bun:sqlite';
import { chmodSync, mkdtempSync, mkdirSync, readFileSync, readdirSync, realpathSync, rmSync, symlinkSync, writeFileSync, existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
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
    expect(result.apply_error).toContain('bound');
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
