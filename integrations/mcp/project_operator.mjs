// Project-node patch and operator owner. No imports from the Python service.
import { Database } from 'bun:sqlite';
import { createHash, randomBytes, randomUUID, timingSafeEqual } from 'node:crypto';
import { chmodSync, copyFileSync, existsSync, lstatSync, readdirSync, mkdirSync, mkdtempSync, readFileSync, realpathSync, renameSync, rmSync, unlinkSync, writeFileSync } from 'node:fs';
import { homedir, tmpdir } from 'node:os';
import { basename, dirname, isAbsolute, join, relative, resolve } from 'node:path';

const RECEIPT_ID = /^patch-[0-9]+-[a-f0-9]{12}$/;
const MAX_PATCH_BYTES = 5_000_000;
const MAX_PREIMAGE_BYTES = 32 * 1024 * 1024;
const MAX_SYNTAX_BYTES = 10_000_000;
const DEFAULT_POLICY = Object.freeze({
  mode: 'power', auto_apply_low_risk_patches: true, auto_commit: false,
  allowed_path_prefixes: ['bonfyre-zig/', 'cmd/', 'Aurekai/', 'integrations/', 'scripts/'],
  denied_path_prefixes: ['.git/', '.bonfyre/', 'node_modules/', 'output/', 'Aurekai/generated/integration-repos/'],
  denied_path_fragments: ['/.env', 'secrets', 'credentials', 'private_key', 'id_rsa'],
  max_patch_files: 12, max_patch_changed_lines: 1200, require_clean_touched_paths: true,
});

const sha = value => createHash('sha256').update(value).digest('hex');
const regular = path => { try { return lstatSync(path).isFile(); } catch { return false; } };
const noSymlink = path => {
  try { if (lstatSync(path).isSymbolicLink()) throw new Error(`symlink refused: ${path}`); }
  catch (error) { if (error?.code !== 'ENOENT') throw error; }
};
const json = value => JSON.stringify(value, null, 2) + '\n';
const sameHashes = (expected, current, paths) => !!expected && !!current &&
  Object.keys(expected).length === paths.length && Object.keys(current).length === paths.length &&
  paths.every(path => Object.hasOwn(expected, path) && Object.hasOwn(current, path) && expected[path] === current[path]);
function canonicalFuturePath(value) {
  let current = resolve(value);
  const missing = [];
  while (!existsSync(current)) {
    missing.unshift(basename(current));
    const parent = dirname(current);
    if (parent === current) throw new Error('unresolvable path');
    current = parent;
  }
  return join(realpathSync(current), ...missing);
}
function atomicJson(path, value) {
  noSymlink(path);
  const temporary = `${path}.${process.pid}.${randomBytes(6).toString('hex')}.tmp`;
  try { writeFileSync(temporary, json(value), { flag: 'wx' }); renameSync(temporary, path); }
  finally { rmSync(temporary, { force: true }); }
}

// Parse the ordinary quoted paths in unified Git headers. Unsafe or exotic
// headers refuse; we do not let git apply mutate an unparsed path.
function shellWords(text) {
  const words = []; let word = '', quote = '', started = false;
  for (let i = 0; i < text.length; i++) {
    const ch = text[i];
    if (quote) {
      if (ch === quote) quote = '';
      else if (ch === '\\' && quote === '"') { if (++i >= text.length) throw new Error('invalid quoted patch path'); word += text[i]; }
      else word += ch;
    } else if (ch === '"' || ch === "'") { quote = ch; started = true; }
    else if (/\s/.test(ch)) { if (started) { words.push(word); word = ''; started = false; } }
    else if (ch === '\\') { if (++i >= text.length) throw new Error('invalid escaped patch path'); word += text[i]; started = true; }
    else { word += ch; started = true; }
  }
  if (quote) throw new Error('unterminated quoted patch path');
  if (started) words.push(word);
  return words;
}

function safeRelative(value) {
  if (typeof value !== 'string' || !value || value.includes('\0') || isAbsolute(value) || value.startsWith('~') || value.includes('\\')) throw new Error('unsafe project path');
  const parts = value.split('/');
  if (parts.some(part => !part || part === '.' || part === '..')) throw new Error('unsafe project path');
  return value;
}

export function createProjectOperator({ root, home = homedir(), env = process.env, run, status } = {}) {
  if (!root || typeof run !== 'function' || typeof status !== 'function') throw new Error('project operator needs root, run and status owners');
  const projectRoot = realpathSync(root);
  const operatorHomeInput = resolve(env.BONFYRE_OPERATOR_HOME ?? join(home, '.bonfyre/operator'));
  noSymlink(operatorHomeInput);
  const operatorHome = canonicalFuturePath(operatorHomeInput);
  const dbPath = join(operatorHome, 'operator.sqlite3');
  const policyPath = resolve(env.BONFYRE_OPERATOR_POLICY ?? join(operatorHome, 'policy.json'));
  const stopPath = join(operatorHome, 'STOP');
  const receipts = join(operatorHome, 'patch-receipts');
  const approvalPath = resolve(env.BONFYRE_MCP_APPROVAL_FILE ?? join(home, '.bonfyre/mcp/write-approval.token'));
  const writeEnabled = env.BONFYRE_MCP_PROJECT_WRITE_ENABLED === '1';
  const lowRiskEnabled = env.BONFYRE_MCP_AUTONOMOUS_WRITE_ENABLED === '1';
  const autoCommitEnabled = env.BONFYRE_MCP_AUTO_COMMIT_ENABLED === '1';
  const childEnv = { ...env, GIT_OPTIONAL_LOCKS: '0', GIT_TERMINAL_PROMPT: '0' };
  for (const name of Object.keys(childEnv)) {
    if (['GIT_DIR', 'GIT_WORK_TREE', 'GIT_INDEX_FILE', 'GIT_COMMON_DIR', 'GIT_EXTERNAL_DIFF'].includes(name) || name.startsWith('GIT_CONFIG_')) delete childEnv[name];
  }
  Object.freeze(childEnv);
  let writeTail = Promise.resolve();
  const serialize = fn => { const work = writeTail.then(fn, fn); writeTail = work.catch(() => {}); return work; };

  function ensureStore() {
    mkdirSync(operatorHome, { recursive: true });
    if (realpathSync(operatorHome) !== operatorHome) throw new Error('operator home symlink refused');
    mkdirSync(receipts, { recursive: true });
    if (realpathSync(receipts) !== receipts) throw new Error('receipt directory symlink refused');
    noSymlink(dbPath); noSymlink(stopPath);
  }
  function projectPath(value) {
    const parts = safeRelative(value).split('/');
    let current = projectRoot;
    for (const part of parts) {
      current = join(current, part);
      noSymlink(current);
    }
    const rel = relative(projectRoot, current);
    if (rel === '..' || rel.startsWith('../') || isAbsolute(rel)) throw new Error('path escapes project root');
    return current;
  }
  function fileHash(value) {
    const path = projectPath(value);
    if (!existsSync(path)) return null;
    if (!regular(path)) throw new Error(`non-file project path: ${value}`);
    if (lstatSync(path).size > MAX_PREIMAGE_BYTES) throw new Error('project file exceeds 32 MiB safety limit');
    return sha(readFileSync(path));
  }
  function hashes(paths) { return Object.fromEntries(paths.map(path => [path, fileHash(path)])); }
  function fileMode(value) {
    const path = projectPath(value);
    if (!existsSync(path)) return null;
    if (!regular(path)) throw new Error(`non-file project path: ${value}`);
    return lstatSync(path).mode & 0o7777;
  }
  function modes(paths) { return Object.fromEntries(paths.map(path => [path, fileMode(path)])); }
  function policy() {
    if (!existsSync(policyPath)) return { ...DEFAULT_POLICY };
    noSymlink(policyPath);
    const loaded = JSON.parse(readFileSync(policyPath, 'utf8'));
    if (!loaded || Array.isArray(loaded) || typeof loaded !== 'object') throw new Error('invalid operator policy');
    return { ...DEFAULT_POLICY, ...loaded };
  }
  async function execute(argv, options = {}) {
    const { retirementArchiveRoot, retirementManifestSha256, ...bounds } = options;
    const scoped = retirementArchiveRoot === undefined ? childEnv : { ...childEnv, BONFYRE_ARCHIVE: retirementArchiveRoot,
      BONFYRE_RETIRE_EXPECTED_MANIFEST_SHA256: retirementManifestSha256 };
    const result = await run(argv, { cwd: projectRoot, ...bounds, env: scoped });
    if (!result || !Number.isInteger(result.exit_code)) throw new Error(`runner gave no exit code for ${argv[0]}`);
    if (result.timeout || result.overflow) throw new Error(`${argv[0]} ${result.timeout ? 'timed out' : 'exceeded the bounded output capture'}`);
    return result;
  }
  async function dirtyPaths(paths = []) {
    const dirty = new Set();
    for (const argv of [
      ['git', '--no-pager', '--literal-pathspecs', '-c', 'core.fsmonitor=false', 'diff', '--no-ext-diff', '--no-textconv', '--name-only', '-z'],
      ['git', '--no-pager', '--literal-pathspecs', '-c', 'core.fsmonitor=false', 'diff', '--no-ext-diff', '--no-textconv', '--cached', '--name-only', '-z'],
      ['git', '--no-pager', '--literal-pathspecs', '-c', 'core.fsmonitor=false', 'ls-files', '--others', '--exclude-standard', '-z'],
    ]) {
      const receipt = await execute([...argv, '--', ...paths], { timeoutMs: 20_000 });
      if (receipt.exit_code !== 0) throw new Error(`dirty-path read failed: ${argv.slice(1).join(' ')}`);
      for (const item of String(receipt.stdout ?? '').split('\0')) if (item) dirty.add(item);
    }
    return dirty;
  }
  async function patchMetadata(patch) {
    if (typeof patch !== 'string' || Buffer.byteLength(patch) > MAX_PATCH_BYTES) throw new Error('patch too large or not text');
    const touched = [];
    let additions = 0, deletions = 0;
    const add = raw => {
      let value = raw.trim();
      if (!value || value === '/dev/null') return;
      if (value.startsWith('a/') || value.startsWith('b/')) value = value.slice(2);
      value = safeRelative(value);
      if (!touched.includes(value)) touched.push(value);
    };
    for (const line of patch.split(/\r?\n/)) {
      if (line.startsWith('diff --git ')) {
        if (line.includes('\\')) throw new Error('escaped Git patch paths are unsupported');
        const parts = shellWords(line);
        if (parts.length !== 4) throw new Error('unsupported diff header');
        add(parts[2]); add(parts[3]);
      } else if (line.startsWith('+++ ') || line.startsWith('--- ')) {
        if (line.includes('\\')) throw new Error('escaped Git patch paths are unsupported');
        const parts = shellWords(line.slice(4));
        if (!parts.length) throw new Error('missing patch path');
        add(parts[0]);
      } else if (/^(?:rename|copy) (?:from|to) /.test(line)) {
        throw new Error('rename or copy patch requires a separate native owner');
      } else if (/^(?:old mode|new mode|deleted file mode) /.test(line) ||
        (line.startsWith('new file mode ') && !/^new file mode 100(?:644|755)$/.test(line)) ||
        line === 'GIT binary patch' || line.startsWith('Binary files ')) {
        throw new Error('mode, symlink or binary patch requires a separate native owner');
      } else if (line.startsWith('+') && !line.startsWith('+++')) additions++;
      else if (line.startsWith('-') && !line.startsWith('---')) deletions++;
    }
    if (!touched.length) throw new Error('patch names no files');
    const files = await pathMetadata(touched);
    return { files, file_count: files.length, additions, deletions,
      changed_lines: additions + deletions, dirty_conflicts: files.filter(item => item.dirty_before).map(item => item.path) };
  }
  async function pathMetadata(paths) {
    const dirty = await dirtyPaths(paths);
    return paths.map(value => {
      const path = projectPath(value);
      return { path: value, exists: existsSync(path), sha256: fileHash(value), mode: fileMode(value), dirty_before: dirty.has(value) };
    });
  }
  function lowRiskErrors(metadata) {
    const rules = policy(), errors = [];
    if (!lowRiskEnabled) errors.push('autonomous low-risk write gate is disabled');
    if (!rules.auto_apply_low_risk_patches) errors.push('policy disables autonomous patch application');
    if (metadata.file_count > Number(rules.max_patch_files)) errors.push('patch touches too many files');
    if (metadata.changed_lines > Number(rules.max_patch_changed_lines)) errors.push('patch changes too many lines');
    if (rules.require_clean_touched_paths && metadata.dirty_conflicts.length) errors.push('patch touches paths already dirty before operator work');
    if (metadata.dirty_conflicts.length && rules.allow_dirty_compare_and_swap === false) errors.push('dirty-tree compare-and-swap authority is disabled');
    for (const item of metadata.files) {
      const value = item.path, lowered = value.toLowerCase();
      if (rules.allowed_path_prefixes?.length && !rules.allowed_path_prefixes.some(prefix => value.startsWith(String(prefix)))) errors.push(`path outside allowed roots: ${value}`);
      if (rules.denied_path_prefixes?.some(prefix => value.startsWith(String(prefix)))) errors.push(`path denied by prefix policy: ${value}`);
      if (rules.denied_path_fragments?.some(fragment => lowered.includes(String(fragment).toLowerCase()))) errors.push(`path denied by secret/sensitive policy: ${value}`);
    }
    return [...new Set(errors)].sort();
  }

  function receiptPaths(id) {
    if (!RECEIPT_ID.test(id)) throw new Error('invalid patch receipt id');
    return { meta: join(receipts, `${id}.json`), patch: join(receipts, `${id}.patch`), preimages: join(receipts, `${id}.preimages`) };
  }
  function readReceipt(id) {
    const paths = receiptPaths(id);
    if (!regular(paths.meta)) throw new Error(`patch receipt not found: ${id}`);
    const record = JSON.parse(readFileSync(paths.meta, 'utf8'));
    if (record.receipt_id !== id || record.patch_path !== paths.patch || record.preimage_dir !== paths.preimages) throw new Error('receipt path or identity mismatch');
    if (!Array.isArray(record.metadata?.files) || !Array.isArray(record.preimages)) throw new Error('receipt file list missing');
    const names = record.metadata.files.map(item => safeRelative(item.path));
    if (new Set(names).size !== names.length || record.preimages.length !== names.length) throw new Error('receipt file list mismatch');
    for (let i = 0; i < names.length; i++) {
      const item = record.preimages[i];
      if (item.path !== names[i]) throw new Error('receipt preimage order mismatch');
      const expected = join(paths.preimages, sha(names[i]));
      if (item.existed && item.snapshot_path !== expected) throw new Error('receipt preimage path mismatch');
      projectPath(names[i]);
    }
    return record;
  }
  function updateReceipt(id, updates) {
    const record = { ...readReceipt(id), ...updates, updated_at: Date.now() / 1000 };
    atomicJson(receiptPaths(id).meta, record);
    return record;
  }
  function saveReceipt(patch, metadata, mode) {
    ensureStore();
    const id = `patch-${Math.floor(Date.now() / 1000)}-${randomBytes(6).toString('hex')}`;
    const paths = receiptPaths(id);
    try {
      mkdirSync(paths.preimages, { recursive: false });
      writeFileSync(paths.patch, patch, { flag: 'wx' });
      const preimages = metadata.files.map(item => {
        const source = projectPath(item.path), destination = join(paths.preimages, sha(item.path));
        if (!existsSync(source)) return { path: item.path, existed: false, size: 0, sha256: null, mode: null, snapshot_path: null };
        if (!regular(source)) throw new Error(`non-file project path: ${item.path}`);
        const size = lstatSync(source).size;
        if (size > MAX_PREIMAGE_BYTES) throw new Error('preimage exceeds 32 MiB safety limit');
        copyFileSync(source, destination);
        return { path: item.path, existed: true, size, sha256: sha(readFileSync(destination)), mode: fileMode(item.path), snapshot_path: destination };
      });
      const record = { receipt_id: id, created_at: Date.now() / 1000, mode, patch_path: paths.patch,
        metadata, preimage_dir: paths.preimages, preimages, state: 'prepared' };
      atomicJson(paths.meta, record);
      return record;
    } catch (error) {
      rmSync(paths.patch, { force: true });
      rmSync(paths.preimages, { recursive: true, force: true });
      throw error;
    }
  }
  function receiptBeforeHashes(record) {
    return Object.fromEntries(record.metadata.files.map(item => [item.path, item.sha256]));
  }
  function receiptBeforeModes(record) {
    return Object.fromEntries(record.metadata.files.map(item => [item.path, item.mode]));
  }
  function preimagesMatch(record) {
    return record.preimages.every(item => {
      const file = record.metadata.files.find(file => file.path === item.path);
      return item.sha256 === file?.sha256 && item.mode === file?.mode;
    });
  }
  async function gitApplyCheck(patchPath, patchInput = undefined) {
    if (patchPath !== '-') noSymlink(patchPath);
    return execute(['git', 'apply', '--check', '--whitespace=error-all', patchPath], { timeoutMs: 60_000, input: patchInput });
  }
  async function verifyAffectedPaths(patch, metadata) {
    const receipt = await execute(['git', 'apply', '--numstat', '-z', '-'], { timeoutMs: 30_000, input: patch });
    if (receipt.exit_code !== 0) throw new Error('git apply could not enumerate affected paths');
    const affected = new Set();
    for (const entry of String(receipt.stdout ?? '').split('\0')) {
      if (!entry) continue;
      const first = entry.indexOf('\t'), second = entry.indexOf('\t', first + 1);
      if (first < 0 || second < 0) throw new Error('unsupported Git affected-path encoding');
      affected.add(safeRelative(entry.slice(second + 1)));
    }
    const declared = metadata.files.map(item => item.path);
    if (affected.size !== declared.length || declared.some(path => !affected.has(path))) throw new Error('parsed patch paths differ from Git affected paths');
  }
  async function preview(patch) {
    const metadata = await patchMetadata(patch);
    const check = await gitApplyCheck('-', patch);
    const stat = await execute(['git', 'apply', '--stat', '-'], { timeoutMs: 30_000, input: patch });
    if (check.exit_code === 0) await verifyAffectedPaths(patch, metadata);
    const policyErrors = lowRiskErrors(metadata);
    return { valid: check.exit_code === 0, low_risk_eligible: check.exit_code === 0 && !policyErrors.length,
      metadata, policy_errors: policyErrors, check, stat };
  }
  async function preflight(paths) {
    if (!Array.isArray(paths) || !paths.length || paths.length > 16 || paths.some(value => typeof value !== 'string'))
      throw new Error('paths must contain 1–16 project paths');
    const normalized = paths.map(safeRelative);
    if (new Set(normalized).size !== normalized.length) throw new Error('duplicate preflight path');
    for (const value of normalized) projectPath(value);
    const files = await pathMetadata(normalized);
    const metadata = { files, file_count: files.length, dirty_conflicts: files.filter(item => item.dirty_before).map(item => item.path) };
    const errors = lowRiskErrors({ ...metadata, changed_lines: 0 });
    const rules = policy();
    return { kind: 'path_preflight', metadata, low_risk_paths_eligible: errors.length === 0, policy_errors: errors,
      limits: { max_patch_files: rules.max_patch_files, max_patch_changed_lines: rules.max_patch_changed_lines },
      manual_write_enabled: writeEnabled, manual_approval_required: true, operator_paused: existsSync(stopPath),
      patch_validation_required: true,
      next_step: 'Prepare changes against these preimages, then call project_patch_preview with the patch. Apply rechecks policy and source state.',
      cannot_infer: 'Path preflight is not patch approval, a reservation of these files, permission to resume the operator, or proof of a valid patch.' };
  }
  function approve(token) {
    if (!writeEnabled) throw new Error('project write gate is disabled');
    const expected = regular(approvalPath) ? readFileSync(approvalPath, 'utf8').trim() : '';
    const supplied = String(token ?? '');
    const left = Buffer.from(expected), right = Buffer.from(supplied);
    if (!left.length || left.length !== right.length || !timingSafeEqual(left, right)) throw new Error('invalid write approval token');
  }
  async function applyPatch(patch, mode) {
    const metadata = await patchMetadata(patch);
    const policyErrors = mode === 'autonomous-low-risk' ? lowRiskErrors(metadata) : [];
    if (policyErrors.length) return { applied: false, policy_errors: policyErrors, metadata };
    const receipt = saveReceipt(patch, metadata, mode);
    const paths = metadata.files.map(item => item.path), before = receiptBeforeHashes(receipt), beforeModes = receiptBeforeModes(receipt);
    const current = hashes(paths), currentModes = modes(paths);
    if (!preimagesMatch(receipt) || !sameHashes(before, current, paths) || !sameHashes(beforeModes, currentModes, paths)) {
      updateReceipt(receipt.receipt_id, { state: 'concurrent_change_blocked', before_hashes: before, current_hashes: current,
        before_modes: beforeModes, current_modes: currentModes });
      return { applied: false, receipt_id: receipt.receipt_id, reason: 'touched paths changed after the preimage snapshot',
        before_hashes: before, current_hashes: current, before_modes: beforeModes, current_modes: currentModes };
    }
    const check = await gitApplyCheck(receipt.patch_path);
    if (check.exit_code !== 0) {
      updateReceipt(receipt.receipt_id, { state: 'check_failed', check });
      return { applied: false, receipt_id: receipt.receipt_id, check };
    }
    try { await verifyAffectedPaths(patch, metadata); }
    catch (error) {
      updateReceipt(receipt.receipt_id, { state: 'path_mismatch_blocked', reason: String(error) });
      return { applied: false, receipt_id: receipt.receipt_id, reason: String(error) };
    }
    const afterCheck = hashes(paths), afterCheckModes = modes(paths);
    if (!sameHashes(before, afterCheck, paths) || !sameHashes(beforeModes, afterCheckModes, paths)) {
      updateReceipt(receipt.receipt_id, { state: 'concurrent_change_blocked_after_check', before_hashes: before, current_hashes: afterCheck,
        before_modes: beforeModes, current_modes: afterCheckModes });
      return { applied: false, receipt_id: receipt.receipt_id, reason: 'touched paths changed after git apply --check',
        before_hashes: before, current_hashes: afterCheck, before_modes: beforeModes, current_modes: afterCheckModes };
    }
    // This owner serializes its calls, but an external process may still race
    // the final git apply. The second hash is not a cross-process lock.
    for (const path of paths) projectPath(path);
    noSymlink(receipt.patch_path);
    let applied, executionError;
    try { applied = await execute(['git', 'apply', '--whitespace=fix', receipt.patch_path], { timeoutMs: 120_000 }); }
    catch (error) { executionError = String(error); }
    let post, postModes, postError;
    try { post = hashes(paths); postModes = modes(paths); } catch (error) { postError = String(error); }
    // Git normally applies atomically, but a killed/failed child or a competing
    // writer can leave an effect we cannot attribute. Record the observation;
    // never present an uncertain outcome as applied or safely reverted.
    const uncertain = !!executionError || !!postError || (!!applied && applied.exit_code !== 0 &&
      (!sameHashes(before, post, paths) || !sameHashes(beforeModes, postModes, paths)));
    const state = uncertain ? 'apply_outcome_uncertain' : applied?.exit_code === 0 ? 'applied' : 'apply_failed';
    const record = updateReceipt(receipt.receipt_id, { state, ...(applied ? { apply: applied } : {}),
      ...(executionError ? { apply_error: executionError } : {}), ...(postError ? { post_hash_error: postError } : {}),
      before_hashes: before, before_modes: beforeModes, ...(post ? { post_hashes: post, post_modes: postModes } : {}) });
    const result = { applied: state === 'applied', receipt_id: receipt.receipt_id,
      metadata, ...(applied ? { apply: applied } : {}), ...(executionError ? { apply_error: executionError } : {}),
      ...(postError ? { post_hash_error: postError } : {}), before_hashes: before, before_modes: beforeModes,
      ...(post ? { post_hashes: post, post_modes: postModes } : {}), receipt: record };
    try { result.status = await status(); } catch (error) { result.status_error = String(error); }
    return result;
  }
  async function revert(id) {
    const record = readReceipt(id);
    if (!['applied', 'revert_concurrent_change_blocked'].includes(record.state)) throw new Error('receipt is not an applied patch');
    const paths = record.metadata.files.map(item => item.path), post = record.post_hashes, postModes = record.post_modes;
    if (!post || Object.keys(post).length !== paths.length || paths.some(path => !Object.hasOwn(post, path))) throw new Error('receipt lacks complete postimages; refusing rollback');
    if (!postModes || record.preimages.some(item => item.existed && !Number.isInteger(item.mode)))
      throw new Error('receipt lacks mode evidence; refusing rollback');
    const current = hashes(paths), currentModes = modes(paths);
    if (!sameHashes(post, current, paths) || !sameHashes(postModes, currentModes, paths)) {
      updateReceipt(id, { state: 'revert_concurrent_change_blocked', expected_post_hashes: post, current_hashes: current,
        expected_post_modes: postModes, current_modes: currentModes });
      return { reverted: false, receipt_id: id, reason: 'operator postimages changed; refusing destructive rollback',
        expected_post_hashes: post, current_hashes: current, expected_post_modes: postModes, current_modes: currentModes };
    }
    // Verify every source before changing even the first destination. A bad
    // later snapshot must not leave a deterministic half-restored checkout.
    for (const item of record.preimages) {
      projectPath(item.path);
      if (item.existed && (!regular(item.snapshot_path) || sha(readFileSync(item.snapshot_path)) !== item.sha256))
        throw new Error('receipt preimage missing or corrupt');
    }
    const restored = [];
    for (const item of record.preimages) {
      const path = projectPath(item.path);
      if (item.existed) {
        mkdirSync(dirname(path), { recursive: true });
        projectPath(item.path); // recheck every parent after creating it
        copyFileSync(item.snapshot_path, path);
        chmodSync(path, item.mode);
      } else if (existsSync(path)) {
        if (!regular(path)) throw new Error(`refusing to remove non-file path: ${item.path}`);
        unlinkSync(path);
      }
      restored.push({ path: item.path, sha256: fileHash(item.path), expected_sha256: item.sha256,
        mode: fileMode(item.path), expected_mode: item.mode });
    }
    const restoration = { verified: restored.every(item => item.sha256 === item.expected_sha256 && item.mode === item.expected_mode),
      verification_scope: 'content-sha256-and-posix-mode', files: restored };
    updateReceipt(id, { state: restoration.verified ? 'reverted' : 'revert_verify_failed', restoration });
    const result = { reverted: restoration.verified, receipt_id: id, restoration };
    try { result.status = await status(); } catch (error) { result.status_error = String(error); }
    return result;
  }
  async function commitReceipt(id, message) {
    if (!autoCommitEnabled) throw new Error('automatic operator commits are disabled');
    const record = readReceipt(id);
    if (record.state !== 'applied') throw new Error('receipt is not an applied patch');
    const paths = record.metadata.files.map(item => item.path);
    if (!paths.length) throw new Error('receipt has no touched files');
    if (!sameHashes(record.post_hashes, hashes(paths), paths) || !sameHashes(record.post_modes, modes(paths), paths))
      throw new Error('operator postimages changed; refusing commit');
    const add = await execute(['git', 'add', '--', ...paths], { timeoutMs: 60_000 });
    if (add.exit_code !== 0) return { committed: false, add };
    const commit = await execute(['git', 'commit', '-m', String(message ?? '').trim() || `operator: ${id}`, '--', ...paths], { timeoutMs: 180_000 });
    if (commit.exit_code === 0) updateReceipt(id, { state: 'committed', commit });
    return { committed: commit.exit_code === 0, receipt_id: id, paths, commit };
  }

  function operatorDb() {
    ensureStore();
    const db = new Database(dbPath, { create: true, strict: true });
    db.exec('PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;');
    db.exec(`CREATE TABLE IF NOT EXISTS goals (id TEXT PRIMARY KEY,dedupe_key TEXT UNIQUE,title TEXT NOT NULL,details TEXT NOT NULL,priority INTEGER NOT NULL,state TEXT NOT NULL,source TEXT NOT NULL,recurring INTEGER NOT NULL DEFAULT 0,interval_seconds INTEGER NOT NULL DEFAULT 0,retry_at REAL NOT NULL DEFAULT 0,attempts INTEGER NOT NULL DEFAULT 0,last_error TEXT NOT NULL DEFAULT '',created_at REAL NOT NULL,updated_at REAL NOT NULL);
      CREATE TABLE IF NOT EXISTS runs (id TEXT PRIMARY KEY,goal_id TEXT NOT NULL,started_at REAL NOT NULL,completed_at REAL,state TEXT NOT NULL,response_id TEXT,summary TEXT NOT NULL DEFAULT '',usage_json TEXT NOT NULL DEFAULT '{}');
      CREATE TABLE IF NOT EXISTS actions (seq INTEGER PRIMARY KEY AUTOINCREMENT,run_id TEXT NOT NULL,tool TEXT NOT NULL,args_json TEXT NOT NULL,state TEXT NOT NULL,result_json TEXT NOT NULL,started_at REAL NOT NULL,duration_ms INTEGER NOT NULL);
      CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY,value TEXT NOT NULL);`);
    return db;
  }
  function summary() {
    const db = operatorDb();
    try {
      const goals = Object.fromEntries(db.query('SELECT state,COUNT(*) AS count FROM goals GROUP BY state').all().map(row => [row.state, row.count]));
      const recentGoals = db.query('SELECT id,title,priority,state,source,attempts,last_error,updated_at FROM goals ORDER BY updated_at DESC LIMIT 25').all();
      const recentRuns = db.query('SELECT id,goal_id,started_at,completed_at,state,summary FROM runs ORDER BY started_at DESC LIMIT 20').all();
      const table = db.query("SELECT 1 FROM sqlite_master WHERE type='table' AND name='operator_provider_health'").get();
      const recorded = table ? db.query("SELECT state,cause,observed_at,retry_at FROM operator_provider_health WHERE provider='openai'").get() : null;
      const historical = recorded ? null : db.query("SELECT updated_at FROM goals WHERE state IN ('blocked','retry') AND (last_error LIKE '%credit_balance_exhausted%' OR last_error LIKE '%insufficient_quota%' OR last_error LIKE '%billing_hard_limit%' OR last_error LIKE '%invalid_api_key%') ORDER BY updated_at DESC LIMIT 1").get();
      const providerHealth = recorded ? {provider:'openai',state:recorded.retry_at>0&&recorded.retry_at<=Date.now()/1000?'recheck_required':recorded.state,cause:recorded.cause,observed_at:recorded.observed_at,admitted:false} : historical ? {provider:'openai',state:'quarantined',cause:'historical_terminal_quota',observed_at:historical.updated_at,admitted:false} : {provider:'openai',state:'unverified',admitted:false};
      return { paused: existsSync(stopPath), mode: policy().mode ?? 'power', goals, recent_goals: recentGoals, recent_runs: recentRuns, provider_health:providerHealth, database: dbPath };
    } finally { db.close(); }
  }
  function backlogAudit(args = {}) {
    const limit = Math.max(1, Math.min(Number.parseInt(args.limit ?? 50, 10), 200));
    const state = String(args.state ?? 'queued');
    if (!['queued', 'blocked', 'retry', 'superseded', 'resolved', 'in_progress'].includes(state)) throw new Error('invalid backlog state');
    const db = operatorDb();
    try {
      const rows = db.query(`SELECT g.id,g.dedupe_key,g.title,g.priority,g.state,g.source,g.recurring,g.interval_seconds,
        g.attempts,g.last_error,g.created_at,g.updated_at,
        (SELECT COUNT(*) FROM runs r WHERE r.goal_id=g.id) AS run_count
        FROM goals g WHERE g.state=? ORDER BY g.priority DESC,g.updated_at ASC,g.id ASC LIMIT ?`).all(state, limit);
      const sourceCounts = db.query('SELECT source,COUNT(*) AS count FROM goals WHERE state=? GROUP BY source ORDER BY count DESC,source ASC LIMIT 50').all(state);
      const titleCounts = db.query(`SELECT lower(trim(title)) AS title_key,COUNT(*) AS count,MIN(updated_at) AS oldest_updated_at,MAX(updated_at) AS newest_updated_at
        FROM goals WHERE state=? GROUP BY lower(trim(title)) HAVING COUNT(*)>1 ORDER BY count DESC,title_key ASC LIMIT 50`).all(state);
      const eligible = rows.filter((row) => state === 'queued' && Number(row.run_count) === 0);
      return {
        schema_version: 'bonfyre.operator.backlog-audit.v1',
        paused: existsSync(stopPath),
        state,
        returned: rows.length,
        limit,
        rows,
        cohorts: {
          by_source: sourceCounts,
          repeated_exact_titles: titleCounts,
        },
        compaction: {
          exact_review_required: true,
          eligible_returned_goal_ids: eligible.map((row) => row.id),
          eligible_returned: eligible.length,
          refused_with_run_history: rows.filter((row) => Number(row.run_count) > 0).map((row) => row.id),
        },
        forbidden_inference: 'Repeated titles or shared sources are review cohorts, not proof that goals are semantically redundant. Only exact supplied queued goal ids with zero run history can be superseded by the bounded compaction tool.',
      };
    } finally { db.close(); }
  }

  function supersedeGoals(args = {}) {
    if (!existsSync(stopPath)) throw new Error('STOP must be active before backlog compaction');
    if (!Array.isArray(args.goal_ids) || !args.goal_ids.length || args.goal_ids.length > 100) throw new Error('goal_ids must contain 1-100 exact goal ids');
    const ids = [...new Set(args.goal_ids.map((value) => String(value).trim()))];
    if (ids.length !== args.goal_ids.length || ids.some((id) => !/^[a-f0-9]{32}$/.test(id))) throw new Error('invalid or duplicate goal id');
    const reason = String(args.reason ?? '').trim();
    if (!reason || reason.length > 500 || reason.includes('\0')) throw new Error('compaction reason is required');
    const db = operatorDb();
    try {
      const now = Date.now() / 1000;
      return db.transaction(() => {
        const rows = ids.map((id) => db.query(`SELECT g.*,(SELECT COUNT(*) FROM runs r WHERE r.goal_id=g.id) AS run_count
          FROM goals g WHERE g.id=?`).get(id));
        const missing = ids.filter((_, index) => !rows[index]);
        if (missing.length) throw new Error(`unknown goal ids: ${missing.join(',')}`);
        const note = `superseded through MCP backlog compaction: ${reason}`.slice(0, 4000);
        const queued = rows.filter((row) => row.state === 'queued' && Number(row.run_count) === 0 && Number(row.recurring) === 0);
        const alreadySuperseded = rows.filter((row) =>
          row.state === 'superseded' && Number(row.run_count) === 0 && Number(row.recurring) === 0 && row.last_error === note);
        const accepted = new Set([...queued, ...alreadySuperseded].map((row) => row.id));
        const refused = rows.filter((row) => !accepted.has(row.id))
          .map((row) => ({
            id: row.id, state: row.state, run_count: Number(row.run_count), recurring: Boolean(row.recurring),
            reason_match: row.state === 'superseded' ? row.last_error === note : null,
          }));
        if (refused.length) throw new Error(`backlog compaction refused: ${JSON.stringify(refused)}`);
        for (const row of queued) {
          db.query("UPDATE goals SET state='superseded',retry_at=0,last_error=?,updated_at=? WHERE id=? AND state='queued'")
            .run(note, now, row.id);
        }
        return {
          schema_version: 'bonfyre.operator.goal-supersede.v1',
          superseded: queued.length,
          already_superseded: alreadySuperseded.length,
          goal_ids: rows.map((row) => row.id),
          reason,
          retained_history: true,
          stopped: true,
          forbidden_inference: 'Superseding queued never-started goals retires them from scheduling; it does not delete their rows, runs, actions, receipts, evidence, or any external state.',
        };
      }).immediate();
    } finally { db.close(); }
  }

  // Historical promises have a separate on-disk TSV ledger. Its identity, rows,
  // evidence and write effects are NOT interchangeable with operator goals.
  // The inventory is sourced only from retained Atlas and handoff texts.
  // Candidate rows are NOT implementation verdicts. No model is allowed to turn
  // an Atlas prohibition or an unchecked donor skill into a DONE claim.
  function maturationCandidates() {
    const items=[],seen=new Set(),count={atlas_gaps:0,handoff_rows:0,plan_markers:0,plan_review_rows:0};
    const add=(kind,ref,promise)=>{
      if(typeof promise!=='string'||!promise.trim()||/[\t\r\n\0]/.test(promise))return;
      const normalized=promise.trim();
      const id='source:'+sha(kind+'\0'+ref+'\0'+normalized).slice(0,32);
      if(seen.has(id))return;
      seen.add(id);items.push({id,kind,source_ref:ref,promise:normalized});count[kind]++;
    };
    const atlas=JSON.parse(readFileSync(projectPath('architecture/atlas.index.json'),'utf8'));
    if(!atlas.architectures||typeof atlas.architectures!=='object')throw new Error('invalid canonical Atlas index');
    for(const [id,item] of Object.entries(atlas.architectures))
      for(const [i,gap] of (item.known_gaps??[]).entries())
        add('atlas_gaps','architecture/atlas.index.json#architectures.'+id+'.known_gaps['+i+']',gap);
    const dirs=['docs/engineering','docs/engineering/handoff-backfill-20261019',
      'docs/engineering/stitch-aurekai-20261104'];
    const sourceDocs=[];
    for(const dir of dirs)for(const entry of readdirSync(projectPath(dir)).sort()){
      const rel=dir+'/'+entry;
      if(!/^[a-zA-Z0-9._-]+[.](?:tsv|md)$/.test(entry))continue;
      if(dir==='docs/engineering'&&entry.endsWith('.tsv')===false&&
         !(/2026|convergence|portfolio|white-label/.test(entry)))continue;
      sourceDocs.push(rel);
    }
    for(const rel of sourceDocs){
      const path=projectPath(rel);
      if(!regular(path)||lstatSync(path).size>1_000_000)continue;
      const rows=readFileSync(path,'utf8').split('\n');
      if(rel.endsWith('.tsv')){
        const headers=rows[0]?.split('\t')??[];
        if(headers.length<2)continue;
        const keyCol=headers.findIndex(x=>/^(?:source_path|screen_id|provider|platform|name|id|source_domain)$/.test(x));
        for(let line=1;line<rows.length;line++){
          if(!rows[line]||rows[line].includes('\0'))continue;
          const fields=rows[line].split('\t');if(fields.length!==headers.length)continue;
          const key=fields[keyCol>=0?keyCol:0];
          // Preserve the operation, receiving owner, test and stage fields
          // together. A display title alone is not the source promise.
          const relevant=fields.map((value,i)=>({field:headers[i],value:value.trim()}))
            .filter(x=>x.value&&x.field&&!/^(?:git_blob_sha|bytes|index_line|same_skill_body_group)$/.test(x.field));
          const purpose=relevant.map(x=>x.field+': '+x.value).join(' | ');
          if(!key||!purpose)continue;
          add('handoff_rows',rel+'#'+(line+1)+':'+sha(key).slice(0,12),purpose.slice(0,1800));
        }
      }else{
        for(let line=0;line<rows.length;line++){
          const text=rows[line].trim();
          const explicit=/^[-*]\s+\[\s\]|^(?:TODO|OPEN|PENDING):/i.test(text);
          // The dated plans also recorded requirements in tables and ordinary bullets.
          // Keep them as review candidates, never assume these are open defects.
          const substantial=/^(?:[-*]\s+|\|\s+)(?!-\s*\|)/.test(text) &&
            /\b(?:must|needs?|remaining|unfinished|pending|missing|blocked|not yet|not complete|cannot yet|requires?|unbuilt|unimplemented|still|next|before release)\b/i.test(text);
          if(!explicit&&!substantial)continue;
          const kind=explicit?'plan_markers':'plan_review_rows';
          add(kind,rel+'#'+(line+1),text.slice(0,1800));
        }
      }
    }
    const stageCohort=(path,stages)=>{
      const bytes=readFileSync(projectPath(path));
      const lines=bytes.toString('utf8').trimEnd().split('\n'),fields=lines.shift().split('\t');
      const positions=Object.fromEntries(stages.map(n=>[n,fields.indexOf(n)]));
      const rows=lines.filter(Boolean).map(line=>line.split('\t'));
      return {source:path,sha256:sha(bytes),items:rows.length,
        stages:Object.fromEntries(stages.map(n=>{const i=positions[n];
          const values=i<0?[]:rows.map(row=>(row[i]??'').trim());
          return [n,{blank:values.filter(v=>!v).length,pending:values.filter(v=>v==='pending').length,
            partial:values.filter(v=>/^partial/i.test(v)).length,pass:values.filter(v=>/^pass/i.test(v)).length}];
        }))};
    };
    const programmes=readFileSync(projectPath('cmd/BonfyreExperienceStage/experience-maturities.yaff'));
    const denominators={skills:stageCohort('docs/engineering/handoff-backfill-20261019/skills-native-demo-scope.tsv',
      ['source_reviewed','operation_implemented','ui_complete','native_continuation','downstream_used']),
      stitch:stageCohort('docs/engineering/stitch-aurekai-20261104/COVERAGE.tsv',
      ['control_parity','source_record_parity','authority_acceptance','browser_acceptance','live_release_readback']),
      experiences:{source:'cmd/BonfyreExperienceStage/experience-maturities.yaff',sha256:sha(programmes),
        items:programmes.toString('utf8').split('\n').filter(s=>/^maturity\s+[0-9]{2}\s+/.test(s)).length}};
    return {items,count,source_docs:sourceDocs.length,atlas_generation:atlas.index?.declaration_digest??null,denominators};
  }

  function maturationLedger(args = {}) {
    const action = args.action ?? 'read';
    if (!['read','record','coverage','seed'].includes(action)) throw new Error('unknown maturation action');
    if (action !== 'record' && args.updates !== undefined) throw new Error('updates require explicit record action');
    const root = join(home, '.bonfyre', 'backlog'), path = join(root, 'items.tsv');
    for (const p of [join(home,'.bonfyre'),root,path]) noSymlink(p);
    if (!regular(path) || lstatSync(path).size > 16 * 1024 * 1024)
      throw new Error('canonical items.tsv is missing or exceeds its read bound');
    const original = readFileSync(path), sourceSha = sha(original);
    const source = new TextDecoder('utf-8',{fatal:true}).decode(original);
    if (!source.endsWith('\n') || source.includes('\r') || source.includes('\0'))
      throw new Error('items.tsv must be a terminated LF UTF-8 ledger');
    const parts = source.slice(0,-1).split('\n'), columns = parts.shift().split('\t');
    if (columns.some(c=>!c||!/^[a-z][a-z0-9_-]*$/.test(c)) ||
        new Set(columns).size !== columns.length) throw new Error('invalid backlog columns');
    const ids = ['id','item_id','key','item'].map(k=>columns.indexOf(k)).find(i=>i>=0);
    if (ids===undefined) throw new Error('existing backlog ledger has no item id column');
    const rows = parts.map(line=>{
      const cells = line.split('\t');
      if (cells.length !== columns.length) throw new Error('invalid backlog TSV row width');
      return cells;
    });
    const byId = new Map();
    for (const [i,row] of rows.entries()) {
      if (!row[ids] || byId.has(row[ids])) throw new Error('missing or repeated backlog item id');
      byId.set(row[ids],i);
    }
    const response = {schema_version:'bonfyre.backlog.maturation.v1',path:'~/.bonfyre/backlog/items.tsv',
      sha256:sourceSha,columns,total:rows.length,paused:existsSync(stopPath)};
    const corpus=['coverage','seed'].includes(action) ? maturationCandidates() : null;
    if (action === 'coverage') return {...response,source_scope:corpus.count,source_docs:corpus.source_docs,
      atlas_generation:corpus.atlas_generation,source_candidates:corpus.items.length,
      already_registered:corpus.items.filter(item=>byId.has(item.id)).length,
      unregistered:corpus.items.filter(item=>!byId.has(item.id)).length,
      denominators:corpus.denominators,verdicts_are_source_checked:false};
    if (action === 'read') {
      const offset = args.offset ?? 0, limit = args.limit ?? 100;
      if (!Number.isSafeInteger(offset)||offset<0||!Number.isSafeInteger(limit)||limit<1||limit>200)
        throw new Error('invalid backlog page');
      return {...response,offset,returned:Math.min(limit,Math.max(rows.length-offset,0)),
        rows:rows.slice(offset,offset+limit).map(row=>Object.fromEntries(columns.map((c,i)=>[c,row[i]]))),
        has_more:offset+limit<rows.length};
    }
    if (!writeEnabled || !existsSync(stopPath)) throw new Error('backlog updates require MCP write permission and operator STOP');
    if (args.expected_sha256 !== sourceSha) throw new Error('backlog ledger changed; review latest source before writing');
    if (action === 'record' && (!Array.isArray(args.updates)||!args.updates.length||args.updates.length>100))
      throw new Error('record 1..100 independently reviewed existing items');
    const targets = new Set(), verdictName = columns.find(c=>['verdict','status','state'].includes(c)) ?? 'verdict';
    const evidenceName = columns.find(c=>['evidence','proof','current_evidence'].includes(c)) ?? 'evidence';
    for (const field of [verdictName,evidenceName,'checked_at'])
      if (!columns.includes(field)) {columns.push(field);for(const row of rows)row.push('');}
    if (action === 'seed') {
      // Register ALL source-backed inventory candidates in one durable pass.
      // An inventory candidate is never a verified open bug or a completed feature.
      for(const field of ['source_ref','promise'])
        if(!columns.includes(field)){columns.push(field);for(const row of rows)row.push('');}
      // Existing historical rows may use title/description/source rather than
      // the new optional source_ref and promise columns. Avoid reintroducing them.
      const matchColumns=['source_ref','source','source_path','reference','origin','promise',
        'requirement','description','text','title','item','claim']
        .map(name=>columns.indexOf(name)).filter(i=>i>=0);
      const existingSources=new Set(rows.flatMap(row=>matchColumns.map(i=>row[i]).filter(Boolean)));
      for(const candidate of corpus.items){
        if(byId.has(candidate.id)||existingSources.has(candidate.source_ref)||existingSources.has(candidate.promise))continue;
        const row=Array(columns.length).fill('');
        row[ids]=candidate.id;
        row[columns.indexOf('source_ref')]=candidate.source_ref;
        row[columns.indexOf('promise')]=candidate.promise;
        row[columns.indexOf(verdictName)]='unverified';
        row[columns.indexOf(evidenceName)]='Source inventory only; requires current-source inspection, implementation and independent witness';
        byId.set(candidate.id,rows.length);rows.push(row);targets.add(candidate.id);
      }
    }
    for (const item of (args.updates??[])) {
      if (!item || typeof item.id!=='string'||!byId.has(item.id)||targets.has(item.id))
        throw new Error('update requires one unique existing exact item id');
      targets.add(item.id);
      if (!['done','partial','open','blocked','deliberate_boundary','stale'].includes(item.verdict))
        throw new Error('invalid evidence verdict');
      if (!Array.isArray(item.evidence)||!item.evidence.length||item.evidence.length>12 ||
          item.evidence.some(s=>typeof s!=='string'||s.length<8||s.length>500||/[\t\r\n\0]/.test(s)))
        throw new Error('evidence must contain exact, single-line witness references');
      if (item.verdict==='done') {
        if (!Array.isArray(item.source_proofs)||!item.source_proofs.length||
            !Array.isArray(item.test_witnesses)||!item.test_witnesses.length)
          throw new Error('done requires current source hashes and independent test witnesses');
        for(const proof of item.source_proofs){
          if (typeof proof?.path!=='string'||typeof proof?.sha256!=='string'||
              fileHash(proof.path)!==proof.sha256)
            throw new Error('done source witness does not match current checkout bytes');
        }
        if(item.test_witnesses.some(s=>typeof s!=='string'||s.length<12||/[\t\r\n\0]/.test(s)))
          throw new Error('invalid test witness');
      }
      const row=rows[byId.get(item.id)];
      row[columns.indexOf(verdictName)]=item.verdict;
      row[columns.indexOf(evidenceName)]=item.evidence.join(' | ');
      row[columns.indexOf('checked_at')]=new Date().toISOString();
    }
    if (action === 'seed' && targets.size === 0) return {...response,seeded:0,sha256:sourceSha,operator_resumed:false};
    // Preserve a content-addressed preimage, then atomically replace the sole TSV.
    // No row outside the exact submitted set changes; wrong CAS refuses everything.
    const backups=join(root,'receipts');
    noSymlink(backups);
    if (!existsSync(backups)) mkdirSync(backups,{mode:0o700});
    if(!realpathSync(backups).startsWith(realpathSync(root)+'/'))throw new Error('backlog receipt escapes root');
    const preimage=join(backups,sourceSha+'.tsv');
    noSymlink(preimage);
    if(!existsSync(preimage))writeFileSync(preimage,original,{flag:'wx',mode:0o600});
    else if(sha(readFileSync(preimage))!==sourceSha)throw new Error('backlog preimage collision');
    const revised=Buffer.from(columns.join('\t')+'\n'+rows.map(row=>row.join('\t')).join('\n')+'\n');
    const tmp=path+'.'+randomBytes(8).toString('hex')+'.tmp';
    try{
      writeFileSync(tmp,revised,{flag:'wx',mode:0o600});
      if(sha(readFileSync(path))!==sourceSha)throw new Error('backlog changed during exact-review write');
      chmodSync(tmp,lstatSync(path).mode&0o777);
      renameSync(tmp,path);
    }finally{rmSync(tmp,{force:true});}
    return {...response,updated:targets.size,previous_sha256:sourceSha,sha256:sha(revised),
      preimage_sha256:sourceSha,independent_verdicts:action==='record',new_candidates:action==='seed'?targets.size:0,operator_resumed:false,
      cannot_infer:'Ledger verdicts are not implementation, publication, payment or production readback.'};
  }
  function addGoal(args) {
    const title = String(args.title).trim(), details = String(args.details ?? '').trim();
    const priority = Math.max(1, Math.min(Number.parseInt(args.priority ?? 50, 10), 100));
    const source = String(args.source ?? 'mcp');
    const dedupe = String(args.dedupe_key ?? '').trim() || `mcp:${sha(`${title}\n${details}`).slice(0, 24)}`;
    const recurring = Boolean(args.recurring), interval = Math.max(0, Number.parseInt(args.interval_seconds ?? 0, 10));
    if (!Number.isInteger(priority) || !Number.isInteger(interval)) throw new Error('invalid goal priority or interval');
    const id = randomUUID().replaceAll('-', ''), now = Date.now() / 1000;
    const db = operatorDb();
    try {
      db.query(`INSERT INTO goals(id,dedupe_key,title,details,priority,state,source,recurring,interval_seconds,retry_at,attempts,last_error,created_at,updated_at)
        VALUES(?,?,?,?,?,'queued',?,?,?,?,0,'',?,?)
        ON CONFLICT(dedupe_key) DO UPDATE SET title=excluded.title,details=excluded.details,
        priority=MAX(goals.priority,excluded.priority),state=CASE WHEN goals.state IN ('in_progress','blocked','retry') OR
          (goals.state='resolved' AND goals.recurring=0) THEN goals.state ELSE 'queued' END,
        retry_at=CASE WHEN goals.state IN ('in_progress','blocked','retry') THEN goals.retry_at ELSE 0 END,
        updated_at=excluded.updated_at WHERE goals.state <> 'superseded'`).run(id, dedupe, title, details, priority, source, recurring ? 1 : 0, interval, 0, now, now);
      return db.query('SELECT * FROM goals WHERE dedupe_key=?').get(dedupe) ?? { dedupe_key: dedupe };
    } finally { db.close(); }
  }
  async function testProfile(args) {
    const profile = String(args.profile), timeoutMs = Math.max(5, Math.min(Number.parseInt(args.timeout_seconds ?? 1800, 10), 3600)) * 1000;
    if (!Number.isFinite(timeoutMs)) throw new Error('invalid timeout_seconds');
    const invoke = (argv, cwd = projectRoot) => execute(argv, { cwd, timeoutMs });
    if (profile === 'python-compile') throw new Error('python-compile retired; use bun-syntax for JS/TS source');
    if (profile === 'git-diff-check') return { profile, result: await invoke(['git', 'diff', '--check']) };
    if (profile === 'bonfyre-zig-build') return { profile, result: await invoke(['zig', 'build'], projectPath('bonfyre-zig')) };
    if (profile === 'bonfyre-final') return { profile, result: await invoke([join(home, '.bun/bin/bun'), 'run', 'final'], projectPath('bonfyre-zig')) };
    if (profile === 'cmd-make') {
      const family = String(args.target ?? '');
      if (!/^Bonfyre[A-Za-z0-9_-]+$/.test(family)) throw new Error('cmd-make target must be a Bonfyre command family');
      // Source-pinned narrow builds do not run the full `all` plus `test` graph.
      const scopedTargets = {
        BonfyreWebAddress: new Set(['build/web_main.o', 'bonfyre-web-address', 'community-journey-witness', 'route-presentation-witness']),
      };
      let selected = null;
      if (args.paths !== undefined) {
        if (!Array.isArray(args.paths) || args.paths.length !== 1 ||
            typeof args.paths[0] !== 'string' || !scopedTargets[family]?.has(args.paths[0]))
          throw new Error('cmd-make scoped target is not an approved native witness');
        selected = args.paths[0];
      }
      const cwd = projectPath(`cmd/${family}`);
      const build = await invoke(['make', '-j2', ...(selected ? [selected] : [])], cwd);
      if (build.exit_code !== 0 || selected) return { profile, scope: selected || 'all', build, result: build };
      const makefile = join(cwd, 'Makefile');
      const ownsTests = existsSync(makefile) && /(^|\n)test\s*:/m.test(readFileSync(makefile, 'utf8'));
      if (!ownsTests) return { profile, build, result: build };
      const tests = await invoke(['make', 'test'], cwd);
      return { profile, build, tests, result: tests };
    }
    // A bounded test runner: caller chooses only existing source-controlled test files,
    // never an argv command, arbitrary script or path outside the repository.
    if (profile === 'bun-tests') {
      if (args.target !== undefined || !Array.isArray(args.paths) || !args.paths.length || args.paths.length > 12)
        throw new Error('bun-tests requires 1–12 test paths and no target');
      const seen = new Set(), tests = args.paths.map(value => {
        const relative = safeRelative(String(value));
        if (!/^(?:components|integrations|Aurekai|scripts|cmd|bonfyre-zig)\//.test(relative) ||
            !relative.includes('/tests/') || !/\.test\.(?:mjs|js|ts|tsx)$/.test(relative))
          throw new Error('bun-tests accepts only repository tests/*.test.mjs/js/ts/tsx files');
        if (seen.has(relative)) throw new Error('duplicate bun test path');
        seen.add(relative);
        const absolute = projectPath(relative);
        if (!regular(absolute) || lstatSync(absolute).size > MAX_SYNTAX_BYTES)
          throw new Error('test file is unavailable or exceeds size bound');
        return absolute;
      });
      return { profile, test_files: [...seen], result: await invoke([join(home, '.bun/bin/bun'), 'test', ...tests]) };
    }
    if (profile === 'service-site-planner') {
      if (args.target !== undefined || args.paths !== undefined) throw new Error('service-site-planner has a fixed test scope');
      const bun = join(home, '.bun/bin/bun');
      const tests = await invoke([bun, 'test', ...[
        'components/BonfyreServiceSite/tests/planner.test.mjs',
        'components/BonfyreServiceSite/tests/planner-projects.test.mjs',
        'components/BonfyreServiceSite/tests/planner-spatial.test.mjs',
        'components/BonfyreServiceSite/tests/opportunity-selection.test.mjs',
      ].map(projectPath)]);
      if (tests.exit_code !== 0) return { profile, tests, result: tests };
      const scratch = mkdtempSync(join(tmpdir(), 'bonfyre-planner-'));
      try {
        const buildDir = join(scratch, 'build'), proofDir = join(scratch, 'proof');
        const build = await invoke([bun, projectPath('components/BonfyreServiceSite/build.mjs'),
          projectPath('packs/aurekai-work-samples/site'), '--out', buildDir]);
        if (build.exit_code !== 0) return { profile, tests, build, result: build };
        // Browser harness uses its own local HTTP worker and synthetic leads; no production writes.
        const browser = await invoke(['/usr/bin/env', 'PLANNER_BUILD='+buildDir, 'PLANNER_PROOF='+proofDir,
          bun, projectPath('components/BonfyreServiceSite/tests/planner-browser.test.mjs')]);
        let proof = null;
        const proofFile = join(proofDir, 'browser-verification.json');
        if (browser.exit_code === 0 && regular(proofFile)) {
          const value = JSON.parse(readFileSync(proofFile, 'utf8'));
          proof = { checks: value.checks?.length ?? 0, errors: value.errors?.length ?? 0,
            external_writes: value.external_writes ?? null };
        }
        return { profile, tests, build, browser, proof, result: browser };
      } finally { rmSync(scratch, { recursive: true, force: true }); }
    }
    if (profile === 'training-mcp-smoke') {
      const tests = [
        'integrations/mcp/tests/training_public_programs.test.mjs',
        'integrations/mcp/tests/project_native.test.mjs',
        'integrations/mcp/tests/project_node.test.mjs',
        'integrations/mcp/tests/project_operator.test.mjs',
        'integrations/mcp/tests/project_service_profiles.test.mjs',
        'integrations/mcp/tests/project_reads.test.mjs',
        // Operator/provider recovery and Fabric locks share this fixed,
        // local, non-dispatching source-owned regression profile.
        'integrations/mcp/tests/operator_store.test.mjs',
        'integrations/mcp/tests/operator_daemon.test.mjs',
        'integrations/mcp/tests/operator_tools.test.mjs',
        'integrations/mcp/tests/operator_provider.test.mjs',
        'integrations/mcp/tests/operator_recovery.test.mjs',
        'integrations/mcp/tests/operator_integration.test.mjs',
        'integrations/mcp/tests/fabric_store.test.mjs',
        'integrations/estate/control_plane.test.mjs',
        'integrations/estate/live_adapters.test.mjs',
        'integrations/estate/provider_fleet.test.mjs',
        'integrations/estate/generation_fabric.test.mjs',
        'integrations/mcp/native_generation_surface.test.mjs',
        'integrations/wordpress/native_generation.test.mjs',
        'components/BonfyreServiceSite/tests/training-requests.test.mjs',
        'components/BonfyreServiceSite/tests/training-publishing.test.mjs',
      ];
      return { profile, result: await invoke([join(home, '.bun/bin/bun'), 'test',
        ...tests.map(projectPath)]) };
    }
    if (profile === 'cloudflare-bridge') {
      if (args.paths !== undefined || args.target !== undefined) throw new Error('cloudflare-bridge has a fixed test scope');
      const tests = [
        'integrations/cloudflare/workgraph-envelope.test.mjs',
        'integrations/cloudflare/workgraph-admission.test.mjs',
        'integrations/cloudflare/native-ingress.test.mjs',
        'integrations/cloudflare/replay-recovery.test.mjs',
        'integrations/cloudflare/codex-delivery.test.mjs',
        'integrations/cloudflare/codex-edge.test.mjs',
      ].map(projectPath);
      const build = await invoke(['make', '-j2'], projectPath('cmd/BonfyreQueue'));
      if (build.exit_code !== 0) return { profile, build, result: build };
      return { profile, build, result: await invoke([join(home, '.bun/bin/bun'), 'test', ...tests]) };
    }
    if (['bun-syntax', 'shell-syntax', 'node-syntax', 'php-lint'].includes(profile)) {
      if (!Array.isArray(args.paths) || !args.paths.length) throw new Error(`${profile} requires paths`);
      const files = args.paths.map(item => ({ relative: safeRelative(String(item)), path: projectPath(String(item)) }));
      if (profile === 'bun-syntax') {
        const results = files.map(item => {
          if (!regular(item.path)) throw new Error(`syntax path is not a file: ${item.relative}`);
          if (lstatSync(item.path).size > MAX_SYNTAX_BYTES) throw new Error('syntax source exceeds 10 MB');
          const extension = item.path.match(/\.(jsx?|mjs|cjs|tsx?|mts|cts)$/)?.[1];
          const loader = ({ js: 'js', mjs: 'js', cjs: 'js', jsx: 'jsx', ts: 'ts', mts: 'ts', cts: 'ts', tsx: 'tsx' })[extension];
          if (!loader) throw new Error(`unsupported Bun syntax file: ${item.relative}`);
          try { new Bun.Transpiler({ loader }).transformSync(readFileSync(item.path, 'utf8'));
            return { argv: ['bun-syntax', item.relative], cwd: projectRoot, exit_code: 0, stdout: '', stderr: '', duration_ms: 0 }; }
          catch (error) { return { argv: ['bun-syntax', item.relative], cwd: projectRoot, exit_code: 1,
            stdout: '', stderr: String(error).slice(0, 2000), duration_ms: 0 }; }
        });
        return { profile, result: { exit_code: results.find(item => item.exit_code)?.exit_code ?? 0, results,
          note: 'Syntax parse only; no typecheck, imports, source execution, or runtime verification.' } };
      }
      const prefix = { 'shell-syntax': ['bash', '-n'], 'node-syntax': ['node', '--check'], 'php-lint': ['php', '-l'] }[profile];
      const results = [];
      for (const item of files) results.push(await invoke([...prefix, item.path]));
      return { profile, result: { exit_code: results.find(item => item.exit_code)?.exit_code ?? 0, results } };
    }
    throw new Error(`unknown project test profile: ${profile}`);
  }

  async function organDriftSync(args = {}) {
    const allowed = ['target', 'install_missing'];
    if (Object.keys(args).some((key) => !allowed.includes(key))) throw new Error('unknown organ drift sync argument');
    const target = String(args.target || '').trim();
    if (!/^bonfyre(?:-[a-z0-9][a-z0-9-]*)?$/.test(target)) throw new Error('organ drift sync requires one exact bonfyre target');
    if (!writeEnabled) throw new Error('project write gate disabled');
    if (existsSync(stopPath)) throw new Error('STOP active; organ drift sync refused');
    const script = projectPath('scripts/organ_drift.sh');
    if (!regular(script)) throw new Error('organ drift owner is not a regular file');
    const argv = [script, '--sync', '--only', target, '--json', ...(args.install_missing === true ? ['--install-missing'] : [])];
    const result = await execute(argv, { timeoutMs: 120_000 });
    if (result.exit_code !== 0) throw new Error(`organ drift sync failed for ${target}`);
    let sync;
    try { sync = JSON.parse(result.stdout); } catch { throw new Error('organ drift sync returned malformed JSON'); }
    if (sync?.schema !== 'bonfyre.organ-drift-report.v2' || sync.mode !== 'sync') throw new Error('organ drift sync returned wrong schema');
    const verifyResult = await execute([script, '--verify', '--only', target, '--json'], { timeoutMs: 60_000 });
    let verify;
    try { verify = JSON.parse(verifyResult.stdout); } catch { throw new Error('organ drift verify returned malformed JSON'); }
    if (verifyResult.exit_code !== 0 || verify?.schema !== 'bonfyre.organ-drift-report.v2' || verify.behind || verify.missing || verify.refused) {
      throw new Error(`organ drift verify failed after sync for ${target}`);
    }
    return {
      schema_version: 'bonfyre.organ-drift.mcp-sync.v1',
      target,
      install_missing: args.install_missing === true,
      sync,
      verify,
      forbidden_inference: 'Managed byte identity after promotion does not prove semantic correctness, service restart, dependency health, or production deployment.',
    };
  }

  // File removal is deliberately a different authority from ordinary Git patches.
  // Every cut needs native replacement bytes, passing owner tests, current hashes,
  // and a byte-for-byte restorable archive before a source can disappear.
  async function retireSources(args) {
    if (args.confirm !== true || !writeEnabled || !lowRiskEnabled ||
        policy().auto_apply_low_risk_patches !== true)
      throw new Error('source retirement needs explicit confirmation and an enabled Project owner');
    if (Object.keys(args).some(key => !['confirm', 'entries', 'native_source', 'native_sha256', 'reason'].includes(key)))
      throw new Error('unsupported source retirement argument');
    const reason = String(args.reason ?? '');
    if (reason.length < 16 || reason.length > 200 || /[\u0000-\u001f\u007f]/.test(reason))
      throw new Error('source retirement needs a bounded, specific reason');
    if (!Array.isArray(args.entries) || args.entries.length < 1 || args.entries.length > 32)
      throw new Error('retire 1–32 reviewed sources at once');
    const nativeSource = safeRelative(args.native_source);
    const match = /^cmd\/(Bonfyre[A-Za-z0-9_-]+)\/src\/[A-Za-z0-9_.-]+\.c$/.exec(nativeSource);
    if (!match || !/^[a-f0-9]{64}$/.test(args.native_sha256) ||
        fileHash(nativeSource) !== args.native_sha256)
      throw new Error('native replacement source is absent or changed');
    const paths = args.entries.map(entry => {
      if (!entry || Object.keys(entry).sort().join(',') !== 'path,sha256' ||
          typeof entry.sha256 !== 'string' || !/^[a-f0-9]{64}$/.test(entry.sha256))
        throw new Error('source retirement requires exact path/SHA-256 entries');
      const path = safeRelative(entry.path);
      if (!path.endsWith('.mjs') && !/\.(?:js|cjs|ts|tsx|py)$/.test(path))
        throw new Error('source retirement is for authored scripts only');
      if (!/^[A-Za-z0-9._/-]+$/.test(path))
        throw new Error('source retirement needs a portable, whitespace-free path');
      if (/(?:^|\/)tests?\//.test(path) || /(?:^|[._-])test(?:s)?\./.test(path))
        throw new Error('retiring tests requires a separate parity owner');
      return path;
    });
    if (new Set(paths).size !== paths.length) throw new Error('duplicate retirement source');
    const metadata = await pathMetadata(paths);
    const policyErrors = lowRiskErrors({ files: metadata, file_count: paths.length, changed_lines: 0,
      dirty_conflicts: metadata.filter(item => item.dirty_before).map(item => item.path) });
    if (policyErrors.length) throw new Error('source retirement refused by Project policy: ' + policyErrors.join('; '));
    for (let i = 0; i < paths.length; i++)
      if (!metadata[i].exists || metadata[i].sha256 !== args.entries[i].sha256)
        throw new Error('retirement source missing or changed: ' + paths[i]);
    const manifestSha256 = sha(paths.map((path, i) => `${args.entries[i].sha256}  ${path}\n`)
      .sort((a, b) => Buffer.compare(Buffer.from(a.slice(66)), Buffer.from(b.slice(66)))).join(''));
    const totalBytes = metadata.reduce((sum, item) => sum + lstatSync(projectPath(item.path)).size, 0);
    if (totalBytes > 8 * 1024 * 1024) throw new Error('source retirement exceeds 8 MiB batch limit');
    const nativeFamily = match[1], nativeMake = projectPath('cmd/' + nativeFamily + '/Makefile');
    if (!regular(nativeMake)) throw new Error('native replacement has no owner test Makefile');
    // A native owner may expose a parity-only retirement target when its full
    // release test also checks the global ratchet; the ratchet still gates release.
    const nativeMakeText = readFileSync(nativeMake, 'utf8');
    const testTarget = /^retirement-proof\s*:/m.test(nativeMakeText) ? 'retirement-proof' : 'test';
    const runNativeTests = () => execute(['make', '-C', 'cmd/' + nativeFamily, testTarget],
      { timeoutMs: 900000, maxOutputBytes: 300000 });
    const before = await runNativeTests();
    if (before.exit_code !== 0) return { retired: false, reason: 'native_tests_failed_before', native_family: nativeFamily,
      exit_code: before.exit_code, stderr_tail: String(before.stderr ?? '').slice(-1000) };
    if (fileHash(nativeSource) !== args.native_sha256 ||
        paths.some((path, i) => fileHash(path) !== args.entries[i].sha256))
      return { retired: false, reason: 'source_changed_during_native_tests' };
    const originals = metadata.map(item => ({ path: item.path, mode: item.mode,
      bytes: readFileSync(projectPath(item.path)) }));
    const restoreOriginals = () => {
      const conflicts = originals.filter((item, i) => existsSync(projectPath(item.path)) &&
        (fileHash(item.path) !== args.entries[i].sha256 || fileMode(item.path) !== item.mode));
      if (conflicts.length) return { restored: false, conflicted: conflicts.map(item => item.path) };
      for (const item of originals) if (!existsSync(projectPath(item.path))) {
        writeFileSync(projectPath(item.path), item.bytes, { flag: 'wx', mode: item.mode });
        chmodSync(projectPath(item.path), item.mode);
      }
      return { restored: originals.every((item, i) => fileHash(item.path) === args.entries[i].sha256 &&
        fileMode(item.path) === item.mode), conflicted: [] };
    };
    const name = 'verified-source-' + Math.floor(Date.now() / 1000) + '-' + randomBytes(6).toString('hex');
    const archiveRoot = join(home, '.bonfyre', 'archive');
    const archive = join(archiveRoot, 'retired', name + '.tar.zst');
    const script = projectPath('scripts/retire.sh');
    if (!regular(script)) throw new Error('source retirement archive owner unavailable');
    ensureStore();
    let cut;
    try {
      cut = await execute([script, name, reason, ...paths],
        { retirementArchiveRoot: archiveRoot, retirementManifestSha256: manifestSha256,
          timeoutMs: 300000, maxOutputBytes: 300000 });
    } catch {
      const recovery = restoreOriginals();
      return { retired: false, reason: 'archive_execution_uncertain', ...recovery,
        archive, requires_manual_reconciliation: !recovery.restored };
    }
    const removed = paths.every(path => !existsSync(projectPath(path)));
    const archivedManifest = join(archiveRoot, 'retired', name + '.sha256');
    const archived = regular(archive) && regular(archivedManifest) && sha(readFileSync(archivedManifest)) === manifestSha256;
    if (cut.exit_code !== 0 || !archived || !removed) {
      const recovery = restoreOriginals();
      return { retired: false, reason: 'archive_or_removal_uncertain', native_family: nativeFamily,
        archive_present: archived, sources_removed: removed, ...recovery, requires_manual_reconciliation: !recovery.restored };
    }
    let after, diff;
    try {
      after = await runNativeTests();
      diff = await execute(['git', 'diff', '--check'], { timeoutMs: 60000, maxOutputBytes: 300000 });
    } catch {
      const recovery = restoreOriginals();
      return { retired: false, reason: 'posttest_execution_uncertain', ...recovery, archive,
        requires_manual_reconciliation: !recovery.restored };
    }
    const good = after.exit_code === 0 && diff.exit_code === 0;
    if (!good) {
      const recovery = restoreOriginals();
      return { retired: false, reason: 'posttest_failed', ...recovery, archive,
        requires_manual_reconciliation: !recovery.restored,
        native_exit_code: after.exit_code, diff_exit_code: diff.exit_code };
    }
    let measurement = null;
    try { measurement = await execute([projectPath('scripts/native-ratchet.sh'), 'measure'],
      { timeoutMs: 120000, maxOutputBytes: 10000 }); } catch { /* Ratchet unverified; preserve receipt. */ }
    const receipt = { schema: 'bonfyre.project.verified-source-retirement.v1', id: name,
      native_source: nativeSource, native_sha256: args.native_sha256, manifest_sha256: manifestSha256, reason,
      files: paths.map((path, i) => ({ path, sha256: args.entries[i].sha256 })),
      archive, archive_verified: true, native_tests_before: before.exit_code,
      native_tests_after: after.exit_code, git_diff_check: diff.exit_code,
      ratchet_measurement: measurement?.exit_code === 0 ? String(measurement.stdout ?? '').trim() : null,
      full_ratchet_passed: false };
    try { atomicJson(join(receipts, name + '.retirement.json'), receipt); }
    catch {
      const recovery = restoreOriginals();
      return { retired: false, reason: 'receipt_persistence_failed', ...recovery, archive,
        requires_manual_reconciliation: !recovery.restored };
    }
    return { retired: true, receipt, next_gate: 'Run the full native ratchet, release acceptance and public readbacks before activation.' };
  }

  async function call(name, args = {}) {
    if (!args || typeof args !== 'object' || Array.isArray(args)) throw new Error('project tool arguments must be an object');
    if (name === 'project_patch_preview') {
      if (Object.keys(args).some(key => !['patch', 'paths'].includes(key)) || (args.patch === undefined) === (args.paths === undefined))
        throw new Error('provide exactly one of patch or paths');
      return args.paths !== undefined ? preflight(args.paths) : preview(args.patch);
    }
    if (name === 'project_test_profile') return testProfile(args);
    if (name === 'operator_status') return summary();
    if (name === 'operator_backlog_audit') return backlogAudit(args);
    if (name === 'operator_maturation_ledger') return args.action === 'read' ? maturationLedger(args) : serialize(() => maturationLedger(args));
    if (name === 'operator_goal_supersede') return serialize(() => supersedeGoals(args));
    if (name === 'organ_drift_sync') return serialize(() => organDriftSync(args));
    if (name === 'project_apply_patch') return serialize(() => { approve(args.approval_token); return applyPatch(String(args.patch), 'manual-approved'); });
    if (name === 'project_apply_low_risk_patch') return serialize(() => applyPatch(String(args.patch), 'autonomous-low-risk'));
    if (name === 'project_retire_sources') return serialize(() => retireSources(args));
    if (name === 'project_revert_receipt') return serialize(() => revert(String(args.receipt_id)));
    if (name === 'project_commit_receipt') return serialize(() => commitReceipt(String(args.receipt_id), args.message));
    if (name === 'operator_goal_add') return serialize(() => addGoal(args));
    if (name === 'operator_pause') return serialize(() => { ensureStore(); atomicJson(stopPath, { paused_at: Date.now() / 1000, reason: String(args.reason ?? 'paused through MCP') }); return summary(); });
    if (name === 'operator_resume') return serialize(() => { ensureStore(); rmSync(stopPath, { force: true }); return summary(); });
    throw new Error(`unknown project operator tool: ${name}`);
  }
  return { call };
}
