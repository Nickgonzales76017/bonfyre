// GitHub source inspection for the hypervisor. A checkout is evidence, never an
// executable or an automatically promoted provider. Repository code is not run.
import { createHash } from 'node:crypto';
import { existsSync, lstatSync, mkdtempSync, readdirSync, realpathSync, renameSync, rmSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { canonicalJson } from './canonical_json.mjs';
import { fabricGitEnvironment } from './fabric_store.mjs';
import { isNativeProbeExecutable, nativeExecutionPlan, nativeProbePassed, nativeProbeReceipt } from './native_probe.mjs';
import { projectRun } from './project_runtime.mjs';
import { readSpinesBytes } from './spines_store.mjs';

const sha = value => createHash('sha256').update(value).digest('hex');
const MAX_CHECKOUT_BYTES = 512 * 1024 * 1024;
const MANIFESTS = ['package.json', 'pyproject.toml', 'Cargo.toml', 'go.mod', 'Gemfile', 'pom.xml',
  'build.gradle', 'requirements.txt', 'Dockerfile', 'flake.nix'];
const READMES = ['README.md', 'README.rst', 'README.txt', 'README'];
function repository(input) {
  if (typeof input !== 'string' || input.length > 512) throw new Error('invalid GitHub repository');
  let value = input.trim();
  if (value.startsWith('git@github.com:')) value = value.slice('git@github.com:'.length);
  else if (value.startsWith('https://github.com/')) value = value.slice('https://github.com/'.length).replace(/\/$/, '');
  value = value.replace(/\.git$/, '');
  if (!/^[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+$/.test(value) || value.split('/').some(part => ['.', '..'].includes(part)))
    throw new Error('repository must be a declared GitHub owner/name');
  return value;
}
function requestedRef(ref) {
  if (ref === null || ref === undefined) return null;
  if (typeof ref !== 'string' || ref.length > 256 || !/^(?:[a-f0-9]{40}|[a-f0-9]{64}|refs\/(?:heads|tags)\/[A-Za-z0-9][A-Za-z0-9._/-]*)$/.test(ref) ||
      ref.includes('..') || ref.includes('//') || ref.endsWith('/') || ref.endsWith('.lock'))
    throw new Error('ref must be an immutable commit or explicit safe heads/tags ref');
  return ref;
}
function stableId(prefix, ...parts) { return `${prefix}-${sha(parts.join('\0')).slice(0, 24)}`; }
function regular(root, name, maximum = 8 * 1024 * 1024) {
  const path = join(root, name);
  try { if (!lstatSync(path).isFile()) return null; return readSpinesBytes(path, maximum).toString('utf8'); }
  catch (error) { if (error.code === 'ENOENT') return null; throw error; }
}
function regularFile(root, name) {
  try { return lstatSync(join(root, name)).isFile(); }
  catch (error) { if (error.code === 'ENOENT') return false; throw error; }
}
function checkoutUsage(root, limit = MAX_CHECKOUT_BYTES) {
  if (!existsSync(root)) return 0;
  const pending = [[root, 0]]; let bytes = 0, entries = 0;
  while (pending.length) {
    const [path, depth] = pending.pop();
    if (depth > 64 || ++entries > 100000) throw new Error('checkout entry budget exceeded');
    const stat = lstatSync(path); bytes += stat.size;
    if (bytes > limit) throw new Error('checkout byte budget exceeded');
    if (stat.isDirectory()) for (const entry of readdirSync(path)) pending.push([join(path, entry), depth + 1]);
  }
  return bytes;
}
function license(root) {
  const names = readdirSync(root).sort().filter(name => /^(?:license|copying)/i.test(name));
  for (const name of names) {
    const source = regular(root, name, 100000);
    if (source === null) continue;
    const text = source.toLowerCase();
    return { name: text.includes('mit license') ? 'MIT' : text.includes('apache license') && text.includes('version 2.0') ? 'Apache-2.0' :
      text.includes('gnu affero general public license') ? 'AGPL' : text.includes('functional source license') ? 'FSL' : 'other',
    path: join(root, name) };
  }
  return { name: 'unknown', path: null };
}
function declaredCommands(root) {
  const found = new Set(), packageText = regular(root, 'package.json');
  if (packageText !== null) {
    try {
      const value = JSON.parse(packageText);
      if (typeof value.bin === 'string') found.add(typeof value.name === 'string' ? value.name : 'package');
      else if (value.bin && typeof value.bin === 'object' && !Array.isArray(value.bin))
        for (const name of Object.keys(value.bin)) found.add(name);
    } catch { /* malformed manifest is not an executable declaration */ }
  }
  const pyproject = regular(root, 'pyproject.toml');
  if (pyproject !== null) for (const match of pyproject.matchAll(/^\s*([A-Za-z0-9_.-]+)\s*=\s*['"][^'"]+['"]/gm)) found.add(match[1]);
  const cargo = regular(root, 'Cargo.toml');
  if (cargo !== null) { const match = /^name\s*=\s*['"]([^'"]+)['"]/m.exec(cargo); if (match) found.add(match[1]); }
  return [...found].sort();
}
export function createHypervisorSources({ store, home = store?.home, env = process.env, run = projectRun,
  checkStop = () => {}, maxCheckoutBytes = MAX_CHECKOUT_BYTES } = {}) {
  if (!store?.registry || !store?.providers || !store?.getSource || !store?.beginSource || !store?.finishSource ||
      !store?.recordSourceUpdate || typeof home !== 'string') throw new Error('hypervisor source store interface incomplete');
  if (!Number.isSafeInteger(maxCheckoutBytes) || maxCheckoutBytes < 1024 || maxCheckoutBytes > MAX_CHECKOUT_BYTES)
    throw new Error('invalid checkout budget');
  const sourceRoot = join(home, 'sources'), childEnv = { ...fabricGitEnvironment(env), GIT_ALLOW_PROTOCOL: 'https' };
  const plan = nativeExecutionPlan('git', { env: childEnv });
  const gitPrefix = plan?.argv?.length === 1 && isNativeProbeExecutable(plan.argv[0]) ? plan.argv : null;
  function admitted() { if (!gitPrefix) throw new Error('native git executable unavailable'); }
  function trusted(repo) {
    const normalized = repository(repo);
    if (!store.registry().projects.some(project => project.repo.toLowerCase() === normalized.toLowerCase()))
      throw new Error('repository is not declared by the hypervisor registry');
    return normalized;
  }
  async function git(args, { cwd = home, timeoutMs = 30000, signal, checkout = null } = {}) {
    admitted(); signal?.throwIfAborted(); checkStop();
    const local = new AbortController(), joined = signal ? AbortSignal.any([signal, local.signal]) : local.signal;
    const timer = checkout ? setInterval(() => {
      try { checkoutUsage(checkout, maxCheckoutBytes); }
      catch (error) { local.abort(error); }
    }, 250) : null;
    const argv = [...gitPrefix, '-c', 'core.hooksPath=/dev/null', '-c', 'core.fsmonitor=false',
      '-c', 'protocol.file.allow=never', '-c', 'protocol.ext.allow=never', ...args];
    try {
      const result = await run(argv, { cwd, env: childEnv, timeoutMs, maxOutputBytes: 2 * 1024 * 1024, signal: joined });
      joined.throwIfAborted(); checkStop();
      if (checkout) checkoutUsage(checkout, maxCheckoutBytes);
      return { result, receipt: nativeProbeReceipt(argv, result) };
    } finally { if (timer) clearInterval(timer); }
  }
  async function inspect(checkout, repo, signal) {
    if (!lstatSync(checkout).isDirectory() || realpathSync(checkout) !== resolve(checkout))
      throw new Error('source checkout identity changed');
    checkoutUsage(checkout, maxCheckoutBytes);
    const status = await git(['status', '--porcelain=v1', '-z', '--untracked-files=all'],
      { cwd: checkout, timeoutMs: 30000, signal });
    if (!nativeProbePassed(status.receipt) || String(status.result.stdout ?? '') !== '')
      throw new Error('source checkout is not clean');
    const head = await git(['rev-parse', 'HEAD'], { cwd: checkout, timeoutMs: 20000, signal });
    const resolved = String(head.result.stdout ?? '').trim();
    if (!nativeProbePassed(head.receipt) || !/^(?:[a-f0-9]{40}|[a-f0-9]{64})$/.test(resolved))
      throw new Error('source HEAD is not a resolved commit');
    const tree = await git(['ls-files', '-z'], { cwd: checkout, timeoutMs: 30000, signal });
    if (!nativeProbePassed(tree.receipt)) throw new Error('source tracked file inventory failed');
    const raw = String(tree.result.stdout ?? '');
    if (!raw.endsWith('\0') && raw.length) throw new Error('truncated tracked file inventory');
    const files = raw ? raw.slice(0, -1).split('\0') : [];
    if (files.length > 100000 || files.some(file => !file || file.includes('\0'))) throw new Error('invalid tracked file inventory');
    const lic = license(checkout), manifests = MANIFESTS.filter(name => regularFile(checkout, name));
    const commands = declaredCommands(checkout);
    const readme = READMES.find(name => regularFile(checkout, name)) ?? null;
    const material = { repo, resolved_ref: resolved, license: lic.name, manifests, commands, files };
    return { material, digest: sha(canonicalJson(material, { spaced: true })), license_path: lic.path,
      readme: readme ? join(checkout, readme) : null, clean: status.receipt, head: head.receipt, tree: tree.receipt };
  }
  async function ingestSource(repo, { ref = null, execute_tests = false, signal } = {}) {
    const normalized = trusted(repo), pin = requestedRef(ref);
    if (execute_tests !== false) throw new Error('source tests require an explicit sandboxed execution owner');
    signal?.throwIfAborted(); checkStop(); admitted();
    const sourceId = stableId('source', normalized, pin ?? 'HEAD'), checkout = join(sourceRoot, sourceId);
    const prior = store.getSource(sourceId);
    if (prior) {
      if (prior.state !== 'inspected' || prior.repo !== normalized || prior.requested_ref !== pin || prior.checkout_path !== checkout)
        throw new Error('source attempt requires reconciliation before retry');
      const current = await inspect(checkout, normalized, signal);
      if (current.digest !== prior.digest || current.material.resolved_ref !== prior.resolved_ref ||
          canonicalJson(current.material) !== canonicalJson(Object.fromEntries(
            ['repo', 'resolved_ref', 'license', 'manifests', 'commands', 'files'].map(key => [key, prior.manifest[key]]))))
        throw new Error('existing source checkout does not match persisted manifest');
      return { source_id: sourceId, state: 'inspected', reused: true, repo: normalized, checkout,
        resolved_ref: prior.resolved_ref, digest: prior.digest, license: prior.license,
        manifests: prior.manifest.manifests, commands: prior.manifest.commands, test_results: [] };
    }
    if (existsSync(checkout)) throw new Error('unowned source checkout requires reconciliation');
    if (!existsSync(sourceRoot) || !lstatSync(sourceRoot).isDirectory() || realpathSync(sourceRoot) !== resolve(sourceRoot))
      throw new Error('source root must be an owned real directory');
    const started = store.beginSource({ sourceId, repo: normalized, requestedRef: pin, checkoutPath: checkout });
    const staging = mkdtempSync(join(sourceRoot, `${sourceId}.stage-`));
    let renamed = false;
    const receipts = {};
    try {
      const url = `https://github.com/${normalized}.git`;
      const cloneArgs = pin ? ['clone', '--filter=blob:none', '--no-checkout', '--', url, staging] :
        ['clone', '--filter=blob:none', '--depth', '1', '--', url, staging];
      const clone = await git(cloneArgs, { timeoutMs: 300000, signal, checkout: staging });
      receipts.clone = clone.receipt;
      if (!nativeProbePassed(clone.receipt)) throw new Error('source clone failed');
      if (pin) {
        const fetch = await git(['fetch', '--depth', '1', 'origin', pin], { cwd: staging, timeoutMs: 300000, signal, checkout: staging });
        receipts.fetch = fetch.receipt;
        if (!nativeProbePassed(fetch.receipt)) throw new Error('source requested ref fetch failed');
        const detached = await git(['checkout', '--detach', 'FETCH_HEAD'], { cwd: staging, timeoutMs: 120000, signal, checkout: staging });
        receipts.checkout = detached.receipt;
        if (!nativeProbePassed(detached.receipt)) throw new Error('source detach checkout failed');
      }
      const found = await inspect(staging, normalized, signal);
      if (existsSync(checkout)) throw new Error('source checkout appeared concurrently');
      renameSync(staging, checkout); renamed = true;
      const manifest = { ...found.material, checkout, license_path: found.license_path ?
        join(checkout, found.license_path.slice(staging.length + 1)) : null,
      readme: found.readme ? join(checkout, found.readme.slice(staging.length + 1)) : null,
      test_results: [], clone: receipts, clean: found.clean, head: found.head, tree: found.tree,
      started_receipt_id: started.started_receipt_id };
      store.finishSource({ sourceId, state: 'inspected', resolvedRef: found.material.resolved_ref,
        license: found.material.license, digest: found.digest, manifest });
      return { source_id: sourceId, state: 'inspected', repo: normalized, resolved_ref: found.material.resolved_ref,
        license: found.material.license, digest: found.digest, checkout, manifests: found.material.manifests,
        commands: found.material.commands, test_results: [] };
    } catch (error) {
      if (!renamed) {
        rmSync(staging, { recursive: true, force: true });
        try { store.finishSource({ sourceId, state: 'failed', manifest: { reason: error?.name ?? 'Error',
          probes: receipts, started_receipt_id: started.started_receipt_id } }); } catch { /* retain started receipt for reconciliation */ }
      }
      throw error;
    }
  }
  async function sourceUpdateCheck({ provider_id = null, signal } = {}) {
    signal?.throwIfAborted(); checkStop(); admitted();
    const providers = provider_id ? [store.provider(provider_id)].filter(Boolean) : store.providers();
    if (provider_id && !providers.length) throw new Error('provider not found');
    if (providers.length > 32) throw new Error('source update provider budget exceeded');
    const deadline = AbortSignal.timeout(300000), boundedSignal = signal ? AbortSignal.any([signal, deadline]) : deadline;
    const updates = [];
    for (const provider of providers) {
      boundedSignal.throwIfAborted(); checkStop();
      if (provider.repo.startsWith('local/')) continue;
      const repo = trusted(provider.repo), url = `https://github.com/${repo}.git`;
      const queried = await git(['ls-remote', url, 'HEAD'], { timeoutMs: 60000, signal: boundedSignal });
      const words = String(queried.result.stdout ?? '').trim().split(/\s+/), remote =
        nativeProbePassed(queried.receipt) && /^(?:[a-f0-9]{40}|[a-f0-9]{64})$/.test(words[0]) && words[1] === 'HEAD' ? words[0] : null;
      const old = provider.source_ref ?? null, state = remote === null ? 'unknown' : old === null ? 'unpinned' : old === remote ? 'current' : 'candidate';
      const update = { provider_id: provider.id, repo, old_ref: old, new_ref: remote, state, probe: queried.receipt };
      updates.push(update);
      if (state === 'candidate') {
        const id = stableId('update', provider.id, old ?? '', remote);
        update.candidate = store.recordSourceUpdate({ id, providerId: provider.id, oldRef: old, newRef: remote, evidence: update });
      }
    }
    return { updates };
  }
  return Object.freeze({ ingestSource, sourceUpdateCheck });
}
