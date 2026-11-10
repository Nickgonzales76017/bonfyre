// Reusable exact-owner promotion. Configuration is authored in the calling
// owner script; it is not accepted from an untrusted network request.
import assert from 'node:assert/strict';
import { readFileSync, mkdtempSync, realpathSync, existsSync, lstatSync } from 'node:fs';
import { homedir } from 'node:os';
import { join, relative, isAbsolute } from 'node:path';
import { createHash } from 'node:crypto';

export const fileDigest = path => createHash('sha256').update(readFileSync(path)).digest('hex');
const nonProgram = value => Object.fromEntries(Object.entries(value).filter(([key]) => key !== 'ProgramArguments'));
const live = pid => { try { process.kill(pid, 0); return true; } catch (error) { if (error.code === 'ESRCH') return false; throw error; } };
export async function promoteOwner({ root, label, expectedProgram, entry, launcher, launcherDigest, launcherSource,
  databases, sources, endpoint, service, collect, continuity, verifyCli, preStop, postStop, cliEnvironment = {}, refresh = [], apply = false }) {
  root = realpathSync(root);
  const binary = realpathSync(process.execPath), domain = `gui/${process.getuid()}`;
  assert.equal(binary.split('/').at(-1), 'bun'); assert(/^com\.bonfyre\.[a-z0-9-]+$/.test(label));
  assert(Boolean(launcher) === Boolean(launcherSource) && Boolean(launcher) === Boolean(launcherDigest), 'partial launcher configuration');
  for (const path of [entry, ...(launcherSource ? [launcherSource] : []), ...sources]) {
    const rel = relative(root, realpathSync(path));
    assert(rel && !rel.startsWith('..') && !isAbsolute(rel), 'deployment input outside repository');
  }
  if (launcher) {
    assert(lstatSync(launcher).isFile() && !lstatSync(launcher).isSymbolicLink(), 'launcher must be a regular file');
    assert.equal(fileDigest(launcher), launcherDigest, 'unexpected installed CLI');
  }
  function run(argv, { allowed = [0], env = process.env, timeout = 30_000 } = {}) {
    const child = Bun.spawnSync(argv, { cwd: root, env, stdout: 'pipe', stderr: 'pipe', timeout });
    assert(allowed.includes(child.exitCode), `${argv[0]} failed (${child.exitCode})`);
    return child.stdout.toString();
  }
  const parse = path => JSON.parse(run(['/usr/bin/plutil', '-convert', 'json', '-o', '-', path]));
  const plist = join(homedir(), 'Library/LaunchAgents', `${label}.plist`), before = parse(plist), plistDigest = fileDigest(plist);
  assert.equal(before.Label, label); assert.deepEqual(before.ProgramArguments, expectedProgram);
  const env = { ...process.env, ...before.EnvironmentVariables };
  const token = readFileSync(env.BONFYRE_MCP_TOKEN_FILE || join(homedir(), '.bonfyre/mcp/token'), 'utf8').trim();
  assert(token, 'missing owner token');
  const redact = error => String(error?.message || error).replaceAll(token, '[redacted]').slice(0, 2000);
  const address = new URL(endpoint);
  assert(address.protocol === 'http:' && address.hostname === '127.0.0.1' && !address.username && !address.password && address.pathname === '/');
  const inspect = name => {
    const output = run(['/bin/launchctl', 'print', `${domain}/${name}`]);
    const pid = Number(output.match(/^\s*pid = (\d+)$/m)?.[1]);
    assert(pid > 1 && /state = running/.test(output), `${name} not running`);
    return { pid, output };
  };
  const quiet = pid => assert(!run(['/usr/sbin/lsof', '-nP', '-a', '-p', String(pid), '-iTCP'], { allowed: [0, 1] }).includes('(ESTABLISHED)'), 'owner has active connections');
  const ready = async (url, name, runtime, ownerLabel, oldPid = null) => {
    const deadline = Date.now() + 20_000;
    while (Date.now() < deadline) {
      try {
        const response = await fetch(`${url}/health`, { redirect: 'error', headers: { connection: 'close' }, signal: AbortSignal.timeout(1500) });
        const health = await response.json();
        if (response.ok && health.service === name && (health.runtime || 'python') === runtime) {
          const state = inspect(ownerLabel), port = new URL(url).port;
          if (state.pid !== oldPid && run(['/usr/sbin/lsof', '-nP', '-a', '-p', String(state.pid), `-iTCP:${port}`, '-sTCP:LISTEN'], { allowed: [0, 1] }).includes('(LISTEN)')) return state;
        }
      } catch { /* Read-only startup observation, not an effect retry. */ }
      await Bun.sleep(200);
    }
    throw new Error(`${name} ${runtime} startup health unavailable after twenty seconds`);
  };
  const previous = inspect(label); quiet(previous.pid);
  const sourceHashes = Object.fromEntries([...new Set([entry, ...(launcherSource ? [launcherSource] : []), ...sources])].map(path => [relative(root, path), fileDigest(path)]));
  const rollbackPolicy = 'Primary configuration/runtime rollback only; accepted database and artifact writes remain. Already-refreshed Bun dependents retain the compatible shared code and are reverified against restored primary service. Backups require explicit reconciliation, never automatic overwrite.';
  if (!apply) return { status: 'ready', service: label, previous_pid: previous.pid, source_sha256: sourceHashes, rollback_policy: rollbackPolicy };
  const baseline = await collect({ token, env, samples: 25 });
  const dependents = refresh.map(owner => {
    const state = inspect(owner.label); assert(state.output.includes(`program = ${binary}`)); quiet(state.pid);
    return { ...owner, previousPid: state.pid };
  });
  const backup = mkdtempSync('/private/tmp/bonfyre-owner-cutover.');
  const original = join(backup, 'original.plist'), candidate = join(backup, 'candidate.plist'), originalLauncher = join(backup, 'launcher.original');
  run(['/bin/cp', '-p', plist, original]); run(['/bin/cp', '-p', plist, candidate]);
  if (launcher) run(['/bin/cp', '-p', launcher, originalLauncher]);
  run(['/usr/libexec/PlistBuddy', '-c', `Set :ProgramArguments:0 ${binary}`, candidate]);
  run(['/usr/libexec/PlistBuddy', '-c', `Set :ProgramArguments:1 ${entry}`, candidate]); run(['/usr/bin/plutil', '-lint', candidate]);
  assert.deepEqual(nonProgram(before), nonProgram(parse(candidate)));
  assert.equal(fileDigest(plist), plistDigest); if (launcher) assert.equal(fileDigest(launcher), launcherDigest);
  for (const [name, digest] of Object.entries(sourceHashes)) assert.equal(fileDigest(join(root, name)), digest, `source changed: ${name}`);
  quiet(previous.pid);
  if (preStop) await preStop({ run, pid: previous.pid, baseline, env });
  const backups = [], refreshAttempts = [];
  let stopped = false;
  try {
    stopped = true; // An uncertain bootout result still requires reconciliation.
    run(['/bin/launchctl', 'bootout', domain, plist]);
    for (let attempt = 0; attempt < 50 && live(previous.pid); attempt++) await Bun.sleep(100);
    assert(!live(previous.pid), 'old owner must exit before backup');
    if (postStop) await postStop({ run, baseline, env });
    for (let index = 0; index < databases.length; index++) {
      const path = databases[index];
      assert(lstatSync(path).isFile() && !lstatSync(path).isSymbolicLink());
      assert(!existsSync(`${path}-wal`), 'database WAL remains; require online backup reconciliation');
      const destination = join(backup, `database-${index}.sqlite3`);
      run(['/bin/cp', '-p', path, destination]); assert.equal(fileDigest(path), fileDigest(destination));
      backups.push({ source: path, path: destination, sha256: fileDigest(destination) });
    }
    run(['/bin/cp', '-p', candidate, plist]); run(['/bin/launchctl', 'bootstrap', domain, plist]);
    const current = await ready(endpoint, service, 'bun', label, previous.pid); assert(current.output.includes(`program = ${binary}`));
    assert(!live(previous.pid)); assert.deepEqual(nonProgram(before), nonProgram(parse(plist)));
    const after = await collect({ token, env, samples: 25 }); await continuity(baseline, after);
    if (launcher) run(['/usr/bin/install', '-m', '755', launcherSource, launcher]);
    const cliEnv = { ...env, ...cliEnvironment, BONFYRE_BUN: binary, BONFYRE_ROOT: root };
    const cli = verifyCli ? await verifyCli({ run: argv => run(argv, { env: cliEnv }), launcher, baseline, after, env: cliEnv }) : { installed: false };
    const refreshed = [];
    for (const owner of dependents) {
      assert.equal(inspect(owner.label).pid, owner.previousPid, 'dependent owner changed during promotion'); quiet(owner.previousPid);
      refreshAttempts.push(owner);
      run(['/bin/launchctl', 'kickstart', '-k', `${domain}/${owner.label}`]);
      await ready(owner.endpoint, owner.service, 'bun', owner.label, owner.previousPid);
      const witness = await owner.collect({ token });
      refreshed.push({ service: owner.label, previous_pid: owner.previousPid, pid: inspect(owner.label).pid, witness });
    }
    for (const [name, digest] of Object.entries(sourceHashes)) assert.equal(fileDigest(join(root, name)), digest, `source changed during promotion: ${name}`);
    return { schema: 'bonfyre.owner-cutover.v1', measured_at: new Date().toISOString(), service: `${domain}/${label}`,
      previous: { pid: previous.pid, program: expectedProgram, exited: true, rollback_plist: original, ...(launcher ? { rollback_launcher: originalLauncher } : {}), database_backups: backups },
      installed: { pid: current.pid, program: parse(plist).ProgramArguments, non_program_configuration_unchanged: true, source_sha256: sourceHashes },
      before: baseline, after, cli: { ...(launcher ? { path: launcher, sha256: fileDigest(launcher) } : {}), ...cli }, refreshed, rollback_policy: rollbackPolicy };
  } catch (error) {
    const rollbackErrors = [];
    if (stopped) {
      for (const [argv, allowed] of [
        ...(launcher ? [[['/bin/cp', '-p', originalLauncher, launcher], [0]]] : []),
        [['/bin/launchctl', 'bootout', domain, plist], [0, 3, 5, 113]],
        [['/bin/cp', '-p', original, plist], [0]],
        [['/bin/launchctl', 'bootstrap', domain, plist], [0]],
      ]) { try { run(argv, { allowed }); } catch (failure) { rollbackErrors.push(redact(failure)); } }
      try {
        await ready(endpoint, service, 'python', label); assert.deepEqual(parse(plist).ProgramArguments, expectedProgram);
        if (launcher) assert.equal(fileDigest(launcher), launcherDigest); assert(inspect(label).output.includes(`program = ${expectedProgram[0]}`));
        await collect({ token, env, samples: 0 });
      } catch (failure) { rollbackErrors.push(redact(failure)); }
      for (const owner of refreshAttempts) {
        try {
          await ready(owner.endpoint, owner.service, 'bun', owner.label);
          await owner.collect({ token });
        } catch (failure) { rollbackErrors.push(`${owner.label}: ${redact(failure)}`); }
      }
    }
    return { status: 'promotion_failed', error: redact(error), backup, rollback_verified: stopped && !rollbackErrors.length,
      rollback_errors: rollbackErrors, rollback_policy: rollbackPolicy };
  }
}
