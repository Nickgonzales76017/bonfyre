#!/usr/bin/env bun
// Exact local fixture/canary owner promotion. No fixtures or routing mutations.
import assert from 'node:assert/strict';
import { readFileSync, mkdtempSync, realpathSync, existsSync, lstatSync } from 'node:fs';
import { homedir } from 'node:os';
import { join } from 'node:path';
import { createHash } from 'node:crypto';
import { collectCanaryWitness } from '../../integrations/mcp/tests/canary_live_witness.mjs';

const root = realpathSync(new URL('../..', import.meta.url).pathname), binary = realpathSync(process.execPath);
assert.equal(binary.split('/').at(-1), 'bun');
const label = 'com.bonfyre.fixture-canary-r4', domain = `gui/${process.getuid()}`;
const plist = join(homedir(), 'Library/LaunchAgents', `${label}.plist`);
const entry = join(root, 'integrations/mcp/canary_server.mjs');
const launcher = join(homedir(), '.local/bin/bonfyre-canary');
assert(lstatSync(launcher).isFile() && !lstatSync(launcher).isSymbolicLink(), 'canary launcher must be a regular file');
const launcherBytes = readFileSync(launcher);
assert(launcherBytes.toString().includes('/canary-r4/package/r4_cli.py'), 'unexpected canary launcher owner');
const sha = bytes => createHash('sha256').update(bytes).digest('hex');
function run(argv, allowed = [0]) {
  const child = Bun.spawnSync(argv, { cwd: root, stdout: 'pipe', stderr: 'pipe', timeout: 30_000 });
  assert(allowed.includes(child.exitCode), `${argv[0]} failed (${child.exitCode})`);
  return child.stdout.toString();
}
const parse = path => JSON.parse(run(['/usr/bin/plutil', '-convert', 'json', '-o', '-', path]));
const before = parse(plist), bytes = readFileSync(plist), env = { ...process.env, ...before.EnvironmentVariables };
assert.equal(before.Label, label);
assert.deepEqual(before.ProgramArguments, ['/opt/homebrew/bin/python3.12', join(homedir(), '.bonfyre/agent-hypervisor/canary-r4/package/r4_server.py')]);
const home = env.BONFYRE_CANARY_HOME || join(homedir(), '.bonfyre/agent-hypervisor/canary-r4');
const token = readFileSync(env.BONFYRE_MCP_TOKEN_FILE || join(homedir(), '.bonfyre/mcp/token'), 'utf8').trim();
const port = Number(env.BONFYRE_CANARY_PORT || 3056), host = env.BONFYRE_CANARY_HOST || '127.0.0.1';
assert.equal(host, '127.0.0.1'); assert.equal(port, 3056);
const inspect = name => {
  const output = run(['/bin/launchctl', 'print', `${domain}/${name}`]);
  const pid = Number(output.match(/^\s*pid = (\d+)$/m)?.[1]);
  assert(pid > 1 && /state = running/.test(output));
  return { pid, output };
};
const quiet = pid => assert(!run(['/usr/sbin/lsof', '-nP', '-a', '-p', String(pid), '-iTCP'], [0, 1]).includes('(ESTABLISHED)'), 'service has active connections');
const alive = pid => {
  try { process.kill(pid, 0); return true; }
  catch (error) { if (error.code === 'ESRCH') return false; throw error; }
};
async function ready(runtime) {
  const deadline = Date.now() + 20_000;
  while (Date.now() < deadline) {
    try {
      const response = await fetch(`http://${host}:${port}/health`, { redirect: 'error', headers: { connection: 'close' }, signal: AbortSignal.timeout(1000) });
      const health = await response.json();
      if (response.ok && health.service === 'bonfyre-fixture-canary-r4' && (health.runtime || 'python') === runtime) return;
    } catch { /* Read-only startup probe; never retry an effect. */ }
    await Bun.sleep(200);
  }
  throw new Error(`canary ${runtime} did not become healthy within twenty seconds`);
}
const previous = inspect(label);
quiet(previous.pid);
const sourceNames = ['canary_server.mjs', 'canary_store.mjs', 'canary_fixtures.mjs', 'canary_cli.mjs', 'canary_catalog.json', 'canary_reviewed_scripts.json', 'http_boundary.mjs', 'project_node.mjs'];
const sources = Object.fromEntries(sourceNames.map(name => [name, sha(readFileSync(join(root, 'integrations/mcp', name)))]));
const rollbackPolicy = 'Configuration/runtime rollback only: restore the original daemon and CLI, retain the live database and certificate/receipt files. Additive indexes and version metadata are backward compatible; the backup is for explicit reconciliation, never automatic overwrite of concurrent accepted writes.';
if (!process.argv.includes('--apply')) {
  console.log(JSON.stringify({ status: 'ready', pid: previous.pid, source_sha256: sources, home, root: realpathSync(env.BONFYRE_ROOT), rollback_policy: rollbackPolicy }));
} else {
  const baseline = await collectCanaryWitness({ token, samples: 25 });
  const backup = mkdtempSync('/private/tmp/bonfyre-canary-cutover.');
  const original = join(backup, 'original.plist'), candidate = join(backup, 'candidate.plist');
  const originalLauncher = join(backup, 'bonfyre-canary.original');
  run(['/bin/cp', '-p', launcher, originalLauncher]);
  run(['/bin/cp', '-p', plist, original]); run(['/bin/cp', '-p', plist, candidate]);
  run(['/usr/libexec/PlistBuddy', '-c', `Set :ProgramArguments:0 ${binary}`, candidate]);
  run(['/usr/libexec/PlistBuddy', '-c', `Set :ProgramArguments:1 ${entry}`, candidate]);
  run(['/usr/bin/plutil', '-lint', candidate]);
  const nonProgram = value => Object.fromEntries(Object.entries(value).filter(([key]) => key !== 'ProgramArguments'));
  assert.deepEqual(nonProgram(before), nonProgram(parse(candidate)));
  assert.equal(sha(readFileSync(plist)), sha(bytes));
  assert.equal(sha(readFileSync(launcher)), sha(launcherBytes));
  for (const [name, digest] of Object.entries(sources)) assert.equal(sha(readFileSync(join(root, 'integrations/mcp', name))), digest, `source changed: ${name}`);
  const projectLabel = 'com.bonfyre.project-mcp', project = inspect(projectLabel);
  assert(project.output.includes(`program = ${binary}`)); quiet(project.pid);
  quiet(previous.pid);
  let stopped = false;
  try {
    run(['/bin/launchctl', 'bootout', domain, plist]); stopped = true;
    for (let attempt = 0; attempt < 50 && alive(previous.pid); attempt++) await Bun.sleep(100);
    assert(!alive(previous.pid), 'old Python canary must exit before the database backup');
    const dbPath = join(home, 'canary.sqlite3');
    assert(!existsSync(`${dbPath}-wal`), 'stopped service still has a WAL; require online backup reconciliation');
    run(['/bin/cp', '-p', dbPath, join(backup, 'canary.sqlite3')]);
    run(['/bin/cp', '-p', candidate, plist]);
    run(['/bin/launchctl', 'bootstrap', domain, plist]);
    await ready('bun');
    const current = inspect(label);
    assert(current.output.includes(`program = ${binary}`));
    assert(!alive(previous.pid), 'old Python canary is still alive');
    assert.deepEqual(nonProgram(before), nonProgram(parse(plist)));
    const after = await collectCanaryWitness({ token, samples: 25 });
    assert.equal(after.runtime, 'bun');
    assert.equal(after.catalog_sha256, baseline.catalog_sha256);
    assert.deepEqual(after.receipt_chain, baseline.receipt_chain);
    run(['/usr/bin/install', '-m', '755', join(root, 'deploy/launchd/bonfyre-canary'), launcher]);
    assert.equal(run([launcher, '--version']).trim(), `bonfyre-canary ${after.version}`);
    assert.deepEqual(JSON.parse(run([launcher, 'verify'])), baseline.receipt_chain);
    // Activate the same extracted wire boundary in the already-Bun project owner.
    assert.equal(inspect(projectLabel).pid, project.pid, 'Project owner changed during canary promotion'); quiet(project.pid);
    run(['/bin/launchctl', 'kickstart', '-k', `${domain}/${projectLabel}`]);
    await Bun.sleep(750);
    const projectWitness = JSON.parse(run([binary, join(root, 'integrations/mcp/tests/project_live_witness.mjs')]));
    console.log(JSON.stringify({ schema: 'bonfyre.canary.cutover.v1', measured_at: new Date().toISOString(), service: `${domain}/${label}`,
      previous: { pid: previous.pid, exited: true, program: before.ProgramArguments, rollback_plist: original, rollback_launcher: originalLauncher, database_backup: join(backup, 'canary.sqlite3') },
      installed: { pid: current.pid, program: parse(plist).ProgramArguments, non_program_configuration_unchanged: true, source_sha256: sources },
      cli: { path: launcher, sha256: sha(readFileSync(launcher)), runtime: 'bun', receipt_chain_verified: true },
      rollback_policy: rollbackPolicy,
      before: baseline, after, project_mcp_refresh: { pid: inspect(projectLabel).pid, tool_count: projectWitness.tool_count, runtime: projectWitness.runtime },
      limitation: 'Serial warm HTTP receipt verification is not fixture/provider performance. No paid agents, fixture corrections, policy promotion, routing decisions or recursive ingestion ran during this cutover.' }, null, 2));
  } catch (error) {
    const rollbackErrors = [];
    if (stopped) {
      for (const [argv, allowed] of [
        [['/bin/cp', '-p', originalLauncher, launcher], [0]],
        [['/bin/launchctl', 'bootout', domain, plist], [0, 3, 5, 113]],
        [['/bin/cp', '-p', original, plist], [0]],
        [['/bin/launchctl', 'bootstrap', domain, plist], [0]],
      ]) { try { run(argv, allowed); } catch (failure) { rollbackErrors.push(failure.message); } }
      try {
        await ready('python');
        assert.deepEqual(parse(plist).ProgramArguments, before.ProgramArguments);
        assert.equal(sha(readFileSync(launcher)), sha(launcherBytes));
        assert(inspect(label).output.includes(`program = ${before.ProgramArguments[0]}`));
        await collectCanaryWitness({ token });
      } catch (failure) { rollbackErrors.push(failure.message); }
    }
    console.error(JSON.stringify({ status: 'promotion_failed', rollback_verified: stopped && !rollbackErrors.length,
      backup, error: String(error.message).replaceAll(token, '[redacted]'), rollback_errors: rollbackErrors.map(message => String(message).replaceAll(token, '[redacted]')), rollback_policy: rollbackPolicy }));
    process.exitCode = 1;
  }
}
