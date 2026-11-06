#!/usr/bin/env bun
// Scoped service promotion. Never rewrites environment, gates, state or logs.
import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, realpathSync } from 'node:fs';
import { homedir } from 'node:os';
import { join } from 'node:path';
import { createHash } from 'node:crypto';

const root = realpathSync(new URL('../..', import.meta.url).pathname);
const label = 'com.bonfyre.project-mcp', domain = `gui/${process.getuid()}`;
const plist = join(homedir(), 'Library/LaunchAgents', `${label}.plist`);
const entry = join(root, 'integrations/mcp/project_node.mjs');
const witness = join(root, 'integrations/mcp/tests/project_live_witness.mjs');
const binary = realpathSync(process.execPath);
assert.equal(binary.split('/').at(-1), 'bun');
const sha = bytes => createHash('sha256').update(bytes).digest('hex');
function run(argv, allowed = [0]) {
  const result = Bun.spawnSync(argv, { cwd: root, stdout: 'pipe', stderr: 'pipe', timeout: 30_000 });
  // Deliberately do not print command output: launchctl can include credentials.
  assert(allowed.includes(result.exitCode), `${argv[0]} failed (${result.exitCode})`);
  return result.stdout.toString();
}
const parse = path => JSON.parse(run(['/usr/bin/plutil', '-convert', 'json', '-o', '-', path]));
const before = parse(plist), beforeBytes = readFileSync(plist);
assert.equal(before.Label, label);
assert.equal(realpathSync(before.EnvironmentVariables.BONFYRE_ROOT), root);
assert.equal(before.EnvironmentVariables.BONFYRE_PROJECT_MCP_HOST, '127.0.0.1');
assert.equal(before.EnvironmentVariables.BONFYRE_PROJECT_MCP_PORT, '3051');
assert.deepEqual(before.ProgramArguments, ['/usr/bin/python3', join(homedir(), '.bonfyre/mcp/project_node.py')]);
const state = run(['/bin/launchctl', 'print', `${domain}/${label}`]);
const previousPid = Number(state.match(/^\s*pid = (\d+)$/m)?.[1]);
assert(previousPid > 1 && /state = running/.test(state), 'legacy service must be running');
const sockets = run(['/usr/sbin/lsof', '-nP', '-a', '-p', String(previousPid), '-iTCP']);
assert(sockets.includes('127.0.0.1:3051 (LISTEN)'), 'legacy listener mismatch');
assert(!sockets.includes('(ESTABLISHED)'), 'active TCP connections; postpone cutover');
const sourceFiles = ['project_node.mjs', 'project_catalog.json', 'project_reads.mjs', 'project_operator.mjs', 'project_native.mjs', 'project_runtime.mjs'];
const sourceHashes = Object.fromEntries(sourceFiles.map(file => [file, sha(readFileSync(join(root, 'integrations/mcp', file)))]));
if (!process.argv.includes('--apply')) {
  console.log(JSON.stringify({ status: 'ready_for_scoped_promotion', previous_pid: previousPid, source_sha256: sourceHashes }));
} else {
  const backup = mkdtempSync('/private/tmp/bonfyre-project-mcp-cutover.');
  const original = join(backup, 'original.plist'), candidate = join(backup, 'candidate.plist');
  run(['/bin/cp', '-p', plist, original]);
  run(['/bin/cp', '-p', plist, candidate]);
  run(['/usr/libexec/PlistBuddy', '-c', `Set :ProgramArguments:0 ${binary}`, candidate]);
  run(['/usr/libexec/PlistBuddy', '-c', `Set :ProgramArguments:1 ${entry}`, candidate]);
  run(['/usr/bin/plutil', '-lint', candidate]);
  const next = parse(candidate);
  const nonProgram = value => Object.fromEntries(Object.entries(value).filter(([key]) => key !== 'ProgramArguments'));
  assert.deepEqual(nonProgram(before), nonProgram(next), 'non-program configuration changed');
  assert.equal(sha(readFileSync(plist)), sha(beforeBytes), 'configuration changed during preparation');
  let stopped = false;
  try {
    run(['/bin/launchctl', 'bootout', domain, plist]);
    stopped = true;
    run(['/bin/cp', '-p', candidate, plist]);
    run(['/bin/launchctl', 'bootstrap', domain, plist]);
    let health;
    for (let attempt = 0; attempt < 30; attempt++) {
      try {
        health = await (await fetch('http://127.0.0.1:3051/health', { signal: AbortSignal.timeout(500) })).json();
        if (health.runtime === 'bun') break;
      } catch { /* bounded startup grace */ }
      await Bun.sleep(250);
    }
    assert.equal(health?.runtime, 'bun', 'new service did not become healthy');
    const proof = JSON.parse(run([binary, witness]));
    const installed = run(['/bin/launchctl', 'print', `${domain}/${label}`]);
    const pid = Number(installed.match(/^\s*pid = (\d+)$/m)?.[1]);
    assert(pid > 1 && pid !== previousPid && /state = running/.test(installed));
    assert(installed.includes(`program = ${binary}`), 'launchd program mismatch');
    let oldAlive = true;
    try { process.kill(previousPid, 0); } catch (error) { if (error.code === 'ESRCH') oldAlive = false; else throw error; }
    assert(!oldAlive, 'old Python process still alive');
    assert.deepEqual(nonProgram(before), nonProgram(parse(plist)));
    console.log(JSON.stringify({ schema: 'bonfyre.project-mcp.cutover.v1', measured_at: new Date().toISOString(),
      service: `${domain}/${label}`, previous: { pid: previousPid, program: before.ProgramArguments, exited: true,
        plist_sha256: sha(beforeBytes), rollback_plist: original },
      installed: { pid, program: next.ProgramArguments, plist_sha256: sha(readFileSync(plist)),
        non_program_configuration_unchanged: true, source_sha256: sourceHashes }, http_witness: proof,
      limitation: 'Historical local service cutover; no real generation/projection, remote Matrix or full Python retirement claim.' }, null, 2));
  } catch (error) {
    if (stopped) {
      run(['/bin/launchctl', 'bootout', domain, plist], [0, 3, 5, 113]);
      run(['/bin/cp', '-p', original, plist]);
      run(['/bin/launchctl', 'bootstrap', domain, plist]);
    }
    console.error(`Promotion failed; original plist restored from ${original}. ${error.message}`);
    process.exitCode = 1;
  }
}
