#!/usr/bin/env bun
// Promote only an already-paused operator. Preserve its STOP, policy, stores,
// model, endpoint and all launchd settings except the interpreter entrypoint.
import assert from 'node:assert/strict';
import { readFileSync, mkdtempSync, realpathSync, existsSync } from 'node:fs';
import { homedir } from 'node:os';
import { join } from 'node:path';
import { createHash } from 'node:crypto';

const root = realpathSync(new URL('../..', import.meta.url).pathname), binary = realpathSync(process.execPath);
assert.equal(binary.split('/').at(-1), 'bun');
const label = 'com.bonfyre.autonomous-power-operator', domain = `gui/${process.getuid()}`;
const plist = join(homedir(), 'Library/LaunchAgents', `${label}.plist`);
const entry = join(root, 'integrations/mcp/operator_daemon.mjs');
const sha = bytes => createHash('sha256').update(bytes).digest('hex');
function run(argv, allowed = [0], env = process.env) {
  const child = Bun.spawnSync(argv, { cwd: root, env, stdout: 'pipe', stderr: 'pipe', timeout: 30_000 });
  assert(allowed.includes(child.exitCode), `${argv[0]} failed (${child.exitCode})`);
  return child.stdout.toString();
}
const parse = path => JSON.parse(run(['/usr/bin/plutil', '-convert', 'json', '-o', '-', path]));
const before = parse(plist), bytes = readFileSync(plist), env = { ...process.env, ...before.EnvironmentVariables };
assert.equal(before.Label, label);
assert.deepEqual(before.ProgramArguments, ['/usr/bin/python3', join(homedir(), '.bonfyre/operator/operator_daemon.py')]);
const home = env.BONFYRE_OPERATOR_HOME || join(homedir(), '.bonfyre/operator');
const stopPath = join(home, 'STOP'), policyPath = env.BONFYRE_OPERATOR_POLICY || join(home, 'policy.json');
assert(existsSync(stopPath), 'operator must already be paused');
const stopHash = sha(readFileSync(stopPath)), policyHash = sha(readFileSync(policyPath));
const inspect = () => JSON.parse(run([binary, entry, '--inspect'], [0], env));
const config = inspect();
assert.equal(config.paused, true); assert.equal(config.runtime, 'bun');
const inspectService = name => {
  const output = run(['/bin/launchctl', 'print', `${domain}/${name}`]);
  const pid = Number(output.match(/^\s*pid = (\d+)$/m)?.[1]);
  assert(pid > 1 && /state = running/.test(output));
  return { pid, output };
};
const old = inspectService(label);
assert(!run(['/usr/sbin/lsof', '-nP', '-a', '-p', String(old.pid), '-iTCP'], [0, 1]).includes('(ESTABLISHED)'), 'old operator has active connections');
// Reload the existing Bun MCP producer to activate its in-progress dedupe fix.
const projectLabel = 'com.bonfyre.project-mcp', project = inspectService(projectLabel);
assert(project.output.includes(`program = ${binary}`), 'project service is not the already-promoted Bun owner');
assert(!run(['/usr/sbin/lsof', '-nP', '-a', '-p', String(project.pid), '-iTCP'], [0, 1]).includes('(ESTABLISHED)'), 'project MCP is busy');
const sourceNames = ['operator_daemon.mjs', 'operator_store.mjs', 'operator_tools.mjs', 'project_operator.mjs'];
const sourceHashes = Object.fromEntries(sourceNames.map(name => [name, sha(readFileSync(join(root, 'integrations/mcp', name)))]));
if (!process.argv.includes('--apply')) console.log(JSON.stringify({ status: 'ready_paused', old_pid: old.pid, config, source_sha256: sourceHashes }));
else {
  const backup = mkdtempSync('/private/tmp/bonfyre-operator-cutover.');
  const original = join(backup, 'original.plist'), candidate = join(backup, 'candidate.plist');
  run(['/bin/cp', '-p', plist, original]); run(['/bin/cp', '-p', plist, candidate]);
  run(['/usr/libexec/PlistBuddy', '-c', `Set :ProgramArguments:0 ${binary}`, candidate]);
  run(['/usr/libexec/PlistBuddy', '-c', `Set :ProgramArguments:1 ${entry}`, candidate]);
  run(['/usr/bin/plutil', '-lint', candidate]);
  const withoutProgram = value => Object.fromEntries(Object.entries(value).filter(([key]) => key !== 'ProgramArguments'));
  assert.deepEqual(withoutProgram(before), withoutProgram(parse(candidate)));
  assert.equal(sha(readFileSync(plist)), sha(bytes));
  assert.equal(sha(readFileSync(stopPath)), stopHash); assert.equal(sha(readFileSync(policyPath)), policyHash);
  let stopped = false;
  try {
    run(['/bin/launchctl', 'bootout', domain, plist]); stopped = true;
    run(['/bin/cp', '-p', candidate, plist]);
    run(['/bin/launchctl', 'bootstrap', domain, plist]);
    await Bun.sleep(1500);
    const installed = inspectService(label);
    assert(installed.output.includes(`program = ${binary}`));
    assert.notEqual(installed.pid, old.pid);
    let oldAlive = true;
    try { process.kill(old.pid, 0); } catch (error) { if (error.code === 'ESRCH') oldAlive = false; else throw error; }
    assert(!oldAlive, 'Python operator still alive');
    assert.equal(sha(readFileSync(stopPath)), stopHash); assert.equal(sha(readFileSync(policyPath)), policyHash);
    assert.deepEqual(withoutProgram(before), withoutProgram(parse(plist)));
    const descriptors = run(['/usr/sbin/lsof', '-p', String(installed.pid)]);
    assert(!descriptors.includes('operator.sqlite3'), 'paused operator opened the live database');
    assert(!run(['/usr/sbin/lsof', '-nP', '-a', '-p', String(installed.pid), '-iTCP'], [0, 1]).includes('(ESTABLISHED)'), 'paused operator opened a network session');
    run(['/bin/launchctl', 'kickstart', '-k', `${domain}/${projectLabel}`]);
    await Bun.sleep(750);
    const projectProof = JSON.parse(run([binary, join(root, 'integrations/mcp/tests/project_live_witness.mjs')]));
    const newProject = inspectService(projectLabel);
    const proof = { schema: 'bonfyre.operator.cutover.v1', measured_at: new Date().toISOString(),
      service: `${domain}/${label}`, previous: { pid: old.pid, program: before.ProgramArguments,
        exited: true, plist_sha256: sha(bytes), rollback_plist: original },
      installed: { pid: installed.pid, program: parse(plist).ProgramArguments, plist_sha256: sha(readFileSync(plist)),
        non_program_configuration_unchanged: true, source_sha256: sourceHashes },
      preserved: { stop_sha256: stopHash, policy_sha256: policyHash, model: config.model, gateway: config.gateway },
      paused_witness: { ...inspect(), live_database_open: false, established_network_connection: false },
      project_mcp_refresh: { previous_pid: project.pid, pid: newProject.pid, tool_count: projectProof.tool_count, runtime: projectProof.runtime },
      limitation: 'Paused production process and offline full-loop fixtures do not prove live paid model execution, global exactly-once effects, or retirement of the other Python services.' };
    console.log(JSON.stringify(proof, null, 2));
  } catch (error) {
    if (stopped) {
      run(['/bin/launchctl', 'bootout', domain, plist], [0, 3, 5, 113]);
      run(['/bin/cp', '-p', original, plist]); run(['/bin/launchctl', 'bootstrap', domain, plist]);
    }
    console.error(`Operator promotion failed; original configuration restored from ${original}. ${error.message}`);
    process.exitCode = 1;
  }
}
