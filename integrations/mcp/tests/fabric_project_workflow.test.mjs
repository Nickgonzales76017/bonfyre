// Real HTTP, Git worktree, Bun test command and Project patch owner. Agent output
// is deterministic injected fixture behavior, not a model-quality measurement.
import { createHash } from 'node:crypto';
import { test, expect } from 'bun:test';
import { mkdtempSync, mkdirSync, writeFileSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createFabricServer } from '../fabric_server.mjs';
import { createProjectNode } from '../project_node.mjs';
import { createFabricStore } from '../fabric_store.mjs';
import { projectRun } from '../project_runtime.mjs';

for(const mode of ['base','source','source-refused']) test(`one HTTP relay preserves Project authority with ${mode}`, async () => {
  const withSources = mode !== 'base';
  const scratch = mkdtempSync(join(tmpdir(), 'bonfyre-relay-http-'));
  const root = join(scratch, 'repo'), home = join(scratch, 'home'), fabricHome = join(home, 'fabric');
  mkdirSync(join(root, 'scripts'), { recursive: true }); mkdirSync(home);
  const git = async args => {
    const result = await projectRun(['git', ...args], { cwd: root, timeoutMs: 10000 });
    if (result.exit_code) throw new Error('fixture git setup failed');
  };
  await git(['init', '-q']);
  writeFileSync(join(root, 'scripts/value.mjs'), 'export const value = 1;\n');
  writeFileSync(join(root, 'scripts/value.test.mjs'), 'import {test,expect} from "bun:test"; import {value} from "./value.mjs"; test("requested behavior",()=>expect(value).toBe(2));\n');
  await git(['add', '--all']);
  await git(['-c', 'core.hooksPath=/dev/null', '-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid', 'commit', '-qm', 'fixture']);
  const expected = withSources ? 11 : 2;
  let source_files;
  if(withSources){
    writeFileSync(join(root,'scripts/value.mjs'),'export const value = 10;\n');
    writeFileSync(join(root,'scripts/context.mjs'),'export const context = 23;\n');
    writeFileSync(join(root,'scripts/value.test.mjs'),'import {test,expect} from "bun:test"; import {value} from "./value.mjs"; import {context} from "./context.mjs"; test("current requested behavior",()=>{expect(value).toBe(11);expect(context).toBe(23)});\n');
    source_files=['scripts/value.mjs','scripts/context.mjs','scripts/value.test.mjs'].map(path=>({path,sha256:createHash('sha256').update(readFileSync(join(root,path))).digest('hex')}));
    source_files.push({path:'scripts/absent.mjs',sha256:null});
  }
  if(mode==='source'){
    mkdirSync(join(home,'operator'));writeFileSync(join(home,'operator/policy.json'),JSON.stringify({require_clean_touched_paths:false,allow_dirty_compare_and_swap:true}));
  }
  const indexBefore = readFileSync(join(root,'.git/index'));
  const token = 'local-workflow-fixture';
  const projectEnv = { ...process.env, BONFYRE_MCP_PROJECT_WRITE_ENABLED: '1',
    BONFYRE_OPERATOR_HOME: join(home, 'operator'), BONFYRE_MCP_AUTONOMOUS_WRITE_ENABLED: '1' };
  const project = await createProjectNode({ root, home, token, env: projectEnv });
  const projectServer = Bun.serve({ hostname: '127.0.0.1', port: 0, fetch: project.handle });
  const store = createFabricStore({ home: fabricHome, projectRoot: root });
  const agents = [];
  let releasePlan;
  const planGate = new Promise(resolve => { releasePlan = resolve; });
  const node = await createFabricServer({ token, home: fabricHome, projectRoot: root, store,
    projectUrl: `${projectServer.url}mcp`, mcpUrl: 'http://127.0.0.1:1/mcp',
    policy: { max_parallel_tasks: 1, max_peer_depth: 1, test_commands: ['bun test scripts/value.test.mjs'],
      agents: { claude: { command: 'claude', read_permission_mode: 'plan' }, codex: { command: 'codex', read_sandbox: 'read-only', write_sandbox: 'workspace-write' } } },
    estate: { status: () => ({}) }, env: process.env, stopPaths: [join(scratch, 'STOP')],
    agentRunner: async ({ argv, cwd, stdoutPath, finalPath }) => {
      const prompt = argv[0] === 'claude' ? argv[argv.indexOf('-p') + 1] : argv.at(-1);
      agents.push({ agent: argv[0], prompt });
      if (prompt.startsWith('Plan this change')) {
        if(withSources)expect(readFileSync(join(cwd,'scripts/context.mjs'),'utf8')).toBe('export const context = 23;\n');
        await planGate;
      }
      if (prompt.startsWith('Implement the mission')) writeFileSync(join(cwd, 'scripts/value.mjs'), `export const value = ${expected};\n`);
      const final = prompt.includes('Return ONLY JSON')
        ? JSON.stringify({ state: 'resolved', evidence: ['fixture requested behavior and test'], blocking_findings: [] }) : 'fixture change';
      writeFileSync(stdoutPath, ''); writeFileSync(finalPath, final);
      return { exit_code: 0, duration_ms: 1 };
    } });
  const server = Bun.serve({ hostname: '127.0.0.1', port: 0, fetch: node.handle });
  projectEnv.BONFYRE_FABRIC_URL = String(server.url).replace(/\/$/, '');
  let id = 0;
  const call = async (name, args) => {
    const res = await fetch(`${projectServer.url}mcp`, { method: 'POST',
      headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' },
      body: JSON.stringify({ jsonrpc: '2.0', id: ++id, method: 'tools/call', params: { name, arguments: args } }) });
    const data = await res.json(); expect(data.result.isError).toBe(false); return data.result.structuredContent;
  };
  try {
    const submitted = await call('implementation_relay', { prompt: `Change the fixture value to ${expected} and verify it`, apply_to_project: true, ...(source_files?{source_files}:{}) });
    expect(submitted.state).toBe('queued');
    const inFlight = await call('task_status', { task_id: submitted.task_id, detail: 'summary' });
    expect(inFlight.execution.owned_by_this_process).toBe(true);
    releasePlan(); await node.drain();
    const summary = await call('task_status', { task_id: submitted.task_id, detail: 'summary' });
    if(mode==='source-refused'){
      expect(summary.state).toBe('review_required');const stopped=store.taskStatus(submitted.task_id).result;
      expect(stopped.stopped_at).toBe('apply_preflight');expect(stopped.apply_result.preview.policy_errors).toContain('patch touches paths already dirty before operator work');
      expect(agents).toHaveLength(2);expect(readFileSync(join(root,'scripts/value.mjs'),'utf8')).toBe('export const value = 10;\n');expect(readFileSync(join(root,'.git/index'))).toEqual(indexBefore);return;
    }
    expect(summary.state).toBe('completed');
    expect(summary.run_states.passed).toBe(5);
    expect(summary.execution.reconciliation_required).toBe(false);
    const full = await call('task_status', { task_id: submitted.task_id, detail: 'full' });
    expect(full.result.tests[0].exit_code).toBe(0);
    expect(full.result.tests[0].command).toBe('bun test scripts/value.test.mjs');
    expect(full.result.apply_result.applied).toBe(true);
    expect(full.result.apply_result.receipt_id).toMatch(/^patch-/);
    expect(readFileSync(join(root, 'scripts/value.mjs'), 'utf8')).toBe(`export const value = ${expected};\n`);
    expect(readFileSync(join(root,'.git/index'))).toEqual(indexBefore);
    if(withSources){
      expect(full.result.source_snapshot.files).toHaveLength(4);
      expect(full.result.diff.names).toBe('M\tscripts/value.mjs\n');
      expect(readFileSync(full.result.patch_receipt.path,'utf8')).toContain('-export const value = 10;');
      expect(readFileSync(full.result.patch_receipt.path,'utf8')).not.toContain('context.mjs');
    }
    expect(agents).toHaveLength(5);
    expect(JSON.stringify(summary).length).toBeLessThan(JSON.stringify(full).length / 2);
    expect(store.summary().locks).toEqual([]);
    const witness=withSources?process.env.BONFYRE_RELAY_SOURCE_WITNESS:process.env.BONFYRE_RELAY_WORKFLOW_WITNESS;
    if (witness) writeFileSync(witness, JSON.stringify({
      schema: 'bonfyre.relay-workflow-http-fixture.v1', measured_at: new Date().toISOString(),
      transport: 'real authenticated loopback HTTP', execution: 'real Git worktree, Bun test and Project patch owner',
      route: 'Project MCP -> Fabric owner -> Project patch owner',
      agent_behavior: 'deterministic injected responses; no model requests', task_submissions: 1, status_requests: id - 1,
      source_snapshot:full.result.source_snapshot??null, project_index_preserved:true, agent_runs: agents.length, test_command: full.result.tests[0].command, test_exit_code: full.result.tests[0].exit_code,
      task_state: summary.state, applied: full.result.apply_result.applied,
      summary_json_bytes: Buffer.byteLength(JSON.stringify(summary)), full_json_bytes: Buffer.byteLength(JSON.stringify(full)),
      verified_file: { path: 'scripts/value.mjs', contents: readFileSync(join(root, 'scripts/value.mjs'), 'utf8') },
      limitations: 'Temporary fixture is removed after the test. This does not prove paid-agent behavior or permission to resume stopped production work.'
    }, null, 2) + '\n');
  } finally {
    releasePlan(); server.stop(true); await node.close(); projectServer.stop(true);
    rmSync(scratch, { recursive: true, force: true });
  }
}, 20000);
