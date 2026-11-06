#!/usr/bin/env bun
import assert from 'node:assert/strict';
import { realpathSync } from 'node:fs';
import { homedir } from 'node:os';
import { join } from 'node:path';
import { promoteOwner } from './service_cutover.mjs';
import { collectFabricWitness, snapshotFabricState } from '../../integrations/mcp/tests/fabric_live_witness.mjs';
import { collectRecursiveWitness } from '../../integrations/mcp/tests/recursive_live_witness.mjs';
import { collectCanaryWitness } from '../../integrations/mcp/tests/canary_live_witness.mjs';

const root = realpathSync(new URL('../..', import.meta.url).pathname), home = join(homedir(), '.bonfyre/dev-fabric');
const sources = ['fabric_server.mjs', 'fabric_store.mjs', 'fabric_schema.sql', 'fabric_work.mjs', 'fabric_estate.mjs', 'fabric_catalog.json',
  'owned_mcp_client.mjs', 'http_boundary.mjs', 'canonical_json.mjs', 'project_runtime.mjs',
  'recursive_server.mjs', 'recursive_store.mjs', 'recursive_schema.sql', 'recursive_catalog.json',
  'canary_server.mjs', 'canary_store.mjs', 'canary_catalog.json', 'canary_fixtures.mjs', 'canary_reviewed_scripts.json', 'project_node.mjs',
  'tests/fabric_live_witness.mjs', 'tests/recursive_live_witness.mjs', 'tests/canary_live_witness.mjs', 'tests/project_live_witness.mjs']
  .map(name => join(root, 'integrations/mcp', name));

const proof = await promoteOwner({ root, label: 'com.bonfyre.dual-agent-fabric',
  expectedProgram: ['/opt/homebrew/bin/python3.12', join(home, 'broker_server.py')],
  entry: join(root, 'integrations/mcp/fabric_server.mjs'), databases: [join(home, 'fabric.sqlite3')],
  sources: [...sources, join(root, 'deploy/launchd/service_cutover.mjs'), join(root, 'deploy/launchd/fabric_cutover.mjs')],
  endpoint: 'http://127.0.0.1:3053', service: 'bonfyre-dual-agent-fabric',
  collect: ({ token, env, samples }) => {
    assert.equal(env.BONFYRE_FABRIC_HOME, home); assert.equal(env.BONFYRE_FABRIC_HOST, '127.0.0.1'); assert.equal(env.BONFYRE_FABRIC_PORT, '3053');
    return collectFabricWitness({ token, home, samples });
  },
  preStop: ({ run, pid, baseline }) => {
    const rows = run(['/bin/ps', '-axo', 'pid,ppid']).trim().split('\n').slice(1).map(row => row.trim().split(/\s+/).map(Number));
    assert(rows.some(([id]) => id === pid)); assert(!rows.some(([, parent]) => parent === pid), 'broker has active descendant processes');
    const born = Date.parse(run(['/bin/ps', '-p', String(pid), '-o', 'lstart=']).trim()); assert(Number.isFinite(born));
    const bootSeconds = Number(run(['/usr/sbin/sysctl', '-n', 'kern.boottime']).match(/sec\s*=\s*(\d+)/)?.[1]);
    assert(Number.isSafeInteger(bootSeconds) && bootSeconds > 0 && bootSeconds * 1000 <= born);
    assert.equal(baseline.snapshot.recorded_nonterminal_tasks.filter(row => row.state === 'queued').length, 0, 'queued broker work requires reconciliation');
    for (const row of [...baseline.snapshot.recorded_nonterminal_tasks, ...baseline.snapshot.recorded_nonterminal_runs]) {
      assert(row.started_at && row.started_at < bootSeconds, 'recorded work may have survived this boot; explicit reconciliation required');
    }
  },
  postStop: ({ baseline }) => {
    const stopped = snapshotFabricState(home);
    assert.deepEqual(stopped.tables, baseline.snapshot.tables,
      'broker accepted work while stopping; outcome uncertain, reconcile before any retry');
    assert.deepEqual(stopped.receipts, baseline.snapshot.receipts, 'receipt files changed while stopping');
  },
  continuity: (before, after) => {
    assert.equal(after.runtime, 'bun');
    for (const key of ['catalog_sha256', 'receipt_metadata_sha256', 'task_detail_sha256', 'events_sha256']) assert.deepEqual(after[key], before[key], `${key} changed`);
    for (const key of ['tables', 'receipts', 'task_states', 'run_states', 'recorded_nonterminal_tasks', 'recorded_nonterminal_runs']) assert.deepEqual(after.snapshot[key], before.snapshot[key], `snapshot ${key} changed`);
    assert.equal(after.snapshot.receipts.verified, true);
  },
  refresh: [
    { label: 'com.bonfyre.recursive-substrate-v5', endpoint: 'http://127.0.0.1:3055', service: 'bonfyre-recursive-substrate-v5', collect: collectRecursiveWitness },
    { label: 'com.bonfyre.fixture-canary-r4', endpoint: 'http://127.0.0.1:3056', service: 'bonfyre-fixture-canary-r4', collect: collectCanaryWitness },
    { label: 'com.bonfyre.project-mcp', endpoint: 'http://127.0.0.1:3051', service: 'bonfyre-project-mcp', collect: () => {
      const child = Bun.spawnSync([process.execPath, join(root, 'integrations/mcp/tests/project_live_witness.mjs')], { cwd: root, stdout: 'pipe', stderr: 'pipe', timeout: 30_000 });
      assert.equal(child.exitCode, 0, 'Project MCP read-only witness failed'); return JSON.parse(child.stdout.toString());
    } },
  ], apply: process.argv.includes('--apply') });
console.log(JSON.stringify(proof, null, 2));
if (proof.status === 'promotion_failed') process.exitCode = 1;
