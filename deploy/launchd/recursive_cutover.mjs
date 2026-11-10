#!/usr/bin/env bun
import assert from 'node:assert/strict';
import { realpathSync } from 'node:fs';
import { homedir } from 'node:os';
import { join } from 'node:path';
import { promoteOwner } from './service_cutover.mjs';
import { collectRecursiveWitness } from '../../integrations/mcp/tests/recursive_live_witness.mjs';
import { collectCanaryWitness } from '../../integrations/mcp/tests/canary_live_witness.mjs';

const root = realpathSync(new URL('../..', import.meta.url).pathname), home = join(homedir(), '.bonfyre/recursive-substrate-v5');
const sources = ['http_boundary.mjs', 'canonical_json.mjs', 'recursive_server.mjs', 'recursive_store.mjs', 'recursive_schema.sql',
  'recursive_catalog.json', 'recursive_cli.mjs', 'canary_server.mjs', 'canary_store.mjs', 'project_node.mjs',
  'tests/recursive_live_witness.mjs', 'tests/canary_live_witness.mjs', 'tests/project_live_witness.mjs'].map(name => join(root, 'integrations/mcp', name));
const proof = await promoteOwner({ root, label: 'com.bonfyre.recursive-substrate-v5',
  expectedProgram: ['/opt/homebrew/bin/python3.12', join(home, 'recursive_substrate_server.py')],
  entry: join(root, 'integrations/mcp/recursive_server.mjs'), launcher: join(homedir(), '.local/bin/bonfyre-recursive'),
  launcherDigest: '59f5387c9e368d9609fcc66810563e38da88d8684cf2b3aa53ab7e5db47fbcb9',
  launcherSource: join(root, 'deploy/launchd/bonfyre-recursive'), databases: [join(home, 'substrate.sqlite3')],
  sources: [...sources, join(root, 'deploy/launchd/service_cutover.mjs'), join(root, 'deploy/launchd/recursive_cutover.mjs')],
  endpoint: 'http://127.0.0.1:3055', service: 'bonfyre-recursive-substrate-v5',
  cliEnvironment: { BONFYRE_RECURSIVE_URL: 'http://127.0.0.1:3055' },
  collect: ({ token, env, samples }) => {
    assert.equal(env.BONFYRE_RECURSIVE_HOME, home); assert.equal(env.BONFYRE_RECURSIVE_HOST, '127.0.0.1'); assert.equal(env.BONFYRE_RECURSIVE_PORT, '3055');
    return collectRecursiveWitness({ token, samples });
  },
  continuity: (before, after) => {
    assert.equal(after.runtime, 'bun');
    for (const key of ['catalog_sha256', 'counts', 'tokenizers', 'chains', 'token_binary']) assert.deepEqual(after[key], before[key], `${key} changed during promotion`);
    assert.equal(after.token_binary_status.exit_code, 0);
  },
  verifyCli: ({ run, launcher, after }) => {
    assert.equal(run([launcher, '--version']).trim(), `bonfyre-recursive ${after.version}`);
    assert.deepEqual(JSON.parse(run([launcher, 'replay'])), after.chains);
    return { runtime: 'bun', replay_verified: true };
  },
  refresh: [
    { label: 'com.bonfyre.fixture-canary-r4', endpoint: 'http://127.0.0.1:3056', service: 'bonfyre-fixture-canary-r4', collect: collectCanaryWitness },
    { label: 'com.bonfyre.project-mcp', endpoint: 'http://127.0.0.1:3051', service: 'bonfyre-project-mcp', collect: () => {
      const child = Bun.spawnSync([process.execPath, join(root, 'integrations/mcp/tests/project_live_witness.mjs')], { cwd: root, stdout: 'pipe', stderr: 'pipe', timeout: 30_000 });
      assert.equal(child.exitCode, 0, 'Project MCP read-only witness failed');
      return JSON.parse(child.stdout.toString());
    } },
  ], apply: process.argv.includes('--apply') });
console.log(JSON.stringify(proof, null, 2));
if (proof.status === 'promotion_failed') process.exitCode = 1;
