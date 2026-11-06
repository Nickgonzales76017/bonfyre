#!/usr/bin/env bun
import assert from 'node:assert/strict';
import { realpathSync } from 'node:fs';
import { homedir } from 'node:os';
import { join } from 'node:path';
import { promoteOwner, fileDigest } from './service_cutover.mjs';
import { collectSpinesWitness, snapshotSpinesState } from '../../integrations/mcp/tests/spines_live_witness.mjs';
const root = realpathSync(new URL('../..', import.meta.url).pathname), home = join(homedir(), '.bonfyre/agent-hypervisor/native-spines-r7');
const binaryNames = ['bonfyre-delta-kernel', 'bonfyre-helix-vm', 'bonfyre-kernel-probe'];
const nativeHashes = Object.fromEntries(binaryNames.map(name => { const path = join(homedir(), '.local/bin', name); return [path, fileDigest(path)]; }));
const inputs = ['spines_store.mjs', 'spines_schema.sql', 'spines_server.mjs', 'spines_catalog.json', 'spines_temporal.mjs',
  'spines_reasoning.mjs', 'spines_model.mjs', 'spines_cli.mjs', 'canonical_json.mjs', 'project_runtime.mjs', 'owned_mcp_client.mjs',
  'http_boundary.mjs', 'spines_wasm.mjs', 'spines_wasm_core.mjs', 'native/spines_helix.wasm', 'native/spines_helix.lock.json',
  'native/spines_helix_export.c', 'native/spines_helix_vm.c', 'native/spines_sha256_compat.h',
  'native/build_spines_wasm.mjs', 'tests/spines_live_witness.mjs'].map(path => join(root, 'integrations/mcp', path));
const continuity = (before, after) => {
  assert.equal(after.runtime, 'bun');
  for (const key of ['catalog_sha256', 'read_models_sha256', 'counts', 'authority', 'native_binaries', 'receipt_chain']) assert.deepEqual(after[key], before[key], `${key} changed`);
  for (const key of ['tables', 'files', 'receipt_chain']) assert.deepEqual(after.snapshot[key], before.snapshot[key], `snapshot ${key} changed`);
};
const proof = await promoteOwner({ root, label: 'com.bonfyre.native-spines-r7',
  expectedProgram: ['/opt/homebrew/bin/python3.12', join(home, 'package/spines_server.py')],
  entry: join(root, 'integrations/mcp/spines_server.mjs'), databases: [join(home, 'spines.sqlite3')],
  sources: [...inputs, join(root, 'deploy/launchd/service_cutover.mjs'), join(root, 'deploy/launchd/spines_cutover.mjs')],
  endpoint: 'http://127.0.0.1:3057', service: 'bonfyre-native-spines-r7',
  launcher: join(homedir(), '.local/bin/bonfyre-spines'), launcherDigest: '2cf4369266df41bd6a2778fa1577fc13248556829d34a9ce00c993322043eec1',
  launcherSource: join(root, 'deploy/launchd/bonfyre-spines'),
  collect: ({ token, env, samples }) => {
    assert.equal(env.BONFYRE_SPINES_HOME, home); assert.equal(env.BONFYRE_SPINES_HOST, '127.0.0.1'); assert.equal(env.BONFYRE_SPINES_PORT, '3057');
    return collectSpinesWitness({ token, home, samples });
  },
  preStop: ({ run, pid }) => {
    const rows = run(['/bin/ps', '-axo', 'pid,ppid']).trim().split('\n').slice(1).map(line => line.trim().split(/\s+/).map(Number));
    assert(rows.some(([id]) => id === pid)); assert(!rows.some(([, parent]) => parent === pid), 'Spines has active child execution');
    for (const [path, digest] of Object.entries(nativeHashes)) assert.equal(fileDigest(path), digest);
  },
  postStop: ({ baseline }) => {
    const stopped = snapshotSpinesState(home);
    for (const key of ['tables', 'files', 'receipt_chain']) assert.deepEqual(stopped[key], baseline.snapshot[key], 'Spines state changed during shutdown; reconcile before retry');
  }, continuity,
  verifyCli: ({ run, launcher, after }) => {
    assert.equal(run([launcher, '--version']).trim(), 'bonfyre-spines 5.7.0-native-spines-r7');
    const status = JSON.parse(run([launcher, 'status'])); assert.deepEqual(status.counts, after.counts); assert.deepEqual(status.receipt_chain, after.receipt_chain);
    for (const [path, digest] of Object.entries(nativeHashes)) assert.equal(fileDigest(path), digest);
    return { installed: true, version_verified: true, status_verified: true, native_binary_sha256: nativeHashes };
  }, apply: process.argv.includes('--apply') });
console.log(JSON.stringify(proof, null, 2)); if (proof.status === 'promotion_failed') process.exitCode = 1;
