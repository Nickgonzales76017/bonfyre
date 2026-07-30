import assert from 'node:assert/strict';
import { createAgentFleet, providerCatalog, reconcileProviderCatalog, toCanonicalReceipt } from './provider_fleet.mjs';

const providers = providerCatalog({ env: { BONFYRE_PROVIDER_HVM4_COMMAND: 'hvm4' }, configured: { hvm4: { command: 'hvm4', state: 'promoted' } } });
const fabric = {
  inventory: async () => [{ id: 'bonfyre-qwen-fpq', capabilities: ['generation'] }],
  profileInventory: () => [{ id: 'coder-large', model: 'external-configured-profile' }],
  generate: async (task) => ({ schema_version: 'bonfyre.native.generation.result.v1', output: `generated:${task.prompt}`, output_sha256: 'abc' }),
  invoke: async (task) => ({ status: 'passed', tool_id: task.toolId, output_sha256: 'def' }),
};
const fleet = createAgentFleet({
  fabric,
  providers,
  providerHandlers: { hvm4: async () => ({ witness: 'bounded-reduction' }) },
});

const inspection = await fleet.inspect();
assert.equal(inspection.providers.length, 19);
assert.equal(inspection.providers.find((provider) => provider.id === 'hvm4').state, 'promoted');
const wave = await fleet.runGenerationWave({ profileId: 'coder-large', prompt: 'write a parser', nativeToolId: 'bonfyre-qwen-fpq', providerIds: ['hvm4', 'cavemem'] });
assert.equal(wave.count, 4);
assert.equal(wave.passed, 3);
assert.equal(wave.skipped, 1);
assert.ok(wave.wave_sha256);
assert.ok(wave.results.every((result) => result.receipt_sha256));
assert.ok(wave.results.every((result) => result.canonical.schema_version === 'bonfyre.execution.receipt.v1'));
assert.equal(toCanonicalReceipt({ task: { id: 'x', b: 2, a: 1 }, result: { z: 3, y: 2 }, status: 'passed', startedAt: 'now' }).input_sha256,
  toCanonicalReceipt({ task: { a: 1, b: 2, id: 'x' }, result: { y: 2, z: 3 }, status: 'passed', startedAt: 'now' }).input_sha256);
let shadowCalled = false;
const shadowProviders = providers.map((provider) => provider.id === 'hvm4' ? { ...provider, state: 'shadow' } : provider);
const shadowFleet = createAgentFleet({ fabric, providers: shadowProviders, providerHandlers: { hvm4: async () => { shadowCalled = true; } } });
const shadowWave = await shadowFleet.runWave([{ id: 'shadow', kind: 'provider', provider_id: 'hvm4' }]);
assert.equal(shadowWave.results[0].status, 'shadow');
assert.equal(shadowCalled, false);
const reconciled = reconcileProviderCatalog({ providers: shadowProviders, adapters: [{ id: 'hvm4', state: 'installed', status: 'passed', command_path: '/usr/local/bin/hvm4' }] });
assert.equal(reconciled.find((provider) => provider.id === 'hvm4').promotion_ready, true);
assert.equal(reconciled.find((provider) => provider.id === 'hvm4').execution_ready, false);
let liveCalls = 0;
const liveFleet = createAgentFleet({
  fabric,
  providers: providerCatalog({ configured: { cavemem: { command: 'cavemem', state: 'promoted' } } }),
  adapterRuntime: {
    discover: async () => [{ id: 'cavemem', state: 'installed', status: 'passed', command_path: '/mock/cavemem' }],
    observations: () => [{ id: 'cavemem', state: 'installed', status: 'passed', command_path: '/mock/cavemem' }],
    providerHandlers: () => ({ cavemem: async () => { liveCalls += 1; return { persisted_observation: 'receipt-1' }; } }),
  },
});
const liveWave = await liveFleet.runWave([{ id: 'memory', kind: 'provider', provider_id: 'cavemem' }]);
assert.equal(liveWave.passed, 1);
assert.equal(liveCalls, 1);
assert.equal((await liveFleet.inspect()).providers.find((provider) => provider.id === 'cavemem').execution_ready, true);
console.log('provider fleet tests passed');
