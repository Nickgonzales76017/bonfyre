import assert from 'node:assert/strict';
import { createAgentFleet, providerCatalog } from './provider_fleet.mjs';

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
assert.equal(inspection.providers.length, 16);
assert.equal(inspection.providers.find((provider) => provider.id === 'hvm4').state, 'promoted');
const wave = await fleet.runGenerationWave({ profileId: 'coder-large', prompt: 'write a parser', nativeToolId: 'bonfyre-qwen-fpq', providerIds: ['hvm4', 'cavemem'] });
assert.equal(wave.count, 4);
assert.equal(wave.passed, 3);
assert.equal(wave.skipped, 1);
assert.ok(wave.wave_sha256);
assert.ok(wave.results.every((result) => result.receipt_sha256));
assert.ok(wave.results.every((result) => result.canonical.schema_version === 'bonfyre.execution.receipt.v1'));
console.log('provider fleet tests passed');
