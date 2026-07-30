import assert from 'node:assert/strict';
import { createNativeGenerationMcpSurface } from './native_generation_surface.mjs';

const calls = [];
const fabric = {
  inventory: async () => [{ id: 'bonfyre-embed', capabilities: ['embedding'] }],
  modelCatalog: async () => [{ id: 'coder-large', available: false, missing: ['model-parts'] }],
  probeNativeTools: async () => ({ schema_version: 'bonfyre.native.tool.health.v1', count: 1, passed: 1 }),
  profileInventory: () => [{ id: 'coder-large', model: 'coder-large' }],
  invoke: async (args) => ({ status: 'passed', tool_id: args.toolId }),
  generate: async () => ({ schema_version: 'bonfyre.native.generation.result.v1', output: 'done', output_sha256: 'abc' }),
  batchGenerate: async () => ({ schema_version: 'bonfyre.native.generation.batch.v1', batch_id: 'batch-1', results: [] }),
};
const cmsClient = {
  async syncGenerationResult(result) { calls.push(['one', result]); },
  async syncGenerationBatch(result) { calls.push(['batch', result]); },
};
const fleet = {
  inspect: async () => ({ providers: [] }),
  runWave: async (tasks) => ({ count: tasks.length }),
  runGenerationWave: async () => ({ count: 1 }),
};
const surface = createNativeGenerationMcpSurface(fabric, { cmsClient, fleet });

assert.deepEqual((await surface.invoke('bonfyre_native_inventory')).result[0].id, 'bonfyre-embed');
assert.equal((await surface.invoke('bonfyre_model_catalog')).result[0].missing[0], 'model-parts');
assert.equal((await surface.invoke('bonfyre_native_health')).result.passed, 1);
assert.equal((await surface.invoke('bonfyre_generation_profiles')).result[0].id, 'coder-large');
assert.equal((await surface.invoke('bonfyre_native_invoke', { toolId: 'bonfyre-embed' })).result.status, 'passed');
await surface.invoke('bonfyre_generation_generate', { profileId: 'coder-large', prompt: 'hello' });
await surface.invoke('bonfyre_generation_batch', { requests: [] });
assert.deepEqual((await surface.invoke('bonfyre_fleet_inspect')).result.providers, []);
assert.equal((await surface.invoke('bonfyre_fleet_wave', { tasks: [{ id: 'one' }] })).result.count, 1);
assert.equal((await surface.invoke('bonfyre_fleet_generation_wave', { prompt: 'hello' })).result.count, 1);
assert.deepEqual(calls.map(([name]) => name), ['one', 'batch']);
console.log('native generation MCP surface tests passed');
