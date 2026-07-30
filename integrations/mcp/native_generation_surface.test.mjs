import assert from 'node:assert/strict';
import { createNativeGenerationMcpSurface } from './native_generation_surface.mjs';

const calls = [];
const fabric = {
  inventory: async () => [{ id: 'bonfyre-embed', capabilities: ['embedding'] }],
  profileInventory: () => [{ id: 'coder-large', model: 'coder-large' }],
  invoke: async (args) => ({ status: 'passed', tool_id: args.toolId }),
  generate: async () => ({ schema_version: 'bonfyre.native.generation.result.v1', output: 'done', output_sha256: 'abc' }),
  batchGenerate: async () => ({ schema_version: 'bonfyre.native.generation.batch.v1', batch_id: 'batch-1', results: [] }),
};
const cmsClient = {
  async syncGenerationResult(result) { calls.push(['one', result]); },
  async syncGenerationBatch(result) { calls.push(['batch', result]); },
};
const surface = createNativeGenerationMcpSurface(fabric, { cmsClient });

assert.deepEqual((await surface.invoke('bonfyre_native_inventory')).result[0].id, 'bonfyre-embed');
assert.equal((await surface.invoke('bonfyre_generation_profiles')).result[0].id, 'coder-large');
assert.equal((await surface.invoke('bonfyre_native_invoke', { toolId: 'bonfyre-embed' })).result.status, 'passed');
await surface.invoke('bonfyre_generation_generate', { profileId: 'coder-large', prompt: 'hello' });
await surface.invoke('bonfyre_generation_batch', { requests: [] });
assert.deepEqual(calls.map(([name]) => name), ['one', 'batch']);
console.log('native generation MCP surface tests passed');
