import { toGenerationCmsEntry } from '../wordpress/native_generation.mjs';

export const MCP_GENERATION_SCHEMA = 'bonfyre.mcp.generation.surface.v1';

export function createNativeGenerationMcpSurface(fabric, { cmsClient } = {}) {
  if (!fabric || typeof fabric.inventory !== 'function' || typeof fabric.generate !== 'function') {
    throw new Error('A generation fabric is required');
  }
  return {
    schema_version: MCP_GENERATION_SCHEMA,
    tools: [
      { name: 'bonfyre_native_inventory', effect: 'read', handler: () => fabric.inventory() },
      { name: 'bonfyre_generation_profiles', effect: 'read', handler: () => fabric.profileInventory?.() || [] },
      { name: 'bonfyre_native_invoke', effect: 'bounded-exec', handler: (args) => fabric.invoke(args) },
      { name: 'bonfyre_generation_generate', effect: 'bounded-model-exec', handler: async (args) => {
        const result = await fabric.generate(args);
        if (cmsClient) await cmsClient.syncGenerationResult(result);
        return result;
      } },
      { name: 'bonfyre_generation_batch', effect: 'bounded-batch-exec', handler: async (args) => {
        const result = await fabric.batchGenerate(args.requests);
        if (cmsClient?.syncGenerationBatch) await cmsClient.syncGenerationBatch(result);
        return result;
      } },
    ],
    async invoke(name, args = {}) {
      const tool = this.tools.find((candidate) => candidate.name === name);
      if (!tool) throw new Error(`Unknown MCP generation tool: ${name}`);
      const result = await tool.handler(args);
      return { schema_version: MCP_GENERATION_SCHEMA, tool: name, result };
    },
    toCmsEntry(result, options) { return toGenerationCmsEntry(result, options); },
  };
}
