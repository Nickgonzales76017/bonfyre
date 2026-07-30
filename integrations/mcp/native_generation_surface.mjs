import { toGenerationCmsEntry } from '../wordpress/native_generation.mjs';

export const MCP_GENERATION_SCHEMA = 'bonfyre.mcp.generation.surface.v1';

export function createNativeGenerationMcpSurface(fabric, { cmsClient, fleet, adapterRuntime } = {}) {
  if (!fabric || typeof fabric.inventory !== 'function' || typeof fabric.generate !== 'function') {
    throw new Error('A generation fabric is required');
  }
  async function syncFleetGeneration(result) {
    if (!cmsClient || !result?.results) return result;
    for (const task of result.results) {
      if (task.kind === 'generate' && task.status === 'passed' && task.result) {
        await cmsClient.syncGenerationResult(task.result);
      }
    }
    return result;
  }

  return {
    schema_version: MCP_GENERATION_SCHEMA,
    tools: [
      { name: 'bonfyre_native_inventory', effect: 'read', handler: () => fabric.inventory() },
      { name: 'bonfyre_native_health', effect: 'bounded-read', handler: (args) => fabric.probeNativeTools?.(args) || { error: 'native health probe unavailable' } },
      { name: 'bonfyre_model_catalog', effect: 'read', handler: () => fabric.modelCatalog?.() || [] },
      { name: 'bonfyre_model_recovery_verify', effect: 'bounded-read', handler: (args) => fabric.verifyModelPackRecovery?.(args) || { error: 'model recovery verifier unavailable' } },
      ...(adapterRuntime ? [
        { name: 'bonfyre_live_adapter_inventory', effect: 'read', handler: () => adapterRuntime.discover() },
        { name: 'bonfyre_live_adapter_probe', effect: 'bounded-read', handler: (args) => adapterRuntime.probe(args) },
      ] : []),
      { name: 'bonfyre_generation_profiles', effect: 'read', handler: () => fabric.profileInventory?.() || [] },
      ...(fleet ? [
        { name: 'bonfyre_fleet_inspect', effect: 'read', handler: () => fleet.inspect() },
        { name: 'bonfyre_fleet_wave', effect: 'bounded-fleet-exec', handler: async (args) => syncFleetGeneration(await fleet.runWave(args.tasks)) },
        { name: 'bonfyre_fleet_generation_wave', effect: 'bounded-fleet-exec', handler: async (args) => syncFleetGeneration(await fleet.runGenerationWave(args)) },
      ] : []),
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
