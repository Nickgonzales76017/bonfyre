import { createNativeGenerationMcpSurface } from '../mcp/native_generation_surface.mjs';
import { createEstateGenerationFabric, discoverGenerationProfiles } from './generation_fabric.mjs';
import { createLiveAdapterRuntime, createReceiptJournal } from './live_adapters.mjs';
import { createAgentFleet, providerCatalog } from './provider_fleet.mjs';

export function createEstateControlPlane({
  root = process.cwd(),
  modelRoot,
  modelBinary,
  llamaBinary,
  profiles,
  providers,
  providerConfig,
  receiptJournalFile,
  cmsClient,
  spawnImpl,
  env = process.env,
} = {}) {
  const receiptJournal = receiptJournalFile ? createReceiptJournal({ file: receiptJournalFile }) : undefined;
  const fabric = createEstateGenerationFabric({ root, modelRoot, modelBinary, llamaBinary, profiles: profiles ?? discoverGenerationProfiles({ env }), spawnImpl });
  const adapterRuntime = createLiveAdapterRuntime({ pathValue: env.PATH ?? '', spawnImpl, receiptJournal });
  const fleet = createAgentFleet({
    fabric,
    providers: providers || providerCatalog({ env, configured: providerConfig }),
    adapterRuntime,
    receiptJournal,
  });
  const mcp = createNativeGenerationMcpSurface(fabric, { cmsClient, fleet, adapterRuntime });
  return { fabric, adapterRuntime, fleet, mcp, receiptJournal };
}
