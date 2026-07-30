import { createNativeGenerationMcpSurface } from '../mcp/native_generation_surface.mjs';
import { createEstateGenerationFabric } from './generation_fabric.mjs';
import { createLiveAdapterRuntime, createReceiptJournal } from './live_adapters.mjs';
import { createAgentFleet, providerCatalog } from './provider_fleet.mjs';

export function createEstateControlPlane({
  root = process.cwd(),
  modelRoot,
  modelBinary,
  profiles,
  providers,
  providerConfig,
  receiptJournalFile,
  cmsClient,
  spawnImpl,
} = {}) {
  const receiptJournal = receiptJournalFile ? createReceiptJournal({ file: receiptJournalFile }) : undefined;
  const fabric = createEstateGenerationFabric({ root, modelRoot, modelBinary, profiles, spawnImpl });
  const adapterRuntime = createLiveAdapterRuntime({ spawnImpl, receiptJournal });
  const fleet = createAgentFleet({
    fabric,
    providers: providers || providerCatalog({ configured: providerConfig }),
    adapterRuntime,
    receiptJournal,
  });
  const mcp = createNativeGenerationMcpSurface(fabric, { cmsClient, fleet, adapterRuntime });
  return { fabric, adapterRuntime, fleet, mcp, receiptJournal };
}
