import assert from 'node:assert/strict';
import { createEstateControlPlane } from './control_plane.mjs';

const plane = createEstateControlPlane({
  root: '/missing',
  providerConfig: { cavemem: { command: 'cavemem', state: 'shadow' } },
});
assert.equal(typeof plane.fabric.modelCatalog, 'function');
assert.equal(typeof plane.adapterRuntime.probe, 'function');
assert.equal(typeof plane.fleet.runWave, 'function');
assert.ok(plane.mcp.tools.some((tool) => tool.name === 'bonfyre_live_adapter_probe'));
assert.equal((await plane.fleet.inspect()).providers.find((provider) => provider.id === 'cavemem').state, 'shadow');
console.log('estate control plane tests passed');
