// One contract table feeds CLI, MCP and distributed administrative bindings.
export const V6_READ_MODELS = Object.freeze({
  estate: 'estate/estate.json', workgraph: 'graphs/workgraph-v61.json', resources: 'resources/resource-graph.json',
  backends: 'backends/backend-fabric.json', missions: 'missions/catalog.json', history: 'history/federated-history.json', sparse: 'sparse/snapshot.json',
});
export const V6_ACTIONS = Object.freeze({
  v6_resource_discover: ['resources', 'discoverResources', { probe_external: { type: 'boolean', default: true } }],
  v6_provider_activate: ['resources', 'activateProviders', {}],
  v6_backend_probe: ['resources', 'discoverBackends', { build: { type: 'boolean', default: false } }],
  v6_mission_activate: ['missions', 'activate', { compile: { type: 'boolean', default: true } }],
  v6_history_federate: ['history', 'federate', {}], v6_sparse_snapshot: ['history', 'sparseSnapshot', {}],
});
const descriptions = {
  v6_status: 'Return capability-fabric state and historical receipt integrity.',
  v6_verify: 'Verify bound historical activation proof, BFP bytes and receipt chain; not current remote health.',
  v6_estate: 'Return the compiled command and application estate.', v6_workgraph: 'Return field/link/resource/mission WorkGraph.',
  v6_resources: 'Return secret-safe resource contracts.', v6_backends: 'Return observed and verified substrate evidence.',
  v6_missions: 'Return governed mission candidates and execution evidence.', v6_history: 'Return verified federated receipt roots.',
  v6_sparse: 'Return bounded payload inventory.', v6_proof: 'Return latest historical activation proof.', v6_outcomes: 'Return operational outcome summaries.',
  r8_bfp_verify: 'Stream-verify every section of a confined BFP.',
  v6_resource_discover: 'Refresh resource and integration observations.',
  v6_provider_activate: 'Run configured read-only reachability probes; no secret or response-body persistence.',
  v6_backend_probe: 'Probe or build/test already-present substrate sources; never fetch remote code.',
  v6_mission_activate: 'Prepare ten governed mission families; activation requires real compiler and canary evidence.',
  v6_history_federate: 'Verify source histories and refresh references without rewriting source owners.',
  v6_sparse_snapshot: 'Build bounded sparse bundles from pinned files and consistent database snapshots.',
};
export const V6_TOOLS = Object.keys(descriptions).sort().map(name => {
  const action = Object.hasOwn(V6_ACTIONS, name);
  return { name, description: descriptions[name], inputSchema: { type: 'object', additionalProperties: false,
    properties: action ? { confirm: { type: 'boolean', const: true }, ...V6_ACTIONS[name][2] }
      : name === 'r8_bfp_verify' ? { path: { type: 'string' } } : {}, ...(action ? { required: ['confirm'] } : {}) },
    annotations: { readOnlyHint: !action, destructiveHint: name === 'v6_backend_probe', idempotentHint: !action,
      openWorldHint: ['v6_resource_discover', 'v6_provider_activate', 'v6_backend_probe'].includes(name) } };
});
