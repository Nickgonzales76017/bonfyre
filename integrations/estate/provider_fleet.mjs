import { createHash, randomUUID } from 'node:crypto';

export const FLEET_SCHEMA = 'bonfyre.agent.fleet.receipt.v1';
export const PROVIDER_SCHEMA = 'bonfyre.agent.provider.catalog.v1';

const PROVIDERS = [
  ['agent-deck', 'agent_fleet', 'executor/reviewer'],
  ['cavemem', 'institutional_memory', 'memory'],
  ['automerge', 'collaborative_state', 'state'],
  ['feldera', 'streaming_computation', 'stream'],
  ['egglog', 'symbolic_rewriting', 'rewrite'],
  ['hvm4', 'recursive_reduction', 'reduction'],
  ['hydro', 'distributed_placement', 'placement'],
  ['cubecl', 'accelerated_kernels', 'kernel'],
  ['tract', 'local_model_execution', 'model'],
  ['burn', 'tensor_runtime', 'tensor'],
  ['yaff', 'artifact_compilation', 'artifact'],
  ['lance', 'vector_multimodal_data', 'vector'],
  ['verus', 'formal_verification', 'proof'],
  ['daft', 'distributed_datasets', 'dataset'],
  ['gigatoken', 'context_compression', 'compression'],
  ['restate', 'durable_workflows', 'workflow'],
];

const keyFor = (id) => id.toUpperCase().replace(/[^A-Z0-9]/g, '_');
function stable(value) {
  if (Array.isArray(value)) return `[${value.map(stable).join(',')}]`;
  if (value && typeof value === 'object') return `{${Object.keys(value).sort().map((key) => `${JSON.stringify(key)}:${stable(value[key])}`).join(',')}}`;
  return JSON.stringify(value);
}

const digest = (value) => createHash('sha256').update(stable(value)).digest('hex');

export function toCanonicalReceipt({ task, result, status, startedAt, completedAt = new Date().toISOString() }) {
  const input = task || {};
  const output = result || null;
  return {
    schema_version: 'bonfyre.execution.receipt.v1',
    receipt_id: randomUUID(),
    task_id: input.id || null,
    tool_id: input.toolId || null,
    provider_id: input.provider_id || null,
    profile_id: input.profileId || null,
    status,
    input_sha256: digest(input),
    output_sha256: digest(output),
    effects: input.kind === 'provider' ? ['adapter'] : ['read', 'compute'],
    proof_refs: [],
    started_at: startedAt,
    completed_at: completedAt,
  };
}

export function providerCatalog({ env = process.env, configured = {} } = {}) {
  return PROVIDERS.map(([id, capability, role]) => {
    const key = keyFor(id);
    const command = configured[id]?.command || env[`BONFYRE_PROVIDER_${key}_COMMAND`] || '';
    const state = configured[id]?.state || (command ? 'shadow' : 'unconfigured');
    return {
      schema_version: PROVIDER_SCHEMA,
      id,
      capability,
      role,
      state,
      command: command || null,
      source: command ? 'configured-adapter' : 'catalog',
    };
  });
}

function receipt({ task, status, result = null, error = null, startedAt }) {
  const body = {
    task_id: task.id,
    kind: task.kind,
    provider_id: task.provider_id || null,
    status,
    result,
    error,
    started_at: startedAt,
    completed_at: new Date().toISOString(),
  };
  const fleetReceipt = {
    schema_version: 'bonfyre.agent.fleet.task.receipt.v1',
    receipt_id: randomUUID(),
    ...body,
    receipt_sha256: digest(body),
  };
  const canonical = toCanonicalReceipt({ task, result, status, startedAt, completedAt: body.completed_at });
  return { ...fleetReceipt, canonical, canonical_sha256: digest(canonical) };
}

export function createAgentFleet({ fabric, providers, maxConcurrency = 2, providerHandlers = {} } = {}) {
  if (!fabric || typeof fabric.inventory !== 'function') throw new Error('A native generation fabric is required');
  const catalog = providers || providerCatalog();

  async function inspect() {
    return {
      schema_version: FLEET_SCHEMA,
      fleet_id: digest(catalog.map(({ id, state, command }) => ({ id, state, command }))),
      providers: catalog,
      native_tools: await fabric.inventory(),
      profiles: fabric.profileInventory?.() || [],
    };
  }

  async function executeTask(task) {
    const startedAt = new Date().toISOString();
    try {
      if (!task?.id || !task.kind) throw new Error('fleet task requires id and kind');
      if (task.kind === 'generate') {
        const result = await fabric.generate(task);
        return receipt({ task, status: 'passed', result, startedAt });
      }
      if (task.kind === 'native') {
        const result = await fabric.invoke(task);
        return receipt({ task, status: result.status === 'passed' ? 'passed' : 'failed', result, startedAt });
      }
      const provider = catalog.find((candidate) => candidate.id === task.provider_id);
      if (!provider) throw new Error(`unknown provider: ${task.provider_id}`);
      if (provider.state === 'unconfigured') return receipt({ task, status: 'skipped', error: 'provider is not configured', startedAt });
      if (provider.state !== 'promoted') return receipt({ task, status: 'shadow', result: { provider, reason: 'provider is not promoted' }, startedAt });
      const handler = providerHandlers[provider.id];
      if (typeof handler !== 'function') return receipt({ task, status: 'shadow', result: { provider, reason: 'no promoted handler' }, startedAt });
      const timeoutMs = Math.max(100, Math.min(Number(task.timeoutMs) || 30_000, 120_000));
      const result = await Promise.race([
        handler(task, provider),
        new Promise((_, reject) => setTimeout(() => reject(new Error(`provider handler timed out after ${timeoutMs}ms`)), timeoutMs)),
      ]);
      return receipt({ task, status: 'passed', result, startedAt });
    } catch (error) {
      return receipt({ task, status: 'failed', error: error.message, startedAt });
    }
  }

  async function runWave(tasks = []) {
    if (!Array.isArray(tasks) || tasks.length > 64) throw new Error('fleet wave is limited to 64 tasks');
    const results = [];
    let cursor = 0;
    async function worker() {
      while (cursor < tasks.length) {
        const index = cursor++;
        results[index] = await executeTask(tasks[index]);
      }
    }
    await Promise.all(Array.from({ length: Math.min(maxConcurrency, Math.max(1, tasks.length)) }, worker));
    const body = {
      schema_version: FLEET_SCHEMA,
      wave_id: randomUUID(),
      count: results.length,
      passed: results.filter((item) => item.status === 'passed').length,
      skipped: results.filter((item) => item.status === 'skipped' || item.status === 'shadow').length,
      failed: results.filter((item) => item.status === 'failed').length,
      results,
      created_at: new Date().toISOString(),
    };
    return { ...body, wave_sha256: digest(body) };
  }

  async function runGenerationWave({ profileId, prompt, nativeToolId, nativeArgs = [], providerIds = [] } = {}) {
    const tasks = [];
    if (profileId) tasks.push({ id: `generate:${profileId}`, kind: 'generate', profileId, prompt });
    if (nativeToolId) tasks.push({ id: `native:${nativeToolId}`, kind: 'native', toolId: nativeToolId, args: nativeArgs });
    for (const providerId of providerIds) tasks.push({ id: `provider:${providerId}`, kind: 'provider', provider_id: providerId });
    return runWave(tasks);
  }

  return { inspect, executeTask, runWave, runGenerationWave };
}
