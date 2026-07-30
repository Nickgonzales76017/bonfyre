import { createHash, randomUUID } from 'node:crypto';
import { spawn } from 'node:child_process';
import { access, appendFile, mkdir, readFile } from 'node:fs/promises';
import { constants } from 'node:fs';
import { dirname, join } from 'node:path';

export const ADAPTER_PROBE_SCHEMA = 'bonfyre.adapter.probe.v1';
export const RECEIPT_JOURNAL_SCHEMA = 'bonfyre.receipt.journal.v1';

const ADAPTERS = [
  { id: 'agent-deck', capability: 'agent_fleet', command: 'agent-deck', args: ['status'], healthyExitCodes: [0] },
  { id: 'cavemem', capability: 'institutional_memory', command: 'cavemem', args: ['status'], healthyExitCodes: [0] },
  { id: 'automerge', capability: 'collaborative_state', command: 'automerge', args: ['--version'], healthyExitCodes: [0] },
  { id: 'feldera', capability: 'streaming_computation', command: 'feldera', args: ['--version'], healthyExitCodes: [0] },
  { id: 'egglog', capability: 'symbolic_rewriting', command: 'egglog', args: ['--version'], healthyExitCodes: [0] },
  { id: 'hvm4', capability: 'recursive_reduction', command: 'hvm4', args: ['--version'], healthyExitCodes: [0] },
  { id: 'hvm2', capability: 'recursive_reduction', command: 'hvm', args: ['--version'], healthyExitCodes: [0] },
  { id: 'hydro', capability: 'distributed_placement', command: 'hydro', args: ['--version'], healthyExitCodes: [0] },
  { id: 'cubecl', capability: 'accelerated_kernels', command: 'cubecl', args: ['--version'], healthyExitCodes: [0] },
  { id: 'tract', capability: 'local_model_execution', command: 'tract', args: ['--version'], healthyExitCodes: [0] },
  { id: 'burn', capability: 'tensor_runtime', command: 'burn', args: ['--version'], healthyExitCodes: [0] },
  { id: 'yaff', capability: 'artifact_compilation', command: 'yaff', args: ['--version'], healthyExitCodes: [0] },
  { id: 'lance', capability: 'vector_multimodal_data', command: 'lance', args: ['--version'], healthyExitCodes: [0] },
  { id: 'verus', capability: 'formal_verification', command: 'verus', args: ['--version'], healthyExitCodes: [0] },
  { id: 'daft', capability: 'distributed_datasets', command: 'daft', args: ['--version'], healthyExitCodes: [0] },
  { id: 'gigatoken', capability: 'context_compression', command: 'gigatoken', args: ['--version'], healthyExitCodes: [0] },
  { id: 'restate', capability: 'durable_workflows', command: 'restate', args: ['--version'], healthyExitCodes: [0] },
  { id: 'bernstein', capability: 'durable_workflows', command: 'bernstein', args: ['--version'], healthyExitCodes: [0] },
  { id: 'rtk', capability: 'context_compression', command: 'rtk', args: ['--help'], healthyExitCodes: [0] },
];

function stable(value) {
  if (Array.isArray(value)) return `[${value.map(stable).join(',')}]`;
  if (value && typeof value === 'object') return `{${Object.keys(value).sort().map((key) => `${JSON.stringify(key)}:${stable(value[key])}`).join(',')}}`;
  return JSON.stringify(value);
}

function sha256(value) {
  return createHash('sha256').update(typeof value === 'string' ? value : stable(value)).digest('hex');
}

function projectReceipt(body) {
  const receipt = { schema_version: ADAPTER_PROBE_SCHEMA, receipt_id: randomUUID(), ...body };
  const receipt_sha256 = sha256(body);
  return {
    ...receipt,
    receipt_sha256,
    ledger: {
      schema_version: 'bonfyre.adapter.ledger.v1',
      event: 'adapter.probe.completed',
      adapter_id: body.adapter_id,
      status: body.status,
      receipt_id: receipt.receipt_id,
      receipt_sha256,
    },
    analytics: {
      schema_version: 'bonfyre.adapter.analytics.v1',
      event_name: 'adapter_probe.completed',
      dimensions: { adapter_id: body.adapter_id, capability: body.capability || 'unknown' },
      measures: { duration_ms: body.duration_ms || 0, success: body.status === 'passed' ? 1 : 0 },
      receipt_id: receipt.receipt_id,
    },
  };
}

async function findExecutable(command, pathValue = process.env.PATH || '') {
  if (command.includes('/')) {
    try { await access(command, constants.X_OK); return command; } catch { return null; }
  }
  for (const directory of pathValue.split(':').filter(Boolean)) {
    const candidate = join(directory, command);
    try { await access(candidate, constants.X_OK); return candidate; } catch { /* continue */ }
  }
  return null;
}

function execute(command, args, { spawnImpl = spawn, timeoutMs = 5_000, maxOutputBytes = 48_000 } = {}) {
  return new Promise((resolve, reject) => {
    const child = spawnImpl(command, args, { stdio: ['ignore', 'pipe', 'pipe'] });
    let stdout = '';
    let stderr = '';
    let settled = false;
    const append = (current, chunk) => current.length >= maxOutputBytes ? current : `${current}${chunk}`.slice(0, maxOutputBytes);
    const done = (value) => { if (!settled) { settled = true; clearTimeout(timer); resolve(value); } };
    const timer = setTimeout(() => { child.kill('SIGKILL'); done({ exit_code: null, signal: 'SIGKILL', stdout, stderr, timed_out: true }); }, timeoutMs);
    child.stdout?.on('data', (chunk) => { stdout = append(stdout, chunk.toString()); });
    child.stderr?.on('data', (chunk) => { stderr = append(stderr, chunk.toString()); });
    child.once('error', (error) => { if (!settled) { settled = true; clearTimeout(timer); reject(error); } });
    child.once('close', (code, signal) => done({ exit_code: code, signal, stdout, stderr, timed_out: false }));
  });
}

export async function discoverLiveAdapters({ pathValue = process.env.PATH } = {}) {
  return Promise.all(ADAPTERS.map(async (adapter) => ({
    ...adapter,
    command_path: await findExecutable(adapter.command, pathValue),
  }))).then((adapters) => adapters.map(({ command_path, ...adapter }) => ({
    schema_version: ADAPTER_PROBE_SCHEMA,
    ...adapter,
    command_path,
    state: command_path ? 'installed' : 'unavailable',
  })));
}

export async function probeLiveAdapters({ adapters, timeoutMs = 5_000, maxConcurrency = 4, spawnImpl = spawn } = {}) {
  const catalog = adapters || await discoverLiveAdapters();
  const results = [];
  let cursor = 0;
  async function probeOne(adapter) {
    const started = Date.now();
    if (!adapter.command_path) return projectReceipt({
      ...adapter, adapter_id: adapter.id, state: 'unavailable', status: 'skipped', duration_ms: 0,
    });
    try {
      const result = await execute(adapter.command_path, adapter.args, { spawnImpl, timeoutMs });
      const status = result.timed_out ? 'timed_out' : adapter.healthyExitCodes.includes(result.exit_code) ? 'passed' : 'degraded';
      const body = { adapter_id: adapter.id, capability: adapter.capability, command_path: adapter.command_path, args: adapter.args, status, ...result, duration_ms: Date.now() - started };
      return projectReceipt(body);
    } catch (error) {
      const body = { adapter_id: adapter.id, command_path: adapter.command_path, status: 'error', error: error.message, duration_ms: Date.now() - started };
      return projectReceipt(body);
    }
  }
  async function worker() {
    while (cursor < catalog.length) {
      const index = cursor++;
      results[index] = await probeOne(catalog[index]);
    }
  }
  const workers = Math.min(Math.max(1, Number(maxConcurrency) || 1), Math.max(1, catalog.length));
  await Promise.all(Array.from({ length: workers }, worker));
  return results;
}

export function createLiveAdapterRuntime({ pathValue = process.env.PATH, spawnImpl = spawn, receiptJournal } = {}) {
  let lastProbes = null;
  async function discover() {
    return discoverLiveAdapters({ pathValue });
  }
  async function probe({ ids, timeoutMs, maxConcurrency } = {}) {
    const requested = ids === undefined ? null : new Set(Array.isArray(ids) ? ids : [ids]);
    const adapters = (await discover()).filter((adapter) => !requested || requested.has(adapter.id));
    const probes = await probeLiveAdapters({ adapters, timeoutMs, maxConcurrency, spawnImpl });
    const recorded = !receiptJournal?.append ? probes : await Promise.all(probes.map(async (probe) => ({ ...probe, journal: await receiptJournal.append(probe) })));
    lastProbes = recorded;
    return recorded;
  }
  function providerHandlers({ providerIds } = {}) {
    const requested = providerIds ? new Set(providerIds) : null;
    return Object.fromEntries(ADAPTERS
      .filter((adapter) => !requested || requested.has(adapter.id))
      .map((adapter) => [adapter.id, async (task = {}) => {
        const [result] = await probe({ ids: [adapter.id], timeoutMs: task.timeoutMs });
        if (!result || result.status !== 'passed') {
          throw new Error(`adapter ${adapter.id} is not healthy: ${result?.status || 'not discovered'}`);
        }
        return { adapter_id: adapter.id, capability: adapter.capability, probe: result };
      }]));
  }
  function observations() { return lastProbes; }
  return { discover, probe, observations, providerHandlers };
}

export function createReceiptJournal({ file } = {}) {
  if (!file) throw new Error('receipt journal file is required');
  async function append(receipt) {
    const body = { schema_version: RECEIPT_JOURNAL_SCHEMA, journal_id: randomUUID(), receipt, written_at: new Date().toISOString() };
    const record = { ...body, record_sha256: sha256(body) };
    await mkdir(dirname(file), { recursive: true });
    await appendFile(file, `${JSON.stringify(record)}\n`, 'utf8');
    return record;
  }
  async function read() {
    try {
      return (await readFile(file, 'utf8')).trim().split('\n').filter(Boolean).map(JSON.parse);
    } catch (error) {
      if (error.code === 'ENOENT') return [];
      throw error;
    }
  }
  return { append, read };
}
