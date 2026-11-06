// Trusted entrypoints only. This is bounded execution, not a code-loading API.
import { Worker, isMainThread, parentPort, workerData } from 'node:worker_threads';
import { fileURLToPath } from 'node:url';
import { lstatSync } from 'node:fs';

const VERSION = 1;
const ERRNO = /^E[A-Z0-9_]{1,63}$/;
const failure = (name, message) => Object.assign(new Error(message), { name });
const record = value => value !== null && typeof value === 'object' && !Array.isArray(value);
function boundedJson(value, maximum) {
  let nodes = 0, bytes = 0;
  const chunks = [], append = text => {
    bytes += Buffer.byteLength(text);
    if (bytes > maximum) throw failure('WorkerProtocolError', 'worker message exceeds byte budget');
    chunks.push(text);
  };
  const visit = (item, depth) => {
    if (++nodes > 200000 || depth > 64) throw failure('WorkerProtocolError', 'worker JSON structure budget exceeded');
    if (typeof item === 'string') {
      if (Buffer.byteLength(item) > maximum) throw failure('WorkerProtocolError', 'worker message exceeds byte budget');
      append(JSON.stringify(item));
    } else if (typeof item === 'number') {
      if (!Number.isFinite(item)) throw failure('WorkerProtocolError', 'worker numbers must be finite');
      append(JSON.stringify(item));
    } else if (Array.isArray(item)) {
      if (item.length > 200000) throw failure('WorkerProtocolError', 'worker array budget exceeded');
      append('[');
      for (let i = 0; i < item.length; i++) {
        if (i) append(',');
        const descriptor = Object.getOwnPropertyDescriptor(item, i);
        if (!descriptor || !Object.hasOwn(descriptor, 'value')) throw failure('WorkerProtocolError', 'worker JSON array must contain only data');
        visit(descriptor.value, depth + 1);
      }
      append(']');
    } else if (record(item) && [Object.prototype, null].includes(Object.getPrototypeOf(item))) {
      append('{'); let first = true;
      for (const key of Object.keys(item)) {
        if (!first) append(','); first = false;
        visit(key, depth + 1);
        append(':');
        const descriptor = Object.getOwnPropertyDescriptor(item, key);
        if (!Object.hasOwn(descriptor, 'value')) throw failure('WorkerProtocolError', 'worker JSON accessors are forbidden');
        visit(descriptor.value, depth + 1);
      }
      append('}');
    } else if (item === null || typeof item === 'boolean') append(String(item));
    else throw failure('WorkerProtocolError', 'worker payload must be plain JSON');
  };
  visit(value, 0);
  return chunks.join('');
}
function decode(source, maximum) {
  if (typeof source !== 'string' || Buffer.byteLength(source) > maximum) throw failure('WorkerProtocolError', 'invalid worker message size');
  const value = JSON.parse(source);
  boundedJson(value, maximum);
  return value;
}

export function createOwnedWorkerPool({ workerUrl, maxWorkers = 2, timeoutMs = 120000, maxBytes = 10 * 1024 * 1024 } = {}) {
  if (!(workerUrl instanceof URL) || workerUrl.protocol !== 'file:' || workerUrl.search || workerUrl.hash ||
      !lstatSync(fileURLToPath(workerUrl)).isFile()) throw new Error('trusted regular worker file URL required');
  if (!Number.isSafeInteger(maxWorkers) || maxWorkers < 1 || maxWorkers > 2) throw new Error('worker limit must be 1 or 2');
  if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 2400000) throw new Error('invalid worker timeout');
  if (!Number.isSafeInteger(maxBytes) || maxBytes < 1024 || maxBytes > 32 * 1024 * 1024) throw new Error('invalid worker byte budget');
  const url = new URL(workerUrl.href), slots = new Set();
  let closed = false, closing, sequence = 0;
  function cleanup(task) {
    clearTimeout(task.timer);
    task.signal?.removeEventListener('abort', task.onAbort);
  }
  function retire(slot, error) {
    if (slot.retirement) return slot.retirement;
    slot.retired = true;
    Atomics.store(slot.cancellation, 0, 1);
    const task = slot.task; slot.task = null;
    if (task) cleanup(task);
    // A slot remains occupied until termination joins. Never overlap its next
    // generation with a cancelled synchronous operation still running.
    slot.retirement = Promise.resolve().then(() => slot.worker.terminate()).then(
      () => { slots.delete(slot); if (task) task.reject(error); },
      () => { if (task) task.reject(failure('WorkerTerminationError', 'worker termination failed; pool refuses reuse')); throw failure('WorkerTerminationError', 'worker termination failed'); },
    );
    slot.retirement.catch(() => {});
    return slot.retirement;
  }
  function spawn() {
    const cancellationBuffer = new SharedArrayBuffer(Int32Array.BYTES_PER_ELEMENT);
    const cancellation = new Int32Array(cancellationBuffer);
    const worker = new Worker(url, { workerData: { ownedWorkerProtocol: VERSION, maxBytes, cancellationBuffer } });
    const slot = { worker, cancellation, task: null, retired: false, retirement: null };
    slots.add(slot);
    worker.on('message', source => {
      if (slot.retired) return;
      const task = slot.task;
      try {
        const message = decode(source, maxBytes);
        if (!task || !record(message) || message.version !== VERSION || message.id !== task.id || typeof message.ok !== 'boolean' ||
            Object.keys(message).some(key => !['version', 'id', 'ok', 'value', 'error'].includes(key)) ||
            (message.ok ? !Object.hasOwn(message, 'value') || Object.hasOwn(message, 'error')
              : !record(message.error) || typeof message.error.message !== 'string' || message.error.message.length > 2000 || Object.hasOwn(message, 'value') ||
                Object.keys(message.error).some(key => !['message', 'code'].includes(key)) ||
                Object.hasOwn(message.error, 'code') && (typeof message.error.code !== 'string' || !ERRNO.test(message.error.code))))
          throw failure('WorkerProtocolError', 'unexpected worker reply');
        slot.task = null; cleanup(task); worker.unref();
        if (message.ok) task.resolve(message.value);
        else task.reject(Object.assign(failure('WorkerExecutionError', message.error.message), message.error.code ? { code: message.error.code } : {}));
      } catch { void retire(slot, failure('WorkerProtocolError', 'invalid worker response')); }
    });
    worker.on('error', () => { void retire(slot, failure('WorkerExecutionError', 'worker runtime failed')); });
    worker.on('exit', code => { if (!slot.retired) void retire(slot, failure('WorkerExitError', `worker exited before completion (${code})`)); });
    return slot;
  }
  async function run(payload, { signal } = {}) {
    if (closed) throw failure('WorkerClosedError', 'worker pool is closed');
    if (signal && (typeof signal.addEventListener !== 'function' || typeof signal.aborted !== 'boolean')) throw new Error('invalid abort signal');
    if (signal?.aborted) throw failure('AbortError', 'worker operation cancelled before admission');
    const id = ++sequence;
    if (!Number.isSafeInteger(id)) throw new Error('worker request identity exhausted');
    const encoded = boundedJson({ version: VERSION, id, payload }, maxBytes);
    let slot = [...slots].find(item => !item.retired && !item.task);
    if (!slot && slots.size >= maxWorkers) throw failure('WorkerBusyError', 'worker capacity exhausted; no work was queued');
    if (!slot) slot = spawn();
    return await new Promise((resolve, reject) => {
      const task = { id, resolve, reject, signal, timer: null, onAbort: null };
      Atomics.store(slot.cancellation, 0, 0);
      slot.task = task; slot.worker.ref();
      task.onAbort = () => { void retire(slot, failure('AbortError', 'worker operation cancelled; no automatic retry')); };
      signal?.addEventListener('abort', task.onAbort, { once: true });
      task.timer = setTimeout(() => { void retire(slot, failure('TimeoutError', 'worker deadline exceeded; no automatic retry')); }, timeoutMs);
      if (signal?.aborted) { task.onAbort(); return; }
      try { slot.worker.postMessage(encoded); }
      catch { void retire(slot, failure('WorkerExecutionError', 'worker request could not be delivered')); }
    });
  }
  function close() {
    if (closing) return closing;
    closed = true;
    closing = Promise.all([...slots].map(slot => retire(slot, failure('WorkerClosedError', 'worker pool closed during operation')))).then(() => {});
    return closing;
  }
  return Object.freeze({ run, close });
}

// Worker modules bind a fixed operation registry to this protocol. Payloads
// cannot choose imports, subprocess commands, or executable module paths.
export function serveOwnedWorker(handler) {
  if (isMainThread || !parentPort || workerData?.ownedWorkerProtocol !== VERSION || typeof handler !== 'function')
    throw new Error('owned worker entrypoint required');
  const maximum = workerData.maxBytes;
  if (!Number.isSafeInteger(maximum) || maximum < 1024 || maximum > 32 * 1024 * 1024) throw new Error('invalid worker message budget');
  if (!(workerData.cancellationBuffer instanceof SharedArrayBuffer) || workerData.cancellationBuffer.byteLength !== Int32Array.BYTES_PER_ELEMENT)
    throw new Error('invalid worker cancellation token');
  const cancellation = new Int32Array(workerData.cancellationBuffer);
  const signal = Object.freeze({
    get aborted() { return Atomics.load(cancellation, 0) !== 0; },
    throwIfAborted() { if (Atomics.load(cancellation, 0) !== 0) throw failure('AbortError', 'worker operation cancelled'); },
  });
  const context = Object.freeze({ signal });
  let busy = false;
  parentPort.on('message', async source => {
    if (busy) { parentPort.close(); return; }
    busy = true;
    let id = null;
    try {
      const message = decode(source, maximum);
      if (!record(message) || message.version !== VERSION || !Number.isSafeInteger(message.id) || message.id < 1 ||
          Object.keys(message).length !== 3 || !Object.hasOwn(message, 'payload')) throw new Error('invalid worker request');
      id = message.id;
      signal.throwIfAborted();
      const value = await handler(message.payload, context);
      signal.throwIfAborted();
      parentPort.postMessage(boundedJson({ version: VERSION, id, ok: true, value }, maximum));
    } catch (error) {
      const message = typeof error?.message === 'string' ? error.message.slice(0, 160) : 'worker operation failed';
      const code = typeof error?.code === 'string' && ERRNO.test(error.code) ? error.code : null;
      parentPort.postMessage(boundedJson({ version: VERSION, id, ok: false, error: { message, ...(code ? { code } : {}) } }, maximum));
    } finally { busy = false; }
  });
}
