// One process-local native execution owner. Planning imports only a descriptor;
// weights are verified and a child is started only when execution is requested.
import { lstat } from 'node:fs/promises';
import { homedir } from 'node:os';
import { join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { CLEF_FLASH_OWNER_IDENTITY, createPinnedClefFlashEvaluator } from './local-systemone.mjs';
import nativeLock from './native/clef-runtime.lock.json' with { type: 'json' };
import { acquireModelMemory } from '../../integrations/estate/model_admission.mjs';

const identityKeys = ['model', 'artifact_digest', 'question_semantics', 'confidence_semantics',
  'deterministic', 'model_sha256', 'native_binary_sha256', 'implementation_digest', 'adapter_source_sha256'];

function waitForCaller(work, signal) {
  signal?.throwIfAborted();
  if (!signal) return work;
  return new Promise((resolve, reject) => {
    const abort = () => { signal.removeEventListener('abort', abort); reject(signal.reason); };
    signal.addEventListener('abort', abort, { once: true });
    Promise.resolve(work).then(value => {
      signal.removeEventListener('abort', abort); resolve(value);
    }, error => { signal.removeEventListener('abort', abort); reject(error); });
    if (signal.aborted) abort();
  });
}

// Low-level constructor is injectable for offline ownership/lifecycle tests.
// A factory owns cleanup of a rejected startup. Its signal belongs to the
// manager, never to whichever caller happened to arrive first.
export function createInstalledDecisionManager({ identity, factory, idleMs = 60000 }) {
  if (!identity || typeof identity.model !== 'string' || !/^[a-f0-9]{64}$/.test(identity.artifact_digest) ||
      typeof factory !== 'function' || !Number.isSafeInteger(idleMs) || idleMs < 1 || idleMs > 3600000)
    throw new Error('invalid installed decision owner configuration');
  const passport = Object.freeze(Object.fromEntries(identityKeys
    .filter(key => Object.hasOwn(identity, key)).map(key => [key, identity[key]])));
  let current = null;
  let closing = null;
  let closeFailure = null;

  function clearIdle(entry) {
    if (entry.timer) clearTimeout(entry.timer);
    entry.timer = null;
  }
  function armIdle(entry) {
    clearIdle(entry);
    if (current !== entry || entry.users !== 0 || !entry.owner) return;
    entry.timer = setTimeout(() => {
      entry.timer = null;
      if (current === entry && entry.users === 0) void retire(entry).catch(() => {});
    }, idleMs);
    entry.timer.unref?.();
  }
  function retire(entry) {
    if (entry.closing) return entry.closing;
    clearIdle(entry);
    if (current === entry) current = null;
    entry.stop.abort(new Error('installed decision runtime closed'));
    const work = entry.pending.then(owner => owner.close(), () => undefined)
      .catch(error => { closeFailure = error; throw error; })
      .finally(() => { if (closing === work) closing = null; });
    entry.closing = work;
    closing = work;
    return work;
  }
  function start() {
    const entry = { stop: new AbortController(), pending: null, owner: null, users: 0, timer: null, closing: null };
    current = entry;
    entry.pending = Promise.resolve().then(() => factory({ signal: entry.stop.signal })).then(async owner => {
      try {
        if (!owner || typeof owner.evaluate !== 'function' || typeof owner.close !== 'function' ||
            identityKeys.some(key => owner[key] !== passport[key]))
          throw new Error('installed native evaluator does not match its declared passport');
      } catch (error) {
        try { await owner?.close?.(); } catch (failure) { closeFailure = failure; }
        throw error;
      }
      entry.owner = owner;
      armIdle(entry);
      return owner;
    }).catch(error => {
      if (current === entry) current = null;
      throw error;
    });
    // Every waiter may cancel before startup completes. Retain cleanup/idle
    // handling without creating an unhandled rejected startup promise.
    void entry.pending.catch(() => {});
    return entry;
  }
  async function use(signal, operation) {
    signal?.throwIfAborted();
    while (closing) await waitForCaller(closing, signal);
    signal?.throwIfAborted();
    if (closeFailure) throw new Error('previous native runtime shutdown failed; refusing a second process', { cause: closeFailure });
    const entry = current || start();
    entry.users++;
    clearIdle(entry);
    try {
      const owner = await waitForCaller(entry.pending, signal);
      signal?.throwIfAborted();
      entry.stop.signal.throwIfAborted();
      // Check again before use: a mutable injected owner cannot change its
      // artifact while the stable descriptor continues to claim the old one.
      const unchanged = () => {
        if (identityKeys.some(key => owner[key] !== passport[key]))
          throw new Error('installed native evaluator passport changed after startup');
      };
      unchanged();
      const value = await operation(owner, entry.stop.signal);
      unchanged();
      return value;
    } finally { entry.users--; armIdle(entry); }
  }
  const evaluator = Object.freeze({ ...passport,
    evaluate: request => use(request?.signal, (owner, shutdown) => owner.evaluate({ ...request,
      signal: request?.signal ? AbortSignal.any([request.signal, shutdown]) : shutdown })),
  });
  return Object.freeze({
    evaluator,
    get: ({ signal } = {}) => use(signal, () => evaluator),
    close: () => current ? retire(current) : closing || (closeFailure ? Promise.reject(closeFailure) : Promise.resolve()),
  });
}

const repository = fileURLToPath(new URL('../../', import.meta.url));
export const installedDecisionPaths = Object.freeze({
  binaryPath: resolve(repository, nativeLock.runtime.local_build.artifact),
  modelPath: join(homedir(), 'BonfyreModels', 'gguf', 'clef-flash', nativeLock.model.file),
});

async function installedFactory({ signal }) {
  for (const [kind, path] of Object.entries(installedDecisionPaths)) {
    try {
      if (!(await lstat(path)).isFile()) throw new Error('not a regular file');
    } catch (cause) {
      const instruction = kind === 'modelPath'
        ? `Install ${nativeLock.model.repository}/${nativeLock.model.file} at revision ${nativeLock.model.revision}.`
        : 'Build and install the approved native Clef binary using native/build-clef.sh and its binary passport.';
      throw new Error(`Local Clef ${kind} is unavailable at ${path}. ${instruction} No remote fallback is enabled.`, { cause });
    }
  }
  signal.throwIfAborted();
  // Six gigabytes of weights: ask the memory ledger first, give the grant back
  // when the idle eviction closes the child. The pinned adapter is untouched.
  const grant = acquireModelMemory({ modelPath: installedDecisionPaths.modelPath, plane: 'clef-decision' });
  let owner;
  try {
    owner = await createPinnedClefFlashEvaluator({ ...installedDecisionPaths, startupMs: 120000, requestMs: 120000,
      signal: AbortSignal.any([signal, AbortSignal.timeout(120000)]) });
  } catch (error) { grant.release(); throw error; }
  return Object.freeze({ ...owner, close: () => Promise.resolve(owner.close()).finally(() => grant.release()) });
}

const installed = createInstalledDecisionManager({ identity: CLEF_FLASH_OWNER_IDENTITY, factory: installedFactory });
export const installedDecisionOwner = installed.evaluator;
export const getInstalledDecisionEvaluator = installed.get;
// CLIs must await this in finally; resident services use the 60-second idle
// eviction and may call it explicitly during shutdown. No process exit hook.
export const closeInstalledDecisionEvaluator = installed.close;
