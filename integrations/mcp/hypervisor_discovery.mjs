import { findProbeExecutable, createNativeProbe, nativeProbePassed } from './native_probe.mjs';
import { projectRun } from './project_runtime.mjs';

// Refreshes observation rows only. It never installs, promotes, licenses,
// launches agents, or mistakes version/help success for capability correctness.
export function createHypervisorDiscovery({ store, root = store?.root, env = store?.env ?? process.env,
  run = projectRun, checkStop = () => {}, now = () => Date.now() / 1000, concurrency = 4 } = {}) {
  if (!store || typeof store.registry !== 'function' || typeof store.recordDiscovery !== 'function') throw new Error('discovery store interface missing');
  if (!Number.isSafeInteger(concurrency) || concurrency < 1 || concurrency > 4) throw new Error('discovery concurrency must be 1..4');
  if (typeof checkStop !== 'function' || typeof now !== 'function') throw new Error('invalid discovery dependencies');
  const environment = { ...env }, probe = createNativeProbe({ run, env: environment, cwd: root, checkStop });
  let running = false;
  async function discover({ signal } = {}) {
    if (running) throw new Error('discovery already running; no scan queued');
    signal?.throwIfAborted(); checkStop();
    const registry = store.registry();
    if (!registry || !Array.isArray(registry.projects) || registry.projects.length > 256) throw new Error('invalid or oversized discovery registry');
    const ids = new Set(), projects = registry.projects.map(project => {
      if (!project || typeof project.id !== 'string' || !/^[A-Za-z0-9_.-]{1,128}$/.test(project.id) || ids.has(project.id)) throw new Error('invalid or duplicate provider identity');
      ids.add(project.id);
      const detect = project.detect ?? [];
      if (!Array.isArray(detect) || detect.length > 16 || detect.some(name => typeof name !== 'string' || !/^[A-Za-z0-9_.+-]{1,128}$/.test(name)) || new Set(detect).size !== detect.length)
        throw new Error('invalid provider discovery commands');
      return { id: project.id, detect: [...detect] };
    });
    const observed_at = now();
    if (!Number.isFinite(observed_at) || observed_at < 0) throw new Error('invalid observation clock');
    running = true;
    const controller = new AbortController(), combined = signal ? AbortSignal.any([signal, controller.signal]) : controller.signal;
    const checkpoint = () => { combined.throwIfAborted(); checkStop(); };
    // STOP may change while a bounded child runs. Abort propagates to the shared
    // process-group owner; joining all lanes prevents detached probes on error.
    const timer = setInterval(() => { try { checkStop(); } catch { controller.abort(new Error('STOP active; discovery aborted')); } }, 50);
    timer.unref();
    let cursor = 0;
    const providers = Array.from({ length: projects.length });
    async function lane() {
      try {
        while (cursor < projects.length) {
          checkpoint(); const index = cursor++, project = projects[index];
          const commands = project.detect.map(name => [name, findProbeExecutable(name, environment)]).filter(([, path]) => path);
          const evidence = []; let version = null, health = 'missing';
          if (commands.length) {
            health = 'degraded';
            for (const option of ['--version', 'version', '--help']) {
              checkpoint();
              const result = await probe([commands[0][1], option], { timeoutMs: 12000, signal: combined });
              evidence.push(result); checkpoint();
              if (nativeProbePassed(result)) { version = 'reachable'; health = 'reachable'; break; }
              if (result.refused || result.timed_out || result.overflow || result.cancelled) break;
            }
          }
          providers[index] = { provider: project.id, installed: commands.length > 0, commands, version, health, evidence };
        }
      } catch (error) { controller.abort(error); throw error; }
    }
    try {
      const results = await Promise.allSettled(Array.from({ length: Math.min(concurrency, projects.length) }, lane));
      const failed = results.find(result => result.status === 'rejected');
      if (failed) throw failed.reason;
      checkpoint();
      // The store validates every row and commits the complete observation plus
      // its event atomically; no partial update is made by an individual lane.
      store.recordDiscovery({ providers, observed_at });
      return { providers };
    } finally { clearInterval(timer); running = false; }
  }
  return Object.freeze({ discover });
}
