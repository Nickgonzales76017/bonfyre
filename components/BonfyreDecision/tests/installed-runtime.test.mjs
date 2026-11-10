import { expect, test } from 'bun:test';
import { setTimeout as delay } from 'node:timers/promises';
import { createInstalledDecisionManager, installedDecisionOwner, installedDecisionPaths } from '../installed-runtime.mjs';
import { CLEF_FLASH_OWNER_IDENTITY } from '../local-systemone.mjs';

const identity = Object.freeze({ model: 'offline-fixture', artifact_digest: 'a'.repeat(64),
  question_semantics: 'joint_schema', confidence_semantics: 'fixture', deterministic: false });
const deferred = () => {
  let resolve, reject;
  const promise = new Promise((a, b) => { resolve = a; reject = b; });
  return { promise, resolve, reject };
};
const owner = (extra = {}) => ({ ...identity, evaluate: async () => new Float32Array([1]), close: async () => {}, ...extra });

test('lazy descriptor is the actual factory passport and requires no startup for planning', async () => {
  let starts = 0;
  const manager = createInstalledDecisionManager({ identity, factory: async () => { starts++; return owner(); } });
  expect(manager.evaluator.model).toBe(identity.model);
  expect(manager.evaluator.artifact_digest).toBe(identity.artifact_digest);
  expect(Object.isFrozen(manager.evaluator)).toBe(true);
  for (const [key, value] of Object.entries(CLEF_FLASH_OWNER_IDENTITY)) expect(installedDecisionOwner[key]).toEqual(value);
  expect(installedDecisionPaths.modelPath).toEndWith('/BonfyreModels/gguf/clef-flash/Clef-Flash-Q4_K_M.gguf');
  await manager.close();
  expect(starts).toBe(0);
});

test('concurrent callers share one startup and one caller abort cannot poison another', async () => {
  const ready = deferred();
  let starts = 0, closes = 0, factorySignal;
  const manager = createInstalledDecisionManager({ identity, factory: ({ signal }) => {
    starts++; factorySignal = signal; return ready.promise;
  } });
  try {
    const cancel = new AbortController();
    const a = manager.get({ signal: cancel.signal });
    const b = manager.get();
    await Promise.resolve();
    cancel.abort(new Error('only first caller'));
    await expect(a).rejects.toThrow('only first caller');
    expect(starts).toBe(1);
    expect(factorySignal.aborted).toBe(false);
    ready.resolve(owner({ close: async () => { closes++; } }));
    expect(await b).toBe(manager.evaluator);
    expect(await manager.get()).toBe(manager.evaluator);
    expect(starts).toBe(1);
  } finally { ready.resolve(owner()); await manager.close(); }
  expect(closes).toBe(1);
});

test('pre-aborted callers do not initialize the model and failed startup is retryable', async () => {
  let starts = 0;
  const manager = createInstalledDecisionManager({ identity, factory: async () => {
    starts++; if (starts === 1) throw new Error('missing local artifact'); return owner();
  } });
  try {
    const cancel = new AbortController(); cancel.abort(new Error('already cancelled'));
    await expect(manager.get({ signal: cancel.signal })).rejects.toThrow('already cancelled');
    expect(starts).toBe(0);
    await expect(manager.get()).rejects.toThrow('missing local artifact');
    expect(await manager.get()).toBe(manager.evaluator);
    expect(starts).toBe(2);
  } finally { await manager.close(); }
});

test('explicit close joins late startup and old cleanup before admitting a replacement', async () => {
  const ready = deferred(), closed = deferred();
  let starts = 0, closes = 0, firstSignal;
  const manager = createInstalledDecisionManager({ identity, factory: ({ signal }) => {
    starts++; if (starts === 1) { firstSignal = signal; return ready.promise; } return owner();
  } });
  const first = manager.get();
  await Promise.resolve();
  const stop = manager.close();
  expect(firstSignal.aborted).toBe(true);
  const second = manager.get();
  ready.resolve(owner({ close: async () => { closes++; await closed.promise; } }));
  await expect(first).rejects.toThrow('runtime closed');
  await delay(5);
  expect(closes).toBe(1);
  expect(starts).toBe(1);
  closed.resolve();
  await stop;
  expect(await second).toBe(manager.evaluator);
  expect(starts).toBe(2);
  await manager.close();
});

test('idle eviction never closes active execution and a retained descriptor transparently restarts', async () => {
  const result = deferred();
  let starts = 0, closes = 0, calls = 0;
  const manager = createInstalledDecisionManager({ identity, idleMs: 15,
    factory: async () => { starts++; return owner({
      evaluate: async () => { calls++; return calls === 1 ? result.promise : new Float32Array([2]); },
      close: async () => { closes++; },
    }); } });
  try {
    const pending = manager.evaluator.evaluate({ state: {} });
    await delay(40);
    expect(closes).toBe(0);
    result.resolve(new Float32Array([1]));
    expect(Array.from(await pending)).toEqual([1]);
    await delay(40);
    expect(closes).toBe(1);
    expect(Array.from(await manager.evaluator.evaluate({ state: {} }))).toEqual([2]);
    expect(starts).toBe(2);
  } finally { result.resolve(new Float32Array([1])); await manager.close(); }
  expect(closes).toBe(2);
});

test('all cancelled startup waiters still leave the eventual owner eligible for idle eviction', async () => {
  const ready = deferred();
  let closes = 0;
  const manager = createInstalledDecisionManager({ identity, idleMs: 10, factory: () => ready.promise });
  const cancel = new AbortController();
  const pending = manager.get({ signal: cancel.signal });
  cancel.abort(new Error('gone'));
  await expect(pending).rejects.toThrow('gone');
  ready.resolve(owner({ close: async () => { closes++; } }));
  await delay(35);
  expect(closes).toBe(1);
  await manager.close();
});

test('changed passports are refused and failed shutdown cannot silently spawn a second child', async () => {
  let starts = 0, closes = 0;
  const mutable = owner();
  const manager = createInstalledDecisionManager({ identity, factory: async () => { starts++; return mutable; } });
  await manager.get();
  mutable.artifact_digest = 'b'.repeat(64);
  await expect(manager.evaluator.evaluate({})).rejects.toThrow('passport changed');
  await manager.close();
  const invalid = createInstalledDecisionManager({ identity, factory: async () => owner({
    model: 'wrong-owner', close: async () => { closes++; },
  }) });
  await expect(invalid.get()).rejects.toThrow('declared passport');
  expect(closes).toBe(1);
  await invalid.close();
  const failed = createInstalledDecisionManager({ identity, factory: async () => {
    starts++; return owner({ close: async () => { throw new Error('child still alive'); } });
  } });
  await failed.get();
  await expect(failed.close()).rejects.toThrow('child still alive');
  await expect(failed.get()).rejects.toThrow('refusing a second process');
  expect(starts).toBe(2);
});

test('identity mutation during an awaited native call cannot acquire the stable descriptor passport', async () => {
  const pending = deferred();
  const mutable = owner({ evaluate: () => pending.promise });
  const manager = createInstalledDecisionManager({ identity, factory: async () => mutable });
  try {
    await manager.get();
    const call = manager.evaluator.evaluate({});
    await delay(1);
    mutable.model = 'changed-while-running';
    pending.resolve(new Float32Array([9]));
    await expect(call).rejects.toThrow('passport changed');
  } finally { pending.resolve(new Float32Array([9])); await manager.close(); }
});
