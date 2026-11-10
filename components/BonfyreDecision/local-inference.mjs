// Derived inference, not cached decisions: every caller still recompiles its
// current owner contract and projections. No state text, credentials, receipts,
// authority grants, failures or mutable hosted aliases are retained here.
import { isDeepStrictEqual, types } from 'node:util';
import { answersFromLogits, decodeDecisionAnswers } from './typed-decision.mjs';

const caches = new WeakMap();
const lanes = new WeakMap();

// A backend may recycle its scratch/output. Keep its lane until the result has
// been snapshotted, even if its caller aborts. Different owners still run in
// parallel. Cancellation is cooperative, not a preemption of native execution.
export function serializeDecisionOwner(owner, task) {
  const previous = lanes.get(owner);
  const work = previous ? previous.then(task) : task();
  const tail = Promise.resolve(work).then(() => undefined, () => undefined);
  lanes.set(owner, tail);
  void tail.then(() => { if (lanes.get(owner) === tail) lanes.delete(owner); });
  return work;
}

export function clearLocalDecisionInference(local) { caches.delete(local); }

export function evaluateLocalDecision(local, request, options) {
  const identity = [local.model, local.artifact_digest, local.question_semantics, local.confidence_semantics, local.deterministic, local.evaluate];
  // Capture the rubric before yielding or calling an owner that receives the
  // request object. A returned legend cannot redefine its own validation basis.
  const scoreCriteria = new Map(request.schema.fields.filter(field => field.type === 'score')
    .map(field => [field.id, structuredClone(request.questions[field.id].criteria)]));
  return serializeDecisionOwner(local, () => evaluateOwned(local, request, options, identity, scoreCriteria));
}

function snapshotAnswers(schema, source, scoreCriteria) {
  // A local SystemOne server returns probabilities, which may include exact
  // zero. Never round-trip them through log(p) or synthetic finite logits.
  const captured = structuredClone(source);
  decodeDecisionAnswers(schema, captured);
  return Object.fromEntries(schema.fields.map(field => {
    const answer = captured[field.id];
    if (field.type === 'score' && answer.legend !== undefined) {
      const legend = answer.legend, criteria = scoreCriteria.get(field.id);
      if (!legend || typeof legend !== 'object' || Array.isArray(legend) ||
          Object.keys(legend).length !== criteria.length || criteria.some((criterion, index) =>
            !Object.hasOwn(legend, String(index)) || !isDeepStrictEqual(legend[String(index)], criterion)))
        throw new Error(`${field.id}: score legend disagrees with the authored criteria`);
    }
    if (field.type === 'noul') return [field.id, { type: 'noul', noul: answer.noul }];
    const probabilities = Object.fromEntries(field.options.map(option => [option, answer.probabilities[option]]));
    return [field.id, field.type === 'choice'
      ? { type: 'choice', choice: answer.choice, confidence: answer.confidence, probabilities }
      : { type: 'score', score: answer.score, confidence: answer.confidence, probabilities }];
  }));
}

async function evaluateOwned(local, request, {
  key, signal, max_entries, max_logit_bytes,
}, identity, scoreCriteria) {
  if (!Number.isSafeInteger(max_entries) || max_entries < 0 ||
      !Number.isSafeInteger(max_logit_bytes) || max_logit_bytes < 0 ||
      !/^[a-f0-9]{64}$/.test(key)) throw new Error('invalid local inference residency budget or identity');
  signal?.throwIfAborted();
  const unchanged = () => {
    if ([local.model, local.artifact_digest, local.question_semantics, local.confidence_semantics, local.deterministic, local.evaluate]
      .some((value, index) => value !== identity[index]))
      throw new Error('local inference owner changed during execution');
  };
  unchanged();
  // An artifact hash alone does not promise deterministic inference (dropout,
  // sampling and time-dependent owners exist). Unknown owners always execute.
  const enabled = local.deterministic === true && max_entries > 0 && max_logit_bytes > 0;
  let cache;
  if (enabled) {
    cache = caches.get(local);
    if (!cache || cache.evaluate !== local.evaluate || cache.max_entries !== max_entries ||
        cache.max_logit_bytes !== max_logit_bytes) {
      cache = { evaluate: local.evaluate, max_entries, max_logit_bytes, entries: new Map(), bytes: 0 };
      caches.set(local, cache);
    }
    const hit = cache.entries.get(key);
    if (hit) {
      cache.entries.delete(key); cache.entries.set(key, hit);
      return { answers: hit.answers ? structuredClone(hit.answers) : answersFromLogits(request.schema, hit.logits),
        inference: { state: 'reused', producer_ms: hit.producer_ms, executed: false } };
    }
  } else caches.delete(local);
  const started = performance.now();
  const result = local.evaluate({ ...request, signal });
  const value = result && typeof result.then === 'function' ? await result : result;
  signal?.throwIfAborted();
  unchanged();
  let owned;
  if (value && typeof value === 'object' && Object.hasOwn(value, 'answers')) {
    if (Object.keys(value).length !== 1) throw new Error('local typed result must contain only answers');
    owned = { answers: snapshotAnswers(request.schema, value.answers, scoreCriteria) };
  } else {
    if (!(value instanceof Float32Array || value instanceof Float64Array) ||
        types.isSharedArrayBuffer(value.buffer) || value.length !== request.schema.width)
      throw new Error('local logits require an owned floating-point buffer matching the schema');
    // Own the snapshot: a backbone may recycle its output on the next invocation.
    owned = { logits: value.slice() };
  }
  const answers = owned.answers ? structuredClone(owned.answers) : answersFromLogits(request.schema, owned.logits);
  const retainedBytes = owned.answers ? Buffer.byteLength(JSON.stringify(owned.answers), 'utf8') : owned.logits.byteLength;
  const producer_ms = performance.now() - started;
  // Clear/reconfiguration during an awaited inference must not resurrect data.
  if (enabled && caches.get(local) === cache && local.evaluate === cache.evaluate &&
      retainedBytes <= max_logit_bytes) {
    const old = cache.entries.get(key);
    if (old) { cache.bytes -= old.bytes; cache.entries.delete(key); }
    while (cache.entries.size >= max_entries || cache.bytes + retainedBytes > max_logit_bytes) {
      const first = cache.entries.keys().next().value;
      cache.bytes -= cache.entries.get(first).bytes; cache.entries.delete(first);
    }
    cache.entries.set(key, { ...owned, bytes: retainedBytes, producer_ms }); cache.bytes += retainedBytes;
  }
  return { answers, inference: { state: enabled ? 'computed' : 'uncached', producer_ms, executed: true } };
}
