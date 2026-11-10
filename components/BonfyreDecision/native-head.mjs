// Clef's learned head as a shared local DecisionPlane evaluator. The backbone
// supplies complete joint-schema activations, not generated answer strings.
import { createHash } from 'node:crypto';
import { createRequire } from 'node:module';
import { readFileSync } from 'node:fs';
import { types } from 'node:util';
import { decisionDigest } from './decision-plane.mjs';
import { serializeDecisionOwner } from './local-inference.mjs';

const require = createRequire(import.meta.url);
// Capture trusted intrinsics at module initialization. User .buffer getters
// must not run while admitting backing stores, nor spoof shared storage.
const typedArrayBuffer = Function.call.bind(Object.getOwnPropertyDescriptor(
  Object.getPrototypeOf(Uint8Array.prototype), 'buffer').get);
const isView = ArrayBuffer.isView, isSharedArrayBuffer = types.isSharedArrayBuffer;
const adapterImplementationDigest = createHash('sha256').update(readFileSync(new URL(import.meta.url))).digest('hex');
const fail = message => { throw new Error(`native decision: ${message}`); };
const digestPattern = /^[a-f0-9]{64}$/;
const typeIds = Object.freeze({ noul: 0, choice: 1, score: 2 });
export function nativeDecisionBinding(path = './native/build/decision_head.node') {
  const resolved = require.resolve(path), binding = require(resolved);
  const ownedBuffers = values => {
    // Bun's napi_is_arraybuffer currently also accepts SharedArrayBuffer.
    // util.types checks the real backing store across realms before native use.
    for (const value of values) if (isView(value) && isSharedArrayBuffer(typedArrayBuffer(value)))
      fail('invalid typed input: shared backing store');
  };
  return Object.freeze({ layout: binding.layout,
    compile(config, weights) { ownedBuffers([weights]); return binding.compile(config, weights); },
    workspace: binding.workspace,
    run(handle, ...buffers) { ownedBuffers(buffers); return binding.run(handle, ...buffers); },
    implementation_digest: createHash('sha256').update(readFileSync(resolved)).digest('hex') });
}

// Exact tensor names/shapes come from the native layout owner, not a parallel
// JS schema. Import once; the addon snapshots immutable F32 weights.
export function compileNativeDecisionHead(config, tensors, {
  binding = nativeDecisionBinding(), maxWeightBytes = 2 ** 30,
} = {}) {
  if (!Number.isSafeInteger(maxWeightBytes) || maxWeightBytes < 4 || !digestPattern.test(binding.implementation_digest))
    fail('invalid weight budget or native implementation identity');
  const layout = binding.layout(config), last = layout.at(-1);
  const count = last.offset + last.rows * last.columns;
  if (!Number.isSafeInteger(count) || count * 4 > maxWeightBytes) fail('head exceeds weight budget');
  if (!tensors || typeof tensors !== 'object' || Object.keys(tensors).length !== layout.length)
    fail('head requires every named tensor and no extras');
  const packed = new Float32Array(count);
  const hasher = createHash('sha256').update(decisionDigest(config));
  for (const item of layout) {
    const tensor = Object.hasOwn(tensors, item.name) && tensors[item.name];
    const shape = [item.rows, item.columns].slice(0, item.rank);
    if (!(tensor?.data instanceof Float32Array) || types.isSharedArrayBuffer(tensor.data.buffer) ||
        tensor.data.length !== item.rows * item.columns ||
        !Array.isArray(tensor.shape) || decisionDigest(tensor.shape) !== decisionDigest(shape))
      fail(`tensor shape mismatch: ${item.name}`);
    packed.set(tensor.data, item.offset);
    hasher.update(item.name).update(Buffer.from(tensor.data.buffer, tensor.data.byteOffset, tensor.data.byteLength));
  }
  return Object.freeze({ handle: binding.compile(config, packed), binding,
    config: Object.freeze({ ...config }), artifact_digest: hasher.digest('hex'), weight_bytes: packed.byteLength });
}

// Adapt a native prefill owner, not a text-generation endpoint. The shared
// encoder owns all schema spans. Only option-token output embeddings are
// gathered; never materialize a [sequence,vocabulary] language-model logit head.
export function createNativeDecisionBackbone({ encoder, runtime, maxTokens = 16384,
  maxActivationBytes = 512 * 2 ** 20 }) {
  if (typeof encoder?.encodeRecord !== 'function' || !digestPattern.test(encoder.encoder_digest || '') ||
      typeof runtime?.forward !== 'function' || typeof runtime?.gatherOutputEmbeddings !== 'function' ||
      !digestPattern.test(runtime.artifact_digest || '') || encoder.artifact_digest !== runtime.artifact_digest ||
      !Number.isSafeInteger(runtime.hidden_size) || runtime.hidden_size < 1 ||
      !Number.isSafeInteger(maxTokens) || maxTokens < 1 ||
      !Number.isSafeInteger(maxActivationBytes) || maxActivationBytes < 4)
    fail('matching pinned encoder and native prefill/output-embedding owner required');
  const encode = encoder.encodeRecord.bind(encoder), forward = runtime.forward.bind(runtime);
  const gather = runtime.gatherOutputEmbeddings.bind(runtime), hiddenSize = runtime.hidden_size;
  const artifact_digest = runtime.artifact_digest, encoder_digest = encoder.encoder_digest;
  const deterministic = encoder.deterministic === true && runtime.deterministic === true;
  const floats = (value, count) => {
    if (!(value instanceof Float32Array) || value.length !== count || types.isSharedArrayBuffer(value.buffer) ||
        value.some(x => !Number.isFinite(x))) fail('prefill owner returned invalid F32 activations');
    return value.slice();
  };
  const invoke = async (fn, args, count) => {
    const result = fn(args);
    return floats(result && typeof result.then === 'function' ? await result : result, count);
  };
  return Object.freeze({ artifact_digest, encoder_digest, deterministic, hidden_size: hiddenSize,
    encodeAndForward({ state, questions, request_digest, signal }) {
      return serializeDecisionOwner(runtime, async () => {
      signal?.throwIfAborted();
      if (runtime.artifact_digest !== artifact_digest || encoder.encoder_digest !== encoder_digest)
        fail('prefill owner identity changed');
      if (request_digest !== decisionDigest({ state, questions })) fail('prefill request identity mismatch');
      const record = encode({ state, questions });
      if (record.truncated !== false || record.media !== null || record.encoder_digest !== encoder_digest ||
          !Array.isArray(record.input_ids) || !record.input_ids.length || record.input_ids.length > maxTokens ||
          record.input_ids.some(id => !Number.isSafeInteger(id) || id < 0 || id > 0x7fffffff) ||
          !Array.isArray(record.questions) || !record.questions.length)
        fail('encoder must return complete bounded text tokens and fields');
      const tokens = Uint32Array.from(record.input_ids), tokenSlots = new Map();
      const fields = record.questions.map(field => {
        const type = ['noul', 'choice', 'score'][field.question_type];
        if (!type || !Array.isArray(field.option_spans) || !Array.isArray(field.option_ids) ||
            field.option_spans.length !== field.option_ids.length || !field.option_ids.length ||
            !Array.isArray(field.question_span) || field.question_span.length !== 2 ||
            !field.question_span.every(Number.isSafeInteger) || field.question_span[0] < 0 ||
            field.question_span[1] <= field.question_span[0] || field.question_span[1] > tokens.length)
          fail('encoder returned invalid field options');
        const options = field.option_spans.map((span, i) => {
          if (!Array.isArray(span) || span.length !== 2 || !span.every(Number.isSafeInteger) ||
              span[0] < 0 || span[1] <= span[0] || span[1] > tokens.length)
            fail('encoder returned invalid option span');
          for (let token = span[0]; token < span[1]; token++)
            if (!tokenSlots.has(tokens[token])) tokenSlots.set(tokens[token], tokenSlots.size);
          return { id: field.option_ids[i], span: [...span] };
        });
        return { id: field.question_id, type, question_span: [...field.question_span], options };
      });
      const optionCount = fields.reduce((sum, field) => sum + field.options.length, 0);
      // Includes owned snapshots plus producer arrays; excludes model weights,
      // backend-internal scratch and JS metadata (this is not an RSS sandbox).
      const elements = (2 * (tokens.length + tokenSlots.size) + optionCount) * hiddenSize;
      if (!Number.isSafeInteger(elements) || elements * 4 > maxActivationBytes)
        fail('prefill activations exceed admission budget');
      const hidden = await invoke(forward, { input_ids: tokens.slice(), signal }, tokens.length * hiddenSize);
      signal?.throwIfAborted();
      const embeddings = await invoke(gather, { token_ids: Uint32Array.from(tokenSlots.keys()), signal },
        tokenSlots.size * hiddenSize);
      signal?.throwIfAborted();
      if (runtime.artifact_digest !== artifact_digest || encoder.encoder_digest !== encoder_digest)
        fail('prefill owner identity changed');
      const lexical_options = new Float32Array(optionCount * hiddenSize);
      let offset = 0;
      for (const field of fields) for (const { span: [start, end] } of field.options) {
        for (let column = 0; column < hiddenSize; column++) {
          let sum = 0;
          for (let token = start; token < end; token++) sum += embeddings[tokenSlots.get(tokens[token]) * hiddenSize + column];
          lexical_options[offset++] = sum / (end - start);
        }
      }
      return { request_digest, truncated: false, backbone_artifact_digest: artifact_digest,
        encoder_digest, fields, hidden, lexical_options };
      });
    } });
}

export function createNativeDecisionEvaluator({ model, head, backbone, maxWorkspaceBytes = 256 * 2 ** 20,
  maxTokens = 16384 }) {
  if (typeof model !== 'string' || !model || !head?.handle || !digestPattern.test(head.artifact_digest) ||
      typeof backbone?.encodeAndForward !== 'function' || !digestPattern.test(backbone.artifact_digest) ||
      !digestPattern.test(backbone.encoder_digest) || !Number.isSafeInteger(maxWorkspaceBytes) || maxWorkspaceBytes < 4 ||
      !Number.isSafeInteger(maxTokens) || maxTokens < 1 || maxTokens > 0x7fffffff)
    fail('pinned head, backbone, encoder and positive workspace budget required');
  backbone = Object.freeze({ artifact_digest: backbone.artifact_digest, deterministic: backbone.deterministic === true,
    encoder_digest: backbone.encoder_digest, encodeAndForward: backbone.encodeAndForward.bind(backbone) });
  const artifact_digest = decisionDigest({ head: head.artifact_digest, backbone: backbone.artifact_digest,
    encoder: backbone.encoder_digest, implementation: head.binding.implementation_digest,
    adapter_implementation: adapterImplementationDigest,
    adapter: 'bonfyre-clef-head-f32-v1' });
  // One cached arena per evaluator. Native run is synchronous; concurrent
  // backbone awaits cannot overlap its use. Workers need their own evaluator.
  let workspace = new Float32Array(0);
  return Object.freeze({ model, artifact_digest, question_semantics: 'joint_schema', deterministic: backbone.deterministic,
    async evaluate({ state, questions, schema, signal }) {
      signal?.throwIfAborted();
      const request_digest = decisionDigest({ state, questions });
      const record = await backbone.encodeAndForward({ state, questions, request_digest, signal });
      signal?.throwIfAborted();
      if (record?.request_digest !== request_digest || record?.truncated !== false ||
          record?.backbone_artifact_digest !== backbone.artifact_digest ||
          record?.encoder_digest !== backbone.encoder_digest || !Array.isArray(record.fields) ||
          record.fields.length !== schema.fields.length)
        fail('activations must bind the complete request, backbone, encoder and fields');
      if (!(record.hidden instanceof Float32Array) || !record.hidden.length ||
          record.hidden.length % head.config.hidden_size || !(record.lexical_options instanceof Float32Array))
        fail('hidden and pooled output-embedding tensors must have valid F32 shapes');
      if (record.hidden.length / head.config.hidden_size > maxTokens) fail('head exceeds token budget');
      const fieldWords = [], optionWords = [], permutation = [];
      const seen = new Set();
      const integer = n => Number.isInteger(n) && n >= 0 && n <= 0xffffffff;
      const span = value => Array.isArray(value) && value.length === 2 && value.every(integer);
      let offset = 0;
      for (const field of record.fields) {
        const expected = schema.fields.find(f => f.id === field.id);
        if (!expected || seen.has(field.id) || field.type !== expected.type || !span(field.question_span) ||
            !Array.isArray(field.options) || field.options.length !== expected.options.length)
          fail('encoded field does not match the typed schema');
        seen.add(field.id);
        const options = new Set();
        for (const option of field.options) {
          const index = expected.options.indexOf(option.id);
          if (index < 0 || options.has(option.id) || !span(option.span)) fail('encoded option does not match typed schema');
          options.add(option.id); optionWords.push(...option.span); permutation.push(expected.offset + index);
        }
        fieldWords.push(typeIds[field.type], ...field.question_span, offset, field.options.length);
        offset += field.options.length;
      }
      const fields = Uint32Array.from(fieldWords), spans = Uint32Array.from(optionWords);
      const dimensions = { tokens: record.hidden.length / head.config.hidden_size,
        fields: record.fields.length, options: offset };
      const needed = head.binding.workspace(head.handle, dimensions);
      if (needed * 4 > maxWorkspaceBytes) fail('head exceeds workspace budget');
      if (needed > workspace.length) workspace = new Float32Array(needed);
      const native = head.binding.run(head.handle, record.hidden, fields, spans, record.lexical_options, workspace);
      const logits = new Float32Array(schema.width);
      for (let i = 0; i < native.length; i++) logits[permutation[i]] = native[i];
      signal?.throwIfAborted();
      return logits;
    },
  });
}
