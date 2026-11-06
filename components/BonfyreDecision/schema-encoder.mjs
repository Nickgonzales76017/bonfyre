// Text-only encode_record from Cloudflare/clef-flash joint_schema_model.py at
// 17f0b0ad64efb65d273590632833508766b2aae6. This prepares token IDs and
// spans; it is not a tokenizer, multimodal processor, or learned backbone.
import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

export const CLEF_FLASH_SOURCE_REVISION = '17f0b0ad64efb65d273590632833508766b2aae6';
export const CLEF_FLASH_SYSTEM_PROMPT =
  'Read the complete state and schema. Decide every field jointly. Each answer ' +
  "must be exactly one of that field's allowed options.";

const QUESTION_TYPES = Object.freeze({ noul: 0, choice: 1, score: 2 });
const NOUL_OPTIONS = Object.freeze({
  true: 'The proposition is true or the answer is yes.',
  false: 'The proposition is false or the answer is no.',
});
const DIGEST = /^[a-f0-9]{64}$/;
const fail = message => { throw new Error(`Clef text encoder: ${message}`); };
const hash = value => createHash('sha256').update(value).digest('hex');
const implementationDigest = hash(readFileSync(fileURLToPath(import.meta.url)));
const own = (value, key) => Object.hasOwn(value, key);
const plain = value => value !== null && typeof value === 'object' &&
  (Object.getPrototypeOf(value) === Object.prototype || Object.getPrototypeOf(value) === null);
function dataField(value, key) {
  const descriptor = Object.getOwnPropertyDescriptor(value, key);
  if (descriptor && !own(descriptor, 'value')) fail('accessor is not a JSON field');
  return descriptor?.value;
}

function codepointCompare(a, b) {
  const left = Array.from(a, char => char.codePointAt(0));
  const right = Array.from(b, char => char.codePointAt(0));
  for (let index = 0; index < Math.min(left.length, right.length); index++) {
    if (left[index] !== right[index]) return left[index] - right[index];
  }
  return left.length - right.length;
}

function quoted(value) {
  for (let index = 0; index < value.length; index++) {
    const unit = value.charCodeAt(index);
    if (unit >= 0xd800 && unit <= 0xdbff) {
      if (index + 1 >= value.length || value.charCodeAt(index + 1) < 0xdc00 ||
          value.charCodeAt(index + 1) > 0xdfff) fail('unpaired surrogate cannot match Python UTF-8 rendering');
      index++;
    } else if (unit >= 0xdc00 && unit <= 0xdfff) {
      fail('unpaired surrogate cannot match Python UTF-8 rendering');
    }
  }
  return JSON.stringify(value);
}

// Python json.dumps(..., ensure_ascii=False, separators=(',', ':'),
// sort_keys=True). JS loses float lexemes (including 1.0 and -0.0) on parse,
// so this boundary admits only safe integer Numbers. Original numeric spellings
// already erased by an upstream JSON.parse cannot be reconstructed here.
export function pythonJson(value, { maxBytes = 2 ** 20, maxDepth = 64 } = {}) {
  if (!Number.isSafeInteger(maxBytes) || maxBytes < 1 ||
      !Number.isSafeInteger(maxDepth) || maxDepth < 1) fail('JSON budgets must be positive integers');
  const seen = new Set();
  const chunks = [];
  let bytes = 0;
  const emit = text => {
    bytes += Buffer.byteLength(text, 'utf8');
    if (bytes > maxBytes) fail('rendered JSON exceeds byte budget');
    chunks.push(text);
  };
  const emitQuoted = text => {
    if (Buffer.byteLength(text, 'utf8') + 2 > maxBytes - bytes)
      fail('rendered JSON exceeds byte budget');
    emit(quoted(text));
  };
  const render = (item, depth) => {
    if (depth > maxDepth) fail('JSON nesting exceeds depth budget');
    if (item === null) { emit('null'); return; }
    if (typeof item === 'string') { emitQuoted(item); return; }
    if (typeof item === 'boolean') { emit(item ? 'true' : 'false'); return; }
    if (typeof item === 'number') {
      if (!Number.isSafeInteger(item) || Object.is(item, -0))
        fail('non-integer, unsafe, or negative-zero JSON number has no assured Python spelling');
      emit(String(item)); return;
    }
    if (Array.isArray(item)) {
      if (seen.has(item)) fail('cyclic JSON state');
      seen.add(item);
      emit('[');
      for (let index = 0; index < item.length; index++) {
        if (!own(item, index)) fail('sparse arrays are not JSON values');
        if (index) emit(',');
        render(item[index], depth + 1);
      }
      seen.delete(item);
      emit(']'); return;
    }
    if (!plain(item)) fail('non-JSON state value');
    if (seen.has(item)) fail('cyclic JSON state');
    seen.add(item);
    emit('{');
    for (const [index, key] of Object.keys(item).sort(codepointCompare).entries()) {
      if (index) emit(',');
      emitQuoted(key); emit(':'); render(dataField(item, key), depth + 1);
    }
    seen.delete(item);
    emit('}');
  };
  render(value, 0);
  return chunks.join('');
}

function upstreamRender(value, maxBytes) {
  if (typeof value === 'string') {
    if (Buffer.byteLength(value, 'utf8') > maxBytes) fail('rendered string exceeds byte budget');
    quoted(value); // Validate the same Unicode boundary as nested strings.
    return value;
  }
  return pythonJson(value, { maxBytes });
}

function questionOptions(question) {
  const type = dataField(question, 'type'), criteria = dataField(question, 'criteria');
  if (type === 'noul') {
    if (criteria !== undefined && criteria !== null && !plain(criteria))
      fail('noul criteria must be an object');
    const given = criteria || {};
    if (Object.keys(given).some(key => key !== 'true' && key !== 'false'))
      fail('noul criteria contain an ignored option');
    return [['true', own(given, 'true') ? dataField(given, 'true') : NOUL_OPTIONS.true],
      ['false', own(given, 'false') ? dataField(given, 'false') : NOUL_OPTIONS.false]];
  }
  if (type === 'choice') {
    if (!plain(criteria) || !Object.keys(criteria).length)
      fail('choice requires nonempty criteria');
    return Object.keys(criteria).sort(codepointCompare).map(key => [key, dataField(criteria, key)]);
  }
  if (!Array.isArray(criteria) || !criteria.length)
    fail('score requires nonempty ordered criteria');
  return Array.from({ length: criteria.length }, (_, index) => {
    if (!own(criteria, index)) fail('sparse score criteria are not JSON values');
    return [String(index), dataField(criteria, index)];
  });
}

export function createClefTextEncoder({ tokenizer, artifactDigest, tokenizerDigest,
  maxLength = 16384, maxStateTokens = null, maxInputBytes = 2 ** 20,
  maxQuestions = 64, maxOptions = 255,
  maxCompiledSchemas = 64, maxCompiledSchemaBytes = 2 ** 20 } = {}) {
  if (typeof tokenizer?.encode !== 'function' || !DIGEST.test(artifactDigest || '') ||
      !DIGEST.test(tokenizerDigest || '')) fail('pinned artifact, tokenizer digest, and encode() are required');
  const encodeToken = tokenizer.encode.bind(tokenizer);
  const deterministic = tokenizer.deterministic === true;
  for (const [name, value] of Object.entries({ maxLength, maxInputBytes, maxQuestions, maxOptions })) {
    if (!Number.isSafeInteger(value) || value < 1) fail(`${name} must be a positive integer`);
  }
  if (maxStateTokens !== null && (!Number.isSafeInteger(maxStateTokens) || maxStateTokens < 1))
    fail('maxStateTokens must be null or a positive integer');
  for (const [name, value] of Object.entries({ maxCompiledSchemas, maxCompiledSchemaBytes }))
    if (!Number.isSafeInteger(value) || value < 0) fail(`${name} must be a nonnegative integer`);
  const encoderDigest = hash(`${CLEF_FLASH_SOURCE_REVISION}\0${implementationDigest}\0${artifactDigest}\0${tokenizerDigest}`);
  // Reusable token programs, not inferred answers. Field order is significant:
  // Clef's complete schema participates in the backbone and joint head. State
  // tokens are never resident here. The byte budget charges serialized program
  // payloads (not JS heap overhead); the entry bound also limits metadata.
  const programs = new Map();
  let programBytes = 0, compiled = 0, reused = 0;

  function encodeRecord(record) {
    if (!plain(record) || !own(record, 'state'))
      fail('record requires state and 1..maxQuestions ordered questions');
    const questionsInput = dataField(record, 'questions');
    if (!plain(questionsInput) || !Object.keys(questionsInput).length ||
        Object.keys(questionsInput).length > maxQuestions)
      fail('record requires state and 1..maxQuestions ordered questions');
    if (['images', 'videos', 'media_kwargs'].some(key => own(record, key)))
      fail('media and media options require the upstream processor; text-only encoding refused');
    const recordId = dataField(record, 'id');
    if (own(record, 'id') && typeof recordId !== 'string')
      fail('record id must be a string to preserve Python spelling');
    if (recordId !== undefined) quoted(recordId);
    // JS enumerates canonical array-index keys before other keys, unlike
    // Python's insertion-ordered dict. Refuse rather than reorder fields.
    const questionIds = Object.keys(questionsInput);
    if (questionIds.some(id => /^(0|[1-9]\d*)$/.test(id) && Number(id) < 0xffffffff))
      fail('integer-index question IDs cannot preserve Python field order');

    const canReuse = deterministic && maxCompiledSchemas > 0 && maxCompiledSchemaBytes > 0;
    // Capture exactly the consumed schema once. Tokenizers are caller-supplied
    // and may mutate the original rubric during encode(); compiling from live
    // fields after hashing them could retain B's program under A's cache key.
    // The ordered array preserves Python field order; canonical JSON owns the
    // nested criteria and rejects accessors even on warm hits.
    const capturedSchema = pythonJson(questionIds.map(id => {
      const question = dataField(questionsInput, id);
      if (!plain(question)) fail('question requires an ID and noul, choice, or score type');
      return [id, dataField(question, 'type') ?? null,
        dataField(question, 'instructions') ?? null, dataField(question, 'criteria') ?? null];
    }), { maxBytes: maxInputBytes });
    const signature = canReuse ? hash(capturedSchema) : null;
    const resident = signature === null ? undefined : programs.get(signature);
    const capturedQuestions = resident ? null : JSON.parse(capturedSchema).map(
      ([id, type, instructions, criteria]) => [id, { type, instructions, criteria }]);

    let inputBytes = 0, tokenCount = 0;
    const tokens = (value, allowEmpty = false) => {
      inputBytes += Buffer.byteLength(value, 'utf8');
      if (inputBytes > maxInputBytes) fail('rendered input exceeds byte budget');
      const result = encodeToken(value, { add_special_tokens: false });
      if ((!Array.isArray(result) && !ArrayBuffer.isView(result)) ||
          !Number.isSafeInteger(result.length) || result.length < 0)
        fail('tokenizer.encode must return token IDs');
      if (result.length > maxLength - tokenCount) fail('token budget exceeded (no truncation)');
      const ids = [];
      for (const id of result) {
        if (!Number.isSafeInteger(id) || id < 0 || id > 0x7fffffff)
          fail('segment produced invalid token IDs');
        ids.push(id);
      }
      if (!allowEmpty && !ids.length)
        fail('segment produced empty or invalid token IDs');
      tokenCount += ids.length;
      return ids;
    };
    const append = (target, value) => {
      for (const id of tokens(value)) target.push(id);
    };

    const compile = () => {
      const schemaIds = tokens('\n\nSCHEMA FIELDS:\n'), questions = [];
      for (const [index, [questionId, question]] of capturedQuestions.entries()) {
        const questionType = question.type;
        if (!questionId || !plain(question) || !own(QUESTION_TYPES, questionType))
          fail('question requires an ID and noul, choice, or score type');
        quoted(questionId);
        append(schemaIds, `\nFIELD ${index + 1}\nID: ${questionId}\nTYPE: ${questionType}\nINSTRUCTION: `);
        const questionStart = schemaIds.length;
        const rawInstructions = question.instructions;
        const instructions = rawInstructions === undefined || rawInstructions === null ||
          rawInstructions === '' ? questionId : rawInstructions;
        append(schemaIds, upstreamRender(instructions, maxInputBytes - inputBytes));
        const questionEnd = schemaIds.length;
        if (questionEnd === questionStart) fail(`${questionId}: empty question span`);
        append(schemaIds, '\nALLOWED OPTIONS:\n');
        const options = questionOptions(question);
        if (options.length > maxOptions) fail(`${questionId}: option budget exceeded`);
        const optionSpans = [], optionIds = [];
        for (const [optionIndex, [optionId, description]] of options.entries()) {
          append(schemaIds, `OPTION ${optionIndex + 1}: `);
          const start = schemaIds.length;
          const semantics = { option_id: optionId };
          if (description !== null) semantics.description = description;
          append(schemaIds, upstreamRender(semantics, maxInputBytes - inputBytes));
          if (schemaIds.length === start) fail(`${questionId}: empty option span`);
          optionSpans.push([start, schemaIds.length]); optionIds.push(optionId);
          append(schemaIds, '\n');
        }
        append(schemaIds, 'END FIELD\n');
        questions.push({ question_id: questionId, question_type: QUESTION_TYPES[questionType],
          question_span: [questionStart, questionEnd], option_spans: optionSpans,
          option_ids: optionIds });
      }

      const prefix = tokens(`<|im_start|>system\n${CLEF_FLASH_SYSTEM_PROMPT}<|im_end|>\n<|im_start|>user\nSTATE:\n`);
      const suffix = tokens('\n<|im_end|>\n<|im_start|>assistant\n<think>\n\n</think>\n\nJOINT SCHEMA DECISIONS:');
      return { schemaIds, questions, prefix, suffix, inputBytes, tokenCount };
    };
    const program = resident?.program ?? compile();
    const { schemaIds, questions, prefix, suffix } = program;
    inputBytes = program.inputBytes; tokenCount = program.tokenCount;
    const state = tokens(upstreamRender(dataField(record, 'state'), maxInputBytes - inputBytes), true);
    if (maxStateTokens !== null && state.length > maxStateTokens)
      fail(`state requires ${state.length} tokens; maximum is ${maxStateTokens} (no truncation)`);
    const fixedLength = prefix.length + schemaIds.length + suffix.length;
    if (fixedLength > maxLength)
      fail(`schema requires ${fixedLength} tokens before state; maximum is ${maxLength}`);
    if (fixedLength + state.length > maxLength)
      fail(`record requires ${fixedLength + state.length} tokens; maximum is ${maxLength} (no truncation)`);
    const offset = prefix.length + state.length;
    const shifted = questions.map(question => ({ ...question,
      option_ids: [...question.option_ids],
      question_span: question.question_span.map(value => value + offset),
      option_spans: question.option_spans.map(span => span.map(value => value + offset)) }));
    // Publish only after the entire record passes admission. Failed or
    // oversized states cannot fill residency, nor bypass warm token budgets.
    if (resident) {
      reused++;
      programs.delete(signature); programs.set(signature, resident);
    } else {
      compiled++;
      if (canReuse) {
        const bytes = Buffer.byteLength(JSON.stringify(program), 'utf8');
        if (bytes <= maxCompiledSchemaBytes) {
          while (programs.size >= maxCompiledSchemas || programBytes + bytes > maxCompiledSchemaBytes) {
            const oldest = programs.keys().next().value;
            programBytes -= programs.get(oldest).bytes; programs.delete(oldest);
          }
          programs.set(signature, { program, bytes }); programBytes += bytes;
        }
      }
    }
    return { input_ids: prefix.concat(state, schemaIds, suffix), questions: shifted,
      record_id: recordId ?? 'unknown', media: null, truncated: false,
      artifact_digest: artifactDigest, tokenizer_digest: tokenizerDigest, encoder_digest: encoderDigest };
  }

  return Object.freeze({ artifact_digest: artifactDigest, tokenizer_digest: tokenizerDigest,
    implementation_digest: implementationDigest, encoder_digest: encoderDigest, deterministic,
    upstream_revision: CLEF_FLASH_SOURCE_REVISION, encodeRecord,
    residency: () => Object.freeze({ compiled, reused, entries: programs.size, program_bytes: programBytes,
      retained_state_bytes: 0 }),
    clearResidency() { programs.clear(); programBytes = 0; },
  });
}
