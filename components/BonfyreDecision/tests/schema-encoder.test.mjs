import { expect, test } from 'bun:test';
import { createClefTextEncoder, pythonJson, CLEF_FLASH_SYSTEM_PROMPT,
  CLEF_FLASH_SOURCE_REVISION } from '../schema-encoder.mjs';

const artifactDigest = 'a'.repeat(64);
const tokenizerDigest = 'b'.repeat(64);
const prefix = `<|im_start|>system\n${CLEF_FLASH_SYSTEM_PROMPT}<|im_end|>\n<|im_start|>user\nSTATE:\n`;
const suffix = '\n<|im_end|>\n<|im_start|>assistant\n<think>\n\n</think>\n\nJOINT SCHEMA DECISIONS:';

function byteTokenizer(calls = []) {
  return { deterministic: true, encode(text, options) {
    calls.push({ text, options });
    return [...Buffer.from(text, 'utf8')];
  } };
}
function encoder(tokenizer = byteTokenizer(), options = {}) {
  return createClefTextEncoder({ tokenizer, artifactDigest, tokenizerDigest, ...options });
}
const decode = ids => Buffer.from(ids).toString('utf8');

test('pinned upstream text-only segment order, templates, and full-input spans', () => {
  const calls = [], e = encoder(byteTokenizer(calls));
  const result = e.encodeRecord({ id: 'case-1', state: { z: 1, a: 'é', arr: [true, null] },
    questions: {
      urgent: { type: 'noul', instructions: 'Is this urgent?' },
      route: { type: 'choice', instructions: 'Route it',
        criteria: { '🪼': 'jelly', '\uE000': 'private', alpha: 'first' } },
      severity: { type: 'score', criteria: ['low', null, 3] },
    } });
  const segments = [
    '\n\nSCHEMA FIELDS:\n',
    '\nFIELD 1\nID: urgent\nTYPE: noul\nINSTRUCTION: ', 'Is this urgent?',
    '\nALLOWED OPTIONS:\n',
    'OPTION 1: ', '{"description":"The proposition is true or the answer is yes.","option_id":"true"}', '\n',
    'OPTION 2: ', '{"description":"The proposition is false or the answer is no.","option_id":"false"}', '\n',
    'END FIELD\n',
    '\nFIELD 2\nID: route\nTYPE: choice\nINSTRUCTION: ', 'Route it',
    '\nALLOWED OPTIONS:\n',
    'OPTION 1: ', '{"description":"first","option_id":"alpha"}', '\n',
    'OPTION 2: ', '{"description":"private","option_id":"\uE000"}', '\n',
    'OPTION 3: ', '{"description":"jelly","option_id":"🪼"}', '\n',
    'END FIELD\n',
    '\nFIELD 3\nID: severity\nTYPE: score\nINSTRUCTION: ', 'severity',
    '\nALLOWED OPTIONS:\n',
    'OPTION 1: ', '{"description":"low","option_id":"0"}', '\n',
    'OPTION 2: ', '{"option_id":"1"}', '\n',
    'OPTION 3: ', '{"description":3,"option_id":"2"}', '\n',
    'END FIELD\n', prefix, suffix, '{"a":"é","arr":[true,null],"z":1}',
  ];
  expect(calls.map(call => call.text)).toEqual(segments);
  expect(calls.every(call => call.options.add_special_tokens === false)).toBe(true);
  const state = segments.at(-1), schema = segments.slice(0, -3).join('');
  expect(decode(result.input_ids)).toBe(prefix + state + schema + suffix);
  expect(result.record_id).toBe('case-1');
  expect(result.media).toBeNull();
  expect(result.truncated).toBe(false);
  expect(result.questions.map(q => q.question_type)).toEqual([0, 1, 2]);
  expect(result.questions.map(q => q.option_ids)).toEqual([
    ['true', 'false'], ['alpha', '\uE000', '🪼'], ['0', '1', '2'],
  ]);
  expect(result.questions.map(q => decode(result.input_ids.slice(...q.question_span)))).toEqual([
    'Is this urgent?', 'Route it', 'severity',
  ]);
  expect(result.questions.map(q => q.option_spans.map(span => decode(result.input_ids.slice(...span))))).toEqual([
    segments.slice(5, 11).filter((_, i) => i % 3 === 0),
    ['{"description":"first","option_id":"alpha"}',
      '{"description":"private","option_id":"\uE000"}',
      '{"description":"jelly","option_id":"🪼"}'],
    ['{"description":"low","option_id":"0"}', '{"option_id":"1"}',
      '{"description":3,"option_id":"2"}'],
  ]);
  expect(e.upstream_revision).toBe(CLEF_FLASH_SOURCE_REVISION);
  expect(e.deterministic).toBe(true);
  expect(e.artifact_digest).toBe(artifactDigest);
  expect(e.tokenizer_digest).toBe(tokenizerDigest);
  expect(result.encoder_digest).toBe(e.encoder_digest);
});

test('Python key ordering and conservative safe-integer-only JSON contract', () => {
  expect(pythonJson({ '🪼': 1, '\uE000': 2, a: 3 })).toBe('{"a":3,"\uE000":2,"🪼":1}');
  expect(pythonJson({ x: -2, nested: ['é', true, null] }))
    .toBe('{"nested":["é",true,null],"x":-2}');
  for (const number of [1.5, -0, 1e100, Number.NaN, Number.POSITIVE_INFINITY,
    Number.MAX_SAFE_INTEGER + 1]) {
    expect(() => pythonJson({ number })).toThrow('number');
  }
  for (const value of [undefined, 1n, new Date(), /x/])
    expect(() => pythonJson(value)).toThrow();
  const sparse = [0, 1]; delete sparse[0];
  expect(() => pythonJson(sparse)).toThrow('sparse');
  const cyclic = {}; cyclic.self = cyclic;
  expect(() => pythonJson(cyclic)).toThrow('cyclic');
  expect(() => pythonJson({ x: { y: 1 } }, { maxDepth: 1 })).toThrow('depth');
  expect(() => pythonJson({ x: 'long' }, { maxBytes: 5 })).toThrow('byte');
  expect(() => pythonJson('\ud800')).toThrow('surrogate');
});

test('refuses media, unsupported schemas, empty significant spans, and every truncation path', () => {
  const one = { state: 'hello', questions: { q: { type: 'noul' } } };
  const baseline = encoder().encodeRecord(one);
  expect(decode(baseline.input_ids)).toContain('STATE:\nhello\n');
  expect(() => encoder().encodeRecord({ ...one, images: [] })).toThrow('media');
  expect(() => encoder().encodeRecord({ ...one, media_kwargs: {} })).toThrow('media');
  expect(() => encoder().encodeRecord({ ...one, questions: { '0': one.questions.q } })).toThrow('integer-index');
  expect(() => encoder().encodeRecord({ ...one, questions: { '\ud800': one.questions.q } })).toThrow('surrogate');
  expect(() => encoder().encodeRecord({ ...one, questions: { q: { type: 'choice', criteria: {} } } }))
    .toThrow('choice requires');
  expect(() => encoder().encodeRecord({ ...one, questions: { q: { type: 'unknown' } } }))
    .toThrow('question requires');
  expect(() => encoder().encodeRecord({ ...one, questions: { q: { type: 'noul', criteria: { maybe: 1 } } } }))
    .toThrow('ignored option');
  expect(() => encoder().encodeRecord({ ...one, questions: { q: { type: 'score', criteria: [1, 2] } } }))
    .not.toThrow();
  expect(() => encoder(byteTokenizer(), { maxLength: baseline.input_ids.length - 1 }).encodeRecord(one))
    .toThrow('token budget');
  expect(() => encoder(byteTokenizer(), { maxStateTokens: 4 }).encodeRecord(one))
    .toThrow('no truncation');
  expect(() => encoder(byteTokenizer(), { maxInputBytes: 20 }).encodeRecord(one))
    .toThrow('byte budget');
  expect(() => encoder(byteTokenizer(), { maxQuestions: 1 }).encodeRecord({ ...one,
    questions: { q: one.questions.q, r: one.questions.q } })).toThrow('ordered questions');
  expect(() => encoder(byteTokenizer(), { maxOptions: 1 }).encodeRecord(one)).toThrow('option budget');
  const accessor = { type: 'noul', get instructions() { throw new Error('must not execute'); } };
  expect(() => encoder().encodeRecord({ ...one, questions: { q: accessor } })).toThrow('accessor');
  const sparseScore = ['low', 'high']; delete sparseScore[0];
  expect(() => encoder().encodeRecord({ ...one,
    questions: { q: { type: 'score', criteria: sparseScore } } })).toThrow('sparse');
  expect(() => encoder({ encode(text) { return text.includes('q') ? [] : [...Buffer.from(text)]; } })
    .encodeRecord(one)).toThrow('empty');
  expect(() => encoder({ encode() { return new DataView(new ArrayBuffer(8)); } }).encodeRecord(one))
    .toThrow('token IDs');
  expect(() => encoder({ encode() { return Array.from({ length: 1000 }, () => 1); } }, { maxLength: 100 })
    .encodeRecord(one)).toThrow('token budget');
  const emptyState = encoder({ encode(text) { return [...Buffer.from(text)]; } })
    .encodeRecord({ ...one, state: '' });
  expect(decode(emptyState.input_ids)).toContain('STATE:\n\n\nSCHEMA FIELDS:');
});

test('factory snapshots encoder method and deterministic assertion; digests bind source and artifacts', () => {
  const tokenizer = byteTokenizer();
  const first = encoder(tokenizer);
  tokenizer.encode = () => { throw new Error('mutated'); };
  tokenizer.deterministic = false;
  expect(() => first.encodeRecord({ state: '', questions: { q: { type: 'noul' } } })).not.toThrow();
  expect(first.deterministic).toBe(true);
  expect(encoder(tokenizer).deterministic).toBe(false);
  expect(encoder(byteTokenizer(), { tokenizerDigest: 'c'.repeat(64) }).encoder_digest)
    .not.toBe(first.encoder_digest);
  expect(encoder(byteTokenizer(), { artifactDigest: 'c'.repeat(64) }).encoder_digest)
    .not.toBe(first.encoder_digest);
  expect(() => createClefTextEncoder({ tokenizer: byteTokenizer(), artifactDigest: 'wrong', tokenizerDigest }))
    .toThrow('pinned artifact');
});

test('default schema compilation reuses the whole ordered program, never state or caller-owned output', () => {
  const warmCalls = [], coldCalls = [];
  const warm = encoder(byteTokenizer(warmCalls));
  const cold = encoder(byteTokenizer(coldCalls), { maxCompiledSchemas: 0 });
  const questions = { relevant: { type: 'noul', instructions: 'Addresses the purpose?' },
    route: { type: 'choice', criteria: { atlas: 'Search', flow: 'Work', none: null } } };
  let firstCalls;
  for (let index = 0; index < 100; index++) {
    const record = { id: String(index), state: `private passage ${index}`, questions };
    const actual = warm.encodeRecord(record), reference = cold.encodeRecord(record);
    expect(actual).toEqual(reference);
    if (index === 0) firstCalls = warmCalls.length;
    // All exposed arrays must be independent of the resident program.
    actual.input_ids.fill(0);
    actual.questions[0].option_ids[0] = 'corrupt';
    actual.questions[0].question_span.fill(0);
    actual.questions[0].option_spans[0].fill(0);
  }
  expect(coldCalls.length).toBe(100 * firstCalls);
  expect(warmCalls.length).toBe(firstCalls + 99);
  expect(warmCalls.filter(call => call.text.startsWith('private passage')).length).toBe(100);
  expect(warm.residency()).toMatchObject({ compiled: 1, reused: 99, entries: 1, retained_state_bytes: 0 });
  expect(warm.residency().program_bytes).toBeGreaterThan(0);
  const reversed = Object.fromEntries(Object.entries(questions).reverse());
  expect(warm.encodeRecord({ state: 'changed', questions: reversed }))
    .toEqual(cold.encodeRecord({ state: 'changed', questions: reversed }));
  expect(warm.residency().compiled).toBe(2);
  questions.route.criteria.atlas = 'Changed rubric';
  expect(warm.encodeRecord({ state: 'changed', questions }))
    .toEqual(cold.encodeRecord({ state: 'changed', questions }));
  expect(warm.residency().compiled).toBe(3);
  const malformed = { ...questions, relevant: { type: 'noul', get instructions() {
    throw new Error('must not run');
  } } };
  expect(() => warm.encodeRecord({ state: 'changed', questions: malformed })).toThrow('accessor');
});

test('cache key and compiled program use one owned rubric when tokenizer mutates caller input', () => {
  const questions = { q: { type: 'noul', instructions: 'Before' } };
  let mutate = true;
  const e = encoder({ deterministic: true, encode(text) {
    if (mutate && text.includes('SCHEMA FIELDS')) {
      questions.q.instructions = 'After'; mutate = false;
    }
    return [...Buffer.from(text)];
  } });
  const before = e.encodeRecord({ state: '', questions });
  expect(decode(before.input_ids.slice(...before.questions[0].question_span))).toBe('Before');
  const after = e.encodeRecord({ state: '', questions });
  expect(decode(after.input_ids.slice(...after.questions[0].question_span))).toBe('After');
  questions.q.instructions = 'Before';
  const reused = e.encodeRecord({ state: '', questions });
  const cold = encoder(byteTokenizer(), { maxCompiledSchemas: 0 }).encodeRecord({ state: '', questions });
  expect(reused).toEqual(cold);
  expect(e.residency()).toMatchObject({ compiled: 2, reused: 1, entries: 2 });
});

test('compiled schemas honor LRU, byte budgets, nondeterministic owners and explicit eviction', () => {
  const e = encoder(byteTokenizer(), { maxCompiledSchemas: 2 });
  const record = id => ({ state: 's', questions: { [id]: { type: 'noul' } } });
  e.encodeRecord(record('a')); e.encodeRecord(record('b')); e.encodeRecord(record('a'));
  e.encodeRecord(record('c')); e.encodeRecord(record('a')); e.encodeRecord(record('b'));
  expect(e.residency()).toMatchObject({ compiled: 4, reused: 2, entries: 2 });
  e.clearResidency();
  expect(e.residency()).toMatchObject({ entries: 0, program_bytes: 0 });
  e.encodeRecord(record('a'));
  expect(e.residency().compiled).toBe(5);
  const tiny = encoder(byteTokenizer(), { maxCompiledSchemaBytes: 1 });
  tiny.encodeRecord(record('a')); tiny.encodeRecord(record('a'));
  expect(tiny.residency()).toMatchObject({ compiled: 2, reused: 0, entries: 0 });
  const tokenizer = byteTokenizer(); delete tokenizer.deterministic;
  const uncertain = encoder(tokenizer);
  uncertain.encodeRecord(record('a')); uncertain.encodeRecord(record('a'));
  expect(uncertain.residency()).toMatchObject({ compiled: 2, reused: 0, entries: 0 });
  for (const options of [{ maxCompiledSchemas: -1 }, { maxCompiledSchemaBytes: 0.5 }])
    expect(() => encoder(byteTokenizer(), options)).toThrow('nonnegative integer');
});

test('warm programs retain exact byte/token/state limits and failures never populate residency', () => {
  const record = { state: 'ok', questions: { q: { type: 'noul' } } };
  const baseline = encoder().encodeRecord(record);
  for (const [options, message] of [
    [{ maxLength: baseline.input_ids.length }, 'token budget'],
    [{ maxInputBytes: baseline.input_ids.length }, 'byte budget'],
    [{ maxStateTokens: 2 }, 'no truncation'],
  ]) {
    const e = encoder(byteTokenizer(), options);
    e.encodeRecord(record);
    expect(() => e.encodeRecord({ ...record, state: 'too long' })).toThrow(message);
    expect(e.residency()).toMatchObject({ compiled: 1, reused: 0, entries: 1 });
    const fresh = encoder(byteTokenizer(), options);
    expect(() => fresh.encodeRecord({ ...record, state: 'too long' })).toThrow(message);
    expect(fresh.residency()).toMatchObject({ compiled: 0, reused: 0, entries: 0 });
  }
});
