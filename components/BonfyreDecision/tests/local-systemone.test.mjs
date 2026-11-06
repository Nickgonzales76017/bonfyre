// What this does not prove:
// - a server started in this test answers in place of the outside service, so nothing here is evidence about that service.
import { test, expect } from 'bun:test';
import { createHash } from 'node:crypto';
import { mkdtempSync, writeFileSync, symlinkSync, rmSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { CLEF_FLASH_Q4, CLEF_FLASH_OWNER_IDENTITY, clefFlashOwnerIdentity, createPinnedClefFlashEvaluator,
  startOwnedSystemOneTransport } from '../local-systemone.mjs';
import { assessDecision, compileDecisionCase, decisionDigest } from '../decision-plane.mjs';

const digest = value => createHash('sha256').update(value).digest('hex');
const binaryPath = process.execPath;
const binarySha256 = digest(readFileSync(binaryPath));
const fixture = resolve(import.meta.dir, 'fixtures/mock-systemone-server.mjs');
const questions = {
  pick: { type: 'choice', instructions: 'Choose one.', criteria: { a: 'Alpha', b: 'Beta' } },
  risk: { type: 'noul', instructions: 'Is review required?' },
  grade: { type: 'score', instructions: 'Rate it.', criteria: ['Low', 'High'] },
};

test('lazy owner passport binds adapter source and locked execution without opening weights', () => {
  expect(CLEF_FLASH_OWNER_IDENTITY).toEqual(clefFlashOwnerIdentity());
  expect(CLEF_FLASH_OWNER_IDENTITY.adapter_source_sha256).toBe(
    digest(readFileSync(resolve(import.meta.dir, '../local-systemone.mjs'))));
  expect(CLEF_FLASH_OWNER_IDENTITY.confidence_semantics).toBe('typesafe-uniform-adjusted');
  expect(clefFlashOwnerIdentity({ requestMs: 90000 }).artifact_digest)
    .not.toBe(CLEF_FLASH_OWNER_IDENTITY.artifact_digest);
  expect(() => clefFlashOwnerIdentity({ maxResponseBytes: 1048577 })).toThrow('bounded');
});

function modelFixture() {
  const dir = mkdtempSync(join(tmpdir(), 'bonfyre-systemone-test-'));
  const modelPath = join(dir, 'model.gguf');
  const bytes = Buffer.from('synthetic, not trained Clef weights');
  writeFileSync(modelPath, bytes);
  return { dir, modelPath, modelBytes: bytes.length, modelSha256: digest(bytes),
    cleanup: () => rmSync(dir, { recursive: true, force: true }) };
}

async function transport(model, extra = {}) {
  return startOwnedSystemOneTransport({ binaryPath, binarySha256,
    modelPath: model.modelPath, modelSha256: model.modelSha256, modelBytes: model.modelBytes,
    alias: 'fixture-systemone', startupMs: 4000, requestMs: 1000,
    maxRequestBytes: 8192, maxResponseBytes: 1024, prefixArgs: [fixture], ...extra });
}

test('owned loopback child returns validated joint typed probabilities, including exact zero', async () => {
  const model = modelFixture();
  let owner;
  try {
    owner = await transport(model);
    const result = await owner.request({ state: { purpose: 'offline' }, questions });
    expect(result.answers.pick).toEqual({ type: 'choice', choice: 'a', confidence: 0.5,
      probabilities: { a: 1, b: 0 } });
    expect(result.answers.risk).toEqual({ type: 'noul', noul: 1 });
    expect(result.answers.grade).toEqual({ type: 'score', score: 0, confidence: 0.5,
      probabilities: { '0': 1, '1': 0 } });
    const firstClose = owner.close(), joinedClose = owner.close();
    expect(joinedClose).toBe(firstClose);
    await firstClose;
    await expect(owner.request({ state: {}, questions })).rejects.toThrow('closed');
  } finally { await owner?.close(); model.cleanup(); }
});

test('ordinary shared assessment consumes child-owned typed answers without granting an effect', async () => {
  const model = modelFixture();
  let owner;
  try {
    owner = await transport(model);
    const compiled = compileDecisionCase({ program: 'context_passage', owner: 'ContextCut',
      subject_id: 'offline-fixture', mask_digest: decisionDigest('offline-mask'), gate_state: 'ADMIT',
      information_policy_class: 'private',
      frontier: [{ id: 'passage', description: 'Fixture passage', owner_ref: 'ContextCut:fixture' }],
      state: { purpose: 'Inspect', passage: { id: 'passage', text: 'Diagnostic evidence only.' } } });
    const localEvaluator = { model: 'fixture-systemone', artifact_digest: digest('untrained fixture'),
      question_semantics: 'joint_schema', deterministic: false,
      evaluate: ({ state, questions, signal }) => owner.request({ state, questions, signal }) };
    const receipt = await assessDecision(compiled, { execute: true, localEvaluator });
    expect(receipt.provider).toBe('local');
    expect(receipt.inference.executed).toBe(true);
    expect(receipt.answers.relevant.noul).toBe(1);
    expect(receipt.proposal.action).toBe('none');
  } finally { await owner?.close(); model.cleanup(); }
});

test('schema IDs and choice labels named images remain text, not media payloads', async () => {
  const model = modelFixture();
  let owner;
  try {
    owner = await transport(model);
    const schema = { images: { type: 'choice', instructions: 'Classify the source material.',
      criteria: { images: 'Still images', audio: 'Recorded audio', articles: 'Written articles' } } };
    const result = await owner.request({ state: { description: 'A photography archive' }, questions: schema });
    expect(result.answers.images.type).toBe('choice');
    expect(Object.keys(result.answers.images.probabilities)).toEqual(['images', 'audio', 'articles']);
    await expect(owner.request({ state: { nested: [{ images: ['payload'] }] }, questions: schema }))
      .rejects.toThrow('media');
    await expect(owner.request({ state: {}, questions: { images: { ...schema.images,
      instructions: '<<clef:reserved' } } })).rejects.toThrow('reserved Clef');
    for (const extra of ['images', 'image_url', 'unexpected']) {
      await expect(owner.request({ state: {}, questions: { images: { ...schema.images,
        [extra]: 'unsupported descriptor payload' } } })).rejects.toThrow('unsupported question fields');
    }
  } finally { await owner?.close(); model.cleanup(); }
});

test('preflight refuses reserved markers, media, missing instructions, oversize and too many questions', async () => {
  const model = modelFixture();
  let owner;
  try {
    owner = await transport(model, { maxRequestBytes: 1024 });
    await expect(owner.request({ state: { '<<clef:marker': 1 }, questions })).rejects.toThrow('reserved Clef');
    await expect(owner.request({ state: { image_url: 'x' }, questions })).rejects.toThrow('media');
    await expect(owner.request({ state: null, questions })).rejects.toThrow('non-null');
    await expect(owner.request({ state: {}, questions: { ...questions, bad: { type: 'noul' } } })).rejects.toThrow('instructions');
    await expect(owner.request({ state: {}, questions: { bad: { type: 'noul', instructions: 'Yes?',
      criteria: { ignored: 'Unsafe silent option' } } } })).rejects.toThrow('true/false');
    await expect(owner.request({ state: {}, questions: { bad: { type: 'score', instructions: 'Rate',
      criteria: Array.from({ length: 11 }, (_, i) => String(i)) } } })).rejects.toThrow('ten score levels');
    await expect(owner.request({ state: { text: 'x'.repeat(1100) }, questions })).rejects.toThrow('byte budget');
    const many = Object.fromEntries(Array.from({ length: 65 }, (_, i) => [`q${i}`, { type: 'noul', instructions: 'Yes?' }]));
    await expect(owner.request({ state: {}, questions: many })).rejects.toThrow('question budget');
  } finally { await owner?.close(); model.cleanup(); }
});

test('protocol refuses invalid answer, model mismatch, redirect, oversized body and cancellation', async () => {
  const model = modelFixture();
  let owner;
  try {
    owner = await transport(model);
    await expect(owner.request({ state: { invalid: true }, questions })).rejects.toThrow('invalid probability');
    await expect(owner.request({ state: { wrong_model: true }, questions })).rejects.toThrow('different model identity');
    await expect(owner.request({ state: { redirect: true }, questions })).rejects.toThrow();
    await expect(owner.request({ state: { oversized: true }, questions })).rejects.toThrow('byte budget');
    const backend = await owner.request({ state: { backend_error: true }, questions })
      .then(() => null, error => error.message);
    expect(backend).toContain('HTTP 500 code=503 type=native_failure message=Compute error.');
    expect(backend).not.toContain('private_state');
    const privateError = await owner.request({ state: { backend_private_error: true }, questions })
      .then(() => null, error => error.message);
    expect(privateError).toContain('message=sha256:');
    expect(privateError).not.toContain('secret phrase');
    const cancel = new AbortController();
    cancel.abort(new Error('caller cancelled'));
    await expect(owner.request({ state: {}, questions, signal: cancel.signal })).rejects.toThrow();
  } finally { await owner?.close(); model.cleanup(); }
});

test('startup pin is rechecked before each request, and an in-flight call can be cancelled', async () => {
  const model = modelFixture();
  let owner;
  try {
    owner = await transport(model, { requestMs: 3000 });
    const cancel = new AbortController();
    const pending = owner.request({ state: { slow: true }, questions, signal: cancel.signal });
    setTimeout(() => cancel.abort(new Error('cancelled during native execution')), 30);
    await expect(pending).rejects.toThrow();
    writeFileSync(model.modelPath, 'changed model bytes');
    await expect(owner.request({ state: {}, questions })).rejects.toThrow('changed after startup');
  } finally { await owner?.close(); model.cleanup(); }
});

test('artifact bytes are pinned; official factory cannot relabel a synthetic file as trained Clef', async () => {
  const model = modelFixture();
  try {
    await expect(transport(model, { modelSha256: '0'.repeat(64) })).rejects.toThrow('SHA-256');
    await expect(transport(model, { binarySha256: '0'.repeat(64) })).rejects.toThrow('SHA-256');
    const link = join(model.dir, 'linked.gguf');
    symlinkSync(model.modelPath, link);
    await expect(transport(model, { modelPath: link })).rejects.toThrow();
    await expect(createPinnedClefFlashEvaluator({ binaryPath, binarySha256,
      modelPath: model.modelPath, startupMs: 1000 })).rejects.toThrow('approved pinned native binary');
    expect(CLEF_FLASH_Q4.sha256).toBe('3243a51d7a7bb205fb4fcf402a7df763f7d31e6645a9c10e5754c4395ab85b66');
  } finally { model.cleanup(); }
});
