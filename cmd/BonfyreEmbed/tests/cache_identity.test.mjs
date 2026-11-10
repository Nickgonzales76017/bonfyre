import { test, expect, afterAll } from 'bun:test';
import { mkdtempSync, mkdirSync, symlinkSync, copyFileSync, writeFileSync, readFileSync, unlinkSync, rmSync, existsSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import { join, resolve } from 'node:path';
import { tmpdir } from 'node:os';

const binary = resolve(import.meta.dir, '../bonfyre-embed');
const source = join(process.env.HOME, '.cache/bonfyre/models/nomic-embed-text-onnx');
const available = existsSync(join(source, 'onnx/model.onnx')) && existsSync(join(source, 'vocab.txt'));
const temp = mkdtempSync(join(tmpdir(), 'bonfyre-cache-identity-'));
afterAll(() => rmSync(temp, { recursive: true, force: true }));

(available ? test : test.skip)('cache follows vocabulary and model bytes, and cannot hide unavailable assets', () => {
  const model = join(temp, 'model');
  mkdirSync(join(model, 'onnx'), { recursive: true });
  const graph = join(model, 'onnx/model.onnx');
  symlinkSync(join(source, 'onnx/model.onnx'), graph);
  const vocab = join(model, 'vocab.txt');
  copyFileSync(join(source, 'vocab.txt'), vocab);
  const input = join(temp, 'input.txt');
  writeFileSync(input, 'The market is near the harbour.');
  const run = name => spawnSync(binary, ['--text', input, '--model', model, '--out', join(temp, name + '.json')], { encoding: 'utf8' });
  const first = run('first');
  expect(first.status).toBe(0);
  expect(first.stderr).not.toContain('cache hit');
  const cached = run('cached');
  expect(cached.status).toBe(0);
  expect(cached.stderr).toContain('cache hit');

  const words = readFileSync(vocab, 'utf8').split('\n');
  const a = words.indexOf('market'), b = words.indexOf('school');
  expect(a).toBeGreaterThan(0);
  expect(b).toBeGreaterThan(0);
  [words[a], words[b]] = [words[b], words[a]];
  writeFileSync(vocab, words.join('\n'));
  const changed = run('changed');
  expect(changed.status).toBe(0);
  expect(changed.stderr).not.toContain('cache hit');
  const changedVector = JSON.parse(readFileSync(join(temp, 'changed.json'))).vector;
  const firstVector = JSON.parse(readFileSync(join(temp, 'first.json'))).vector;
  expect(changedVector.length).toBe(768);
  expect(firstVector.length).toBe(768);
  expect(changedVector).not.toEqual(firstVector);

  unlinkSync(graph);
  writeFileSync(graph, 'This is not an ONNX graph.');
  const invalid = run('invalid');
  expect(invalid.status).not.toBe(0);
  expect(invalid.stderr).not.toContain('cache hit');
  unlinkSync(graph);
  const missing = run('missing');
  expect(missing.status).not.toBe(0);
  expect(missing.stderr).not.toContain('cache hit');
});
