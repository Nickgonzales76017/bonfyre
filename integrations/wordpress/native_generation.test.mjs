import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { readFile } from 'node:fs/promises';
import { createNativeGenerationClient, toGenerationCmsEntry } from './native_generation.mjs';

const fakeSpawn = (_binary, args) => {
  const child = new EventEmitter();
  child.stdout = new EventEmitter();
  child.stderr = new EventEmitter();
  const out = args[args.indexOf('--out') + 1];
  setImmediate(async () => {
    const prompt = await readFile(args[args.indexOf('--prompt') + 1], 'utf8');
    await import('node:fs/promises').then(({ writeFile }) => writeFile(out, `${prompt}    if n <= 0:\n`, 'utf8'));
    child.stderr.emit('data', 'native generation smoke');
    child.emit('close', 0, null);
  });
  return child;
};

const client = createNativeGenerationClient({
  binary: '/opt/cmd/bonfyre-qwen-fpq',
  modelPack: '/models/qwen.fpq-pack.json',
  tokenizer: '/models/tokenizer.json',
  spawnImpl: fakeSpawn,
});
const result = await client.generate({ prompt: 'def fibonacci(n):\n', maxNewTokens: 8, greedy: true });
assert.equal(result.schema_version, 'bonfyre.native.generation.result.v1');
assert.match(result.output, /if n <= 0:/);
assert.equal(result.output_sha256.length, 64);
assert.equal(result.request.max_new_tokens, 8);

const entry = toGenerationCmsEntry(result);
assert.equal(entry.content_type, 'cms_generation');
assert.equal(entry.namespace, 'bonfyre-generation');
assert.equal(entry.fields.output, result.output);
assert.equal(entry.fields.output_sha256, result.output_sha256);
assert.equal(entry.idempotency_key, `bonfyre.native.generation.result.v1:${result.output_sha256}`);
console.log('native generation estate contract passed');
