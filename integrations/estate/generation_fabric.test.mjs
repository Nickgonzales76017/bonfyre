import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { writeFile } from 'node:fs/promises';
import { createEstateGenerationFabric, discoverGenerationProfiles } from './generation_fabric.mjs';

assert.equal(discoverGenerationProfiles({ env: { BONFYRE_MODEL_PROFILES: JSON.stringify([
  { id: 'coder-small', binary: '/bin/qwen', modelPack: '/models/a', tokenizer: '/models/t' },
]) } })[0].schema_version, 'bonfyre.native.generation.profile.v1');

function fakeSpawn(_path, args) {
  const child = new EventEmitter();
  child.stdout = new EventEmitter();
  child.stderr = new EventEmitter();
  child.stdin = { end() {} };
  setImmediate(async () => {
    if (args.includes('--out')) {
      const out = args[args.indexOf('--out') + 1];
      await writeFile(out, '    if n <= 0:', 'utf8');
      child.stdout.emit('data', 'native-tool-output');
    } else {
      child.stdout.emit('data', '<|im_start|>assistant\\ndef add(a, b):\\n    return a + b\\n\\n[ Prompt: 1.0 t/s ]');
    }
    child.emit('close', 0, null);
  });
  return child;
}

const tools = [
  { id: 'bonfyre-qwen-fpq', name: 'bonfyre-qwen-fpq', path: '/bin/qwen', capabilities: ['generation'], available: true },
  { id: 'bonfyre-embed', name: 'bonfyre-embed', path: '/bin/embed', capabilities: ['embedding'], available: true },
];
const fabric = createEstateGenerationFabric({
  tools,
  spawnImpl: fakeSpawn,
  profiles: [{ id: 'qwen-coder-small', tool_id: 'bonfyre-qwen-fpq', binary: '/bin/qwen', modelPack: '/models/qwen.pack', tokenizer: '/models/tokenizer.json' }],
});

assert.equal((await fabric.inventory()).length, 2);
const invoked = await fabric.invoke({ toolId: 'bonfyre-embed', args: ['status'] });
assert.equal(invoked.status, 'passed');
assert.equal(invoked.ledger.schema_version, 'bonfyre.native.tool.receipt.v1');
const batch = await fabric.batchGenerate([
  { profileId: 'qwen-coder-small', prompt: 'def fibonacci(n):\n', maxNewTokens: 8 },
  { profileId: 'qwen-coder-small', prompt: 'def factorial(n):\n', maxNewTokens: 8 },
]);
assert.equal(batch.count, 2);
assert.equal(batch.passed, 2);
assert.match(batch.results[0].output, /if n <= 0:/);
assert.equal(batch.results[0].analytics.event_name, 'native_generation.completed');
assert.equal(batch.ledger.event, 'native.generation.batch.completed');
assert.equal(batch.analytics.measures.passed, 2);
assert.equal(fabric.profileInventory()[0].id, 'qwen-coder-small');

const llamaFabric = createEstateGenerationFabric({
  tools,
  spawnImpl: fakeSpawn,
  profiles: [{ id: 'coder-llama', binary: '/bin/llama-cli', modelPack: '/models/coder.gguf', runtime: 'llama-cpp' }],
});
const llamaBatch = await llamaFabric.batchGenerate([{ profileId: 'coder-llama', prompt: 'Write add.', maxNewTokens: 8 }]);
assert.equal(llamaBatch.passed, 1);
assert.equal(llamaBatch.results[0].backend, 'llama-cpp');
console.log('estate generation fabric tests passed');
