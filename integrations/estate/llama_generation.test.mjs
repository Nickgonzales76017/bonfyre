import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';

import { createLlamaGenerationClient, extractLlamaResponse } from './llama_generation.mjs';

assert.equal(extractLlamaResponse('banner\n<|im_start|>assistant\n```python\ndef add(a, b):\n    return a + b\n```\n\n[ Prompt: 1.0 t/s ]\nExiting...'), 'def add(a, b):\n    return a + b');

let observedArgs;
function fakeSpawn(_binary, args) {
  observedArgs = args;
  const child = new EventEmitter();
  child.stdout = new EventEmitter();
  child.stderr = new EventEmitter();
  setImmediate(() => {
    child.stdout.emit('data', '<|im_start|>assistant\ndef add(a, b):\n    return a + b\n\n[ Prompt: 1.0 t/s ]');
    child.emit('close', 0, null);
  });
  return child;
}

const client = createLlamaGenerationClient({ binary: '/bin/llama-cli', modelPath: '/models/coder.gguf', spawnImpl: fakeSpawn });
const result = await client.generate({ prompt: 'Write add.', maxNewTokens: 24 });
assert.equal(result.backend, 'llama-cpp');
assert.equal(result.output, 'def add(a, b):\n    return a + b');
assert.equal(observedArgs.includes('--single-turn'), true);
assert.equal(observedArgs[observedArgs.indexOf('-n') + 1], '24');
assert.equal(observedArgs.includes('-p'), false);
assert.equal(observedArgs.includes('-f'), true);
console.log('llama generation client tests passed');
