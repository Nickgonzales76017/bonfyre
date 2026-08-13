import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';

import { createLlamaGenerationClient, createLlamaServerGenerationClient, extractLlamaResponse } from './llama_generation.mjs';

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

let observedRequest;
const serverClient = createLlamaServerGenerationClient({
  serverUrl: 'http://127.0.0.1:7000/',
  model: 'qwen-moe.gguf',
  fetchImpl: async (url, request) => {
    observedRequest = { url, request };
    return {
      ok: true,
      status: 200,
      text: async () => JSON.stringify({ choices: [{ message: { content: 'Estate MoE online' } }] }),
    };
  },
});
const serverResult = await serverClient.generate({ prompt: 'Reply exactly.', maxNewTokens: 12 });
assert.equal(serverResult.backend, 'llama-cpp-server');
assert.equal(serverResult.output, 'Estate MoE online');
assert.equal(observedRequest.url, 'http://127.0.0.1:7000/v1/chat/completions');
assert.equal(JSON.parse(observedRequest.request.body).max_tokens, 12);
console.log('llama generation client tests passed');
