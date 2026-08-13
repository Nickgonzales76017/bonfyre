import assert from 'node:assert/strict';
import { createHmac } from 'node:crypto';
import { mkdtemp, rm } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import net from 'node:net';
import { spawn } from 'node:child_process';
import { dirname, join } from 'node:path';
import { normalizeWebhook } from './bridge.mjs';
import { createNativeCmsClient } from './native_cms_client.mjs';

const repo = dirname(dirname(dirname(fileURLToPath(import.meta.url))));
const binary = process.env.BONFYRE_CMS_BIN || join(repo, 'cmd/BonfyreCMS/bonfyre-cms');
const schemas = join(repo, 'cmd/BonfyreCMS/content-types');

function freePort() {
  return new Promise((resolve, reject) => {
    const server = net.createServer();
    server.once('error', reject);
    server.listen(0, '127.0.0.1', () => {
      const port = server.address().port;
      server.close(() => resolve(port));
    });
  });
}

async function stopChild(child) {
  if (child.exitCode != null) return;
  const exited = new Promise((resolve) => child.once('exit', resolve));
  child.kill('SIGTERM');
  await Promise.race([
    exited,
    new Promise((resolve) => setTimeout(resolve, 500)),
  ]);
  if (child.exitCode == null) {
    child.kill('SIGKILL');
    await exited;
  }
}

async function waitForHealth(url, child) {
  for (let attempt = 0; attempt < 50; attempt++) {
    if (child.exitCode != null) throw new Error(`BonfyreCMS exited during startup: ${child.exitCode}`);
    try {
      const response = await fetch(url);
      if (response.ok) return;
    } catch { /* server is still starting */ }
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  throw new Error('BonfyreCMS health endpoint did not become ready');
}

const rawBody = JSON.stringify({
  post_id: 42,
  site: 'https://e2e.example.test',
  modified_gmt: '2026-07-30T12:00:00Z',
  title: 'Native CMS E2E',
  link: 'https://e2e.example.test/native-cms',
});
const event = normalizeWebhook({
  body: JSON.parse(rawBody),
  rawBody,
  signature: `sha256=${createHmac('sha256', 'secret').update(rawBody).digest('hex')}`,
  secret: 'secret',
});

const tempDir = await mkdtemp('/tmp/bonfyre-cms-e2e-');
const port = await freePort();
const child = spawn(binary, ['serve', '--port', String(port), '--db', join(tempDir, 'cms.db'), '--schemas', schemas], {
  cwd: repo,
  stdio: ['ignore', 'ignore', 'pipe'],
});

try {
  await waitForHealth(`http://127.0.0.1:${port}/v1/health`, child);
  const client = createNativeCmsClient({ baseUrl: `http://127.0.0.1:${port}` });

  const generation = {
    schema_version: 'bonfyre.native.generation.result.v1',
    generated_at: '2026-07-30T12:00:01.000Z',
    model: { name: 'bonfyre-qwen-fpq' },
    backend: 'cpu_neon',
    request: { prompt: 'def fibonacci(n):\n', max_new_tokens: 8, greedy: true },
    prompt_sha256: 'prompt-receipt',
    output: '    if n <= 0:',
    output_sha256: 'output-receipt',
  };
  const schemaProbe = await fetch(`http://127.0.0.1:${port}/v1/schemas`).then(async (response) => ({
    status: response.status,
    body: await response.json(),
  }));
  assert.equal(schemaProbe.status, 200);
  assert.ok(schemaProbe.body.data.some((schema) => schema.name === 'cms_generation'));
  const generationSync = await client.syncGenerationResult(generation);
  assert.equal(generationSync.operation, 'created');
  const generated = await fetch(`http://127.0.0.1:${port}/v1/api/bonfyre-generation/cms_generation?pageSize=100`).then((r) => r.json());
  assert.equal(generated.data.length, 1);
  assert.equal(generated.data[0].attributes.output, generation.output);

  const first = await client.syncWordPressEvent(event, { namespace: 'wordpress' });
  assert.equal(first.operation, 'created');
  const second = await client.syncWordPressEvent(event, { namespace: 'wordpress2' });
  assert.equal(second.operation, 'created');

  const wordpress = await fetch(`http://127.0.0.1:${port}/v1/api/wordpress/cms_article?pageSize=100`).then((r) => r.json());
  const wordpress2 = await fetch(`http://127.0.0.1:${port}/v1/api/wordpress2/cms_article?pageSize=100`).then((r) => r.json());
  assert.equal(wordpress.data.length, 1);
  assert.equal(wordpress2.data.length, 1);

  const id = wordpress.data[0].id;
  const wrongNamespaceGet = await fetch(`http://127.0.0.1:${port}/v1/api/wordpress2/cms_article/${id}`);
  assert.equal(wrongNamespaceGet.status, 404);
  const duplicate = await fetch(`http://127.0.0.1:${port}/v1/api/wordpress/cms_article`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ ...wordpress.data[0].attributes, title: 'Duplicate identity' }),
  });
  assert.equal(duplicate.ok, false);
  const wrongNamespaceUpdate = await fetch(`http://127.0.0.1:${port}/v1/api/wordpress2/cms_article/${id}`, {
    method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ title: 'Cross-namespace write' }),
  });
  assert.equal(wrongNamespaceUpdate.ok, false);
  const wrongNamespaceDelete = await fetch(`http://127.0.0.1:${port}/v1/api/wordpress2/cms_article/${id}`, { method: 'DELETE' });
  assert.equal(wrongNamespaceDelete.ok, false);
  const validUpdate = await fetch(`http://127.0.0.1:${port}/v1/api/wordpress/cms_article/${id}`, {
    method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ title: 'Updated' }),
  });
  assert.equal(validUpdate.status, 200);
  const invalidUpdate = await fetch(`http://127.0.0.1:${port}/v1/api/wordpress/cms_article/${id}`, {
    method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ title: '' }),
  });
  assert.equal(invalidUpdate.ok, false);
  const afterRejectedUpdate = await fetch(`http://127.0.0.1:${port}/v1/api/wordpress/cms_article/${id}`).then((r) => r.json());
  assert.equal(afterRejectedUpdate.data.attributes.title, 'Updated');
  console.log('native CMS end-to-end tests passed');
} finally {
  await stopChild(child);
  await rm(tempDir, { recursive: true, force: true });
}
