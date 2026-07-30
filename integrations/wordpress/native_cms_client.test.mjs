import assert from 'node:assert/strict';
import { createHmac } from 'node:crypto';
import { createNativeCmsClient } from './native_cms_client.mjs';
import { normalizeWebhook } from './bridge.mjs';

const rawBody = JSON.stringify({
  post_id: 42,
  site: 'https://example.test',
  modified_gmt: '2026-07-30T12:00:00Z',
  title: 'Native CMS',
  link: 'https://example.test/native-cms',
});
const event = normalizeWebhook({
  body: JSON.parse(rawBody),
  rawBody,
  signature: `sha256=${createHmac('sha256', 'secret').update(rawBody).digest('hex')}`,
  secret: 'secret',
});

const calls = [];
const responses = [
  { ok: true, json: async () => ({ data: [] }) },
  { ok: true, json: async () => ({ data: { id: 9 } }) },
  { ok: true, json: async () => ({ data: [{ id: 9, attributes: { external_id: 'https://example.test:42' } }] }) },
  { ok: true, json: async () => ({ data: { id: 9 } }) },
];
const client = createNativeCmsClient({
  baseUrl: 'http://127.0.0.1:8800',
  token: 'cms-token',
  fetchImpl: async (url, options) => {
    calls.push({ url, options });
    return responses.shift();
  },
});

assert.equal((await client.syncWordPressEvent(event)).operation, 'created');
assert.equal((await client.syncWordPressEvent(event)).operation, 'updated');
assert.equal(calls[0].url, 'http://127.0.0.1:8800/v1/api/wordpress/cms_article?pageSize=100');
assert.equal(calls[1].options.method, 'POST');
assert.equal(calls[2].options.method, undefined);
assert.equal(calls[3].options.method, 'PUT');
assert.match(calls[3].url, /\/cms_article\/9$/);
assert.equal(calls[1].options.headers.Authorization, 'Bearer cms-token');
console.log('native CMS client tests passed');

