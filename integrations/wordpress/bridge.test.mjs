import assert from 'node:assert/strict';
import { createHmac } from 'node:crypto';
import { createWordPressBridge, issueSiteClaimChallenge, normalizeWebhook, toNativeCmsEntry, verifySiteClaimResponse, verifyWebhookSignature, WORDPRESS_SITE_CLAIM_SCHEMA } from './bridge.mjs';

const rawBody = JSON.stringify({ post_id: 42, site: 'https://example.test', modified_gmt: '2026-07-30T12:00:00Z' });
const secret = 'test-secret';
const signature = `sha256=${createHmac('sha256', secret).update(rawBody).digest('hex')}`;
assert.equal(verifyWebhookSignature(rawBody, signature, secret), true);
assert.equal(verifyWebhookSignature(rawBody, 'sha256=00', secret), false);
const event = normalizeWebhook({ body: JSON.parse(rawBody), rawBody, signature, secret });
assert.equal(event.idempotency_key, 'https://example.test:42:2026-07-30T12:00:00Z');
const missingSiteBody = JSON.stringify({ post_id: 42 });
const missingSiteSignature = `sha256=${createHmac('sha256', secret).update(missingSiteBody).digest('hex')}`;
assert.throws(
  () => normalizeWebhook({ body: JSON.parse(missingSiteBody), rawBody: missingSiteBody, signature: missingSiteSignature, secret }),
  /site identity/i,
);
const nativeEntry = toNativeCmsEntry(event);
assert.deepEqual(nativeEntry, {
  schema_version: 'bonfyre.native.cms.entry.v1',
  content_type: 'cms_article',
  namespace: 'wordpress',
  external_id: 'https://example.test:42',
  idempotency_key: event.idempotency_key,
  fields: {
    title: '',
    status: 'publish',
    url: '',
    source_site: 'https://example.test',
    source_post_id: '42',
    source_post_type: 'post',
  },
  source_event: 'post.updated',
  received_at: event.received_at,
});
assert.throws(() => toNativeCmsEntry({}), /normalized WordPress event/i);
assert.throws(() => toNativeCmsEntry({ ...event, idempotency_key: '' }), /replay metadata/i);
const claim = issueSiteClaimChallenge({ site: 'https://Example.test/', nonce: 'n-42', secret });
assert.deepEqual(claim, { schema_version: WORDPRESS_SITE_CLAIM_SCHEMA, site: 'https://example.test', nonce: 'n-42' });
const claimProof = `sha256=${createHmac('sha256', secret).update(`${WORDPRESS_SITE_CLAIM_SCHEMA}\nhttps://example.test\nn-42`).digest('hex')}`;
assert.equal(verifySiteClaimResponse({ challenge: claim, proof: claimProof, secret, verifiedAt: '2026-09-28T00:00:00Z' }).proof_state, 'bridge_control_verified');
assert.throws(() => verifySiteClaimResponse({ challenge: claim, proof: 'sha256=00', secret }), /invalid WordPress site claim proof/i);

let calls = 0;
const bridge = createWordPressBridge({
  baseUrl: 'https://example.test',
  fetchImpl: async () => {
    calls++;
    await new Promise((resolve) => setTimeout(resolve, 2));
    return { ok: true, json: async () => ({ id: 42, title: 'Cached' }) };
  },
});
const [first, second] = await Promise.all([bridge.fetchPost(42), bridge.fetchPost(42)]);
assert.deepEqual(first, second);
assert.equal(calls, 1);
assert.equal(bridge.cacheStats().coalesced, 1);
bridge.invalidate(42);
assert.equal(bridge.cacheStats().entries, 0);

const publicBridge = createWordPressBridge({
  baseUrl: 'https://public.example',
  fetchImpl: async () => ({ ok: true, json: async () => ({ routes: {
    '/wp/v2/posts': {}, '/wp/v2/pages': {}, '/wp/v2/media': {}, '/wp/v2/categories': {},
    '/wc/store/v1/products': {},
  } }) }),
});
const capabilities = await publicBridge.discoverPublicCapabilities();
assert.equal(capabilities.routes.woocommerce_store, true);
assert.equal(capabilities.routes.activitypub, false);
assert.match(capabilities.cannot_infer, /authority/);

let release;
const pendingResponse = new Promise((resolve) => { release = resolve; });
const raceBridge = createWordPressBridge({
  baseUrl: 'https://example.test',
  fetchImpl: async () => ({ ok: true, json: async () => pendingResponse }),
});
const staleRead = raceBridge.fetchPost(7);
raceBridge.invalidate(7);
release({ id: 7, title: 'Stale' });
await staleRead;
assert.equal(raceBridge.cacheStats().entries, 0);
console.log('wordpress bridge tests passed');
