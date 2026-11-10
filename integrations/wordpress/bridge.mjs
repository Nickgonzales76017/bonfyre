import { createHmac, timingSafeEqual } from 'node:crypto';
import { AsyncTTLCache } from '../../lib/async_cache.mjs';

export const WORDPRESS_SCHEMA = 'bonfyre.wordpress.bridge.v1';
export const NATIVE_CMS_SCHEMA = 'bonfyre.native.cms.entry.v1';
export const WORDPRESS_SITE_CLAIM_SCHEMA = 'bonfyre.wordpress.site-claim.v1';

function text(value) {
  return typeof value === 'string' ? value.trim() : '';
}

function canonicalSite(value) {
  const raw = text(value);
  if (!raw) throw new Error('site identity is required');
  const url = new URL(raw);
  if (url.protocol !== 'https:' || url.username || url.password || url.search || url.hash) {
    throw new Error('site identity must be a clean HTTPS origin');
  }
  return url.origin;
}

function claimMac(site, nonce, secret) {
  return createHmac('sha256', secret).update(`${WORDPRESS_SITE_CLAIM_SCHEMA}\n${site}\n${nonce}`).digest('hex');
}

// The bridge/plugin shares its installation secret with the receiving service.
// This only verifies control of that installed bridge, not ownership of a
// person, institution, publication, or any authority outside the bridge.
export function issueSiteClaimChallenge({ site, nonce, secret }) {
  const origin = canonicalSite(site);
  const token = text(nonce);
  if (!token || !text(secret)) throw new Error('claim nonce and bridge secret are required');
  return { schema_version: WORDPRESS_SITE_CLAIM_SCHEMA, site: origin, nonce: token };
}

export function verifySiteClaimResponse({ challenge, proof, secret, verifiedAt = new Date().toISOString() }) {
  if (!challenge || challenge.schema_version !== WORDPRESS_SITE_CLAIM_SCHEMA) throw new Error('WordPress site claim challenge is required');
  const site = canonicalSite(challenge.site);
  const nonce = text(challenge.nonce);
  if (!nonce || !text(secret) || !text(proof)) throw new Error('claim proof and bridge secret are required');
  const expected = claimMac(site, nonce, secret);
  const received = text(proof).replace(/^sha256=/i, '');
  const a = Buffer.from(received, 'hex'); const b = Buffer.from(expected, 'hex');
  if (a.length !== b.length || !timingSafeEqual(a, b)) throw new Error('invalid WordPress site claim proof');
  return { schema_version: WORDPRESS_SITE_CLAIM_SCHEMA, site, nonce, proof_state: 'bridge_control_verified', verified_at: verifiedAt,
    cannot_infer: 'bridge control proof does not identify an Actor, establish institutional authority, import private WordPress data, or authorize publishing' };
}

export function verifyWebhookSignature(rawBody, signature, secret) {
  const received = text(signature).replace(/^sha256=/i, '');
  if (!received || !secret) return false;
  const expected = createHmac('sha256', secret).update(rawBody).digest('hex');
  const a = Buffer.from(received, 'hex');
  const b = Buffer.from(expected, 'hex');
  return a.length === b.length && timingSafeEqual(a, b);
}

export function normalizeWebhook({ body, rawBody = body?.rawBody || '', signature, secret, event = 'post.updated' }) {
  if (!verifyWebhookSignature(rawBody, signature, secret)) throw new Error('Invalid WordPress webhook signature');
  const postId = String(body.post_id ?? body.id ?? '').trim();
  if (!postId) throw new Error('WordPress webhook is missing post_id');
  const site = text(body.site || body.home_url);
  if (!site) throw new Error('WordPress webhook is missing site identity');
  const version = text(body.modified_gmt || body.modified || body.updated_at || body.date_gmt || '0');
  return {
    schema_version: WORDPRESS_SCHEMA,
    event,
    idempotency_key: `${site}:${postId}:${version}`,
    source: { site, post_id: postId, post_type: text(body.post_type || 'post') },
    content: { status: text(body.status || 'publish'), url: text(body.link || body.url), title: text(body.title) },
    received_at: new Date().toISOString(),
  };
}

export function toNativeCmsEntry(event, { contentType = 'cms_article', namespace = 'wordpress' } = {}) {
  if (!event || event.schema_version !== WORDPRESS_SCHEMA) {
    throw new Error('A normalized WordPress event is required');
  }
  if (!text(event.idempotency_key) || !text(event.event) || !text(event.received_at)) {
    throw new Error('Normalized WordPress event is missing replay metadata');
  }
  if (!text(contentType) || !text(namespace)) throw new Error('contentType and namespace are required');
  const source = event.source || {};
  const site = text(source.site);
  const postId = text(source.post_id);
  if (!site || !postId) throw new Error('Normalized event is missing source identity');
  const externalId = `${site}:${postId}`;
  const content = event.content || {};
  return {
    schema_version: NATIVE_CMS_SCHEMA,
    content_type: contentType,
    namespace,
    external_id: externalId,
    idempotency_key: event.idempotency_key,
    fields: {
      title: text(content.title),
      status: text(content.status || 'publish'),
      url: text(content.url),
      source_site: site,
      source_post_id: postId,
      source_post_type: text(source.post_type || 'post'),
    },
    source_event: event.event,
    received_at: event.received_at,
  };
}

export function createWordPressBridge({ baseUrl, token = '', fetchImpl = globalThis.fetch, cache } = {}) {
  if (!text(baseUrl)) throw new Error('baseUrl is required');
  if (typeof fetchImpl !== 'function') throw new Error('fetchImpl is required');
  const responseCache = cache || new AsyncTTLCache({ maxEntries: 512, ttlMs: 15_000 });
  const root = baseUrl.replace(/\/+$/, '');
  const headers = { Accept: 'application/json', ...(token ? { Authorization: `Bearer ${token}` } : {}) };

  async function fetchJson(path, { signal } = {}) {
    const response = await fetchImpl(`${root}${path}`, { headers, signal });
    if (!response.ok) throw new Error(`WordPress request failed: ${response.status}`);
    return response.json();
  }

  // This reads the public WP REST route index. Capability output is limited to
  // routes the site actually advertises; a WordPress-looking page alone does
  // not claim WooCommerce, ActivityPub, write access, or private content.
  async function discoverPublicCapabilities({ signal } = {}) {
    const index = await responseCache.getOrLoad('site:index', () => fetchJson('/wp-json/', { signal }));
    const routes = index && typeof index.routes === 'object' ? Object.keys(index.routes) : [];
    const has = (prefix) => routes.some((route) => route === prefix || route.startsWith(`${prefix}/`));
    return {
      schema_version: 'bonfyre.wordpress.public-capabilities.v1',
      site: root,
      routes: {
        posts: has('/wp/v2/posts'), pages: has('/wp/v2/pages'), media: has('/wp/v2/media'),
        taxonomies: has('/wp/v2/categories') || has('/wp/v2/tags'),
        woocommerce_store: has('/wc/store/v1/products'), activitypub: has('/activitypub/1.0'),
      },
      cannot_infer: 'an advertised public route does not establish current content, account control, private API access, institutional authority, or a right to publish',
    };
  }

  async function fetchPost(postId, { signal } = {}) {
    const id = String(postId).trim();
    if (!id) throw new Error('postId is required');
    return responseCache.getOrLoad(`post:${id}`, async () => {
      const response = await fetchImpl(`${root}/wp-json/wp/v2/posts/${encodeURIComponent(id)}`, { headers, signal });
      if (!response.ok) throw new Error(`WordPress request failed: ${response.status}`);
      return response.json();
    });
  }

  return {
    fetchPost,
    discoverPublicCapabilities,
    invalidate(postId) { return responseCache.delete(`post:${String(postId).trim()}`); },
    cacheStats() { return responseCache.stats(); },
    normalizeWebhook(payload) { return normalizeWebhook(payload); },
  };
}
