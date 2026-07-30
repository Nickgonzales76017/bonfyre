import { createHmac, timingSafeEqual } from 'node:crypto';
import { AsyncTTLCache } from '../../lib/async_cache.mjs';

export const WORDPRESS_SCHEMA = 'bonfyre.wordpress.bridge.v1';
export const NATIVE_CMS_SCHEMA = 'bonfyre.native.cms.entry.v1';

function text(value) {
  return typeof value === 'string' ? value.trim() : '';
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
  const site = text(body.site || body.home_url || 'wordpress');
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
  if (!text(contentType) || !text(namespace)) throw new Error('contentType and namespace are required');
  const source = event.source || {};
  const externalId = `${text(source.site)}:${text(source.post_id)}`;
  if (externalId === ':') throw new Error('Normalized event is missing source identity');
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
      source_site: text(source.site),
      source_post_id: text(source.post_id),
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
    invalidate(postId) { return responseCache.delete(`post:${String(postId).trim()}`); },
    cacheStats() { return responseCache.stats(); },
    normalizeWebhook(payload) { return normalizeWebhook(payload); },
  };
}
