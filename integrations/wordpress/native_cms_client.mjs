import { toNativeCmsEntry } from './bridge.mjs';
import { toGenerationCmsEntry } from './native_generation.mjs';

function text(value) {
  return typeof value === 'string' ? value.trim() : '';
}

function encodeSegment(value) {
  return encodeURIComponent(text(value));
}

export function createNativeCmsClient({ baseUrl, token = '', fetchImpl = globalThis.fetch } = {}) {
  if (!text(baseUrl)) throw new Error('baseUrl is required');
  if (typeof fetchImpl !== 'function') throw new Error('fetchImpl is required');
  const root = baseUrl.replace(/\/+$/, '');
  const headers = {
    Accept: 'application/json',
    'Content-Type': 'application/json',
    ...(token ? { Authorization: `Bearer ${token}` } : {}),
  };
  const inflight = new Map();

  async function request(url, options = {}) {
    const response = await fetchImpl(url, { ...options, headers: { ...headers, ...(options.headers || {}) } });
    let body = null;
    try { body = await response.json(); } catch { /* empty response */ }
    if (!response.ok) throw new Error(`Native CMS request failed: ${response.status}`);
    if (body == null || typeof body !== 'object') throw new Error('Native CMS returned malformed JSON');
    return body;
  }

  async function syncWordPressEvent(event, { contentType = 'cms_article', namespace = 'wordpress' } = {}) {
    const entry = toNativeCmsEntry(event, { contentType, namespace });
    const inflightKey = `${namespace}:${contentType}:${entry.external_id}`;
    if (inflight.has(inflightKey)) return inflight.get(inflightKey);
    const pending = syncEntry(entry, contentType, namespace);
    inflight.set(inflightKey, pending);
    try { return await pending; } finally { inflight.delete(inflightKey); }
  }

  async function syncGenerationResult(result, { namespace = 'bonfyre-generation' } = {}) {
    return syncEntry(toGenerationCmsEntry(result, { namespace }));
  }

  async function syncEntry(entry, contentType = entry?.content_type, namespace = entry?.namespace) {
    if (!text(contentType) || !text(namespace)) {
      throw new Error('Native CMS entry is missing content type or namespace');
    }
    const collection = `${root}/v1/api/${encodeSegment(namespace)}/${encodeSegment(contentType)}`;
    const listing = await request(`${collection}?pageSize=100`);
    if (!Array.isArray(listing.data)) throw new Error('Native CMS listing is malformed');
    const existing = listing.data.find(
      (item) => item?.attributes?.external_id === entry.external_id,
    );
    if (!existing && listing.data.length >= 100) {
      throw new Error('Native CMS listing is full; refusing a non-idempotent create');
    }
    const payload = JSON.stringify({
      ...entry.fields,
      external_id: entry.external_id,
      idempotency_key: entry.idempotency_key,
      source_event: entry.source_event,
      source_schema_version: entry.schema_version,
      source_received_at: entry.received_at,
    });
    if (existing?.id != null) {
      const updated = await request(`${collection}/${encodeURIComponent(String(existing.id))}`, {
        method: 'PUT',
        body: payload,
      });
      if (updated?.data?.id == null) throw new Error('Native CMS update response is malformed');
      return {
        ...updated,
        operation: 'updated',
        external_id: entry.external_id,
      };
    }
    const created = await request(collection, { method: 'POST', body: payload });
    if (created?.data?.id == null) throw new Error('Native CMS create response is malformed');
    return {
      ...created,
      operation: 'created',
      external_id: entry.external_id,
    };
  }

  return { syncWordPressEvent, syncGenerationResult, syncEntry };
}
