import { toNativeCmsEntry } from './bridge.mjs';

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

  async function request(url, options = {}) {
    const response = await fetchImpl(url, { ...options, headers: { ...headers, ...(options.headers || {}) } });
    let body = null;
    try { body = await response.json(); } catch { /* empty response */ }
    if (!response.ok) throw new Error(`Native CMS request failed: ${response.status}`);
    return body;
  }

  async function syncWordPressEvent(event, { contentType = 'cms_article', namespace = 'wordpress' } = {}) {
    const entry = toNativeCmsEntry(event, { contentType, namespace });
    const collection = `${root}/v1/api/${encodeSegment(namespace)}/${encodeSegment(contentType)}`;
    const listing = await request(`${collection}?pageSize=100`);
    const existing = (listing?.data || []).find(
      (item) => item?.attributes?.external_id === entry.external_id,
    );
    const payload = JSON.stringify({ ...entry.fields, external_id: entry.external_id });
    if (existing?.id != null) {
      return {
        ...await request(`${collection}/${encodeURIComponent(String(existing.id))}`, {
          method: 'PUT',
          body: payload,
        }),
        operation: 'updated',
        external_id: entry.external_id,
      };
    }
    return {
      ...await request(collection, { method: 'POST', body: payload }),
      operation: 'created',
      external_id: entry.external_id,
    };
  }

  return { syncWordPressEvent };
}

