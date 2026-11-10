// The browser and generated intake worker use this same submission contract.
// A valid envelope identifies source material; it does not authenticate its author.
import { preparationFields } from './catalogue-preparation.mjs';
export async function validateReviewDraft(draft, catalogue) {
  const fail = message => { throw new Error(message); };
  if (!draft || draft.schema !== 'service-site.product-draft.v1' || draft.state !== 'draft') fail('Invalid product draft.');
  const store = catalogue.stores.find(s => s.id === draft.store);
  const product = catalogue.products.find(p => p.key === draft.store + ':' + draft.product_id);
  if (!store || !product || draft.source_sha256 !== store.sha256) fail('This draft uses a different catalogue. Reload the workspace.');
  let source;
  try { source = new URL(draft.source_url); } catch { fail('Invalid source address.'); }
  if (source.origin !== store.origin || source.username || source.password || !source.pathname.startsWith(store.product_path || '/products/')) fail('Invalid source address.');
  if (draft.source_url !== product.source_url) fail('Submitted source address differs from the retained product.');
  if (!Array.isArray(draft.changes) || !draft.changes.length || draft.changes.length > 256) fail('The draft must contain product changes.');
  const seen = new Set();
  for (const change of draft.changes) {
    const preparation = Object.hasOwn(catalogue.preparation_fields ?? {}, change?.field) ? catalogue.preparation_fields[change.field] : null;
    if (!change || typeof change.field !== 'string' || (!preparation && !/^(title|description|tags|(sku|price):[0-9]+)$/.test(change.field)) || seen.has(change.field) || typeof change.before !== 'string' || typeof change.after !== 'string' || change.before === change.after) fail('Invalid product change.');
    if (preparation && (change.before !== '' || change.after.length > preparation.max)) fail('Invalid preparation proposal.');
    if (change.field === 'title' && (!change.after.trim() || change.after.length > 255)) fail('Invalid product title.');
    if (!preparation && change.field.includes(':') && !product.variant_ids.includes(change.field.split(':')[1])) fail('Submitted option is not in the retained product.');
    if (change.field.startsWith('sku:') && change.after.length > 255) fail('Use 255 characters or fewer for an option SKU.');
    if (change.field === 'tags' && change.after.length > 4000) fail('Product tags are too long.');
    if (change.field === 'description' && change.after.length > 50000) fail('Product description is too long.');
    if (change.field.startsWith('price:') && !/^\d{1,9}(?:\.\d{1,2})?$/.test(change.after)) fail('Invalid product price.');
    const expected = product.before_sha256?.[change.field];
    if (typeof expected !== 'string' || !/^[a-f0-9]{64}$/.test(expected)) fail('The original field could not be verified. Reload the workspace.');
    const actual = [...new Uint8Array(await crypto.subtle.digest('SHA-256', new TextEncoder().encode(change.before)))].map(b => b.toString(16).padStart(2, '0')).join('');
    if (actual !== expected) fail('The original field differs from the retained product. Reload the workspace.');
    seen.add(change.field);
  }
  return draft;
}

export function catalogueReviewForm(section, page) {
  return {
    page, max_bytes: 24000,
    fields: { Name: true, Email: { max_length: 254, email: true }, 'Product draft': { max_length: 18000 } },
    required: ['Name', 'Email', 'Product draft'],
    catalogue: {
      preparation_fields: Object.fromEntries(preparationFields.map(({field, max}) => [field, {max}])),
      stores: section.stores.map(({ id, origin, sha256, product_path }) => ({ id, origin, sha256, product_path })),
      products: section.entries.map(p => {
        const variant_ids = p.variant_ids ?? p.variants?.map(v => v.id);
        if (typeof p.source_url !== 'string' || !Array.isArray(variant_ids) || !variant_ids.length) throw new Error('Catalogue review requires retained product addresses and options.');
        const fields = ['title', 'description', 'tags', ...preparationFields.map(p => p.field), ...variant_ids.flatMap(id => ['sku:' + id, 'price:' + id])];
        if (fields.some(field => typeof p.before_sha256?.[field] !== 'string' || !/^[a-f0-9]{64}$/.test(p.before_sha256[field]))) throw new Error('Catalogue review requires retained original field digests.');
        return { key: p.store + ':' + p.id, source_url: p.source_url, variant_ids, before_sha256: p.before_sha256 };
      })
    }
  };
}

export function reviewRequest(form, name, email, draft, page) {
  const fields = [{ label: 'Name', value: name.trim() }, { label: 'Email', value: email.trim() }, { label: 'Product draft', value: JSON.stringify(draft) }];
  if (!fields[0].value || !/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(fields[1].value)) throw new Error('Add your name and email so we can reply.');
  const body = JSON.stringify({ form, page, fields });
  if (fields[2].value.length > 18000 || new TextEncoder().encode(body).length > 24000 || new TextEncoder().encode(JSON.stringify(JSON.stringify(fields))).length > 24000) throw new Error('This draft is too large to send in one review. Save it and contact us to arrange a larger handoff.');
  return body;
}

// A local receipt remembers an acknowledgement of this saved version. It grants no authority.
export const reviewHistoryPrefix = 'review:catalogue:';
export async function reviewVersion(form, draft) {
  if (typeof form !== 'string' || !form || !draft || draft.schema !== 'service-site.product-draft.v1' || draft.state !== 'draft' || !Array.isArray(draft.changes) || !draft.changes.length) throw Error('No saved version to identify.');
  const projection = [form, draft.schema, draft.store, draft.product_id, draft.source_url, draft.source_sha256, draft.observed_at, draft.saved_at, draft.state, draft.changes.map(c => [c.field, c.before, c.after]).sort((a, b) => a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0)];
  return [...new Uint8Array(await crypto.subtle.digest('SHA-256', new TextEncoder().encode(JSON.stringify(projection))))].map(b => b.toString(16).padStart(2, '0')).join('');
}
export function localReviewReceipt(version, reference, recordedAt = new Date().toISOString()) {
  if (!/^[a-f0-9]{64}$/.test(version) || !/^lead:catalogue:[a-f0-9]{64}$/.test(reference) || typeof recordedAt !== 'string' || !Number.isFinite(Date.parse(recordedAt))) throw Error('No valid review receipt was returned. Your draft is saved and can be retried.');
  return { schema: 'service-site.review-receipt.v1', version, reference, recorded_at: recordedAt };
}
export function storedReviewReceipt(version, value) {
  if (!value || value.schema !== 'service-site.review-receipt.v1' || value.version !== version) return null;
  try { return localReviewReceipt(value.version, value.reference, value.recorded_at); } catch { return null; }
}
