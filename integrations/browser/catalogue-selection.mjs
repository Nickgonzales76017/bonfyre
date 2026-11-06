// Pure customer selection and enquiry projection. Catalogue, storage and effects keep their existing owners.
// Same decimal-cent contract as ServiceSite; the integration test checks parity.
function priceCents(value) {
  if (typeof value !== 'string' || !/^\d{1,9}(?:\.\d{1,2})?$/.test(value.trim())) throw Error('Invalid observed price.');
  const [whole, fraction = ''] = value.trim().split('.');
  return BigInt(whole) * 100n + BigInt(fraction.padEnd(2, '0'));
}
const money = cents => String(cents / 100n) + '.' + String(cents % 100n).padStart(2, '0');

export const selectionSchema = 'service-site.catalogue-selection.v1';
const limit = Object.freeze({ lines: 64, quantity: 10000, questions: 24, requestBytes: 24000 });
const digestPattern = /^[a-f0-9]{64}$/;
const idPattern = /^[a-zA-Z0-9][a-zA-Z0-9._:-]{0,159}$/;
const fail = message => { throw new Error(message); };
const plain = value => value !== null && typeof value === 'object' && !Array.isArray(value)
  && [Object.prototype, null].includes(Object.getPrototypeOf(value));
function object(value, fields, label) {
  if (!plain(value) || Object.keys(value).some(key => !fields.includes(key))) fail('Invalid ' + label + '.');
  return value;
}
function text(value, max, label, optional = false) {
  if (typeof value !== 'string' || value.length > max || (!optional && !value.trim())
      || /[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/.test(value)) fail('Invalid ' + label + '.');
  return value.trim();
}
function identifier(value, label) {
  if (typeof value !== 'string' || !idPattern.test(value)) fail('Invalid ' + label + '.');
  return value;
}
function questions(value) {
  if (!Array.isArray(value) || value.length > limit.questions) fail('Too many or missing requirements.');
  return value.map(item => text(item, 1000, 'requirement'));
}
function canonical(value) {
  if (Array.isArray(value)) return '[' + value.map(canonical).join(',') + ']';
  if (plain(value)) return '{' + Object.keys(value).sort().map(key => JSON.stringify(key) + ':' + canonical(value[key])).join(',') + '}';
  if (value === undefined || typeof value === 'function' || typeof value === 'symbol'
      || typeof value === 'bigint' || typeof value === 'number' && !Number.isFinite(value)) fail('Invalid selection value.');
  return JSON.stringify(value);
}
const lineKey = line => JSON.stringify([line.product_id, line.variant_id]);
function catalogueIndex(catalogue, storeId) {
  if (!plain(catalogue) || !Array.isArray(catalogue.stores) || !Array.isArray(catalogue.rows)) fail('Missing retained catalogue.');
  const stores = catalogue.stores.filter(store => store.id === storeId);
  if (stores.length !== 1) fail('Select one known store.');
  const store = stores[0];
  if (!digestPattern.test(store.sha256 ?? '')) fail('Missing catalogue release identity.');
  let origin;
  try { origin = new URL(store.origin); } catch { fail('Invalid catalogue origin.'); }
  if (origin.protocol !== 'https:' || origin.origin !== store.origin) fail('Invalid catalogue origin.');
  const products = new Map();
  for (const product of catalogue.rows.filter(row => row.store === storeId)) {
    const id = identifier(product.id, 'retained product identifier');
    if (products.has(id)) fail('Duplicate retained product identifier.');
    products.set(id, product);
  }
  return { store, products };
}
function retainedLine(index, choice) {
  object(choice, ['product_id', 'variant_id', 'quantity', 'note'], 'selection item');
  const productId = identifier(choice.product_id, 'product identifier');
  const variantId = identifier(choice.variant_id, 'option identifier');
  if (!Number.isSafeInteger(choice.quantity) || choice.quantity < 1 || choice.quantity > limit.quantity) fail('Use a whole quantity from 1 to ' + limit.quantity + '.');
  const product = index.products.get(productId);
  if (!product || !Array.isArray(product.variants)) fail('Product is not in this store.');
  const variants = product.variants.filter(item => item.id === variantId);
  if (variants.length !== 1) fail('Option is not uniquely identified in this product.');
  const variant = variants[0];
  if (product.source_sha256 !== index.store.sha256) fail('Product belongs to a different catalogue release.');
  let address;
  try { address = new URL(product.source_url); } catch { fail('Invalid retained product address.'); }
  const prefix = index.store.product_path ?? '/products/';
  if (typeof prefix !== 'string' || !/^\/(?:[a-zA-Z0-9_-]+\/)+$/.test(prefix)
      || address.protocol !== 'https:' || address.origin !== index.store.origin || address.username || address.password
      || !address.pathname.startsWith(prefix) || address.pathname.length <= prefix.length) fail('Retained product address is outside the store.');
  if (typeof product.observed_at !== 'string' || !Number.isFinite(Date.parse(product.observed_at))) fail('Missing source observation time.');
  const currency = index.store.currency ?? null;
  if (currency !== null && (typeof currency !== 'string' || !/^[A-Z]{3}$/.test(currency))) fail('Invalid declared currency.');
  const price = variant.price === null || variant.price === undefined || variant.price === '' ? null : money(priceCents(variant.price));
  const observedBool = value => value === true || value === false ? value : null;
  return {
    product_id: productId, variant_id: variantId, quantity: choice.quantity,
    note: text(choice.note ?? '', 1000, 'item note', true),
    title: text(product.title, 255, 'retained product title'),
    option: text(variant.label, 255, 'retained option label'),
    kind: text(product.kind, 80, 'retained item kind'),
    sku: variant.sku === null || variant.sku === undefined ? null : text(variant.sku, 255, 'retained SKU', true),
    source_url: product.source_url, source_sha256: product.source_sha256, observed_at: product.observed_at,
    observed_unit_price: price, currency,
    available_at_capture: observedBool(variant.available), shipping_at_capture: observedBool(variant.shipping)
  };
}

/** Build from an owner-supplied catalogue. Caller input carries intent, never seller facts. */
export function makeSelection(input, catalogue) {
  object(input, ['project_ref', 'store', 'title', 'purpose', 'requirements', 'items'], 'selection input');
  const store = identifier(input.store, 'store identifier');
  const index = catalogueIndex(catalogue, store);
  if (!Array.isArray(input.items) || input.items.length < 1 || input.items.length > limit.lines) fail('Select between 1 and ' + limit.lines + ' items.');
  const seen = new Set();
  const items = input.items.map(choice => {
    const line = retainedLine(index, choice), key = lineKey(line);
    if (seen.has(key)) fail('Combine quantities for the same product option.');
    seen.add(key); return line;
  });
  return {
    schema: selectionSchema, state: 'draft', project_ref: identifier(input.project_ref, 'project reference'), store,
    title: text(input.title, 255, 'project title'), purpose: text(input.purpose ?? '', 4000, 'project purpose', true),
    requirements: questions(input.requirements ?? []), items
  };
}

/** Call again against the authoritative catalogue at intake; a browser copy is not authority. */
export function validateSelection(selection, catalogue) {
  object(selection, ['schema', 'state', 'project_ref', 'store', 'title', 'purpose', 'requirements', 'items'], 'saved selection');
  if (selection.schema !== selectionSchema || selection.state !== 'draft' || !Array.isArray(selection.items)) fail('Invalid saved selection.');
  const expected = makeSelection({ project_ref: selection.project_ref, store: selection.store, title: selection.title,
    purpose: selection.purpose, requirements: selection.requirements,
    items: selection.items.map(item => ({ product_id: item?.product_id, variant_id: item?.variant_id,
      quantity: item?.quantity, note: item?.note })) }, catalogue);
  if (canonical(selection) !== canonical(expected)) fail('Saved facts differ from this retained catalogue. Review the source change.');
  return expected;
}

export function selectionKey(projectRef, store) {
  return 'selection:catalogue:' + JSON.stringify([identifier(store, 'store'), identifier(projectRef, 'project reference')]);
}

/** A content identity for local versions, not a receipt, identity proof or approval. */
export async function selectionVersion(selection, catalogue) {
  const bytes = new TextEncoder().encode(canonical(validateSelection(selection, catalogue)));
  const hash = await crypto.subtle.digest('SHA-256', bytes);
  return [...new Uint8Array(hash)].map(value => value.toString(16).padStart(2, '0')).join('');
}

export function selectionSummary(selection, catalogue) {
  const valid = validateSelection(selection, catalogue), totals = new Map();
  let missingPrice = 0, missingCurrency = 0;
  for (const item of valid.items) {
    if (item.observed_unit_price === null) missingPrice++;
    if (item.currency === null) missingCurrency++;
    if (item.observed_unit_price !== null && item.currency !== null)
      totals.set(item.currency, (totals.get(item.currency) ?? 0n) + priceCents(item.observed_unit_price) * BigInt(item.quantity));
  }
  return { line_count: valid.items.length,
    observed_subtotals: [...totals].sort(([a], [b]) => a.localeCompare(b)).map(([currency, cents]) => ({ currency, amount: money(cents) })),
    lines_without_price: missingPrice, lines_without_currency: missingCurrency,
    price_coverage: missingPrice || missingCurrency ? 'incomplete' : 'complete_at_capture',
    pricing_state: 'observed_only', tax_and_delivery: 'not_calculated', is_quote: false,
    availability_state: 'not_reconfirmed', effect: 'none' };
}

/** Separate affected selected content from a catalogue-wide evidence refresh. No work is executed. */
export function compareSelections(before, beforeCatalogue, after, afterCatalogue) {
  const left = validateSelection(before, beforeCatalogue), right = validateSelection(after, afterCatalogue);
  if (left.project_ref !== right.project_ref || left.store !== right.store) fail('Cannot compare different projects or stores.');
  const a = new Map(left.items.map(line => [lineKey(line), line])), b = new Map(right.items.map(line => [lineKey(line), line]));
  const added = [], removed = [], changed = [], evidenceOnly = [];
  const evidenceFields = new Set(['source_sha256', 'observed_at']);
  for (const [key, item] of a) {
    if (!b.has(key)) { removed.push(key); continue; }
    const next = b.get(key), fields = Object.keys(item).filter(field => canonical(item[field]) !== canonical(next[field]));
    const content = fields.filter(field => !evidenceFields.has(field));
    if (content.length) changed.push({ key, fields: content });
    else if (fields.length) evidenceOnly.push({ key, fields });
  }
  for (const key of b.keys()) if (!a.has(key)) added.push(key);
  const projectFields = ['title', 'purpose', 'requirements'].filter(field => canonical(left[field]) !== canonical(right[field]));
  const orderChanged = canonical([...a.keys()]) !== canonical([...b.keys()]);
  return { added, removed, changed, evidence_only: evidenceOnly, project_fields: projectFields,
    order_changed: orderChanged,
    needs_content_review: Boolean(added.length || removed.length || changed.length || projectFields.length || orderChanged),
    version_changed: canonical(left) !== canonical(right), approval_carried_forward: false, effect: 'none' };
}

/** Text can be downloaded or shown with textContent. Do not insert it as HTML. */
export function selectionBrief(selection, catalogue) {
  const valid = validateSelection(selection, catalogue), summary = selectionSummary(valid, catalogue);
  const lines = [valid.title, '', valid.purpose, '', 'Selected items'];
  for (const item of valid.items) {
    const price = item.observed_unit_price === null ? 'price not supplied'
      : item.currency ? item.currency + ' ' + item.observed_unit_price + ' each at capture'
      : item.observed_unit_price + ' each; currency not declared';
    lines.push(`${item.quantity} × ${item.title} / ${item.option} (${price})`, item.source_url);
    if (item.note) lines.push('Note: ' + item.note);
    if (item.shipping_at_capture === false) lines.push('Nonshipping item in the retained catalogue.');
    if (item.available_at_capture === false) lines.push('Unavailable at capture; ask the business about alternatives.');
  }
  if (valid.requirements.length) lines.push('', 'Questions and requirements', ...valid.requirements.map(item => '- ' + item));
  if (summary.observed_subtotals.length) lines.push('', ...summary.observed_subtotals.map(total => 'Observed subtotal: ' + total.currency + ' ' + total.amount));
  if (summary.price_coverage === 'incomplete') lines.push('The subtotal is incomplete: some prices or currencies are not supplied.');
  lines.push('For enquiry only. Confirm price, availability, tax and delivery with the business.');
  return lines.filter(line => line !== undefined).join('\n');
}

/** Same field-envelope shape as ServiceSite intake. Requires a declared selection route before dispatch. */
export function selectionRequest({ form, page, name, email }, selection, catalogue) {
  text(form, 160, 'form');
  if (typeof page !== 'string' || !/^\/(?!\/)[^?#\s]*$/.test(page)) fail('Invalid local page.');
  const sender = text(name, 300, 'name'), reply = text(email, 254, 'email');
  if (!/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(reply)) fail('Add a valid reply email.');
  const valid = validateSelection(selection, catalogue);
  const fields = [{ label: 'Name', value: sender }, { label: 'Email', value: reply },
    { label: 'Customer selection', value: JSON.stringify(valid) }];
  const body = JSON.stringify({ form, page, fields }), bytes = value => new TextEncoder().encode(value).length;
  if (fields[2].value.length > 18000 || bytes(body) > limit.requestBytes
      || bytes(JSON.stringify(JSON.stringify(fields))) > limit.requestBytes) fail('Selection is too large for one enquiry. Keep the local copy and arrange a larger handoff.');
  return body;
}
