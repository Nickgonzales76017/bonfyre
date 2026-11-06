import { test } from 'node:test';
import assert from 'node:assert/strict';
import { makeSelection, validateSelection, selectionKey, selectionVersion, selectionSummary,
  compareSelections, selectionBrief, selectionRequest } from '../catalogue-selection.mjs';

const hash = 'a'.repeat(64), nextHash = 'b'.repeat(64);
function fixture(currency = 'USD') {
  const store = { id: 'sample-store', origin: 'https://shop.example', sha256: hash };
  if (currency !== undefined) store.currency = currency;
  const product = (id, price, kind = 'goods') => ({ id, store: store.id, title: 'Item ' + id, kind,
    source_url: 'https://shop.example/products/' + id, source_sha256: hash, observed_at: '2026-10-08T00:00:00Z',
    variants: [{ id: id + '1', label: 'Standard', sku: null, price, available: true, shipping: kind === 'goods' }] });
  return { stores: [store], rows: [product('10', '19.50'), product('20', '0.10'), product('30', '5.00', 'service')] };
}
function intent(items = [{ product_id: '10', variant_id: '101', quantity: 2 }]) {
  return { project_ref: 'project:display', store: 'sample-store', title: 'My display',
    purpose: 'A countertop display', requirements: ['Please confirm the dimensions.'], items };
}
const clone = value => structuredClone(value);
const one = catalogue => makeSelection(intent(), catalogue);

test('builds a usable enquiry with preserved source facts, without changing the catalogue', () => {
  const catalogue = fixture(), original = clone(catalogue), draft = one(catalogue);
  assert.deepEqual(catalogue, original);
  assert.equal(draft.items[0].sku, null);
  assert.equal(draft.items[0].source_sha256, hash);
  assert.equal(draft.items[0].observed_unit_price, '19.50');
  assert.deepEqual(validateSelection(draft, catalogue), draft);
  const summary = selectionSummary(draft, catalogue);
  assert.deepEqual(summary.observed_subtotals, [{ currency: 'USD', amount: '39.00' }]);
  assert.equal(summary.is_quote, false);
  assert.equal(summary.effect, 'none');
  assert.match(selectionBrief(draft, catalogue), /2 × Item 10/);
  assert.match(selectionBrief(draft, catalogue), /confirm the dimensions/);
});

test('uses exact cents and keeps nonshipping services distinct', () => {
  const catalogue = fixture(), draft = makeSelection(intent([
    { product_id: '20', variant_id: '201', quantity: 3 }, { product_id: '30', variant_id: '301', quantity: 1 }
  ]), catalogue);
  assert.equal(selectionSummary(draft, catalogue).observed_subtotals[0].amount, '5.30');
  assert.equal(draft.items[1].kind, 'service');
  assert.equal(draft.items[1].shipping_at_capture, false);
  assert.match(selectionBrief(draft, catalogue), /Nonshipping item/);
});

test('missing price is not zero and missing currency is not silently USD', () => {
  const catalogue = fixture(); catalogue.rows[0].variants[0].price = null;
  let draft = one(catalogue), summary = selectionSummary(draft, catalogue);
  assert.deepEqual(summary.observed_subtotals, []);
  assert.equal(summary.lines_without_price, 1);
  assert.equal(summary.price_coverage, 'incomplete');
  assert.match(selectionBrief(draft, catalogue), /price not supplied/);
  catalogue.rows[0].variants[0].price = '19.50'; delete catalogue.stores[0].currency;
  draft = one(catalogue); summary = selectionSummary(draft, catalogue);
  assert.deepEqual(summary.observed_subtotals, []);
  assert.equal(summary.lines_without_currency, 1);
  assert.match(selectionBrief(draft, catalogue), /currency not declared/);
});

test('a real zero price stays visible and availability remains an observation', () => {
  const catalogue = fixture(); catalogue.rows[0].variants[0].price = '0';
  catalogue.rows[0].variants[0].available = false;
  const draft = one(catalogue);
  assert.equal(selectionSummary(draft, catalogue).observed_subtotals[0].amount, '0.00');
  assert.equal(selectionSummary(draft, catalogue).availability_state, 'not_reconfirmed');
  assert.match(selectionBrief(draft, catalogue), /Unavailable at capture/);
});

test('rejects wrong stores, forged options, duplicates and malformed quantities', () => {
  const catalogue = fixture();
  for (const quantity of [0, -1, 1.5, '2', NaN, Infinity, 10001])
    assert.throws(() => makeSelection(intent([{ product_id: '10', variant_id: '101', quantity }]), catalogue));
  assert.throws(() => makeSelection({ ...intent(), store: 'elsewhere' }, catalogue));
  assert.throws(() => makeSelection(intent([{ product_id: '10', variant_id: '201', quantity: 1 }]), catalogue));
  assert.throws(() => makeSelection(intent([...intent().items, ...intent().items]), catalogue));
  assert.throws(() => makeSelection({ ...intent(), quote: true }, catalogue));
});

test('rejects substituted seller facts, release identity and unknown envelope fields', () => {
  const catalogue = fixture(), draft = one(catalogue);
  const mutations = [
    d => d.items[0].observed_unit_price = '0.01', d => d.items[0].source_url = 'https://elsewhere.example/products/10',
    d => d.items[0].source_sha256 = nextHash, d => d.items[0].sku = 'invented',
    d => d.items[0].available_at_capture = false, d => d.items[0].currency = 'EUR',
    d => d.items[0].title = 'Forged title', d => d.items[0].approved = true,
    d => d.state = 'approved', d => d.receipt = 'fake'
  ];
  for (const mutate of mutations) { const altered = clone(draft); mutate(altered); assert.throws(() => validateSelection(altered, catalogue)); }
});

test('source addresses, mixed releases and ambiguous retained identities refuse', () => {
  for (const url of ['http://shop.example/products/10', 'https://user:pass@shop.example/products/10',
    'https://elsewhere.example/products/10', 'https://shop.example/products/../private/10']) {
    const catalogue = fixture(); catalogue.rows[0].source_url = url; assert.throws(() => one(catalogue));
  }
  let catalogue = fixture(); catalogue.rows[0].source_sha256 = nextHash; assert.throws(() => one(catalogue));
  catalogue = fixture(); catalogue.rows.push(clone(catalogue.rows[0])); assert.throws(() => one(catalogue));
  catalogue = fixture(); catalogue.rows[0].variants.push(clone(catalogue.rows[0].variants[0])); assert.throws(() => one(catalogue));
  catalogue = fixture('usd'); assert.throws(() => one(catalogue));
});

test('local version survives JSON roundtrip but a changed requirement changes its identity', async () => {
  const catalogue = fixture(), draft = one(catalogue), version = await selectionVersion(draft, catalogue);
  assert.match(version, /^[a-f0-9]{64}$/);
  assert.equal(await selectionVersion(JSON.parse(JSON.stringify(draft)), catalogue), version);
  const reordered = Object.fromEntries(Object.entries(draft).reverse());
  assert.equal(await selectionVersion(reordered, catalogue), version);
  draft.requirements.push('Need it packed for collection.');
  assert.notEqual(await selectionVersion(draft, catalogue), version);
  assert.notEqual(selectionKey('project:a', 'sample-store'), selectionKey('project:b', 'sample-store'));
});

test('catalogue refresh distinguishes evidence-only changes from selected content changes', () => {
  const beforeCatalogue = fixture(), before = one(beforeCatalogue), next = clone(beforeCatalogue);
  next.stores[0].sha256 = nextHash;
  for (const row of next.rows) { row.source_sha256 = nextHash; row.observed_at = '2026-10-09T00:00:00Z'; }
  next.rows[1].title = 'Unselected product changed';
  let after = one(next), report = compareSelections(before, beforeCatalogue, after, next);
  assert.equal(report.changed.length, 0);
  assert.equal(report.evidence_only.length, 1);
  assert.equal(report.needs_content_review, false);
  assert.equal(report.version_changed, true);
  assert.equal(report.approval_carried_forward, false);
  next.rows[0].variants[0].price = '20.00'; after = one(next);
  report = compareSelections(before, beforeCatalogue, after, next);
  assert.deepEqual(report.changed[0].fields, ['observed_unit_price']);
  assert.equal(report.changed.length, 1);
  assert.equal(report.needs_content_review, true);
  assert.throws(() => validateSelection(before, next));
});

test('cross-project comparison refuses and request envelope preserves the exact chosen option', () => {
  const catalogue = fixture(), draft = one(catalogue), other = makeSelection({ ...intent(), project_ref: 'other' }, catalogue);
  assert.throws(() => compareSelections(draft, catalogue, other, catalogue));
  const body = JSON.parse(selectionRequest({ form: 'display-enquiry', page: '/display/', name: 'Demo Person', email: 'demo@example.test' }, draft, catalogue));
  assert.equal(body.form, 'display-enquiry');
  const recovered = JSON.parse(body.fields.find(field => field.label === 'Customer selection').value);
  assert.deepEqual(validateSelection(recovered, catalogue), draft);
  assert.equal(recovered.state, 'draft');
  assert.throws(() => selectionRequest({ form: 'x', page: '//other.example', name: 'N', email: 'n@example.test' }, draft, catalogue));
});

test('bounded request counts escaped native storage and does not execute markup', () => {
  const catalogue = fixture(), input = intent(); input.items[0].note = '<img src=x onerror=alert(1)>';
  const draft = makeSelection(input, catalogue);
  assert.match(selectionBrief(draft, catalogue), /<img src=x/); // This API is plain text, never HTML.
  draft.requirements = Array(24).fill('"'.repeat(950));
  assert.throws(() => selectionRequest({ form: 'x', page: '/', name: 'N', email: 'n@example.test' }, draft, catalogue), /too large/);
});


test('matches the existing ServiceSite decimal-cent contract without a runtime dependency', async () => {
  const { priceCents, money } = await import('../../../components/BonfyreServiceSite/catalogue-model.mjs');
  for (const value of ['0', '00.10', '19.5', '999999999.99', ' 20.00 ']) {
    const catalogue = fixture(); catalogue.rows[0].variants[0].price = value;
    assert.equal(one(catalogue).items[0].observed_unit_price, money(priceCents(value)));
  }
  for (const value of ['-1', '1.234', 'NaN', 'Infinity', 10]) {
    assert.throws(() => priceCents(value));
    const catalogue = fixture(); catalogue.rows[0].variants[0].price = value;
    assert.throws(() => one(catalogue));
  }
});
