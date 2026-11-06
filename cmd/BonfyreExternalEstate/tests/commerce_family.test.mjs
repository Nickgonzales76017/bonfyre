import { test, expect } from 'bun:test';
import { chmodSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { resolve } from 'node:path';
import { compileEnvelope, classifyProbe, planRoute, expand, decimalPrice, minorPrice } from '../tools/compile_commerce_family.mjs';
import { activeProbes, frontierRows, hostOf, probeHosts } from '../../BonfyreSourceFrontier/tools/probe_storefronts.mjs';

const root = resolve(import.meta.dir, '../../..');
const providers = JSON.parse(readFileSync(resolve(root, 'schemas/control/commerce-observation-providers.json')));
const policy = JSON.parse(readFileSync(resolve(root, 'schemas/control/commerce-observation-routing.json')));
const common = { source_ref: 'source:test', receipt_ref: 'receipt:test', observed_at: '2026-09-28T00:00:00Z', merchant: 'tool-shop' };

test('exact money and malformed values', () => {
  expect(minorPrice('1299')).toBe('12.99');
  expect(minorPrice('000000000000000001', 100)).toBe('0.01');
  expect(decimalPrice('00012.5000')).toBe('12.5');
  expect(decimalPrice('1.25e2')).toBe('125');
  expect(decimalPrice('2.5e-2')).toBe('0.025');
  expect(decimalPrice('.50')).toBe('0.5');
  expect(decimalPrice('-0.00')).toBe('0');
  expect(() => decimalPrice('-0.01')).toThrow();
  expect(() => decimalPrice('NaN')).toThrow();
  expect(() => minorPrice(1.25)).toThrow();
});

test('receipt-bound Woo and Shopify observations stay distinct from authority', () => {
  const woo = { ...common, phenotype: 'woocommerce-store-api', payload: [{ id: 7, name: 'Driver',
    permalink: 'https://tool.test/p/driver', sku: 'D7', is_in_stock: true,
    prices: { price: '1299', currency_code: 'USD', currency_minor_unit: 2 } }] };
  const rows = compileEnvelope(providers, woo);
  expect(rows.map(row => row.type)).toEqual(['product_identity', 'merchant_offer', 'price_observation', 'availability_observation']);
  expect(rows[2].body.amount).toBe('12.99');
  expect(rows[2].body.product_id).toBe(rows[0].body.product_id);
  expect(rows.every(row => row.body.receipt_ref === common.receipt_ref)).toBe(true);
  expect(rows[1].body.cannot_infer).toContain('checkout authority');
  expect(classifyProbe(providers, { root: 'tool.test', responses: { '/wp-json/wc/store/v1/products': woo.payload } })[0].phenotype)
    .toBe('woocommerce-store-api');
  expect(frontierRows('tool.test', { '/wp-json/wc/store/v1/products': woo.payload })).toEqual([]);
  expect(frontierRows('tool.test', { '/wp-json/wc/store/v1/products': {
    body: woo.payload, receipt: { policy_decision: 'allow', http_status: 200 } } })[0].state).toBe('eligible');
  expect(hostOf('https://www.tool.test/path')).toBe('tool.test');
  const projected = expand(rows);
  expect(projected.sources[0].state).toBe('shadow');
  expect(projected.aliases.some(alias => alias.kind === 'sku')).toBe(true);

  const shop = { ...common, phenotype: 'shopify-ajax', payload: { id: 9, title: 'Kit', url: '/products/kit.js',
    vendor: 'Maker', variants: [{ id: 1, price: 2500, available: false, sku: 'K1', barcode: '123' }] } };
  const shopRows = compileEnvelope(providers, shop);
  expect(shopRows[0].body.gtin).toBe('123');
  expect(shopRows[2].body.amount).toBe('25.00');
  expect(shopRows[3].body.state).toBe('out_of_stock');
  expect(classifyProbe(providers, { root: 'maker.test', responses: { '/search/suggest.json':
    { resources: { results: { products: [{ url: '/products/kit', handle: 'kit' }] } } } } })[0].phenotype)
    .toBe('shopify-ajax');
});

test('eBay, Keepa, and historical paid prices preserve distinct row grammar', () => {
  const ebay = { ...common, provider: 'ebay-browse', payload: { itemSummaries: [{ itemId: 'v1|42',
    title: 'Used Driver', itemWebUrl: 'https://www.ebay.test/itm/42', price: { value: '12.50', currency: 'USD' },
    availabilityStatus: 'IN_STOCK', condition: 'Used', gtin: '000123' }] } };
  const ebayRows = compileEnvelope(providers, ebay);
  expect(ebayRows[0].body.gtin).toBe('000123');
  expect(ebayRows.at(-1).body.state).toBe('in_stock');

  const keepa = { ...common, provider: 'keepa', payload: { asin: 'B0001', title: 'Router',
    history: [{ observed_at: '2026-09-01T00:00:00Z', amount: '399.00', currency: 'USD' }],
    offers: [{ id: 'seller-1', href: 'https://amazon.test/dp/B0001', seller: 'Seller', amount: '401.25',
      currency: 'USD', available: true }] } };
  const keepaRows = compileEnvelope(providers, keepa);
  expect(keepaRows.filter(row => row.type === 'price_observation').map(row => row.body.kind))
    .toEqual(['historical', 'current']);

  const open = { ...common, provider: 'open-prices', payload: { items: [{ product_code: '3017620422003',
    product_name: 'Spread', price: 3.5, currency: 'EUR', location: { osm_name: 'Food Store' } }] } };
  const openRows = compileEnvelope(providers, open);
  expect(openRows.map(row => row.type)).toEqual(['product_identity', 'price_observation']);
  expect(openRows[1].body.offer_id).toBe('');
});

test('provider routing refuses undeclared effects and unimplemented provider', () => {
  const ready = planRoute(providers, policy, { intent: 'price.history', available: ['keepa'] });
  expect(ready.selected.provider).toBe('keepa');
  const waiting = planRoute(providers, policy, { intent: 'merchant.discover', available: [] });
  expect(waiting.selected).toBeNull();
  expect(waiting.choices[0].state).toBe('waiting_credential');
  expect(() => compileEnvelope(providers, { ...common, phenotype: 'magento-catalog-graphql', payload: [{}] })).toThrow();
  expect(() => compileEnvelope(providers, { ...common, phenotype: 'woocommerce-store-api', receipt_ref: '', payload: [{}] })).toThrow();
  expect(() => compileEnvelope(providers, { ...common, phenotype: 'woocommerce-store-api', payload: [] })).toThrow();
});

test('authored probe declarations drive both endpoint selection and shape classification', () => {
  const spec = structuredClone(providers);
  const woo = spec.phenotypes.find(item => item.id === 'woocommerce-store-api');
  woo.discovery.probes[0].request_path = '/new-public-products';
  woo.discovery.probes[0].response_key = '/new-public-products';
  expect(activeProbes(spec).map(rule => rule.request_path)).toContain('/new-public-products');
  const body = [{ id: 1, name: 'Test', prices: {} }];
  expect(classifyProbe(spec, { root: 'test.example', responses: { '/wp-json/wc/store/v1/products': body } })).toEqual([]);
  expect(frontierRows('test.example', { '/new-public-products': {
    body, receipt: { policy_decision: 'allow', http_status: 200 } } }, spec)[0].address)
    .toBe('https://test.example/new-public-products');
  expect(classifyProbe({ phenotypes: [] }, { root: 'test.example', responses: { '/new-public-products': body } })).toEqual([]);
  expect(classifyProbe(spec, { root: 'test.example', responses: { '/new-public-products': [{ id: 1, name: 'Test' }] } })).toEqual([]);
});

test('storefront probe promotes only receipted endpoint answers to eligible channels', async () => {
  const dir = mkdtempSync(resolve(tmpdir(), 'bonfyre-storefront-test.'));
  try {
    const project = resolve(dir, 'observer');
    writeFileSync(project, '#!/bin/sh\nbody=""\nwhile [ "$#" -gt 0 ]; do\n  if [ "$1" = "--body" ]; then shift; body="$1"; fi\n  shift\ndone\nprintf \'[{"id":7,"name":"Driver","prices":{"price":"100"}}]\\n\' > "$body"\n');
    chmodSync(project, 0o700);
    expect(await probeHosts(['tool.test'], project, 1)).toEqual([]);
    const receiptedProject = resolve(dir, 'receipted-observer');
    writeFileSync(receiptedProject, '#!/bin/sh\nbody=""\nreceipt=""\naddress="$4"\nwhile [ "$#" -gt 0 ]; do\n  case "$1" in\n    --body) shift; body="$1";;\n    --receipt) shift; receipt="$1";;\n  esac\n  shift\ndone\ncase "$address" in\n  */wp-json/*) printf \'[{"id":7,"name":"Driver","prices":{"price":"100"}}]\\n\' > "$body";;\n  *) printf \'{}\\n\' > "$body";;\nesac\nprintf \'{"policy_decision":"allow","http_status":200}\\n\' > "$receipt"\n');
    chmodSync(receiptedProject, 0o700);
    const rows = await probeHosts(['tool.test'], receiptedProject, 1);
    expect(rows.map(row => row.protocol)).toEqual(['woocommerce-store-api']);
    expect(rows[0].state).toBe('eligible');
    const deniedProject = resolve(dir, 'denied-observer');
    writeFileSync(deniedProject, readFileSync(receiptedProject, 'utf8').replace('"policy_decision":"allow"', '"policy_decision":"deny"'));
    chmodSync(deniedProject, 0o700);
    expect(await probeHosts(['tool.test'], deniedProject, 1)).toEqual([]);
    const failedProject = resolve(dir, 'failed-observer');
    writeFileSync(failedProject, readFileSync(receiptedProject, 'utf8').replace('"http_status":200', '"http_status":500'));
    chmodSync(failedProject, 0o700);
    expect(await probeHosts(['tool.test'], failedProject, 1)).toEqual([]);
  } finally { rmSync(dir, { recursive: true, force: true }); }
});
