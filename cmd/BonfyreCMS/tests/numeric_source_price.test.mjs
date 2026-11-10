import { test } from 'bun:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { Database } from 'bun:sqlite';

const root = resolve(import.meta.dir, '../../..');
const binary = process.env.BONFYRE_CMS_BIN ?? join(root, 'cmd/BonfyreCMS/bonfyre-cms');
const facts = JSON.parse(readFileSync(join(root, 'packs/aurekai-work-samples/twc-commercial-facts-20261009.json'), 'utf8'));

test('actual published supplier prices survive numeric and text input without losing cents', () => {
  const folder = mkdtempSync(join(tmpdir(), 'cms-source-prices-'));
  const path = join(folder, 'scratch.db');
  const run = args => {
    const x = Bun.spawnSync([binary, ...args, '--db', path, '--schemas', folder], { stdout: 'pipe', stderr: 'pipe' });
    return { code: x.exitCode, out: x.stdout.toString(), err: x.stderr.toString() };
  };
  const read = fn => { const db = new Database(path, { readonly: true }); try { return fn(db); } finally { db.close(); } };
  const ok = args => { const r = run(args); assert.equal(r.code, 0, r.err); return JSON.parse(r.out); };
  try {
    const applied = run(['schema', 'apply', join(root, 'cmd/BonfyreCMS/content-types/price_observation.json')]);
    assert.equal(applied.code, 0, applied.err);
    const rows = [];
    for (const offer of facts.offers) {
      const source = facts.sources[offer.provider];
      const body = {
        observation_id: offer.id, product_id: offer.id, currency: 'USD', status: 'draft',
        amount: Number(offer.amount_usd), kind: 'published_subscription_price_per_' + offer.billing,
        source_ref: source.url, receipt_ref: source.receipt, observed_at: source.observed_at,
        cannot_infer: 'Regression copy of public source evidence; not a supplier quotation or purchase.',
      };
      const created = ok(['entry', 'create', 'price_observation', JSON.stringify(body), '--ns', 'regression-only']);
      const stored = () => read(db => db.query('SELECT amount FROM price_observation WHERE id=?').get(created.id).amount);
      assert.equal(stored(), String(Number(offer.amount_usd)));
      ok(['entry', 'update', 'price_observation', String(created.id), JSON.stringify({ amount: offer.amount_usd }), '--ns', 'regression-only']);
      assert.equal(stored(), offer.amount_usd);
      ok(['entry', 'update', 'price_observation', String(created.id), JSON.stringify({ amount: Number(offer.amount_usd) }), '--ns', 'regression-only']);
      assert.equal(stored(), String(Number(offer.amount_usd)));
      const native = ok(['entry', 'get', 'price_observation', String(created.id), '--ns', 'regression-only']);
      assert.equal(Number(native.data.attributes.amount), Number(offer.amount_usd));
      rows.push({ source: offer.id, amount: stored() });
    }
    assert.equal(rows.length, 5);
    assert.ok(rows.some(row => row.amount === '349.99'));
    const rejected = run(['entry', 'update', 'price_observation', '1', '{"amount":349.99junk}', '--ns', 'regression-only']);
    assert.notEqual(rejected.code, 0);
    assert.match(rejected.err, /valid JSON object/);
    assert.equal(read(db => db.query('SELECT count(*) n FROM price_observation').get().n), 5);
    console.log(JSON.stringify({ real_source_prices: rows, numeric_create_and_update: true, production_writes: 0 }));
  } finally { rmSync(folder, { recursive: true, force: true }); }
});

test('Frappe decimal storage rejects invalid inputs without changing the stored amount', () => {
  const folder = mkdtempSync(join(tmpdir(), 'cms-frappe-decimal-'));
  const path = join(folder, 'scratch.db');
  const schema = join(root, 'packs/wholesale-20k/schemas/supplier-quotation.json');
  const name = 'frappe_erpnext_supplier_quotation';
  const offer = facts.offers.find(row => row.provider === 'inspirable');
  const run = args => {
    const x = Bun.spawnSync([binary, ...args, '--db', path, '--schemas', folder], { stdout: 'pipe', stderr: 'pipe' });
    return { code: x.exitCode, out: x.stdout.toString(), err: x.stderr.toString() };
  };
  const amount = () => { const db = new Database(path, { readonly: true }); try { return db.query(`SELECT grand_total FROM ${name}`).get().grand_total; } finally { db.close(); } };
  try {
    assert.equal(run(['schema', 'apply', schema]).code, 0);
    // Storage conformance only, using the observed price; no live quotation is created.
    const body = { naming_series: 'PUR-SQTN-.YYYY.-', supplier: offer.provider, company: 'Aurekai', currency: 'USD', conversion_rate: 1, transaction_date: '2026-10-09', grand_total: Number(offer.amount_usd) };
    const created = run(['entry', 'create', name, JSON.stringify(body), '--ns', 'regression-only']);
    assert.equal(created.code, 0, created.err);
    const id = String(JSON.parse(created.out).id);
    assert.equal(amount(), 349.99);
    for (const raw of ['{"grand_total":3.4999e2}', '{"grand_total":"349.99"}']) {
      const result = run(['entry', 'update', name, id, raw, '--ns', 'regression-only']);
      assert.equal(result.code, 0, result.err);
      assert.equal(amount(), 349.99);
    }
    for (const raw of ['{"grand_total":1e309}', '{"grand_total":"NaN"}', '{"grand_total":"349.99 USD"}', '{"grand_total":"0x10"}', '{"grand_total":true}']) {
      const rejected = run(['entry', 'update', name, id, raw, '--ns', 'regression-only']);
      assert.notEqual(rejected.code, 0, raw);
      assert.match(rejected.err, /invalid numeric value/);
      assert.equal(amount(), 349.99);
    }
  } finally { rmSync(folder, { recursive: true, force: true }); }
});
