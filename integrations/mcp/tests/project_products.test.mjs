// The three products as Project operations: data travels in the call, learned
// books are kept as named profiles, read-only operations are declared so, and
// a missing local model is reported instead of failing the learned results.
import { test, expect } from 'bun:test';
import { Database } from 'bun:sqlite';
import { mkdtempSync, rmSync, existsSync, statSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { PRODUCTS_PROJECT_TOOLS, createProjectProducts } from '../project_products.mjs';
import { CATALOG } from '../project_node.mjs';
import { validateArguments } from '../http_boundary.mjs';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '../../..');
const withHome = async fn => { const home = mkdtempSync(join(tmpdir(), 'products-mcp-')); try { return await fn(home); } finally { rmSync(home, { recursive: true, force: true }); } };
// Nothing listens here, so the local model is unavailable.
const env = home => ({ ...process.env, BONFYRE_LOCAL_MODEL: 'http://127.0.0.1:9', BONFYRE_ITEM_ARCHIVE_DB: join(home, 'items.db') });
const history = ['Description,Category', ...Array.from({ length: 12 }, (_, i) => `SHELL OIL ${1000 + i},Fuel`),
  ...Array.from({ length: 12 }, (_, i) => `NETFLIX.COM ${i},Subscription`)].join('\n');

test('the products are Project operations with honest read-only declarations', () => {
  const names = PRODUCTS_PROJECT_TOOLS.map(tool => tool.name);
  expect(names).toEqual(['books_learn', 'books_profiles', 'books_book', 'monitor_mentions', 'manifest_lot']);
  for (const name of names) expect(CATALOG.tools.some(tool => tool.name === name && tool['x-bonfyre-owner'] === 'products')).toBe(true);
  const readOnly = Object.fromEntries(PRODUCTS_PROJECT_TOOLS.map(tool => [tool.name, tool.annotations.readOnlyHint]));
  expect(readOnly).toEqual({ books_learn: false, books_profiles: true, books_book: true, monitor_mentions: true, manifest_lot: true });
  const book = PRODUCTS_PROJECT_TOOLS.find(tool => tool.name === 'books_book').inputSchema;
  expect(() => validateArguments({ transactions_csv: 'x', columns: { amount: 'Amt' } }, book)).not.toThrow();
  expect(() => validateArguments({ transactions_csv: 'x', path: '/etc/passwd' }, book)).toThrow('not allowed');
});

test('learn keeps a named profile; booking books sure lines, holds unsure ones and unread amounts, and says the model was unavailable', () => withHome(async home => {
  const call = createProjectProducts({ root, home, env: env(home) });
  const learned = await call('books_learn', { profile: 'acme', history_csv: history });
  expect(learned).toMatchObject({ profile: 'acme', accounts: ['Fuel', 'Subscription'], learned_from_lines: 24 });
  expect(statSync(learned.kept_at).mode & 0o777).toBe(0o600);
  expect((await call('books_profiles', {})).profiles).toEqual([expect.objectContaining({ profile: 'acme', learned_from_lines: 24 })]);
  const booked = await call('books_book', { profile: 'acme', transactions_csv:
    'Date,Description,Amount\n2026-09-01,SHELL OIL 4471,(40.00)\n2026-09-02,NETFLIX.COM 99,oops\n2026-09-03,SIGHTGLASS CAFE,-6.00\n' });
  expect(booked).toMatchObject({ lines: 3, booked: 1, held_for_review: 2, amounts_unread: 1 });
  expect(booked.booked_sample[0]).toMatchObject({ description: 'SHELL OIL 4471', account: 'Fuel', amount: '-40.00' });
  expect(booked.held.find(row => row.description === 'NETFLIX.COM 99')).toMatchObject({ amount_as_given: 'oops', reason: 'amount not read' });
  expect(booked.local_model_unavailable).toContain('not answering');
  await expect(call('books_book', { profile: '../etc', transactions_csv: 'Description\nx\n' })).rejects.toThrow('profile must be');
  await expect(call('books_book', { profile: 'nobody', transactions_csv: 'Description\nx\n' })).rejects.toThrow('no books profile');
  await expect(call('books_learn', { profile: 'tiny', history_csv: 'Description,Category\nA,B\n' })).rejects.toThrow('at least 20');
  expect(existsSync(join(home, '.bonfyre/estate-local/books/tiny.json'))).toBe(false);
}));

test('monitor answers from the archive without loading a model when nothing matches; manifest says plainly when the model is needed', () => withHome(async home => {
  const db = new Database(join(home, 'items.db'));
  db.run(`CREATE TABLE item(source TEXT, key TEXT, kind TEXT, family TEXT, title TEXT, body TEXT DEFAULT '', href TEXT DEFAULT '', publisher TEXT DEFAULT '', at INTEGER)`);
  db.close();
  const call = createProjectProducts({ root, home, env: env(home) });
  const quiet = await call('monitor_mentions', { watch: ['Fernhollow'], about: 'Fernhollow Bakery in Denton' });
  expect(quiet).toMatchObject({ candidates: 0, model: null, mentions: [] });
  expect(quiet.digest).toContain('no model was asked');
  await expect(call('manifest_lot', { listings: [{ name: 'lot.html', text: '<p>QR Code Link to This Post 40 Dell OptiPlex 7050 desktops, untested</p>' }] }))
    .rejects.toThrow('not answering');
}));
