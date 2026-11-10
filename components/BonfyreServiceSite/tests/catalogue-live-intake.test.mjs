import { test, expect } from 'bun:test';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { createHash } from 'node:crypto';
import { reviewRequest } from '../catalogue-review.mjs';

// Explicit live verification against our work-samples site. The marked invalid
// submission must receive no receipt. No buyer site or merchant is contacted.
const live = process.env.CATALOGUE_VERIFY_LIVE === '1';
const site = join(import.meta.dir, '../../../packs/aurekai-work-samples/site');
const origin = 'https://aurekai-work-samples.pages.dev';
const digest = bytes => createHash('sha256').update(bytes).digest('hex');

test.skipIf(!live)('published catalogue matches this build and refuses an invented original value', async () => {
  for (const path of ['/catalogue/index.html', '/data/product-workspace/catalogue-model.mjs', '/data/product-workspace/catalogue-review.mjs']) {
    const response = await fetch(origin + path, { cache: 'no-store', signal: AbortSignal.timeout(20000) });
    expect(response.status).toBe(200);
    expect(digest(Buffer.from(await response.arrayBuffer()))).toBe(digest(readFileSync(join(site, 'dist', path))));
  }
  const input = JSON.parse(Buffer.from(Bun.gunzipSync(readFileSync(join(site, 'catalogue-input.json.gz')))).toString());
  const product = input.rows.find(p => p.store === 'red3-display');
  const draft = { schema: 'service-site.product-draft.v1', state: 'draft', product_id: product.id, store: product.store,
    source_url: product.source_url, source_sha256: product.source_sha256, observed_at: product.observed_at,
    saved_at: '2026-10-08T00:00:00Z', changes: [{ field: 'title', before: 'Bonfyre verification: deliberately incorrect original', after: 'Bonfyre verification only; refuse this draft' }] };
  const body = reviewRequest('catalogue-product-workspace', 'Bonfyre invalid-draft verification', 'verification@example.invalid', draft, '/catalogue/');
  const response = await fetch(origin + '/api/lead/catalogue-product-workspace', {
    method: 'POST', headers: { origin, 'content-type': 'application/json' }, body, signal: AbortSignal.timeout(20000)
  });
  const result = await response.json();
  expect(response.status).toBe(400);
  expect(result.ok).toBe(false);
  expect(result.why).toBe('The original field differs from the retained product. Reload the workspace.');
  expect(result.receipt).toBeUndefined();
}, 90000);
