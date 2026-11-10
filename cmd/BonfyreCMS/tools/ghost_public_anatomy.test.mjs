import { test, expect } from 'bun:test';
import { createHash } from 'node:crypto';
import { observedRow, observeGhostPage } from './ghost_public_anatomy.mjs';

const html=Buffer.from(`<!doctype html><html><head><link rel='stylesheet' href='/style.css'></head><body><header><nav><a href='/'>Home</a></nav></header><main><h1>Measured</h1><img width='1600' height='900' src='/hero.jpg'><section><article><h2>One</h2></article><article><h2>Two</h2></article></section><form><button>Submit</button></form></main></body></html>`);
test('bounded public anatomy retains only digests and structural measurements', async () => {
  const row=await observedRow('ghost-proof','https://example.test/',html,[Buffer.from('@media (max-width: 40rem){}')]);
  const vector=row.visual_vector;
  expect(row.status).toBe('observed_reference');
  expect(vector.html_sha256).toBe(createHash('sha256').update(html).digest('hex'));
  expect(vector.masthead).toEqual({headers:1,navs:1,links_in_header:1});
  expect(vector.hero.h1_before_first_article).toBe(true);
  expect(vector.media_geometry.declared_aspect_ratios).toEqual({'1.78':1});
  expect(vector.typed_slots).toEqual({h1:1,h2:2,h3:0,article:2,section:1});
  expect(JSON.stringify(row)).not.toContain('<article');
  expect(JSON.stringify(row)).not.toContain('/style.css');
  expect(row.cannot_infer).toContain('authority');
});
test('same-origin stylesheets enter only through a native receipt-bound observer', async () => {
  const page=Buffer.from("<html><head><link rel='stylesheet' href='/style.css'><link rel='stylesheet' href='https://other.test/donor.css'></head><body><h1>One</h1></body></html>");
  const calls=[];
  const observeOne=async (url, options)=>{ calls.push([url,options.maxBytes]); return url.endsWith('style.css')
    ? {body:Buffer.from('@media(max-width:40rem){}'),receipt:{content_sha256:'css',http_evidence:{content_type:'text/css'}},receiptPath:'foreign:css'}
    : {body:page,receipt:{content_sha256:'html',http_evidence:{content_type:'text/html'}},receiptPath:'foreign:page'}; };
  const row=await observeGhostPage('proof','https://example.test/',{project:'native-project',foreignRoot:'foreign-root',observeOne});
  expect(calls).toEqual([['https://example.test/',2_000_000],['https://example.test/style.css',1_000_000]]);
  expect(row.observation_receipts).toEqual({page:'foreign:page',stylesheets:['foreign:css']});
  expect(row.visual_vector.stylesheet_count_observed).toBe(1);
  expect(row.cannot_infer).toContain('released temporary observation bytes');
});
