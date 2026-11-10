#!/usr/bin/env bun
// Retains the desk's capabilities with no asking price, and what each one's
// address answers, so the pricing desk opens on real unpriced work.
//   bun components/BonfyreServiceSite/tools/retain-pricing-sample.mjs OUT.json
import { writeFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
const out = process.argv[2]; if (!out) { console.error('usage: retain-pricing-sample.mjs OUT.json'); process.exit(2); }
const desk = 'https://aurekai.life/studio/', res = await fetch(desk, { headers: { Accept: 'text/html' } }), html = await res.text();
const section = html.slice(html.indexOf('Capabilities with no asking price'), html.indexOf('Written but not published'));
const rows = [...section.matchAll(/class="desk-l">([^<]+)<\/span>\s*<span class="desk-s">([^<]+)<\/span>/g)].map(m => ({ id: m[1].trim(), what: m[2].replace(/&mdash;/g, '—').replace(/&amp;/g, '&').trim() }));
if (!rows.length) throw Error('the desk listed no unpriced capabilities, or its markup changed');
for (const r of rows) { const a = await fetch(`https://aurekai.life/capability/${r.id}`, { headers: { Accept: 'application/json' } }); r.answer = { status: a.status, body: await a.json().catch(() => null), at: new Date().toISOString() }; }
writeFileSync(out, JSON.stringify({ source: { url: desk, fetched_at: new Date().toISOString(), status: res.status, sha256: createHash('sha256').update(html).digest('hex') }, capabilities: rows }, null, 1) + '\n');
console.log(JSON.stringify(rows.map(r => [r.id, r.answer.status])));
