#!/usr/bin/env bun
// Retains the research desk's starting search: the exact OpenAlex and Europe
// PMC answers for one question, with the address, time and digest of each, so
// the sample page opens on real records without asking either service.
//
//   bun components/BonfyreServiceSite/tools/retain-synthesis-sample.mjs OUT.json
import { writeFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { openAlexUrl, europePmcUrl, fromOpenAlex, fromEuropePmc, merge } from '../synthesis.mjs';

const out = process.argv[2];
if (!out) { console.error('usage: retain-synthesis-sample.mjs OUT.json'); process.exit(2); }
const question = 'What happens to productivity and wellbeing when a workplace moves to a four-day week?';
const search = { query: '"four-day week" OR "four day work week" OR "4-day week" productivity wellbeing', from: 2010, to: 2026 };
const read = async url => { const res = await fetch(url, { headers: { 'User-Agent': 'Aurekai work samples (+https://aurekai.work/)' } }); const body = await res.text(); if (!res.ok) throw Error(`${res.status} from ${url}`);
  return { url, fetched_at: new Date().toISOString(), status: res.status, sha256: createHash('sha256').update(body).digest('hex'), json: JSON.parse(body) }; };
const oa = await read(openAlexUrl({ ...search, query: 'four-day work week productivity wellbeing', perPage: 50 }));
const ep = await read(europePmcUrl({ ...search, pageSize: 40 }));
const raw = [...oa.json.results.map(fromOpenAlex), ...ep.json.resultList.result.map(fromEuropePmc)].filter(r => r.abstract.length > 200);
const { records, duplicates } = merge(raw);
writeFileSync(out, JSON.stringify({ question, search, retrieved: [oa, ep].map(({ json, ...receipt }) => receipt), identified: raw.length, duplicates, records }) + '\n');
console.log(JSON.stringify({ identified: raw.length, duplicates: duplicates.length, records: records.length }));
