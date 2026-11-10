#!/usr/bin/env bun
import { Database } from 'bun:sqlite';
import { createHash } from 'node:crypto';

export const sha = value => createHash('sha256').update(String(value)).digest('hex');

// Python json.dumps(sort_keys=True, separators=(',', ':')) identity compatibility.
export function canonical(value) {
  const sorted = v => Array.isArray(v) ? v.map(sorted) : v && typeof v === 'object'
    ? Object.fromEntries(Object.keys(v).sort().map(key => [key, sorted(v[key])])) : v;
  return JSON.stringify(sorted(value)).replace(/[\u007f-\uffff]/g, char => `\\u${char.charCodeAt(0).toString(16).padStart(4, '0')}`);
}

export function occurrence(program, view, op, payload, stamp) {
  const payloadJson = canonical(payload);
  const payloadDigest = sha(payloadJson);
  return {
    id: sha(`${program}|${view}|${op}|${payloadDigest}|${stamp}`),
    programId: sha(`external-derivation|${program}`),
    view, op, payloadDigest, payloadJson, stamp,
  };
}

export async function* lines(body) {
  const decoder = new TextDecoder();
  let rest = '';
  for await (const chunk of body) {
    rest += decoder.decode(chunk, { stream: true });
    let end;
    while ((end = rest.indexOf('\n')) !== -1) {
      yield rest.slice(0, end).trim();
      rest = rest.slice(end + 1);
    }
  }
  rest += decoder.decode();
  if (rest.trim()) yield rest.trim();
}

export async function ingest({ db, url, pipeline, view, program, fetcher = fetch, now = () => new Date().toISOString().replace('Z', '+00:00') }) {
  const endpoint = `${url.replace(/\/+$/, '')}/v0/pipelines/${pipeline}/egress/${view}?format=json&backpressure=true`;
  const response = await fetcher(endpoint, { method: 'POST' });
  if (!response.ok || !response.body) throw new Error(`Feldera egress HTTP ${response.status}`);
  const insert = db.query('INSERT OR IGNORE INTO derived_occurrence VALUES (?,?,?,?,?,?,?,?,?,?)');
  let recorded = 0;
  for await (const line of lines(response.body)) {
    if (!line) continue;
    let doc;
    try { doc = JSON.parse(line); } catch { continue; }
    for (const event of Array.isArray(doc) ? doc : [doc]) {
      if (!event || typeof event !== 'object') continue;
      for (const op of ['insert', 'delete', 'update']) {
        if (!Object.hasOwn(event, op)) continue;
        const row = occurrence(program, view, op, event[op], now());
        const result = insert.run(row.id, row.programId, view, op, row.payloadDigest, row.payloadJson, null, row.stamp, 'derived', row.stamp);
        recorded += result.changes;
      }
    }
  }
  return recorded;
}

if (import.meta.main) {
  const args = process.argv.slice(2);
  const option = name => args[args.indexOf(name) + 1];
  if (['--db', '--pipeline', '--view', '--program'].some(name => !args.includes(name))) {
    console.error('usage: bun scripts/feldera-derived-egress.mjs --db DB --pipeline PIPELINE --view VIEW --program PROGRAM [--url URL]');
    process.exit(2);
  }
  let db;
  try {
    db = new Database(option('--db'));
    await ingest({ db, pipeline: option('--pipeline'), view: option('--view'), program: option('--program'), url: args.includes('--url') ? option('--url') : 'http://127.0.0.1:8080' });
  } catch (error) { console.error(`feldera-derived-egress: ${error.message}`); process.exitCode = 1; }
  finally { db?.close(); }
}
