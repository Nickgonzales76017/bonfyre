#!/usr/bin/env bun
// Feed bodies are only decoded after their observation receipt verifies.
import { createHash } from 'node:crypto';
import { readFileSync, statSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

let cachedPolicy;
export function feedObservationPolicy() {
  if (!cachedPolicy) {
    const path = fileURLToPath(new URL('./feed-observation.cue', import.meta.url));
    const result = Bun.spawnSync(['cue', 'export', path, '-e', 'feedObservation']);
    if (result.exitCode !== 0) throw new Error(new TextDecoder().decode(result.stderr).trim());
    cachedPolicy = JSON.parse(new TextDecoder().decode(result.stdout));
  }
  return cachedPolicy;
}

const field = value => value == null ? '' : String(value);
const jsonFeedItems = document => (document.items ?? []).map(item => ({
  declared_id: field(item.id || item.url), title: field(item.title), url: field(item.url),
  declared_time: field(item.date_modified || item.date_published),
  summary: field(item.summary || item.content_text),
  author: field(item.authors?.[0]?.name ?? item.author?.name),
}));

export function decodeFeed(raw, options = {}) {
  let start = 0;
  while (start < raw.length && [9, 10, 13, 32].includes(raw[start])) start++;
  if (raw[start] === 123) {
    const document = JSON.parse(raw.toString('utf8'));
    if (!document || typeof document !== 'object' || !Array.isArray(document.items))
      throw new Error('not a JSON feed');
    return { carrier: 'json-feed', items: jsonFeedItems(document) };
  }
  const binary = options.xmlBinary ?? fileURLToPath(new URL('./foreign-xml', import.meta.url));
  const result = Bun.spawnSync([binary], { stdin: raw });
  if (result.exitCode !== 0) throw new Error('XML carrier decode failed');
  return JSON.parse(new TextDecoder().decode(result.stdout));
}

export class FeedError extends Error {
  constructor(result, code) { super(result.error); this.result = result; this.code = code; }
}

export function parseReceiptedFeed(receiptPath, options = {}, policy = feedObservationPolicy()) {
  const receipt = JSON.parse(readFileSync(receiptPath, 'utf8'));
  const bodyBytes = statSync(receipt.body_path).size;
  if (bodyBytes > policy.max_body_bytes) throw new FeedError({
    error: 'body_exceeds_parser_limit', receipt: receiptPath, bytes: bodyBytes,
    limit: policy.max_body_bytes, detail: 'Body was not read or parsed; receipt validity is unknown.',
  }, 4);
  const raw = readFileSync(receipt.body_path);
  const got = createHash('sha256').update(raw).digest('hex');
  if (got !== receipt.content_sha256) throw new FeedError({
    error: 'body_does_not_match_receipt', receipt: receiptPath,
    expected_sha256: receipt.content_sha256, actual_sha256: got,
    detail: policy.mismatch_detail,
  }, 3);
  if (raw.byteLength > policy.max_body_bytes) throw new FeedError({
    error: 'body_exceeds_parser_limit', receipt: receiptPath, bytes: raw.byteLength,
    limit: policy.max_body_bytes, detail: 'Body was not parsed; receipt validity is unknown.',
  }, 4);
  let carrier, items;
  try { ({ carrier, items } = decodeFeed(raw, options)); }
  catch { throw new FeedError({ error: 'not_a_feed', receipt: receiptPath, detail: policy.parse_note }, 4); }
  const candidates = [], seen = new Set();
  for (const item of items) {
    const id = item.declared_id || item.url || item.title;
    if (!id || seen.has(id)) continue;
    seen.add(id);
    const declares = Object.fromEntries(Object.entries({ ...item,
      summary: field(item.summary).slice(0, policy.summary_characters) }).filter(([, value]) => value));
    candidates.push({ kind: policy.candidate_kind, declares,
      observed: { source_url: receipt.source_url, observed_at: receipt.observed_at,
        content_sha256: receipt.content_sha256, twin_id: receipt.twin_id,
        receipt: receiptPath, carrier }, status: policy.candidate_status });
  }
  const result = { carrier, source_url: receipt.source_url, items_declared: items.length,
    candidates: candidates.length, forbidden_inference: policy.forbidden_inference };
  if (options.includeItems) result.items = candidates;
  return result;
}

function parseArgs(argv) {
  const asJson = argv.includes('--json');
  const args = argv.filter(arg => arg !== '--json');
  const twinIndex = args.indexOf('--twin');
  if (twinIndex >= 0) {
    const twin = args[twinIndex + 1];
    if (!twin || !/^[a-zA-Z0-9_-]+$/.test(twin)) throw new Error('invalid twin id');
    return { path: join(process.env.BONFYRE_FOREIGN_ROOT ?? '/var/lib/bonfyre/foreign',
      'receipts', `${twin}.json`), asJson };
  }
  if (args.length !== 1) throw new Error('usage: feed-parse.mjs <receipt.json> [--json] | --twin <id>');
  return { path: args[0], asJson };
}

if (import.meta.main) {
  try {
    const args = parseArgs(process.argv.slice(2));
    process.stdout.write(`${JSON.stringify(parseReceiptedFeed(args.path,
      { includeItems: args.asJson }), null, 2)}\n`);
  } catch (error) {
    if (error instanceof FeedError) {
      process.stdout.write(`${JSON.stringify(error.result, null, 2)}\n`);
      process.exitCode = error.code;
    } else {
      process.stderr.write(`${error.message}\n`);
      process.exitCode = 1;
    }
  }
}
