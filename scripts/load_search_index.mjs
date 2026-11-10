#!/usr/bin/env bun
// Publish bounded, address-keyed search projections from the retained item archive.
import { Database } from "bun:sqlite";
import { performance } from "node:perf_hooks";
import { homedir } from "node:os";
import { join } from "node:path";

function parse(argv) {
  const args = { archive: join(homedir(), ".bonfyre/estate-local/items.db"),
    url: process.env.BONFYRE_SEARCH_INDEX_URL || "", attempts: 5 };
  for (let i = 0; i < argv.length; i++) {
    const key = argv[i];
    if (!["--archive", "--url", "--attempts"].includes(key) || !argv[i + 1])
      throw new Error(`unknown or incomplete argument: ${key}`);
    args[key.slice(2)] = argv[++i];
  }
  args.attempts = Math.max(1, Number(args.attempts));
  if (!Number.isInteger(args.attempts)) throw new Error("--attempts must be an integer");
  const ref = process.env.SUPABASE_PROJECT_REF;
  const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
  if (!key || (!ref && !args.url)) throw new Error("search index credentials are required");
  let url = args.url || `https://${ref}.supabase.co/rest/v1/estate_item?on_conflict=address`;
  if (!url.includes("on_conflict=")) url += `${url.includes("?") ? "&" : "?"}on_conflict=address`;
  return { ...args, url, key };
}

const delay = ms => new Promise(resolve => setTimeout(resolve, ms));

export async function submit(batch, args) {
  let last = "unknown failure";
  for (let attempt = 0; attempt < args.attempts; attempt++) {
    try {
      const response = await fetch(args.url, { method: "POST",
        headers: { apikey: args.key, Authorization: `Bearer ${args.key}`,
          "Content-Type": "application/json", Prefer: "resolution=merge-duplicates,return=minimal" },
        body: JSON.stringify(batch), signal: AbortSignal.timeout(90_000) });
      if (response.ok) { await response.arrayBuffer(); return null; }
      last = `HTTP ${response.status}`;
      if (response.status !== 408 && response.status !== 429 && response.status < 500) break;
    } catch (error) { last = String(error).slice(0, 120); }
    if (attempt + 1 < args.attempts) await delay(Math.min(2000, 250 * 2 ** attempt));
  }
  return last;
}

export async function flush(batch, args, result) {
  if (!batch.length) return;
  const error = await submit(batch, args);
  if (error === null) { result.sent += batch.length; return; }
  if (batch.length > 1) {
    const middle = Math.floor(batch.length / 2);
    await flush(batch.slice(0, middle), args, result);
    await flush(batch.slice(middle), args, result);
    return;
  }
  result.failed++;
  console.error(`item refused after ${args.attempts} attempt(s): ${error.slice(0, 120)} (${batch[0].address || "unknown"})`);
}

function clipped(value, length) { return Array.from(String(value || "")).slice(0, length).join(""); }

export async function main(argv = process.argv.slice(2)) {
  const args = parse(argv);
  const db = new Database(args.archive, { readonly: true });
  const started = performance.now();
  const seen = new Set();
  const result = { sent: 0, failed: 0 };
  let batch = [];
  try {
    const rows = db.query("SELECT source,key,kind,COALESCE(family,'') family,title,COALESCE(body,'') body,COALESCE(href,'') href,COALESCE(image,'') image,COALESCE(publisher,'') publisher,COALESCE(at,0) at,COALESCE(first_read,'') first_read FROM item").iterate();
    for (const row of rows) {
      const address = `o/${row.source}/${row.key}`;
      if (seen.has(address)) continue;
      seen.add(address);
      batch.push({ address, source: row.source, key: row.key, kind: row.kind,
        family: row.family, title: clipped(row.title, 600), body: clipped(row.body, 2000),
        href: clipped(row.href, 900), image: clipped(row.image, 900),
        publisher: clipped(row.publisher, 200), at: row.at, first_read: row.first_read });
      if (batch.length >= 500) {
        await flush(batch, args, result);
        batch = [];
        if (result.sent % 5000 === 0) console.error(`  ${result.sent} loaded`);
      }
    }
    await flush(batch, args, result);
  } finally { db.close(); }
  console.log(JSON.stringify({ loaded: result.sent, refused: result.failed,
    seconds: Math.round((performance.now() - started) / 100) / 10 }));
  return result.failed ? 1 : 0;
}

if (import.meta.main) {
  try { process.exitCode = await main(); }
  catch (error) { console.error(error.message); process.exitCode = 1; }
}
