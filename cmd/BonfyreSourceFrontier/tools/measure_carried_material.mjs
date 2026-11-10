#!/usr/bin/env bun
// Retained-body carriage measurement. Observation never establishes future supply.
import { Database } from "bun:sqlite";
import { createReadStream, existsSync, readFileSync, writeFileSync } from "node:fs";
import { basename, join } from "node:path";
import { createGunzip } from "node:zlib";

const cannotInfer = "carriage is what a retained body showed at one reading, not a promise that the source keeps publishing that material";

function options(argv) {
  const out = { cms: "", foreign: "", frontier: "", floor: 3, apply: false, evidence: "" };
  for (let i = 0; i < argv.length; i++) {
    const key = argv[i];
    if (key === "--apply") { out.apply = true; continue; }
    if (!["--cms", "--foreign", "--frontier", "--floor", "--evidence"].includes(key) || !argv[i + 1])
      throw new Error(`unknown or incomplete argument: ${key}`);
    out[key.slice(2)] = argv[++i];
  }
  out.floor = Number(out.floor);
  if (!out.cms || !out.foreign || !Number.isInteger(out.floor) || out.floor < 0)
    throw new Error("--cms and --foreign are required; --floor must be a nonnegative integer");
  return out;
}

function bodyPath(foreign, twin) {
  for (const suffix of [".body", ".body.gz"]) {
    const path = join(foreign, "bodies", twin + suffix);
    if (existsSync(path)) return path;
  }
  const receipt = join(foreign, "receipts", twin + ".json");
  if (!existsSync(receipt)) return "";
  try {
    const path = JSON.parse(readFileSync(receipt, "utf8")).body_path || "";
    if (path && existsSync(path)) return path;
    if (path && existsSync(path + ".gz")) return path + ".gz";
  } catch (_) { /* unreadable receipt is not evidence */ }
  return "";
}

async function bodyText(path) {
  let source;
  try {
    source = createReadStream(path);
    const stream = path.endsWith(".gz") ? source.pipe(createGunzip()) : source;
    const chunks = [];
    let size = 0;
    for await (const chunk of stream) {
      const take = Math.min(chunk.length, 4_000_000 - size);
      if (take > 0) chunks.push(chunk.subarray(0, take));
      size += take;
      if (size >= 4_000_000) break;
    }
    return new TextDecoder("utf-8").decode(Buffer.concat(chunks, size));
  } catch (_) { return ""; }
  finally { source?.destroy(); }
}

function count(text, pattern) {
  let total = 0;
  pattern.lastIndex = 0;
  while (pattern.exec(text)) total++;
  return total;
}

export function measure(text, floor) {
  if (!text) return {};
  if (text.trimStart().slice(0, 15).toUpperCase().startsWith("BEGIN:VCALENDAR")) {
    const events = count(text, /BEGIN:VEVENT/g);
    return events >= floor ? { events } : {};
  }
  const candidates = {
    podcasts: count(text, /type=["']audio\//gi),
    video: count(text, /type=["']video\//gi) + count(text, /<yt:videoId>|<media:group/gi),
    images: count(text, /<media:content[^>]+type=["']image\/|<enclosure[^>]+type=["']image\//gi),
  };
  const items = count(text, /<item[\s>]|<entry[\s>]/gi) || 1;
  return Object.fromEntries(Object.entries(candidates).filter(([, n]) => n >= floor && n * 3 >= items));
}

function hasColumn(db, table, column) {
  return db.query(`PRAGMA table_info(${table})`).all().some(row => row.name === column);
}

async function main() {
  const args = options(process.argv.slice(2));
  const cms = new Database(args.cms);
  cms.exec("PRAGMA busy_timeout=60000");
  const rows = cms.query("SELECT source, family, COALESCE(twin,'') twin FROM internet_source WHERE status='published' AND COALESCE(twin,'')<>'' GROUP BY source").all();
  let seen = 0, unread = 0, written = 0;
  const totals = new Map(), byMaterial = {};
  const evidence = [];
  const updates = [];
  for (const row of rows) {
    seen++;
    const path = bodyPath(args.foreign, row.twin);
    if (!path) { unread++; continue; }
    const carried = measure(await bodyText(path), args.floor);
    delete carried[row.family];
    const materials = Object.keys(carried).sort();
    if (!materials.length) continue;
    for (const material of materials) byMaterial[material] = (byMaterial[material] || 0) + 1;
    totals.set(row.source, carried);
    if (args.evidence) evidence.push(JSON.stringify({ source: row.source, family: row.family, carries: carried, body: basename(path) }));
    if (args.apply) updates.push([materials.join(","), row.source]);
  }
  if (args.apply) {
    cms.transaction(() => {
      if (!hasColumn(cms, "internet_source", "carries"))
        cms.exec("ALTER TABLE internet_source ADD COLUMN carries TEXT DEFAULT ''");
      const update = cms.query("UPDATE internet_source SET carries=? WHERE source=?");
      for (const [materials, source] of updates) { update.run(materials, source); written++; }
    })();
  }
  cms.close();
  if (args.evidence) writeFileSync(args.evidence, evidence.length ? evidence.join("\n") + "\n" : "");
  if (args.frontier && args.apply && existsSync(args.frontier)) {
    const frontier = new Database(args.frontier);
    frontier.exec("PRAGMA busy_timeout=60000");
    frontier.transaction(() => {
      if (!hasColumn(frontier, "channel", "carries")) frontier.exec("ALTER TABLE channel ADD COLUMN carries TEXT DEFAULT ''");
      const updateChannel = frontier.query("UPDATE channel SET carries=? WHERE census_source=?");
      for (const [source, carried] of totals) updateChannel.run(Object.keys(carried).sort().join(","), source);
    })();
    frontier.close();
  }
  console.log(JSON.stringify({ measured: seen, no_retained_body: unread, carrying: totals.size,
    written, by_material: byMaterial, floor: args.floor, at: new Date().toISOString().replace(/\.\d{3}Z$/, "Z"),
    cannot_infer: cannotInfer }));
}

if (import.meta.main) {
  try { await main(); } catch (error) { console.error(error.message); process.exitCode = 1; }
}
