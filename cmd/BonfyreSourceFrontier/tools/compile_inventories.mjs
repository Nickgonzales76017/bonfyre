#!/usr/bin/env bun
// Inventories remain separately labeled listings. Only census-link admits.
import { Database } from "bun:sqlite";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

let cachedPolicy;
const own = (record, key) => Object.hasOwn(record, key) ? record[key] : undefined;
export function inventoryPolicy() {
  if (!cachedPolicy) {
    const path = fileURLToPath(new URL('../inventory-projection.cue', import.meta.url));
    const result = Bun.spawnSync(['cue', 'export', path, '-e', 'sourceInventoryProjection']);
    if (result.exitCode !== 0) throw new Error(`CUE inventory projection: ${new TextDecoder().decode(result.stderr).trim()}`);
    const policy = JSON.parse(new TextDecoder().decode(result.stdout));
    cachedPolicy = { ...policy, compiled_hints: policy.protocol_hints.map(({ pattern, protocol }) =>
      ({ pattern: new RegExp(pattern, 'i'), protocol })) };
  }
  return cachedPolicy;
}

export function protocolOf(url, declared = "") {
  if (declared) return declared;
  for (const { pattern, protocol } of inventoryPolicy().compiled_hints)
    if (pattern.test(url)) return protocol;
  return "unknown";
}
export function listingRow(address, fields = {}) {
  const row = { address };
  for (const [key, value] of Object.entries(fields)) if (value) row[key] = value;
  if (!row.protocol) {
    row.protocol = protocolOf(address);
    if (row.protocol !== "unknown" && !row.discovered_from)
      row.discovered_from = inventoryPolicy().truth;
  }
  const material = row.material || own(inventoryPolicy().material_by_protocol, row.protocol);
  if (material) row.material = material;
  return row;
}
function lines(path) { return readFileSync(path, "utf8").split(/\r?\n/).filter(Boolean); }
function jsonLines(path) {
  const docs = [];
  for (const line of lines(path)) { try { docs.push(JSON.parse(line)); } catch {} }
  return docs;
}
function sqliteRows(path, sql) {
  const db = new Database(path, { readonly: true });
  try { return db.query(sql).all(); }
  catch (error) { if (String(error.message).includes("no such table") || String(error.message).includes("no such column")) return []; throw error; }
  finally { db.close(); }
}
function extId(text) { const match = /ext[-_](\d{5})/.exec(text ?? ""); return match ? `ext-${match[1]}` : ""; }

export function rowsFor(name, path, v4Urls = new Map()) {
  switch (name) {
    case "v4Jsonl": return jsonLines(path).flatMap(doc => {
      const url = doc?.primary_url;
      if (!url) return [];
      const kind = doc.source_kind ?? "";
      const protocol = own(inventoryPolicy().source_kind_protocol, kind) ?? "";
      const material = (doc.projection?.candidate_affordances ?? []).includes("PAY") ? "capability" : "";
      return [listingRow(url, { listing_id: doc.id ?? "", name: doc.observed_name ?? "", kind,
        truth_level: doc.truth_level ?? "observed_candidate", protocol, material })];
    });
    case "v4Sqlite": return sqliteRows(path, "SELECT id, observed_name, primary_url, source_kind FROM occurrence").flatMap(r =>
      r.primary_url ? [listingRow(r.primary_url, { listing_id: r.id ?? "", name: r.observed_name ?? "", kind: r.source_kind ?? "",
        truth_level: "observed_candidate", protocol: own(inventoryPolicy().source_kind_protocol, r.source_kind ?? "") ?? "" })] : []);
    case "advertisements": return sqliteRows(path, "SELECT identity, candidate_kind FROM candidate_advertisement").flatMap(r => {
      const url = v4Urls.get(extId(r.identity));
      return url ? [listingRow(url, { kind: r.candidate_kind ?? "" })] : [];
    });
    case "feeds": return sqliteRows(path, "SELECT COALESCE(label,'') AS label, COALESCE(url,'') AS url, "
      + "COALESCE(kind,'') AS kind, COALESCE(family,'') AS family FROM internet_source").flatMap(r =>
      r.url ? [listingRow(r.url, { name: r.label, protocol: r.kind.startsWith("rss") ? "rss" : "", material: "publication", kind: r.family })] : []);
    case "discovery": return lines(path).flatMap(line => {
      const parts = line.trim().split("|");
      return parts.length >= 2 && parts[1].startsWith("http") ? [listingRow(parts[1], { kind: "discovery" })] : [];
    });
    case "datasetRegistry": return jsonLines(path).flatMap(doc => {
      const url = doc?.url || doc?.landing_page || doc?.address;
      if (!url) return [];
      const refusal = doc.robots == null || doc.robots === "" || doc.robots === "allow" ? "" : `robots: ${doc.robots}`;
      return [listingRow(url, { name: doc.title || doc.name || "", material: "dataset", kind: doc.publisher ?? "", refusal })];
    });
    case "liveSources": {
      const doc = JSON.parse(readFileSync(path, "utf8"));
      return (Array.isArray(doc) ? doc : doc.rows ?? []).flatMap(r => r.url ? [listingRow(r.url,
        { name: r.publisher_name || r.label || "", identity: r.publisher_ident ?? "",
          material: own(inventoryPolicy().live_family_material, r.family) ?? "",
          kind: r.family ?? "", census_source: r.source ?? "", state: "eligible" })] : []);
    }
    case "passports": {
      const doc = JSON.parse(readFileSync(path, "utf8"));
      const passports = Array.isArray(doc) ? doc : doc.passports ?? doc.rows ?? [];
      return (Array.isArray(passports) ? passports : []).flatMap(r => [r.root, r.endpoint, ...(r.endpoints ?? [])]
        .filter(url => String(url ?? "").startsWith("http"))
        .map(url => listingRow(String(url), { name: r.name || r.id || "", kind: "public commons",
          rights: r.licence || r.rights || "", state: "eligible" })));
    }
    default: throw new Error(`unknown inventory: ${name}`);
  }
}

function options(argv) {
  const args = {};
  const keyFor = Object.fromEntries(inventoryPolicy().inventories.map(row => [row.arg, row.key]));
  Object.assign(keyFor, { "--db": "db", "--frontier": "frontier", "--out": "out" });
  for (let i = 0; i < argv.length; i++) {
    const key = keyFor[argv[i]];
    if (!key || i + 1 >= argv.length) throw new Error(`invalid argument: ${argv[i]}`);
    args[key] = argv[++i];
  }
  for (const key of ["db", "frontier"]) if (!args[key]) throw new Error(`missing --${key}`);
  args.out ??= join(dirname(resolve(args.db)), "listings");
  return args;
}
function native(argv) {
  const result = Bun.spawnSync(argv, { stdout: "pipe", stderr: "pipe" });
  return { code: result.exitCode, stdout: new TextDecoder().decode(result.stdout).trim(), stderr: new TextDecoder().decode(result.stderr).trim() };
}
export function compile(args, run = native) {
  mkdirSync(args.out, { recursive: true });
  const init = run([args.frontier, "init", "--db", args.db]);
  if (init.code !== 0) throw new Error(`frontier init failed: ${init.stderr}`);
  const v4Urls = new Map(), report = {};
  for (const { label, key, file } of inventoryPolicy().inventories) {
    const path = args[key];
    if (!path || !existsSync(path)) continue;
    const rows = rowsFor(key, path, v4Urls);
    const listing = join(args.out, `${label.toLowerCase()}.${file}.jsonl`);
    for (const row of rows) {
      const id = extId(row.listing_id);
      if (id && !v4Urls.has(id)) v4Urls.set(id, row.address);
    }
    writeFileSync(listing, rows.map(row => JSON.stringify(row)).join("\n") + (rows.length ? "\n" : ""));
    const done = run([args.frontier, "import", "--db", args.db, "--label", label, "--jsonl", listing]);
    if (done.code === 0 && done.stdout) {
      try { report[label] = JSON.parse(done.stdout.split(/\r?\n/).at(-1)); }
      catch { report[label] = { error: "non-JSON frontier import response", listed: rows.length }; }
    } else report[label] = { error: done.stderr.slice(0, 200), listed: rows.length };
  }
  return report;
}

if (import.meta.main) {
  try { console.log(JSON.stringify(compile(options(process.argv.slice(2))))); }
  catch (error) { console.error(error); process.exitCode = 1; }
}
