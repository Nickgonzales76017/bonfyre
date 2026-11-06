#!/usr/bin/env bun
// Give retained publishers estate-owned addresses; never infer publisher consent or control.
import { Database } from "bun:sqlite";
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { homedir } from "node:os";
import { join } from "node:path";

const cannotInfer = "these are the sources this estate reads that published under this name; the page is this estate reading them, not the publisher's own site, and not anything the publisher agreed to";
const sha = text => createHash("sha256").update(text).digest("hex");
const slug = text => String(text || "").toLowerCase().replace(/[^a-z0-9]/g, "-")
  .replace(/-+/g, "-").replace(/^-|-$/g, "").slice(0, 48).replace(/-$/g, "");

function pythonJson(value, sortKeys = false) {
  if (typeof value === "string") return JSON.stringify(value).replace(/[\u007f-\uffff]/g,
    char => "\\u" + char.charCodeAt(0).toString(16).padStart(4, "0"));
  if (value === null || typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(item => pythonJson(item, sortKeys)).join(", ")}]`;
  const keys = Object.keys(value);
  if (sortKeys) keys.sort();
  return `{${keys.map(key => `${pythonJson(key)}: ${pythonJson(value[key], sortKeys)}`).join(", ")}}`;
}

function argsOf(argv) {
  const args = { estate: join(homedir(), ".bonfyre/estate-local"), repo: join(homedir(), "Developer/Bonfyre"),
    territory: "life", minItems: 3, limit: 4000, apply: false };
  for (let i = 0; i < argv.length; i++) {
    const key = argv[i];
    if (key === "--apply") { args.apply = true; continue; }
    if (!["--estate", "--repo", "--territory", "--min-items", "--limit"].includes(key) || !argv[i + 1])
      throw new Error(`unknown or incomplete argument: ${key}`);
    args[key.slice(2).replace("-", "")] = argv[++i];
  }
  args.minItems = Number(args.minItems);
  args.limit = Number(args.limit);
  if (![args.minItems, args.limit].every(Number.isInteger) || args.minItems < 0 || args.limit < 0)
    throw new Error("--min-items and --limit must be nonnegative integers");
  return args;
}

function matrixCall(web, argv) {
  return spawnSync(web, argv, { encoding: "utf8" }).status === 0;
}

function report(value) { console.log(JSON.stringify(value, null, 1)); }

export function main(argv = process.argv.slice(2)) {
  const args = argsOf(argv);
  const now = new Date().toISOString().replace(/\.\d{3}Z$/, "Z");
  const archive = new Database(join(args.estate, "items.db"), { readonly: true });
  const cms = new Database(join(args.estate, "cms.db"));
  const fabric = new Database(join(args.estate, "subject_fabric.db"));
  cms.exec("PRAGMA busy_timeout=60000");
  fabric.exec("PRAGMA busy_timeout=60000");
  try {
    const publishers = archive.query("SELECT publisher, COUNT(*) AS items, COUNT(DISTINCT source) AS sources FROM item WHERE COALESCE(publisher,'')<>'' GROUP BY publisher HAVING items >= ? ORDER BY items DESC LIMIT ?").all(args.minItems, args.limit);
    const taken = new Set(cms.query("SELECT host FROM site_presentation WHERE status='published'").all().map(row => row.host));
    const seen = new Set(), planned = [];
    let skipped = 0, clashed = 0;
    for (const row of publishers) {
      const label = slug(row.publisher);
      if (!label) { skipped++; continue; }
      const host = `${label}.aurekai.${args.territory}`;
      if (taken.has(host) || seen.has(host)) { clashed++; continue; }
      seen.add(host);
      planned.push({ label, host, name: row.publisher, items: row.items, sources: row.sources });
    }
    if (!args.apply) {
      report({ candidates: publishers.length, to_address: planned.length,
        already_addressed_or_clashing: clashed, unlabelable: skipped,
        sample: planned.slice(0, 8).map(row => row.host) });
      return 0;
    }
    const web = join(args.repo, "cmd/BonfyreWebAddress/bonfyre-web-address");
    const matrixDb = join(args.estate, "matrix.db");
    const writeSite = cms.query("INSERT INTO site_presentation(namespace,created_at,updated_at,published_at,status,site_id,host,subject_address,profile,territory,navigation) VALUES('root',?,?,'','published',?,?,?,'newsroom',?,'[]')");
    const clearReaders = fabric.query("DELETE FROM subject_reader WHERE subject=? AND scope='public'");
    const readSources = archive.query("SELECT source, COALESCE(family,'') AS family, COALESCE(kind,'') AS kind, COUNT(*) AS n, MAX(COALESCE(at,0)) AS latest FROM item WHERE publisher=? GROUP BY source, family");
    const writeReader = fabric.query("INSERT OR REPLACE INTO subject_reader(subject,scope,census_source,root_id,channel_id,material,protocol,state,last_contributed,contributions,first_seen) VALUES(?,'public',?,?,?,?,?,'admitted',?,?,'')");
    const writeBundle = fabric.query("INSERT OR REPLACE INTO subject_bundle(subject,scope,bundle_digest,closure_digest,scene_digest,compiled_at,counts_json,payload_json) VALUES(?,'public',?,?,'',?,?,?)");
    let addressed = 0, refused = 0, readersWritten = 0;
    for (const item of planned) {
      const subject = `institution:publisher:${item.label}`;
      const institution = `inst:${item.label}`;
      const digest = sha(item.name);
      matrixCall(web, ["institution", "--db", matrixDb, "--id", institution,
        "--kind", "publisher", "--world", "live", "--evidence", "a publisher whose material this estate holds"]);
      matrixCall(web, ["subject", "--db", matrixDb, "--id", `org:${item.label}`,
        "--kind", "institution", "--digest", digest,
        "--evidence", "a publisher whose material this estate holds"]);
      if (!matrixCall(web, ["domain", "--db", matrixDb, "--host", item.host,
        "--institution", institution, "--territory", args.territory, "--scope", "public",
        "--world", "live", "--repr", "HTML", "--evidence", "an address this estate gives a publisher it reads"])) {
        refused++; continue;
      }
      writeSite.run(now, now, `publisher-${item.label}`, item.host, subject, args.territory);
      clearReaders.run(subject);
      const rows = readSources.all(item.name);
      for (const reader of rows) {
        const last = reader.latest ? new Date(Number(reader.latest) * 1000).toISOString().replace(/\.\d{3}Z$/, "Z") : "";
        writeReader.run(subject, reader.source, item.label, reader.source, reader.family,
          reader.kind, last, reader.n);
        readersWritten++;
      }
      const payload = { subject, host: item.host, publisher: item.name, cannot_infer: cannotInfer };
      const seed = pythonJson(payload, true);
      writeBundle.run(subject, sha(seed), sha(rows.map(row => row.source).join("|")), now,
        pythonJson({ readers: rows.length, items: item.items, sources: item.sources }), pythonJson(payload));
      addressed++;
    }
    report({ addressed, refused, readers: readersWritten,
      already_addressed_or_clashing: clashed, territory: args.territory, cannot_infer: cannotInfer });
    return 0;
  } finally { archive.close(); cms.close(); fabric.close(); }
}

if (import.meta.main) {
  try { process.exitCode = main(); }
  catch (error) { console.error(error.message); process.exitCode = 1; }
}
