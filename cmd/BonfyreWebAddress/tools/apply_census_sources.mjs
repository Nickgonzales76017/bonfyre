#!/usr/bin/env bun
// Lower CUE-declared public source rows through the native CMS owner.
import { Database } from "bun:sqlite";
import { spawnSync } from "node:child_process";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";

const root = resolve(dirname(import.meta.path), "../../..");
const maxUrl = 7000;
const expressions = ["nativeWikimediaSources", "nativePublicCommonsSources", "nativeOpenContentSources", "nativeMainstreamSources", "nativeCreatorSources"];
const quote = value => encodeURIComponent(value).replace(/[!'()*]/g,
  char => `%${char.charCodeAt(0).toString(16).toUpperCase()}`);

function argsOf(argv) {
  const args = { json: "", dryRun: false, cms: "", db: "", schemas: "", fetchInto: "", only: "", maxBytes: "4000000" };
  for (let i = 0; i < argv.length; i++) {
    const key = argv[i];
    if (key === "--dry-run") { args.dryRun = true; continue; }
    const field = { "--json": "json", "--cms": "cms", "--db": "db", "--schemas": "schemas",
      "--fetch-into": "fetchInto", "--only": "only", "--max-bytes": "maxBytes" }[key];
    if (!field || !argv[i + 1]) throw new Error(`unknown or incomplete argument: ${key}`);
    args[field] = argv[++i];
  }
  return args;
}

function declared(path) {
  if (path) return JSON.parse(readFileSync(path, "utf8"));
  const rows = [];
  for (const expression of expressions) {
    const result = spawnSync("cue", ["export", ".", "-e", expression, "--out", "json"],
      { cwd: join(root, "schemas/control"), encoding: "utf8" });
    if (result.status !== 0) throw new Error(`cue ${expression}: ${String(result.stderr).slice(0, 300)}`);
    rows.push(...JSON.parse(result.stdout));
  }
  return rows;
}

function address(row) {
  if (row.url) return row.url;
  const query = String(row.query).replace(/\s+/g, " ").trim();
  return `https://qlever.dev/api/${row.dataset}?query=${quote(query)}`;
}

function fieldsOf(row) {
  const fields = { family: row.family, source: row.source, label: row.label, says: row.says,
    url: address(row), twin: row.twin, state: "observed", body_regime: "REMOTE",
    status: "published", tier: row.tier, cadence_seconds: row.cadence_seconds,
    kind: row.kind, front_door: row.front_door || "yes", note: row.note };
  for (const key of ["credential", "publisher_ident", "publisher_name", "compiled_by"])
    if (row[key]) fields[key] = row[key];
  if (row.status) fields.status = row.status;
  return Object.assign(fields, row.reading);
}

function utcPart(seconds, slashes) {
  const date = new Date(Date.now() - seconds * 1000).toISOString().slice(0, 10);
  return slashes ? date.replaceAll("-", "/") : date;
}

function expandDateTokens(url) {
  for (const [token, seconds, slashes] of [
    ["{today-iso}", 0, false], ["{yesterday-iso}", 86400, false],
    ["{week-ago-iso}", 604800, false], ["{month-ago-iso}", 2592000, false],
    ["{today}", 0, true], ["{yesterday}", 86400, true],
    ["{week-ago}", 604800, true],
  ]) url = url.replaceAll(token, utcPart(seconds, slashes));
  return url;
}

export async function main(argv = process.argv.slice(2)) {
  const args = argsOf(argv);
  const rows = declared(args.json).filter(row => !args.only || row.source === args.only);
  let problems = 0;
  for (const row of rows) {
    const length = Array.from(address(row)).length;
    if (length > maxUrl) {
      console.error(`${row.source}: address is ${length} characters; the reheat reads at most ${maxUrl}`);
      problems++;
    }
  }
  if (problems) return 1;
  if (args.dryRun) {
    for (const row of rows) console.log(`${String(row.source).padEnd(32)} ${String(row.family).padEnd(14)} ${String(Array.from(address(row)).length).padStart(5)} ${row.twin}`);
    return 0;
  }
  if (args.fetchInto) {
    const seen = new Set();
    mkdirSync(join(args.fetchInto, "receipts"), { recursive: true });
    mkdirSync(join(args.fetchInto, "bodies"), { recursive: true });
    // Each body is read as a stream under a byte ceiling and each source has
    // its own outcome: one refusal never stops the rest, and nothing larger
    // than the ceiling is kept in part as if it were whole.
    const ceiling = Number(args.maxBytes);
    if (!Number.isSafeInteger(ceiling) || ceiling < 1) throw new Error("--max-bytes must be a positive integer");
    const outcome = { retained: 0, failed: 0, held: 0 };
    for (const row of rows) {
      if (seen.has(row.twin)) continue;
      seen.add(row.twin);
      if (row.credential) { outcome.held++; console.log(`held ${row.twin}: needs a credential`); continue; }
      const url = expandDateTokens(address(row));
      try {
        const response = await fetch(url, { headers: { "User-Agent": "BonfyreProject/1.0" }, signal: AbortSignal.timeout(90_000) });
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        const declared = Number(response.headers.get("content-length") || 0);
        if (declared > ceiling) throw new Error(`declares ${declared} bytes, over the ${ceiling}-byte ceiling`);
        const chunks = []; let size = 0;
        for await (const chunk of response.body) {
          size += chunk.length;
          if (size > ceiling) throw new Error(`exceeds the ${ceiling}-byte ceiling`);
          chunks.push(chunk);
        }
        const body = join(args.fetchInto, "bodies", `${row.twin}.body`);
        writeFileSync(body, Buffer.concat(chunks));
        writeFileSync(join(args.fetchInto, "receipts", `${row.twin}.json`),
          JSON.stringify({ body_path: body, http_status: response.status, bytes: size, observed_at: new Date().toISOString() }) + "\n");
        outcome.retained++;
        console.log(`retained ${row.twin} (${size} bytes)`);
      } catch (error) {
        outcome.failed++;
        console.log(`failed ${row.twin}: ${error.message}`);
      }
    }
    console.log(JSON.stringify(outcome));
    return outcome.failed && !outcome.retained ? 1 : 0;
  }
  if (!args.cms || !args.db || !args.schemas) throw new Error("--cms, --db and --schemas are required to write");
  if (!existsSync(args.db)) throw new Error(`CMS database does not exist: ${args.db}`);
  const db = new Database(args.db, { readonly: true });
  try {
    const find = db.query("SELECT id FROM internet_source WHERE source=? AND status='published' ORDER BY id DESC LIMIT 1");
    for (const row of rows) {
      const existing = find.get(row.source);
      const fields = fieldsOf(row);
      const cmd = ["entry", existing ? "update" : "create", "internet_source"];
      if (existing) cmd.push(String(existing.id));
      cmd.push(JSON.stringify(fields), "--ns", "root", "--db", args.db, "--schemas", args.schemas);
      const result = spawnSync(args.cms, cmd, { encoding: "utf8" });
      const verb = existing ? "updated" : "created";
      if (![0, 2].includes(result.status)) {
        console.error(`${row.source}: not ${verb}: ${String(result.stderr || result.stdout).trim()}`);
        problems++;
      } else console.log(`${verb} ${row.source}`);
    }
  } finally { db.close(); }
  return problems ? 1 : 0;
}

if (import.meta.main) {
  try { process.exitCode = await main(); }
  catch (error) { console.error(error.message); process.exitCode = 1; }
}
