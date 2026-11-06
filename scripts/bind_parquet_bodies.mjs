#!/usr/bin/env bun
// Bind observed columnar body slices and a verified derived field to material closure.
import { spawnSync } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import { basename, join, resolve } from "node:path";
import { bodyIdentity } from "./columnar_body.mjs";
import { inspect } from "./inspect_foreign_field.mjs";

const defaults = { parquet: "/var/lib/bonfyre/state/fleming/estate.parquet",
  matrix: "/var/lib/bonfyre/matrix.db", organ: "/usr/local/bin/bonfyre-web-address",
  query: "/usr/local/bin/bonfyre-query", scratch: "/tmp/parquet-bind.duckdb",
  field: "/var/lib/bonfyre/state/fields/observation-density.zarr", dryRun: false };

export function options(args) {
  const result = { ...defaults };
  const values = new Map([["--parquet", "parquet"], ["--matrix", "matrix"], ["--organ", "organ"],
    ["--query", "query"], ["--scratch", "scratch"], ["--field", "field"]]);
  for (let i = 0; i < args.length; i++) {
    const key = values.get(args[i]);
    if (key) {
      if (!args[i + 1]) throw new Error(`${args[i]} requires a value`);
      result[key] = args[++i];
    } else if (args[i] === "--dry-run") result.dryRun = true;
    else throw new Error(`unknown option: ${args[i]}`);
  }
  return result;
}

function querySources(config) {
  const parquet = resolve(config.parquet);
  const sql = `SELECT DISTINCT src_id FROM read_parquet('${parquet.replaceAll("'", "''")}') ORDER BY src_id`;
  const child = spawnSync(config.query, ["sql", config.scratch, sql], {
    encoding: "utf8", timeout: 600_000, maxBuffer: 64 * 1024 * 1024 });
  if (child.error || child.status !== 0) throw new Error(`columnar source query failed: ${child.error?.message ?? child.stderr.trim()}`);
  const body = child.stdout.split(/\r?\n/).filter(line => !line.startsWith("[query]")).join("\n").trim();
  let rows;
  try { rows = JSON.parse(body); } catch { throw new Error("columnar source query returned invalid JSON"); }
  if (!Array.isArray(rows)) throw new Error("columnar source query did not return rows");
  const sources = rows.map(row => row?.src_id);
  if (sources.some(source => typeof source !== "string" || !source)) throw new Error("columnar source has no identity");
  if (new Set(sources).size !== sources.length) throw new Error("columnar query returned duplicate sources");
  return sources;
}

function subject(source) { return `foreign_twin:${source.replaceAll("src:", "src-").replaceAll(":", "-")}`; }

function bind(config, source, representation, resource, digest, evidence) {
  const sid = subject(source);
  const child = spawnSync(config.organ, ["closure-contribution", "--db", config.matrix,
    "--subject", sid, "--presence", `presence:${sid}:observed`, "--plane", "material",
    "--owner", "BonfyreQuery", "--resource", resource, "--representation", representation,
    "--digest", digest, "--evidence", evidence],
  { encoding: "utf8", timeout: 60_000, maxBuffer: 1024 * 1024 });
  return !child.error && child.status === 0;
}

export async function plan(config) {
  const parquet = resolve(config.parquet);
  if (!existsSync(parquet)) throw new Error(`no columnar body at ${parquet}`);
  const body = await bodyIdentity(parquet), sources = querySources(config);
  const fieldPath = resolve(config.field);
  let field;
  if (existsSync(fieldPath)) {
    const identity = await bodyIdentity(fieldPath);
    let attrs;
    try { attrs = JSON.parse(readFileSync(join(fieldPath, ".zattrs"), "utf8")); }
    catch { throw new Error("field has no readable lineage metadata"); }
    if (attrs["bonfyre:derivedFrom"]?.digest !== body.digest)
      throw new Error("field derivation digest does not match the current columnar body");
    const order = attrs["bonfyre:sources"];
    if (!Array.isArray(order) || new Set(order).size !== order.length || order.some(value => typeof value !== "string"))
      throw new Error("field has invalid source coordinates");
    const fieldShape = inspect(fieldPath);
    if (fieldShape.shape[0] !== order.length || !fieldShape.has_derivation_frontier)
      throw new Error("field shape or derivation frontier does not match its source coordinates");
    field = { path: fieldPath, ...identity, index: new Map(order.map((source, row) => [source, row])) };
  }
  return { parquet, body, sources, field };
}

export async function run(config) {
  const { parquet, body, sources, field } = await plan(config);
  let bound = 0, skipped = 0, fieldBound = 0, fieldSkipped = 0;
  if (config.dryRun) bound = sources.length;
  else {
    for (const source of sources) {
      const resource = `bonfyre://observation-estate/${basename(parquet)}#src_id=${source}`;
      if (bind(config, source, "Parquet", resource, body.digest,
        `columnar-estate:${body.bytes}bytes:${body.digest.slice(0, 16)}`)) bound++;
      else skipped++;
    }
    if (field) for (const source of sources) {
      const row = field.index.get(source);
      if (row === undefined) continue;
      const resource = `bonfyre://observation-field/${basename(field.path)}#row=${row}`;
      if (bind(config, source, "Zarr", resource, field.digest,
        `lowered-from:${body.digest.slice(0, 16)}:${field.bytes}bytes`)) fieldBound++;
      else fieldSkipped++;
    }
  }
  return { schema: "bonfyre.parquet.material-binding.v1", field_rows_bound: fieldBound,
    field_rows_skipped: fieldSkipped, body: parquet, body_bytes: body.bytes,
    body_members: body.members, body_digest: body.digest, representation: "Parquet",
    plane: "material", slices: sources.length, bound, skipped, dry_run: Boolean(config.dryRun),
    forbidden_inference: "a bound body is not an admitted fact and not a licence to redistribute it; the columnar estate is a compressed factual representation and AssertionBoundary still controls admission" };
}

if (import.meta.main) {
  try {
    const receipt = await run(options(process.argv.slice(2)));
    console.log(JSON.stringify(receipt, null, 2));
    if (receipt.skipped || receipt.field_rows_skipped) process.exitCode = 1;
  } catch (error) { console.error(error.message); process.exitCode = 1; }
}
