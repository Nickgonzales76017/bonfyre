#!/usr/bin/env bun
// Lower a receipted Parquet observation body through BonfyreQuery into a
// lineage-bearing Zarr v2 field. A field is a derivation, not a new observation.
import { spawnSync } from "node:child_process";
import { randomBytes } from "node:crypto";
import { existsSync, lstatSync, mkdirSync, readdirSync, renameSync, rmSync, writeFileSync } from "node:fs";
import { basename, dirname, join, resolve, sep } from "node:path";
import { bodyDigest } from "./columnar_body.mjs";
export { bodyDigest } from "./columnar_body.mjs";

const bucket = "5 minutes";
const defaults = { parquet: "/var/lib/bonfyre/state/fleming/estate.parquet",
  out: "/var/lib/bonfyre/state/fields/observation-density.zarr",
  query: "/usr/local/bin/bonfyre-query", scratch: "/tmp/lower.duckdb", dryRun: false };

export function options(args) {
  const result = { ...defaults };
  const values = new Map([["--parquet", "parquet"], ["--out", "out"], ["--query", "query"], ["--scratch", "scratch"]]);
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

function queryRows(config, derivation) {
  const child = spawnSync(config.query, ["sql", config.scratch, derivation], {
    encoding: "utf8", timeout: 900_000, maxBuffer: 128 * 1024 * 1024,
  });
  if (child.error || child.status !== 0) throw new Error(`field query failed: ${child.error?.message ?? child.stderr.trim()}`);
  const body = child.stdout.split(/\r?\n/).filter(line => !line.startsWith("[query]")).join("\n").trim();
  let rows;
  try { rows = JSON.parse(body); } catch { throw new Error("field query returned invalid JSON"); }
  if (!Array.isArray(rows)) throw new Error("field query did not return rows");
  return rows;
}

function sourceAxes(rows) {
  for (const row of rows) {
    if (row.src_id == null || row.bucket == null || String(row.src_id) === "" || String(row.bucket) === "")
      throw new Error("field row has no source or bucket identity");
  }
  const sources = [...new Set(rows.map(row => String(row.src_id)))].sort();
  const buckets = [...new Set(rows.map(row => String(row.bucket)))].sort();
  const sourceIndex = new Map(sources.map((source, index) => [source, index]));
  const bucketIndex = new Map(buckets.map((value, index) => [value, index]));
  const cells = sources.length * buckets.length;
  if (!Number.isSafeInteger(cells) || cells > 0x7fffffff / 4) throw new Error("field is too large for one int32 chunk");
  const grid = new Int32Array(cells);
  const occupied = new Set();
  for (const row of rows) {
    const count = Number(row.observations);
    if (!Number.isSafeInteger(count) || count < 0 || count > 0x7fffffff) throw new Error("observation count exceeds int32");
    const cell = sourceIndex.get(String(row.src_id)) * buckets.length + bucketIndex.get(String(row.bucket));
    if (occupied.has(cell)) throw new Error("field query returned duplicate source/bucket coordinates");
    occupied.add(cell);
    grid[cell] = count;
  }
  return { sources, buckets, grid };
}

export async function lower(config) {
  const parquet = resolve(config.parquet), out = resolve(config.out);
  if (!existsSync(parquet)) throw new Error(`no columnar body at ${parquet}`);
  if (!basename(out).endsWith(".zarr")) throw new Error("field output must name a .zarr directory");
  if (out === parquet || out.startsWith(`${parquet}${sep}`) || parquet.startsWith(`${out}${sep}`))
    throw new Error("field output must not overlap its source body");
  const derivation = `SELECT src_id, time_bucket(INTERVAL '${bucket}', CAST(observed_at AS TIMESTAMP)) AS bucket, `
    + `COUNT(*) AS observations FROM read_parquet('${parquet.replaceAll("'", "''")}') `
    + "GROUP BY src_id, bucket ORDER BY src_id, bucket";
  const rows = queryRows(config, derivation);
  if (!rows.length) throw new Error("derivation produced no rows");
  const { sources, buckets, grid } = sourceAxes(rows);
  const sourceDigest = await bodyDigest(parquet), frontier = buckets.at(-1);
  if (config.dryRun) return { sources: sources.length, buckets: buckets.length, cells: grid.length,
    source_digest: sourceDigest, frontier, dry_run: true };

  const parent = dirname(out);
  mkdirSync(parent, { recursive: true });
  const nonce = randomBytes(6).toString("hex"), staging = `${out}.tmp-${nonce}`, backup = `${out}.old-${nonce}`;
  mkdirSync(staging);
  let movedOld = false, installed = false;
  try {
    writeFileSync(join(staging, ".zarray"), `${JSON.stringify({ zarr_format: 2,
      shape: [sources.length, buckets.length], chunks: [sources.length, buckets.length], dtype: "<i4",
      compressor: null, fill_value: 0, order: "C", filters: null }, null, 2)}\n`);
    writeFileSync(join(staging, ".zattrs"), `${JSON.stringify({
      "bonfyre:derivedFrom": { representation: "Parquet", resource: `bonfyre://observation-estate/${basename(parquet)}`,
        digest: sourceDigest, owner: "BonfyreQuery" },
      "bonfyre:derivation": derivation, "bonfyre:frontier": frontier,
      "bonfyre:axes": { "0": "src_id", "1": `observed_at bucketed by ${bucket}` },
      "bonfyre:sources": sources, "bonfyre:buckets": buckets,
      "bonfyre:forbiddenInference": "a field cell is not a fact about the world; it counts observations this institution made and inherits every limit of those observations, including that a source observed twice as often is not twice as real",
    }, null, 2)}\n`);
    const bytes = Buffer.alloc(grid.length * 4);
    let nonzero = 0, total = 0;
    for (const [index, value] of grid.entries()) {
      bytes.writeInt32LE(value, index * 4);
      nonzero += Number(value !== 0);
      total += value;
    }
    writeFileSync(join(staging, "0.0"), bytes);
    const fieldDigest = await bodyDigest(staging);
    if (existsSync(out)) {
      const current = lstatSync(out);
      if (!current.isDirectory() || current.isSymbolicLink() ||
          readdirSync(out).some(name => ![".zarray", ".zattrs", "0.0"].includes(name) ||
            !lstatSync(join(out, name)).isFile()))
        throw new Error("refusing to replace a field containing unowned files");
      renameSync(out, backup);
      movedOld = true;
    }
    renameSync(staging, out);
    installed = true;
    if (movedOld) rmSync(backup, { recursive: true });
    return { schema: "bonfyre.parquet-to-zarr.lowering.v1", field: out,
      shape: [sources.length, buckets.length], cells: grid.length, nonzero,
      total_observations: total, field_digest: fieldDigest,
      derived_from_digest: sourceDigest, frontier,
      forbidden_inference: "a derived field is not a new observation; it is one arrangement of the rows it was computed from, and it is stale the moment they change" };
  } finally {
    if (!installed && movedOld) renameSync(backup, out);
    if (existsSync(staging)) rmSync(staging, { recursive: true });
  }
}

if (import.meta.main) {
  try { process.stdout.write(`${JSON.stringify(await lower(options(process.argv.slice(2))), null, 2)}\n`); }
  catch (error) { process.stderr.write(`${error.message}\n`); process.exitCode = 1; }
}
