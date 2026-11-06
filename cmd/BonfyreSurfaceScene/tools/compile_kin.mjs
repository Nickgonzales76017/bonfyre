#!/usr/bin/env bun
// Materialize bounded chart neighbours through the existing vGPU kin kernel.
import { Database } from "bun:sqlite";
import { spawnSync } from "node:child_process";
import { closeSync, mkdtempSync, openSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { codeDigest, digest, sha256, inspectDerived, commitDerived } from "./derived_cuts.mjs";
import { performance } from "node:perf_hooks";

function argsOf(argv) {
  const args = { atlas: "", db: "", chart: "words-256",
    kernel: resolve(dirname(import.meta.path), "../runtime/kin-compute-vgpu.mjs"),
    k: 8, tile: 1024, min: 0.85, backend: "auto", limit: 20000, node: "node" };
  const fields = { "--atlas": "atlas", "--db": "db", "--chart": "chart", "--kernel": "kernel",
    "--k": "k", "--tile": "tile", "--min": "min", "--backend": "backend",
    "--limit": "limit", "--node": "node" };
  for (let i = 0; i < argv.length; i++) {
    const field = fields[argv[i]];
    if (!field || !argv[i + 1]) throw new Error(`unknown or incomplete argument: ${argv[i]}`);
    args[field] = argv[++i];
  }
  args.k = Number(args.k); args.tile = Number(args.tile);
  args.min = Number(args.min); args.limit = Number(args.limit);
  if (!args.atlas || !args.db || !Number.isInteger(args.k) || !Number.isInteger(args.tile) ||
      !Number.isInteger(args.limit) || !Number.isFinite(args.min))
    throw new Error("--atlas and --db are required with numeric k/tile/min/limit");
  return args;
}

export function main(argv = process.argv.slice(2)) {
  const args = argsOf(argv);
  const atlas = new Database(args.atlas, { readonly: true });
  let dims = 0, selections = [];
  try {
    const chart = atlas.query("SELECT dims FROM chart WHERE chart_id=?").get(args.chart);
    if (chart) {
      dims = Number(chart.dims);
      if (!Number.isInteger(dims) || dims < 2 || dims > 8192)
        throw Error("native chart has invalid dimensions");
      const chosen = atlas.query("SELECT address,coordinate FROM projection WHERE chart_id=? ORDER BY updated_at DESC LIMIT ?").all(args.chart, args.limit);
      for (const row of chosen) {
        const bytes = Buffer.from(row.coordinate || []);
        if (bytes.length >= dims * 4) selections.push({ address: row.address, bytes: bytes.subarray(0, dims * 4) });
      }
    }
  } finally { atlas.close(); }
  // Depend on the chart's actual ordered vector bytes, not the whole Atlas DB
  // or timestamps written by an independent owner.
  const input = digest({ chart: args.chart, dims, k: args.k, tile: args.tile,
    min: args.min, backend: args.backend, limit: args.limit,
    vectors: selections.map(row => [row.address, sha256(row.bytes)]),
    code: codeDigest(import.meta.path, resolve(args.kernel),
      fileURLToPath(new URL("./derived_cuts.mjs", import.meta.url))) });
  const db = new Database(args.db);
  const started = performance.now();
  try {
    db.exec("PRAGMA journal_mode=DELETE");
    db.exec("CREATE TABLE IF NOT EXISTS kin(address TEXT NOT NULL, kin_address TEXT NOT NULL, chart TEXT NOT NULL, score REAL NOT NULL, rank INTEGER NOT NULL, computed TEXT NOT NULL, PRIMARY KEY(address, kin_address))");
    db.exec("CREATE INDEX IF NOT EXISTS kin_by_address ON kin(address, rank)");
    const read = () => db.query("SELECT address,kin_address,chart,score,rank FROM kin WHERE chart=? ORDER BY address,kin_address").all(args.chart);
    const identity = row => JSON.stringify([row.address, row.kin_address]);
    const cut = inspectDerived(db, { name: "kin:"+args.chart, input, read, identity });
    if (cut.hit) {
      console.log(JSON.stringify({ items: selections.length, chart: args.chart, dims,
        pairs_kept: cut.rows.length, owner_operations: 0, kernel_invocations: 0,
        rows_retained: cut.rows.length, avoided_comparisons: "unchanged_vector_scoped_generation",
        skipped: "verified_kin_generation_and_materialized_rows_unchanged" }));
      return 0;
    }
    let receipt = { actual_backend: "none", tiles: 0, pairs: 0, elapsed_ms: 0 };
    let wanted = [];
    if (selections.length >= 2) {
      const work = mkdtempSync(join(tmpdir(), "bonfyre-kin-native-"));
      try {
        const vectors = join(work, "vectors.f32"), keysPath = join(work, "keys.json");
        const handle = openSync(vectors, "w");
        try { for (const row of selections) writeFileSync(handle, row.bytes); }
        finally { closeSync(handle); }
        writeFileSync(keysPath, JSON.stringify(selections.map(row => row.address)));
        const done = spawnSync(args.node, [resolve(args.kernel), "--vectors", vectors, "--keys", keysPath,
          "--dims", String(dims), "--k", String(args.k), "--tile", String(args.tile),
          "--min", String(args.min), "--backend", args.backend],
        { encoding: "utf8", timeout: 7_200_000 });
        if (done.status !== 0) { process.stderr.write(String(done.stderr || done.error || "").slice(-600)); return 2; }
        const answer = JSON.parse(done.stdout.trim().split("\n").at(-1));
        if (answer?.receipt?.schema !== "bonfyre.kin-compute-receipt.v1" ||
            answer.receipt.items !== selections.length || !Array.isArray(answer.nearest))
          throw Error("native kin kernel returned incomplete receipt");
        receipt = answer.receipt;
        for (const row of answer.nearest) {
          if (typeof row.key !== "string" || !Array.isArray(row.near)) throw Error("invalid kin kernel row");
          for (const [rank, one] of row.near.entries()) {
            if (typeof one.key !== "string" || !Number.isFinite(one.score)) throw Error("invalid kin score");
            wanted.push({ address: row.key, kin_address: one.key, chart: args.chart, score: one.score, rank });
          }
        }
      } finally { rmSync(work, { recursive: true, force: true }); }
    }
    const stamp = new Date().toISOString().replace(/\.\d{3}Z$/, "Z");
    const remove = db.query("DELETE FROM kin WHERE address=? AND kin_address=? AND chart=?");
    const insert = db.query("INSERT OR REPLACE INTO kin(address,kin_address,chart,score,rank,computed) VALUES(?,?,?,?,?,?)");
    const writes = commitDerived(db, { name: "kin:"+args.chart, input, read,
      observed: cut, desired: wanted, identity,
      remove: row => remove.run(row.address, row.kin_address, args.chart),
      upsert: row => insert.run(row.address, row.kin_address, row.chart, row.score, row.rank, stamp) });
    console.log(JSON.stringify({ items: selections.length, chart: args.chart, dims,
      with_kin: selections.length >= 2 ? receipt.items : 0, pairs_kept: wanted.length,
      backend: receipt.actual_backend, tiles: receipt.tiles, pairs: receipt.pairs,
      kernel_ms: receipt.elapsed_ms, seconds: Math.round((performance.now()-started)/100)/10,
      owner_operations: 1, kernel_invocations: selections.length >= 2 ? 1 : 0,
      rows_updated: writes.updated, rows_removed: writes.deleted, rows_retained: writes.retained,
      floor: args.min, ...(selections.length < 2 ? { skipped: "fewer than two items; obsolete kin retired" } : {}),
      cannot_infer: receipt.cannot_infer || "word-chart similarity is not a shared subject, citation, corroboration or endorsement" }));
    return 0;
  } finally { db.close(); }
}

if (import.meta.main) {
  try { process.exitCode = main(); }
  catch (error) { console.error(error.message); process.exitCode = 1; }
}
