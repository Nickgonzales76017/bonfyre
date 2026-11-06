#!/usr/bin/env bun
// Materialize nearest placed items through the existing bounded vGPU kernel.
import { Database } from "bun:sqlite";
import { spawnSync } from "node:child_process";
import { createReadStream } from "node:fs";
import { codeDigest, digest, inspectDerived, commitDerived } from "./derived_cuts.mjs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { createInterface } from "node:readline";
import { performance } from "node:perf_hooks";

function argsOf(argv) {
  const args = { items: "", db: "", kernel: resolve(dirname(import.meta.path), "../runtime/stage-compute-vgpu.mjs"),
    k: 8, tile: 2048, backend: "auto", maxKm: 4, node: "node" };
  const fields = { "--items": "items", "--db": "db", "--kernel": "kernel", "--k": "k",
    "--tile": "tile", "--backend": "backend", "--max-km": "maxKm", "--node": "node" };
  for (let i = 0; i < argv.length; i++) {
    const field = fields[argv[i]];
    if (!field || !argv[i + 1]) throw new Error(`unknown or incomplete argument: ${argv[i]}`);
    args[field] = argv[++i];
  }
  args.k = Number(args.k); args.tile = Number(args.tile); args.maxKm = Number(args.maxKm);
  if (!args.items || !args.db || !Number.isInteger(args.k) || !Number.isInteger(args.tile) ||
      !Number.isFinite(args.maxKm)) throw new Error("--items and --db are required with numeric k/tile/max-km");
  return args;
}

async function materialFrom(path) {
  const material = {}, placed = { count: 0 };
  const lines = createInterface({ input: createReadStream(path), crlfDelay: Infinity });
  for await (const line of lines) {
    if (!line.trim()) continue;
    let item;
    try { item = JSON.parse(line); } catch (_) { continue; }
    if (!item.placed) continue;
    const kind = item.kind || "story";
    (material[kind] ||= []).push({ key: `o/${item.source}/${item.key}`, source: item.source,
      title: Array.from(String(item.title || "")).slice(0, 200).join(""), kind,
      lon: item.lon, lat: item.lat });
    placed.count++;
  }
  return [material, placed.count];
}

export async function main(argv = process.argv.slice(2)) {
  const args = argsOf(argv);
  const [material, placed] = await materialFrom(args.items);
  // The kernel's source-shaped material is the exact dependency, not the
  // Archive DB or its last_read timestamps. Include program and kernel bytes
  // so an upgraded physical execution contract invalidates the old cut.
  const input = digest({ material, k: args.k, tile: args.tile, backend: args.backend, node: args.node,
    maxKm: args.maxKm, code: codeDigest(import.meta.path, resolve(args.kernel),
      fileURLToPath(new URL("./derived_cuts.mjs", import.meta.url))) });
  const started = performance.now();
  const db = new Database(args.db);
  try {
    db.exec("PRAGMA journal_mode=DELETE");
    db.exec("CREATE TABLE IF NOT EXISTS neighbour(address TEXT NOT NULL, near_address TEXT NOT NULL, kind TEXT DEFAULT '', metres REAL NOT NULL, rank INTEGER NOT NULL, computed TEXT NOT NULL, PRIMARY KEY(address, near_address))");
    db.exec("CREATE INDEX IF NOT EXISTS neighbour_by_address ON neighbour(address, rank)");
    const read = () => db.query("SELECT address,near_address,kind,metres,rank FROM neighbour ORDER BY address,near_address").all();
    const identity = row => JSON.stringify([row.address, row.near_address]);
    const cut = inspectDerived(db, { name: "neighbour", input, read, identity });
    if (cut.hit) return console.log(JSON.stringify({ placed,
      pairs_kept: cut.rows.length, owner_operations: 0, kernel_invocations: 0,
      rows_retained: cut.rows.length, avoided_comparisons: "unchanged_content_scoped_generation",
      skipped: "verified_neighbour_generation_and_materialized_rows_unchanged" })), 0;

    let receipt = { actual_backend: "none", tiles: 0, pairs: 0, elapsed_ms: 0 };
    let rows = [];
    if (placed >= 2) {
      const closure = { schema: "bonfyre.stage-closure.v1", material };
      const done = spawnSync(args.node, [resolve(args.kernel), "--backend", args.backend,
        "--k", String(args.k), "--tile", String(args.tile)],
      { input: JSON.stringify(closure), encoding: "utf8", timeout: 3_600_000 });
      if (done.status !== 0) { process.stderr.write(String(done.stderr || done.error || "").slice(-500)); return 2; }
      const answer = JSON.parse(done.stdout.trim().split("\n").at(-1));
      if (answer?.receipt?.schema !== "bonfyre.stage-compute-receipt.v1" ||
        !Array.isArray(answer.nearest) || !Number.isInteger(answer.receipt.points) ||
        answer.receipt.points > placed || answer.receipt.points < 0)
        throw Error("native spatial kernel returned incomplete receipt");
      receipt = answer.receipt;
      for (const row of answer.nearest) {
        if (typeof row.key !== "string" || !Array.isArray(row.near)) throw Error("invalid spatial kernel row");
        for (const [rank, near] of row.near.entries()) {
          if (!Number.isFinite(near.meters) || typeof near.key !== "string") throw Error("invalid spatial distance");
          if (near.meters > args.maxKm * 1000) continue;
          rows.push({ address: row.key, near_address: near.key, kind: near.kind || "",
            metres: near.meters, rank });
        }
      }
    }
    const stamp = new Date().toISOString().replace(/\.\d{3}Z$/, "Z");
    const remove = db.query("DELETE FROM neighbour WHERE address=? AND near_address=?");
    const upsert = db.query("INSERT OR REPLACE INTO neighbour(address,near_address,kind,metres,rank,computed) VALUES(?,?,?,?,?,?)");
    const changed = commitDerived(db, { name: "neighbour", input, identity, read,
      observed: cut, desired: rows,
      remove: row => remove.run(row.address, row.near_address),
      upsert: row => upsert.run(row.address, row.near_address, row.kind, row.metres, row.rank, stamp) });
    console.log(JSON.stringify({ placed, neighbourhoods: placed >= 2 ? receipt.points : 0,
      pairs_kept: rows.length, backend: receipt.actual_backend,
      tiles: receipt.tiles, pairs: receipt.pairs, kernel_ms: receipt.elapsed_ms,
      owner_operations: 1, kernel_invocations: placed >= 2 ? 1 : 0,
      rows_updated: changed.updated, rows_removed: changed.deleted, rows_retained: changed.retained,
      seconds: Math.round((performance.now() - started) / 100) / 10,
      ...(placed < 2 ? { skipped: "fewer than two placed things; obsolete neighbours retired" } : {}),
      cannot_infer: "a distance between two stated points is not a relation, an access, an attendance or a recommendation" }));
    return 0;
  } finally { db.close(); }
}

if (import.meta.main) {
  try { process.exitCode = await main(); }
  catch (error) { console.error(error.message); process.exitCode = 1; }
}
