import { afterAll, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdtempSync, existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { bodyDigest } from "../../../scripts/columnar_body.mjs";
import { materializeZarr, planZarr } from "../../../scripts/representation-manifold-zarr.mjs";

const root = mkdtempSync(join(tmpdir(), "bonfyre-native-zarr-"));
afterAll(() => rmSync(root, { recursive: true, force: true }));
const source = fileURLToPath(new URL("../src/main.cpp", import.meta.url));
const include = process.env.ZARR_ARROW_INCLUDE;
const bundle = process.env.ZARR_ARROW_BUNDLE;
const reader = process.env.ZARR_READER_PYTHON;
const parity = include && bundle;

test.skipIf(!parity)("native Arrow lowerer writes chunked numeric Zarr with exact validity and provenance", async () => {
  const binary = join(root, "bonfyre-zarr-forge"), parquet = join(root, "body.parquet"), out = join(root, "field.zarr");
  const lib = join(bundle, "lib");
  const compile = Bun.spawnSync(["clang++", "-std=c++20", "-O2", "-Wall", "-Wextra", "-Werror",
    "-isystem", include, source, join(lib, "libarrow.2500.dylib"), join(lib, "libparquet.2500.dylib"),
    `-Wl,-rpath,${lib}`, "-o", binary], { stdout: "pipe", stderr: "pipe" });
  if (compile.exitCode) throw new Error(compile.stderr.toString());
  const sql = `COPY (SELECT i::BIGINT AS id,
    CASE WHEN i=2 THEN NULL ELSE (i+0.5)::DOUBLE END AS score,
    CASE WHEN i=4 THEN NULL ELSE i::INTEGER END AS optional,
    'label'::VARCHAR AS text, [i]::INTEGER[] AS nested
    FROM range(5) t(i)) TO '${parquet.replaceAll("'", "''")}' (FORMAT PARQUET)`;
  const fixture = Bun.spawnSync(["duckdb", "-batch", ":memory:", sql], { stdout: "pipe", stderr: "pipe" });
  if (fixture.exitCode) throw new Error(fixture.stderr.toString());
  const digest = await bodyDigest(parquet);
  const lowered = Bun.spawnSync([binary, parquet, out, digest, "2"], { stdout: "pipe", stderr: "pipe" });
  if (lowered.exitCode) throw new Error(lowered.stderr.toString());
  expect(JSON.parse(lowered.stdout.toString())).toEqual({ rows: 5, numeric_columns: 3 });
  expect(JSON.parse(readFileSync(join(out, ".zgroup"), "utf8"))).toEqual({ zarr_format: 2 });
  const attributes = JSON.parse(readFileSync(join(out, ".zattrs"), "utf8"));
  expect(attributes["bonfyre:sourceParquetSha256"]).toBe(digest);
  expect(attributes["bonfyre:columns"].map(column => column.name)).toEqual(["id", "score", "optional"]);
  expect(JSON.parse(readFileSync(join(out, "c0", ".zarray"), "utf8"))).toMatchObject({
    shape: [5], chunks: [2], dtype: "<i8", compressor: null });
  expect(readFileSync(join(out, "m2", "2"))).toEqual(Buffer.from([0, 0]));
  expect(existsSync(join(out, "c0", "2"))).toBe(true);
  if (reader) {
    const verify = Bun.spawnSync([reader, "-c", `import json,sys,zarr\ng=zarr.open_group(sys.argv[1],mode='r')\nprint(json.dumps({'id':g['c0'][:].tolist(),'score':g['c1'][:].tolist(),'score_valid':g['m1'][:].tolist(),'optional_valid':g['m2'][:].tolist()}))`, out],
      { stdout: "pipe", stderr: "pipe" });
    if (verify.exitCode) throw new Error(`independent Zarr reader refused output: ${verify.stderr.toString()}`);
    expect(JSON.parse(verify.stdout.toString())).toEqual({ id: [0, 1, 2, 3, 4],
      score: [0.5, 1.5, 0, 3.5, 4.5], score_valid: [1, 1, 0, 1, 1], optional_valid: [1, 1, 1, 1, 0] });
  }
  const refused = Bun.spawnSync([binary, parquet, join(root, "invalid.zarr"), "0".repeat(64), "2"],
    { stdout: "pipe", stderr: "pipe" });
  expect(refused.exitCode).toBe(1);
  expect(existsSync(join(root, "invalid.zarr"))).toBe(false);
  const estate = join(root, "estate");
  mkdirSync(estate);
  const manifold = new Database(join(estate, "representation-manifold.db"), { create: true });
  manifold.exec(readFileSync(new URL("../../../schemas/storage/representation-manifold.sql", import.meta.url), "utf8"));
  manifold.query("INSERT INTO representation_instance VALUES(?,?,?,?,?,?,?,?,?,?)").run(
    "source-repr", null, "parquet", "derived", "frontier-1", digest, "fixture", new Date().toISOString(), 1, "physical");
  manifold.query("INSERT INTO physical_realization VALUES(?,?,?,?,?,?,?,?,?)").run(
    "source-file", "source-repr", "local", parquet, null, digest, "warm", 1, new Date().toISOString());
  manifold.query("INSERT INTO representation_target VALUES(?,?,?,?,?)").run("fixture", "zarr", "declared", null, "explicit demand");
  manifold.close();
  const planned = await planZarr(estate, "fixture");
  expect(planned.state).toBe("demanded_unbuilt");
  const bound = await materializeZarr(estate, "fixture", { binary, bundle, chunkSize: 2 });
  expect(bound).toMatchObject({ state: "physical", input_digest: digest, numeric_columns: 3 });
  expect((await materializeZarr(estate, "fixture", { binary, bundle, chunkSize: 2 })).state).toBe("verified_existing");
  const chunkPath = join(bound.coordinate, "c0", "0");
  const originalChunk = readFileSync(chunkPath);
  const modifiedChunk = Buffer.from(originalChunk);
  modifiedChunk[0] ^= 1;
  writeFileSync(chunkPath, modifiedChunk);
  await expect(materializeZarr(estate, "fixture", { binary, bundle, chunkSize: 2 }))
    .rejects.toThrow("exact materialization receipt");
  writeFileSync(chunkPath, originalChunk);
  const receipts = new Database(join(estate, "representation-manifold.db"), { readonly: true });
  expect(receipts.query("SELECT input_digest,output_digest,proof_state FROM transformation_occurrence WHERE transform='parquet->zarr-numeric'").get())
    .toMatchObject({ input_digest: digest, output_digest: bound.digest, proof_state: "materialized" });
  expect(receipts.query("SELECT status FROM representation_target WHERE semantic_kind='fixture' AND representation_kind='zarr'").get().status).toBe("physical");
  receipts.close();
  writeFileSync(join(bound.coordinate, "foreign"), "unowned");
  await expect(materializeZarr(estate, "fixture", { binary, bundle, chunkSize: 2 })).rejects.toThrow("unowned");
  rmSync(join(bound.coordinate, "foreign"));
  writeFileSync(parquet, "drift", { flag: "a" });
  await expect(materializeZarr(estate, "fixture", { binary, bundle, chunkSize: 2 })).rejects.toThrow("digest drift");
});
