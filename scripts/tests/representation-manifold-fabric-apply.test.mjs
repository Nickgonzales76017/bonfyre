// What this does not prove:
// - the network call is a function supplied by this test, so no request leaves the process and no remote behaviour is observed.
import { afterAll, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { createHash } from "node:crypto";
import { chmodSync, existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { bodyDigest } from "../columnar_body.mjs";
import { applyFabric, stageXTableRuntime } from "../representation-manifold-fabric.mjs";

const estate = mkdtempSync(join(tmpdir(), "bonfyre-fabric-apply-"));
afterAll(() => rmSync(estate, { recursive: true, force: true }));
const bodyDir = join(estate, "representations", "parquet");
mkdirSync(bodyDir, { recursive: true });
const parquet = join(bodyDir, "fixture.parquet");
const copied = Bun.spawnSync(["duckdb", "-batch", ":memory:",
  `COPY (SELECT 7::BIGINT AS id, 'nested'::VARCHAR AS name, [1,2]::INTEGER[] AS items)
   TO '${parquet.replaceAll("'", "''")}' (FORMAT PARQUET)`], { stdout: "pipe", stderr: "pipe" });
if (copied.exitCode) throw new Error(copied.stderr.toString());
const digest = await bodyDigest(parquet);
const dbPath = join(estate, "representation-manifold.db");
const db = new Database(dbPath);
db.exec(readFileSync(new URL("../../schemas/storage/representation-manifold.sql", import.meta.url), "utf8"));
db.query("INSERT INTO representation_instance VALUES(?,?,?,?,?,?,?,?,?,?)")
  .run("root", null, "parquet", "derived", "frontier-1", digest, "fixture", "now", 1, "physical");
db.query("INSERT INTO physical_realization VALUES(?,?,?,?,?,?,?,?,?)")
  .run("root-file", "root", "local", parquet, statSync(parquet).size, digest, "warm", 1, "now");
db.query("INSERT INTO representation_target VALUES(?,?,?,?,?)")
  .run("fixture", "substrait", "declared", null, "test demand");
db.close();

test("Bun Fabric apply coordinates Substrait, carrier, XTable staging and report without Python", async () => {
  const runtime = join(estate, "runtime");
  const result = await applyFabric({ estate, runtime, runXTable: false });
  expect(result.state).toBe("APPLIED");
  expect(result.canonical_roots).toBe(1);
  expect(result.substrait).toEqual({ state: "MATERIALIZED", plans: 1 });
  expect(result.flight.state).toBe("native_bundle_required");
  expect(result.carrier).toEqual({ state: "BOUND_VERIFIED", representations: 1 });
  expect(result.xtable.configs).toEqual([]);
  expect(result.xtable.runtime.state).toContain("jar_missing");
  expect(result.adbc.state).toBe("explicit_native_driver_and_probe_required");
  const check = new Database(dbPath, { readonly: true });
  expect(check.query("SELECT state FROM metadata_fabric WHERE kind='xtable-runtime'").get().state)
    .toBe(result.xtable.runtime.state);
  expect(check.query("SELECT state FROM carrier_binding WHERE carrier='native-fs'").get().state)
    .toBe("ready_verified");
  expect(check.query("SELECT state FROM plan_representation WHERE plan_kind='substrait-scan'").get().state)
    .toBe("physical");
  expect(check.query("SELECT count(*) n FROM transport_binding").get().n).toBe(0);
  check.close();
  expect(JSON.parse(readFileSync(result.report.json, "utf8")).metadata)
    .toContainEqual(["xtable-runtime", result.xtable.runtime.state, 1]);
  const runner = resolve(import.meta.dir, "../representation-manifold-fabric.mjs");
  const child = Bun.spawnSync([process.execPath, runner, "apply", "--estate", estate,
    "--runtime", runtime], { stdout: "pipe", stderr: "pipe" });
  if (child.exitCode) throw new Error(child.stderr.toString());
  expect(JSON.parse(child.stdout.toString()).state).toBe("APPLIED");
  const again = new Database(dbPath, { readonly: true });
  expect(again.query("SELECT count(*) n FROM plan_representation WHERE plan_kind='substrait-scan'").get().n).toBe(1);
  again.close();
});

test("staged Fabric install runs the same apply consumer with no repository sibling imports", () => {
  const stage = join(estate, "stage"), script = resolve(import.meta.dir, "..");
  expect(existsSync(join(script, "representation_manifold_fabric.py"))).toBe(false);
  expect(existsSync(join(script, "representation_manifold_fabric.py.archive"))).toBe(true);
  const installed = Bun.spawnSync(["make", "-C", script, "install-representation-fabric",
    `MEMBRANE_ROOT=${stage}`], { stdout: "pipe", stderr: "pipe" });
  if (installed.exitCode) throw new Error(installed.stderr.toString());
  expect(existsSync(join(stage, "scripts", "representation-manifold-substrait.mjs"))).toBe(true);
  expect(existsSync(join(stage, "scripts", "representation-manifold-zero-copy.mjs"))).toBe(true);
  expect(existsSync(join(stage, "scripts", "representation_manifold_fabric.py"))).toBe(false);
  const child = Bun.spawnSync([process.execPath, join(stage, "scripts", "representation-manifold-fabric.mjs"),
    "apply", "--estate", estate, "--runtime", join(estate, "runtime")],
  { stdout: "pipe", stderr: "pipe" });
  if (child.exitCode) throw new Error(child.stderr.toString());
  const result = JSON.parse(child.stdout.toString());
  expect(result.state).toBe("APPLIED");
  expect(result.substrait.plans).toBe(1);
  expect(result.carrier.representations).toBe(1);
});

test("XTable request without Spark is refused before manifold mutation", () => {
  const before = readFileSync(dbPath);
  const jar = join(estate, "xtable.jar");
  writeFileSync(jar, "fixture jar");
  const child = Bun.spawnSync([process.execPath, resolve(import.meta.dir, "../representation-manifold-fabric.mjs"),
    "apply", "--estate", estate, "--runtime", join(estate, "runtime"),
    "--xtable-jar", jar, "--run-xtable"], { stdout: "pipe", stderr: "pipe",
    env: { ...process.env, PATH: dirname(process.execPath) } });
  expect(child.exitCode).toBe(1);
  expect(child.stderr.toString()).toContain("requires an installed jar and spark-submit");
  expect(readFileSync(dbPath).equals(before)).toBe(true);
});

test("a wrong XTable jar pin is refused before manifold mutation", () => {
  const before = readFileSync(dbPath);
  const child = Bun.spawnSync([process.execPath, resolve(import.meta.dir, "../representation-manifold-fabric.mjs"),
    "apply", "--estate", estate, "--runtime", join(estate, "runtime"),
    "--xtable-jar", join(estate, "xtable.jar"), "--xtable-sha256", "0".repeat(64)],
  { stdout: "pipe", stderr: "pipe" });
  expect(child.exitCode).toBe(1);
  expect(child.stderr.toString()).toContain("XTable jar digest mismatch");
  expect(readFileSync(dbPath).equals(before)).toBe(true);
});

test("network bootstrap admits only an exact digest-pinned XTable body", async () => {
  const bytes = Buffer.from("xtable-runtime-fixture");
  const digest = createHash("sha256").update(bytes).digest("hex");
  const runtime = join(estate, "pinned-runtime");
  const options = { runtime, xtableUrl: "https://example.invalid/pinned.jar", xtableSha256: digest };
  let requests = 0;
  const fetcher = async () => { requests++; return new Response(bytes); };
  const staged = await stageXTableRuntime(options, fetcher);
  expect(staged.digest).toBe(digest);
  expect(readFileSync(staged.jar).equals(bytes)).toBe(true);
  const repeat = await stageXTableRuntime(options, fetcher);
  expect(repeat.digest).toBe(digest);
  expect(requests).toBe(1);
  await expect(stageXTableRuntime({ ...options, runtime: join(estate, "wrong-pin"),
    xtableSha256: "0".repeat(64) }, fetcher)).rejects.toThrow("downloaded jar digest mismatch");
  expect(existsSync(join(estate, "wrong-pin", "xtable", `${"0".repeat(64)}.jar`))).toBe(false);
});

test("an XTable consumer is invoked only over an admitted Iceberg receipt and stays unverified", () => {
  const metadata = join(estate, "representations", "iceberg-zero-copy", "warehouse", "estate",
    "fixture", "metadata", "v1.json");
  mkdirSync(dirname(metadata), { recursive: true });
  writeFileSync(metadata, JSON.stringify({ "format-version": 2,
    "table-uuid": "00000000-0000-4000-8000-000000000000",
    schemas: [{ "schema-id": 0, fields: [] }] }));
  const overlay = new Database(dbPath);
  overlay.query("INSERT INTO metadata_overlay VALUES(?,?,?,?,?,?,?,?)")
    .run("fixture", "iceberg", "root", parquet, metadata, "physical_zero_copy", "upstream fixture", "now");
  overlay.close();
  const bin = join(estate, "fake-bin");
  mkdirSync(bin);
  const spark = join(bin, "spark-submit"), captured = join(estate, "spark-argv.txt");
  writeFileSync(spark, '#!/bin/sh\nprintf "%s\\n" "$@" > "$BONFYRE_XTABLE_ARGS"\n');
  chmodSync(spark, 0o755);
  const jar = join(estate, "xtable.jar"), runner = resolve(import.meta.dir, "../representation-manifold-fabric.mjs");
  const child = Bun.spawnSync([process.execPath, runner, "apply", "--estate", estate,
    "--runtime", join(estate, "runtime"), "--xtable-jar", jar, "--run-xtable"],
  { stdout: "pipe", stderr: "pipe", env: { ...process.env, PATH: `${bin}:${process.env.PATH}`,
    BONFYRE_XTABLE_ARGS: captured } });
  if (child.exitCode) throw new Error(child.stderr.toString());
  const result = JSON.parse(child.stdout.toString());
  expect(result.xtable.configs).toHaveLength(1);
  expect(result.xtable.runs[0].state).toBe("conversion_exited_zero_unverified");
  expect(readFileSync(captured, "utf8")).toContain("org.apache.xtable.spark.XTableSparkSync");
  expect(readFileSync(captured, "utf8")).toContain(result.xtable.configs[0].config);
  const check = new Database(dbPath, { readonly: true });
  expect(check.query("SELECT state FROM metadata_overlay WHERE semantic_kind='fixture' AND format='xtable-delta-hudi'").get().state)
    .toBe("config_ready");
  expect(check.query("SELECT state FROM zero_copy_event WHERE kind='xtable'").get().state)
    .toBe("conversion_exited_zero_unverified");
  expect(check.query("SELECT count(*) n FROM representation_instance WHERE kind IN ('delta','hudi')").get().n).toBe(0);
  check.close();
});

test("a failing XTable consumer cannot be reported as a complete Fabric apply", () => {
  const bin = join(estate, "failing-bin");
  mkdirSync(bin);
  const spark = join(bin, "spark-submit");
  writeFileSync(spark, '#!/bin/sh\necho "conversion failed" >&2\nexit 17\n');
  chmodSync(spark, 0o755);
  const child = Bun.spawnSync([process.execPath, resolve(import.meta.dir, "../representation-manifold-fabric.mjs"),
    "apply", "--estate", estate, "--runtime", join(estate, "runtime"),
    "--xtable-jar", join(estate, "xtable.jar"), "--run-xtable"],
  { stdout: "pipe", stderr: "pipe", env: { ...process.env, PATH: `${bin}:${process.env.PATH}` } });
  if (child.exitCode) throw new Error(child.stderr.toString());
  const result = JSON.parse(child.stdout.toString());
  expect(result.state).toBe("PARTIAL_XTABLE_FAILURE");
  expect(result.xtable.runs[0]).toMatchObject({ state: "failed", exit_code: 17 });
  expect(result.xtable.runs[0].detail).toContain("conversion failed");
  const check = new Database(dbPath, { readonly: true });
  expect(check.query("SELECT count(*) n FROM representation_instance WHERE kind IN ('delta','hudi')").get().n).toBe(0);
  check.close();
});
