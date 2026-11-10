import { afterAll, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { copyFileSync, existsSync, lstatSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { bodyDigest } from "../columnar_body.mjs";
import { applyZeroCopy, hardlinkExactParquet, planZeroCopy, probeDuckDB, stageXTableConfigs,
  writeZeroCopyReport } from "../representation-manifold-zero-copy.mjs";

const estate = mkdtempSync(join(tmpdir(), "bonfyre-zero-copy-"));
afterAll(() => rmSync(estate, { recursive: true, force: true }));
const bodyDir = join(estate, "representations", "bodies", "parquet", "sha256", "fixture");
mkdirSync(bodyDir, { recursive: true });
const one = join(bodyDir, "one.parquet"), two = join(bodyDir, "two.parquet");
const built = Bun.spawnSync(["duckdb", "-batch", ":memory:",
  `COPY (SELECT 1 AS source, 2 AS target) TO '${one.replaceAll("'", "''")}' (FORMAT PARQUET)`],
  { stdout: "pipe", stderr: "pipe" });
if (built.exitCode) throw new Error(built.stderr.toString());
copyFileSync(one, two);
const digest = await bodyDigest(one);
const schema = readFileSync(new URL("../../schemas/storage/representation-manifold.sql", import.meta.url), "utf8");
const db = new Database(join(estate, "representation-manifold.db"));
db.exec(schema);
for (const [kind, coordinate] of [["fixture.one", one], ["fixture.two", two]]) {
  db.query("INSERT INTO representation_instance VALUES(?,?,?,?,?,?,?,?,?,?)")
    .run(`root:${kind}`, null, "parquet", "derived", "frontier-1", digest, kind, new Date().toISOString(), 1, "physical");
  db.query("INSERT INTO physical_realization VALUES(?,?,?,?,?,?,?,?,?)")
    .run(`file:${kind}`, `root:${kind}`, "local", coordinate, null, digest, "warm", 1, new Date().toISOString());
  db.query("INSERT INTO representation_target VALUES(?,?,?,?,?)")
    .run(kind, "iceberg", "declared_target", null, "fixture");
}
db.close();

test("installed Bun entry point plans without Python and legacy worker is archived", () => {
  const worker = fileURLToPath(new URL("../representation-manifold-zero-copy.mjs", import.meta.url));
  const makefile = readFileSync(new URL("../Makefile", import.meta.url), "utf8");
  expect(makefile).toMatch(/^install-representation-zero-copy: install-representation-fabric$/m);
  expect(makefile).toContain("install -m 0755 representation-manifold-fabric.mjs representation-manifold-zero-copy.mjs");
  expect(makefile).not.toMatch(/install[^\n]*representation_manifold_zero_copy\.py/);
  expect(existsSync(fileURLToPath(new URL("../representation_manifold_zero_copy.py", import.meta.url)))).toBe(false);
  expect(existsSync(fileURLToPath(new URL("../representation_manifold_zero_copy.py.archive", import.meta.url)))).toBe(true);
  const child = Bun.spawnSync([process.execPath, worker, "--estate", estate], {
    stdout: "pipe", stderr: "pipe", env: { ...process.env, PATH: dirname(process.execPath) },
  });
  expect(child.exitCode).toBe(0);
  const plan = JSON.parse(child.stdout.toString());
  expect(plan.canonical.map(row => row.kind)).toEqual(["fixture.one", "fixture.two"]);
  expect(plan.native_gaps).toContain("Iceberg add-files: no spec-valid native writer/manifest reader is installed");
  const bundle = join(estate, "preflight-bundle"); mkdirSync(bundle);
  writeFileSync(join(bundle, "manifest.json"), "{}\n");
  const apply = Bun.spawnSync([process.execPath, worker, "--estate", estate,
    "--native-arrow-bundle", bundle, "--apply"], {
    stdout: "pipe", stderr: "pipe", env: { ...process.env, PATH: dirname(process.execPath) },
  });
  expect(apply.exitCode).toBe(1);
  expect(apply.stderr.toString()).toContain("Iceberg targets require add-files; native writer unavailable");
  const check = new Database(join(estate, "representation-manifold.db"), { readonly: true });
  expect(check.query("SELECT count(*) n FROM sqlite_master WHERE type='table' AND name='metadata_overlay'").get().n).toBe(0);
  check.close();
});

test("plan is read-only and Iceberg add-files refusal precedes all mutation", async () => {
  const plan = await planZeroCopy(estate);
  expect(plan.canonical.length).toBe(2);
  expect(plan.native_gaps.some(gap => gap.includes("Iceberg add-files"))).toBe(true);
  const bundle = join(estate, "bundle"); mkdirSync(bundle);
  writeFileSync(join(bundle, "manifest.json"), "{}");
  await expect(applyZeroCopy({ estate, bundle, allowPartial: false })).rejects.toThrow("native writer unavailable");
  const jar = join(estate, "xtable.jar"); writeFileSync(jar, "fixture");
  if (!Bun.which("spark-submit"))
    await expect(applyZeroCopy({ estate, bundle, runXTable: true, xtableJar: jar, allowPartial: true }))
      .rejects.toThrow("spark-submit");
  const check = new Database(join(estate, "representation-manifold.db"), { readonly: true });
  expect(check.query("SELECT count(*) n FROM representation_instance WHERE kind='iceberg'").get().n).toBe(0);
  check.close();
});

test("exact registered canonical dedup links bytes and excludes Delta clone", async () => {
  const cloneDir = join(estate, "representations", "delta", "fixture"); mkdirSync(cloneDir, { recursive: true });
  const clone = join(cloneDir, "part.parquet"); copyFileSync(one, clone);
  const cloneInode = lstatSync(clone).ino;
  expect(lstatSync(one).ino).not.toBe(lstatSync(two).ino);
  const result = await hardlinkExactParquet(estate, { minBytes: 0 });
  expect(result).toMatchObject({ eligible: 2, links: 1 });
  expect(result.logical_saved_bytes).toBeGreaterThan(0);
  const bytes = lstatSync(one).size;
  expect(result.verification.sha256_scans).toBe(4);
  expect(result.verification.logical_bytes_hashed).toBe(String(bytes * 4));
  expect(result.verification.physical_io_bytes).toBeNull();
  expect(result.verification.physical_space_reclaimed_bytes).toBeNull();
  expect(lstatSync(one).ino).toBe(lstatSync(two).ino);
  expect(lstatSync(clone).ino).toBe(cloneInode);
  const again = await hardlinkExactParquet(estate, { minBytes: 0 });
  expect(again.links).toBe(0);
  expect(again.verification.sha256_scans).toBe(2);
  expect(again.verification.logical_bytes_hashed).toBe(String(bytes * 2));
});

test("DuckDB probe records exact native execution, not DataFusion or Polars", async () => {
  const probed = await probeDuckDB(estate);
  expect(probed.map(row => row.state)).toEqual(["executable", "executable"]);
  const check = new Database(join(estate, "representation-manifold.db"), { readonly: true });
  expect(check.query("SELECT count(*) n FROM execution_binding WHERE engine='duckdb' AND state='executable'").get().n).toBe(2);
  expect(check.query("SELECT count(*) n FROM execution_binding WHERE engine IN ('datafusion','polars','adbc')").get().n).toBe(0);
  check.close();
});

test("XTable config requires an existing Iceberg receipt and never asserts conversion", async () => {
  expect(await stageXTableConfigs(estate)).toEqual([]);
  const metadata = join(estate, "representations", "iceberg-zero-copy", "warehouse", "estate", "fixture_one", "metadata", "v1.json");
  mkdirSync(join(metadata, ".."), { recursive: true });
  writeFileSync(metadata, "{}\n");
  const source = new Database(join(estate, "representation-manifold.db"));
  source.query("INSERT INTO metadata_overlay VALUES(?,?,?,?,?,?,?,?)")
    .run("fixture.one", "iceberg", "root:fixture.one", one, metadata, "physical_zero_copy", "upstream fixture receipt", new Date().toISOString());
  source.close();
  await expect(stageXTableConfigs(estate)).rejects.toThrow("header is incomplete");
  writeFileSync(metadata, JSON.stringify({ "format-version": 2,
    "table-uuid": "00000000-0000-4000-8000-000000000000", schemas: [{ "schema-id": 0, fields: [] }] }));
  const configs = await stageXTableConfigs(estate);
  expect(configs.length).toBe(1);
  const body = readFileSync(configs[0].config, "utf8");
  expect(body).toContain("sourceFormat: ICEBERG");
  expect(body).toContain("targetFormats:\n  - DELTA\n  - HUDI");
  const policy = new Database(join(estate, "representation-manifold.db"));
  policy.query("UPDATE byte_economics SET rationale='locally reviewed policy' WHERE representation_kind='parquet'").run();
  policy.close();
  expect(await stageXTableConfigs(estate)).toEqual(configs);
  const check = new Database(join(estate, "representation-manifold.db"), { readonly: true });
  expect(check.query("SELECT rationale FROM byte_economics WHERE representation_kind='parquet'").get().rationale)
    .toBe("locally reviewed policy");
  expect(check.query("SELECT state FROM metadata_overlay WHERE semantic_kind='fixture.one' AND format='xtable-delta-hudi'").get().state)
    .toBe("config_ready");
  expect(check.query("SELECT count(*) n FROM representation_instance WHERE kind IN ('delta','hudi')").get().n).toBe(0);
  check.close();
});

test("report preserves old schema and names unavailable native stages", () => {
  const report = writeZeroCopyReport(estate, 1234, 5678);
  expect(report.schema).toBe("bonfyre-zero-copy-pivot.v1");
  expect(report.physical_kinds).toContainEqual(["parquet", 2]);
  expect(report.native_gaps.some(gap => gap.includes("Iceberg"))).toBe(true);
  expect(existsSync(join(estate, "representations", "zero-copy-report.txt"))).toBe(true);
});
