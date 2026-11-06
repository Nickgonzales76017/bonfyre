import { afterAll, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { existsSync, lstatSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { bodyDigest } from "../columnar_body.mjs";
import { deltaType, materializeDelta, planDelta } from "../representation-manifold-delta.mjs";

const estate = mkdtempSync(join(tmpdir(), "bonfyre-delta-forge-"));
const parquet = join(estate, "canonical.parquet");
const schema = readFileSync(new URL("../../schemas/storage/representation-manifold.sql", import.meta.url), "utf8");
afterAll(() => rmSync(estate, { recursive: true, force: true }));

const build = Bun.spawnSync(["duckdb", "-batch", ":memory:", `COPY (
  SELECT 7::BIGINT AS id, 'alpha'::VARCHAR AS label, [1,2]::INTEGER[] AS items,
    12.34::DECIMAL(10,2) AS amount, DATE '2026-01-01' AS observed, NULL::VARCHAR AS optional
  UNION ALL
  SELECT 8::BIGINT, 'beta', [3]::INTEGER[], 5.67::DECIMAL(10,2), DATE '2026-01-02', 'present'
) TO '${parquet.replaceAll("'", "''")}' (FORMAT PARQUET)`], { stdout: "pipe", stderr: "pipe" });
if (build.exitCode) throw new Error(build.stderr.toString());
const digest = await bodyDigest(parquet);
const db = new Database(join(estate, "representation-manifold.db"));
db.exec(schema);
db.query(`INSERT INTO representation_instance VALUES(?,?,?,?,?,?,?,?,?,?)`).run(
  "canonical-root", null, "parquet", "derived", "frontier-1", digest, "fixture", new Date().toISOString(), 1, "physical");
db.query(`INSERT INTO physical_realization VALUES(?,?,?,?,?,?,?,?,?)`).run(
  "canonical-file", "canonical-root", "local", parquet, null, digest, "warm", 1, new Date().toISOString());
db.query(`INSERT INTO representation_target VALUES(?,?,?,?,?)`).run("fixture", "delta", "declared", null, "test demand");
db.close();

test("Delta v1 maps supported Parquet types and refuses silent conversion", () => {
  expect(deltaType("INTEGER[]")).toEqual({ type: "array", elementType: "integer", containsNull: true });
  expect(deltaType("DECIMAL(10,2)")).toBe("decimal(10,2)");
  expect(() => deltaType("TIMESTAMP_NS")).toThrow("cannot represent");
  expect(() => deltaType("DECIMAL(39,2)")).toThrow("cannot represent");
});

test("planning is read-only; one demand creates a metadata-only Delta view with exact lineage", async () => {
  const planned = await planDelta(estate, "fixture");
  expect(planned.rows).toBe(2);
  expect(existsSync(planned.directory)).toBe(false);
  const first = await materializeDelta(estate, "fixture");
  expect(first).toMatchObject({ state: "physical", input_digest: digest, rows: 2, clone: "copy_on_write" });
  const log = readFileSync(join(first.coordinate, "_delta_log", "00000000000000000000.json"), "utf8");
  const [protocol, metadata, add] = log.trim().split("\n").map(JSON.parse);
  expect(protocol.protocol).toEqual({ minReaderVersion: 1, minWriterVersion: 2 });
  expect(add.add.path).toBe(`part-${digest}.parquet`);
  expect(lstatSync(join(first.coordinate, add.add.path)).ino).not.toBe(lstatSync(parquet).ino);
  expect(add.add.tags["bonfyre.sourceSha256"]).toBe(digest);
  expect(JSON.parse(metadata.metaData.schemaString).fields.map(field => field.name))
    .toEqual(["id", "label", "items", "amount", "observed", "optional"]);
  const check = new Database(join(estate, "representation-manifold.db"), { readonly: true });
  expect(check.query("SELECT status FROM representation_target WHERE semantic_kind='fixture' AND representation_kind='delta'").get().status).toBe("physical");
  expect(check.query("SELECT proof_state,input_digest,output_digest FROM transformation_occurrence WHERE transform='parquet->delta-log'").get())
    .toMatchObject({ proof_state: "materialized", input_digest: digest, output_digest: first.digest });
  expect(check.query("SELECT count(*) n FROM representation_edge WHERE parent_representation_id='canonical-root' AND child_representation_id=?").get(first.representation_id).n).toBe(1);
  check.close();
  const second = await materializeDelta(estate, "fixture");
  expect(second.state).toBe("verified_existing");
  expect(second.representation_id).toBe(first.representation_id);
  expect(second.digest).toBe(first.digest);
  const deltaReader = process.env.DELTA_READER_PYTHON;
  if (deltaReader) {
    const verify = Bun.spawnSync([deltaReader, "-c", `from deltalake import DeltaTable\nimport json,sys\nt=DeltaTable(sys.argv[1]); body=t.to_pyarrow_table(); print(json.dumps({'version':t.version(),'rows':body.num_rows,'columns':body.schema.names}))`, first.coordinate],
      { stdout: "pipe", stderr: "pipe", env: { ...process.env, RAYON_NUM_THREADS: "1", OMP_NUM_THREADS: "1", ARROW_IO_THREADS: "1" } });
    if (verify.exitCode !== 0) throw new Error(`independent Delta reader refused view: ${verify.stderr.toString()}`);
    expect(JSON.parse(verify.stdout.toString())).toMatchObject({ version: 0, rows: 2,
      columns: ["id", "label", "items", "amount", "observed", "optional"] });
  }
}, 20_000);

test("changed canonical bytes or unowned view member cannot acquire a new receipt", async () => {
  const baseline = await planDelta(estate, "fixture");
  const logPath = join(baseline.directory, "_delta_log", "00000000000000000000.json");
  const goodLog = readFileSync(logPath, "utf8");
  writeFileSync(logPath, goodLog.replace('"numRecords\\\":2', '"numRecords\\\":99'));
  await expect(materializeDelta(estate, "fixture")).rejects.toThrow("does not match");
  writeFileSync(logPath, goodLog);
  writeFileSync(join(baseline.directory, "foreign"), "not ours");
  await expect(materializeDelta(estate, "fixture")).rejects.toThrow("unowned member");
  rmSync(join(baseline.directory, "foreign"));
  writeFileSync(parquet, "drift", { flag: "a" });
  expect(await bodyDigest(join(baseline.directory, `part-${digest}.parquet`))).toBe(digest);
  await expect(materializeDelta(estate, "fixture")).rejects.toThrow("digest drift");
  const check = new Database(join(estate, "representation-manifold.db"), { readonly: true });
  expect(check.query("SELECT count(*) n FROM transformation_occurrence WHERE transform='parquet->delta-log'").get().n).toBe(1);
  check.close();
});
