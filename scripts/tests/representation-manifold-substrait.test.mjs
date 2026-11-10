import { afterAll, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { appendFileSync, existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { bodyDigest } from "../columnar_body.mjs";
import { materializeAllSubstrait, materializeSubstrait, planSubstrait, substraitType } from "../representation-manifold-substrait.mjs";

const estate = mkdtempSync(join(tmpdir(), "bonfyre-substrait-"));
const parquet = join(estate, "representations", "parquet", "fixture.parquet");
mkdirSync(join(estate, "representations", "parquet"), { recursive: true });
afterAll(() => rmSync(estate, { recursive: true, force: true }));
const copy = Bun.spawnSync(["duckdb", "-batch", ":memory:", `COPY (
  SELECT true::BOOLEAN AS flag, 7::BIGINT AS id, 'alpha'::VARCHAR AS caption,
    12.34::DECIMAL(10,2) amount, DATE '2026-01-01' observed,
    [1,2]::INTEGER[] items, NULL::VARCHAR optional
  UNION ALL
  SELECT false, 8, 'beta', 5.67, DATE '2026-01-02', [3], 'present'
) TO '${parquet.replaceAll("'", "''")}' (FORMAT PARQUET)`], { stdout: "pipe", stderr: "pipe" });
if (copy.exitCode) throw new Error(copy.stderr.toString());
const digest = await bodyDigest(parquet);
const db = new Database(join(estate, "representation-manifold.db"));
db.exec(readFileSync(new URL("../../schemas/storage/representation-manifold.sql", import.meta.url), "utf8"));
db.query("INSERT INTO representation_instance VALUES(?,?,?,?,?,?,?,?,?,?)")
  .run("root", null, "parquet", "derived", "frontier-1", digest, "fixture", new Date().toISOString(), 1, "physical");
db.query("INSERT INTO physical_realization VALUES(?,?,?,?,?,?,?,?,?)")
  .run("root-file", "root", "local", parquet, null, digest, "warm", 1, new Date().toISOString());
db.query("INSERT INTO representation_target VALUES(?,?,?,?,?)")
  .run("fixture", "substrait", "declared", null, "test demand");
db.close();

test("scan type lowering refuses unsupported types instead of inventing a conversion", () => {
  expect(substraitType("VARCHAR").length).toBeGreaterThan(0);
  expect(substraitType("INTEGER[]").length).toBeGreaterThan(0);
  expect(() => substraitType("TIMESTAMP_NS")).toThrow("requires explicit lowering");
});

test("native Substrait plan has exact canonical lineage and independent consumer parity", async () => {
  const planned = await planSubstrait(estate, "fixture");
  expect(planned.columns).toHaveLength(7);
  expect(existsSync(planned.coordinate)).toBe(false);
  const stage = join(estate, "stage");
  const install = Bun.spawnSync(["make", "-C", resolve(import.meta.dir, ".."),
    "install-representation-substrait", `MEMBRANE_ROOT=${stage}`], { stdout: "pipe", stderr: "pipe" });
  expect(install.exitCode).toBe(0);
  const stagedPlan = Bun.spawnSync(["bun", join(stage, "scripts/representation-manifold-substrait.mjs"),
    "plan", "--estate", estate, "--kind", "fixture"], { stdout: "pipe", stderr: "pipe" });
  expect(stagedPlan.exitCode).toBe(0);
  expect(JSON.parse(stagedPlan.stdout.toString()).digest).toBe(planned.digest);
  const first = await materializeSubstrait(estate, "fixture");
  expect(first).toMatchObject({ source_digest: digest, state: "physical" });
  expect(await bodyDigest(first.coordinate)).toBe(first.digest);
  const check = new Database(join(estate, "representation-manifold.db"), { readonly: true });
  expect(check.query("SELECT digest,state FROM plan_representation WHERE semantic_kind='fixture' AND plan_kind='substrait-scan'").get())
    .toEqual({ digest: first.digest, state: "physical" });
  expect(check.query("SELECT input_digest,output_digest,proof_state FROM transformation_occurrence WHERE transform='parquet->substrait-scan'").get())
    .toEqual({ input_digest: digest, output_digest: first.digest, proof_state: "materialized" });
  expect(check.query("SELECT count(*) n FROM representation_edge WHERE parent_representation_id='root' AND child_representation_id=?").get(first.representation_id).n).toBe(1);
  check.close();
  const reader = process.env.DATAFUSION_READER_PYTHON;
  if (reader) {
    const code = `import json,sys\nfrom datafusion import SessionContext\nfrom datafusion.substrait import Consumer,Serde\nc=SessionContext(); c.register_parquet(sys.argv[3],sys.argv[2]); p=Serde.deserialize(sys.argv[1]); d=c.create_dataframe_from_logical_plan(Consumer.from_substrait_plan(c,p)); rows=d.to_pylist(); print(json.dumps({'rows':len(rows),'columns':list(rows[0]),'ids':[r['id'] for r in rows]}))`;
    const consumed = Bun.spawnSync([reader, "-B", "-c", code, first.coordinate, parquet, first.table],
      { stdout: "pipe", stderr: "pipe" });
    if (consumed.exitCode) throw new Error(`independent DataFusion rejected native plan: ${consumed.stderr.toString()}`);
    expect(JSON.parse(consumed.stdout.toString())).toMatchObject({ rows: 2, ids: [7, 8],
      columns: ["flag", "id", "caption", "amount", "observed", "items", "optional"] });
  }
  const repeat = await materializeSubstrait(estate, "fixture");
  expect(repeat.digest).toBe(first.digest);
  expect(repeat.representation_id).toBe(first.representation_id);
  expect(await materializeAllSubstrait(estate)).toEqual({ state: "MATERIALIZED", plans: 1 });
  const cli = Bun.spawnSync([process.execPath, resolve(import.meta.dir, "../representation-manifold-substrait.mjs"),
    "materialize-all", "--estate", estate], { stdout: "pipe", stderr: "pipe" });
  if (cli.exitCode) throw new Error(cli.stderr.toString());
  expect(JSON.parse(cli.stdout.toString())).toEqual({ state: "MATERIALIZED", plans: 1 });
  appendFileSync(parquet, "drift");
  await expect(materializeSubstrait(estate, "fixture")).rejects.toThrow("canonical Parquet drift");
});
