import { afterAll, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, symlinkSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { inspectFabric, writeFabricReport, inspectTargetLaws } from "../representation-manifold-report.mjs";

const estate = mkdtempSync(join(tmpdir(), "bonfyre-fabric-report-"));
mkdirSync(join(estate, "representations"));
afterAll(() => rmSync(estate, { recursive: true, force: true }));
const db = new Database(join(estate, "representation-manifold.db"));
db.exec(readFileSync(new URL("../../schemas/storage/representation-manifold.sql", import.meta.url), "utf8"));
db.query("INSERT INTO plan_representation VALUES(?,?,?,?,?,?)")
  .run("fixture", "substrait-scan", "/fixture/plan", "digest", "physical", "now");
db.query("INSERT INTO carrier_binding VALUES(?,?,?,?,?,?,?)")
  .run("root", "native-fs", "parquet/fixture", "/fixture", "ready_verified", "{}", "now");
db.query("INSERT INTO transport_binding VALUES(?,?,?,?,?,?)")
  .run("fixture", "arrow-flight", "flight://estate/fixture", "installed_unserved", "digest", "now");
db.query("INSERT INTO metadata_fabric VALUES(?,?,?,?,?)")
  .run("xtable-runtime", "/fixture/jar", "jar_missing+spark_missing", null, "now");
db.query("INSERT INTO representation_target VALUES(?,?,?,?,?)")
  .run("fixture", "substrait", "physical", null, "test");
db.close();

test("native report preserves fabric counts through its installed CLI", () => {
  const before = inspectFabric(estate);
  expect(before.plans).toEqual([["substrait-scan", "physical", 1]]);
  expect(before.transports).toEqual([["arrow-flight", "installed_unserved", 1]]);
  const written = writeFabricReport(estate);
  expect(written.state).toBe("DERIVED_REPORT");
  const nativeJson = JSON.parse(readFileSync(written.json, "utf8"));
  for (const field of ["schema", "plans", "carriers", "transports", "metadata", "targets"])
    expect(nativeJson[field]).toEqual(before[field]);
  expect(readFileSync(written.text, "utf8")).toContain("  arrow-flight                 installed_unserved     1");
  const native = Bun.spawnSync([process.execPath, resolve(import.meta.dir, "../representation-manifold-report.mjs"),
    "write", "--estate", estate],
    { stdout: "pipe", stderr: "pipe" });
  if (native.exitCode) throw new Error(native.stderr.toString());
  expect(JSON.parse(native.stdout.toString()).state).toBe("DERIVED_REPORT");
  const check = new Database(join(estate, "representation-manifold.db"), { readonly: true });
  expect(check.query("SELECT state FROM transport_binding WHERE semantic_kind='fixture' AND transport='arrow-flight'").get().state)
    .toBe("installed_unserved");
  check.close();
});

test("report refuses an existing symlink output", () => {
  const json = join(estate, "representations/fabric-report.json");
  rmSync(json);
  symlinkSync(join(estate, "representation-manifold.db"), json);
  expect(() => writeFabricReport(estate)).toThrow("unsafe existing report");
});

test("Cartesian representation targets reconstruct all five fields from laws and exceptions without writes", () => {
  const lawEstate = mkdtempSync(join(tmpdir(), "bonfyre-target-laws-"));
  try {
    const db = new Database(join(lawEstate, "representation-manifold.db"));
    db.exec(readFileSync(new URL("../../schemas/storage/representation-manifold.sql", import.meta.url), "utf8"));
    db.exec("BEGIN");
    const binding = db.query("INSERT INTO source_binding VALUES(?,?,?,?,?)");
    const add = db.query("INSERT INTO representation_target VALUES(?,?,?,?,?)");
    for (let s=0;s<202;s++) {
      const subject="kind-"+String(s).padStart(3,"0");
      binding.run("owner.db","table-"+String(s).padStart(3,"0"),subject,"sqlite","owner:row");
      for (let k=0;k<118;k++)
        add.run(subject,"repr-"+String(k).padStart(3,"0"),"declared_target",null,"authored policy");
    }
    for (let i=0;i<60;i++) {
      db.query("UPDATE representation_target SET status=?,substrate_path=?,reason=? WHERE semantic_kind=? AND representation_kind=?")
        .run("physical","/sub-"+i,i%2 ? null : "distinct exception","kind-"+String(i).padStart(3,"0"),"repr-"+String(i).padStart(3,"0"));
    }
    db.exec("COMMIT");
    db.close();
    const proof = inspectTargetLaws(lawEstate);
    expect(proof.state).toBe("EXACT_READ_PROVEN");
    expect(proof.source_rows).toBe(23836);
    expect(proof.law_rows).toBe(118);
    expect(proof.exception_rows).toBe(60);
    expect(proof.factorable_records).toBe(23658);
    expect(proof.original_digest_sha256).toBe(proof.reconstructed_digest_sha256);
    expect(proof.semantic_dimension_owner_complete).toBe(true);
    expect(proof.storage_replaced).toBe(false);
    const repeat = inspectTargetLaws(lawEstate);
    expect(repeat.original_digest_sha256).toBe(proof.original_digest_sha256);
    const mutation = new Database(join(lawEstate,"representation-manifold.db"));
    mutation.query("UPDATE representation_target SET reason=NULL,status='reconstructable' WHERE semantic_kind='kind-080' AND representation_kind='repr-090'").run();
    mutation.close();
    const updated = inspectTargetLaws(lawEstate);
    expect(updated.state).toBe("EXACT_READ_PROVEN");
    expect(updated.original_digest_sha256).not.toBe(proof.original_digest_sha256);
    expect(updated.reconstructed_digest_sha256).toBe(updated.original_digest_sha256);
    const sparse = new Database(join(lawEstate,"representation-manifold.db"));
    sparse.query("DELETE FROM representation_target WHERE semantic_kind='kind-000' AND representation_kind='repr-000'").run();
    sparse.close();
    const held = inspectTargetLaws(lawEstate);
    expect(held.state).toBe("HELD_SPARSE_OR_UNOWNED");
    expect(held.source_rows).toBe(23835);
    expect(held.reconstructed_digest_sha256).toBeNull();
  } finally { rmSync(lawEstate, { recursive: true, force: true }); }
});
