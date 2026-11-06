import { test, expect } from "bun:test";
import { Database } from "bun:sqlite";
import { createHash } from "node:crypto";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { planDerivedGc, reclaimDerived } from "../representation-manifold-gc.mjs";

const sha = bytes => createHash("sha256").update(bytes).digest("hex");
const schema = readFileSync(join(import.meta.dir, "../../schemas/storage/representation-manifold.sql"), "utf8");

function fixture({ drift = false, shared = false, missingParent = false, directory = false } = {}) {
  const estate = mkdtempSync(join(tmpdir(), "bonfyre-repr-gc-"));
  const root = join(estate, "representations");
  mkdirSync(join(root, "parquet"), { recursive: true });
  const kind = directory ? "zarr" : "arrow";
  mkdirSync(join(root, kind), { recursive: true });
  const parent = join(root, "parquet", "canonical.parquet");
  const child = join(root, kind, directory ? "derived.zarr" : "derived.arrow");
  writeFileSync(parent, "canonical fixture");
  let childDigest = sha("derived fixture");
  if (directory) {
    mkdirSync(join(child, "values"), { recursive: true });
    writeFileSync(join(child, "zarr.json"), "zarr root");
    writeFileSync(join(child, "values", "zarr.json"), drift ? "changed values" : "zarr values");
    childDigest = sha(`values/zarr.json:${sha("zarr values")}|zarr.json:${sha("zarr root")}`);
  } else writeFileSync(child, drift ? "changed fixture" : "derived fixture");
  const db = new Database(join(estate, "representation-manifold.db"));
  db.exec(schema);
  db.query(`INSERT INTO representation_instance
    (id,kind,authority,frontier,digest,schema_ref,created_at,reconstructable,state)
    VALUES(?,?,?,?,?,?,?,?,?)`).run("parent", "parquet", "derived", "cut-1",
      sha("canonical fixture"), "fixture", "2026-10-01T00:00:00Z", 1, "physical");
  db.query(`INSERT INTO representation_instance
    (id,kind,authority,frontier,digest,schema_ref,created_at,reconstructable,state)
    VALUES(?,?,?,?,?,?,?,?,?)`).run("child", kind, "derived", "cut-1",
      childDigest, "fixture", "2026-10-01T00:00:00Z", 1, "physical");
  db.query("INSERT INTO physical_realization VALUES(?,?,?,?,?,?,?,?,?)")
    .run("parent-physical", "parent", "file", parent, 17, sha("canonical fixture"), "pinned", 1, "2026-10-01T00:00:00Z");
  db.query("INSERT INTO physical_realization VALUES(?,?,?,?,?,?,?,?,?)")
    .run("child-physical", "child", "file", child, 15, childDigest, "warm", 1, "2026-10-01T00:00:00Z");
  db.query("INSERT INTO representation_edge VALUES(?,?,?,?,?)")
    .run("parent", "child", "parquet-to-arrow", "materialized", "2026-10-01T00:00:00Z");
  db.query("INSERT INTO byte_economics VALUES(?,?,?,?,?,?)")
    .run(kind, "execution_on_demand", "reconstructable", "parquet", "derived bytes", "2026-10-01T00:00:00Z");
  db.query("INSERT INTO representation_target VALUES(?,?,?,?,?)")
    .run("fixture", kind, "physical", null, null);
  if (missingParent) db.query("UPDATE physical_realization SET available=0 WHERE id='parent-physical'").run();
  if (shared) db.query("INSERT INTO physical_realization VALUES(?,?,?,?,?,?,?,?,?)")
    .run("other-physical", "parent", "file", child, 15, sha("derived fixture"), "warm", 1, "2026-10-01T00:00:00Z");
  db.close();
  return { estate, parent, child };
}

test("GC verifies the historical sorted Zarr directory manifest before removing a derived tree", async () => {
  const { estate, parent, child } = fixture({ directory: true });
  try {
    const plan = await planDerivedGc(estate, ["zarr"]);
    expect(plan).toHaveLength(1);
    expect(plan[0].directory).toBe(true);
    expect(plan[0].bytes).toBe(Buffer.byteLength("zarr rootzarr values"));
    const result = await reclaimDerived(estate, { kinds: ["zarr"], floorBytes: Number.MAX_SAFE_INTEGER });
    expect(result.evicted).toHaveLength(1);
    expect(existsSync(child)).toBe(false);
    expect(existsSync(parent)).toBe(true);
  } finally { rmSync(estate, { recursive: true, force: true }); }
});

test("GC refuses a Zarr tree whose manifest has drifted", async () => {
  const { estate, parent, child } = fixture({ directory: true, drift: true });
  try {
    expect(await planDerivedGc(estate, ["zarr"])).toHaveLength(0);
    expect((await reclaimDerived(estate, {
      kinds: ["zarr"], floorBytes: Number.MAX_SAFE_INTEGER
    })).evicted).toHaveLength(0);
    expect(existsSync(child)).toBe(true);
    expect(existsSync(parent)).toBe(true);
  } finally { rmSync(estate, { recursive: true, force: true }); }
});

test("GC refuses a Zarr tree shared with another live realization", async () => {
  const { estate, parent, child } = fixture({ directory: true });
  try {
    const db = new Database(join(estate, "representation-manifold.db"));
    db.query(`INSERT INTO representation_instance
      (id,kind,authority,frontier,digest,schema_ref,created_at,reconstructable,state)
      VALUES(?,?,?,?,?,?,?,?,?)`).run("other", "member", "derived", "cut-1",
        sha("zarr root"), "fixture", "2026-10-01T00:00:00Z", 1, "physical");
    db.query("INSERT INTO physical_realization VALUES(?,?,?,?,?,?,?,?,?)")
      .run("shared-member", "other", "file", join(child, "zarr.json"), 9,
        sha("zarr root"), "hot", 1, "2026-10-01T00:00:00Z");
    db.close();
    expect(await planDerivedGc(estate, ["zarr"])).toHaveLength(0);
    expect(existsSync(child)).toBe(true);
    expect(existsSync(parent)).toBe(true);
  } finally { rmSync(estate, { recursive: true, force: true }); }
});

test("GC rejects symlink members in a derived directory", async () => {
  const { estate, parent, child } = fixture({ directory: true });
  try {
    symlinkSync(parent, join(child, "linked-canonical"));
    await expect(planDerivedGc(estate, ["zarr"])).rejects.toThrow("unsafe directory member");
    expect(existsSync(child)).toBe(true);
    expect(existsSync(parent)).toBe(true);
  } finally { rmSync(estate, { recursive: true, force: true }); }
});

test("GC only evicts a digest-verified derived file with a live canonical parent", async () => {
  const { estate, parent, child } = fixture();
  try {
    const plan = await planDerivedGc(estate, ["arrow"]);
    expect(plan).toHaveLength(1);
    expect(existsSync(child)).toBe(true);
    const result = await reclaimDerived(estate, { kinds: ["arrow"], floorBytes: Number.MAX_SAFE_INTEGER });
    expect(result.evicted).toHaveLength(1);
    expect(existsSync(child)).toBe(false);
    expect(existsSync(parent)).toBe(true);
    const db = new Database(join(estate, "representation-manifold.db"), { readonly: true });
    expect(db.query("SELECT state FROM representation_instance WHERE id='child'").get().state).toBe("reconstructable");
    expect(db.query("SELECT available FROM physical_realization WHERE id='child-physical'").get().available).toBe(0);
    expect(db.query("SELECT status FROM representation_target WHERE semantic_kind='fixture' AND representation_kind='arrow'").get().status).toBe("reconstructable");
    expect(db.query("SELECT reconstruct_from FROM eviction_receipt").get().reconstruct_from).toBe(parent);
    db.close();
  } finally { rmSync(estate, { recursive: true, force: true }); }
});

test.each([{ drift: true }, { shared: true }, { missingParent: true }])(
  "GC refuses drift, shared coordinates, and missing canonical lineage (%p)", async options => {
    const { estate, parent, child } = fixture(options);
    try {
      expect(await planDerivedGc(estate, ["arrow"])).toHaveLength(0);
      const result = await reclaimDerived(estate, { kinds: ["arrow"], floorBytes: Number.MAX_SAFE_INTEGER });
      expect(result.evicted).toHaveLength(0);
      expect(existsSync(parent)).toBe(true);
      expect(existsSync(child)).toBe(true);
    } finally { rmSync(estate, { recursive: true, force: true }); }
  });
