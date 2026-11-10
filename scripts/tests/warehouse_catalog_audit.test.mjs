import { test, expect } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { inspectWarehouseCatalog, formatWarehouseCatalog } from "../warehouse-catalog-audit.mjs";

const schema = readFileSync(join(import.meta.dir, "../../schemas/storage/representation-manifold.sql"),"utf8");
function fixture() {
  const estate=mkdtempSync(join(tmpdir(),"bonfyre-warehouse-audit-"));
  const db=new Database(join(estate,"representation-manifold.db"));
  db.exec(schema);
  const insert=db.query("INSERT INTO representation_instance "
    + "(id,kind,authority,frontier,digest,schema_ref,created_at,reconstructable,state) "
    + "VALUES(?,?,?,?,?,?,?,?,?)");
  for(let i=0;i<180;i++)
    insert.run("other-"+i,"parquet","derived","f"+i,"d"+i,
      "other","2026-11-05T00:00:00Z",1,"reconstructable");
  insert.run("live","parquet","derived","current","digest",
    "target","2026-11-05T01:00:00Z",1,"physical");
  db.query("INSERT INTO physical_realization VALUES(?,?,?,?,?,?,?,?,?)")
    .run("real-live","live","file","/tmp/not-opened",100,"digest","hot",1,"2026-11-05");
  db.query("INSERT INTO eviction_receipt VALUES(?,?,?,?,?,?,?,?,?)")
    .run("evict-old","other-1","parquet","old","digest",100,"historical","parent","2026-11-05");
  db.query("INSERT INTO representation_target VALUES(?,?,?,?,?)")
    .run("target","parquet","physical","/tmp/not-opened",null);
  db.close();
  return estate;
}

test("catalog audit counts metadata and detects newest lookup scan", () => {
  const estate=fixture();
  try {
    const report=inspectWarehouseCatalog(estate);
    expect(report.state).toBe("UNINDEXED_LOOKUP");
    expect(report.instances.total).toBe(181);
    expect(report.instances.physical).toBe(1);
    expect(report.evictions).toBe(1);
    expect(report.cohorts[0].semantic_kind).toBe("other");
    expect(formatWarehouseCatalog(report)).toContain("READ ONLY");
  } finally { rmSync(estate,{recursive:true,force:true}); }
});

test("catalog audit detects scoped newest-generation index without altering it", () => {
  const estate=fixture();
  try {
    const db=new Database(join(estate,"representation-manifold.db"));
    db.exec("CREATE INDEX idx_warehouse_test ON representation_instance(schema_ref,kind,created_at DESC,id DESC)");
    db.close();
    const report=inspectWarehouseCatalog(estate);
    expect(report.latest_lookup.indexes).toContain("idx_warehouse_test");
    expect(report.state).toBe("INDEXED_LOOKUP");
    expect(report.instances.total).toBe(181);
  } finally { rmSync(estate,{recursive:true,force:true}); }
});

test("catalog audit fails closed when the source is absent", () => {
  const estate=mkdtempSync(join(tmpdir(),"bonfyre-warehouse-empty-"));
  try {
    expect(() => inspectWarehouseCatalog(estate)).toThrow("missing manifold");
    expect(() => inspectWarehouseCatalog(estate,0)).toThrow("limit");
  } finally { rmSync(estate,{recursive:true,force:true}); }
});
