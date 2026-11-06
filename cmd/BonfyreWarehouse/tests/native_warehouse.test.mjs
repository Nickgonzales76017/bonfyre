import {test,expect} from "bun:test";
import {Database} from "bun:sqlite";
import {mkdtempSync,readFileSync,rmSync,symlinkSync} from "node:fs";
import {tmpdir} from "node:os";
import {join} from "node:path";
import {installWarehouseHead,bindWarehouseHead}
  from "../../../scripts/warehouse-snapshot-head.mjs";

const schema=readFileSync(join(import.meta.dir,"../../../schemas/storage/representation-manifold.sql"),"utf8");
const binary=join(import.meta.dir,"../bonfyre-warehouse");
const seedBinary=join(import.meta.dir,"../../BonfyreWarehouseSeed/bonfyre-warehouse-seed");
const hex=c=>c.repeat(64);
function create() {
  const estate=mkdtempSync(join(tmpdir(),"bonfyre-native-warehouse-"));
  const db=new Database(join(estate,"representation-manifold.db"));
  db.exec(schema);
  db.query("INSERT INTO source_binding VALUES(?,?,?,?,?)").run(
    "/tmp/s","events","example","sqlite",null);
  db.query("INSERT INTO representation_target VALUES(?,?,?,?,?)")
    .run("example","parquet","physical",null,"derived target");
  db.query("INSERT INTO representation_instance "
    +"(id,kind,authority,frontier,digest,schema_ref,created_at,reconstructable,state) "
    +"VALUES(?,?,?,?,?,?,?,?,?)").run(
      "current","parquet","derived","f1",hex("a"),"example","2026-11-05T00:00:00Z",1,"physical");
  db.query("INSERT INTO physical_realization VALUES(?,?,?,?,?,?,?,?,?)")
    .run("body","current","file","/never-open",44,hex("a"),"warm",1,"2026-11-05");
  db.close();
  return estate;
}
function run(...args) {
  const selected=args[0]==="seed" ? seedBinary : binary;
  const p=Bun.spawnSync([selected,...args],{stdout:"pipe",stderr:"pipe"});
  const stdout=new TextDecoder().decode(p.stdout);
  return {code:p.exitCode,stdout,stderr:new TextDecoder().decode(p.stderr),
    data:p.exitCode===0?JSON.parse(stdout):null};
}
test("native status reads original manifold without installing a head",()=>{
  const estate=create();
  try{
    const r=run("status","--estate",estate);
    expect(r.code).toBe(0);
    expect(r.data).toMatchObject({state:"READ_ONLY",warehouse_heads_installed:false,
      representation_instances:1,source_bindings:1,execution_authority:false});
  }finally{rmSync(estate,{recursive:true,force:true});}
});
test("native head follows Bun's canonical head writer and rejects invalidation",()=>{
  const estate=create();
  try{
    const flags=["--estate",estate,"--semantic-kind","example","--kind","parquet"];
    expect(run("head",...flags).data.state).toBe("NOT_INSTALLED");
    installWarehouseHead(estate);
    expect(run("head",...flags).data.state).toBe("NO_HEAD");
    bindWarehouseHead(estate,{semanticKind:"example",representationKind:"parquet",
      representationId:"current",digest:hex("a"),expectedGeneration:0});
    const read=run("head",...flags);
    expect(read.code).toBe(0);
    expect(read.data).toMatchObject({state:"METADATA_HEAD_CURRENT",generation:1,
      representation_id:"current",digest:hex("a"),physical_bytes_verified:false});
    const plan=run("plan",...flags);
    expect(plan.data.state).toBe("QUERY_PLAN");
    expect(plan.data.plan).toContain("INDEX");
    const db=new Database(join(estate,"representation-manifold.db"));
    db.query("UPDATE physical_realization SET available=0 WHERE id='body'").run();
    db.close();
    expect(run("head",...flags).data).toMatchObject({state:"STALE_HEAD_REFUSED",head:null});
  }finally{rmSync(estate,{recursive:true,force:true});}
});
test("native reader refuses missing, symlinked and malformed paths",()=>{
  const estate=mkdtempSync(join(tmpdir(),"bonfyre-native-symlink-"));
  try{
    expect(run("status","--estate",estate).code).not.toBe(0);
    expect(run("head","--estate",estate,"--semantic-kind","';DROP","--kind","parquet").code).not.toBe(0);
    const target=join(estate,"target.db");
    const db=new Database(target);db.exec(schema);db.close();
    symlinkSync(target,join(estate,"representation-manifold.db"));
    expect(run("status","--estate",estate).code).not.toBe(0);
  }finally{rmSync(estate,{recursive:true,force:true});}
});
test("integrity classifies orphan metadata without deleting or rewriting anything",()=>{
  const estate=create();
  try{
    installWarehouseHead(estate);
    bindWarehouseHead(estate,{semanticKind:"example",representationKind:"parquet",
      representationId:"current",digest:hex("a"),expectedGeneration:0});
    const db=new Database(join(estate,"representation-manifold.db"));
    db.query("INSERT INTO physical_realization VALUES(?,?,?,?,?,?,?,?,?)")
      .run("orphan","missing","file","/never-open-2",10,hex("b"),"warm",1,"2026-11-05");
    db.query("INSERT INTO representation_edge VALUES(?,?,?,?,?)")
      .run("missing","current","derived","materialized","2026-11-05");
    db.query("INSERT INTO eviction_receipt VALUES(?,?,?,?,?,?,?,?,?)")
      .run("er-missing","missing","parquet","/never-open-2",hex("b"),10,"obsolete","/nothing","2026-11-05");
    db.query("UPDATE physical_realization SET available=0 WHERE id='body'").run();
    db.close();
    const report=run("integrity","--estate",estate);
    expect(report.code).toBe(0);
    expect(report.data).toMatchObject({state:"METADATA_INTEGRITY_OBSERVED",
      dangling_realizations:1,dangling_edges:1,dangling_eviction_receipts:1,
      stale_heads:1,deleted:0,safe_to_delete_inferred:false});
    const verify=new Database(join(estate,"representation-manifold.db"),{readonly:true});
    expect(verify.query("SELECT COUNT(*) n FROM eviction_receipt").get().n).toBe(1);
    expect(verify.query("SELECT COUNT(*) n FROM physical_realization").get().n).toBe(2);
    verify.close();
  }finally{rmSync(estate,{recursive:true,force:true});}
});

test("read-only key audit refuses to infer a one-to-one source identity from shared semantic labels",()=>{
  const estate=create();
  try{
    let r=run("cohorts","--estate",estate);
    expect(r.code).toBe(0);
    expect(r.data).toMatchObject({state:"SOURCE_KEY_COHORT_AUDIT",source_rows:1,
      semantic_format_keys:1,collision_keys:0,keys_with_live_physical:1,
      keys_with_multiple_live_generations:0,unambiguous_single_live_keys:1,
      safe_to_seed_all:true,readonly:true,heads_written:0});
    const db=new Database(join(estate,"representation-manifold.db"));
    db.query("INSERT INTO source_binding VALUES(?,?,?,?,?)").run(
      "/tmp/other-source","other_table","example","parquet",null);
    db.close();
    r=run("cohorts","--estate",estate);
    expect(r.code).toBe(0);
    expect(r.data).toMatchObject({source_rows:2,semantic_format_keys:1,
      collision_keys:1,source_rows_in_collision_keys:2,
      unambiguous_single_live_keys:0,safe_to_seed_all:false,heads_written:0});
  }finally{rmSync(estate,{recursive:true,force:true});}
});
test("key audit distinguishes multiple live versions without deleting history",()=>{
  const estate=create();
  try{
    const db=new Database(join(estate,"representation-manifold.db"));
    db.query("INSERT INTO representation_instance "
      +"(id,kind,authority,frontier,digest,schema_ref,created_at,reconstructable,state) "
      +"VALUES(?,?,?,?,?,?,?,?,?)").run("v2","parquet","derived","f2",hex("b"),
      "example","2026-11-05T01:00:00Z",1,"physical");
    db.query("INSERT INTO physical_realization VALUES(?,?,?,?,?,?,?,?,?)")
      .run("body2","v2","file","/never-open-v2",42,hex("b"),"warm",1,"2026-11-05");
    db.close();
    const r=run("cohorts","--estate",estate);
    expect(r.code).toBe(0);
    expect(r.data).toMatchObject({collision_keys:0,keys_with_live_physical:1,
      keys_with_multiple_live_generations:1,unambiguous_single_live_keys:0,
      safe_to_seed_all:false});
  }finally{rmSync(estate,{recursive:true,force:true});}
});

test("native seed installs indexed heads and incremental stats in one existing manifold",()=>{
  const estate=create();
  try{
    const before=run("status","--estate",estate);
    expect(before.data.warehouse_heads_installed).toBe(false);
    expect(run("seed","--estate",estate).code).not.toBe(0);
    const seeded=run("seed","--apply","--estate",estate);
    expect(seeded.code).toBe(0);
    expect(seeded.data).toMatchObject({state:"HEADS_SEEDED",source_keys:1,
      seeded:1,reused:0,statistics_updated:1,physical_mutations:0});
    const flags=["--estate",estate,"--semantic-kind","example","--kind","parquet"];
    expect(run("head",...flags).data).toMatchObject({state:"METADATA_HEAD_CURRENT",
      generation:1,representation_id:"current"});
    expect(run("plan",...flags).data.plan).toContain("USING INDEX");
    const second=run("seed","--apply","--estate",estate);
    expect(second.data).toMatchObject({seeded:0,reused:1,statistics_updated:1});
    const db=new Database(join(estate,"representation-manifold.db"),{readonly:true});
    expect(db.query("PRAGMA index_list(\'representation_instance\')").all()
      .some(row=>row.name==="idx_warehouse_ri_latest")).toBe(true);
    expect(db.query("SELECT generation,realization_count,max_realization_bytes "
      +"FROM warehouse_snapshot_stats").get()).toMatchObject({
        generation:1,realization_count:1,max_realization_bytes:44});
    expect(db.query("SELECT COUNT(*) n FROM physical_realization").get().n).toBe(1);
    db.close();
  }finally{rmSync(estate,{recursive:true,force:true});}
});
test("native seed refuses colliding source identities without any schema writes",()=>{
  const estate=create();
  try{
    const db=new Database(join(estate,"representation-manifold.db"));
    db.query("INSERT INTO source_binding VALUES(?,?,?,?,?)")
      .run("/tmp/other-source","other","example","sqlite",null);
    db.close();
    const blocked=run("seed","--apply","--estate",estate);
    expect(blocked.code).not.toBe(0);
    const after=run("status","--estate",estate);
    expect(after.data).toMatchObject({warehouse_heads_installed:false,source_bindings:2});
  }finally{rmSync(estate,{recursive:true,force:true});}
});
test("native seed refuses ambiguous generations and restores prior schema on rollback",()=>{
  const estate=create();
  try{
    const db=new Database(join(estate,"representation-manifold.db"));
    db.query("INSERT INTO representation_instance "
      +"(id,kind,authority,frontier,digest,schema_ref,created_at,reconstructable,state) "
      +"VALUES(?,?,?,?,?,?,?,?,?)").run("other-v","parquet","derived","f2",hex("b"),
        "example","2026-11-05T01:00:00Z",1,"physical");
    db.query("INSERT INTO physical_realization VALUES(?,?,?,?,?,?,?,?,?)")
      .run("body2","other-v","file","/never-open-v2",42,hex("b"),"warm",1,"2026-11-05");
    db.close();
    expect(run("seed","--apply","--estate",estate).code).not.toBe(0);
    expect(run("status","--estate",estate).data.warehouse_heads_installed).toBe(false);
  }finally{rmSync(estate,{recursive:true,force:true});}
});
test("native seed refuses an unverified target or a corrupted existing head",()=>{
  const estate=create();
  try{
    const db=new Database(join(estate,"representation-manifold.db"));
    db.query("UPDATE representation_target SET status='declared_target' WHERE semantic_kind='example'").run();
    db.close();
    expect(run("seed","--apply","--estate",estate).code).not.toBe(0);
    const change=new Database(join(estate,"representation-manifold.db"));
    change.query("UPDATE representation_target SET status='physical' WHERE semantic_kind='example'").run();
    change.close();
    expect(run("seed","--apply","--estate",estate).code).toBe(0);
    const corrupt=new Database(join(estate,"representation-manifold.db"));
    corrupt.query("UPDATE warehouse_snapshot_head SET digest=? WHERE semantic_kind='example'")
      .run(hex("f"));
    corrupt.close();
    expect(run("seed","--apply","--estate",estate).code).not.toBe(0);
    expect(run("head","--estate",estate,"--semantic-kind","example","--kind","parquet").data.state)
      .toBe("STALE_HEAD_REFUSED");
  }finally{rmSync(estate,{recursive:true,force:true});}
});

test("read-only warehouse binary cannot execute or advertise an effect",()=>{
  const estate=create();
  try{
    const p=Bun.spawnSync([binary,"seed","--apply","--estate",estate],{
      stdout:"pipe",stderr:"pipe"});
    expect(p.exitCode).not.toBe(0);
    expect(run("status","--estate",estate).data.warehouse_heads_installed).toBe(false);
  }finally{rmSync(estate,{recursive:true,force:true});}
});

test("native integrity and reader reject ambiguous sources and withdrawn physical targets",()=>{
  const estate=create();
  try{
    expect(run("seed","--apply","--estate",estate).code).toBe(0);
    const flags=["--estate",estate,"--semantic-kind","example","--kind","parquet"];
    const db=new Database(join(estate,"representation-manifold.db"));
    db.query("INSERT INTO source_binding VALUES(?,?,?,?,?)")
      .run("/tmp/duplicate","table","example","sqlite",null);
    db.close();
    expect(run("head",...flags).data.state).toBe("STALE_HEAD_REFUSED");
    expect(run("integrity","--estate",estate).data.stale_heads).toBe(1);
    const edit=new Database(join(estate,"representation-manifold.db"));
    edit.query("DELETE FROM source_binding WHERE source_db='/tmp/duplicate'").run();
    edit.query("UPDATE representation_target SET status='declared_target' "
      +"WHERE semantic_kind='example' AND representation_kind='parquet'").run();
    edit.close();
    expect(run("head",...flags).data.state).toBe("STALE_HEAD_REFUSED");
    expect(run("integrity","--estate",estate).data.stale_heads).toBe(1);
  }finally{rmSync(estate,{recursive:true,force:true});}
});
test("independent read-only recovery verification rejects drift and accepts only pinned metadata",()=>{
  const estate=create();
  try{
    const args=["verify","--estate",estate];
    expect(run(...args).data).toMatchObject({state:"NOT_INSTALLED",verified:false});
    expect(run("seed","--apply","--estate",estate).code).toBe(0);
    expect(run(...args).data).toMatchObject({
      state:"WAREHOUSE_METADATA_VERIFIED",verified:true,source_keys:1,
      heads:1,statistics:1,invalid_heads:0,indexes_present:3,
      sqlite_quick_check:true,physical_bytes_verified:false,
      distributed_execution_verified:false});
    const db=new Database(join(estate,"representation-manifold.db"));
    db.query("UPDATE warehouse_snapshot_stats SET max_realization_bytes=5").run();
    db.close();
    expect(run(...args).data).toMatchObject({
      state:"RECOVERY_REFUSED",verified:false,invalid_heads:1});
    const fix=new Database(join(estate,"representation-manifold.db"));
    fix.query("UPDATE warehouse_snapshot_stats SET max_realization_bytes=44").run();
    fix.query("UPDATE physical_realization SET available=0 WHERE id='body'").run();
    fix.close();
    expect(run(...args).data).toMatchObject({
      state:"RECOVERY_REFUSED",verified:false,invalid_heads:1});
  }finally{rmSync(estate,{recursive:true,force:true});}
});
