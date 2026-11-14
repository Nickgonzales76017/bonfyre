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

test("query plan is pinned and returns candidate local or distributed resource stages",()=>{
  const estate=create();
  try{
    const base=["query-plan","--estate",estate,"--semantic-kind","example","--kind","parquet",
      "--operation","aggregate","--generation","1","--memory-limit-bytes","1048576",
      "--workers","4","--input-bound-bytes","4096"];
    expect(run(...base).code).not.toBe(0);
    const observed=new Database(join(estate,"representation-manifold.db"));
    observed.query("UPDATE representation_instance SET frontier='f1;rows:17' WHERE id='current'").run();
    observed.close();
    expect(run("seed","--apply","--estate",estate).code).toBe(0);
    const local=run(...base);
    expect(local.code).toBe(0);
    expect(local.data).toMatchObject({
      schema:"bonfyre.warehouse.query-plan.v1",decision:"LOCAL_CANDIDATE",
      operation:"aggregate",proposed_route:"local-duckdb-candidate",
      input_bound_bytes:4096,worker_count:4,actor_closure_verified:false,
      packgraph_verified:false,scheduler_admitted:false,exchange_executed:false,
      physical_bytes_verified:false,left:{semantic_kind:"example",generation:1,digest:hex("a"),frontier_rows:17}});
    const big=base.map(x=>x==="4096"?"600000":x);
    expect(run(...big).data).toMatchObject({decision:"DISTRIBUTED_CANDIDATE",
      proposed_route:"distributed-exchange-candidate",aggregate_stage_budget_bytes:1048576,
      proposed_stages:["bounded-scan","partial-aggregate","partition-exchange","final-aggregate"]});
    const limited=big.map(x=>x==="4"?"1":x);
    expect(run(...limited).data).toMatchObject({decision:"RESOURCE_REFUSED",
      proposed_route:"none",proposed_stages:[],scheduler_admitted:false});
    expect(run(...base.map(x=>x==="4096"?"1":x)).code).not.toBe(0);
    expect(run(...base.map(x=>x==="1048576"?"0":x)).code).not.toBe(0);
    expect(run(...base.map(x=>x==="4096"?"18446744073709551615":x)).code).not.toBe(0);
    expect(run(...base.map(x=>x==="1"?"2":x)).code).not.toBe(0);
  }finally{rmSync(estate,{recursive:true,force:true});}
});
test("query planning binds both join inputs and refuses drift rather than silently rebinding",()=>{
  const estate=create();
  try{
    const db=new Database(join(estate,"representation-manifold.db"));
    db.query("INSERT INTO source_binding VALUES(?,?,?,?,?)").run("/tmp/r","right","right","sqlite",null);
    db.query("INSERT INTO representation_target VALUES(?,?,?,?,?)")
      .run("right","parquet","physical",null,"right target");
    db.query("INSERT INTO representation_instance "
      +"(id,kind,authority,frontier,digest,schema_ref,created_at,reconstructable,state) "
      +"VALUES(?,?,?,?,?,?,?,?,?)").run("right-rep","parquet","derived","f2",hex("b"),
      "right","2026-11-05T00:00:00Z",1,"physical");
    db.query("INSERT INTO physical_realization VALUES(?,?,?,?,?,?,?,?,?)")
      .run("right-body","right-rep","file","/never-open-right",128,hex("b"),"warm",1,"2026-11-05");
    db.close();
    expect(run("seed","--apply","--estate",estate).code).toBe(0);
    const args=["query-plan","--estate",estate,"--semantic-kind","example","--kind","parquet",
      "--operation","join","--generation","1","--input-bound-bytes","500000",
      "--memory-limit-bytes","1048576","--workers","4",
      "--right-semantic-kind","right","--right-generation","1","--right-input-bound-bytes","400000"];
    const plan=run(...args);
    expect(plan.code).toBe(0);
    expect(plan.data).toMatchObject({decision:"DISTRIBUTED_CANDIDATE",
      operation:"join",input_bound_bytes:900000,
      proposed_stages:["bounded-scan","partition-exchange","join-stage","result-merge"],
      left:{representation_id:"current",digest:hex("a"),generation:1},
      right:{representation_id:"right-rep",digest:hex("b"),generation:1,frontier_rows:null},
      worker_recovery_verified:false,scheduler_admitted:false});
    expect(run(...args.map(x=>x==="400000"?"1":x)).code).not.toBe(0);
    const edit=new Database(join(estate,"representation-manifold.db"));
    edit.query("UPDATE physical_realization SET available=0 WHERE id='right-body'").run();
    edit.close();
    expect(run(...args).code).not.toBe(0);
  }finally{rmSync(estate,{recursive:true,force:true});}
});

test("metadata pressure census classifies old unbound reconstructable history without deleting",()=>{
  const estate=create();
  try{
    const db=new Database(join(estate,"representation-manifold.db"));
    const ri=db.query("INSERT INTO representation_instance "
      +"(id,kind,authority,frontier,digest,schema_ref,created_at,reconstructable,state) "
      +"VALUES(?,?,?,?,?,?,?,?,?)");
    const stamp="2026-01-01T00:00:00Z";
    ri.run("orphan-old","parquet","derived","f",hex("c"),"orphan",stamp,1,"reconstructable");
    ri.run("referenced-old","parquet","derived","f",hex("c"),"orphan",stamp,1,"reconstructable");
    ri.run("receipted-old","parquet","derived","f",hex("c"),"orphan",stamp,1,"reconstructable");
    ri.run("unbound-new","parquet","derived","f",hex("c"),"orphan",
      "2026-11-30T00:00:00Z",1,"reconstructable");
    ri.run("bound-old","parquet","derived","f",hex("c"),"example",stamp,1,"reconstructable");
    ri.run("auth-old","parquet","source","f",hex("c"),"orphan",stamp,1,"reconstructable");
    db.query("INSERT INTO representation_edge VALUES(?,?,?,?,?)")
      .run("referenced-old","current","derived","materialized",stamp);
    db.query("INSERT INTO eviction_receipt VALUES(?,?,?,?,?,?,?,?,?)")
      .run("e1","receipted-old","parquet","/old",hex("c"),10,
        "obsolete","/none","2026-01-01");
    db.close();
    const argv=["retention","--estate",estate,"--cutoff","2026-06-01T00:00:00Z"];
    const first=run(...argv);
    expect(first.code).toBe(0);
    expect(first.data).toMatchObject({
      schema:"bonfyre.warehouse.metadata-pressure.v1",state:"RETAINED_METADATA_CENSUS",
      representation_instances:7,pinned_heads:0,instances_without_current_source:5,
      old_reconstructable_without_available_body:5,
      unreferenced_history_candidates:1,eviction_receipts:1,
      deleted:0,gc_authority:false,candidate_safe_to_delete:false});
    expect(run("seed","--apply","--estate",estate).code).toBe(0);
    expect(run(...argv).data).toMatchObject({
      pinned_heads:1,unreferenced_history_candidates:1});
    const verify=new Database(join(estate,"representation-manifold.db"),{readonly:true});
    expect(verify.query("SELECT COUNT(*) n FROM representation_instance").get().n).toBe(7);
    verify.close();
  }finally{rmSync(estate,{recursive:true,force:true});}
});
test("metadata pressure census rejects malformed retention cutoffs",()=>{
  const estate=create();
  try{
    for(const bad of ["2026-01-01","2026-01-01T00:00:00Z;DELETE",
      "2026-01-01T00:00:00+00:00","wrong",
      "2026-13-01T00:00:00Z","2026-02-30T00:00:00Z",
      "2025-02-29T00:00:00Z","2026-01-01T24:00:00Z"])
      expect(run("retention","--estate",estate,"--cutoff",bad).code).not.toBe(0);
    expect(run("retention","--estate",estate).code).not.toBe(0);
    expect(run("retention","--estate",estate,
      "--cutoff","2026-06-01T00:00:00Z","--apply").code).not.toBe(0);
  }finally{rmSync(estate,{recursive:true,force:true});}
});


test("202-table metadata stress with 100k retired generations and one indexed generation advance",()=>{
  const estate=create();
  try{
    const db=new Database(join(estate,"representation-manifold.db"));
    const addSource=db.query("INSERT INTO source_binding VALUES(?,?,?,?,?)");
    const target=db.query("INSERT INTO representation_target VALUES(?,?,?,?,?)");
    const ri=db.query("INSERT INTO representation_instance "
      +"(id,kind,authority,frontier,digest,schema_ref,created_at,reconstructable,state) "
      +"VALUES(?,?,?,?,?,?,?,?,?)");
    const body=db.query("INSERT INTO physical_realization VALUES(?,?,?,?,?,?,?,?,?)");
    const transaction=db.transaction(()=>{
      for(let i=1;i<202;i++){
        const semantic="source-"+i,id="active-"+i;
        addSource.run("/tmp/db-"+i,"events",semantic,"sqlite",null);
        target.run(semantic,"parquet","physical",null,"materialized");
        ri.run(id,"parquet","derived","f",hex("a"),semantic,
          "2026-11-05T00:00:00Z",1,"physical");
        body.run("body-"+i,id,"file","/never-open-"+i,44+i,hex("a"),
          "warm",1,"2026-11-05");
      }
      for(let i=0;i<100000;i++)
        ri.run("retired-"+i,"parquet","derived","f",hex("e"),
          "deleted-"+Math.floor(i/100),"2026-01-01T00:00:00Z",1,"reconstructable");
    });
    transaction();db.close();
    const seedStart=performance.now();
    const seeded=run("seed","--apply","--estate",estate);
    const seedMs=Math.round(performance.now()-seedStart);
    expect(seeded.code).toBe(0);
    expect(seeded.data).toMatchObject({state:"HEADS_SEEDED",source_keys:202,
      seeded:202,statistics_updated:202,physical_mutations:0});
    expect(run("verify","--estate",estate).data).toMatchObject({
      state:"WAREHOUSE_METADATA_VERIFIED",verified:true,heads:202,statistics:202});
    const retention=run("retention","--estate",estate,
      "--cutoff","2026-06-01T00:00:00Z");
    expect(retention.code).toBe(0);
    expect(retention.data).toMatchObject({
      representation_instances:100202,instances_without_current_source:100000,
      unreferenced_history_candidates:100000,deleted:0,gc_authority:false});
    const writable=new Database(join(estate,"representation-manifold.db"));
    writable.transaction(()=>{
      writable.query("INSERT INTO representation_instance "
        +"(id,kind,authority,frontier,digest,schema_ref,created_at,reconstructable,state) "
        +"VALUES(?,?,?,?,?,?,?,?,?)").run("current-next","parquet","derived","f-next",
          hex("b"),"example","2026-11-06T00:00:00Z",1,"physical");
      writable.query("INSERT INTO physical_realization VALUES(?,?,?,?,?,?,?,?,?)")
        .run("next-body","current-next","file","/never-open-next",46,
          hex("b"),"warm",1,"2026-11-06");
      writable.query("UPDATE physical_realization SET available=0 WHERE id='body'").run();
    })();
    writable.close();
    const casStart=performance.now();
    const update=bindWarehouseHead(estate,{semanticKind:"example",representationKind:"parquet",
      representationId:"current-next",digest:hex("b"),expectedGeneration:1});
    const casMs=Math.round(performance.now()-casStart);
    expect(update.head.generation).toBe(2);
    const verification=run("verify","--estate",estate);
    expect(verification.data).toMatchObject({verified:true,heads:202,invalid_heads:0});
    const planDb=new Database(join(estate,"representation-manifold.db"),{readonly:true});
    const plan=planDb.query("EXPLAIN QUERY PLAN SELECT id FROM representation_instance "
      +"WHERE schema_ref=? AND kind=? ORDER BY created_at DESC,id DESC LIMIT 1")
      .all("example","parquet");
    expect(plan.some(x=>x.detail.includes("idx_warehouse_ri_latest"))).toBe(true);
    expect(planDb.query("SELECT COUNT(*) n FROM representation_instance").get().n)
      .toBe(100203);
    planDb.close();
    console.log("WAREHOUSE_CHURN_FIXTURE",JSON.stringify({
      active_sources:202,retired_generations:100000,seed_ms:seedMs,
      one_table_cas_ms:casMs,metadata_verification:"PASS",
      scope:"temporary-local-SQLite-fixture-not-PostHog-scale"}));
  }finally{rmSync(estate,{recursive:true,force:true});}
});

test("Aurekai portfolio pages 202 active data products without leaking body paths",()=>{
 const estate=create();
 try{
  const db=new Database(join(estate,"representation-manifold.db"));
  const tx=db.transaction(()=>{
    const source=db.query("INSERT INTO source_binding VALUES(?,?,?,?,?)");
    const target=db.query("INSERT INTO representation_target VALUES(?,?,?,?,?)");
    const repr=db.query("INSERT INTO representation_instance "
      +"(id,kind,authority,frontier,digest,schema_ref,created_at,reconstructable,state) "
      +"VALUES(?,?,?,?,?,?,?,?,?)");
    const body=db.query("INSERT INTO physical_realization VALUES(?,?,?,?,?,?,?,?,?)");
    for(let i=1;i<202;i++){
      const k="src-"+String(i).padStart(3,"0"),id="rid-"+i;
      source.run("/tmp/private-"+i,"table",k,"sqlite",null);
      target.run(k,"parquet","physical",null,"target");
      repr.run(id,"parquet","derived","f",hex("a"),k,"2026-11-05T00:00:00Z",1,"physical");
      body.run("body-"+i,id,"file","/private/bytes-"+i,100,hex("a"),"warm",1,"2026-11-05");
    }
  });tx();db.close();
  expect(run("portfolio","--estate",estate,"--offset","0","--limit","100").data.state)
    .toBe("NOT_INSTALLED");
  expect(run("seed","--apply","--estate",estate).code).toBe(0);
  const first=run("portfolio","--estate",estate,"--offset","0","--limit","100");
  const second=run("portfolio","--estate",estate,"--offset","100","--limit","100");
  const third=run("portfolio","--estate",estate,"--offset","200","--limit","100");
  expect(first.code).toBe(0);
  expect(first.data).toMatchObject({schema:"bonfyre.warehouse.portfolio.v1",
    state:"READ_ONLY",total:202,offset:0,next_offset:100,read_only:true,
    execution_authority:false,tenant_access_verified:false,physical_bytes_verified:false});
  expect(first.data.rows).toHaveLength(100);
  expect(second.data.rows).toHaveLength(100);
  expect(third.data.rows).toHaveLength(2);
  expect(third.data.next_offset).toBeNull();
  expect(first.data.rows[0]).toMatchObject({semantic_kind:"example",state:"PINNED_METADATA",
    representation_kind:"parquet",generation:1,representation_id:"current",
    digest:hex("a"),realization_count:1,max_realization_bytes:44});
  const outputs=[first,second,third].map(r=>r.stdout).join("");
  expect(outputs).not.toContain("/private/");
  expect(outputs).not.toContain("/tmp/private");
  expect(outputs).not.toContain("source_db");
  expect(run("portfolio","--estate",estate,"--offset","0","--limit","101").code).not.toBe(0);
  expect(run("portfolio","--estate",estate,"--offset","-1","--limit","10").code).not.toBe(0);
  const change=new Database(join(estate,"representation-manifold.db"));
  change.query("UPDATE physical_realization SET available=0 WHERE id='body'").run();
  change.close();
  const changed=run("portfolio","--estate",estate,"--offset","0","--limit","100");
  expect(changed.data.rows[0]).toEqual({semantic_kind:"example",state:"UNAVAILABLE"});
 }finally{rmSync(estate,{recursive:true,force:true});}
});
