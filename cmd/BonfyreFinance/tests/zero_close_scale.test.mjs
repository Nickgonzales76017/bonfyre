
import {test,expect} from "bun:test";
import {Database} from "bun:sqlite";
import {mkdtempSync,writeFileSync,appendFileSync,rmSync,readFileSync,statSync} from "node:fs";
import {tmpdir} from "node:os";
import {join,resolve,dirname} from "node:path";
import {fileURLToPath} from "node:url";
import {spawnSync} from "node:child_process";
import {createHash} from "node:crypto";

const here=dirname(fileURLToPath(import.meta.url)),finance=resolve(here,"../bonfyre-finance");
const header="id\tsource_kind\tsource_ref\tsource_digest\tobserved_at\ttitle\tdesired_state\tcompensation_cents\tcurrency\tdeadline\tcontact_boundary\texpected_compute_cents\texpected_external_cents\thuman_minutes\treuse_points\trisk_points\n";
const reqHeader="opportunity_id\tkind\trequirement_key\towner_ref\tstate\tevidence_ref\n";
const digest=s=>createHash("sha256").update(s).digest("hex");
function run(args,timeout=180000){const r=spawnSync(finance,args,{encoding:"utf8",timeout,maxBuffer:4_000_000});expect(r.status,r.stderr).toBe(0);return r;}
function row(id,i){return [id,"classified","https://classified.example/"+id,digest("source:"+id),"2026-10-05T12:00:00Z",
  "Observed external demand "+i,"Produce and verify requested outcome",String(20000+(i%1000)*25),"USD","2026-10-12T00:00:00Z",
  "email",String(i%500),String(i%7===0?500:0),String(5+(i%60)),String(i%20),String(i%10)].join("\t")+"\n";}

test("100k opportunities close capability requirements and project pre-authority work without effects",()=>{
  const dir=mkdtempSync(join(tmpdir(),"bf-zc-scale-"));
  try{
    const db=join(dir,"finance.db"),opp=join(dir,"opportunities.tsv"),req=join(dir,"requirements.tsv");
    const batch=join(dir,"queue.tsv"),count=Number(process.env.BONFYRE_ZERO_CLOSE_SCALE??100000);
    expect(count).toBeGreaterThanOrEqual(10000);
    writeFileSync(opp,header);writeFileSync(req,reqHeader);
    for(let start=0;start<count;start+=1000){
      let a="",b="";
      for(let i=start;i<Math.min(count,start+1000);i++){
        const id="scale-"+String(i).padStart(6,"0");
        a+=row(id,i);
        b+=id+"\tcapability\tdeliverable\tcapability:project\tsatisfied\tcapability://project\n";
        if(i===0) b+=id+"\tmaterial\tcost_basis_confirmed\treviewer:costs\tsatisfied\treceipt://full-cost-envelope\n";
      }
      appendFileSync(opp,a);appendFileSync(req,b);
    }
    const begin=performance.now();
    run(["--db",db,"opportunity","import","--file",opp]);
    const imported=performance.now();
    run(["--db",db,"opportunity","requirements-import","--file",req]);
    const closed=performance.now();
    const ranked=run(["--db",db,"opportunity","rank","--limit","25"]).stdout;
    expect(ranked.split("\n").length).toBeGreaterThan(20);
    run(["--db",db,"opportunity","queue-batch","--out",batch,"--limit",String(count)]);
    const projected=performance.now();

    const f=new Database(db,{readonly:true});
    try{
      expect(f.query("SELECT count(*) n FROM opportunities").get().n).toBe(count);
      expect(f.query("SELECT count(*) n FROM opportunities WHERE stage='solvable'").get().n).toBe(count);
      expect(f.query("SELECT count(*) n FROM opportunity_events").get().n).toBe(0);
      expect(f.query("SELECT count(*) n FROM opportunities WHERE authority_ref IS NOT NULL OR customer_ref IS NOT NULL OR payment_ref IS NOT NULL").get().n).toBe(0);
    }finally{f.close();}
    // Only the first record has a receipted complete cost basis; the other
    // 99,999 have typed-in estimates, not evidence for their margins.
    expect(readFileSync(batch,"utf8").trim().split("\n").length).toBe(1);
    expect(readFileSync(batch,"utf8")).toContain("finance://opportunity/scale-000000");
    console.log(JSON.stringify({
      witness:"zero_close_scale",opportunities:count,
      import_ms:imported-begin,requirements_and_closure_ms:closed-imported,
      rank_and_projection_ms:projected-closed,total_ms:projected-begin,
      finance_db_bytes:statSync(db).size,queue_batch_bytes:statSync(batch).size,
      outreach_effects:0,payment_effects:0
    }));
  }finally{rmSync(dir,{recursive:true,force:true});}
},240000);
