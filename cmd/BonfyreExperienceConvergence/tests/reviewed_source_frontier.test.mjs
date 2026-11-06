import {test,expect} from "bun:test";
import {Database} from "bun:sqlite";
import {mkdtempSync,existsSync,mkdirSync,readFileSync,rmSync} from "node:fs";
import {tmpdir} from "node:os";
import {join,resolve} from "node:path";
import {spawnSync} from "node:child_process";
import {inspect,decide,compose,run} from "../tools/reviewed_source_frontier.mjs";
import {openVerification} from "../tools/open_reviewed_verification.mjs";

const root=resolve(import.meta.dir,"../../..");
const cmsBin=resolve(root,"cmd/BonfyreCMS/bonfyre-cms");
const projectBin=resolve(root,"components/BonfyreProject/bonfyre-project");
const schemas=resolve(root,"cmd/BonfyreCMS/content-types");
function exec(bin,args,env={}) {
  const r=spawnSync(bin,args,{encoding:"utf8",maxBuffer:1024*1024,timeout:60000,
    env:{...process.env,BONFYRE_FACT_DELTAS:"",...env}});
  if(r.status!==0)throw Error(bin+" returned "+r.status+": "+(r.stderr||r.stdout).slice(-1100));
  return r.stdout;
}
const fixture=()=>{
  const dir=mkdtempSync(join(tmpdir(),"bonfyre-reviewed-source-"));
  const cms=join(dir,"cms.db"),frontier=join(dir,"frontier.db"),archive=join(dir,"archive.db");
  exec(cmsBin,["schema","migrate","--db",cms,"--schemas",schemas]);
  const create=(type,payload)=>exec(cmsBin,["entry","create",type,JSON.stringify(payload),
    "--ns","root","--db",cms,"--schemas",schemas]);
  const at="2026-10-14T21:00:00Z";
  create("discovery_set",{set_id:"real-set",label:"Actual observed school source channels",
    criteria:"host_suffix=.edu;publishes_plane=calendar",monitoring:"manual",
    opened_by:"operator",opened_at:at,status:"published",
    cannot_infer:"A criterion proposes a source, not approval or observation"});
  for(const [host,source]of [["a.edu","a-calendar"],["b.edu","b-calendar"],["c.edu","c-calendar"]])
    create("internet_source",{family:"calendar",source,label:host+" calendar",says:"Published schedules are unverified",
      url:"https://"+host+"/calendar/",state:"declared",status:"published"});
  const fr=new Database(frontier);
  fr.exec("CREATE TABLE root(root_id TEXT PRIMARY KEY,host TEXT,name TEXT,identity TEXT,kind TEXT,robots TEXT,economic TEXT,first_seen TEXT,last_seen TEXT);"+
    "CREATE TABLE channel(channel_id TEXT PRIMARY KEY,family_id TEXT,root_id TEXT,address TEXT,exact_address TEXT,protocol TEXT,state TEXT,truth_level TEXT,refusal TEXT);");
  const roots=[["a.edu","ra"],["b.edu","rb"],["c.edu","rc"]];
  for(const [host,id]of roots){
    fr.query("INSERT INTO root VALUES(?,?,?,?,?,?,?,?,?)").run(id,host,"Test "+host,"","","unknown","",at,at);
    fr.query("INSERT INTO channel VALUES(?,?,?,?,?,?,?,?,?)").run("c"+id,"family",id,host,
      "https://"+host+"/calendar.ics","ics","admitted","observed","");
  }
  fr.close();
  const archiveDb=new Database(archive);
  archiveDb.exec("CREATE TABLE item(source TEXT,key TEXT);");
  for(const source of ["a-calendar","b-calendar","c-calendar"])
    archiveDb.query("INSERT INTO item(source,key) VALUES(?,?)").run(source,"public-observation");
  archiveDb.close();
  const nativeEnv={BONFYRE_CMS_DB:cms,BONFYRE_SOURCE_FRONTIER_DB:frontier};
  const written=exec(projectBin,["discovery","run","real-set","--write"],nativeEnv);
  expect(JSON.parse(written).recorded).toBe(3);
  const opts={cms,set:"real-set",frontier,archive};
  return {dir,cms,frontier,archive,opts,create};
};
test("Native SourceFrontier discovery writes only proposed candidates; review never invents an approved source",()=>{
  const f=fixture();
  try{
    const snap=inspect(f.opts);
    expect(snap.candidates.length).toBe(3);
    expect(snap.pending.length).toBe(3);
    expect(snap.usable).toEqual([]);
    expect(compose(snap,{recipient:"Example investigator",domain:"cross-domain"}).markdown)
      .toContain("Do not compile a client fact brief yet");
  }finally{rmSync(f.dir,{recursive:true,force:true});}
},60000);
test("Native CMS decisions, source admission and ItemArchive control the actual accepted subset",()=>{
  const f=fixture();
  try{
    const begin=inspect(f.opts);
    const a=begin.candidates.find(x=>x.root_host==="a.edu"),
      b=begin.candidates.find(x=>x.root_host==="b.edu"),
      c=begin.candidates.find(x=>x.root_host==="c.edu");
    const args={cms:f.cms,set:"real-set",actor:"human-reviewer",
      cmsBinary:cmsBin,schemas,namespace:"root"};
    const approved=decide({...args,host:"a.edu",verdict:"accepted",
      reason:"Reviewer selected the source for further assessment",
      expectedUpdated:a.updated_at,expectedEvaluated:a.evaluated_at});
    expect(approved.readback).toBe(true);
    const rejected=decide({...args,host:"b.edu",verdict:"rejected",
      reason:"Rejected: this provider source is unsuitable for this review",
      expectedUpdated:b.updated_at,expectedEvaluated:b.evaluated_at});
    expect(rejected.verdict).toBe("rejected");
    const now=inspect(f.opts);
    expect(now.accepted.map(x=>x.root_host)).toEqual(["a.edu"]);
    expect(now.rejected.map(x=>x.root_host)).toEqual(["b.edu"]);
    expect(now.pending.map(x=>x.root_host)).toEqual(["c.edu"]);
    expect(now.usable.map(x=>x.source)).toEqual(["a-calendar"]);
    const docs=compose(now,{recipient:"Reviewer, not a publisher",domain:"property"});
    expect(docs.markdown).toContain("a-calendar");
    expect(docs.markdown).not.toContain("b-calendar");
    expect(docs.markdown).not.toContain("b.edu");
    expect(docs.markdown).not.toContain("c.edu");
    expect(docs.yaff).toContain("admitted_material \"a-calendar\"");
    expect(docs.yaff).toContain("rejected_count 1");
    expect(docs.yaff).not.toContain("b.edu");
    expect(docs.yaff).not.toContain('admitted_material "b-calendar"');
    expect(docs.yaff).not.toContain('admitted_material "c-calendar"');
    expect(()=>decide({...args,host:"a.edu",verdict:"rejected",
      reason:"Cannot silently overwrite a recorded human decision",
      expectedUpdated:a.updated_at,expectedEvaluated:a.evaluated_at})).toThrow(/pending/);
    expect(()=>decide({...args,host:"unknown.edu",verdict:"accepted",
      reason:"Not a real native candidate with a source owner",
      expectedUpdated:c.updated_at,expectedEvaluated:c.evaluated_at})).toThrow(/pending/);
    // A second native publisher run must not erase a person's accepted/rejected standing.
    exec(projectBin,["discovery","run","real-set","--write"],{
      BONFYRE_CMS_DB:f.cms,BONFYRE_SOURCE_FRONTIER_DB:f.frontier});
    expect(inspect(f.opts).accepted.map(x=>x.root_host)).toEqual(["a.edu"]);
    expect(inspect(f.opts).rejected.map(x=>x.root_host)).toEqual(["b.edu"]);
    // An accepted publisher without retained material must not appear in a material-based brief.
    const storage=new Database(f.archive);
    storage.exec("DELETE FROM item WHERE source='a-calendar'");
    storage.close();
    expect(inspect(f.opts).usable.length).toBe(0);
    expect(compose(inspect(f.opts),{recipient:"Researcher",domain:"itad"}).markdown)
      .toContain("material not yet admitted and retained");
  }finally{rmSync(f.dir,{recursive:true,force:true});}
},90000);
test("Stale evaluated source revisions are refused before native CMS writes; no browser-owned approval store",()=>{
  const f=fixture();
  try{
    const before=inspect(f.opts),c=before.candidates.find(x=>x.root_host==="c.edu");
    const db=new Database(f.cms);
    db.query("UPDATE discovery_candidate SET evaluated_at=? WHERE id=?").run("2099-03-15T12:00:00Z",c.id);
    db.close();
    expect(()=>decide({cms:f.cms,set:"real-set",host:c.root_host,
      actor:"human-reviewer",reason:"This is a legitimate review explanation",
      verdict:"accepted",cmsBinary:cmsBin,schemas,
      expectedUpdated:c.updated_at,expectedEvaluated:c.evaluated_at})).toThrow(/stale source generation/);
    expect(inspect(f.opts).pending.length).toBe(3);
    expect(()=>decide({cms:f.cms,set:"real-set",host:c.root_host,
      actor:"human-reviewer",reason:"A valid but unauthorized type of status",
      verdict:"settled",cmsBinary:cmsBin,schemas,
      expectedUpdated:c.updated_at,expectedEvaluated:c.evaluated_at})).toThrow(/only a person/);
  }finally{rmSync(f.dir,{recursive:true,force:true});}
},60000);
test("Human deliverables are Markdown and typed YAFF, not a fake serialized purchase",()=>{
  const f=fixture();
  try{
    const dest=join(f.dir,"handoff");
    const reply=run(["brief","--cms",f.cms,"--set","real-set","--frontier",f.frontier,
      "--archive",f.archive,"--recipient","Civic researcher","--domain","civic","--out",dest]);
    expect(reply.text).toContain("Reviewed source handoff");
    expect(existsSync(join(dest,"Source review.md"))).toBe(true);
    expect(existsSync(join(dest,"Source review.yaff"))).toBe(true);
    expect(readFileSync(join(dest,"Source review.yaff"),"utf8")).toContain("effect none");
    expect(()=>run(["brief","--cms",f.cms,"--set","real-set","--frontier",f.frontier,
      "--archive",f.archive,"--recipient","Civic researcher","--out",dest])).toThrow(/non-existing/);
  }finally{rmSync(f.dir,{recursive:true,force:true});}
},60000);
test("Native accepted sources open exact owner verification cases; rejected, stale and unadmitted refuse",()=>{
  const f=fixture();
  try{
    const before=inspect(f.opts),a=before.candidates.find(x=>x.root_host==="a.edu");
    const b=before.candidates.find(x=>x.root_host==="b.edu");
    const cmsBin=resolve(root,"cmd/BonfyreCMS/bonfyre-cms");
    const econBin=resolve(root,"cmd/BonfyreEconomy/bonfyre-economy");
    const economyDb=join(f.dir,"economy.db");
    const args={cms:f.cms,set:"real-set",actor:"reviewer",
      cmsBinary:cmsBin,schemas,namespace:"root"};
    decide({...args,host:"a.edu",verdict:"accepted",
      reason:"Accepted for independent source-specific verification",
      expectedUpdated:a.updated_at,expectedEvaluated:a.evaluated_at});
    decide({...args,host:"b.edu",verdict:"rejected",
      reason:"Publisher outside the intended source boundary",
      expectedUpdated:b.updated_at,expectedEvaluated:b.evaluated_at});
    const ready=inspect(f.opts),opts={domain:"civic",
      recipient:"Civic research reviewer",budgetCents:"30000",
      expectedGeneration:ready.census_generation,economyBin:econBin,economyDb};
    expect(ready.usable.map(x=>x.source)).toEqual(["a-calendar"]);
    expect(()=>openVerification(ready,{...opts,source:"b-calendar"}))
      .toThrow(/accepted, admitted and archived/);
    expect(()=>openVerification(ready,{...opts,source:"c-calendar"}))
      .toThrow(/accepted, admitted and archived/);
    expect(()=>openVerification(ready,{...opts,source:"a-calendar",expectedGeneration:before.census_generation}))
      .toThrow(/Stale accepted source generation/);
    expect(()=>openVerification(ready,{...opts,source:"a-calendar",domain:"property"}))
      .toThrow(/no native semantic binding/);
    const created=openVerification(ready,{...opts,source:"a-calendar"});
    expect(created.count).toBe(3);
    expect(created.ownerReadback).toBe(true);
    expect(created.markdown).toContain("Source-linked verification");
    expect(created.markdown).toContain("rights / CMS:internet_source:a-calendar");
    expect(created.markdown).not.toContain("b-calendar");
    const second=openVerification(ready,{...opts,source:"a-calendar"});
    expect(second.unchanged).toBe(true);
    expect(second.caseId).toBe(created.caseId);
    expect(()=>openVerification(ready,{...opts,budgetCents:"1",source:"a-calendar"}))
      .toThrow(/refused source requirements/);
    const odb=new Database(economyDb,{readonly:true});
    expect(odb.query("SELECT count(*) n FROM verification_cases").get().n).toBe(1);
    expect(odb.query("SELECT count(*) n FROM verification_needs").get().n).toBe(3);
    expect(odb.query("SELECT count(*) n FROM verification_offers").get().n).toBe(0);
    odb.close();
    // Changing the native source family changes the generation. A civic
    // approval never silently becomes a property proof.
    const changedSource=new Database(f.cms);
    changedSource.query("UPDATE internet_source SET family=? WHERE source=?").run("parcel","a-calendar");
    changedSource.close();
    const newCut=inspect(f.opts);
    expect(newCut.census_generation).not.toBe(ready.census_generation);
    expect(()=>openVerification(newCut,{...opts,source:"a-calendar"}))
      .toThrow(/Stale accepted source generation/);
    const property=openVerification(newCut,{...opts,source:"a-calendar",domain:"property",
      recipient:"Property investigator",expectedGeneration:newCut.census_generation});
    expect(property.count).toBe(4);
    expect(property.markdown).toContain("title / CMS:internet_source:a-calendar");
    expect(property.caseId).not.toBe(created.caseId);
    const archiveDb=new Database(f.archive);
    archiveDb.exec("DELETE FROM item WHERE source='a-calendar'");archiveDb.close();
    const drift=inspect(f.opts);
    expect(()=>openVerification(drift,{...opts,expectedGeneration:drift.census_generation,source:"a-calendar"}))
      .toThrow(/accepted, admitted and archived/);
  }finally{rmSync(f.dir,{recursive:true,force:true});}
},90000);
