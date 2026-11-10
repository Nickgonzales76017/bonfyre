import { test, expect } from "bun:test";
import { Database } from "bun:sqlite";
import { existsSync, mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { run as compileOwner, policy } from "../compile-program-fabric.mjs";
import { renderProgramServicePolicy } from "../project-program-service-policy.mjs";

const repo = resolve(dirname(fileURLToPath(import.meta.url)), "../..");
// A real empty owner index, not the developer's live estate or a fabricated
// approximation of the AddressPlane schema, accompanies each test estate.
function run(args) {
  const addressIndex=join(args.estate,'address.db');
  if(args.verb==='compile'&&!existsSync(addressIndex)){
    const input=join(args.estate,'empty-advertisements.jsonl');writeFileSync(input,'');
    const made=Bun.spawnSync([join(repo,'cmd/BonfyreExternalEstate/bonfyre-external-estate'),
      'import-advertisements','--db',addressIndex,'--advertisements',input]);
    expect(made.exitCode).toBe(0);
  }
  return compileOwner({...args,addressIndex:args.addressIndex??addressIndex});
}
const types = ["program_template", "program_instance", "semantic_slot", "application_binding", "program_binding", "source_collection", "program_requirement", "funding_work_package"];
const keys = { program_template: "template_id", program_instance: "instance_id", semantic_slot: "slot_id", application_binding: "binding_id",
  program_binding: "binding_id", source_collection: "collection_id", program_requirement: "requirement_id", funding_work_package: "package_id" };

test("ProgramFabric service continuations are generated from the same CUE policy as lowerings",()=>{
  const generated=renderProgramServicePolicy(policy);
  expect(readFileSync(join(repo,"integrations/browser/program-service-policy.mjs"),"utf8")).toBe(generated);
  for(const role of Object.keys(policy.lowerings))expect(policy.continuations[role]?.length).toBeGreaterThan(0);
  const workspacePacks=new Set(Object.values(policy.service_packs));
  for(const specs of Object.values(policy.continuations))for(const spec of specs)if(spec.kind==="workspace")expect(workspacePacks.has(spec.pack)).toBe(true);
});

function cmsFixture(estate) {
  const db = new Database(join(estate, "cms.db"));
  for (const type of types) {
    const doc = JSON.parse(readFileSync(join(repo, "cmd/BonfyreCMS/content-types", `${type}.json`), "utf8"));
    const columns = new Set(["namespace", "created_at", "updated_at", "published_at", "status", ...Object.keys(doc.attributes)]);
    db.exec(`CREATE TABLE "${type}"(id INTEGER PRIMARY KEY, ${[...columns].map(name => `"${name}" ${doc.attributes[name]?.type === "integer" ? "INTEGER" : "TEXT"}`).join(", ")}, UNIQUE(namespace,"${keys[type]}"))`);
  }
  return db;
}

test("templates compile into real owner bindings, open obligations and bounded funding scopes", () => {
  const base = mkdtempSync(join(tmpdir(), "bonfyre-program-fabric-"));
  const estate = join(base, "estate");
  mkdirSync(estate);
  const packs = join(base, "packs.json");
  writeFileSync(packs, JSON.stringify({ packs: [{ program_id: "school-learning", name: "School Learning", archetype: "education",
    semantic_slots: ["notices", "mentoring"], capability_families: ["source_intake"], grant_stack: ["Education stream"] }] }));
  try {
    const db = cmsFixture(estate);
    db.query("INSERT INTO semantic_slot(namespace,slot_id,site_id,ordinal,status) VALUES('root','existing','school-site',10,'published')").run();
    db.close();
    expect(run({ verb: "load", estate, packs }).templates).toBe(1);
    const result = run({ verb: "compile", estate, subject: "institution:school:one", siteId: "school-site", programs: ["school-learning", "absent"] });
    expect(result.compiled).toHaveLength(1);
    expect(result.no_such_template).toEqual(["absent"]);
    expect(result.compiled[0]).toMatchObject({ slots: 2, lowered: 1, declared_unlowered: 1, requirements: 2, work_packages: 1 });
    const cms = new Database(join(estate, "cms.db"), { readonly: true });
    expect(cms.query("SELECT slot_name,ordinal FROM semantic_slot WHERE site_id='school-site' ORDER BY ordinal").all()).toEqual([
      { slot_name: null, ordinal: 10 }, { slot_name: "notices", ordinal: 11 }, { slot_name: "mentoring", ordinal: 12 }]);
    expect(cms.query("SELECT COUNT(*) AS n FROM application_binding").get().n).toBe(1);
    expect(cms.query("SELECT program_role,lowered_by FROM program_binding ORDER BY slot_id").all()).toEqual([
      { program_role: "mentoring", lowered_by: "" }, { program_role: "notices", lowered_by: "Newsroom" }]);
    expect(cms.query("SELECT COUNT(*) AS n FROM program_requirement WHERE state='open'").get().n).toBe(2);
    expect(cms.query("SELECT funding_program,scope,cost_codes FROM funding_work_package").get()).toEqual({ funding_program: "Education stream", scope: "[]", cost_codes: "[]" });
    expect(JSON.parse(cms.query("SELECT source_selector FROM source_collection").get().source_selector)).toMatchObject({ source_refs: [], resolver: "SourceFrontier" });
    cms.close();
    const stage = join(base, "staged");
    const installed = Bun.spawnSync(["make", "-C", join(repo, "scripts"), "install-program-fabric", `MEMBRANE_ROOT=${stage}`]);
    expect(installed.exitCode).toBe(0);
    expect(existsSync(join(stage, "scripts/program-fabric.cue"))).toBe(true);
    const staged = Bun.spawnSync(["bun", join(stage, "scripts/compile-program-fabric.mjs"), "compile", "--estate", estate,
      "--subject", "institution:school:one", "--site-id", "school-site", "--program", "absent"]);
    expect(staged.exitCode).toBe(0);
    expect(JSON.parse(staged.stdout.toString()).no_such_template).toEqual(["absent"]);
  } finally { rmSync(base, { recursive: true, force: true }); }
});

test("a missing CMS type rolls back the entire program instance", () => {
  const base = mkdtempSync(join(tmpdir(), "bonfyre-program-rollback-"));
  const estate = join(base, "estate");
  mkdirSync(estate);
  const packs = join(base, "packs.json");
  writeFileSync(packs, JSON.stringify([{ program_id: "one", semantic_slots: ["board"] }]));
  try {
    const db = cmsFixture(estate);
    db.close();
    run({ verb: "load", estate, packs });
    const changed = new Database(join(estate, "cms.db"));
    changed.exec("DROP TABLE program_binding");
    changed.close();
    expect(() => run({ verb: "compile", estate, subject: "person:one", siteId: "one", programs: ["one"] })).toThrow();
    const check = new Database(join(estate, "cms.db"), { readonly: true });
    expect(check.query("SELECT COUNT(*) AS n FROM program_instance").get().n).toBe(0);
    check.close();
  } finally { rmSync(base, { recursive: true, force: true }); }
});

test("colliding slot labels cannot silently overwrite an authored program role", () => {
  const base = mkdtempSync(join(tmpdir(), "bonfyre-program-collision-"));
  const estate = join(base, "estate");
  mkdirSync(estate);
  const packs = join(base, "packs.json");
  writeFileSync(packs, JSON.stringify([{ program_id: "one", semantic_slots: ["case work", "case-work"] }]));
  try {
    cmsFixture(estate).close();
    run({ verb: "load", estate, packs });
    expect(() => run({ verb: "compile", estate, subject: "person:one", siteId: "one", programs: ["one"] })).toThrow("colliding");
    const db = new Database(join(estate, "cms.db"), { readonly: true });
    expect(db.query("SELECT COUNT(*) AS n FROM program_instance").get().n).toBe(0);
    db.close();
  } finally { rmSync(base, { recursive: true, force: true }); }
});

function continuityFixture(check) {
  const estate = mkdtempSync(join(tmpdir(), "bf-program-continuity-case-"));
  const packs = join(estate, "templates.json");
  writeFileSync(packs, JSON.stringify([{program_id:"repeat",name:"Repeat",archetype:"shared",semantic_slots:["sources","work"],capability_families:["source_intake"],grant_stack:["First fund"]}]));
  try {
    cmsFixture(estate).close(); run({verb:"load",estate,packs});
    const args={verb:"compile",estate,subject:"institution:one",siteId:"one",programs:["repeat"]};
    run(args); const db=new Database(join(estate,"cms.db"));
    try { check(db,args,packs); } finally {db.close();}
  } finally {rmSync(estate,{recursive:true,force:true});}
}
test("recompiling keeps authored frontend, admitted sources and completed owner state",()=>continuityFixture((db,args)=>{
  db.exec("UPDATE semantic_slot SET ordinal=41,extent='custom',max_items=23,htmx_boundary='author-boundary' WHERE slot_name='sources'; UPDATE program_requirement SET state='met'; UPDATE program_instance SET opened_at='original',published_at='approved',status='draft';");
  db.query('UPDATE application_binding SET material_query=?,source_collection=?').run(JSON.stringify({author:true}),'curated');
  db.query('UPDATE funding_work_package SET scope=?,cost_codes=?').run(JSON.stringify(['grant-scope']),JSON.stringify(['labor']));
  const sel=JSON.parse(db.query('SELECT source_selector FROM source_collection').get().source_selector);sel.source_refs=['admitted:publisher'];sel.evidence='qualification';
  db.query('UPDATE source_collection SET source_selector=?').run(JSON.stringify(sel));
  run(args);
  expect(db.query("SELECT ordinal,extent,max_items,htmx_boundary FROM semantic_slot WHERE slot_name='sources'").get()).toEqual({ordinal:41,extent:'custom',max_items:23,htmx_boundary:'author-boundary'});
  expect(db.query('SELECT material_query,source_collection FROM application_binding LIMIT 1').get()).toEqual({material_query:JSON.stringify({author:true}),source_collection:'curated'});
  expect(db.query("SELECT count(*) n FROM program_requirement WHERE state='met'").get().n).toBe(2);
  expect(db.query('SELECT scope,cost_codes FROM funding_work_package').get()).toEqual({scope:JSON.stringify(['grant-scope']),cost_codes:JSON.stringify(['labor'])});
  expect(db.query('SELECT opened_at,published_at,status FROM program_instance').get()).toEqual({opened_at:'original',published_at:'approved',status:'draft'});
  expect(JSON.parse(db.query('SELECT source_selector FROM source_collection').get().source_selector)).toMatchObject({source_refs:['admitted:publisher'],evidence:'qualification'});
}));
test("unchanged recompilation performs no logical row updates",()=>continuityFixture((db,args)=>{
  for(const type of types)db.exec('UPDATE '+type+" SET updated_at='unchanged-marker'");
  run(args);
  for(const type of types)expect(db.query('SELECT count(*) n FROM '+type+" WHERE updated_at<>'unchanged-marker'").get().n).toBe(0);
}));
test("new slots append without reordering an existing frontend",()=>continuityFixture((db,args)=>{
  db.exec("UPDATE semantic_slot SET ordinal=70 WHERE slot_name='work'");
  db.query('UPDATE program_template SET semantic_slots=?').run(JSON.stringify(['work','sources','community']));run(args);
  expect(db.query('SELECT slot_name,ordinal FROM semantic_slot ORDER BY ordinal').all()).toEqual([{slot_name:'sources',ordinal:1},{slot_name:'work',ordinal:70},{slot_name:'community',ordinal:71}]);
}));
test("another subject cannot take an existing programme identity",()=>continuityFixture((db,args)=>{
  expect(()=>run({...args,subject:'institution:other'})).toThrow('identity collision');
  expect(db.query('SELECT owner_subject FROM program_instance').get().owner_subject).toBe(args.subject);
}));
test("changed funder cannot overwrite scoped funding work",()=>continuityFixture((db,args)=>{
  db.query('UPDATE program_template SET funding_candidates=?').run(JSON.stringify(['Different fund']));
  expect(()=>run(args)).toThrow('funding slot changed');expect(db.query('SELECT funding_program FROM funding_work_package').get().funding_program).toBe('First fund');
}));
test("colliding capability keys refuse before a partial programme exists",()=>continuityFixture((db,args)=>{
  db.query('UPDATE program_template SET capability_families=?').run(JSON.stringify(['case work','case-work']));
  expect(()=>run(args)).toThrow('colliding');expect(db.query('SELECT count(*) n FROM program_requirement').get().n).toBe(2);
}));
