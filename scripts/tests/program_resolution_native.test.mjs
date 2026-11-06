import {test,expect} from 'bun:test';
import {Database} from 'bun:sqlite';
import {mkdtempSync,mkdirSync,copyFileSync,readFileSync,writeFileSync,rmSync,existsSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,resolve,dirname} from 'node:path';
import {fileURLToPath} from 'node:url';
import {spawnSync} from 'node:child_process';
import {createHash} from 'node:crypto';
import {run as compile} from '../compile-program-fabric.mjs';
import {resolveProgram} from '../resolve_program_sources.mjs';
import {resolveDemands,nativeAddressRun} from '../../cmd/BonfyreExternalEstate/tools/resolve_program_demand.mjs';
const repo=resolve(dirname(fileURLToPath(import.meta.url)),'../..');
const external=join(repo,'cmd/BonfyreExternalEstate/bonfyre-external-estate');
const cmsBinary=join(repo,'cmd/BonfyreCMS/bonfyre-cms');
const types=['program_template','program_instance','semantic_slot','application_binding','program_binding','source_collection','program_requirement','funding_work_package','internet_source'];
const sha=path=>createHash('sha256').update(readFileSync(path)).digest('hex');
function command(binary,args){const r=spawnSync(binary,args,{cwd:repo,encoding:'utf8',timeout:30000,maxBuffer:4*1024*1024});expect(r.status,r.stderr).toBe(0);return r.stdout;}
function advertisement(identity,kind,owner,terms,dependencies=[]){return {identity,candidate_kind:kind,owner_ref:owner,
 exact_verifier:kind==='external_source'?'CMS:publication':'fixture:exact-verifier',lowerer_ref:'MaterialCompiler:source',validity_frontier:'fixture-revision',terms,dependencies};}
const demands=[{id:'composition',kinds:['external_source','pack','projection'],need:['produces:nutrient_composition'],discovery_queries:['explicit residual query only']},
 {id:'creator-material',kinds:['creator_channel'],need:['domain:food']}];
function fixture(body){
 const base=mkdtempSync(join(tmpdir(),'bf-program-native-resolution-')),cmsPath=join(base,'cms.db'),index=join(base,'address.db'),schemas=join(base,'schemas');mkdirSync(schemas);let db;
 try{
  for(const type of types)copyFileSync(join(repo,'cmd/BonfyreCMS/content-types',type+'.json'),join(schemas,type+'.json'));
  command(cmsBinary,['schema','migrate','--db',cmsPath,'--schemas',schemas]);
  db=new Database(cmsPath);
  for(const [namespace,source,label]of [['root','food-source','Composition source'],['another-account','private-source','Private source']])
   command(cmsBinary,['entry','create','internet_source',JSON.stringify({source,family:'food',label,says:'Fixture',url:'https://fixture.example/'+(source==='food-source'?'food':'private'),twin:source,state:'observed',status:'published'}),
    '--ns',namespace,'--db',cmsPath,'--schemas',schemas]);
  const ads=[advertisement('cms:internet_source:food-source','external_source','CMS:internet_source:food-source',['produces:nutrient_composition'],['cms:internet_source:food-source','ForeignTwin:https://fixture.example/food']),
   advertisement('cms:internet_source:private-source','external_source','CMS:internet_source:private-source',['produces:nutrient_composition']),
   advertisement('fixture:food-pack','pack','FixturePack:food',['produces:nutrient_composition'],Array.from({length:80},(_,i)=>'food:revision:'+i)),
   advertisement('fixture:food-projection','projection','FixtureProjection:food',['produces:nutrient_composition']),
   advertisement('fixture:food-creator','creator_channel','FixtureCreator:food',['domain:food'],['creator:fixture']),
   advertisement('fixture:unrelated','provider','FixtureProvider:unrelated',['domain:computers'],['unrelated:revision'])];
  const adsPath=join(base,'advertisements.jsonl');writeFileSync(adsPath,ads.map(a=>JSON.stringify(a)).join('\n'));
  command(external,['import-advertisements','--db',index,'--advertisements',adsPath]);
  const packs=join(base,'programmes.json');writeFileSync(packs,JSON.stringify([{program_id:'shared-food',name:'Shared food programme',archetype:'not-in-any-hardcoded-map',semantic_slots:['sources','work'],capability_families:['source_intake'],address_demands:demands}]));
  compile({verb:'load',estate:base,packs});
  const args={verb:'compile',estate:base,subject:'household:fixture',siteId:'fixture-home',programs:['shared-food'],addressIndex:index};
  body({base,db,index,cmsPath,args,ads,adsPath,packs});
 }finally{db?.close();rmSync(base,{recursive:true,force:true})}
}
function sources(db){return JSON.parse(db.query('SELECT source_selector FROM source_collection').get().source_selector);}
test('ordinary programme compilation resolves existing owners without provider names or a new search',()=>fixture(({db,args,index})=>{
 const before=sha(index),result=compile(args),local=result.compiled[0].local_resolution,s=sources(db);
 expect(local.owner).toBe('AddressPlane');expect(local.owner_queries).toBe(4);expect(local.requirements_satisfied).toBe(0);
 expect(s.source_refs).toEqual(['food-source']);expect(s.resolved_source_refs).toEqual(['food-source']);
 expect(s.candidate_refs).toContain('fixture:food-pack');expect(s.candidate_refs).toContain('fixture:food-creator');expect(s.candidate_refs).not.toContain('fixture:unrelated');
 expect(s.template_ref).toBe('shared-food');expect(s.address_demands).toBeUndefined();
 expect(s.resolved_dependency_refs).toContain('food:revision:79');expect(s.resolved_dependency_refs).toContain('ForeignTwin:https://fixture.example/food');
 expect(db.query("SELECT count(*) n FROM program_requirement WHERE state='met'").get().n).toBe(0);expect(sha(index)).toBe(before);
}));
test('initial compile and refresh share membership semantics, preserve authored material, and respect withdrawal',()=>fixture(({base,db,args,index})=>{
 const first=compile(args).compiled[0],instance=first.instance;
 let s=sources(db);s.source_refs.push('author-kept');db.query('UPDATE source_collection SET source_selector=?').run(JSON.stringify(s));
 db.exec("UPDATE program_requirement SET state='met',satisfied_by='accepted-owner-evidence'");
 const call={estate:base,repo,instance,namespace:'root',addressIndex:index,needs:'',apply:true,discover:false};
 const guarded=argv=>{expect(argv[0]).toBe(external);return nativeAddressRun(argv)};
 resolveProgram(call,{runFn:guarded});expect(sources(db).source_refs).toEqual(['author-kept','food-source']);
 db.exec("UPDATE source_collection SET updated_at='unchanged-marker'");resolveProgram(call,{runFn:guarded});expect(db.query('SELECT updated_at FROM source_collection').get().updated_at).toBe('unchanged-marker');
 db.exec("UPDATE internet_source SET status='draft' WHERE source='food-source'");resolveProgram(call,{runFn:guarded});expect(sources(db).source_refs).toEqual(['author-kept']);
 expect(db.query('SELECT state,satisfied_by FROM program_requirement').get()).toEqual({state:'met',satisfied_by:'accepted-owner-evidence'});
 expect(resolveProgram({...call,namespace:'another-account'}).exitCode).toBe(2);
}));
test('resolved source dependencies reach native FactDelta handling, including references beyond the former 32-slot ceiling',()=>fixture(({base,db,args,index,cmsPath})=>{
 const instance=compile(args).compiled[0].instance,identity='cms:source_collection:'+instance+'-sources';
 const records=command(external,['emit-cms','--cms-db',cmsPath]).trim().split('\n').map(JSON.parse);
 const record=records.find(r=>r.identity===identity);expect(record.dependencies).toContain('food:revision:79');expect(record.dependencies).toContain('cms:program_template:shared-food');
 expect(record.dependencies.length).toBeGreaterThan(80);
 const path=join(base,'collection.jsonl');writeFileSync(path,JSON.stringify(record)+'\n');command(external,['import-advertisements','--db',index,'--advertisements',path]);
 const affected=JSON.parse(command(external,['affected','--db',index,'--write','food:revision:79']));expect(affected.affected.map(a=>a.identity)).toContain(identity);expect(affected.affected.map(a=>a.identity)).not.toContain('fixture:unrelated');
 const delta=join(base,'delta.json');writeFileSync(delta,JSON.stringify({delta_id:'fixture:food-revision-change',writes:['food:revision:79']}));
 const result=JSON.parse(command(external,['fact-delta','--db',index,'--delta',delta]));expect(result.lowering_state).toBe('queued');
 const owner=new Database(index,{readonly:true});try{expect(owner.query('SELECT lowerer_ref,state FROM candidate_lowering_queue WHERE identity=?').get(identity)).toEqual({lowerer_ref:'MaterialCompiler:source_collection',state:'queued'});}finally{owner.close()}
 const s=sources(db);s.resolved_dependency_refs=['valid',true];db.query('UPDATE source_collection SET source_selector=?').run(JSON.stringify(s));
 expect(spawnSync(external,['emit-cms','--cms-db',cmsPath],{encoding:'utf8'}).status).not.toBe(0);
}));
test('query reuse, kinds, prohibitions, unknown coordinates and truncation remain explicit',()=>fixture(({base,index})=>{
 let calls=0;const result=resolveDemands([demands[0],{...demands[0],id:'same-cut'}],{binary:external,index,runFn:argv=>{calls++;return nativeAddressRun(argv)}});
 expect(calls).toBe(3);expect(result.resolutions[0].candidates).toHaveLength(4);
 const no=resolveDemands([{id:'unknown',kinds:['pack'],need:['produces:not_declared'],discovery_queries:['residual only']}],{binary:external,index});
 expect(no.resolutions[0].candidates).toEqual([]);expect(no.resolutions[0].discovery_queries).toEqual(['residual only']);
 const forbidden=JSON.parse(command(external,['forward','--db',index,'--kind','pack','--need','produces:nutrient_composition','--forbid','produces:nutrient_composition']));expect(forbidden.returned).toBe(0);
 const limited=JSON.parse(command(external,['forward','--db',index,'--kind','external_source','--need','produces:nutrient_composition','--limit','1']));expect(limited.eligible).toBe(2);expect(limited.returned).toBe(1);expect(limited.has_more).toBe(true);
 expect(()=>resolveDemands([demands[0]],{binary:external,index,runFn:()=>({exitCode:0,stdout:JSON.stringify({...limited,candidate_kind:'external_source'}),stderr:''})})).toThrow('bound');
 expect(()=>resolveDemands([demands[0]],{binary:external,index:join(base,'absent.db')})).toThrow('no fallback discovery');expect(existsSync(join(base,'absent.db'))).toBe(false);
}));
test('failed local binding rolls back compilation instead of creating a partially connected application',()=>fixture(({db,args,base})=>{
 expect(()=>compile({...args,addressIndex:join(base,'missing-index.db')})).toThrow();
 expect(db.query('SELECT count(*) n FROM program_instance').get().n).toBe(0);expect(db.query('SELECT count(*) n FROM source_collection').get().n).toBe(0);
}));

test('legacy schema and declared families remain usable; new declarations cannot be silently dropped',()=>fixture(({db,args,base,packs})=>{
 db.exec('ALTER TABLE program_template DROP COLUMN address_demands');
 writeFileSync(packs,JSON.stringify([{program_id:'legacy',name:'Legacy programme',archetype:'existing',semantic_slots:['sources'],capability_families:['source_intake']}]));
 expect(compile({verb:'load',estate:base,packs}).templates).toBe(1);
 const result=compile({...args,programs:['legacy']});
 expect(result.compiled[0].local_resolution.owner).toBe('AddressPlane');
 expect(result.compiled[0].local_resolution.resolutions.length).toBeGreaterThan(0);
 expect(sources(db).resolution_state).toBe('no_indexed_candidate');
 writeFileSync(packs,JSON.stringify([{program_id:'new',address_demands:demands}]));
 expect(()=>compile({verb:'load',estate:base,packs})).toThrow('cannot preserve declared fields');
 expect(db.query("SELECT count(*) n FROM program_template WHERE template_id='new'").get().n).toBe(0);
}));

test('native delta outbox drives the existing programme resolver and updates the same collection',()=>fixture(({base,db,args,index,cmsPath})=>{
 const instance=compile(args).compiled[0].instance,identity='cms:source_collection:'+instance+'-sources';
 const initial=sources(db);initial.source_refs.push('author-kept');db.query('UPDATE source_collection SET source_selector=?').run(JSON.stringify(initial));
 const record=command(external,['emit-cms','--cms-db',cmsPath]).trim().split('\n').map(JSON.parse).find(r=>r.identity===identity);
 const ad=join(base,'derived-collection.jsonl');writeFileSync(ad,JSON.stringify(record)+'\n');command(external,['import-advertisements','--db',index,'--advertisements',ad]);
 db.query('UPDATE program_template SET address_demands=? WHERE template_id=?').run('[]','shared-food');
 db.exec("UPDATE program_requirement SET state='met',satisfied_by='accepted-evidence'");
 const delta=join(base,'changed-template.json'),inbox=join(base,'lowerer-inbox'),artifacts=join(base,'artifacts.db');
 writeFileSync(delta,JSON.stringify({delta_id:'fixture:template-revision',writes:['cms:program_template:shared-food']}));
 expect(JSON.parse(command(external,['fact-delta','--db',index,'--delta',delta])).affected_count).toBe(1);
 command(external,['drain-lowering-queue','--db',index,'--dir',inbox]);
 const worker=join(repo,'cmd/BonfyreLoweringWorkers/bonfyre-lowering-workers');
 const argv=['drain','--db',artifacts,'--lowerer-dir',inbox,'--cms',cmsPath,'--external-estate',index];
 const absent=spawnSync(worker,argv,{encoding:'utf8',env:{...process.env,BONFYRE_PROGRAM_RESOLVER:'',BONFYRE_BUN_BINARY:''}});
 expect(absent.status).toBe(1);expect(JSON.parse(absent.stdout).handled).toBe(0);expect(sources(db).source_refs).toContain('food-source');
 const answer=JSON.parse(command(worker,[...argv,'--program-resolver',join(repo,'scripts/resolve_program_sources.mjs'),'--program-runtime',process.execPath]));
 expect(answer.handled).toBe(1);expect(answer.failed_or_deferred).toBe(0);expect(sources(db).source_refs).toEqual(['author-kept']);
 expect(db.query('SELECT state,satisfied_by FROM program_requirement').get()).toEqual({state:'met',satisfied_by:'accepted-evidence'});
 const repeated=JSON.parse(command(worker,[...argv,'--program-resolver',join(repo,'scripts/resolve_program_sources.mjs'),'--program-runtime',process.execPath]));
 expect(repeated.seen).toBe(0);
}));
