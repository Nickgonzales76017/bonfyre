// Compile temporary CMS source through the real C owner, then exercise the
// same workspace composition used by the browser. No operator records are read.
import assert from 'node:assert/strict';
import {mkdtempSync,rmSync,mkdirSync} from 'node:fs';
import {join,resolve} from 'node:path';
import {tmpdir} from 'node:os';
import {fileURLToPath} from 'node:url';
import {execFileSync} from 'node:child_process';
import {SurfaceRuntime} from '../runtime/surface-runtime.mjs';
const root=resolve(fileURLToPath(new URL('../../..',import.meta.url)));
const home=mkdtempSync(join(tmpdir(),'bf-workspace-')),db=join(home,'cms.db');
const cms=join(root,'cmd/BonfyreCMS/bonfyre-cms'),schemas=join(root,'cmd/BonfyreCMS/content-types');
const env={...process.env,BONFYRE_CMS_BINARY:cms,BONFYRE_CMS_SCHEMAS:schemas,BONFYRE_SURFACE_SCENE_SCHEMA:join(schemas,'surface_scene.json'),BONFYRE_FACT_DELTAS:join(home,'deltas')};
mkdirSync(env.BONFYRE_FACT_DELTAS);
const run=(bin,args)=>{try{return execFileSync(bin,args,{cwd:root,env,encoding:'utf8',timeout:20000,maxBuffer:4*1024*1024,stdio:['pipe','pipe','pipe']});}catch(error){throw Error(bin+' '+args.slice(0,3).join(' ')+': '+String(error.stderr??error.message).slice(-2400));}};
const create=(type,value)=>run(cms,['entry','create',type,JSON.stringify(value),'--ns','root','--db',db,'--schemas',schemas]);
const compile=()=>JSON.parse(run(join(root,'cmd/BonfyreSurfaceScene/bonfyre-surface-scene'),['compile','--db',db,'--site','workspace-test','--actor-scope','public']));
try{
 run(cms,['schema','migrate','--db',db,'--schemas',schemas]);
 create('world',{world:'workspace-test',title:'Workspace test',character:'work',template:'ghost:night',lede:'Synthetic isolated composition',subject:'world:workspace-test',sources:'test-sources',grammars:'records,details',cannot_infer:'not a production authority',status:'published'});
 create('site_presentation',{site_id:'workspace-test',host:'workspace.example.test',subject_address:'world:workspace-test',profile:'ghost:night',territory:'work',navigation:['Records'],status:'published'});
 create('site_program',{program_id:'workspace-test:v1',site_id:'workspace-test',experience_programs:[],application_bindings:['test:records','test:detail'],renderer_preferences:{},source_collections:['test-sources'],capability_programs:[],actor_presentations:{public:'public'},status:'published'});
 create('semantic_slot',{slot_id:'test:primary',site_id:'workspace-test',slot_name:'primary',ordinal:1,extent:'page',accepts:['PublicationFeed','Community'],min_material_quality:'observed',max_items:1,status:'published'});
 create('application_binding',{binding_id:'test:records',site_id:'workspace-test',slot_id:'test:primary',application_kind:'PublicationFeed',material_query:{kind:'PublicationOccurrence'},source_collection:'test-sources',action_aperture:'review',continuity_key:'test-sources',status:'published'});
 create('application_binding',{binding_id:'test:detail',site_id:'workspace-test',slot_id:'test:primary',application_kind:'Community',material_query:{community:'test'},action_aperture:'',continuity_key:'test-sources',status:'published'});
 create('surface_interaction_profile',{profile_id:'test:interaction',site_id:'workspace-test',world:'workspace-test',default_lens:'Records',lens_order:['Records'],semantic_zoom:[],interaction_depths:['focus'],context_rail:{},relation_halo:{},command_aperture:{shortcut:'Meta+K'},transition_policy:{},mobile_posture:{},desktop_posture:{initial_layout:'work',layouts:[{id:'work',panels:[{id:'test:records'},{id:'test:detail',relative:'test:records',direction:'right'}]}]},live_update_policy:{mode:'SurfaceDelta'},status:'published'});
 const scene=compile();assert.equal(scene.schema,'bonfyre-surface-scene.v1');
 let dirty=false,allowed=true,calls=0;
 const contributions=new Map([
  ['test:records',{kind:'PublicationFeed',label:'Records',element:()=>null,dirty:()=>dirty,acceptSelection:id=>id==='item:one',commands:[{id:'review',label:'Review selected item',reads:['test:records','test:detail'],resultPolicy:'preview',enabled:()=>allowed,run:context=>{assert.equal(context.inputs['test:detail'].selected,context.selected);calls++;return context.selected;}}]}],
  ['test:detail',{kind:'Community',label:'Details',element:()=>null,acceptSelection:id=>id==='item:one'}]
 ]);
 const runtime=new SurfaceRuntime(scene,{root:null}),work=runtime.composeWorkspace(contributions);
 assert.equal(work.plan.panels.length,2);assert.equal(work.plan.revision,scene.source_digest);
 assert.equal(work.plan.focus.actor_scope,'public');
 work.select('test:records','item:one');assert.equal(work.context('test:detail').selected,'item:one');
 assert.equal(await work.execute('review'),'item:one');assert.equal(calls,1);
 allowed=false;await assert.rejects(work.execute('review'));assert.equal(calls,1);
 const rowId=run('sqlite3',[db,"SELECT id FROM world WHERE world='workspace-test' AND namespace='root';"]).trim();assert.match(rowId,/^\d+$/);
 run(cms,['entry','update','world',rowId,JSON.stringify({title:'Revised workspace source'}),'--ns','root','--db',db,'--schemas',schemas]);
 const successor=compile();assert.notEqual(successor.source_digest,scene.source_digest);
 const delta={schema:'bonfyre-surface-delta.v1',before_digest:scene.source_digest,after_digest:successor.source_digest,scene:successor};
 dirty=true;assert.equal(runtime.applyDelta(delta),false);assert.equal(runtime.model.sourceDigest,scene.source_digest);assert.equal(work.context('test:detail').selected,'item:one');
 dirty=false;assert.equal(runtime.applyDelta(delta),true);assert.equal(runtime.model.sourceDigest,successor.source_digest);assert.equal(work.plan.revision,successor.source_digest);assert.equal(work.context('test:detail').selected,null);
 const invalid=structuredClone(successor);invalid.bindings[0].application_kind='Unregistered';assert.throws(()=>runtime.updateScene(invalid));assert.equal(runtime.model.sourceDigest,successor.source_digest);
 runtime.disposeWorkspace();assert.equal(work.closed,true);await assert.rejects(work.execute('review'));assert.equal(calls,1);
 console.log('workspace_native_test: PASS real CMS -> C SurfaceScene -> runtime composition, selection, current command refusal, dirty correction retention, successor adoption and teardown');
}finally{rmSync(home,{recursive:true,force:true});}
