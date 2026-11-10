import assert from 'node:assert/strict';
import {spawnSync,spawn} from 'node:child_process';
import {copyFileSync} from 'node:fs';
import {join} from 'node:path';
import {packFixture,repo} from './pack_lifecycle_fixture.mjs';
import {encode,decode} from '../../../integrations/browser/workspace-msgpack.mjs';
const f=packFixture(),checks=[];
const check=(name,value)=>{assert.ok(value,name);checks.push(name);console.log('PASS '+name);};
const ok=r=>{assert.equal(r.code,0,r.raw+'\n'+r.stderr);return r;};
function declaration(id,entry='catalogue/'){
 f.pack(id);f.add('pack_demand',{pack:id,demand_kind:'workspace',needs:'spaces',preferred_sources:'a'.repeat(64),fields:entry,status:'published'});
 for(const fields of ['draft:catalogue:','fixture-plan:','product-model:'])f.add('pack_demand',{pack:id,demand_kind:'workspace-record',needs:'spaces',fields,status:'published'});
}
function change(op,id,state,context={}){return ok(f.call(op,id,{...context,revision:Number(state.revision),digest:state.digest||state.pinned_digest}));}
function install(id,context={}){const review=ok(f.call('plan',id,context)),installed=change('install',id,review,context);return change('activate',id,installed,context);}
const binding=installation=>f.sql.query('SELECT binding_id FROM _application_workspace WHERE installation_id=?').get(Number(installation.installation)).binding_id;
const args=(id,op,context={})=>['pack-workspace','--binding',id,'--operation',op,'--subject',context.subject??'user:101','--scope',context.scope??'self','--actor',context.actor??'user:101','--prefix',context.prefix??'','--cursor',context.cursor??'','--epoch',String(context.epoch??-1),'--db',f.db,'--schemas',f.schemas];
function call(id,op,body,context={}){const r=spawnSync(f.binary,args(id,op,context),{env:f.env,input:body?encode(body):undefined,timeout:15000,maxBuffer:1024*1024});return {code:r.status,stderr:r.stderr.toString(),packet:r.status===0?decode(r.stdout):null};}
function write(id,request,rows,context={}){return call(id,'write',[1,request,rows.map(([key,rev,value,mode=0])=>[key,rev,mode,mode===0?encode([2,value,[]]):null])],context);}
const row=(r,key)=>r.packet?.[4].find(x=>x[0]===key),value=(r,key)=>decode(row(r,key)[2])[1];
try{
 for(const type of ['application_binding','household'])copyFileSync(join(repo,'cmd/BonfyreCMS/content-types/'+type+'.json'),join(f.schemas,type+'.json'));
 const migration=spawnSync(f.binary,['schema','migrate','--db',f.db,'--schemas',f.schemas],{env:f.env,encoding:'utf8'});assert.equal(migration.status,0,migration.stderr);
 f.add('household',{household_id:'fixture-home',label:'Isolated native test household',steward:'user:101',opened_at:'2026-10-15',cannot_infer:'Isolated fixture, not a real household or permission grant.',status:'published'});
 declaration('catalogue');declaration('rooms','product-room/');
 const plan=ok(f.call('plan','catalogue')),installed=change('install','catalogue',plan),id=binding(installed);
 check('installation commits an actual backend binding beside its retained dependency pins',id.startsWith('ws-')&&f.sql.query('SELECT count(*) n FROM application_binding').get().n===1);
 check('installation alone does not open the draft owner',call(id,'read').code===4);
 const active=change('activate','catalogue',installed);
 check('activation opens an empty native destination',call(id,'read').packet[4].length===0);
 check('private backend bindings are not public site-program advertisements',f.sql.query("SELECT count(*) n FROM application_binding WHERE status='published'").get().n===0);
 const key='draft:catalogue:source:lamp',first={title:'Lamp 日本語',price:'69.99',options:[{id:'W002337865'}],x:-0};
 const saved=write(id,'save-one',[[key,0,first]]);check('a browser-compatible record survives a new native process',saved.code===0&&Object.is(value(call(id,'read'),key).x,-0));assert.deepEqual(value(saved,key),first);
 const second=write(id,'save-two',[[key,1,{...first,title:'Changed'}]]);check('native saves advance the existing record revision',row(second,key)[1]===2);
 const replay=write(id,'save-one',[[key,0,first]]);check('lost-response replay returns the original revision without undoing later edits',row(replay,key)[1]===1&&value(call(id,'read'),key).title==='Changed');
 check('reusing an operation identity for different bytes is refused',write(id,'save-one',[[key,2,{title:'Other'}]]).code===4);
 check('a stale member cannot overwrite a newer draft',write(id,'stale',[[key,1,{title:'Stale'}]]).code===4);
 check('one stale record refuses the entire multi-record edit',write(id,'batch-stale',[['fixture-plan:new',0,{id:'new'}],[key,1,{title:'Stale'}]]).code===4&&!row(call(id,'read'),'fixture-plan:new'));
 check('undeclared record families never enter the store',write(id,'wrong-family',[['training:member:primary',0,{history:[]}]]).code===3);
 check('duplicate keys are refused before a batch writes',write(id,'duplicate',[[key,2,first],[key,2,first]]).code===3);
 const rooms=install('rooms'),roomBinding=binding(rooms);
 const proposal={id:'preserved-plan',catalogue:'observed-source',items:[{key:'unchanged-fixture',product:'W002337865',x:1.25,y:2,rotation:0}]};
 check('the next installed product reads the same destination-owned output',write(id,'room-output',[['fixture-plan:project',0,proposal]]).code===0&&value(call(roomBinding,'read'),'fixture-plan:project').items[0].key==='unchanged-fixture');
 const otherContext={subject:'institution:other',scope:'institution'},other=install('catalogue',otherContext),otherId=binding(other);
 check('another organization starts with independent drafts',call(otherId,'read',null,otherContext).packet[4].length===0);
 check('a forged destination cannot read the first organization',call(id,'read',null,otherContext).code===4);
 check('personal access requires the actual personal subject',call(id,'read',null,{actor:'user:202'}).code!==0);
 const householdContext={subject:'household:fixture-home',scope:'household'},household=install('catalogue',householdContext);
 check('household bindings retain a distinct destination',call(binding(household),'read',null,householdContext).packet[4].length===0);
 const del=write(id,'delete',[[key,2,null,2]]);check('deletion retains a revision tombstone',row(del,key)[1]===3&&row(del,key)[2]===null&&!row(call(id,'read'),key));
 check('deletion cannot reset a stale create to revision zero',write(id,'stale-create',[[key,0,first]]).code===4);
 check('an explicitly reviewed restore can use the tombstone revision',write(id,'restore',[[key,3,first]]).code===0);
 const originalBodies=f.sql.query('SELECT count(*) n FROM _workspace_body').get().n;
 check('identical record bodies are retained once within their destination',write(id,'same-body',[['draft:catalogue:copy',0,first]]).code===0&&f.sql.query('SELECT count(*) n FROM _workspace_body').get().n===originalBodies);
 const many=Array.from({length:130},(_,n)=>['draft:catalogue:page:'+String(n).padStart(3,'0'),0,{title:'Page '+n}]);assert.equal(write(id,'many',many).code,0);
 const page=call(id,'read',null,{prefix:'draft:catalogue:page:'}),next=call(id,'read',null,{prefix:'draft:catalogue:page:',cursor:page.packet[3],epoch:page.packet[1]});
 check('bounded pagination accounts for every row',page.packet[2]&&page.packet[4].length===128&&next.packet[4].length===2&&!next.packet[2]);
 write(id,'advance',[['draft:catalogue:page:next',0,{title:'Next'}]]);
 check('a changed collection cannot masquerade as a consistent paged read',call(id,'read',null,{prefix:'draft:catalogue:page:',cursor:page.packet[3],epoch:page.packet[1]}).code===4);
 const racer=request=>new Promise((resolve,reject)=>{const p=spawn(f.binary,args(id,'write'),{env:f.env,stdio:['pipe','pipe','pipe']});p.on('error',reject);p.stdout.resume();p.stderr.resume();p.on('close',resolve);p.stdin.end(encode([1,request,[['draft:catalogue:race',0,0,encode([2,{title:request},[]])]]]));});
 const race=await Promise.all([racer('race-one'),racer('race-two')]);check('concurrent native writers commit only one expected revision',race.filter(x=>x===0).length===1&&race.filter(x=>x===4).length===1);
 const bodyCount=f.sql.query('SELECT count(*) n FROM _workspace_body').get().n;
 f.sql.exec("CREATE TRIGGER stop_draft BEFORE INSERT ON _workspace_record WHEN NEW.record_key='draft:catalogue:fail' BEGIN SELECT RAISE(ABORT,'test failure'); END");
 check('a mid-batch storage failure rolls back bodies, pointers and replay receipts',write(id,'failed-save',[['draft:catalogue:early',0,{title:'Early'}],['draft:catalogue:fail',0,{title:'Fail'}]]).code!==0&&f.sql.query('SELECT count(*) n FROM _workspace_body').get().n===bodyCount&&!row(call(id,'read'),'draft:catalogue:early'));
 f.sql.exec('DROP TRIGGER stop_draft');check('retry after failed commit does not inherit partial work',write(id,'failed-save',[['draft:catalogue:early',0,{title:'Early'}],['draft:catalogue:fail',0,{title:'Fail'}]]).code===0);
 const before=f.sql.query('SELECT release FROM _application_workspace WHERE binding_id=?').get(id).release;
 f.sql.run("UPDATE _application_workspace SET release=? WHERE binding_id=?",'b'.repeat(64),id);check('a substituted release cannot borrow the installation authority',call(id,'read').code===4);f.sql.run('UPDATE _application_workspace SET release=? WHERE binding_id=?',before,id);
 change('deactivate','catalogue',active);check('deactivation blocks new reads and writes without deleting drafts',call(id,'read').code===4&&value(call(roomBinding,'get',null,{prefix:'fixture-plan:project'}),'fixture-plan:project').id==='preserved-plan');
 const current=ok(f.call('current','catalogue'));change('activate','catalogue',current);check('reactivation reopens the preserved draft',value(call(id,'get',null,{prefix:key}),key).title===first.title);
 f.add('pack_demand',{pack:'catalogue',demand_kind:'source',needs:'Changed fixture source',status:'published'});check('source changes revoke old draft access until a reviewed upgrade',call(id,'read').code===4);
 declaration('rollback');const rollback=ok(f.call('plan','rollback')),count=f.sql.query('SELECT count(*) n FROM pack_installation').get().n;
 f.sql.exec("CREATE TRIGGER stop_binding BEFORE INSERT ON application_binding WHEN NEW.slot_id='rollback' BEGIN SELECT RAISE(ABORT,'test binding failure'); END");
 const failure=f.call('install','rollback',{revision:Number(rollback.revision),digest:rollback.digest});check('binding failure rolls back installation and pins together',failure.code!==0&&f.sql.query('SELECT count(*) n FROM pack_installation').get().n===count);
 console.log('WORKSPACE_NATIVE_PASS\t'+checks.length);
} finally { f.close(); }
