// Upgrade one existing private editor through Pack lifecycle. No record rewrites,
// new account grants, service restart, operator admission or external effects.
import assert from 'node:assert/strict';
import {readFileSync,writeFileSync,mkdirSync,existsSync,statfsSync} from 'node:fs';
import {join,resolve} from 'node:path';
import {homedir} from 'node:os';
import {createHash} from 'node:crypto';
import {Database} from 'bun:sqlite';
import {materializeReferenceRepair} from '../materialize_reference_repair.mjs';
import {verifyWorkspacePackage} from '../workspace_material.mjs';
import {projectConnectedInstrumentsSetup} from '../publish_experience_setups.mjs';
import {encode} from '../../integrations/browser/workspace-msgpack.mjs';
const root=resolve(import.meta.dir,'../..'),home=homedir(),hash=b=>createHash('sha256').update(b).digest('hex');
const target=Object.freeze({subject:'user:19',scope:'self',binding:'ws-bd08c6776460a11c6d1e970b01f53cc15b65a37a',pack:'experience.connected-instruments.workspace',base:'c70d97e7004b69149941f1805f16d7f556792110b51977befc39bd663983e654'});
const sourcePaths=['integrations/browser/retained-material.mjs','integrations/browser/retained-material-view.mjs','integrations/browser/retained-material.css','scripts/materialize_reference_repair.mjs','scripts/publish_experience_setups.mjs','scripts/tests/retained-material.test.mjs','scripts/tests/retained-material-fixture.mjs','scripts/tests/reference-repair-browser.test.mjs'];
function run(args,{env=process.env,timeout=30000}={}){const p=Bun.spawnSync(args,{cwd:root,env,stdout:'pipe',stderr:'pipe',timeout});assert.equal(p.exitCode,0,p.stderr.toString().slice(-5000));return p.stdout.toString();}
export async function rolloutReferences({apply=false,expectedRelease}={}){
 assert(typeof apply==='boolean');const packages=join(home,'.bonfyre/workspace-packages'),servicePath=join(home,'Library/LaunchAgents/com.bonfyre.public-web-address.plist'),serviceBytes=readFileSync(servicePath),config=JSON.parse(run(['/usr/bin/plutil','-convert','json','-o','-',servicePath]));
 const env={...process.env,...config.EnvironmentVariables},dbPath=config.EnvironmentVariables.BONFYRE_CMS_DB,binary=config.EnvironmentVariables.BONFYRE_CMS_BINARY;
 assert(dbPath?.startsWith('/')&&binary?.startsWith('/'),'Use the deployed CMS owner.');
 const schemas=join(home,'.bonfyre/workflow-recovery/20261022-shipments/schemas');for(const name of ['pack_artifact','pack_version','pack_head','pack_demand','pack_installation','pack_activation','application_binding'])assert(existsSync(join(schemas,name+'.json')),'The reviewed lifecycle schemas are missing.');
 const db=new Database(dbPath,{readonly:true});db.exec('PRAGMA query_only=ON');
 const command=args=>run([binary,...args,'--db',dbPath,'--schemas',schemas],{env});
 const lifecycle=(operation,extra=[])=>Object.fromEntries(command(['pack-lifecycle','--operation',operation,'--pack',target.pack,'--subject',target.subject,'--scope',target.scope,'--actor',target.subject,...extra]).trim().split('\n').map(l=>l.split('\t')).filter(x=>x.length===2));
 const binding=()=>db.query('SELECT w.*,i.pack,i.installed_for FROM _application_workspace w JOIN pack_installation i ON i.id=w.installation_id WHERE w.binding_id=?').get(target.binding);
 try{
  const before=binding();assert(before&&before.pack===target.pack&&before.installed_for===target.subject,'The receiving account binding changed.');
  assert.equal(before.release,target.base,'The installed release changed; review it before upgrading.');
  const initial=lifecycle('current');assert.equal(initial.state,'active','The receiving editor is not currently active.');
  const packageInfo=await materializeReferenceRepair({sourceRelease:target.base,root:packages});await verifyWorkspacePackage(packages,packageInfo.id);
  if(expectedRelease)assert.equal(packageInfo.id,expectedRelease,'The reviewed candidate changed.');
  const candidateVersion=projectConnectedInstrumentsSetup(packageInfo.id).find(r=>r.type==='pack_version').value,expectedPriorVersion=projectConnectedInstrumentsSetup(target.base).find(r=>r.type==='pack_version').value;
  const version=candidateVersion.version,head=db.query("SELECT * FROM pack_head WHERE namespace='root' AND pack=? AND channel='stable'").get(target.pack),demand=db.query("SELECT * FROM pack_demand WHERE namespace='root' AND pack=? AND demand_kind='workspace'").all(target.pack);
  assert(head&&demand.length===1);const oldVersion=db.query("SELECT * FROM pack_version WHERE namespace='root' AND pack=? AND version=?").get(target.pack,head.version);
  assert(oldVersion?.root===expectedPriorVersion.root&&head.version===expectedPriorVersion.version&&demand[0].preferred_sources===target.base,'A separately prepared release owns the declarations. Do not overwrite it.');
  const review={target,candidate:packageInfo.id,version,initialRevision:initial.revision,priorVersion:head.version,changedFiles:packageInfo.changedFiles,inputs:sourcePaths.map(path=>({path,sha256:hash(readFileSync(join(root,path)))}))};
  if(!apply){console.log('REFERENCE_RELEASE_REVIEW',review);return review;}
  assert(expectedRelease,'Activation requires the exact reviewed package digest.');const disk=statfsSync(home);assert(disk.bavail*disk.bsize>96*1024*1024,'Insufficient test and rollback disk reserve.');
  const test=Bun.spawnSync([process.execPath,'test','scripts/tests/retained-material.test.mjs','scripts/tests/reference-repair-browser.test.mjs'],{cwd:root,env:process.env,stdout:'pipe',stderr:'pipe',timeout:120000});assert.equal(test.exitCode,0,test.stderr.toString().slice(-5000));
  console.log('REFERENCE_ACCEPTANCE',test.stderr.toString().slice(-1800));
  for(const input of review.inputs)assert.equal(hash(readFileSync(join(root,input.path))),input.sha256,'Source changed after tests.');assert.equal((await materializeReferenceRepair({sourceRelease:target.base,root:packages})).id,expectedRelease);
  assert.equal(hash(readFileSync(servicePath)),hash(serviceBytes),'A concurrent service release changed the owner.');assert.deepEqual(binding(),before);assert.deepEqual(lifecycle('current'),initial);
  assert.deepEqual(db.query('SELECT * FROM pack_head WHERE id=?').get(head.id),head);assert.deepEqual(db.query('SELECT * FROM pack_demand WHERE id=?').get(demand[0].id),demand[0]);
  const recordsBefore=db.query('SELECT * FROM _workspace_record ORDER BY scope_key,record_key').all(),bindingsBefore=db.query('SELECT * FROM _application_workspace ORDER BY binding_id').all(),stopPath=join(home,'.bonfyre/operator/STOP'),stop=readFileSync(stopPath);
  const existing=db.query("SELECT * FROM pack_version WHERE namespace='root' AND pack=? AND version=?").get(target.pack,version);
  if(existing)assert(existing.root===candidateVersion.root&&existing.status==='published','The candidate version identity is already occupied.');
  else command(['entry','create','pack_version',JSON.stringify({...candidateVersion,changed:'Account-scoped reference reader; original text and evidence offsets preserved; package '+expectedRelease,proof:'scripts/tests/retained-material.test.mjs; scripts/tests/reference-repair-browser.test.mjs'}),'--ns','root']);
  command(['entry','update','pack_head',String(head.id),JSON.stringify({version}),'--ns','root']);
  command(['entry','update','pack_demand',String(demand[0].id),JSON.stringify({preferred_sources:expectedRelease}),'--ns','root']);
  let current=lifecycle('plan');assert.equal(current.revision,initial.revision,'Installation changed during preparation.');
  current=lifecycle('install',['--expected-digest',current.digest,'--expected-revision',current.revision]);
  if(current.state==='installed')current=lifecycle('activate',['--expected-digest',current.pinned_digest,'--expected-revision',current.revision]);assert.equal(current.state,'active');
  const after=binding();assert(after.binding_id===target.binding&&after.release===expectedRelease&&after.installed_for===target.subject);
  assert.deepEqual(db.query('SELECT * FROM _workspace_record ORDER BY scope_key,record_key').all(),recordsBefore,'Saved records changed during rollout. No record rollback was attempted.');
  const bindingsAfter=db.query('SELECT * FROM _application_workspace ORDER BY binding_id').all();for(const row of bindingsBefore.filter(r=>r.binding_id!==target.binding))assert.deepEqual(bindingsAfter.find(r=>r.binding_id===row.binding_id),row,'Another binding changed during rollout.');
  assert.deepEqual(readFileSync(stopPath),stop);assert.equal(hash(readFileSync(servicePath)),hash(serviceBytes));
  const receipt={schema:'bonfyre.reference-reader-rollout.v1',at:new Date().toISOString(),...review,state:current.state,revision:current.revision,recordCount:recordsBefore.length,savedRecordsUnchanged:true,otherBindingsUnchanged:true,serviceUnchanged:true,operatorPauseUnchanged:true,sourceReadsOnly:true,testLogSha256:hash(test.stdout)+':'+hash(test.stderr)};
  const retained=join(home,'.bonfyre/workflow-recovery/20261022-references');mkdirSync(retained,{recursive:true,mode:0o700});writeFileSync(join(retained,expectedRelease+'.installed.mpk'),encode(receipt),{flag:'wx',mode:0o600});
  console.log('REFERENCE_READER_LIVE',receipt);return receipt;
 }finally{db.close();}
}
if(import.meta.main){const [mode,expectedRelease]=process.argv.slice(2);assert(['review','activate'].includes(mode),'Use review or activate EXACT_PACKAGE_DIGEST');await rolloutReferences({apply:mode==='activate',expectedRelease});}
