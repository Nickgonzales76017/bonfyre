// Upgrade an explicitly selected existing workspace through its CMS owners.
// The journal records intent before dispatch; an uncertain write is never replayed.
import assert from 'node:assert/strict';
import {createHash,randomBytes} from 'node:crypto';
import {readFileSync,writeFileSync,mkdirSync,lstatSync,realpathSync,openSync,closeSync,fsyncSync,linkSync,unlinkSync,existsSync} from 'node:fs';
import {join,resolve,isAbsolute} from 'node:path';
import {homedir} from 'node:os';
import {Database} from 'bun:sqlite';
import {verifyWorkspacePackage} from '../workspace_material.mjs';
import {encode} from '../../integrations/browser/workspace-msgpack.mjs';

const root=resolve(import.meta.dir,'../..'),digest=value=>createHash('sha256').update(value).digest('hex');
const sha=value=>digest(canonical(value));
const validDigest=value=>typeof value==='string'&&/^[a-f0-9]{64}$/.test(value);
const stages=['version','head','demand','install','activate'];
function validateSpec(spec){
 assert(spec&&/^[a-z][a-z0-9-]{2,79}$/.test(spec.id)&&typeof spec.schema==='string','Choose an explicit workspace upgrade specification.');
 const t=spec.target;assert(t&&/^experience\.[a-z0-9.-]+\.workspace$/.test(t.pack)&&/^user:[1-9][0-9]*$/.test(t.subject)&&t.scope==='self'&&/^ws-[a-f0-9]{40}$/.test(t.binding)&&/^[a-z][a-z0-9-]*$/.test(t.family)&&/^[a-z][a-z0-9-]*\/$/.test(t.entry),'Invalid current-account workspace target.');
 for(const key of ['versionFor','baseRoot','materialize','verify'])assert(typeof spec[key]==='function','Missing workspace specification: '+key);
 assert(spec.allowInitialStale===undefined||typeof spec.allowInitialStale==='boolean','allowInitialStale must be an explicit boolean.');
 assert(Array.isArray(spec.sourcePaths)&&spec.sourcePaths.length>0&&spec.sourcePaths.every(path=>typeof path==='string'&&!isAbsolute(path)&&!path.split('/').includes('..')),'Explicit source inputs required.');
}
function canonical(value){
 if(value===null||typeof value!=='object')return JSON.stringify(value);
 if(Array.isArray(value))return '['+value.map(canonical).join(',')+']';
 return '{'+Object.keys(value).sort().map(key=>JSON.stringify(key)+':'+canonical(value[key])).join(',')+'}';
}
function same(actual,expected,label){assert.equal(canonical(actual),canonical(expected),label);}
function row(actual,before,patch={},label='Native row changed'){
 // CMS refreshes these timestamps when its lifecycle publishes a row.
 const clean=value=>Object.fromEntries(Object.entries(value??{}).filter(([key])=>!['updated_at','published_at'].includes(key)));
 same(clean(actual),clean({...before,...patch}),label);
}
function fileHash(path){const stat=lstatSync(path);assert(stat.isFile()&&!stat.isSymbolicLink(),'Expected a regular file: '+path);return digest(readFileSync(path));}
function directory(path){mkdirSync(path,{recursive:true,mode:0o700});const stat=lstatSync(path);assert(stat.isDirectory()&&!stat.isSymbolicLink(),'Journal directory is not a real directory.');return realpathSync(path);}
function durableOnce(path,value){
 const bytes=Buffer.from(canonical(value)+'\n');assert(bytes.length<=1024*1024,'Upgrade receipt exceeds its bound.');
 const parent=resolve(path,'..'),temporary=path+'.'+process.pid+'.'+randomBytes(6).toString('hex')+'.pending';
 const fd=openSync(temporary,'wx',0o600);try{writeFileSync(fd,bytes);fsyncSync(fd);}finally{closeSync(fd);}
 let created=false;try{linkSync(temporary,path);created=true;}catch(error){if(error.code!=='EEXIST')throw error;}finally{unlinkSync(temporary);}
 if(created){const dir=openSync(parent,'r');try{fsyncSync(dir);}finally{closeSync(dir);}}
 return created;
}
function readJSON(path){
 const stat=lstatSync(path);assert(stat.isFile()&&!stat.isSymbolicLink()&&stat.size<=1024*1024,'Invalid upgrade receipt.');
 return JSON.parse(readFileSync(path,'utf8'));
}
export function upgradeJournal(base,candidate){
 assert(validDigest(candidate),'Choose the exact reviewed package digest.');
 const dir=directory(join(directory(base),candidate));
 return {dir,read:name=>existsSync(join(dir,name+'.json'))?readJSON(join(dir,name+'.json')):null,put:(name,value)=>durableOnce(join(dir,name+'.json'),value)};
}
function matchesVersion(actual,expected){return actual&&actual.namespace==='root'&&Object.entries(expected).every(([key,value])=>actual[key]===value);}
export function workspaceEntrySnapshot(value,columns){
 // Same typed row envelope as cmd/BonfyreCMS/src/entry_snapshot.h. SQLite's
 // physical column order and storage classes are read inside the same snapshot.
 assert(Array.isArray(columns)&&columns.length>0&&columns.length<=1024,'Invalid native entry columns.');
 const rows=columns.map(([name,kind])=>{
  const entry=value[name];assert(Object.hasOwn(value,name),'The entry snapshot is incomplete.');
  if(kind==='null'){assert(entry===null);return [name,5,null];}
  if(kind==='integer'){assert(Number.isSafeInteger(entry));return [name,1,entry];}
  if(kind==='text'){assert(typeof entry==='string');return [name,3,Buffer.from(entry)];}
  if(kind==='blob'){assert(entry instanceof Uint8Array);return [name,4,entry];}
  throw Error('Unsupported native entry storage class: '+kind);
 });
 return digest(encode(rows,{maxBytes:2*1024*1024}));
}
function pins(value){return value.map(value=>value.slice(0,3)).sort(([a],[b])=>a.localeCompare(b));}
function baseline(review,snapshot){
 const before=review.before,t=review.target;
 same(snapshot.preserved,before.preserved,'Saved records, other bindings, source declarations or service changed.');
 same(snapshot.baseVersion,before.baseVersion,'The previously installed version changed.');
 assert(snapshot.binding.binding_id===t.binding&&snapshot.binding.family===t.family&&snapshot.binding.entry===t.entry,'The receiving editor changed.');
}
function assertStage(snapshot,review,stage,plan){
 baseline(review,snapshot);const before=review.before,t=review.target,index=stages.indexOf(stage);
 if(index>=0)assert(matchesVersion(snapshot.candidateVersion,review.version),'The candidate version is unavailable or differs.');
 else same(snapshot.candidateVersion,before.candidateVersion,'The candidate version changed before dispatch.');
 row(snapshot.head,before.head,index>=1?{version:review.version.version}:{},'The stable head changed outside this review.');
 row(snapshot.demand,before.demand,index>=2?{preferred_sources:review.candidate}:{},'The workspace demand changed outside this review.');
 if(index<3){
  same(snapshot.binding,before.binding,'The installed workspace changed before installation.');
  same(snapshot.installation,before.installation,'The installation changed before dispatch.');
  same(snapshot.installationState,before.installationState,'The installation revision changed before dispatch.');
  same(snapshot.activation,before.activation,'The activation changed before dispatch.');
  same(snapshot.application,before.application,'The application binding changed before dispatch.');
  const expectedState=index>=2?'stale':before.native.state;
  assert(snapshot.native.state===expectedState,'The installed lifecycle state changed.');
  assert(snapshot.native.installation===before.native.installation&&snapshot.native.revision===before.native.revision&&snapshot.native.pinned_digest===before.native.pinned_digest,'The native current view changed.');
 }else{
  assert(plan&&validDigest(plan.digest),'A reviewed native plan is required.');const active=index===4;
  row(snapshot.binding,before.binding,{release:review.candidate});
  row(snapshot.installation,before.installation,{pinned_version:review.version.version,installed_by:t.subject,status:'published'});
  row(snapshot.installationState,before.installationState,{revision:before.installationState.revision+(active?2:1),source_digest:plan.digest,last_operation:active?'activate':'install',last_actor:t.subject});
  row(snapshot.activation,before.activation,{conditions:'pack-lock:'+plan.digest,activated_by:t.subject,status:active?'published':'withdrawn'});
  row(snapshot.application,before.application,{status:active?'active':'draft'});
  assert(snapshot.native.state===(active?'active':'installed')&&snapshot.native.installation===before.native.installation&&Number(snapshot.native.revision)===snapshot.installationState.revision&&snapshot.native.pinned_digest===plan.digest,'Native installation readback differs.');
  same(pins(snapshot.native.pins),pins(plan.nodes),'The installed version pins differ from the reviewed plan.');
 }
}
function validateInitial(snapshot,spec){
 const target=spec.target,initialState=snapshot.native.state;
 const declarationStale=spec.allowInitialStale===true&&initialState==='stale'&&snapshot.native.error==='resolution_refused';
 const initialAllowed=(initialState==='active'&&!snapshot.native.error)||declarationStale;
 assert(initialAllowed,'The receiving workspace must be active, or explicitly allow an exact resolution-refused declaration-stale closure, before review.');
 assert(snapshot.binding.binding_id===target.binding&&snapshot.binding.family===target.family&&snapshot.binding.entry===target.entry&&validDigest(snapshot.binding.release),'The current workspace identity is not the receiving editor.');
 assert(snapshot.installation.pack===target.pack&&snapshot.installation.installed_for===target.subject&&snapshot.installation.status==='published','The receiving account installation changed.');
 assert(snapshot.activation.pack===target.pack&&snapshot.activation.activated_for===target.subject&&snapshot.activation.scope===target.scope&&snapshot.activation.status==='published','The receiving activation changed.');
 assert(snapshot.application.status==='active'&&snapshot.application.binding_id===target.binding,'The receiving application is inactive.');
 assert(Number(snapshot.native.revision)===snapshot.installationState.revision&&Number(snapshot.native.installation)===snapshot.binding.installation_id&&snapshot.native.pinned_digest===snapshot.installationState.source_digest,'Native and database reads disagree; repeat the review.');
 assert(snapshot.head.channel==='stable'&&snapshot.head.status==='published'&&snapshot.head.version===snapshot.installation.pinned_version,'A separately prepared head owns the next upgrade.');
 assert(snapshot.baseVersion.version===snapshot.head.version&&snapshot.baseVersion.root===spec.baseRoot(snapshot.binding.release)&&snapshot.baseVersion.status==='published','The current version does not identify the installed workspace package.');
 assert(snapshot.demand.status==='published'&&snapshot.demand.needs===target.family&&snapshot.demand.fields===target.entry&&snapshot.demand.preferred_sources===snapshot.binding.release,'A separately prepared workspace demand owns the next upgrade.');
}
export async function reviewWorkspaceUpgrade(adapter,spec){
 validateSpec(spec);const {target,schema}=spec;const material=await adapter.materialize();assert(validDigest(material.id),'Invalid materialized package.');
 await adapter.verify(material.id);const journal=upgradeJournal(adapter.journalRoot,material.id),existing=journal.read('review');
 if(existing){assert(existing.digest===sha(existing.review),'The retained review changed.');return {...summary(existing.review,journal),state:journal.read('complete')?'already_verified':'review_retained'};}
 const versionName=spec.versionFor(material.id,null).version,before=await adapter.inspect({candidateVersion:versionName});validateInitial(before,spec);
 assert(material.id!==before.binding.release,'This exact package is already installed.');
 const version=spec.versionFor(material.id,before.binding.release);assert(!before.candidateVersion||matchesVersion(before.candidateVersion,version),'The candidate version identity is occupied.');
 const review={schema,target,candidate:material.id,version,before,inputs:await adapter.inputs(),created:new Date().toISOString()};
 const packet={review,digest:sha(review)};assert(journal.put('review',packet),'Another review was retained concurrently; reopen it.');
 return {...summary(review,journal),state:'reviewed'};
}
function summary(review,journal){return {candidate:review.candidate,version:review.version.version,binding:review.target.binding,previousRelease:review.before.binding.release,initialRevision:review.before.installationState.revision,reviewSha256:sha(review),journal:journal.dir};}
function planCheck(plan,review){
 assert(!plan.error&&plan.version===review.version.version&&validDigest(plan.digest),'The native plan did not resolve the reviewed package.');
 assert(plan.installation===review.before.native.installation&&plan.revision===review.before.native.revision,'The native installation changed before planning.');
 const expected=review.before.native.pins.map(node=>node[0]===review.target.pack?[node[0],review.version.version,review.version.root]:node);
 same(pins(plan.nodes),pins(expected),'The plan would change another version pin.');
}
export async function applyWorkspaceUpgrade(adapter,candidate,spec){
 validateSpec(spec);const {target,schema}=spec;
 const journal=upgradeJournal(adapter.journalRoot,candidate),packet=journal.read('review');assert(packet&&packet.digest===sha(packet.review),'No intact review exists for this package.');
 const review=packet.review;assert(review.schema===schema&&review.candidate===candidate,'The review identifies another package.');same(review.target,target,'The reviewed target changed.');
 same(await adapter.inputs(),review.inputs,'Source inputs changed after review.');await adapter.verify(candidate);
 assert((await adapter.materialize()).id===candidate,'The materialized package changed after review.');
 const inspect=()=>adapter.inspect({candidateVersion:review.version.version,baseVersion:review.before.baseVersion.version,closurePacks:review.before.native.pins.map(node=>node[0])});
 let plan=journal.read('plan')?.plan;
 if(journal.read('complete')){assertStage(await inspect(),review,'activate',plan);return {...summary(review,journal),state:'already_verified',revision:review.before.installationState.revision+2};}
 for(let index=0;index<stages.length;index++){
  const stage=stages[index],previous=stages[index-1]??'before',done=journal.read(stage+'-done');
  if(done){assert(done.reviewSha256===packet.digest,'A completion receipt belongs to another review.');continue;}
  if(stage==='install'){
   if(!plan){const current=await inspect();assertStage(current,review,'demand');const proposed=await adapter.plan(review.version.version);planCheck(proposed,review);assert(journal.put('plan',{reviewSha256:packet.digest,plan:proposed}),'Another process retained a plan; reconcile before continuing.');plan=proposed;}
   assert(journal.read('plan').reviewSha256===packet.digest,'The native plan belongs to another review.');planCheck(plan,review);
  }
  const intent=journal.read(stage+'-intent');let current=await inspect();
  if(intent){
   assert(intent.reviewSha256===packet.digest,'A pending write belongs to another review.');
   try{assertStage(current,review,stage,plan);}catch(error){throw new Error('Unconfirmed '+stage+' write. Keep the journal; no native write was replayed. '+error.message,{cause:error});}
   journal.put(stage+'-done',{reviewSha256:packet.digest,readbackSha256:sha(current),reconciled:true});continue;
  }
  assertStage(current,review,previous,plan);
  if(stage==='version'&&review.before.candidateVersion){journal.put(stage+'-done',{reviewSha256:packet.digest,readbackSha256:sha(current),alreadyPresent:true});continue;}
  if(stage==='install'){const fresh=await adapter.plan(review.version.version);planCheck(fresh,review);same(fresh,plan,'The reviewed native plan changed.');}
  const action=stage==='version'?{operation:'create',table:'pack_version',value:review.version}:
   stage==='head'?{operation:'update',table:'pack_head',id:review.before.head.id,ifDigest:review.before.headDigest,value:{version:review.version.version}}:
   stage==='demand'?{operation:'update',table:'pack_demand',id:review.before.demand.id,ifDigest:review.before.demandDigest,value:{preferred_sources:candidate}}:
   {operation:stage,digest:plan.digest,revision:review.before.installationState.revision+(stage==='activate'?1:0),version:review.version.version};
  assert(journal.put(stage+'-intent',{reviewSha256:packet.digest,beforeSha256:sha(current),action}),'Another process began this step. Reconcile the retained intent; no duplicate write was sent.');
  let outcome;try{outcome=await adapter.mutate(action);}catch(error){outcome={error:String(error?.message??error).slice(0,2000)};}
  journal.put(stage+'-reply',{reviewSha256:packet.digest,outcome:outcome??null});
  try{current=await inspect();assertStage(current,review,stage,plan);}catch(error){throw new Error('Unconfirmed '+stage+' write. Keep the journal and rerun for native readback; do not replay the write. '+error.message,{cause:error});}
  journal.put(stage+'-done',{reviewSha256:packet.digest,readbackSha256:sha(current),reconciled:Boolean(outcome?.error||outcome?.exitCode)});
 }
 const after=await inspect();assertStage(after,review,'activate',plan);same(await adapter.inputs(),review.inputs,'Source inputs changed during the upgrade.');await adapter.verify(candidate);
 const receipt={...summary(review,journal),schema,state:'installed_verified',revision:after.installationState.revision,editorURL:'https://aurekai.work/apps/'+review.target.binding+'/'+review.target.entry,savedRecordsUnchanged:true,otherBindingsUnchanged:true,serviceUnchanged:true,operatorPauseUnchanged:true,readbackSha256:sha(after)};
 journal.put('complete',receipt);return receipt;
}

export function parseWorkspaceLifecycle(raw,pack){
 assert(typeof raw==='string'&&raw.length<=256*1024,'Native lifecycle reply exceeds its bound.');const result={pins:[],nodes:[]};
 for(const line of raw.trim().split('\n')){const [key,...values]=line.split('\t');if(key==='pin'||key==='node')result[key==='pin'?'pins':'nodes'].push(values);else if(values.length===1){assert(!Object.hasOwn(result,key),'Duplicate lifecycle field.');result[key]=values[0];}}
 assert(result.schema==='bonfyre.pack.lifecycle.v1'&&result.pack===pack,'Invalid native lifecycle reply.');
 assert(/^\d+$/.test(result.revision??'')&&/^\d+$/.test(result.installation??'')&&validDigest(result.pinned_digest),'Native lifecycle identity is unavailable.');return result;
}
function run(argv,{env=process.env,timeout=30000}={}){
 const child=Bun.spawnSync(argv,{cwd:root,env,stdout:'pipe',stderr:'pipe',timeout,maxBuffer:1024*1024});
 const stdout=child.stdout.toString(),stderr=child.stderr.toString();return {exitCode:child.exitCode,stdout,stderr};
}
function successful(argv,options){const result=run(argv,options);assert.equal(result.exitCode,0,result.stderr.slice(-2000));return result.stdout;}
function unique(rows,label){assert(rows.length===1,'Expected one '+label+'.');return rows[0];}
function hashRows(rows){let count=0;const h=createHash('sha256');for(const item of rows){assert(++count<=100000,'Native read exceeds its row bound.');h.update(canonical(item)+'\n');}return {count,sha256:h.digest('hex')};}
export function nativeWorkspaceUpgradeAdapter({home=homedir(),spec}={}){
 validateSpec(spec);const {target}=spec,pack=target.pack;
 const servicePath=join(home,'Library/LaunchAgents/com.bonfyre.public-web-address.plist'),serviceSha=fileHash(servicePath);
 const config=JSON.parse(successful(['/usr/bin/plutil','-convert','json','-o','-',servicePath]));assert(config.Label==='com.bonfyre.public-web-address','Unexpected public service.');
 const env={...process.env,...config.EnvironmentVariables},binary=config.EnvironmentVariables?.BONFYRE_CMS_BINARY,dbPath=config.EnvironmentVariables?.BONFYRE_CMS_DB;
 assert(isAbsolute(binary??'')&&isAbsolute(dbPath??''),'Use the deployed CMS owner and canonical database.');
 const schemas=join(home,'.bonfyre/workflow-recovery/20261022-shipments/schemas'),packages=config.EnvironmentVariables.BONFYRE_WORKSPACE_PACKAGES||join(home,'.bonfyre/workspace-packages');
 assert(isAbsolute(packages),'The workspace package root must be absolute.');
 const schemaNames=['pack_artifact','pack_version','pack_head','pack_demand','pack_installation','pack_activation','application_binding'];
 const sourceHashes=()=>[...new Set(['scripts/release/workspace_upgrade.mjs',...spec.sourcePaths])].sort().map(path=>({path,sha256:fileHash(join(root,path))}));
 const ownerHashes=()=>({servicePath,serviceSha:fileHash(servicePath),binary:realpathSync(binary),binarySha:fileHash(realpathSync(binary)),dbPath:realpathSync(dbPath),schemas:schemaNames.map(name=>({name,sha256:fileHash(join(schemas,name+'.json'))})),packages:resolve(packages),operatorPause:fileHash(join(home,'.bonfyre/operator/STOP'))});
 const expectedOwner=ownerHashes();assert(expectedOwner.serviceSha===serviceSha,'The service changed while opening the upgrade owner.');
 const db=new Database(dbPath,{readonly:true});db.exec('PRAGMA query_only=ON');
 const command=args=>run([binary,...args,'--db',dbPath,'--schemas',schemas],{env});
 const lifecycle=(operation,extra=[])=>command(['pack-lifecycle','--operation',operation,'--pack',pack,'--subject',target.subject,'--scope',target.scope,'--actor',target.subject,...extra]);
 const tableRows=(table,key,value)=>db.query('SELECT * FROM '+table+' WHERE '+key+'=? ORDER BY id').all(value);
 const readOne=(table,key,value)=>unique(tableRows(table,key,value),table+' row');
 const entryDigest=(table,value)=>{
  assert(['pack_head','pack_demand'].includes(table));
  const names=db.query('PRAGMA table_info('+table+')').all().sort((a,b)=>a.cid-b.cid).map(column=>column.name);assert(names.every(name=>/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(name)),'Unsupported native column name.');
  const types=db.query('SELECT '+names.map(name=>'typeof("'+name+'") AS "'+name+'"').join(',')+' FROM '+table+' WHERE id=? AND namespace=?').get(value.id,'root');
  assert(types,'The native entry disappeared.');return workspaceEntrySnapshot(value,names.map(name=>[name,types[name]]));
 };
 const inspect=({candidateVersion,baseVersion,closurePacks}={})=>{
  const nativeReply=lifecycle('current'),native=parseWorkspaceLifecycle(nativeReply.stdout,pack);assert(nativeReply.exitCode===0||(nativeReply.exitCode===1&&native.state==='stale'),'The current native lifecycle could not be read.');
  const owner=ownerHashes();same(owner,expectedOwner,'The deployed owner or service changed.');
  db.exec('BEGIN');try{
   const binding=unique(db.query('SELECT * FROM _application_workspace WHERE binding_id=?').all(target.binding),'workspace binding'),installation=readOne('pack_installation','id',binding.installation_id);
   const installationState=unique(db.query('SELECT * FROM _pack_installation_state WHERE installation_id=?').all(installation.id),'installation state');
   const activation=unique(db.query('SELECT * FROM pack_activation WHERE namespace=? AND pack=? AND activated_for=? AND scope=?').all(installation.namespace,pack,target.subject,target.scope),'account activation');
   const application=readOne('application_binding','id',binding.cms_id);
   const rootRows=table=>db.query('SELECT * FROM '+table+" WHERE namespace='root' AND pack=? ORDER BY id").all(pack);
   const head=unique(rootRows('pack_head').filter(item=>item.channel==='stable'),'stable head'),demand=unique(rootRows('pack_demand').filter(item=>item.demand_kind==='workspace'),'workspace demand');
   const old=unique(rootRows('pack_version').filter(item=>item.version===(baseVersion||installation.pinned_version)),'installed version');
   const candidates=rootRows('pack_version').filter(item=>item.version===candidateVersion);assert(candidates.length<=1,'Duplicate candidate version.');
   const selected=closurePacks??native.pins.map(item=>item[0]);assert(selected.length>0&&selected.length<=64,'Invalid installed closure.');const definitions=[];
   for(const closurePack of [...selected].sort())for(const table of ['pack_artifact','pack_version','pack_head','pack_dependency','pack_overlay','pack_demand']){
    const key=table==='pack_overlay'?'overlay':'pack';for(const item of tableRows(table,key,closurePack)){
     if(item.namespace!=='root'||(closurePack===pack&&((table==='pack_head'&&item.id===head.id)||(table==='pack_demand'&&item.id===demand.id)||(table==='pack_version'&&item.version===candidateVersion))))continue;
     definitions.push({table,row:item});
    }
   }
   const preserved={owner,definitions:hashRows(definitions),records:hashRows(db.query('SELECT * FROM _workspace_record ORDER BY scope_key,record_key').iterate()),otherBindings:hashRows(db.query('SELECT w.*,a.status AS application_status FROM _application_workspace w JOIN application_binding a ON a.id=w.cms_id WHERE w.binding_id<>? ORDER BY w.binding_id').iterate(target.binding))};
   const snapshot={binding,installation,installationState,activation,application,head,demand,headDigest:entryDigest('pack_head',head),demandDigest:entryDigest('pack_demand',demand),baseVersion:old,candidateVersion:candidates[0]??null,preserved,native};
   assert(Number(native.revision)===installationState.revision&&Number(native.installation)===installation.id&&native.pinned_digest===installationState.source_digest,'Native state changed during readback.');db.exec('COMMIT');return snapshot;
  }catch(error){db.exec('ROLLBACK');throw error;}
 };
 return {
  journalRoot:join(home,'.bonfyre/release-handoff',spec.id),inputs:sourceHashes,inspect,
  materialize:()=>spec.materialize({root:packages}),
  verify:async id=>{const result=await verifyWorkspacePackage(packages,id);await spec.verify({root:packages,id});return result;},
  plan:version=>{const result=lifecycle('plan',['--version',version]);assert.equal(result.exitCode,0,'The candidate plan was refused.');return parseWorkspaceLifecycle(result.stdout,pack);},
  mutate:action=>{
   let result;if(action.operation==='create'||action.operation==='update'){
    assert(['pack_version','pack_head','pack_demand'].includes(action.table));
    if(action.operation==='update')assert(validDigest(action.ifDigest),'An exact native entry baseline is required.');
    result=command(['entry',action.operation,action.table,...(action.operation==='update'?[String(action.id)]:[]),JSON.stringify(action.value),'--ns','root',...(action.operation==='update'?['--if-digest',action.ifDigest]:[])]);
   }else{assert(['install','activate'].includes(action.operation));result=lifecycle(action.operation,['--version',action.version,'--expected-digest',action.digest,'--expected-revision',String(action.revision)]);}
   return {exitCode:result.exitCode,stdoutSha256:digest(result.stdout),stderrSha256:digest(result.stderr)};
  },close:()=>db.close()
 };
}
