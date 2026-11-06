// Extend the existing public-service release owner. Its accepted hashes,
// concurrency refusal, rollback and public checks remain mandatory.
import assert from 'node:assert/strict';
import {readFileSync,writeFileSync,mkdirSync,lstatSync,realpathSync,existsSync,rmSync,unlinkSync} from 'node:fs';
import {join,resolve,relative,dirname} from 'node:path';
import {homedir} from 'node:os';
import {createHash} from 'node:crypto';
import {encode,decode} from '../../integrations/browser/workspace-msgpack.mjs';
import {inspectRelease,verifyInputs,verifyBrowser} from './public_surface_rollout.mjs';
import {connectionWorkspaceMaterial,connectionModules} from '../materialize_connections_workspace.mjs';
import {mergeConnectionCredentialRegistries} from './connection_credential_registry.mjs';
const root=resolve(import.meta.dir,'../..'),home=homedir(),base=join(home,'.bonfyre/releases'),runner=join(root,'scripts/release/public_surface_rollout.mjs');
const sha=b=>createHash('sha256').update(b).digest('hex'),hash=p=>sha(readFileSync(p));
function run(args,{env=process.env,timeout=240000}={}){const p=Bun.spawnSync(args,{cwd:root,env,stdout:'pipe',stderr:'pipe',timeout});assert.equal(p.exitCode,0,p.stderr.toString().slice(-5000));return p.stdout.toString();}
const read=p=>decode(readFileSync(p),{maxBytes:16*1024*1024});
const save=(p,value)=>writeFileSync(p,encode(value,{maxBytes:16*1024*1024}),{flag:'wx',mode:0o600});
const plist=p=>JSON.parse(run(['/usr/bin/plutil','-convert','json','-o','-',p]));
function pathOf(path){const result=realpathSync(path);assert(relative(base,result)&&!relative(base,result).startsWith('..')&&lstatSync(result).isDirectory());return result;}
export function freeze(dir,path,target=join('native',path),mode=0o644){
 const source=join(root,path);assert(lstatSync(source).isFile());const body=readFileSync(source),dest=join(dir,target);mkdirSync(dirname(dest),{recursive:true});
 // The base public stage may inherit a connection-catalog symlink to the
 // currently active release. Replace only the new candidate's symlink:
 // overwriting its target would mutate an immutable live binary.
 if(target==='bin/bonfyre-connection-catalog'&&lstatSync(dest,{throwIfNoEntry:false})?.isSymbolicLink())
  unlinkSync(dest);
 if(existsSync(dest))assert.equal(hash(dest),sha(body),'An existing frozen release member differs: '+target);else writeFileSync(dest,body,{flag:'wx',mode});
 return{input:{path,sha256:sha(body)},material:{path:target,sha256:sha(body)}};
}
function ownerClosure(entries){
 const seen=new Set();function visit(path){assert(!path.startsWith('../')&&!path.startsWith('/')&&path.endsWith('.mjs'));if(seen.has(path))return;seen.add(path);const text=readFileSync(join(root,path),'utf8');
  for(const match of text.matchAll(/(?:from\s+|import\s*\(\s*|import\s*)['"](\.[^'"]+)['"]/g)){const child=relative(root,resolve(root,dirname(path),match[1]));if(child.endsWith('.mjs'))visit(child);}
 }entries.forEach(visit);assert(seen.size<100,'Unexpected owner dependency expansion');return[...seen].sort();
}
async function stage(){
 const output=run([process.execPath,runner,'stage']);console.log(output);const dir=pathOf(output.match(/^STAGED (.+)$/m)?.[1]);const original=read(join(dir,'stage.mpk'));assert(!existsSync(join(dir,'accepted.mpk')));
 const inputs=new Map(original.inputs.map(r=>[r.path,r])),material=new Map(original.material.map(r=>[r.path,r]));
 const add=row=>{const prior=inputs.get(row.input.path);if(prior)assert.equal(prior.sha256,row.input.sha256);inputs.set(row.input.path,row.input);material.set(row.material.path,row.material);};
 for(const name of connectionModules)add(freeze(dir,'integrations/browser/'+name,'browser/'+name));
 const ownerPaths=ownerClosure(['integrations/browser/onboarding/account-connections.mjs','integrations/mcp/fabric_activity_cli.mjs']);
 for(const path of ownerPaths)add(freeze(dir,path));
 for(const path of ['docs/engineering/handoff-backfill-20261019/onboarding-enrollment.tsv','docs/engineering/handoff-backfill-20261019/onboarding-recovery-p0.tsv','integrations/mcp/fabric_schema.sql'])add(freeze(dir,path));
 const credentialBase=freeze(dir,'cmd/BonfyreCredentials/provider-credentials.json','native/cmd/BonfyreCredentials/provider-credentials.base.json');add(credentialBase);
 const credentialSupplement=freeze(dir,'integrations/browser/onboarding/connection-credential-supplement.json','native/integrations/browser/onboarding/connection-credential-supplement.json');add(credentialSupplement);
 const mergedDeclaration=join(dir,'native/cmd/BonfyreCredentials/provider-credentials.json');writeFileSync(mergedDeclaration,mergeConnectionCredentialRegistries(readFileSync(join(root,'cmd/BonfyreCredentials/provider-credentials.json')),readFileSync(join(root,'integrations/browser/onboarding/connection-credential-supplement.json'))),{flag:'wx',mode:0o600});material.set('native/cmd/BonfyreCredentials/provider-credentials.json',{path:'native/cmd/BonfyreCredentials/provider-credentials.json',sha256:hash(mergedDeclaration)});
 run(['make','-C','cmd/BonfyreCredentials','bonfyre-credentials']);add(freeze(dir,'cmd/BonfyreCredentials/bonfyre-credentials','bin/bonfyre-credentials',0o755));run(['make','-C','cmd/BonfyreConnectionCatalog','test']);for(const path of ['cmd/BonfyreConnectionCatalog/src/main.c','cmd/BonfyreConnectionCatalog/Makefile','cmd/BonfyreConnectionCatalog/tests/catalog_test.sh'])add(freeze(dir,path));add(freeze(dir,'cmd/BonfyreConnectionCatalog/bonfyre-connection-catalog','bin/bonfyre-connection-catalog',0o755));for(const[source,target]of [[realpathSync('/opt/homebrew/opt/duckdb/lib/libduckdb.dylib'),'runtime/lib/libduckdb.dylib'],[join(home,'.bonfyre/estate-local/workboard-connector-facets-20261102-v2.parquet'),'private/connection-facets.parquet'],[join(home,'.bonfyre/estate-local/workboard-source-entries-20261102.parquet'),'private/connection-entries.parquet']]){const body=readFileSync(source),dest=join(dir,target);mkdirSync(dirname(dest),{recursive:true,mode:0o700});writeFileSync(dest,body,{flag:'wx',mode:0o600});assert.equal(hash(source),sha(body),'Catalog source changed during staging');material.set(target,{path:target,sha256:sha(body)});}const catalogBin=join(dir,'bin/bonfyre-connection-catalog');run(['/usr/bin/install_name_tool','-change','/opt/homebrew/opt/duckdb/lib/libduckdb.dylib','@loader_path/../runtime/lib/libduckdb.dylib',catalogBin]);run(['/usr/bin/codesign','--force','--sign','-',catalogBin]);run(['/usr/bin/codesign','--verify','--strict',catalogBin]);material.set('bin/bonfyre-connection-catalog',{path:'bin/bonfyre-connection-catalog',sha256:hash(catalogBin)});
 const candidatePath=join(dir,'candidate-service.plist'),candidate=plist(candidatePath),env=candidate.EnvironmentVariables;
 env.BONFYRE_CONNECTION_OWNER=join(dir,'native/integrations/browser/onboarding/account-connections.mjs');
 env.BONFYRE_CONNECTION_BINDINGS=join(home,'.bonfyre/onboarding/account-bindings.mpk');env.BONFYRE_CONNECTION_CATALOG_SUBJECT='user:19';env.BONFYRE_CONNECTION_CATALOG_BINARY=join(dir,'bin/bonfyre-connection-catalog');env.BONFYRE_CONNECTION_CATALOG_FACETS=join(dir,'private/connection-facets.parquet');env.BONFYRE_CONNECTION_CATALOG_ENTRIES=join(dir,'private/connection-entries.parquet');
 env.BONFYRE_ACTIVITY_OWNER=join(dir,'native/integrations/mcp/fabric_activity_cli.mjs');
 env.BONFYRE_ENROLLMENT_HOME=join(home,'.bonfyre/onboarding/native');env.BONFYRE_ENROLLMENT_DB=join(env.BONFYRE_ENROLLMENT_HOME,'enrollment.db');
 env.BONFYRE_QUEUE_BINARY=join(dir,'bin/bonfyre-queue');env.BONFYRE_CREDENTIALS_BINARY=join(dir,'bin/bonfyre-credentials');
 const declaration=join(dir,'native/cmd/BonfyreCredentials/provider-credentials.json');env.BONFYRE_CREDENTIAL_PROVIDERS=declaration;env.BONFYRE_CREDENTIALS_PROVIDERS=declaration;
 assert(existsSync(env.BONFYRE_CONNECTION_BINDINGS),'Prepare the explicitly assigned personal account first.');
 const temp=join(dir,'.connections-candidate');writeFileSync(temp,JSON.stringify(candidate),{mode:0o600,flag:'wx'});run(['/usr/bin/plutil','-convert','xml1','-o',candidatePath,temp]);rmSync(temp);
 material.set('candidate-service.plist',{path:'candidate-service.plist',sha256:hash(candidatePath)});
 const workspace=await connectionWorkspaceMaterial();const next={...original,inputs:[...inputs.values()],material:[...material.values()],connections:{ownerPaths,workspace}};
 save(join(dir,'connections-base-stage.mpk'),original);writeFileSync(join(dir,'stage.mpk'),encode(next,{maxBytes:16*1024*1024}),{mode:0o600});
 inspectRelease(dir);verifyInputs(next);await verifyBrowser(dir);save(join(dir,'connections-stage.mpk'),{stageSha256:hash(join(dir,'stage.mpk')),workspace,assignmentSha256:hash(env.BONFYRE_CONNECTION_BINDINGS)});
 console.log('CONNECTIONS_STAGED',dir,'WORKSPACE_RELEASE',workspace.id);return dir;
}
async function verify(dir){
 const staged=read(join(dir,'connections-stage.mpk'));assert.equal(staged.stageSha256,hash(join(dir,'stage.mpk')));const stage=inspectRelease(dir);verifyInputs(stage);
 const candidate=plist(join(dir,'candidate-service.plist')),env=candidate.EnvironmentVariables;
 assert.equal(hash(env.BONFYRE_CONNECTION_BINDINGS),staged.assignmentSha256,'Account assignment changed after review');
 const built=await Bun.build({entrypoints:[join(dir,'browser/connection-studio.mjs')],target:'browser',write:false});assert(built.success,'The frozen connection UI is incomplete.');const catalog=JSON.parse(run([env.BONFYRE_CONNECTION_CATALOG_BINARY,'list','--facets',env.BONFYRE_CONNECTION_CATALOG_FACETS,'--entries',env.BONFYRE_CONNECTION_CATALOG_ENTRIES,'--view','app','--limit','50']));assert.equal(catalog.schema,'bonfyre.connection-catalog.v1');assert.equal(catalog.entries.length,50);assert(catalog.entries.every(row=>Object.keys(row).sort().join(',')==='ecosystem,name,safe_link,status'&&row.status==='unverified_source_candidate'),'Catalog app output contains unexpected fields.');
 for(const identity of ['gmail','proton']){const out=Bun.spawnSync([env.BONFYRE_BUN_BINARY,'--no-env-file','--no-install',env.BONFYRE_CONNECTION_OWNER,'--operation','list','--actor','user:19','--subject','user:19','--scope','self','--identity',identity],{env:{...env,HOME:home},stdout:'pipe',stderr:'pipe',timeout:20000});assert.equal(out.exitCode,0);const packet=decode(out.stdout);assert.equal(packet[1],true);assert.equal(packet[2].prepared,225);assert.equal(packet[2].binding.subject,'user:19');}
 for(const id of ['204','209','211','212']){const out=Bun.spawnSync([env.BONFYRE_BUN_BINARY,'--no-env-file','--no-install',env.BONFYRE_CONNECTION_OWNER,'--operation','get','--actor','user:19','--subject','user:19','--scope','self','--identity','gmail','--work',id+'-gmail'],{env:{...env,HOME:home},stdout:'pipe',stderr:'pipe',timeout:20000});assert.equal(out.exitCode,0);const packet=decode(out.stdout);assert.equal(packet[1],true);assert.equal(packet[2].credential_fields.length,1);assert.equal(packet[2].credential_fields[0].field,'api_key');const method=packet[2].entry.auth.methods.find(row=>row.strategy==='api_key');assert(method&&method.available&&method.owner==='BonfyreCredentials','Candidate owner did not bind '+id+' to its native API-key field.');}
 const tests=['scripts/tests/connections-rollout-freeze.test.mjs','cmd/BonfyreWebAddress/tests/connections_workspace.test.mjs','integrations/browser/tests/account-connections.test.mjs','integrations/browser/tests/enrollment-continuation.test.mjs','integrations/browser/tests/provider-continuation.test.mjs'];
 const testEnv={...process.env,BONFYRE_TEST_WEB_BINARY:join(dir,'bin/bonfyre-web-address'),BONFYRE_TEST_WORKSPACE_CLIENT:join(dir,'browser')};
 const result=Bun.spawnSync([process.execPath,'test',...tests],{cwd:root,env:testEnv,stdout:'pipe',stderr:'pipe',timeout:240000});const log=Buffer.concat([result.stdout,result.stderr]);writeFileSync(join(dir,'connections-acceptance.log'),log,{mode:0o600});console.log(log.toString().slice(-5000));assert.equal(result.exitCode,0,'Connection acceptance failed.');
 console.log(run([process.execPath,runner,'verify',dir],{timeout:600000}));verifyInputs(stage);inspectRelease(dir);
 save(join(dir,'connections-accepted.mpk'),{stageSha256:hash(join(dir,'stage.mpk')),testsSha256:hash(join(dir,'connections-acceptance.log')),baseAcceptedSha256:hash(join(dir,'accepted.mpk'))});
 console.log('CONNECTIONS_ACCEPTED',dir);
}
async function readback(dir){
 const rows=[];for(const host of ['aurekai.work','aurekai.live','aurekai.life','aurekai.shop']){
  for(const name of ['connection-source.mjs','connection-studio.mjs','connection-studio.css','connection-lab.mjs']){const r=await fetch('https://'+host+'/apps/'+name,{redirect:'error',signal:AbortSignal.timeout(15000)});assert.equal(r.status,200);assert.equal(sha(Buffer.from(await r.arrayBuffer())),hash(join(dir,'browser',name)));rows.push({host,path:'/apps/'+name,status:200});}
  const p=await fetch('https://'+host+'/apps/_connections?context=self&identity=gmail&operation=list',{redirect:'manual',signal:AbortSignal.timeout(15000)});assert.equal(p.status,403);await p.body?.cancel();rows.push({host,path:'/apps/_connections',status:403});const c=await fetch('https://'+host+'/apps/_connection_catalog?offset=0',{redirect:'manual',signal:AbortSignal.timeout(15000)});assert.equal(c.status,403);await c.body?.cancel();rows.push({host,path:'/apps/_connection_catalog',status:403});
 }return rows;
}
async function activate(dir){
 const approved=read(join(dir,'connections-accepted.mpk'));assert.equal(approved.stageSha256,hash(join(dir,'stage.mpk')));assert.equal(approved.testsSha256,hash(join(dir,'connections-acceptance.log')));assert.equal(approved.baseAcceptedSha256,hash(join(dir,'accepted.mpk')));
 console.log(run([process.execPath,runner,'activate',dir],{timeout:240000}));const rows=await readback(dir);save(join(dir,'connections-live.mpk'),{rows,at:new Date().toISOString()});console.log('CONNECTIONS_LIVE',dir,rows.length);
}
if(import.meta.main){const[operation,path]=process.argv.slice(2);if(operation==='stage')await stage();else{const dir=pathOf(path);if(operation==='verify')await verify(dir);else if(operation==='activate')await activate(dir);else if(operation==='readback')console.log(await readback(dir));else throw Error('Use stage, verify, activate or readback.');}}
