import {afterAll,beforeAll,describe,expect,test} from 'bun:test';
import {mkdtempSync,readFileSync,writeFileSync,rmSync,existsSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {resolve,dirname,join} from 'node:path';
import {decode,encode} from '../../../integrations/browser/workspace-msgpack.mjs';
import {readInbox,setInboxState,readInboxReceipt} from '../../../integrations/browser/aurekai-inbox-source.mjs';
import {readTasks,readAgents,readCompute} from '../../../integrations/browser/aurekai-activity-source.mjs';
import {Database} from 'bun:sqlite';

const root=resolve(import.meta.dir,'../../..'),dependencies=process.env.BONFYRE_TEST_DEPENDENCIES||root;
const queue=process.env.BONFYRE_TEST_QUEUE||join(root,'cmd/BonfyreQueue/bonfyre-queue');
const include=process.env.BONFYRE_TEST_SQLITE_INCLUDE;
let temporary,binary,database,environment;
function native(args,input){
 const result=Bun.spawnSync([queue,...args,'--db',database],{stdin:input??new Uint8Array(),env:environment});
 if(result.exitCode)throw Error(result.stderr.toString()+result.stdout.toString());return result.stdout;
}
function fn(source,name){
 const start=source.search(new RegExp('^static [^\\n]*\\b'+name+'\\(', 'm'));
 if(start<0)throw Error('Missing native helper '+name);
 const brace=source.indexOf('{',start),lineEnd=source.indexOf('\n',brace),firstClose=source.indexOf('}',brace);
 const end=firstClose<lineEnd?firstClose+1:source.indexOf('\n}',brace)+2;
 if(end<start)throw Error('Incomplete native helper '+name);return source.slice(start,end);
}
beforeAll(()=>{
 if(!existsSync(queue))throw Error('Build the real BonfyreQueue owner before this test.');
 temporary=mkdtempSync(join(tmpdir(),'bonfyre-account-gateway-'));database=join(temporary,'fleming-work.db');binary=join(temporary,'gateway');
 environment={...process.env,BONFYRE_BIN_DIR:dirname(queue),BONFYRE_TEST_ACTIVITY_ROOT:temporary};
 const workspace=readFileSync(join(root,'cmd/BonfyreWebAddress/src/application_workspace.h'),'utf8');
 const setup=readFileSync(join(root,'cmd/BonfyreWebAddress/src/pack_setup.h'),'utf8');
 // The exact production context resolver and transport helpers are compiled.
 // Only external identity/grant lookups are controlled by these fixture tokens.
 const source=`#define _DEFAULT_SOURCE
#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <strings.h>
#include <limits.h>
#include <errno.h>
#include <unistd.h>
#include <signal.h>
#include <sys/wait.h>
#include <sqlite3.h>
#include "bf_json.h"
#include "bf_msgpack.h"
#define AW_PACKET (512u*1024u)
typedef struct {long id;} StudioViewer;
typedef struct {char role[16];} BfHouseholdAccess;
static int session_identity(const char *token,const char *db,StudioViewer *who){(void)db;
 if(!strcmp(token,"reader7")||!strcmp(token,"writer7")||!strcmp(token,"revoked7")){who->id=7;return 1;}
 if(!strcmp(token,"reader8")){who->id=8;return 1;}return 0;}
static int session_self_authoring(const char *token,const char *db){(void)db;return !strcmp(token,"writer7");}
static int session_institution_authorized(const char *token,const char *db,const char *id,const char *role){(void)db;
 return !strcmp(id,"acme")&&(!strcmp(token,"writer7")||(!strcmp(role,"read")&&(!strcmp(token,"reader7")||!strcmp(token,"reader8"))));}
static sqlite3 *cms_open_ro(void){return NULL;}
static int bhh_context(sqlite3 *db,const char *id,const char *actor,BfHouseholdAccess *out){(void)db;
 if(strcmp(id,"home")||strcmp(actor,"user:7"))return 0;snprintf(out->role,sizeof(out->role),"member");return 1;}
static int bhh_rank(const char *role){return !strcmp(role,"member")?1:0;}
${fn(setup,'psetup_hex')}
${fn(setup,'psetup_decode')}
${fn(setup,'psetup_context')}
${fn(workspace,'aw_response_with_referrer')}\n${fn(workspace,'aw_response')}
${fn(workspace,'aw_error')}
${fn(workspace,'aw_header')}
${fn(workspace,'aw_native_result')}
static const char *fleming_root(void){return getenv("BONFYRE_TEST_ACTIVITY_ROOT");}
#include "account_activity_web.h"
int main(int argc,char **argv){
 if(argc!=6)return 2;char body[4097]={0};size_t bytes=fread(body,1,4096,stdin);if(ferror(stdin))return 2;
 if(!strcmp(argv[1],"INPUT")){char account[33],context[96],actor[160],subject[160];const char *scope=NULL;
  if(!aaw_fleming_input(body,account,context))return 3;
  if(psetup_context(argv[3],"fixture",context,2,actor,sizeof(actor),subject,sizeof(subject),&scope)!=1)return 4;
  printf("%s\\t%s\\t%s\\t%s",account,context,subject,scope);return 0;}
 return serve_account_activity(argv[1],"/apps/_activity",argv[2],bytes?body:NULL,argv[4],argv[5],argv[3],"fixture")?0:2;
}`;
 writeFileSync(join(temporary,'gateway.c'),source);
 const compile=Bun.spawnSync(['cc','-std=c11','-O1','-Wall','-Wextra',...(process.platform==='darwin'?['-D_DARWIN_C_SOURCE']:[]),
  ...(include?['-I'+include]:[]),'-I'+join(dependencies,'lib/libbonfyre/include'),'-I'+join(root,'cmd/BonfyreWebAddress/src'),
  join(temporary,'gateway.c'),join(dependencies,'lib/libbonfyre/bf_msgpack.c'),join(dependencies,'lib/libbonfyre/bf_json.c'),
  process.env.BONFYRE_TEST_SQLITE_LINK||'-lsqlite3','-o',binary]);
 if(compile.exitCode)throw Error(compile.stderr.toString());
 for(const [id,subject,scope] of [['account-seven','user:7','self'],['account-eight','user:8','self'],['account-shared','institution:acme','institution'],['account-home','household:home','household']]){
  native(['enqueue','fixture','private-input-secret','--mission',id,'--subject',subject,'--scope',scope,'--title','Private task '+id,
   '--continuation','/apps?context='+ (scope==='self'?'self':subject)]);
 }
 native(['enqueue','fixture','legacy-public-input','--mission','legacy-task']);
});
afterAll(()=>{if(temporary)rmSync(temporary,{recursive:true,force:true});});
function gateway({operation='list',context='self',method='GET',token='reader7',extra={},body,raw,host='aurekai.work',query}={}){
 const headers=raw??`HTTP/1.1\r\nOrigin: https://aurekai.work\r\nContent-Type: application/msgpack\r\nContent-Length: ${body?.byteLength??0}\r\n\r\n`;
 const result=Bun.spawnSync([binary,method,query??new URLSearchParams({context,operation,...extra}).toString(),token,headers,host],{stdin:body??new Uint8Array(),env:environment});
 expect(result.exitCode).toBe(0);const output=new Uint8Array(result.stdout),split=result.stdout.indexOf('\r\n\r\n');
 expect(split).toBeGreaterThan(0);const head=result.stdout.subarray(0,split).toString(),status=Number(head.match(/^HTTP\/1.1 (\d+)/)[1]);
 return {status,diagnostic:result.stderr.toString(),head,bytes:output.slice(split+4),value:status===200?decode(output.slice(split+4)):null};
}
const task=(options={})=>gateway(options).value[2].tasks[0];
function requestAs(token='reader7'){
 return (url,options)=>{
  const parsed=new URL(url,'https://aurekai.work');const result=gateway({token,method:options.method,query:parsed.search.slice(1),body:options.body});
  return Promise.resolve(new Response(result.bytes,{status:result.status,headers:{'Content-Type':result.status===200?'application/msgpack':'text/plain','Content-Length':String(result.bytes.length)}}));
 };
}
describe('current grants → HTTP owner → actual Queue → production Inbox adapter',()=>{
 test('self projection excludes another account and unscoped operator work',()=>{
  const result=gateway();expect(result.status).toBe(200);expect(result.value[2].tasks.map(x=>x.id)).toEqual(['account-seven']);
  expect(result.value[2].binding).toEqual({actor:'user:7',subject:'user:7',scope:'self'});expect(result.value[2].source).toBe('account-activity');
  expect(result.head).toContain('private, no-store');expect(new TextDecoder().decode(result.bytes)).not.toContain('private-input-secret');
  expect(gateway({token:'reader8'}).value[2].tasks.map(x=>x.id)).toEqual(['account-eight']);
 });
 test('organization and household reads use native context resolution',()=>{
  expect(task({context:'institution:acme'}).id).toBe('account-shared');expect(task({context:'household:home'}).id).toBe('account-home');
  expect(gateway({context:'institution:acme',token:'reader8'}).value[2].binding).toEqual({actor:'user:8',subject:'institution:acme',scope:'institution'});
  expect(gateway({operation:'get',context:'household:home',extra:{mission:'account-home'}}).value[2].binding).toEqual({actor:'user:7',subject:'household:home',scope:'household'});
  expect(gateway({context:'household:home',token:'reader8'}).status).toBe(403);
  expect(gateway({context:'institution:elsewhere'}).status).toBe(403);
 });
 test('missing session and a revoked grant stop each read',()=>{
  expect(gateway({token:'expired'}).status).toBe(403);
  expect(gateway({context:'institution:acme',token:'revoked7'}).status).toBe(403);
 });
 test('browser actor, duplicate coordinates, methods and traversal cannot widen a query',()=>{
  for(const query of ['context=self&operation=list&actor=user:8','context=self&context=institution:acme&operation=list',
    'context=self&operation=list&','context=self&operation=get&mission=..%2Fsecret','context=self&operation=inbox&mission=account-seven&cursor=9'])expect(gateway({query}).status).toBe(400);
  expect(gateway({method:'DELETE'}).status).toBe(400);
  expect(gateway({operation:'get',extra:{mission:'account-eight'}}).status).toBe(503);
 });
 test('a reader can mark only their own Inbox without task write authority',async()=>{
  const request=requestAs(),before=await readInbox('self',{request});const row=before.notices[0];
  const change={id:row.id,version:row.version,revision:row.revision,action:'read',requestId:'gateway-read-0001',expectedBinding:before.binding};
  const result=await setInboxState('self',change,{request});expect(result.notice.read).toBe(true);expect(result.appliedRevision).toBe(1);
  const replay=await setInboxState('self',change,{request});expect(replay.replayed).toBe(true);expect(replay.appliedRevision).toBe(1);
  const receipt=await readInboxReceipt('self',change.requestId,{request});expect(receipt.notice.read).toBe(true);
  expect((await readInbox('self',{request:requestAs('reader8')})).notices[0].read).toBe(false);
 });
 test('organization notification dispositions belong to the recipient',async()=>{
  const request=requestAs(),before=await readInbox('institution:acme',{request}),row=before.notices[0];
  const updated=await setInboxState('institution:acme',{id:row.id,version:row.version,revision:row.revision,action:'dismiss',requestId:'gateway-dismiss-001',expectedBinding:before.binding},{request});
  expect(updated.notice.dismissed).toBe(true);
  expect((await readInbox('institution:acme',{request:requestAs('reader8')})).notices[0].dismissed).toBe(false);
  await expect(readInboxReceipt('institution:acme','gateway-dismiss-001',{request:requestAs('revoked7')})).rejects.toMatchObject({status:403});
 });
 test('mutation requires exact origin, native media type, single headers and bounded length',async()=>{
  const before=await readInbox('self',{request:requestAs()}),row=before.notices[0];
  const body=encode([1,{id:row.id,version:row.version,revision:row.revision,action:'dismiss',requestId:'origin-refusal-01',expectedBinding:before.binding}]);
  const base=`HTTP/1.1\r\nOrigin: https://aurekai.work\r\nContent-Type: application/msgpack\r\nContent-Length: ${body.length}\r\n\r\n`;
  for(const raw of [base.replace('https://aurekai.work','https://elsewhere.test'),base.replace('https://aurekai.work','http://aurekai.work'),
    base.replace('Content-Type:','Origin: https://aurekai.work\r\nContent-Type:')])expect(gateway({method:'POST',operation:'inbox-set',body,raw}).status).toBe(403);
  for(const raw of [base.replace('application/msgpack','application/json'),base.replace(String(body.length),'4097'),
    base.replace('Content-Length:',`Content-Length: ${body.length}\r\nContent-Length:`)])expect(gateway({method:'POST',operation:'inbox-set',body,raw}).status).toBe(400);
  expect((await readInbox('self',{request:requestAs()})).notices[0].dismissed).toBe(false);
 });
 test('forged Inbox target and stale snapshot leave the saved notification intact',async()=>{
  const request=requestAs(),before=await readInbox('self',{request}),row=before.notices[0];
  await expect(setInboxState('self',{id:'account-eight',version:row.version,revision:0,action:'dismiss',requestId:'forged-target-001',expectedBinding:before.binding},{request})).rejects.toMatchObject({code:'not_found'});
  await expect(setInboxState('self',{id:row.id,version:row.version,revision:0,action:'dismiss',requestId:'stale-revision-01',expectedBinding:before.binding},{request})).rejects.toMatchObject({code:'version_conflict'});
  expect((await readInbox('self',{request})).notices[0].dismissed).toBe(false);
 });
 test('a session switch inside the same organization cannot apply the previous actor notification action',async()=>{
  const before=await readInbox('institution:acme',{request:requestAs('reader7')}),row=before.notices[0];
  const body=encode([1,{id:row.id,version:row.version,revision:row.revision,action:'read',requestId:'account-switch-001',expectedBinding:before.binding}]);
  expect(gateway({context:'institution:acme',operation:'inbox-set',method:'POST',token:'reader8',body}).status).toBe(409);
  const current=await readInbox('institution:acme',{request:requestAs('reader8')});expect(current.notices[0].read).toBe(false);
  await expect(readInboxReceipt('institution:acme','account-switch-001',{request:requestAs('reader8')})).rejects.toMatchObject({code:'receipt_pending'});
 });
 test('Fleming admission parses one exact audience and requires creation authority',()=>{
  const input=(value,token='writer7')=>Bun.spawnSync([binary,'INPUT','',''+token,'','aurekai.work'],{stdin:new TextEncoder().encode(typeof value==='string'?value:JSON.stringify(value)),env:environment});
  expect(input({account:'123',context:'institution:acme'}).stdout.toString()).toBe('123\tinstitution:acme\tinstitution:acme\tinstitution');
  expect(input({account:'12-3'}).stdout.toString()).toBe('12-3\tself\tuser:7\tself');
  expect(input({account:'123'},'reader7').exitCode).toBe(4);
  expect(input({account:'123',context:'institution:other'}).exitCode).toBe(4);
  for(const body of ['{"account":"123","context":"self","context":"institution:acme"}',
    '{"account":"123","context":{"value":"self"}}','{"account":"123","actor":"user:8"}','{"account":"123","nested":{"context":"self"}}'])expect(input(body).exitCode).toBe(3);
 });
 test('account work comes from the owners that record it, under the owner alone',async()=>{const t=Math.floor(Date.now()/1000),operator=join(temporary,'operator.sqlite3'),fabric=join(temporary,'fabric.sqlite3'),index=join(temporary,'account-work.db');const op=new Database(operator);op.run(`CREATE TABLE goals(id TEXT PRIMARY KEY,title TEXT,state TEXT,created_at REAL,updated_at REAL)`);op.run(`CREATE TABLE runs(id TEXT PRIMARY KEY,goal_id TEXT,started_at REAL,completed_at REAL,state TEXT)`);op.run(`CREATE TABLE actions(seq INTEGER PRIMARY KEY,run_id TEXT,tool TEXT,state TEXT,started_at REAL,duration_ms INTEGER)`);op.run(`INSERT INTO goals VALUES('g1','Fold product workflows into live Aurekai','retry',${t-900},${t-800}),('g2','Watch a source','queued',${t-30*86400},${t-30*86400})`);op.run(`INSERT INTO runs VALUES('r1','g1',${t-850},${t-840},'failed')`);op.run(`INSERT INTO actions VALUES(1,'r1','project_search','passed',${t-848},125)`);op.close();const fb=new Database(fabric);fb.run(`CREATE TABLE tasks(id TEXT PRIMARY KEY,title TEXT,state TEXT,created_at REAL,started_at REAL,completed_at REAL)`);fb.run(`CREATE TABLE runs(id TEXT PRIMARY KEY,task_id TEXT,agent TEXT,role TEXT,state TEXT,started_at REAL,completed_at REAL)`);fb.run(`INSERT INTO tasks VALUES('t1','Procurement editors','running',${t-600},${t-590},NULL)`);fb.run(`INSERT INTO runs VALUES('a1','t1','codex','implementer','running',${t-590},NULL)`);fb.close();const saved={...environment};Object.assign(environment,{BONFYRE_ACCOUNT_WORK_DB:index,BONFYRE_ACCOUNT_WORK_OWNER:'user:7',BONFYRE_ACCOUNT_WORK_SOURCES:`operator=${operator};fabric=${fabric}`});try{const own=await readTasks('self',{request:requestAs()});expect(own.tasks.map(t=>[t.id,t.phase])).toEqual([['account-fabric-t1','active'],['account-operator-g1','attention'],['account-operator-g2','pending']]);expect(own.tasks[1].workers.map(w=>[w.workerId,w.state])).toEqual([['Aurekai assistant','failed']]);expect(own.tasks[1].nodes.map(n=>[n.parentId,n.operatorId,n.phase])).toEqual([[null,'Aurekai assistant','pending'],['job','project_search','completed']]);
  // Another account reads nothing, and its read never copies the owner's work under its own name.
expect((await readTasks('self',{request:requestAs('reader8')})).tasks).toEqual([]);expect(new Database(index,{readonly:true}).query(`SELECT DISTINCT subject FROM account_work`).all()).toEqual([{subject:'user:7'}]);
const inbox=await readInbox('self',{request:requestAs()});expect(inbox.notices.map(n=>n.id)).toEqual(['account-fabric-t1','account-operator-g1']);const marked=await setInboxState('self',{id:'account-operator-g1',version:inbox.notices[1].version,revision:inbox.notices[1].revision,action:'read',requestId:'account-work-read-01',expectedBinding:inbox.binding},{request:requestAs()});expect(marked.notice.read).toBe(true);expect((await readInbox('self',{request:requestAs('reader8')})).notices).toEqual([]);}finally{for(const key of Object.keys(environment))if(!(key in saved))delete environment[key];}}); // the Inbox announces only the last week's change, keeps read state per recipient, and Fleming answers neither.
});

test('signed-in WorkGraph outputs refresh from the native owner, never another account',async()=>{
  const workgraph=join(temporary,'fresh-output-workgraph.db'),index=join(temporary,'fresh-output-account.db');
  const timestamp=new Date().toISOString(),t=Date.now();
  const db=new Database(workgraph);
  db.exec("CREATE TABLE missions(id TEXT,status TEXT,created_at TEXT,updated_at TEXT)");
  db.exec("CREATE TABLE workgraph_nodes(mission_id TEXT,node_id TEXT,operator_id TEXT,status TEXT,attempt INTEGER,last_heartbeat_at_ms INTEGER,claim_acquired_at_ms INTEGER,output_uri TEXT)");
  db.query('INSERT INTO missions VALUES(?,?,?,?)').run('m-output','complete',timestamp,timestamp);
  db.query('INSERT INTO workgraph_nodes VALUES(?,?,?,?,?,?,?,?)').run('m-output','render','RenderAgent','complete',1,t,t,'artifact://private/first');
  for(let i=0;i<24;i++){
    const id='m-recent-'+String(i).padStart(2,'0');
    db.query('INSERT INTO missions VALUES(?,?,?,?)').run(id,'running',timestamp,timestamp);
    db.query('INSERT INTO workgraph_nodes VALUES(?,?,?,?,?,?,?,?)').run(id,'prepare','Agent','ready',0,t,t,null);
  }
  db.close();
  const saved={...environment};
  Object.assign(environment,{BONFYRE_ACCOUNT_WORK_DB:index,BONFYRE_ACCOUNT_WORK_OWNER:'user:7',
    BONFYRE_ACCOUNT_WORK_SOURCES:'workgraph='+workgraph});
  try {
    const recent=(await readTasks('self',{request:requestAs()})).tasks;
    expect(recent).toHaveLength(20);expect(recent.some(row=>row.id==='account-workgraph-m-output')).toBe(false);
    const first=(await readTasks('self',{request:requestAs(),outputsOnly:true})).tasks;
    expect(first).toHaveLength(1);
    expect(first[0].id).toBe('account-workgraph-m-output');
    expect(first[0].outputs).toHaveLength(1);
    expect(JSON.stringify(first)).not.toContain('artifact://private/first');
    expect((await readTasks('self',{request:requestAs('reader8')})).tasks).toEqual([]);
    expect((await readTasks('self',{request:requestAs('reader8'),outputsOnly:true})).tasks).toEqual([]);
    const updated=new Database(workgraph);
    updated.query('UPDATE workgraph_nodes SET output_uri=?').run('artifact://private/second');
    updated.close();
    const cached=new Database(index);
    cached.exec("UPDATE account_work_meta SET value=0 WHERE key='indexed_ms'");
    cached.close();
    const second=(await readTasks('self',{request:requestAs(),outputsOnly:true})).tasks;
    expect(second[0].outputs).toHaveLength(1);
    expect(second[0].outputs[0].reference).not.toBe(first[0].outputs[0].reference);
    expect(JSON.stringify(second)).not.toContain('artifact://private/second');
    expect((await readInbox('self',{request:requestAs('reader8')})).notices).toEqual([]);
  } finally {
    for(const k of Object.keys(environment))if(!(k in saved))delete environment[k];
    Object.assign(environment,saved);
  }
});

test('the Agents lane reads Fabric runs for the owner alone, with reported usage only',async()=>{const fabric=join(temporary,'agents-fabric.sqlite3'),index=join(temporary,'agents-account.db'),t=Date.now()/1000,stream=join(temporary,'agents-codex.jsonl');writeFileSync(stream,'{"type":"turn.completed","usage":{"input_tokens":100,"cached_input_tokens":80,"output_tokens":12}}\n');
 const db=new Database(fabric);db.exec('CREATE TABLE tasks(id TEXT PRIMARY KEY,kind TEXT,title TEXT,state TEXT,created_at REAL,started_at REAL,completed_at REAL);CREATE TABLE runs(id TEXT,task_id TEXT,agent TEXT,role TEXT,state TEXT,started_at REAL,completed_at REAL,stdout_path TEXT,duration_ms INTEGER,result_json TEXT)');db.query('INSERT INTO tasks VALUES(?,?,?,?,?,?,?)').run('r1','relay','Relay fix','running',t-60,t-60,null);db.query('INSERT INTO runs VALUES(?,?,?,?,?,?,?,?,?,?)').run('x1','r1','codex','implementation','passed',t-60,t-30,stream,30000,'{}');db.query('INSERT INTO runs VALUES(?,?,?,?,?,?,?,?,?,?)').run('x2','r1','claude','review','running',t-20,null,'',null,'{}');db.close();
 const saved={...environment};Object.assign(environment,{BONFYRE_ACCOUNT_WORK_DB:index,BONFYRE_ACCOUNT_WORK_OWNER:'user:7',BONFYRE_ACCOUNT_WORK_SOURCES:'fabric='+fabric});try{
  const own=await readAgents('self',{request:requestAs()});expect(own.binding).toEqual({actor:'user:7',subject:'user:7',scope:'self'});expect(own.counts).toEqual({active:1,attention:0,completed:1});expect(own.agents.map(row=>[row.id,row.agent,row.role,row.session,row.phase,row.wallMs])).toEqual([['x2','claude','review','relay','active',null],['x1','codex','implementation','relay','completed',30000]]);expect(own.agents[0].usage).toBeNull();expect(own.agents[1].usage).toEqual({inputTokens:100,outputTokens:12,cacheReadTokens:80,cacheWriteTokens:null,estimatedMicroUsd:null});expect((await readAgents('self',{request:requestAs('reader8')})).agents).toEqual([]);
 expect(gateway({operation:'agents',extra:{cursor:'5'}}).status).toBe(400);}finally{for(const k of Object.keys(environment))if(!(k in saved))delete environment[k];Object.assign(environment,saved);}expect(gateway({operation:'agents'}).status).toBe(503);});


test('Compute shows real account Meter counters, separates inherited snapshots and never invents charges',async()=>{
 const meter=join(temporary,'compute-evidence.db'),index=join(temporary,'compute-account-work.db');
 const db=new Database(meter);
 db.exec('CREATE TABLE events(key_id TEXT,op TEXT,provider TEXT,session_ref TEXT,counter_basis TEXT,uncached_input_tokens INTEGER,cached_input_tokens INTEGER,cache_write_tokens INTEGER,output_tokens INTEGER)');
 db.exec("INSERT INTO events VALUES('user:7','model.tokens','claude','session-one','message_usage',10,90,0,5),('user:7','model.tokens','codex','session-two','inherited_cumulative_snapshot',500,0,0,0),('user:8','model.tokens','codex','session-other','counter_delta',800,0,0,0)");
 db.close();
 const saved={...environment};
 Object.assign(environment,{BONFYRE_ACCOUNT_WORK_DB:index,BONFYRE_ACCOUNT_WORK_OWNER:'user:7',BONFYRE_ACCOUNT_WORK_SOURCES:'meter='+meter});
 try{
  const owner=Bun.spawnSync([queue,'account-work','compute','--db',index,'--subject','user:7','--scope','self','--owner','user:7','--source','meter='+meter],{env:environment});
  expect(owner.exitCode,'Queue Compute failed: '+owner.stderr.toString()+' '+owner.stdout.toString()).toBe(0);
   const probe=gateway({operation:'compute'});expect(probe.status,probe.diagnostic).toBe(200);
  const own=await readCompute('self',{request:requestAs()});
  expect(own).toMatchObject({source:'BonfyreMeter',records:2,providerSessions:2,additiveTokens:105,inheritedSnapshotTokens:500,cachedInputTokens:90,costStatus:'not_valued_no_verified_charge'});
  expect(own.providers.map(x=>[x.provider,x.additiveTokens])).toEqual([['claude',105],['codex',0]]);
  expect(gateway({operation:'compute',context:'institution:acme'}).status).toBe(403);
  expect(gateway({operation:'compute',extra:{cursor:'1'}}).status).toBe(400);
  expect(gateway({operation:'compute',token:'reader8'}).status).toBe(503);
  environment.BONFYRE_ACCOUNT_WORK_OWNER='user:8';
  const other=await readCompute('self',{request:requestAs('reader8')});
  expect(other).toMatchObject({records:1,additiveTokens:800,cachedInputTokens:0});
  expect(other.providers.map(p=>p.provider)).toEqual(['codex']);
 }finally{for(const k of Object.keys(environment))if(!(k in saved))delete environment[k];Object.assign(environment,saved);}
});
