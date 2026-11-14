import { Database } from 'bun:sqlite';
import { createHash } from 'node:crypto';
import { constants, closeSync, fstatSync, lstatSync, openSync, readSync, realpathSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { spawnSync } from 'node:child_process';
import { accountAuth } from '../../components/BonfyreServiceSite/account-auth.mjs';
import { queue as nativeQueue } from '../../components/BonfyreControlPlane/obligation-work.mjs';
import { nativeReview } from './review.mjs';
import { commandBindings, reviewedCommandEffects } from './command_effects.mjs';

// The caller supplies only IDs and a session. Configuration belongs to the
// installed owner. CMS programs, native Auth and Queue DBs remain authoritative.
const hash=/^[a-f0-9]{64}$/;
const absolute=(value,name)=>{
  if(typeof value!=='string'||!value.startsWith('/'))throw Error('Owner '+name+' required');
  return resolve(value);
};
// Review/installer digest native executables in 64 KiB chunks, not 512 MiB
// heap copies. The open descriptor must retain its identity throughout.
function nativeBinaryDigest(path){
  const st=lstatSync(path);
  if(!st.isFile()||st.isSymbolicLink()||(st.mode&0o111)===0||
     st.size<1||st.size>512*1024*1024||realpathSync(path)!==path)
    throw Error('Installed native executor is unavailable or unsafe');
  let fd;
  try {
    fd=openSync(path,constants.O_RDONLY|constants.O_NOFOLLOW);
    const before=fstatSync(fd);
    if(!before.isFile()||before.dev!==st.dev||before.ino!==st.ino||
       before.size!==st.size||before.mtimeMs!==st.mtimeMs)
      throw Error('Installed native executor changed during verification');
    const hash=createHash('sha256'),chunk=Buffer.allocUnsafe(64*1024);
    let remaining=before.size;
    while(remaining>0){
      const bytes=readSync(fd,chunk,0,Math.min(chunk.length,remaining),null);
      if(bytes<=0)throw Error('Installed native executor truncated during verification');
      hash.update(chunk.subarray(0,bytes));remaining-=bytes;
    }
    const after=fstatSync(fd);
    if(after.dev!==before.dev||after.ino!==before.ino||
       after.size!==before.size||after.mtimeMs!==before.mtimeMs)
      throw Error('Installed native executor changed during verification');
    return hash.digest('hex');
  }finally{if(fd!==undefined)closeSync(fd);}
}
function parseRows(db) {
  const rows=db.query("SELECT * FROM capability_program WHERE status='published' AND application_kind='native-command' ORDER BY id LIMIT 513").all();
  if(rows.length>512)throw Error('Native command capability registry exceeded the bounded scan');
  return rows.map(row=>{
    const p={...row};
    for(const key of ['consumes','produces','carrier_candidates','capability_requirements','transition_conditions'])
      if(typeof p[key]==='string')p[key]=JSON.parse(p[key]);
    return p;
  });
}
function policies(programs){
  const policies=new Map();
  for(const program of programs) {
    const r=program.transition_conditions?.review;
    if(!r||typeof r.source_root!=='string'||!r.source_root.startsWith('/'))
      throw Error('Native review source-root declaration is required');
    const declaration={experience:r.experience,institution:r.institution,
      approvedFamily:r.approved_family,sourceRoot:absolute(r.source_root,'sourceRoot')};
    const existing=policies.get(r.experience);
    if(existing&&JSON.stringify(existing)!==JSON.stringify(declaration))
      throw Error('Conflicting review policy for native command capability');
    policies.set(r.experience,declaration);
  }
  return [...policies.values()];
}

// Installed native workers belong to the destination's current PackGraph.
// This installer-only function is not an HTTP/MCP dispatch capability.
export function registerNativeCommandWorker({cmsDb,cmsBinary,cmsSchemas,queueDb,binRoot,
  command,programId,workspaceBinding,expectedSha256}={}){
  const id=/^[a-z0-9](?:[a-z0-9-]{0,62}[a-z0-9])?$/;
  if(!id.test(command??'')||!id.test(programId??'')||
     !/^ws-[a-f0-9]{40}$/.test(workspaceBinding??'')||
     !hash.test(expectedSha256??''))throw Error('Invalid installed command identity');
  cmsDb=absolute(cmsDb,'CMS database');queueDb=absolute(queueDb,'Queue database');
  cmsBinary=absolute(cmsBinary,'CMS binary');cmsSchemas=absolute(cmsSchemas,'CMS schemas');
  const root=realpathSync(absolute(binRoot,'native binary root')),binary=join(root,'bonfyre-'+command);
  const actual=nativeBinaryDigest(binary);
  if(actual!==expectedSha256)throw Error('Installed native executor digest changed');
  const cms=new Database(cmsDb,{readonly:true});
  let binding,context;
  try {
    const rows=parseRows(cms).filter(p=>p.program_id===programId);
    if(rows.length!==1)throw Error('Published native capability is unavailable or ambiguous');
    binding=commandBindings(rows).get(command);
    if(!binding||binding.program!==programId)throw Error('Command differs from its declared capability');
    const installed=cms.query(
      "SELECT i.installed_for AS subject,a.scope,a.activated_by AS actor,"+
      " w.binding_id AS workspace_binding,w.release AS workspace_release"+
      " FROM _application_workspace w"+
      " JOIN pack_installation i ON i.id=w.installation_id"+
      " JOIN application_binding b ON b.id=w.cms_id AND b.namespace=i.namespace"+
      " JOIN pack_activation a ON a.pack=i.pack AND a.activated_for=i.installed_for AND a.namespace=i.namespace"+
      " WHERE w.binding_id=? AND w.family=? AND i.pack=?"+
      " AND i.status='published' AND a.status='published' AND b.status='active'"+
      " AND b.site_id=i.installed_for AND b.slot_id=i.pack LIMIT 2").all(
        workspaceBinding,binding.family,binding.pack);
    if(installed.length!==1)throw Error('Native capability has no unique active destination');
    context=installed[0];
    if(!hash.test(context.workspace_release??'')||!['self','institution'].includes(context.scope))
      throw Error('Installed destination is not attested');
  }finally{cms.close();}
  const run=spawnSync(cmsBinary,['pack-lifecycle','--operation','current',
    '--pack',binding.pack,'--subject',context.subject,'--scope',context.scope,
    '--actor',context.actor,'--version','','--expected-digest','','--expected-revision','0',
    '--db',cmsDb,'--schemas',cmsSchemas],{encoding:'utf8',timeout:10000,maxBuffer:32768});
  if(run.status!==0)throw Error('Native PackGraph current read failed');
  const current=Object.fromEntries(run.stdout.trimEnd().split('\n').map(line=>{
    const at=line.indexOf('\t');return at<1?['','']:[line.slice(0,at),line.slice(at+1)];
  }));
  if(current.state!=='active'||!hash.test(current.digest??'')||
     current.digest!==current.pinned_digest)
    throw Error('Installed PackGraph is not active and pinned');
  const db=new Database(queueDb,{create:false,strict:true});
  try {
    if(!db.query("SELECT 1 FROM sqlite_master WHERE type='table' AND name='workgraph_nodes'").get())
      throw Error('Canonical native WorkGraph store is missing');
    db.exec("CREATE TABLE IF NOT EXISTS _native_command_executor("+
      "operator_id TEXT NOT NULL,program_id TEXT NOT NULL,pack_digest TEXT NOT NULL,"+
      "workspace_binding TEXT NOT NULL,worker_binary TEXT NOT NULL,worker_sha256 TEXT NOT NULL,"+
      "artifact_kind TEXT NOT NULL,state TEXT NOT NULL,"+
      "PRIMARY KEY(operator_id,program_id,pack_digest,workspace_binding))");
    db.transaction(()=>{
      const key=[binding.operator,programId,current.digest,workspaceBinding];
      const previous=db.query("SELECT worker_binary,worker_sha256,artifact_kind,state"+
        " FROM _native_command_executor WHERE operator_id=? AND program_id=?"+
        " AND pack_digest=? AND workspace_binding=?").get(...key);
      if(previous && (previous.worker_binary!==binary||previous.worker_sha256!==actual||
         previous.artifact_kind!=='immutable-review-decision'||previous.state!=='ready'))
        throw Error('Installed native executor changed; use a new reviewed release');
      if(!previous)db.query("INSERT INTO _native_command_executor"+
        " (operator_id,program_id,pack_digest,workspace_binding,worker_binary,"+
        "worker_sha256,artifact_kind,state) VALUES(?,?,?,?,?,?,'immutable-review-decision','ready')")
        .run(...key,binary,actual);
    }).immediate();
  }finally{db.close();}
  return Object.freeze({program_id:programId,operator_id:binding.operator,
    pack_digest:current.digest,workspace_binding:workspaceBinding,worker_sha256:actual,
    registered:true,executed:false});
}

export function readNativeCommandWorker({queueDb,operator,program,packDigest,
  workspaceBinding,artifactKind}={}){
  let db;
  try {
    db=new Database(queueDb,{readonly:true,create:false});
    const rows=db.query("SELECT operator_id,program_id,pack_digest,"+
      "workspace_binding,worker_binary,worker_sha256,state,artifact_kind"+
      " FROM _native_command_executor WHERE operator_id=? AND program_id=?"+
      " AND pack_digest=? AND workspace_binding=? LIMIT 2").all(
        operator,program,packDigest,workspaceBinding);
    if(rows.length!==1)return null;
    const row=rows[0];
    if(row.state!=='ready'||row.artifact_kind!==artifactKind||
       row.operator_id!==operator||row.program_id!==program||
       row.workspace_binding!==workspaceBinding||row.pack_digest!==packDigest||
       !hash.test(row.worker_sha256)||typeof row.worker_binary!=='string'||
       !row.worker_binary.startsWith('/'))return null;
    const current=nativeBinaryDigest(row.worker_binary);
    if(current!==row.worker_sha256)return null;
    return {state:'ready',operator_id:operator,program_id:program,
      pack_digest:packDigest,workspace_binding:workspaceBinding,binary_sha256:current};
  }catch{return null;}finally{db?.close();}
}

export function openNativeCommandOwner({home,authDb,authBinary,cmsDb,cmsBinary,
  cmsSchemas,queueDb}){
  home=absolute(home,'review home');
  authDb=absolute(authDb,'Auth database');
  authBinary=absolute(authBinary,'Auth binary');
  cmsDb=absolute(cmsDb,'CMS database');
  cmsBinary=absolute(cmsBinary,'CMS binary');
  cmsSchemas=absolute(cmsSchemas,'CMS schemas');
  if(queueDb && absolute(queueDb,'Queue database')!==join(home,'work.db'))
    throw Error('Review WorkGraph database must be destination-owned');
  queueDb=join(home,'work.db');
  const cms=new Database(cmsDb,{readonly:true});
  let review;
  try{
    const programs=parseRows(cms);
    if(!programs.length)throw Error('No published command capabilities installed');
    const auth=accountAuth(authDb,{binary:authBinary});
    review=nativeReview({home,auth,authDb,experiences:policies(programs),queue:nativeQueue});
    const packCurrent=({pack,subject,scope,actor})=>{
      const run=spawnSync(cmsBinary,['pack-lifecycle','--operation','current',
        '--pack',pack,'--subject',subject,'--scope',scope,'--actor',actor,
        '--version','','--expected-digest','','--expected-revision','0',
        '--db',cmsDb,'--schemas',cmsSchemas],
        {encoding:'utf8',timeout:10000,maxBuffer:32768});
      if(run.status!==0)return null;
      const record={};
      for(const line of run.stdout.trimEnd().split('\n')){
        const i=line.indexOf('\t');
        if(i<=0)continue;
        const key=line.slice(0,i),value=line.slice(i+1);
        if(['state','digest','pinned_digest'].includes(key))record[key]=value;
      }
      return record;
    };
    const workspaceCurrent=({pack,subject,family})=>{
      const rows=cms.query(`SELECT w.binding_id AS binding,w.release,a.status
        FROM _application_workspace w JOIN application_binding a ON a.id=w.cms_id
        JOIN pack_installation i ON i.id=w.installation_id
        WHERE i.pack=? AND i.installed_for=? AND w.family=? AND a.site_id=?
        AND a.slot_id=? LIMIT 2`).all(pack,subject,family,subject,pack);
      return rows.length===1?rows[0]:null;
    };
    const workerCurrent=spec=>readNativeCommandWorker({queueDb,...spec});
    const binder=reviewedCommandEffects({programs,review,auth,packCurrent,
      workspaceCurrent,workerCurrent,queue:nativeQueue,queueDb});
    return Object.freeze({dispatch:binder.dispatch,catalogue:binder.catalogue,
      close:()=>{review.close();cms.close();}});
  }catch(error){review?.close();cms.close();throw error;}
}

// Installer-only command. No network transport accepts these credentials or
// paths; a browser can only submit a reviewed command, never register a worker.
if(import.meta.main){
  try {
    if(process.argv.length!==3||process.argv[2]!=='--register')
      throw Error('Use the native command installation boundary');
    const pieces=[];let total=0;
    for(;;){
      const buffer=Buffer.alloc(512),n=readSync(0,buffer,0,buffer.length,null);
      if(!n)break;total+=n;
      if(total>2048)throw Error('Native installation request exceeds its bound');
      pieces.push(buffer.subarray(0,n));
    }
    const packet=JSON.parse(Buffer.concat(pieces).toString('utf8'));
    if(!packet||Array.isArray(packet)||typeof packet!=='object'||
       Object.keys(packet).sort().join(',')!=='command,expectedSha256,programId,workspaceBinding')
      throw Error('Native installation requires exact capability identifiers only');
    const home=absolute(process.env.BONFYRE_COMMAND_REVIEW_HOME,'review home');
    const result=registerNativeCommandWorker({
      ...packet,queueDb:join(home,'work.db'),
      cmsDb:process.env.BONFYRE_CMS_DB,
      cmsBinary:process.env.BONFYRE_CMS_BINARY,
      cmsSchemas:process.env.BONFYRE_NATIVE_COMMAND_CMS_SCHEMAS||process.env.BONFYRE_CMS_SCHEMAS,
      binRoot:process.env.BONFYRE_INSTALLED_NATIVE_BIN_DIR
    });
    process.stdout.write(JSON.stringify(result)+'\n');
  }catch(error){
    process.stderr.write(String(error?.message||'Native installation refused').slice(0,240)+'\n');
    process.exitCode=3;
  }
}
