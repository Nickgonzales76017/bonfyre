import { Database } from 'bun:sqlite';
import { createHash } from 'node:crypto';
import { readFileSync, statSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { spawnSync } from 'node:child_process';
import { accountAuth } from '../../components/BonfyreServiceSite/account-auth.mjs';
import { queue as nativeQueue } from '../../components/BonfyreControlPlane/obligation-work.mjs';
import { nativeReview } from './review.mjs';
import { reviewedCommandEffects } from './command_effects.mjs';

// The caller supplies only IDs and a session. Configuration belongs to the
// installed owner. CMS programs, native Auth and Queue DBs remain authoritative.
const hash=/^[a-f0-9]{64}$/;
const absolute=(value,name)=>{
  if(typeof value!=='string'||!value.startsWith('/'))throw Error('Owner '+name+' required');
  return resolve(value);
};
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
    const workerCurrent=({command,program,operator,subject,packDigest,workspaceBinding,artifactKind})=>{
      let db;
      try {
        db=new Database(queueDb,{readonly:true});
        const rows=db.query(`SELECT operator_id,program_id,pack_digest,
          workspace_binding,worker_binary,worker_sha256,state,artifact_kind
          FROM _native_command_executor WHERE operator_id=? AND program_id=?
          AND pack_digest=? AND workspace_binding=? LIMIT 2`).all(
            operator,program,packDigest,workspaceBinding);
        if(rows.length!==1)return null;
        const row=rows[0];
        if(row.state!=='ready'||row.artifact_kind!==artifactKind||
           row.operator_id!==operator||row.program_id!==program||
           row.workspace_binding!==workspaceBinding||row.pack_digest!==packDigest||
           !hash.test(row.worker_sha256)||typeof row.worker_binary!=='string'||
           !row.worker_binary.startsWith('/'))return null;
        const st=statSync(row.worker_binary);
        if(!st.isFile()||(st.mode&0o111)===0||st.size>512*1024*1024)return null;
        const current=createHash('sha256').update(readFileSync(row.worker_binary)).digest('hex');
        if(current!==row.worker_sha256)return null;
        return {state:'ready',operator_id:operator,program_id:program,
          pack_digest:packDigest,workspace_binding:workspaceBinding,
          binary_sha256:current};
      }catch{return null;}finally{db?.close();}
    };
    const binder=reviewedCommandEffects({programs,review,auth,packCurrent,
      workspaceCurrent,workerCurrent,queue:nativeQueue,queueDb});
    return Object.freeze({dispatch:binder.dispatch,catalogue:binder.catalogue,
      close:()=>{review.close();cms.close();}});
  }catch(error){review?.close();cms.close();throw error;}
}
