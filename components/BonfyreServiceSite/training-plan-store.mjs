import {mkdirSync,mkdtempSync,writeFileSync,rmSync} from 'node:fs';
import {resolve,join} from 'node:path';
import {execFileSync} from 'node:child_process';
import {createHash} from 'node:crypto';
import {checkPlan} from './training-plan.mjs';
const uuid=/^[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/;
const sha=value=>createHash('sha256').update(JSON.stringify(value)).digest('hex');
const conflict=message=>Object.assign(Error(message),{status:409});
const draftType='training_plan_draft',releaseType='training_plan_release';
export function planStore(home,material,publishing,{binary=resolve(import.meta.dir,'../../cmd/BonfyreCMS/bonfyre-cms')}={}){
  home=resolve(home);mkdirSync(home,{recursive:true,mode:0o700});const db=join(home,'catalogue.db'),schemas=resolve(import.meta.dir,'../../cmd/BonfyreCMS/content-types');
  function native(...args){try{return execFileSync(binary,[...args,'--db',db,'--schemas',schemas],{encoding:'utf8',env:{...process.env,BONFYRE_FACT_DELTAS:''},stdio:'pipe',timeout:10000,maxBuffer:16*1024*1024});}catch{throw Error('The native plan owner refused this operation.');}}
  for(const t of [draftType,releaseType])native('schema','apply',join(schemas,t+'.json'));
  const studio=id=>{const c=material.creators.find(c=>c.id===id);if(!c)throw Error('Unknown studio.');return c;};
  function rows(type,id){studio(id);const result=JSON.parse(native('entry','list',type,'--ns','training-'+id,'--limit','101')).data;if(!Array.isArray(result)||result.length>100)throw Error('This studio has reached the local limit of 100 plan drafts or releases.');return result.map(r=>({id:r.id,...r.attributes}));}
  function write(type,id,body,row,expected){const dir=mkdtempSync(join(home,'plan-')),file=join(dir,'body.json');writeFileSync(file,JSON.stringify(body),{mode:0o600});try{return native('entry',row?'update':'create',type,...(row?[String(row)]:[]),'@'+file,'--ns','training-'+id,...(row?['--if-field','content_sha','--if-value',expected]:[]));}finally{rmSync(dir,{recursive:true,force:true});}}
  function bounded(plan){if(Buffer.byteLength(JSON.stringify(plan))>65536)throw Error('A plan must fit within 64 KB. Reuse routine versions across weeks.');return plan;}
  function checkedDraft(row,id){if(row.namespace!=='training-'+id||!uuid.test(row.draft_key)||!Number.isSafeInteger(row.revision)||row.revision<1||row.content_sha!==sha({revision:row.revision,plan:row.plan}))throw Error('Plan draft changed.');checkPlan(row.plan,{draft:true});if(row.plan.id!==row.draft_key)throw Error('Draft identity changed.');return {id:row.draft_key,revision:row.revision,plan:row.plan,savedAt:row.updated_at};}
  function drafts(id){return rows(draftType,id).map(r=>checkedDraft(r,id));}
  function readDraft(id,key){const row=rows(draftType,id).find(r=>r.draft_key===key);if(!row)throw Object.assign(Error('Plan draft not found.'),{status:404});return checkedDraft(row,id);}
  function normalize(raw,id,key,prior){
    if(!raw||Object.keys(raw).some(k=>!['id','name','description','weeks','routines','source'].includes(k)))throw Error('Unexpected plan fields.');
    if(!Array.isArray(raw.weeks)||raw.weeks.length>52)throw Error('Choose up to 52 weeks.');
    const weeks=raw.weeks.map(w=>({name:w.name,sessions:Array.isArray(w.sessions)?w.sessions.map(s=>({id:s.id,day:s.day,routineId:s.routineId})):null}));
    const needed=[...new Set(weeks.flatMap(w=>w.sessions?.map(s=>s.routineId)??[]))],available=[...(prior?.routines??[]),...studio(id).routines,...publishing.list(id,{publicOnly:true}).map(p=>p.routine)];
    const routines=needed.map(key=>{const r=available.find(r=>r.id===key);if(!r||r.source?.creator!==id)throw Error('Choose a routine version from this studio.');return structuredClone(r);});
    return bounded(checkPlan({id:key,name:raw.name,description:raw.description,weeks,routines},{draft:true}));
  }
  function saveDraft(id,actor,key,input){
    if(!uuid.test(key)||!input||Object.keys(input).some(k=>!['plan','expectedRevision'].includes(k))||!Number.isSafeInteger(input.expectedRevision)||input.expectedRevision<0||input.expectedRevision>=2147483646)throw Error('Invalid plan revision.');
    const all=rows(draftType,id),row=all.find(r=>r.draft_key===key);if(row)checkedDraft(row,id);
    const plan=normalize(input.plan,id,key,row?.plan),revision=input.expectedRevision+1,content_sha=sha({revision,plan});
    if(row?.content_sha===content_sha&&row.editor_actor===actor)return checkedDraft(row,id);
    if((row?.revision??0)!==input.expectedRevision)throw conflict('This plan changed in another tab. Load its saved draft before editing.');if(!row&&all.length>=100)throw Error('The local library holds up to 100 plan drafts.');
    try{write(draftType,id,{draft_key:key,revision,plan,content_sha,editor_actor:actor,status:'draft'},row?.id,row?.content_sha);}catch(error){const after=rows(draftType,id).find(r=>r.draft_key===key);if(after?.content_sha===content_sha&&after.editor_actor===actor)return checkedDraft(after,id);if(after?.revision!==row?.revision)throw conflict('This plan changed in another tab.');throw error;}
    const saved=readDraft(id,key);if(saved.revision!==revision||sha({revision,plan:saved.plan})!==content_sha)throw conflict('This plan changed in another tab.');return saved;
  }
  function checkedRelease(row,id){checkPlan(row.plan);if(row.namespace!=='training-'+id||!uuid.test(row.plan_key)||row.content_sha!==sha(row.plan)||row.plan.source?.creator!==id||row.plan.source.program!==row.plan_key||row.plan.source.version!==String(row.version)||row.plan.id!==`studio:${id}:plan:${row.plan_key}:v${row.version}`)throw Error('Plan release changed.');return {key:row.plan_key,version:row.version,plan:row.plan,publishedAt:row.created_at,releaseId:row.id,status:row.status};}
  function list(id,{publicOnly=false}={}){const latest=new Map();for(const r of rows(releaseType,id)){const p=checkedRelease(r,id);if(!latest.has(p.key)||latest.get(p.key).version<p.version)latest.set(p.key,p);}return [...latest.values()].filter(p=>!publicOnly||p.status==='published').map(p=>{if(!publicOnly)return p;const{releaseId,status,...publicRow}=p;return publicRow;});}
  function publish(id,actor,input){
    if(!input||Object.keys(input).some(k=>!['publicationId','planId','expectedVersion','draftId','draftRevision'].includes(k))||!['publicationId','planId','draftId'].every(k=>uuid.test(input[k]))||!Number.isSafeInteger(input.expectedVersion)||input.expectedVersion<0||input.expectedVersion>=100||!Number.isSafeInteger(input.draftRevision)||input.draftRevision<1)throw Error('Invalid plan publication.');
    const all=rows(releaseType,id),submission=sha({...input,actor}),retry=all.find(r=>r.publication_key===input.publicationId);if(retry){if(retry.submission_sha!==submission)throw conflict('This publication ID names different content.');return checkedRelease(retry,id);}
    const head=Math.max(0,...all.filter(r=>r.plan_key===input.planId).map(r=>r.version));if(head!==input.expectedVersion)throw conflict('A newer plan release exists. Refresh before publishing.');if(all.length>=100)throw Error('The local catalogue holds up to 100 plan releases.');
    const saved=readDraft(id,input.draftId);if(saved.revision!==input.draftRevision)throw conflict('The plan changed after review. Review the saved draft again.');
    const version=head+1,plan=structuredClone(saved.plan);plan.id=`studio:${id}:plan:${input.planId}:v${version}`;plan.source={creator:id,program:input.planId,version:String(version)};
    for(const w of plan.weeks)for(const s of w.sessions)s.id='session:'+sha([input.planId,s.id]).slice(0,40);bounded(checkPlan(plan));
    const body={release_key:`${input.planId}:${version}`,publication_key:input.publicationId,plan_key:input.planId,version,plan,content_sha:sha(plan),submission_sha:submission,origin_draft:input.draftId,origin_revision:input.draftRevision,publisher_actor:actor,status:'published'};
    try{write(releaseType,id,body);}catch(error){const after=rows(releaseType,id),same=after.find(r=>r.publication_key===input.publicationId);if(same?.submission_sha===submission)return checkedRelease(same,id);if(after.some(r=>r.release_key===body.release_key))throw conflict('Another publication saved this plan version.');throw error;}
    const result=rows(releaseType,id).find(r=>r.publication_key===input.publicationId);if(!result)throw Error('Plan publication could not be verified.');return checkedRelease(result,id);
  }
  function withdraw(id,releaseId){const row=rows(releaseType,id).find(r=>r.id===releaseId);if(!row)throw Error('Plan release not found.');checkedRelease(row,id);write(releaseType,id,{status:'withdrawn'},row.id,row.content_sha);const result=rows(releaseType,id).find(r=>r.id===releaseId);if(result?.status!=='withdrawn')throw Error('Plan withdrawal could not be verified.');return {releaseId,status:'withdrawn'};}
  function choices(id){return [...studio(id).routines,...publishing.list(id,{publicOnly:true}).map(p=>p.routine)];}
  return {list,drafts,readDraft,saveDraft,publish,withdraw,choices};
}
