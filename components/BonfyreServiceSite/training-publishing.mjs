import {mkdirSync,mkdtempSync,writeFileSync,rmSync} from 'node:fs';
import {resolve,join} from 'node:path';
import {execFileSync} from 'node:child_process';
import {createHash} from 'node:crypto';
import {requestRoutine} from './training-draft.mjs';
import {checkRoutine} from './training-model.mjs';
const uuid=/^[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/;
const digest=value=>createHash('sha256').update(JSON.stringify(value)).digest('hex');
const conflict=message=>Object.assign(Error(message),{status:409});
const type='training_program_release';
export const programInstitution=id=>{const value=`training-${id}-programs`;if(!/^[a-z0-9-]{1,63}$/.test(value))throw Error('Studio ID is too long for a program grant.');return value;};
export function publicProgramRoutine(raw,studio,key,version,title){
  const routine=requestRoutine(raw,key,studio);routine.id=`studio:${studio}:${key}:v${version}`;routine.name=title;routine.source={creator:studio,program:key,version:String(version)};
  routine.exercises.forEach(e=>{e.id='exercise:'+digest([studio,key,e.id]).slice(0,40);e.sets.forEach((s,j)=>{s.id=`set:${j+1}`;});});
  checkRoutine(routine);if(Buffer.byteLength(JSON.stringify(routine))>65536)throw Error('A published routine must fit within 64 KB.');return routine;
}
export function publishingStore(home,material,requests,{binary=resolve(import.meta.dir,'../../cmd/BonfyreCMS/bonfyre-cms')}={}){
  home=resolve(home);mkdirSync(home,{recursive:true,mode:0o700});const db=join(home,'catalogue.db');
  const schemas=resolve(import.meta.dir,'../../cmd/BonfyreCMS/content-types');
  const env={...process.env,BONFYRE_FACT_DELTAS:''};
  function native(...args){try{return execFileSync(binary,[...args,'--db',db,'--schemas',schemas],{encoding:'utf8',env,stdio:['pipe','pipe','pipe'],timeout:10000,maxBuffer:16*1024*1024});}catch{throw Error('The native content owner refused this operation.');}}
  native('schema','apply',join(schemas,type+'.json'));
  native('schema','apply',join(schemas,'training_program_draft.json'));
  const namespace=studio=>{if(!material.creators.some(c=>c.id===studio))throw Error('Unknown studio.');return 'training-'+studio;};
  function writeBody(body,perform){const scratch=mkdtempSync(join(home,'content-')),file=join(scratch,'body.json');writeFileSync(file,JSON.stringify(body),{mode:0o600});try{return perform('@'+file);}finally{rmSync(scratch,{recursive:true,force:true});}}
  function draftRecords(studio){const rows=JSON.parse(native('entry','list','training_program_draft','--ns',namespace(studio),'--limit','101')).data;if(!Array.isArray(rows)||rows.length>100)throw Error('This studio has reached the local library limit of 100 drafts.');return rows.map(row=>({id:row.id,...row.attributes}));}
  function checkedDraft(row,studio){
    if(row.namespace!==namespace(studio)||!uuid.test(row.draft_key)||!Number.isSafeInteger(row.revision)||row.revision<1||row.content_sha!==digest({revision:row.revision,routine:row.routine}))throw Error('Saved program draft changed.');
    const routine=requestRoutine(row.routine,row.draft_key,studio,{draft:true});
    if(JSON.stringify(routine)!==JSON.stringify(row.routine))throw Error('Invalid saved program draft.');
    return {id:row.draft_key,revision:row.revision,routine,savedAt:row.updated_at};
  }
  function drafts(studio){return draftRecords(studio).map(row=>checkedDraft(row,studio));}
  function readDraft(studio,id){if(!uuid.test(id))throw Error('Invalid draft identity.');const row=draftRecords(studio).find(r=>r.draft_key===id);if(!row)throw Object.assign(Error('Program draft not found.'),{status:404});return checkedDraft(row,studio);}
  function saveDraft(studio,actor,id,input){
    if(!uuid.test(id)||!input||Array.isArray(input)||Object.keys(input).some(k=>!['routine','expectedRevision'].includes(k))||!Number.isSafeInteger(input.expectedRevision)||input.expectedRevision<0||input.expectedRevision>=2147483646)throw Error('Invalid draft or revision.');
    const routine=requestRoutine(input.routine,id,studio,{draft:true});if(Buffer.byteLength(JSON.stringify(routine))>65536)throw Error('A program draft must fit within 64 KB.');
    const rows=draftRecords(studio),row=rows.find(r=>r.draft_key===id),revision=input.expectedRevision+1,content_sha=digest({revision,routine});
    if(row){checkedDraft(row,studio);if(row.revision===revision&&row.content_sha===content_sha&&row.editor_actor===actor)return checkedDraft(row,studio);if(row.revision!==input.expectedRevision)throw conflict('Another tab saved this draft. Load its saved version before editing.');}
    else if(input.expectedRevision!==0)throw conflict('Program draft no longer exists.');
    else if(rows.length>=100)throw Error('This studio has reached the local library limit of 100 drafts.');
    const body={draft_key:id,revision,routine,content_sha,editor_actor:actor,status:'draft'};
    try{writeBody(body,file=>row?native('entry','update','training_program_draft',String(row.id),file,'--ns',namespace(studio),'--if-field','content_sha','--if-value',row.content_sha):native('entry','create','training_program_draft',file,'--ns',namespace(studio)));}
    catch(error){const after=draftRecords(studio).find(r=>r.draft_key===id);if(after?.content_sha===content_sha&&after.editor_actor===actor)return checkedDraft(after,studio);if(after&&(!row||after.content_sha!==row.content_sha))throw conflict('Another tab saved this draft. Load its saved version before editing.');throw error;}
    const saved=readDraft(studio,id);if(saved.revision!==revision||digest({revision,routine:saved.routine})!==content_sha)throw conflict('Another tab saved this draft. Load its saved version before editing.');return saved;
  }
  function records(studio){const rows=JSON.parse(native('entry','list',type,'--ns',namespace(studio),'--limit','101')).data;if(!Array.isArray(rows)||rows.length>100)throw Error('This studio has reached the local catalogue limit of 100 releases.');return rows.map(row=>({id:row.id,...row.attributes}));}
  function checked(row,studio){
    if(row.namespace!==namespace(studio)||!uuid.test(row.program_key)||!Number.isSafeInteger(row.version)||row.version<1)throw Error('Invalid program release.');
    if(row.content_sha!==digest({title:row.title,summary:row.summary,routine:row.routine}))throw Error('Published program content changed.');
    checkRoutine(row.routine);if(row.routine.source.creator!==studio||row.routine.source.program!==row.program_key||row.routine.source.version!==String(row.version)||row.routine.id!==`studio:${studio}:${row.program_key}:v${row.version}`)throw Error('Program version does not match.');
    return row;
  }
  function publicRow(row,studio){checked(row,studio);return {key:row.program_key,version:row.version,title:row.title,description:row.summary,routine:row.routine,publishedAt:row.created_at};}
  function list(studio,{publicOnly=false}={}){const latest=new Map();for(const row of records(studio)){checked(row,studio);if(!latest.has(row.program_key)||latest.get(row.program_key).version<row.version)latest.set(row.program_key,row);}return [...latest.values()].filter(r=>!publicOnly||r.status==='published').map(r=>({...publicRow(r,studio),...(publicOnly?{}:{releaseId:r.id,status:r.status})}));}
  function publish(studio,actor,input){
    if(!input||typeof input!=='object'||Array.isArray(input)||Object.keys(input).some(k=>!['publicationId','programId','expectedVersion','requestId','draftId','draftRevision','title','description'].includes(k)))throw Error('Unexpected publication fields.');
    const fromDraft=Object.hasOwn(input,'draftId');
    if(fromDraft?(Object.hasOwn(input,'requestId')||!uuid.test(input.draftId)||!Number.isSafeInteger(input.draftRevision)||input.draftRevision<1):(!uuid.test(input.requestId)||Object.hasOwn(input,'draftRevision')))throw Error('Choose one delivered request or saved program draft.');
    if(!uuid.test(input.publicationId)||!uuid.test(input.programId)||!Number.isSafeInteger(input.expectedVersion)||input.expectedVersion<0||input.expectedVersion>=100)throw Error('Invalid publication identity or version.');
    for(const [key,max] of [['title',160],['description',1000]])if(typeof input[key]!=='string'||!input[key].trim()||input[key].length>max||(key==='title'?/[\x00-\x1f]/:/[\x00-\x08\x0b\x0c\x0e-\x1f]/).test(input[key]))throw Error('Enter a title and a short public description.');
    const rows=records(studio),submission=digest({...input,actor}),retry=rows.find(r=>r.publication_key===input.publicationId);
    if(retry){if(retry.submission_sha!==submission)throw conflict('This publication ID already names different content.');return {...publicRow(retry,studio),releaseId:retry.id,status:retry.status};}
    const head=Math.max(0,...rows.filter(r=>r.program_key===input.programId).map(r=>r.version));if(head!==input.expectedVersion)throw conflict('A newer program version exists. Refresh the library before publishing.');
    if(rows.length>=100)throw Error('This studio has reached the local catalogue limit of 100 releases.');
    let raw,provenance;
    if(fromDraft){const saved=readDraft(studio,input.draftId);if(saved.revision!==input.draftRevision)throw conflict('This draft changed after review. Open the saved draft and review it again.');raw=saved.routine;provenance={origin_draft:input.draftId,origin_draft_revision:input.draftRevision};}
    else{const delivered=requests.creatorRead(input.requestId,studio);if(delivered.state!=='complete'||!delivered.delivery)throw Error('Deliver the routine before publishing it to the studio.');raw=delivered.delivery.routine;provenance={origin_request:input.requestId,origin_delivery_sha:delivered.delivery.sha256};}
    const version=head+1,routine=publicProgramRoutine(raw,studio,input.programId,version,input.title),content={title:input.title,summary:input.description,routine};
    const body={release_key:`${input.programId}:${version}`,publication_key:input.publicationId,program_key:input.programId,version,...content,content_sha:digest(content),submission_sha:submission,...provenance,publisher_actor:actor,status:'published'};
    const scratch=mkdtempSync(join(home,'publication-')),file=join(scratch,'release.json');writeFileSync(file,JSON.stringify(body),{mode:0o600});
    try{native('entry','create',type,'--ns',namespace(studio),'@'+file);}catch(error){const after=records(studio),same=after.find(r=>r.publication_key===input.publicationId);if(same?.submission_sha===submission)return {...publicRow(same,studio),releaseId:same.id,status:same.status};if(after.some(r=>r.release_key===body.release_key))throw conflict('Another publication saved this version. Refresh the library.');throw error;}finally{rmSync(scratch,{recursive:true,force:true});}
    const stored=records(studio).find(r=>r.publication_key===input.publicationId);if(!stored||stored.content_sha!==body.content_sha)throw Error('Publication could not be verified.');return {...publicRow(stored,studio),releaseId:stored.id,status:stored.status};
  }
  function withdraw(studio,releaseId){if(!Number.isSafeInteger(releaseId)||releaseId<1)throw Error('Invalid release.');const row=records(studio).find(r=>r.id===releaseId);if(!row)throw Error('Release not found.');checked(row,studio);native('entry','update',type,String(releaseId),JSON.stringify({status:'withdrawn'}),'--ns',namespace(studio));const stored=records(studio).find(r=>r.id===releaseId);if(stored?.status!=='withdrawn')throw Error('Withdrawal could not be verified.');return {releaseId,status:stored.status};}
  function release(studio,key,version){const row=records(studio).find(r=>r.program_key===key&&r.version===version);if(!row||row.status!=='published')throw Error('Published routine version not found.');return publicRow(row,studio);}
  return {list,publish,withdraw,drafts,readDraft,saveDraft,release};
}
