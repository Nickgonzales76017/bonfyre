// Read-only view of the creator app's existing published CMS releases.
// Never a second content owner, subscription grant, or member-history reader.
import {createHash} from 'node:crypto';
import {checkRoutine} from '../../components/BonfyreServiceSite/training-model.mjs';

const sha=value=>createHash('sha256').update(JSON.stringify(value)).digest('hex');
const isObject=v=>v!==null&&typeof v==='object'&&!Array.isArray(v);
function fields(value,allowed){
  if(!isObject(value)||Object.keys(value).some(k=>!allowed.includes(k)))
    throw Error('Published training content failed its public-shape check.');
}
function publicRoutine(value,studio,program,version){
  fields(value,['id','name','source','exercises']);
  fields(value.source,['creator','program','version']);
  if(value.id!==`studio:${studio}:${program}:v${version}`||
     value.source.creator!==studio||value.source.program!==program||
     value.source.version!==String(version))
    throw Error('Published program identity does not match the requested studio.');
  if(!Array.isArray(value.exercises))throw Error('Invalid published exercises.');
  for(const e of value.exercises){
    fields(e,['id','name','kind','restSeconds','superset','progression','sets']);
    if(e.progression!==null)fields(e.progression,
      ['kind','incrementKg','incrementSeconds','minReps','maxReps','deloadAfter','deloadPercent']);
    if(!Array.isArray(e.sets))throw Error('Invalid published sets.');
    for(const s of e.sets)fields(s,
      ['id','type','loadKg','reps','seconds','distanceM','rir','rpe','done']);
  }
  checkRoutine(value);
  return structuredClone(value);
}
export function publicTrainingReleases(rows,studio){
  if(!/^[a-z0-9-]{1,64}$/.test(studio)||!Array.isArray(rows)||rows.length>100)
    throw Error('Invalid studio or public release inventory.');
  const latest=new Map(),identities=new Set();
  for(const record of rows){
    const a=record?.attributes;
    if(!isObject(a)||a.namespace!==`training-${studio}`||
       !/^[a-f0-9-]{36}$/.test(a.program_key)||
       !Number.isSafeInteger(a.version)||a.version<1||
       a.release_key!==`${a.program_key}:${a.version}`||
       !['published','withdrawn'].includes(a.status)||
       typeof a.title!=='string'||typeof a.summary!=='string'||
       typeof a.content_sha!=='string')
      throw Error('Invalid source training publication.');
    if(identities.has(a.release_key))throw Error('Duplicate source training release.');
    identities.add(a.release_key);
    if(sha({title:a.title,summary:a.summary,routine:a.routine})!==a.content_sha)
      throw Error('Published program changed after native CMS approval.');
    const routine=publicRoutine(a.routine,studio,a.program_key,a.version);
    const view={key:a.program_key,version:a.version,title:a.title,
      description:a.summary,routine,publishedAt:a.created_at,
      contentSha256:a.content_sha};
    const previous=latest.get(a.program_key);
    if(!previous||previous.version<a.version)latest.set(a.program_key,{...view,status:a.status});
  }
  // A withdrawn newest release cannot expose an older still-published version.
  return [...latest.values()].filter(a=>a.status==='published')
    .map(({status,...view})=>view).sort((a,b)=>a.key.localeCompare(b.key));
}
