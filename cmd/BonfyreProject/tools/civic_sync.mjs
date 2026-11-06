#!/usr/bin/env bun
// Existing BonfyreProject ForeignTwin ingestion -> typed native CMS. No new DB.
// Published SourceCollection selects civic pairs; one run reconciles all exact
// file-number joins with bounded sources, conservative conflicts and one
// transactional native CMS import-jsonl. No media byte downloads or publication.
import { createHash, randomBytes } from 'node:crypto';
import { readFileSync, writeFileSync, mkdirSync, mkdtempSync, rmSync, openSync, readSync, closeSync, statSync, fstatSync, constants } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, isAbsolute } from 'node:path';
import { spawnSync } from 'node:child_process';
import { robotsAllows } from '../../BonfyreSourceFrontier/tools/robots_policy.mjs';
import { observePublic } from '../../BonfyreSourceFrontier/tools/foreign_observation.mjs';
import { parseLegistarAgendaHtml } from '../../../integrations/mcp/project_native.mjs';
const sha = b => createHash('sha256').update(b).digest('hex');
const FILE = /\b\d{2}-\d{4}[A-Z]\b/g;
const trim = s => String(s || '').replace(/\s+/g, ' ').trim();
const attrs = tag => {
  const a = {};
  for(const m of tag.matchAll(/([a-z][a-z0-9-]*)\s*=\s*(?:"([^"]*)"|'([^']*)')/gi))a[m[1].toLowerCase()]=m[2]??m[3];
  return a;
};
const unescapeHtml = s => String(s||'').replace(/&(#(?:x[0-9a-f]+|\d+)|amp|lt|gt|quot|apos|nbsp);/gi,(m,x)=>{
  const named = {amp:'&',lt:'<',gt:'>',quot:'"',apos:"'",nbsp:' '};
  if(named[x.toLowerCase()])return named[x.toLowerCase()];
  if(!x.startsWith('#'))return m;
  const code=x[1].toLowerCase()==='x'?parseInt(x.slice(2),16):parseInt(x.slice(1),10);
  return Number.isInteger(code)&&code>0&&code<=0x10ffff&&!(code>=0xd800&&code<=0xdfff)?String.fromCodePoint(code):m;
});
const safeUrl = (input,allow) => {
  const url=new URL(input);
  if(url.protocol!=='https:'||url.username||url.password||url.port||url.hash||!allow.test(url.hostname))
    throw Error('refused unapproved provider URL: '+url.hostname);
  return url;
};
export function parseSwagitIndex(html, videoId, origin='https://dallastx.new.swagit.com') {
  if(!/^\d{1,10}$/.test(String(videoId)))throw Error('invalid video identity');
  const byStart=new Map(), conflicts=[];
  for(const m of String(html).matchAll(/<a\b[^>]{0,12000}>/gi)){
    const a=attrs(m[0]);
    if(!String(a.class||'').split(/\s+/).includes('playerControl'))continue;
    if(!/^\d{1,8}$/.test(a['data-ts']??''))continue;
    const seconds=Number(a['data-ts']);
    const end=/^\d{1,8}$/.test(a['data-end-ts']??'')?Number(a['data-end-ts']):null;
    const url=new URL(a.href||'',origin);
    if(url.origin!==origin||url.pathname!=='/play/'+videoId+'/'+seconds||url.search||url.hash){
      conflicts.push({state:'invalid_official_chapter_link',second:seconds});continue;
    }
    const title=trim(unescapeHtml(a['data-title']));
    if(!title)continue;
    const matches=[...new Set(title.match(FILE)||[])];
    const row={start_seconds:seconds,end_seconds:end!==null&&end>seconds?end:null,
      title,href:url.href,file:matches.length===1?matches[0]:null,
      identity_state:matches.length>1?'ambiguous_file_numbers':matches.length===1?'exact_file_number':'unindexed_section'};
    const old=byStart.get(seconds);
    if(old){if(old.title!==title||old.href!==row.href)conflicts.push({state:'conflicting_chapter_start',second:seconds});}
    else byStart.set(seconds,row);
  }
  if(byStart.size>500)throw Error('Swagit chapter bound exceeded');
  return {chapters:[...byStart.values()].sort((a,b)=>a.start_seconds-b.start_seconds),conflicts};
}
export function discoverCivicPairs(collections,sources,{limit=12,dueSources=null}={}){
  if(!Number.isSafeInteger(limit)||limit<1||limit>64)throw Error('meeting budget must be 1..64');
  const bySource=new Map(sources.map(s=>[s.source,s]));
  const pairs=[],held=[],handled=new Map();
  let deferred=0;
  for(const row of collections){
    let selector;try{selector=typeof row.source_selector==='string'?JSON.parse(row.source_selector):row.source_selector;}catch{held.push({collection:row.collection_id,reason:'invalid_source_selector'});continue;}
    const refs=selector?.source_refs;
    if(!Array.isArray(refs)||refs.length>64||!refs.length)continue;
    const selected=refs.map(s=>bySource.get(s)).filter(Boolean);
    const agenda=selected.filter(s=>s.family==='civic-meeting');
    const video=selected.filter(s=>s.family==='civic-meeting-video');
    if(!agenda.length&&!video.length)continue;
    if(agenda.length!==1||video.length!==1){held.push({collection:row.collection_id,reason:'unpaired_or_ambiguous_civic_source'});continue;}
    if(dueSources && !dueSources.has(agenda[0].source) && !dueSources.has(video[0].source))continue;
    try{
      const a=safeUrl(agenda[0].url,/(?:^|\.)legistar\.com$/i);
      const v=safeUrl(video[0].url,/\.new\.swagit\.com$/i);
      if(!/\/MeetingDetail\.aspx$/i.test(a.pathname)||!/^\d+$/.test(a.searchParams.get('ID')||'')||!/^\/videos\/\d+$/.test(v.pathname))throw Error('invalid provider resource coordinate');
      const stamp=agenda[0].source.match(/\d{8}/)?.[0];
      if(!stamp)throw Error('missing stable meeting date coordinate');
      const key=agenda[0].source+'\0'+video[0].source;
      if(handled.has(key)){
        const prior=handled.get(key);
        if(prior)prior.collections.push(row.collection_id);
        continue;
      }
      if(pairs.length>=limit){
        deferred++;handled.set(key,null);
        if(held.length<128)held.push({collection:row.collection_id,reason:'budget_deferred',agenda:agenda[0].source,video:video[0].source});
        continue;
      }
      const pair={collection:row.collection_id,collections:[row.collection_id],agenda:agenda[0],video:video[0],
        event_id:Number(a.searchParams.get('ID')),video_id:v.pathname.split('/')[2],stamp};
      pairs.push(pair);handled.set(key,pair);
    }catch(e){held.push({collection:row.collection_id,reason:'invalid_provider_pair',detail:e.message});}
  }
  return {pairs,held,deferred,considered:collections.length};
}
export function reconcileMeeting(pair,agenda,chapters,existing,{observedAt,agendaDigest,videoDigest}={}){
  const byAgenda=new Map(),byExisting=new Map();
  const review=[],rows=[];
  for(const a of agenda){const file=trim(a.file).match(/\b\d{2}-\d{4}[A-Z]\b/)?.[0];if(!file){review.push({reason:'agenda_without_file_number',agenda_number:a.agenda_number});continue;}
    const group=byAgenda.get(file)||[];group.push(a);byAgenda.set(file,group);}
  for(const e of existing){const file=String(e.occurrence_id||'').match(/\b\d{2}-\d{4}[A-Z]\b/)?.[0];
    if(!file)continue;const key=e.source_ref+'\0'+file;
    const group=byExisting.get(key)||[];group.push(e);byExisting.set(key,group);}
  const chapterFiles=new Map();
  for(const c of chapters){if(!c.file)continue;const group=chapterFiles.get(c.file)||[];group.push(c);chapterFiles.set(c.file,group);}
  let unchanged=0,matched=0,changed=0,incomplete=0,missing=0;
  // Source-owned material, not whole-page digests or observation timestamps, decides reuse.
  // A changed row is a reviewable proposal, never an automatic overwrite of an edited record.
  const fields=['source_twin','source_ref','canonical_href','title','summary','rights_basis',
    'retention','visibility','assertion_state','cannot_infer','status'];
  function compare(previous,next,kind,file){
    const unknown=fields.filter(field=>typeof previous[field]!=='string');
    const delta=fields.filter(field=>typeof previous[field]==='string'&&previous[field]!==next[field]);
    if(!unknown.length&&!delta.length){unchanged++;return;}
    if(delta.length)changed++;else incomplete++;
    const timeChanged=kind==='video'&&delta.includes('canonical_href');
    const reason=timeChanged?'video_index_time_changed':delta.length?kind+'_metadata_changed':'persisted_'+kind+'_metadata_incomplete';
    const proposed=Object.fromEntries([...delta,...unknown].map(field=>[field,next[field]]));
    review.push({reason,file,occurrence_id:previous.occurrence_id,source_ref:next.source_ref,
      previous_receipt_ref:previous.receipt_ref??null,observed_receipt_ref:next.receipt_ref,
      changed_fields:delta,unverified_fields:unknown,
      expected_fields:Object.fromEntries(delta.map(field=>[field,previous[field]])),proposed_fields:proposed,
      ...(timeChanged?{previous_href:previous.canonical_href,current_href:next.canonical_href}:{}),
      requires_review:true,automatic_update:false});
  }
  const nameRoot=pair.agenda.source.replace(/-legistar$/,'');
  const videoStem=nameRoot.includes('-council-')?nameRoot.replace('-council-','-civic-video-'):'civic-video-'+nameRoot;
  const agendaStem=nameRoot.includes('-council-')?nameRoot.replace('-council-','-civic-'):'civic-'+nameRoot;
  for(const [file,items] of byAgenda){
    if(items.length!==1){review.push({reason:'duplicate_agenda_file',file,count:items.length});continue;}
    const key=pair.agenda.source+'\0'+file,found=byExisting.get(key)||[];
    if(found.length>1){review.push({reason:'duplicate_persisted_agenda',file});continue;}
    const candidate={occurrence_id:agendaStem+'-'+file,
      source_twin:pair.agenda.twin||pair.agenda.source,source_ref:pair.agenda.source,
      canonical_href:pair.agenda.url,title:(file+' '+trim(items[0].title)).slice(0,2000),
      summary:('Official agenda item '+String(items[0].agenda_number||'')+'; item action and votes separately unverified.').slice(0,3500),
      observed_at:observedAt,receipt_ref:'foreign:sha256:'+agendaDigest,
      rights_basis:'official_public_agenda_reference_only',retention:'metadata_only',visibility:'public',
      assertion_state:'official_agenda_item_observed_action_unverified',cannot_infer:'Agenda item not a vote, grant award, parcel, or cleared media license.',status:'published'};
    if(!found.length)rows.push(candidate);else compare(found[0],candidate,'agenda',file);
  }
  for(const [file,items] of chapterFiles){
    if(items.length!==1){review.push({reason:'duplicate_chapter_file',file,count:items.length});continue;}
    if((byAgenda.get(file)||[]).length!==1){review.push({reason:'video_file_without_unique_agenda',file,start_seconds:items[0].start_seconds});continue;}
    matched++;
    const c=items[0],key=pair.video.source+'\0'+file,found=byExisting.get(key)||[];
    if(found.length>1){review.push({reason:'duplicate_persisted_video',file});continue;}
    const candidate={occurrence_id:videoStem+'-'+file,source_twin:pair.video.twin||pair.video.source,
      source_ref:pair.video.source,canonical_href:c.href,title:('Swagit index · '+file+' · '+c.title).slice(0,2000),
      summary:('Official index chapter '+file+'; start '+c.start_seconds+'s'+(c.end_seconds===null?'':', end '+c.end_seconds+'s')+'; meeting '+pair.video.url+'; source agenda '+pair.agenda.url+'. Chapter is not statement-level verification.').slice(0,3500),
      observed_at:observedAt,receipt_ref:'foreign:sha256:'+videoDigest,
      rights_basis:'swagit_public_player_reference_only_no_download_or_derivative_license',
      retention:'metadata_only',visibility:'public',assertion_state:'official_media_index_observed_transcript_not_verified',
      cannot_infer:'Video index start is not an approved action, transcript, media ownership or clip license.',status:'published'};
    if(!found.length)rows.push(candidate);else compare(found[0],candidate,'video',file);
  }
  const absent=new Set();
  for(const [source,index,kind] of [[pair.agenda.source,byAgenda,'agenda'],[pair.video.source,chapterFiles,'video_index']]){
    for(const e of existing.filter(e=>e.source_ref===source)){
      const file=String(e.occurrence_id||'').match(/\b\d{2}-\d{4}[A-Z]\b/)?.[0],key=source+'\0'+file;
      if(!file||index.has(file)||absent.has(key))continue;
      absent.add(key);missing++;
      review.push({reason:'previous_'+kind+'_no_longer_advertised',file,occurrence_id:e.occurrence_id,
        source_ref:source,previous_href:e.canonical_href,previous_receipt_ref:e.receipt_ref??null,
        observed_receipt_ref:'foreign:sha256:'+(kind==='agenda'?agendaDigest:videoDigest),
        requires_review:true,automatic_delete:false});
    }
  }
  return {collection:pair.collection,agenda_items:agenda.length,index_entries:chapters.length,
    file_index_entries:[...chapterFiles.values()].reduce((n,v)=>n+v.length,0),exact_joins:matched,
    unindexed_chapters:chapters.filter(c=>!c.file).map(c=>({start_seconds:c.start_seconds,title:c.title.slice(0,140)})),
    unchanged,changed,incomplete,missing,new_rows:rows,review};
}
const maxRead = async (reply,limit) => {
  const len=Number(reply.headers.get('content-length'));
  if(Number.isFinite(len)&&len>limit)throw Error('source response exceeds byte budget');
  if(!reply.body)throw Error('source returned no byte stream');
  const chunks=[];let bytes=0;
  for await(const chunk of reply.body){bytes+=chunk.length;if(bytes>limit)throw Error('source streamed beyond byte budget');chunks.push(Buffer.from(chunk));}
  return Buffer.concat(chunks,bytes);
};
async function observedRead(source,limit,{request=fetch}={}){
  const u=new URL(source.url),policy=new URL('/robots.txt',u.origin);
  const robot=await request(policy.href,{redirect:'manual',signal:AbortSignal.timeout(15000),headers:{'User-Agent':'BonfyreProject/1.0'}});
  if(robot.status!==200&&robot.status!==404)throw Error('robots policy unavailable: '+robot.status);
  if(robot.status===200){const text=(await maxRead(robot,256*1024)).toString('utf8');if(!robotsAllows(text,'BonfyreProject/1.0',u.href))throw Error('robots policy excludes this source');}
  const response=await request(u.href,{redirect:'manual',signal:AbortSignal.timeout(30000),headers:{Accept:'text/html','User-Agent':'BonfyreProject/1.0'}});
  if(response.status!==200)throw Error('official provider HTTP '+response.status);
  const bytes=await maxRead(response,limit);
  return {bytes,digest:sha(bytes),status:response.status};
}
function opt(argv){const r={apply:false,limit:12,namespace:'root',state:'',db:'',schemas:'',cms:'',project:'',foreign:'',dueSources:[]};
  for(let i=0;i<argv.length;i++){const name=argv[i];if(name==='--apply'){r.apply=true;continue;}
    if(name==='--due-source'){
      const id=String(argv[++i]||'');
      if(!/^[A-Za-z0-9._:-]{1,128}$/.test(id)||r.dueSources.length>=64)throw Error('invalid bounded due source');
      r.dueSources.push(id);continue;}
    const field={'--limit':'limit','--ns':'namespace','--state':'state','--cms-db':'db','--schemas':'schemas','--cms-bin':'cms','--project-bin':'project','--foreign-root':'foreign'}[name];
    if(!field||!argv[i+1])throw Error('unknown or missing option '+name);r[field]=argv[++i];}
  r.limit=Number(r.limit);if(!Number.isSafeInteger(r.limit)||r.limit<1||r.limit>64)throw Error('invalid bounded --limit');
  for(const key of ['db','schemas','cms','project','state'])if(!r[key]||!isAbsolute(r[key]))throw Error('absolute '+key+' required');
  if(r.apply&&(!r.foreign||!isAbsolute(r.foreign)))throw Error('apply requires an absolute ForeignTwin root');
  if(!/^[A-Za-z0-9._-]{1,64}$/.test(r.namespace))throw Error('invalid CMS namespace');
  return r;
}
function inspectDb(db,ns){const collections=db.query("SELECT collection_id,subject_address,source_selector FROM source_collection WHERE namespace=? AND status='published' ORDER BY collection_id").all(ns);
  const sources=db.query("SELECT source,family,url,label,COALESCE(twin,'') AS twin FROM internet_source WHERE namespace=? AND status='published' AND family IN ('civic-meeting','civic-meeting-video')").all(ns);
  const releases=db.query("SELECT occurrence_id,source_ref,canonical_href,receipt_ref,title,summary,source_twin,rights_basis,retention,visibility,assertion_state,cannot_infer,status FROM publication_occurrence WHERE namespace=? AND source_ref IN (SELECT source FROM internet_source WHERE namespace=? AND family IN ('civic-meeting','civic-meeting-video'))").all(ns,ns);
  return {collections,sources,releases};
}
// Accept only an actual native observation over the exact source and bytes.
// HTTP 304 is a revalidation of the previous representation, not new content.
export function verifyNativeObservation(receipt,code,source,digest){
  const twin=String(source.twin||source.source);
  if(receipt?.schema!=='bonfyre-foreign-observation.v1'||
     receipt.twin_id!==twin||receipt.source_url!==source.url||
     receipt.observe_right!=='public'||receipt.policy_decision!=='allow'||
     !/^[a-f0-9]{64}$/.test(digest)||receipt.content_sha256!==digest)
    throw Error('native ForeignTwin observation does not identify exact verified source bytes');
  const full=code===0&&receipt.observation_kind==='source_observed'&&
    receipt.http_status>=200&&receipt.http_status<300;
  const revalidated=code===3&&receipt.http_status===304&&
    receipt.conditional===true&&receipt.observation_kind==='repeat_observed'&&
    receipt.unchanged_from_content_sha256===digest;
  if(!full&&!revalidated)throw Error('native ForeignTwin source read refused or changed');
  return {kind:receipt.observation_kind,observed_at:receipt.observed_at,revalidated};
}
function lastNativeReceipt(path){
  const size=statSync(path).size;
  const fd=openSync(path,'r');
  let tail;
  try { const n=Math.min(size,65536);tail=Buffer.alloc(n);
    if(readSync(fd,tail,0,n,size-n)!==n)throw Error('short native receipt read');
  } finally { closeSync(fd); }
  const last=tail.toString('utf8').trim().split(/\r?\n/).at(-1);
  if(!last)throw Error('native ForeignTwin wrote no observation');
  return {text:last,receipt:JSON.parse(last)};
}
// Consume the exact bytes retained by the established SourceFrontier/ForeignTwin adapter.
// Receipt paths cannot redirect the reader; missing or policy-discarded bodies never trigger a fallback fetch.
export function consumeNativeObservation(observation,source,{limit=4000000}={}){
  if(!Number.isSafeInteger(limit)||limit<1||limit>12000000)throw Error('invalid retained source bound');
  const {receipt,bodyPath,receiptPath,exitCode}=observation;
  verifyNativeObservation(receipt,exitCode,source,receipt?.content_sha256);
  if(receipt.body_path!==bodyPath)throw Error('native receipt body coordinate mismatch');
  const fd=openSync(bodyPath,constants.O_RDONLY|constants.O_NOFOLLOW);
  let bytes;
  try{
    const before=fstatSync(fd);
    if(!before.isFile()||before.size<1||before.size>limit)throw Error('native retained source is empty, oversized or not a file');
    bytes=Buffer.alloc(before.size);let offset=0;
    while(offset<bytes.length){const n=readSync(fd,bytes,offset,bytes.length-offset,offset);if(!n)throw Error('short native retained body');offset+=n;}
    const after=fstatSync(fd);
    if(before.size!==after.size||before.mtimeMs!==after.mtimeMs||before.ctimeMs!==after.ctimeMs)throw Error('native retained source changed during read');
  }finally{closeSync(fd);}
  const digest=sha(bytes),proof=verifyNativeObservation(receipt,exitCode,source,digest);
  if(!proof.revalidated&&receipt.body_bytes!==bytes.length)throw Error('native retained source byte count mismatch');
  const last=lastNativeReceipt(receiptPath);
  if(JSON.stringify(last.receipt)!==JSON.stringify(receipt))throw Error('native observation receipt changed during read');
  return {bytes,digest,status:receipt.http_status,proof:{...proof,receipt_sha256:sha(Buffer.from(last.text)),receipt_path:receiptPath,body_path:bodyPath}};
}
export async function observeCivicSource(project,source,{foreignRoot,cmsDb}={}){
  const twin=String(source.twin||source.source);
  if(!/^[A-Za-z0-9._:-]{1,128}$/.test(twin)||!isAbsolute(foreignRoot??'')||!isAbsolute(cmsDb??''))throw Error('invalid native source coordinates');
  const observed=await observePublic(project,foreignRoot,twin,source.url,{readText:false,fullMaxBytes:4000000,timeout:55000,
    env:{...process.env,BONFYRE_FOREIGN_ROOT:foreignRoot,BONFYRE_CMS_DB:cmsDb}});
  verifyNativeObservation(observed.receipt,observed.exitCode,source,observed.receipt?.content_sha256);
  if(observed.receipt.body_path!==observed.bodyPath)throw Error('native receipt body coordinate mismatch');
  return observed;
}
export async function run(argv=process.argv.slice(2)){
  const args=opt(argv);const {Database}=await import('bun:sqlite');const db=new Database(args.db,{readonly:true,create:false});
  const owned=inspectDb(db,args.namespace);db.close();
  const inventory=discoverCivicPairs(owned.collections,owned.sources,{limit:args.limit,
    dueSources:args.dueSources.length?new Set(args.dueSources):null});
  const selected=inventory.pairs;
  const work=[],newRows=[],held=[...inventory.held],observedAt=new Date().toISOString();
  const sourcesRead=new Map();let readAttempts=0,readSuccesses=0;
  function readSource(source){
    const key=[source.source,source.twin||source.source,source.url].join('\0');
    if(!args.apply){readAttempts++;return observedRead(source,4000000).then(value=>{readSuccesses++;return value;});}
    if(!sourcesRead.has(key)){
      readAttempts++;
      const read=observeCivicSource(args.project,source,{foreignRoot:args.foreign,cmsDb:args.db});
      // Cache receipt coordinates only, not up to 128 full page bodies.
      sourcesRead.set(key,read.then(value=>{readSuccesses++;return value;}));
    }
    return sourcesRead.get(key).then(observation=>consumeNativeObservation(observation,source));
  }
  mkdirSync(args.state,{recursive:true,mode:0o700});
  for(const pair of selected){
    try{
      const reads=await Promise.allSettled([readSource(pair.agenda),readSource(pair.video)]);
      const failure=reads.find(r=>r.status==='rejected');if(failure)throw failure.reason;
      const [agendaSource,videoSource]=reads.map(r=>r.value);
      const agenda=parseLegistarAgendaHtml(agendaSource.bytes.toString('utf8'));
      const parsed=parseSwagitIndex(videoSource.bytes.toString('utf8'),pair.video_id,new URL(pair.video.url).origin);
      if(parsed.conflicts.length){held.push(...parsed.conflicts.map(c=>({collection:pair.collection,...c})));continue;}
      const observed=args.apply?{agenda:agendaSource.proof,video:videoSource.proof}:{};
      const result=reconcileMeeting(pair,agenda,parsed.chapters,owned.releases.filter(x=>[pair.agenda.source,pair.video.source].includes(x.source_ref)),
        {observedAt,agendaDigest:agendaSource.digest,videoDigest:videoSource.digest});
      result.provenance={agenda_sha256:agendaSource.digest,video_sha256:videoSource.digest,
        agenda_url:pair.agenda.url,video_url:pair.video.url,
        native_observation:args.apply?observed:'not_run_in_review_mode'};
      const candidateCount=result.new_rows.length;newRows.push(...result.new_rows);delete result.new_rows;work.push({...result,new_row_count:candidateCount});
    }catch(error){held.push({collection:pair.collection,reason:'provider_or_policy_unavailable',detail:String(error.message||error).slice(0,250)});}
  }
  const seen=new Set();for(const row of newRows){if(seen.has(row.occurrence_id))throw Error('duplicate generated CMS identity');seen.add(row.occurrence_id);}
  const receipt={schema:'bonfyre.project.civic-reconcile.v1',observed_at:observedAt,mode:args.apply?'apply':'review',
    published_collections:owned.collections.length,source_collections_considered:inventory.considered,paired_meetings:selected.length,
    deferred_pairs:inventory.deferred,review_cases:held.filter(v=>v.reason!=='budget_deferred').length+work.reduce((n,v)=>n+(v.review?.length||0),0),
    provider_reads:readSuccesses,source_read_attempts:readAttempts,
    native_observation_calls:args.apply?readAttempts:0,
    read_accounting:'source observations, not raw HTTP requests; robots and transport are owned by the selected reader',
    candidate_creates:newRows.length,meetings:work,held,
    policy:'No inferred votes, awards, geocodes, transcript truth, video rights, or automatic World publication.'};
  if(args.apply&&held.some(x=>x.reason==='provider_or_policy_unavailable')){
    receipt.applied=0;receipt.state='partial_refused_source_unavailable';
  }else if(args.apply&&newRows.length){
    const dir=mkdtempSync(join(tmpdir(),'bonfyre-civic-batch-'));
    try{
      const body=join(dir,'new.jsonl');writeFileSync(body,newRows.map(v=>JSON.stringify(v)).join('\n')+'\n',{flag:'wx',mode:0o600});
      const proc=spawnSync(args.cms,['entry','import-jsonl','publication_occurrence',body,'--ns',args.namespace,'--db',args.db,'--schemas',args.schemas],{encoding:'utf8',timeout:180000,maxBuffer:2*1024*1024});
      if(proc.error||proc.status!==0)throw Error('native CMS transaction refused: '+String(proc.stderr||proc.error||proc.status).slice(0,300));
      const result=JSON.parse(proc.stdout);if(result.imported!==newRows.length)throw Error('native CMS incomplete batch readback');
      const check=new Database(args.db,{readonly:true,create:false});
      try{const statement=check.query('SELECT 1 FROM publication_occurrence WHERE namespace=? AND occurrence_id=? AND receipt_ref=?');
        for(const row of newRows)if(!statement.get(args.namespace,row.occurrence_id,row.receipt_ref))throw Error('CMS readback missing published source identity '+row.occurrence_id);
      }finally{check.close();}
      receipt.applied=newRows.length;receipt.cms_batch={imported:result.imported,ann_indexed:result.ann_indexed,families:result.families};receipt.state='applied';
    }finally{rmSync(dir,{recursive:true,force:true});}
  }else{receipt.applied=0;receipt.state=args.apply?'unchanged_or_held':'review_only';}
  // Every run has a compact reviewable immutable receipt, including failures.
  const digest=sha(Buffer.from(JSON.stringify(receipt))),file=join(args.state,'civic-'+observedAt.replace(/[^0-9TZ]/g,'')+'-'+randomBytes(4).toString('hex')+'.json');
  writeFileSync(file,JSON.stringify(receipt,null,2)+'\n',{flag:'wx',mode:0o600});
  return {...receipt,review_receipt:file,receipt_sha256:digest};
}
if(import.meta.main){try{const out=await run();console.log(JSON.stringify(out));}catch(error){console.error('[civic-sync] '+String(error.message||error));process.exitCode=1;}}
