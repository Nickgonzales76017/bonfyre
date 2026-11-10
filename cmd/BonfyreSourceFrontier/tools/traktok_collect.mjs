#!/usr/bin/env bun
// TraktoK-compatible video exports and official Research API collection.
// Collection state belongs to the existing SourceFrontier DB. Raw bodies remain
// content-addressed foreign material. No cookies, evasion, or implied API access.
import {Database} from 'bun:sqlite';
import {createHash,randomUUID} from 'node:crypto';
import {createReadStream,existsSync,mkdirSync,readFileSync,writeFileSync,writeSync,renameSync,openSync,closeSync,fsyncSync,rmSync,realpathSync,statSync} from 'node:fs';
import {join,dirname,resolve} from 'node:path';
import {fileURLToPath} from 'node:url';
import {spawnSync} from 'node:child_process';
import {once} from 'node:events';
export const API='https://open.tiktokapis.com/v2/research/video/query/';
export const FIELDS=['id','video_description','create_time','region_code','share_count','view_count','like_count','comment_count','music_id','hashtag_names','username','effect_ids','playlist_id','voice_to_text','video_duration'];
const hash=v=>createHash('sha256').update(v).digest('hex');
const fail=s=>{throw Error(s)};
const MAX_PAGE=8*1024*1024;
const checkedInt=(v,min,max)=>{if(!Number.isSafeInteger(v)||v<min||v>max)fail('integer outside configured bounds');return v};
const identifier=v=>{if(typeof v==='number'&&!Number.isSafeInteger(v))fail('rounded identifier refused');const s=String(v??'');if(!/^[1-9][0-9]{0,18}$/.test(s)||BigInt(s)>9223372036854775807n)fail('invalid TikTok identifier');return s};
const cursor=v=>{if(typeof v==='number'&&!Number.isSafeInteger(v))fail('rounded cursor refused');const s=String(v??'');if(!/^(0|[1-9][0-9]{0,18})$/.test(s)||BigInt(s)>9223372036854775807n)fail('invalid cursor');return s};
// JSON remains the platform boundary. Preserve unsafe integers before parsing,
// not after JSON.parse has already rounded them. Numbers inside strings stay intact.
export function losslessJSON(text){
 if(typeof text!=='string'||Buffer.byteLength(text)>MAX_PAGE)fail('page too large');
 let encoded='',quoted=false,escape=false;
 for(let i=0;i<text.length;){const c=text[i];
  if(quoted){encoded+=c;i++;if(escape)escape=false;else if(c==='\\')escape=true;else if(c==='"')quoted=false;continue;}
  if(c==='"'){quoted=true;encoded+=c;i++;continue;}
  if(c==='-'||(c>='0'&&c<='9')){const match=/^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?/.exec(text.slice(i));if(!match)fail('invalid JSON number');const n=match[0];
   encoded+=/^-?[0-9]+$/.test(n)&&!Number.isSafeInteger(Number(n))?JSON.stringify(n):n;i+=n.length;continue;}
  encoded+=c;i++;
 }
 return JSON.parse(encoded);
}
export function videoRecord(row){
 if(!row||typeof row!=='object'||Array.isArray(row))fail('video object required');
 const id=identifier(row.id??row.video_id),username=row.username??row.author_name;
 if(typeof username!=='string'||!/^[A-Za-z0-9_.]{1,64}$/.test(username))fail('exact creator handle required');
 if(row.video_description!==undefined&&row.video_description!==null&&typeof row.video_description!=='string')fail('caption must be text');
 const metadata={...row,id,username};delete metadata.video_id;delete metadata.author_name;
 const relations=[['creator',username]];
 for(const [key,kind] of [['hashtag_names','hashtag'],['effect_ids','effect']]){
  const values=row[key]??[];if(!Array.isArray(values)||values.length>1000)fail('invalid relation list');
  for(const v of values){if(kind==='effect'&&String(v)==='0')continue;if(kind==='hashtag'&&typeof v!=='string')fail('hashtag must be text');const s=kind==='effect'?identifier(v):v;if(!s||s.length>256)fail('invalid relation identity');relations.push([kind,s]);}
 }
 for(const [key,kind] of [['music_id','music'],['playlist_id','playlist']])if(row[key]!==null&&row[key]!==undefined&&String(row[key])!=='0'&&String(row[key])!=='')relations.push([kind,identifier(row[key])]);
 return {id,username,metadata,relations,url:`https://www.tiktok.com/@${username}/video/${id}`};
}
function date(v){if(typeof v!=='string'||!/^\d{8}$/.test(v))fail('dates must be YYYYMMDD');const n=Date.parse(v.slice(0,4)+'-'+v.slice(4,6)+'-'+v.slice(6,8)+'T00:00:00Z');if(!Number.isFinite(n)||stamp(n)!==v)fail('invalid calendar date');return n;}
const stamp=n=>new Date(n).toISOString().slice(0,10).replaceAll('-','');
export function windows(start,end){let a=date(start),b=date(end);if(a>b||(b-a)/86400000>36525)fail('invalid date range');const out=[];while(a<=b){const z=Math.min(b,a+29*86400000);out.push({start:stamp(a),end:stamp(z)});a=z+86400000;}return out;}
export function queryContract(q){
 const fields=new Set(['create_date','username','region_code','video_id','hashtag_name','keyword','music_id','effect_id','video_length','view_count','comment_count']);
 if(!q||typeof q!=='object'||Array.isArray(q)||Object.keys(q).some(k=>!['and','or','not'].includes(k)))fail('explicit TikTok query required');
 let count=0;const out={};for(const op of ['and','or','not']){if(q[op]===undefined)continue;if(!Array.isArray(q[op])||q[op].length>100)fail('invalid boolean query');out[op]=q[op].map(c=>{
  if(!c||!fields.has(c.field_name)||!['EQ','IN','GT','GTE','LT','LTE'].includes(c.operation)||!Array.isArray(c.field_values)||!c.field_values.length||c.field_values.length>100||c.field_values.some(v=>typeof v!=='string'||!v||v.length>500))fail('invalid typed query condition');count++;return {operation:c.operation,field_name:c.field_name,field_values:[...c.field_values]};});}
 if(!count)fail('empty query refused');return out;
}
export function openCollection(path){
 if(!existsSync(path))fail('existing SourceFrontier database required');const db=new Database(path,{readwrite:true});
 if(!db.query("SELECT 1 FROM sqlite_master WHERE name='channel' AND type='table'").get()){db.close();fail('not a SourceFrontier owner database');}
 db.exec(`PRAGMA busy_timeout=5000;
 CREATE TABLE IF NOT EXISTS traktok_collection(id TEXT PRIMARY KEY,query_json TEXT NOT NULL,source TEXT NOT NULL,created_at TEXT NOT NULL);
 CREATE TABLE IF NOT EXISTS traktok_shard(id TEXT PRIMARY KEY,collection_id TEXT NOT NULL,start_date TEXT NOT NULL,end_date TEXT NOT NULL,cursor TEXT NOT NULL DEFAULT '0',search_id TEXT NOT NULL DEFAULT '',state TEXT NOT NULL DEFAULT 'pending',lease TEXT NOT NULL DEFAULT '',lease_until INTEGER NOT NULL DEFAULT 0,retry_at INTEGER NOT NULL DEFAULT 0,attempts INTEGER NOT NULL DEFAULT 0,last_error TEXT NOT NULL DEFAULT '');
 CREATE TABLE IF NOT EXISTS traktok_page(id INTEGER PRIMARY KEY,shard_id TEXT NOT NULL,input_cursor TEXT NOT NULL,digest TEXT NOT NULL,body_path TEXT NOT NULL,observed_at TEXT NOT NULL,rows INTEGER NOT NULL,UNIQUE(shard_id,input_cursor));
 CREATE TABLE IF NOT EXISTS traktok_video_observation(video_id TEXT NOT NULL,page_id INTEGER NOT NULL,username TEXT NOT NULL,metadata_json TEXT NOT NULL,PRIMARY KEY(video_id,page_id)) WITHOUT ROWID;
 CREATE TABLE IF NOT EXISTS traktok_relation(video_id TEXT NOT NULL,kind TEXT NOT NULL,target TEXT NOT NULL,page_id INTEGER NOT NULL,PRIMARY KEY(video_id,kind,target,page_id)) WITHOUT ROWID;
 CREATE TABLE IF NOT EXISTS traktok_delivery(username TEXT PRIMARY KEY,page_digest TEXT NOT NULL,address TEXT NOT NULL,title TEXT NOT NULL,delivered INTEGER NOT NULL DEFAULT 0) WITHOUT ROWID;
 CREATE INDEX IF NOT EXISTS traktok_delivery_pending ON traktok_delivery(delivered);
 CREATE TABLE IF NOT EXISTS traktok_rate(key TEXT PRIMARY KEY,day TEXT NOT NULL,requests INTEGER NOT NULL,next_at INTEGER NOT NULL);
 `);return db;
}
export function enqueue(db,q,start,end,generation=""){
 if(typeof generation!=="string"||generation.length>128||/[\x00-\x1f]/.test(generation))fail("invalid explicit collection generation");
 q=queryContract(q);const id=hash(JSON.stringify({q,start,end,...(generation?{generation}:{})}));
 db.transaction(()=>{db.query('INSERT OR IGNORE INTO traktok_collection VALUES(?,?,?,?)').run(id,JSON.stringify(q),API,new Date().toISOString());
  const insert=db.query('INSERT OR IGNORE INTO traktok_shard(id,collection_id,start_date,end_date) VALUES(?,?,?,?)');
  for(const w of windows(start,end))insert.run(hash(id+':'+w.start+':'+w.end),id,w.start,w.end);
 })();return {collection:id,shards:windows(start,end).length};
}
export function claim(db,collection,now=Date.now()){
 return db.transaction(()=>{const row=db.query("SELECT * FROM traktok_shard WHERE collection_id=? AND retry_at<=? AND (state='pending' OR (state='leased' AND lease_until<?)) ORDER BY start_date,id LIMIT 1").get(collection,now,now);if(!row)return null;
  const lease=randomUUID();db.query("UPDATE traktok_shard SET lease=?,lease_until=?,state='leased',attempts=attempts+1 WHERE id=?").run(lease,now+120000,row.id);return {...row,lease};
 })();
}
function retain(dir,raw){
 mkdirSync(dir,{recursive:true,mode:0o700});const digest=hash(raw),path=join(dir,digest+'.body');
 if(existsSync(path)){if(hash(readFileSync(path))!==digest)fail('retained body digest mismatch');return {digest,path};}
 const temp=path+'.'+randomUUID()+'.tmp';try{writeFileSync(temp,raw,{flag:'wx',mode:0o600});const fd=openSync(temp,'r');try{fsyncSync(fd)}finally{closeSync(fd)}renameSync(temp,path);const d=openSync(dir,'r');try{fsyncSync(d)}finally{closeSync(d)}}finally{rmSync(temp,{force:true})}return {digest,path};
}
export function acceptPage(db,shard,raw,bodyDir,{exportRows,final=false}={}){
 if(Buffer.byteLength(raw)>MAX_PAGE)fail('page too large');
 let data,rows,next,search,more;
 if(exportRows){rows=exportRows;next=String(BigInt(shard.cursor)+BigInt(rows.length));search='';more=!final;}
 else{const answer=losslessJSON(raw);if(answer.error?.code!=='ok')fail('platform response error '+String(answer.error?.code??'missing').slice(0,80));data=answer.data;
  if(!data||!Array.isArray(data.videos)||data.videos.length>100||typeof data.has_more!=='boolean')fail('invalid page contract');
  rows=data.videos;next=cursor(data.cursor);search=data.search_id??'';more=data.has_more;
  if(typeof search!=='string'||search.length>500||(more&&!search)||(shard.search_id&&search!==shard.search_id))fail('changed or missing search identity');
  if(more&&BigInt(next)<=BigInt(shard.cursor))fail('non-progressing pagination cursor');
 }
 const videos=rows.map(videoRecord);if(new Set(videos.map(v=>v.id)).size!==videos.length)fail('duplicate video identity inside page');
 const body=retain(bodyDir,raw),seenAt=new Date().toISOString();
 return db.transaction(()=>{
  const current=db.query('SELECT * FROM traktok_shard WHERE id=?').get(shard.id);
  if(!current||current.lease!==shard.lease||current.cursor!==shard.cursor||current.state!=='leased')fail('stale collection lease');
  const pageId=db.query('INSERT INTO traktok_page(shard_id,input_cursor,digest,body_path,observed_at,rows) VALUES(?,?,?,?,?,?)').run(shard.id,shard.cursor,body.digest,body.path,seenAt,videos.length).lastInsertRowid;
  const insertVideo=db.query('INSERT OR IGNORE INTO traktok_video_observation VALUES(?,?,?,?)'),insertRelation=db.query('INSERT OR IGNORE INTO traktok_relation VALUES(?,?,?,?)');
  const deliver=db.query('INSERT OR IGNORE INTO traktok_delivery(username,page_digest,address,title) VALUES(?,?,?,?)');let inserted=0;
  for(const v of videos){inserted+=insertVideo.run(v.id,pageId,v.username,JSON.stringify(v.metadata)).changes;
   for(const [kind,target] of v.relations)insertRelation.run(v.id,kind,target,pageId);
   deliver.run(v.username,body.digest,'https://www.tiktok.com/@'+v.username,v.username+' — observed TikTok creator');
  }
  db.query("UPDATE traktok_shard SET cursor=?,search_id=?,state=?,lease='',lease_until=0,retry_at=0,last_error='' WHERE id=? AND lease=?").run(next,search,more?'pending':'complete',shard.id,shard.lease);
  return {rows:videos.length,new_observations:inserted,next_cursor:next,has_more:more,digest:body.digest};
 })();
}
function reserve(db,daily,interval,now){return db.transaction(()=>{const day=new Date(now).toISOString().slice(0,10),key='research-video';const r=db.query('SELECT * FROM traktok_rate WHERE key=?').get(key);const count=r?.day===day?r.requests:0;
 if(count>=daily)return {ok:false,retry:Date.parse(day+'T00:00:00Z')+86400000,reason:'daily_request_budget'};
 if(r&&r.next_at>now)return {ok:false,retry:r.next_at,reason:'request_spacing'};
 db.query('INSERT INTO traktok_rate VALUES(?,?,?,?) ON CONFLICT(key) DO UPDATE SET day=excluded.day,requests=excluded.requests,next_at=excluded.next_at').run(key,day,count+1,now+interval);return {ok:true};})();}
async function bodyText(response){const reader=response.body?.getReader();if(!reader)return '';const parts=[];let n=0;try{for(;;){const r=await reader.read();if(r.done)break;n+=r.value.byteLength;if(n>MAX_PAGE){await reader.cancel();fail('page size limit');}parts.push(Buffer.from(r.value));}}finally{reader.releaseLock()}return Buffer.concat(parts).toString('utf8');}
// Use the existing Credentials owner for sealed provider state. Returned
// authorization remains in memory and is never written to the collection.
export function credentialHeaders({binary,dbPath,keyPath,providersPath,provider}){
 if(![binary,dbPath,keyPath,providersPath,provider].every(v=>typeof v==='string'&&v.length>0))fail('complete credential owner configuration required');
 return async url=>{
  if(url!==API+'?fields='+FIELDS.join(','))fail('credential target is not the fixed collection endpoint');
  const r=spawnSync(binary,['apply','--db',dbPath,'--key',keyPath,'--providers',providersPath,'--provider',provider,'--url',url],{encoding:'utf8',timeout:30000,maxBuffer:256*1024});
  if(r.status!==0)fail('credential owner could not authorize request');
  let envelope;try{envelope=JSON.parse(r.stdout);}catch{fail('invalid credential-owner envelope');}
  if(envelope.url!==url||!Array.isArray(envelope.headers))fail('credential owner changed request target');
  const headers={'Content-Type':'application/json'};
  for(const line of envelope.headers){
   if(typeof line!=='string'||/[\r\n]/.test(line))fail('invalid credential header');const at=line.indexOf(':');if(at<1)fail('invalid credential header');
   const name=line.slice(0,at).toLowerCase(),value=line.slice(at+1).trim();
   if(name!=='authorization')fail('unexpected credential header');
   if(headers.Authorization||!/^Bearer [^\s]+$/.test(value))fail('invalid bearer authorization');headers.Authorization=value;
  }
  if(!headers.Authorization)fail('credential owner supplied no authorization');return headers;
 };
}
export function collectionState(db,id){
 const rows=db.query('SELECT state,count(*) n FROM traktok_shard WHERE collection_id=? GROUP BY state').all(id);
 if(!rows.length)return 'missing_shards';if(rows.every(r=>r.state==='complete'))return 'complete';
 if(rows.some(r=>r.state==='blocked'))return 'access_refused';return 'deferred';
}
export async function collect(db,{collection,bodyDir,token=process.env.TIKTOK_RESEARCH_ACCESS_TOKEN,maxPages=100,dailyBudget=1000,intervalMs=1000,transport=fetch,headersFor=null,now=()=>Date.now()}){
 checkedInt(maxPages,1,10000);checkedInt(dailyBudget,1,1000);checkedInt(intervalMs,0,3600000);
 const contract=db.query('SELECT * FROM traktok_collection WHERE id=?').get(collection);if(!contract||contract.source!==API)fail('collection is not a Research API query');
 const initial=collectionState(db,collection);
 if(initial==='complete'||initial==='access_refused')return {state:initial,remote_requests:0,pages:0,rows:0,collection};
 if(!token&&!headersFor)return {state:'credentials_required',remote_requests:0,pages:0,rows:0,collection};
 let pages=0,rows=0,requests=0,state='budget_yield';
 while(pages<maxPages){const work=claim(db,collection,now());if(!work){state=collectionState(db,collection);break;}
  const budget=reserve(db,dailyBudget,intervalMs,now());
  if(!budget.ok){db.query("UPDATE traktok_shard SET state='pending',retry_at=?,lease='' WHERE id=? AND lease=?").run(budget.retry,work.id,work.lease);state=budget.reason;if(state==='request_spacing'){await new Promise(r=>setTimeout(r,Math.max(1,budget.retry-now())));continue;}break;}
  try{
   const payload={query:JSON.parse(contract.query_json),start_date:work.start_date,end_date:work.end_date,max_count:100,...(work.search_id?{search_id:work.search_id}:{})};
   const serialized=JSON.stringify(payload).slice(0,-1)+',"cursor":'+cursor(work.cursor)+'}';
   const url=API+'?fields='+FIELDS.join(','),headers=headersFor?await headersFor(url):{Authorization:'Bearer '+token,'Content-Type':'application/json'};requests++;
   const response=await transport(url,{method:'POST',headers,body:serialized,signal:AbortSignal.timeout(45000),redirect:'error'});
   if(!response.ok){const auth=response.status===401||response.status===403;const retry=response.headers.get('retry-after');let delay=retry&&/^[0-9]+$/.test(retry)?Number(retry)*1000:60000;
    if(retry&&!/^[0-9]+$/.test(retry)){const t=Date.parse(retry);if(Number.isFinite(t))delay=Math.max(1000,t-now());}
    db.query("UPDATE traktok_shard SET state=?,retry_at=?,lease='',last_error=? WHERE id=? AND lease=?").run(auth?'blocked':'pending',now()+Math.min(Math.max(delay,1000),86400000),'HTTP '+response.status,work.id,work.lease);
    await response.body?.cancel();state=auth?'access_refused':'http_retry_deferred';break;}
   const raw=await bodyText(response),answer=acceptPage(db,work,raw,bodyDir);pages++;rows+=answer.rows;
  }catch(e){const reason=e.name==='TimeoutError'?'request_timeout':'collection_input_or_transport_failure';db.query("UPDATE traktok_shard SET state='pending',retry_at=?,lease='',last_error=? WHERE id=? AND lease=?").run(now()+60000,reason,work.id,work.lease);state='failed_page_not_committed';break;}
 }
 if(collectionState(db,collection)==='complete')state='complete';
 return {collection,state,remote_requests:requests,pages,rows};
}
// Retain the exact input once before projection so a file edited mid-import
// cannot be misattributed to the digest of an earlier version.
async function snapshotExport(input,dir){
 mkdirSync(dir,{recursive:true,mode:0o700});const temp=join(dir,'.export-'+randomUUID()),h=createHash('sha256');let fd=-1,bytes=0;
 try{fd=openSync(temp,'wx',0o600);for await(const chunk of createReadStream(input)){bytes+=chunk.length;if(bytes>2**31)fail('export exceeds 2 GiB bound');h.update(chunk);let offset=0;while(offset<chunk.length){const n=writeSync(fd,chunk,offset,chunk.length-offset);if(n<=0)fail('short export write');offset+=n;}}fsyncSync(fd);closeSync(fd);fd=-1;
  const digest=h.digest('hex'),path=join(dir,digest+'.jsonl');if(existsSync(path)){const check=createHash('sha256');for await(const chunk of createReadStream(path))check.update(chunk);if(check.digest('hex')!==digest)fail('export digest collision/corruption');}else renameSync(temp,path);
  return {digest,path,bytes};
 }finally{if(fd>=0)closeSync(fd);rmSync(temp,{force:true})}
}
export async function importExport(db,{path,bodyDir,sourceURI,batchSize=500,maxRows=1000000}){
 checkedInt(batchSize,1,2000);checkedInt(maxRows,1,10000000);if(!sourceURI||sourceURI.length>2000)fail('export provenance URI required');
 const retained=await snapshotExport(path,bodyDir);path=retained.path;const fileDigest=retained.digest;
 const id=hash('traktok-export:'+sourceURI+':'+fileDigest);
 db.transaction(()=>{db.query('INSERT OR IGNORE INTO traktok_collection VALUES(?,?,?,?)').run(id,'{}',sourceURI,new Date().toISOString());db.query('INSERT OR IGNORE INTO traktok_shard(id,collection_id,start_date,end_date) VALUES(?,?,?,?)').run(id,id,'','');})();
 let position=0,batch=[],rawLines=[],buffer='',imported=0;const verify=createHash('sha256');
 let existing=db.query('SELECT * FROM traktok_shard WHERE id=?').get(id);if(existing.state==='complete')return {collection:id,state:'already_imported',rows:0,file_digest:fileDigest};
 const skip=Number(existing.cursor);let accepted=skip,batchBytes=0;
 const flush=()=>{if(!batch.length)return;const shard=claim(db,id);if(!shard||Number(shard.cursor)!==accepted-batch.length)fail('export is leased or advanced by another worker');acceptPage(db,shard,rawLines.join('\n')+'\n',bodyDir,{exportRows:batch});imported+=batch.length;batch=[];rawLines=[];batchBytes=0;};
 const line=text=>{if(!text.trim())return;position++;if(position<=skip)return;if(position>maxRows)return;const bytes=Buffer.byteLength(text)+1;if(bytes>MAX_PAGE)fail('export line too large');if(batchBytes+bytes>MAX_PAGE)flush();batch.push(losslessJSON(text));rawLines.push(text);batchBytes+=bytes;accepted++;if(batch.length===batchSize)flush();};
 for await(const chunk of createReadStream(path,{encoding:'utf8'})){verify.update(chunk);buffer+=chunk;let at;while((at=buffer.indexOf('\n'))>=0){line(buffer.slice(0,at).replace(/\r$/,''));buffer=buffer.slice(at+1);}if(Buffer.byteLength(buffer)>MAX_PAGE)fail('export line too large');}
 if(buffer)line(buffer);flush();if(verify.digest('hex')!==fileDigest)fail('export changed while importing; raw chunks retained but completion refused');
 if(position<=maxRows)db.query("UPDATE traktok_shard SET state='complete' WHERE id=? AND state='pending'").run(id);
 return {collection:id,state:position>maxRows?'row_budget_yield':'complete',rows:imported,source_rows:position,file_digest:fileDigest};
}
export function deliver(db,{dbPath,frontier,dir,batchSize=1000,maxBatches=1000}){
 checkedInt(batchSize,1,10000);checkedInt(maxBatches,1,10000);mkdirSync(dir,{recursive:true,mode:0o700});let delivered=0;
 for(let i=0;i<maxBatches;i++){const rows=db.query('SELECT * FROM traktok_delivery WHERE delivered=0 ORDER BY username LIMIT ?').all(batchSize);if(!rows.length)break;
  const path=join(dir,'traktok-frontier-'+randomUUID()+'.jsonl');writeFileSync(path,rows.map(r=>JSON.stringify({address:r.address,protocol:'https',identity:'tiktok:creator:'+r.username,kind:'creator',material:'video_metadata',state:'shadow',truth_level:'observed_metadata_only',title:r.title,body_sha256:r.page_digest,discovered_from:'traktok:'+r.page_digest,rights:'source_specific_reuse_unverified'})).join('\n')+'\n',{flag:'wx',mode:0o600});
  try{const r=spawnSync(frontier,['import','--db',dbPath,'--label','TRAKTOK_OBSERVED_METADATA','--jsonl',path],{encoding:'utf8',timeout:120000,maxBuffer:1024*1024});if(r.status!==0)fail('native SourceFrontier delivery failed: '+(r.stderr??'').slice(0,300));
   const receipt=JSON.parse(r.stdout);if(receipt.rows!==rows.length||receipt.unusable!==0||receipt.refused!==0)fail('native delivery incomplete; outbox remains pending');
   db.transaction(()=>{const done=db.query('UPDATE traktok_delivery SET delivered=1 WHERE username=? AND page_digest=?');for(const row of rows)done.run(row.username,row.page_digest);})();delivered+=rows.length;
  }finally{rmSync(path,{force:true})}
 }
 return {delivered,pending:db.query('SELECT count(*) n FROM traktok_delivery WHERE delivered=0').get().n};
}
export async function exportObserved(db,out=process.stdout){
 for(const row of db.query('SELECT v.video_id,v.metadata_json,p.digest,p.observed_at,c.source FROM traktok_video_observation v JOIN traktok_page p ON p.id=v.page_id JOIN traktok_shard s ON s.id=p.shard_id JOIN traktok_collection c ON c.id=s.collection_id ORDER BY v.video_id,v.page_id').iterate()){
  if(!out.write(JSON.stringify({...JSON.parse(row.metadata_json),observation:{body_sha256:row.digest,observed_at:row.observed_at,source:row.source,truth:'observed_metadata_not_current_availability'}})+'\n'))await once(out,'drain');
 }
}
// A selected collected observation re-enters the existing BrowserHost/workbench.
// The owner checks raw bytes and the indexed projection before returning anything.
export function readCapture(db,{collection,video,bodyDir}){
 if(typeof collection!=='string'||!/^[a-f0-9]{64}$/.test(collection))fail('exact collection identity required');
 video=identifier(video);
 const row=db.query('SELECT v.metadata_json,p.digest,p.body_path,p.observed_at,c.source,c.query_json FROM traktok_video_observation v JOIN traktok_page p ON p.id=v.page_id JOIN traktok_shard s ON s.id=p.shard_id JOIN traktok_collection c ON c.id=s.collection_id WHERE c.id=? AND v.video_id=? ORDER BY p.observed_at DESC,p.id DESC LIMIT 1').get(collection,video);
 if(!row)fail('no observed video in the selected collection');
 const root=realpathSync(bodyDir),path=realpathSync(row.body_path);
 if(!path.startsWith(root+'/'))fail('retained source is outside the configured body owner');
 const info=statSync(path);if(!info.isFile()||info.size>MAX_PAGE)fail('retained page exceeds read bound');
 const raw=readFileSync(path);if(hash(raw)!==row.digest)fail('retained source digest mismatch');
 let candidates;
 if(row.query_json!=='{}'){
  const answer=losslessJSON(raw.toString('utf8'));
  if(answer.error?.code!=='ok'||!Array.isArray(answer.data?.videos))fail('invalid retained platform response');
  candidates=answer.data.videos;
 }else candidates=raw.toString('utf8').split('\n').filter(v=>v.trim()).map(losslessJSON);
 const matched=candidates.filter(v=>identifier(v.id??v.video_id)===video);
 if(matched.length!==1)fail('retained page membership is ambiguous');
 const value=videoRecord(matched[0]);
 if(JSON.stringify(value.metadata)!==row.metadata_json)fail('indexed video differs from retained evidence');
 const text=(v,n)=>typeof v==='string'?v.slice(0,n):'';
 const description=text(value.metadata.video_description,6000),transcript=text(value.metadata.voice_to_text,16000);
 const numeric=v=>(typeof v==='number'||(typeof v==='string'&&/^[0-9]+$/.test(v)))?Number(v):NaN;
 const seconds=numeric(value.metadata.create_time),duration=numeric(value.metadata.video_duration);
 const media={'@context':'https://schema.org','@type':'VideoObject',name:description.slice(0,240)||'Observed TikTok video',
  description,url:value.url,author:{'@type':'Person',name:value.username},identifier:value.id,
  ...(transcript?{transcript}:{}),
  ...(Number.isSafeInteger(seconds)&&seconds>0&&seconds<1e11?{uploadDate:new Date(seconds*1000).toISOString()}:{}),
  ...(Number.isSafeInteger(duration)&&duration>=0?{duration:'PT'+duration+'S'}:{})};
 return {schema:'aurekai.collected-page-capture.v1',url:value.url,title:media.name,canonical:value.url,
  captured_at:row.observed_at,structured:[media],selected_text:'',term_snippets:[],
  source_receipt:{owner:'BonfyreSourceFrontier',collection,video_id:video,body_sha256:row.digest,source:row.source,observed_at:row.observed_at},
  coverage:{kind:'retained_video_metadata',original_page_verified:true,live_network_requests:0,
   description_truncated:typeof value.metadata.video_description==='string'&&value.metadata.video_description.length>6000,
   transcript_truncated:typeof value.metadata.voice_to_text==='string'&&value.metadata.voice_to_text.length>16000},
  usage:'Source-specific permission still applies; not a recipe, merchant offer, current availability or media-reuse grant.'};
}
export function status(db){return {collections:db.query('SELECT count(*) n FROM traktok_collection').get().n,shards:db.query('SELECT state,count(*) n FROM traktok_shard GROUP BY state').all(),pages:db.query('SELECT count(*) n FROM traktok_page').get().n,unique_videos:db.query('SELECT count(DISTINCT video_id) n FROM traktok_video_observation').get().n,observations:db.query('SELECT count(*) n FROM traktok_video_observation').get().n,relations:db.query('SELECT count(*) n FROM traktok_relation').get().n,pending_delivery:db.query('SELECT count(*) n FROM traktok_delivery WHERE delivered=0').get().n};}
if(import.meta.main){let db;try{
 const [verb,...argv]=process.argv.slice(2),args={};for(let i=0;i<argv.length;i+=2){if(!argv[i].startsWith('--')||argv[i+1]===undefined)fail('flag value required');args[argv[i].slice(2)]=argv[i+1];}
 if(verb==='preflight'){console.log(JSON.stringify({research_token_configured:!!process.env.TIKTOK_RESEARCH_ACCESS_TOKEN,upstream:'JBGruber/traktok',api:API,scope:'research.data.basic',remote_requests:0}));}
 else{db=verb==='read'?new Database(args.db,{readonly:true}):openCollection(args.db);let r;const bodyDir=args.bodies??join(dirname(args.db),'foreign','bodies','traktok');
 if(verb==='read')r=readCapture(db,{collection:args.collection,video:args.video,bodyDir});
 else if(verb==='enqueue')r=enqueue(db,JSON.parse(readFileSync(args.query,'utf8')),args.start,args.end,args.generation??'');
 else if(verb==='collect'||verb==='run'){
  let collection=args.collection;
  if(verb==='run')collection=enqueue(db,JSON.parse(readFileSync(args.query,'utf8')),args.start,args.end,args.generation??'').collection;
  const headersFor=args.provider?credentialHeaders({binary:args.credentials??resolve(process.env.HOME,'.bonfyre/bin/bonfyre-credentials'),dbPath:args['credentials-db'],keyPath:args['credentials-key'],providersPath:args.providers,provider:args.provider}):null;
  r=await collect(db,{collection,bodyDir,headersFor,maxPages:Number(args.pages??100),dailyBudget:Number(args['daily-budget']??1000),intervalMs:Number(args['interval-ms']??1000)});
  if(verb==='run'&&(r.pages||r.state==='complete'))r.delivery=deliver(db,{dbPath:args.db,frontier:args.frontier??resolve(process.env.HOME,'.bonfyre/bin/bonfyre-source-frontier'),dir:args.out??join(dirname(args.db),'discovery')});
 }
 else if(verb==='resume'){
  if(!args.collection||!db.query('SELECT 1 FROM traktok_collection WHERE id=?').get(args.collection))fail('existing collection required');
  r={collection:args.collection,resumed:db.query("UPDATE traktok_shard SET state='pending',retry_at=0,lease='' WHERE collection_id=? AND state='blocked'").run(args.collection).changes,network_requests:0};
 }
 else if(verb==='import')r=await importExport(db,{path:args.file,bodyDir,sourceURI:args.source,'maxRows':Number(args.rows??1000000)});
 else if(verb==='deliver')r=deliver(db,{dbPath:args.db,frontier:args.frontier??resolve(process.env.HOME,'.bonfyre/bin/bonfyre-source-frontier'),dir:args.out??join(dirname(args.db),'discovery'),maxBatches:Number(args.batches??1000)});
 else if(verb==='export'){await exportObserved(db);r=null;}else if(verb==='status')r=status(db);else fail('use preflight|enqueue|collect|run|resume|import|deliver|read|export|status');
 if(r)console.log(JSON.stringify(r));if(r?.state==='credentials_required'||r?.state==='access_refused'||r?.state==='failed_page_not_committed')process.exitCode=2;}
 }catch(e){console.error(e.message);process.exitCode=1;}finally{db?.close();}}
