#!/usr/bin/env bun
// One retained classifieds crawl projects both paid demand and reusable local
// operator supply into SourceFrontier. It reads public indexes/posts only.
// Contact, hiring, quoting, selection and spending remain other owners.
import {mkdirSync,writeFileSync,existsSync,readFileSync,renameSync,rmSync,realpathSync} from 'node:fs';
import {join,resolve} from 'node:path';
import {homedir} from 'node:os';
import {createHash} from 'node:crypto';
import {spawnSync} from 'node:child_process';
import {classifyWorkflowTitle,WORKFLOW_PATTERNS,expansionQueries} from './operator_supply.mjs';
import {observePublic,successfulPublicObservation} from './foreign_observation.mjs';

export const DEFAULT_METROS=[
  ['Dallas, TX','dallas'],['Houston, TX','houston'],['Austin, TX','austin'],['San Antonio, TX','sanantonio'],
  ['New York, NY','newyork'],['Los Angeles, CA','losangeles'],['San Francisco Bay, CA','sfbay'],['Miami, FL','miami'],
  ['Chicago, IL','chicago'],['Phoenix, AZ','phoenix'],['Atlanta, GA','atlanta'],['Seattle, WA','seattle'],
  ['Denver, CO','denver'],['Boston, MA','boston'],['Philadelphia, PA','philadelphia']
];
export const DEFAULT_CATEGORIES=[
  ['cpg','demand','computer_gigs'],['crg','demand','creative_gigs'],['wrg','demand','writing_gigs'],['evg','demand','event_gigs'],
  ['res','supply','resumes'],['cps','supply','computer_services'],['crs','supply','creative_services'],['evs','supply','event_services'],
  ['lbs','supply','labor_services'],['sks','supply','skilled_services']
];
export const WHOLESALE_CATEGORIES=[
  ['waa','demand','goods_wanted'],['bfa','supply','business_inventory'],
  ['sya','supply','computers'],['syp','supply','computer_parts'],
  ['ela','supply','electronics'],['tla','supply','tools'],['maa','supply','materials']
];
export function marketSelection({profile='services',markets=''}={}){
  if(!['services','wholesale'].includes(profile))throw Error('profile must be services or wholesale');
  const names=markets?markets.split(','):DEFAULT_METROS.map(([,slug])=>slug);
  const metros=[...new Set(names)].map(slug=>{
    const metro=DEFAULT_METROS.find(([,name])=>name===slug);
    if(!metro)throw Error('unknown market: '+slug);
    return metro;
  });
  return {metros,categories:profile==='wholesale'?WHOLESALE_CATEGORIES:DEFAULT_CATEGORIES};
}
const decode=s=>String(s??'').replace(/&amp;/g,'&').replace(/&#x27;|&#39;/g,"'").replace(/&quot;/g,'"').replace(/&lt;/g,'<').replace(/&gt;/g,'>');
const sha=v=>createHash('sha256').update(v).digest('hex');
const safeName=s=>String(s).replace(/[^a-zA-Z0-9._-]+/g,'-').slice(0,120);
export function parseIndex(html,{city,slug,cat,side,family,url,digest}){
  assertIndexIdentity(html,{cat,url});
  const clean=s=>decode((s||'').replace(/<[^>]+>/g,'').replace(/\s+/g,' ').trim());
  const rows=[...html.matchAll(/<li class="cl-static-search-result"[^>]*>([\s\S]*?)<\/li>/g)]
    .map(([,body])=>({title:clean(body.match(/<div class="title">([\s\S]*?)<\/div>/)?.[1]),
      address:body.match(/<a href="([^"]+)"/)?.[1],
      location:clean(body.match(/<div class="location">([\s\S]*?)<\/div>/)?.[1]),
      asking_price_text:clean(body.match(/<div class="price">([\s\S]*?)<\/div>/)?.[1])}))
    .filter(x=>{try{const u=new URL(x.address);return u.protocol==='https:'&&(u.hostname==='craigslist.org'||u.hostname.endsWith('.craigslist.org'))}catch{return false}});
  return rows.map(row=>({schema:'bonfyre-classified-index-row.v1',city,market:slug,category:cat,side,family,
    title:row.title,address:row.address,location:row.location,asking_price_text:row.asking_price_text,
    market_kind:WHOLESALE_CATEGORIES.some(([c])=>c===cat)?'goods':'services',index_url:url,index_body_sha256:digest,
    workflows:classifyWorkflowTitle(row.title),truth:'observed_index_title_only'}));
}
export function assertIndexIdentity(html,{cat,url}){
  const requested=new URL(url),canonicals=[];
  for(const tag of html.slice(0,html.search(/<body\b/i)>=0?html.search(/<body\b/i):html.length).matchAll(/<link\b[^>]*>/gi)){
    const attrs=Object.fromEntries([...tag[0].matchAll(/([\w:-]+)\s*=\s*(["'])(.*?)\2/g)].map(m=>[m[1].toLowerCase(),decode(m[3])]));
    if((attrs.rel||'').toLowerCase().split(/\s+/).includes('canonical'))canonicals.push(attrs.href);
  }
  if(canonicals.length!==1||!canonicals[0])throw Error('index canonical identity missing or ambiguous');
  const canonical=new URL(canonicals[0],url);
  if(requested.searchParams.getAll('cat').length!==1||requested.searchParams.get('cat')!==cat||
    canonical.searchParams.getAll('cat').length!==1||canonical.searchParams.get('cat')!==cat||
    canonical.origin!==requested.origin||canonical.pathname!==requested.pathname)
    throw Error(`index identity mismatch: requested ${cat}; canonical ${canonical.href}`);
  return canonical.href;
}

// Reuse retained observations without requesting pages again. Failed identity
// or digest checks remain visible and contribute no demand/supply rows.
export function replayIndexes({indexes:input,out}){
  const sourceDir=realpathSync(resolve(input,'..'));
  const outputDir=existsSync(out)?realpathSync(out):resolve(out);
  if(sourceDir===outputDir)throw Error('replay output must be separate from the retained source directory');
  const indexes=JSON.parse(readFileSync(input,'utf8'));
  if(!Array.isArray(indexes)||indexes.length>10000)throw Error('invalid retained index manifest');
  const checked=[],rows=[];
  for(const meta of indexes){
    try{
      if(meta.error)throw Error('original observation failed: '+meta.error);
      const raw=readFileSync(meta.body_path);
      if(sha(raw)!==meta.digest)throw Error('retained index digest mismatch');
      const parsed=parseIndex(raw.toString('utf8'),meta);
      rows.push(...parsed);checked.push({...meta,count:parsed.length,sample:parsed.slice(0,8),replay_state:'verified'});
    }catch(e){checked.push({...meta,count:0,sample:[],replay_state:'refused',error:e.message});}
  }
  mkdirSync(out,{recursive:true});
  const indexFile=join(out,'classified-indexes.json'),rowFile=join(out,'classified-rows.jsonl');
  atomic(indexFile,JSON.stringify(checked,null,2)+'\n');
  atomic(rowFile,rows.map(JSON.stringify).join('\n')+(rows.length?'\n':''));
  const side=rows.reduce((a,x)=>(a[x.side]=(a[x.side]||0)+1,a),{});
  const summary={schema:'bonfyre-classified-market-replay.v1',source_manifest:resolve(input),network_requests:0,
    index_pages:checked.filter(x=>x.replay_state==='verified').length,refused_indexes:checked.filter(x=>x.replay_state==='refused').length,
    listings:rows.length,demand_listings:side.demand||0,supply_listings:side.supply||0,outputs:{indexFile,rowFile},
    cannot_infer:'A digest-checked retained index is not current availability, a distinct buyer, a qualified lead or authority to contact.'};
  atomic(join(out,'summary.json'),JSON.stringify(summary,null,2)+'\n');
  return summary;
}
function atomic(path,data){
  const tmp=path+'.tmp-'+process.pid;writeFileSync(tmp,data,{mode:0o600});renameSync(tmp,path);
}
function retained(bodyDir,raw){
  mkdirSync(bodyDir,{recursive:true,mode:0o700});const digest=sha(raw),path=join(bodyDir,digest+'.body');
  if(!existsSync(path))writeFileSync(path,raw,{flag:'wx',mode:0o600});
  else if(sha(readFileSync(path))!==digest)throw Error('retained body corruption');
  return {digest,path};
}
function channel(row){
  return {address:row.address,protocol:'https',identity:'craigslist:'+sha(row.address).slice(0,24),
    name:'Craigslist '+(row.workflows?.[0]??row.family),kind:row.side+'_candidate',
    material:row.market_kind==='goods'?(row.side==='demand'?'goods_wanted_candidate':'goods_supply_candidate'):
      (row.side==='demand'?'paid_demand_candidate':'operator_candidate'),state:'shadow',
    truth_level:'observed_index_title_only',title:row.title,census_source:'craigslist-'+row.side,
    rights:'public_listing_index',cadence:'daily',discovered_from:row.index_url,body_sha256:row.index_body_sha256};
}
function indexChannel(meta){
  return {address:meta.url,protocol:'https',name:'Craigslist '+meta.city+' '+meta.family,identity:'craigslist:'+meta.slug+':'+meta.cat,
    kind:meta.side+'_index',material:WHOLESALE_CATEGORIES.some(([c])=>c===meta.cat)?
      (meta.side==='demand'?'goods_wanted_index':'goods_supply_index'):(meta.side==='demand'?'paid_demand_index':'operator_supply_index'),
    state:'shadow',truth_level:'observed_public_index',census_source:'craigslist-'+meta.side,cadence:'daily',
    rights:'public_listing_index',discovered_from:'craigslist-market-fanout',body_sha256:meta.digest,title:meta.city+' '+meta.family};
}
async function fetchBody(url,{timeoutMs=15000,maxBytes=8*1024*1024}={}){
  const res=await fetch(url,{headers:{'user-agent':'Mozilla/5.0'},signal:AbortSignal.timeout(timeoutMs)});
  if(!res.ok)throw Error('HTTP '+res.status+' '+url);
  const reader=res.body?.getReader();if(!reader)return Buffer.alloc(0);
  const parts=[];let n=0;
  try{for(;;){const x=await reader.read();if(x.done)break;n+=x.value.byteLength;if(n>maxBytes){await reader.cancel();throw Error('body too large '+url)}parts.push(Buffer.from(x.value))}}
  finally{reader.releaseLock()}
  return Buffer.concat(parts);
}
export function selectEnrichmentRows(rows,{side='both',query='',limit=100}={}){
  if(!['demand','supply','both'].includes(side))throw Error('side must be demand, supply or both');
  if(!Number.isSafeInteger(limit)||limit<1||limit>1000)throw Error('limit must be 1..1000');
  const terms=query.toLowerCase().split(',').map(s=>s.trim()).filter(Boolean),groups=new Map();
  let matching=0;
  for(const row of rows){
    if(side!=='both'&&row.side!==side)continue;
    if(terms.length&&!terms.some(term=>String(row.title).toLowerCase().includes(term)))continue;
    const u=new URL(row.address);
    if(u.protocol!=='https:'||!(u.hostname==='craigslist.org'||u.hostname.endsWith('.craigslist.org'))||u.username||u.password)
      throw Error('enrichment requires public Craigslist listing addresses');
    matching++;
    const key=row.side+'\x1f'+String(row.title).toLowerCase().replace(/\s+/g,' ').trim();
    const existing=groups.get(key);
    if(existing){if(!existing.same_title_addresses.includes(row.address))existing.same_title_addresses.push(row.address);continue;}
    groups.set(key,{...row,same_title_addresses:[row.address]});
  }
  const markets=new Map();
  for(const row of groups.values()){const bucket=markets.get(row.market)||[];bucket.push(row);markets.set(row.market,bucket);}
  const selected=[];
  while(selected.length<limit){let added=0;for(const bucket of markets.values()){
    if(bucket.length&&selected.length<limit){selected.push(bucket.shift());added++;}
  }if(!added)break;}
  return {matching,distinct_title_groups:groups.size,selected};
}
export function parseListingObservation(row,observation){
  if(observation.receipt.source_url!==row.address)throw Error('listing receipt source mismatch');
  if(!successfulPublicObservation(observation.receipt))throw Error('observation refused: '+(observation.receipt.policy_decision||'unknown')+' HTTP '+(observation.receipt.http_status||0));
  const raw=readFileSync(observation.bodyPath);
  if(sha(raw)!==observation.receipt.content_sha256)throw Error('listing body digest mismatch');
  const html=raw.toString('utf8'),section=html.match(/<section id="postingbody">([\s\S]*?)<\/section>/i)?.[1];
  if(!section)throw Error('no posting body; availability unconfirmed');
  const clean=s=>decode(String(s||'').replace(/<br\s*\/?>/gi,'\n').replace(/<[^>]+>/g,' ').replace(/&nbsp;/g,' ').replace(/[ \t]+/g,' ').replace(/\n\s*\n/g,'\n').trim());
  const body=clean(section),postId=html.match(/post id:\s*(\d+)/i)?.[1]||'';
  return {...row,post_id:postId,posted:html.match(/datetime="([^"]+)"/i)?.[1]||'',
    observed_at:observation.receipt.observed_at,body,body_sha256:sha(raw),body_path:observation.bodyPath,receipt_path:observation.receiptPath,
    body_text_sha256:sha(body.replace(/QR Code Link to This Post/g,'').replace(/\s+/g,' ').trim()),
    compensation:clean(html.match(/compensation:<\/span>\s*<span class="valu">([\s\S]*?)<\/span>/i)?.[1]),
    truth:'observed_full_post_unqualified'};
}
export async function enrichListings({rows,out,side='both',query='',limit=100,workers=4,evidence=[],offline=false,
  project=join(homedir(),'.bonfyre/bin/bonfyre-project'),observer=observePublic}={}){
  const selected=selectEnrichmentRows(rows,{side,query,limit});
  const retained=new Map();
  for(const file of evidence){
    if(resolve(file)===resolve(join(out,'enriched-listings.jsonl')))throw Error('evidence input must be separate from output');
    for(const line of readFileSync(file,'utf8').split(/\r?\n/).filter(Boolean)){
      const row=JSON.parse(line),previous=retained.get(row.address);
      if(!previous||String(row.observed_at||'')>String(previous.observed_at||''))retained.set(row.address,row);
    }
  }
  mkdirSync(out,{recursive:true});
  const results=[],cursor={i:0};let networkRequests=0,reused=0;
  const worker=async()=>{for(;;){const i=cursor.i++;if(i>=selected.selected.length)return;
    const row=selected.selected[i];let observed;
    try{
      const existing=retained.get(row.address);
      if(existing){
        reused++;
        if(!existing.receipt_path)throw Error('retained evidence has no receipt');
        const receipt=JSON.parse(readFileSync(existing.receipt_path,'utf8'));
        observed={receipt,receiptPath:existing.receipt_path,bodyPath:receipt.body_path};
      }else{
        if(offline)throw Error('no retained observation; offline mode');
        networkRequests++;
        observed=await observer(project,join(out,'foreign'),'classified-'+sha(row.address).slice(0,20),row.address,{readText:false});
      }
      results[i]=parseListingObservation(row,observed);
    }catch(e){results[i]={...row,error:e.message,receipt_path:observed?.receiptPath||'',truth:'observation_failed'};}
  }};
  await Promise.all(Array.from({length:Math.max(1,Math.min(Number(workers)||4,8))},worker));
  const posts=new Map(),textGroups=new Map();
  for(const row of results.filter(x=>!x.error)){
    const key=row.post_id||row.address;
    if(posts.has(key))row.same_post_as=posts.get(key);else posts.set(key,row.address);
    const previous=textGroups.get(row.body_text_sha256);
    if(previous)row.same_body_as=previous;else textGroups.set(row.body_text_sha256,row.address);
  }
  const path=join(out,'enriched-listings.jsonl');atomic(path,results.map(JSON.stringify).join('\n')+'\n');
  const summary={schema:'bonfyre-classified-enrichment.v1',matching_index_rows:selected.matching,title_groups:selected.distinct_title_groups,
    network_observations:networkRequests,retained_observations_reused:reused,offline,
    attempted:results.length,observed:results.filter(x=>!x.error).length,failed:results.filter(x=>x.error).length,
    distinct_observed_post_ids:posts.size,distinct_observed_bodies:textGroups.size,output:path,
    cannot_infer:'Matching titles or bodies do not prove a common owner. Full posts do not establish stock, accepted bids, resale rights, margins or permission to contact.'};
  atomic(join(out,'summary.json'),JSON.stringify(summary,null,2)+'\n');return summary;
}
export async function crawl({out,frontier='',bodyDir='',metros=DEFAULT_METROS,categories=DEFAULT_CATEGORIES,paceMs=35,hydrateDemand=false,workers=6}){
  mkdirSync(out,{recursive:true});if(!bodyDir)bodyDir=join(homedir(),'.bonfyre/estate-local/foreign/bodies/classified-market');
  const indexes=[],rows=[],marketChannels=[];let failed=0;
  for(const [city,slug] of metros)for(const [cat,side,family] of categories){
    const url='https://www.craigslist.org/search/area/'+slug+'?cat='+cat;
    try{
      const raw=await fetchBody(url),keep=retained(bodyDir,raw),meta={city,slug,cat,side,family,url,digest:keep.digest,body_path:keep.path,bytes:raw.length};
      const parsed=parseIndex(raw.toString('utf8'),meta);indexes.push({...meta,count:parsed.length,sample:parsed.slice(0,8)});
      rows.push(...parsed);marketChannels.push(indexChannel(meta));
    }catch(e){failed++;indexes.push({city,slug,cat,side,family,url,error:e.message})}
    if(paceMs)await Bun.sleep(paceMs);
  }
  const candidateRows=rows.filter(x=>x.market_kind==='goods'||x.workflows.length),uniqueChannels=new Map();
  for(const row of candidateRows){const c=channel(row);if(!uniqueChannels.has(c.address))uniqueChannels.set(c.address,c)}
  const hydrated=[];
  if(hydrateDemand){
    const targets=[...new Map(candidateRows.filter(x=>x.side==='demand').map(x=>[x.address,x])).values()],cursor={i:0};
    const worker=async()=>{for(;;){const i=cursor.i++;if(i>=targets.length)return;const x=targets[i];try{
      const raw=await fetchBody(x.address),keep=retained(join(bodyDir,'posts'),raw),html=raw.toString('utf8');
      const body=decode((html.match(/<section id="postingbody">([\s\S]*?)<\/section>/i)?.[1]||'').replace(/<br\s*\/?>/gi,'\n').replace(/<[^>]+>/g,' ').replace(/&nbsp;/g,' ').replace(/\s+/g,' ').trim());
      const compensation=decode((html.match(/compensation:<\/span>\s*<span class="valu">([\s\S]*?)<\/span>/i)?.[1]||'').replace(/<[^>]+>/g,' ').replace(/&nbsp;/g,' ').replace(/\s+/g,' ').trim());
      hydrated[i]={...x,posted:html.match(/datetime="([^"]+)"/i)?.[1]||'',compensation,body:body.slice(0,20000),body_sha256:keep.digest,body_path:keep.path};
    }catch(e){hydrated[i]={...x,error:e.message}} if(paceMs)await Bun.sleep(paceMs)}};
    await Promise.all(Array.from({length:Math.max(1,Math.min(workers,12))},worker));
  }
  const indexFile=join(out,'classified-indexes.json'),rowFile=join(out,'classified-rows.jsonl'),
    indexChannels=join(out,'market-channels.jsonl'),candidateChannels=join(out,'workflow-candidate-channels.jsonl'),
    demandFile=join(out,'hydrated-demand.jsonl');
  atomic(indexFile,JSON.stringify(indexes,null,2)+'\n');atomic(rowFile,rows.map(JSON.stringify).join('\n')+'\n');
  atomic(indexChannels,marketChannels.map(JSON.stringify).join('\n')+'\n');
  atomic(candidateChannels,[...uniqueChannels.values()].map(JSON.stringify).join('\n')+'\n');
  if(hydrateDemand)atomic(demandFile,hydrated.map(JSON.stringify).join('\n')+'\n');
  const workflows=[...new Set(WORKFLOW_PATTERNS.map(([name])=>name))],cities=metros.map(([city])=>city);
  const queryRows=expansionQueries({cities,workflows,languages:['en','es']});
  atomic(join(out,'cross-source-queries.jsonl'),queryRows.map(JSON.stringify).join('\n')+'\n');
  const sideCounts=rows.reduce((a,x)=>(a[x.side]=(a[x.side]||0)+1,a),{}),workflowCounts={};
  for(const x of candidateRows)for(const w of x.workflows){const k=x.side+':'+w;workflowCounts[k]=(workflowCounts[k]||0)+1}
  const summary={schema:'bonfyre-classified-market-refresh.v1',metros:metros.length,categories:categories.length,index_pages:indexes.length-failed,failed_indexes:failed,
    listings:rows.length,demand_listings:sideCounts.demand||0,supply_listings:sideCounts.supply||0,workflow_associations:candidateRows.reduce((n,x)=>n+x.workflows.length,0),
    goods_wanted_listings:rows.filter(x=>x.market_kind==='goods'&&x.side==='demand').length,
    goods_supply_listings:rows.filter(x=>x.market_kind==='goods'&&x.side==='supply').length,
    unique_workflow_candidates:uniqueChannels.size,workflow_counts:workflowCounts,cross_source_queries:queryRows.length,
    hydrated_demand:hydrateDemand?hydrated.filter(x=>x&&!x.error).length:0,hydration_failures:hydrateDemand?hydrated.filter(x=>x?.error).length:0,
    outputs:{indexFile,rowFile,indexChannels,candidateChannels,demandFile:hydrateDemand?demandFile:''}};
  atomic(join(out,'summary.json'),JSON.stringify(summary,null,2)+'\n');
  if(frontier){
    const bin=join(homedir(),'.bonfyre/bin/bonfyre-source-frontier');
    for(const [label,file] of [['CLASSIFIED_MARKET_INDEXES',indexChannels],['CLASSIFIED_WORKFLOW_CANDIDATES',candidateChannels]]){
      const p=spawnSync(bin,['import','--db',frontier,'--label',label,'--jsonl',file],{encoding:'utf8',timeout:120000,maxBuffer:2*1024*1024});
      if(p.status!==0)throw Error(p.stderr||p.stdout);
    }
  }
  return summary;
}
// Candidate routing reuses the observation owner. Rules are operator-authored
// hypotheses; even a literal match never establishes a buyer's acceptance.
export function routeListings({evidence,providers,out}){
  if(!Array.isArray(evidence)||!evidence.length)throw Error('routing requires retained evidence files');
  if(!Array.isArray(providers)||!providers.length||providers.length>100)throw Error('routing requires 1..100 provider rules');
  const output=join(resolve(out),'routing-candidates.jsonl'),withheldOutput=join(resolve(out),'routing-withheld.jsonl'),summaryPath=join(resolve(out),'routing-summary.json');
  const canonical=p=>existsSync(p)?realpathSync(p):resolve(p);
  if(evidence.some(p=>[output,withheldOutput,summaryPath].some(o=>canonical(p)===canonical(o))))throw Error('routing output must be separate from evidence');
  const plain=s=>decode(s.replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi,' ').replace(/<style\b[^>]*>[\s\S]*?<\/style>/gi,' ').replace(/<[^>]+>/g,' ').replace(/&nbsp;|&#160;/g,' ')).replace(/\s+/g,' ').trim();
  const ids=new Set(),rules=providers.map(p=>{
    if(!p.id||ids.has(p.id)||!p.name||!p.terms_stated||!p.terms_excerpt||!p.receipt_path)throw Error('invalid or duplicate provider rule');
    ids.add(p.id);
    const u=new URL(p.source_url);
    if(!['http:','https:'].includes(u.protocol)||u.username||u.password)throw Error('invalid provider source URL');
    const validTerms=terms=>Array.isArray(terms)&&terms.length>0&&terms.length<=40&&terms.every(t=>typeof t==='string'&&t.trim()&&t.length<=100);
    if(!validTerms(p.match_any))throw Error('invalid routing terms');
    if(p.exclude_any!==undefined&&!validTerms(p.exclude_any))throw Error('invalid routing exclusion terms');
    if(p.transaction_model!==undefined&&!['referral','purchase_resale'].includes(p.transaction_model))throw Error('invalid routing transaction model');
    if(p.match_groups!==undefined&&(!Array.isArray(p.match_groups)||!p.match_groups.length||p.match_groups.length>10||!p.match_groups.every(validTerms)))throw Error('invalid routing term groups');
    const receipt=JSON.parse(readFileSync(p.receipt_path,'utf8'));
    if(receipt.source_url!==p.source_url||!successfulPublicObservation(receipt))throw Error('provider observation source or policy mismatch');
    const raw=readFileSync(receipt.body_path);
    if(sha(raw)!==receipt.content_sha256)throw Error('provider body digest mismatch');
    if(!plain(raw.toString('utf8')).includes(plain(p.terms_excerpt)))throw Error('provider terms excerpt missing from retained body');
    return {...p,transaction_model:p.transaction_model||'referral',observed_at:receipt.observed_at,body_sha256:receipt.content_sha256};
  });
  const sources=new Map(),refused=[];let inputRows=0;
  for(const file of evidence)for(const line of readFileSync(file,'utf8').split(/\r?\n/).filter(Boolean)){
    if(++inputRows>10000)throw Error('routing input exceeds 10000 rows');
    const row=JSON.parse(line);
    try{
      if(row.error)throw Error(row.error);
      if(row.side!=='supply')throw Error('not a supply observation');
      selectEnrichmentRows([row],{side:'supply',limit:1});
      const receipt=JSON.parse(readFileSync(row.receipt_path,'utf8'));
      const verified=parseListingObservation(row,{receipt,receiptPath:row.receipt_path,bodyPath:receipt.body_path});
      const previous=sources.get(row.address);
      if(!previous||String(verified.observed_at)>String(previous.observed_at))sources.set(row.address,verified);
    }catch(e){refused.push({address:row.address||'',evidence:file,reason:e.message});}
  }
  const candidates=[],withheld=[];
  for(const row of sources.values())for(const provider of rules){
    const matches=provider.match_any.filter(term=>new RegExp('(^|[^a-z0-9])'+term.trim().replace(/[.*+?^${}()|[\]\\]/g,'\\$&')+'($|[^a-z0-9])','i').test(row.body));
    if(!matches.length)continue;
    const matchedGroups=(provider.match_groups||[]).map(group=>group.filter(term=>new RegExp('(^|[^a-z0-9])'+term.trim().replace(/[.*+?^${}()|[\]\\]/g,'\\$&')+'($|[^a-z0-9])','i').test(row.body)));
    if(matchedGroups.some(group=>!group.length))continue;
    const excluded=(provider.exclude_any||[]).filter(term=>new RegExp('(^|[^a-z0-9])'+term.trim().replace(/[.*+?^${}()|[\]\\]/g,'\\$&')+'($|[^a-z0-9])','i').test(row.body));
    const candidate={schema:'bonfyre-classified-routing-candidate.v1',
      candidate_id:sha(row.address+'\n'+provider.id),state:'needs_commercial_review',
      source:{address:row.address,index_title:row.title,index_location:row.location,asking_price_text:row.asking_price_text,
        observed_at:row.observed_at,receipt_path:row.receipt_path,body_sha256:row.body_sha256},
      provider:{id:provider.id,name:provider.name,source_url:provider.source_url,receipt_path:provider.receipt_path,
        observed_at:provider.observed_at,body_sha256:provider.body_sha256,terms_stated:provider.terms_stated,terms_excerpt:provider.terms_excerpt},
      transaction_model:provider.transaction_model,
      matched_body_terms:matches,matched_body_groups:matchedGroups,review_questions:provider.review_questions||[],
      unresolved:provider.transaction_model==='purchase_resale'
        ? ['Current lot availability, condition and seller authority','Seller purchase terms and transferable title','Buyer bid for the exact lot and acceptance conditions','Net margin after freight, rigging, fees and other transaction costs','Funding, payment order and delivery responsibilities; no purchase authorized']
        : ['Current lot availability and seller authority','Owner permission for an introduction','Provider acceptance, territory and removal costs','Written fee basis, registration and payment trigger'],
      cannot_infer:'Literal terms suggest a review candidate, not equipment compatibility, a shared owner, an accepted referral, a purchase, revenue or permission to contact.'};
    if(excluded.length)withheld.push({...candidate,state:'withheld_by_provider_rule',excluded_body_terms:excluded,
      state_reason:'Verified seller text matches an operator-authored exclusion. Read the full source before reconsidering; do not use this association for outreach.'});
    else candidates.push(candidate);
  }
  mkdirSync(out,{recursive:true});
  atomic(output,candidates.map(JSON.stringify).join('\n')+(candidates.length?'\n':''));
  atomic(withheldOutput,withheld.map(JSON.stringify).join('\n')+(withheld.length?'\n':''));
  const summary={schema:'bonfyre-classified-routing.v1',network_requests:0,input_rows:inputRows,
    distinct_verified_sources:sources.size,refused_rows:refused.length,provider_rules:rules.length,candidate_associations:candidates.length,
    matched_associations:candidates.length+withheld.length,withheld_associations:withheld.length,withheld_output:withheldOutput,
    withheld_by_provider:Object.fromEntries(rules.map(p=>[p.id,withheld.filter(c=>c.provider.id===p.id).length])),
    by_provider:Object.fromEntries(rules.map(p=>[p.id,candidates.filter(c=>c.provider.id===p.id).length])),refused,output,
    cannot_infer:'Associations can repeat a lot across buyers. Counts are not distinct owners, qualified leads, expected earnings or agreed introductions.'};
  atomic(summaryPath,JSON.stringify(summary,null,2)+'\n');return summary;
}
if(import.meta.main){
  try{
    const argv=process.argv.slice(2),arg=n=>{const i=argv.indexOf(n);return i>=0?argv[i+1]:''},has=n=>argv.includes(n);
    const verb=argv[0];if(!['refresh','replay','enrich','route'].includes(verb))throw Error('use refresh, replay, enrich, or route --evidence FILE[,FILE] --providers FILE --out DIR');
    if(verb==='route'){
      if(!arg('--evidence')||!arg('--providers')||!arg('--out'))throw Error('route requires --evidence FILE[,FILE] --providers FILE --out DIR');
      if(['routing-summary.json','routing-candidates.jsonl','routing-withheld.jsonl'].map(name=>join(resolve(arg('--out')),name)).includes(resolve(arg('--providers'))))throw Error('provider input must be separate from output');
      console.log(JSON.stringify(routeListings({evidence:arg('--evidence').split(','),providers:JSON.parse(readFileSync(arg('--providers'),'utf8')),out:resolve(arg('--out'))}),null,2));
    }else if(verb==='enrich'){
      if(!arg('--rows')||!arg('--out'))throw Error('enrich requires --rows FILE and --out DIR');
      const rows=readFileSync(arg('--rows'),'utf8').split(/\r?\n/).filter(Boolean).map(JSON.parse);
      console.log(JSON.stringify(await enrichListings({rows,out:resolve(arg('--out')),side:arg('--side')||'both',query:arg('--query'),limit:Number(arg('--limit')||100),workers:Number(arg('--workers')||4),evidence:(arg('--evidence')||'').split(',').filter(Boolean),offline:has('--offline')}),null,2));
    }else if(verb==='replay'){
      if(!arg('--indexes')||!arg('--out'))throw Error('replay requires --indexes FILE and --out DIR');
      console.log(JSON.stringify(replayIndexes({indexes:resolve(arg('--indexes')),out:resolve(arg('--out'))}),null,2));
    }else{
    const out=resolve(arg('--out')||join(homedir(),'Downloads/aurekai-classified-market-latest'));
    console.log(JSON.stringify(await crawl({out,...marketSelection({profile:arg('--profile')||'services',markets:arg('--markets')}),bodyDir:arg('--body-dir'),frontier:arg('--frontier'),hydrateDemand:has('--hydrate-demand'),paceMs:Number(arg('--pace-ms')||35),workers:Number(arg('--workers')||6)}),null,2));
    }
  }catch(e){console.error(e.message);process.exitCode=1}
}
