#!/usr/bin/env node
/** Public source-to-customer witness: read-only, no effect or cost claims.
 * The published CMS reference was independently observed at review time;
 * this command does not recheck the private owner DB. */
import {createHash} from 'node:crypto';
import {writeFileSync} from 'node:fs';
import {pathToFileURL} from 'node:url';
import {performance} from 'node:perf_hooks';

export const PUBLIC=Object.freeze({
  feed:'https://wanderatlasguides.com/events.ics',
  surfaces:['https://aurekai.live/','https://aurekai.live/stage'],
  cms_source_collection:'cms:source_collection:estate-carries-event',
  cms_internet_source:'events-wanderatlasguides-com-d48ec1',
  publisher_domain:'wanderatlasguides.com'
});
const sha=value=>createHash('sha256').update(value).digest('hex');
const byteLen=value=>Buffer.byteLength(value,'utf8');
const entities=value=>String(value)
  .replace(/&#x([0-9a-f]+);/gi,(_,h)=>String.fromCodePoint(parseInt(h,16)))
  .replace(/&#([0-9]+);/g,(_,d)=>String.fromCodePoint(Number(d)))
  .replace(/&(?:amp|quot|apos|lt|gt);/g,v=>({'&amp;':'&','&quot;':'"','&apos;':"'",'&lt;':'<','&gt;':'>'})[v]);
const visible=value=>entities(String(value).replace(/<[^>]*>/g,''));
export const canonical=value=>visible(String(value).replace(/\\[nN]/g,' ').replace(/\\,/g,',')
  .replace(/\\;/g,';').replace(/\\\\/g,'\\')).normalize('NFC')
  .replace(/[\u2018\u2019]/g,"'").toLowerCase().replace(/\s+/g,' ').trim();

export function parseIcs(text){
  if(!text.includes('BEGIN:VCALENDAR')||!text.includes('END:VCALENDAR'))
    throw Error('publisher did not return a complete iCalendar envelope');
  const lines=text.replace(/\r\n?/g,'\n').replace(/\n[ \t]/g,'').split('\n');
  const events=[];let event=null;
  for(const line of lines){
    if(line==='BEGIN:VEVENT'){if(event)throw Error('nested VEVENT');event={};continue}
    if(line==='END:VEVENT'){
      if(!event||!event.summary||!event.uid)throw Error('malformed VEVENT');
      events.push(event);event=null;continue;
    }
    if(!event)continue;
    const p=line.indexOf(':');if(p<0)continue;
    const field=line.slice(0,p).split(';')[0].toUpperCase();
    const value=line.slice(p+1);
    if(field==='SUMMARY')event.summary=canonical(value);
    else if(field==='UID')event.uid=value.trim();
  }
  if(event)throw Error('unterminated VEVENT');
  if(!events.length)throw Error('calendar has no VEVENT observations');
  return events;
}

export function attributedTitles(html,domain=PUBLIC.publisher_domain){
  const main=html.match(/<main\b[^>]*>([\s\S]*?)<\/main\s*>/i)?.[1];
  if(!main)throw Error('customer page lacks main content');
  const rows=[...main.matchAll(/<(li|article)\b[^>]*>([\s\S]*?)<\/\1\s*>/gi)];
  const titles=new Set();
  for(const [,kind,fragment] of rows){
    if(!visible(fragment).toLowerCase().includes(domain))continue;
    const match=kind.toLowerCase()==='article'
      ? fragment.match(/<h2\b[^>]*>([\s\S]*?)<\/h2>/i)
      : fragment.match(/<(?:b|strong)\b[^>]*>([\s\S]*?)<\/(?:b|strong)>/i);
    if(match)titles.add(canonical(match[1]));
  }
  return [...titles].filter(Boolean);
}

export function analyze(feedText,pages,timings={}){
  const events=parseIcs(feedText),sourceTitles=new Set(events.map(x=>x.summary));
  const consumers=PUBLIC.surfaces.map(url=>{
    const body=pages[url];
    if(typeof body!=='string'||!body)throw Error('missing customer surface '+url);
    const attributed=attributedTitles(body),matched=attributed.filter(t=>sourceTitles.has(t));
    return {url,body_bytes:byteLen(body),body_sha256:sha(body),
      source_attributed_titles:attributed.length,source_matched_titles:matched.length,
      matched_titles:matched,not_in_current_feed:attributed.filter(t=>!sourceTitles.has(t)),
      client_get_wall_ms:timings[url]?.wall_ms??null};
  });
  const shared=consumers[0].matched_titles.filter(x=>consumers[1].matched_titles.includes(x));
  const sourceMatches=consumers.reduce((n,x)=>n+x.source_matched_titles,0);
  return {schema:'bonfyre.source-customer-closure.v1',observed_at:new Date().toISOString(),
    mode:'public-read-only',
    source:{url:PUBLIC.feed,cms_publication_reference:PUBLIC.cms_source_collection,
      cms_internet_source_reference:PUBLIC.cms_internet_source,
      cms_declaration_verified_during_this_run:false,
      body_bytes:byteLen(feedText),body_sha256:sha(feedText),vevents:events.length,
      unique_event_uids:new Set(events.map(x=>x.uid)).size,
      unique_event_titles:sourceTitles.size,client_get_wall_ms:timings[PUBLIC.feed]?.wall_ms??null},
    consumers,
    overlap:{matched_title_occurrences_on_surfaces:sourceMatches,
      exact_titles_visible_on_both_surfaces:shared.length,shared_titles:shared,
      physical_reuse_proven:false},
    observations:{public_get_invocations:1+PUBLIC.surfaces.length,
      response_body_bytes:byteLen(feedText)+consumers.reduce((n,x)=>n+x.body_bytes,0),
      avoided_fetches:null,physical_bytes_reused:null,provider_cost_usd:null,
      economic_measure:'unpriced',resource_admission:'not_observed',
      materialization_receipts:'not_observed',receiving_owner_effect:'none'},
    verdict:consumers.every(x=>x.source_matched_titles>0)?
      'source_titles_observed_on_customer_surfaces':'insufficient_current_source_overlap',
    cannot_infer:[
      'title equality proves identical source occurrence UID or freshness',
      'CMS declaration remains published without current owner readback',
      'shared rendered material proves one physical download or cache hit',
      'HTTP payload bytes are network transport cost or server CPU work',
      'rendered overlap establishes avoided usage or cash savings',
      'event publication is endorsement, attendance or availability'
    ]};
}

async function fetchPublic(url){
  const start=performance.now();
  const response=await fetch(url,{redirect:'follow',signal:AbortSignal.timeout(25000),
    headers:{'User-Agent':'Bonfyre-public-source-witness/1.0 (+https://aurekai.work)',
      Accept:'text/html,text/calendar,*/*;q=0.8'}});
  if(!response.ok)throw Error('public GET failed: '+url+' HTTP '+response.status);
  const body=Buffer.from(await response.arrayBuffer());
  if(body.byteLength>3_000_000)throw Error('witness byte ceiling exceeded: '+url);
  return {body:body.toString('utf8'),wall_ms:Math.round((performance.now()-start)*100)/100};
}
export async function runPublic(){
  const urls=[PUBLIC.feed,...PUBLIC.surfaces],results=await Promise.all(urls.map(fetchPublic));
  return analyze(results[0].body,
    Object.fromEntries(PUBLIC.surfaces.map((u,i)=>[u,results[i+1].body])),
    Object.fromEntries(urls.map((u,i)=>[u,{wall_ms:results[i].wall_ms}]))
  );
}
if(process.argv[1]&&import.meta.url===pathToFileURL(process.argv[1]).href){
  try{
    const argv=process.argv.slice(2);
    const output=argv.length===0?null:argv.length===2&&argv[0]==='--output'&&
      argv[1]&&!argv[1].startsWith('--')?argv[1]:undefined;
    if(output===undefined)throw Error('usage: source_customer_closure.mjs [--output FILE.json]');
    const report=await runPublic(),json=JSON.stringify(report,null,2)+'\n';
    if(output)writeFileSync(output,json,{flag:'wx',mode:0o600});
    process.stdout.write(json);
    if(report.verdict!=='source_titles_observed_on_customer_surfaces')process.exitCode=1;
  }catch(error){console.error(error.message);process.exitCode=2;}
}
