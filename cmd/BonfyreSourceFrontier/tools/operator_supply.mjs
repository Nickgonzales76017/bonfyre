#!/usr/bin/env bun
import { createReadStream, createWriteStream, existsSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { once } from 'node:events';
import { createInterface } from 'node:readline';

const sha=v=>createHash('sha256').update(v).digest('hex');
const ID=/^[a-z0-9][a-z0-9:._/-]{1,120}$/i;
const MONEY=/^(0|[1-9][0-9]{0,8})(?:\.[0-9]{1,2})?$/;
const RATE_UNIT=new Set(['hour','day','project','sale','week','month','year','unknown']);
const SOURCE_KIND=new Set(['operator_profile','operator_directory','labor_market_rate','job_posting_rate']);
const CONTACT=new Set(['platform_message','profile','email','phone','application','unknown']);
const clean=(v,n)=>typeof v==='string'?v.trim().slice(0,n):'';
const list=(v,n=64)=>Array.isArray(v)?[...new Set(v.filter(x=>typeof x==='string').map(x=>x.trim()).filter(Boolean))].slice(0,n):[];

export const WORKFLOW_PATTERNS=[
  ['office_assistance',/(clerical|office assistant|administrative|admin assistant|data entry|outlook|office help|virtual assistant|personal assistant)/i],
  ['bookkeeping',/(quickbooks|bookkeep|accounting|accounts payable|accounts receivable|payroll|reconciliation|invoic(?:e|ing))/i],
  ['b2b_calling',/(cold call|telemarket|outbound call|inside sales|business development|sales rep|caller)/i],
  ['appointment_setting',/(appointment set|appointment setter|lead gen|lead generation|setter)/i],
  ['video_capture',/(videograph|video production|camera operator|camera person|camera crew|film crew|videographer)/i],
  ['podcast_capture',/(podcast|livestream|live stream|audio video creator)/i],
  ['video_editing',/(video editor|video editing|post production|post-production|premiere pro|davinci resolve|after effects)/i],
  ['photography',/(photograph|photographer|photo shoot|product photo|headshot)/i],
  ['marketplace_listing',/(facebook marketplace|marketplace|offerup|ebay seller|product listing|product listings|resell|resale)/i],
  ['social_media_sales',/(social media|instagram|tiktok|facebook ads?|social selling|community manager)/i],
  ['website_build',/(website build|website design|web design|web(?:site)? developer|wordpress|shopify|woocommerce|ecommerce|e-commerce)/i],
  ['website_repair',/(website repair|website fix|website edit|website updat|website maintenance|wordpress fix|site fix|hacked site|malware removal|website help)/i],
  ['seo',/(\bseo\b|search engine optimization|local search|google business profile|google maps ranking)/i],
  ['digital_marketing',/(digital marketing|online marketing|growth marketing|marketing manager|marketing specialist)/i],
  ['paid_ads',/(media buyer|paid ads?|google ads|meta ads|facebook ads|ppc|paid media)/i],
  ['email_marketing',/(email marketing|marketing automation|mailchimp|klaviyo|customer journey|nurture campaign)/i],
  ['graphic_design',/(graphic design|graphic designer|photoshop|illustrator|flyer|logo design|ad creative)/i],
  ['content_writing',/(content writer|copywriter|copy writing|article writer|blog writer|technical writer|seo writer)/i],
  ['customer_support',/(customer support|customer service|call center|chat support|support rep|support specialist)/i],
  ['transcription',/(transcription|transcriber|captioning|subtitles|closed caption)/i],
  ['local_labor',/(mover|moving|junk removal|handyman|painter|carpenter|driver|courier|delivery|labor|cleaning|hauling)/i]
]
export const classifyWorkflowTitle=title=>WORKFLOW_PATTERNS.filter(([,re])=>re.test(String(title??''))).map(([name])=>name);

function publicUrl(v){const u=new URL(v);if(!['https:','http:'].includes(u.protocol)||u.username||u.password)throw Error('public http(s) URL required');u.hash='';return u.href}
function rate(v){
  if(v==null)return null;
  if(!v||typeof v!=='object'||Array.isArray(v)||!MONEY.test(String(v.amount??''))||
     !/^[A-Z]{3}$/.test(String(v.currency??''))||!RATE_UNIT.has(v.unit))throw Error('invalid rate');
  return {amount:String(v.amount),currency:v.currency,unit:v.unit,basis:clean(v.basis,240)||'source_listed'};
}
function standing(v){
  if(v==null)return {};
  if(!v||typeof v!=='object'||Array.isArray(v)||Object.keys(v).length>32)throw Error('invalid standing');
  const out={};
  for(const [k,x] of Object.entries(v)){
    if(!/^[a-z][a-z0-9_]{0,63}$/.test(k))throw Error('invalid standing key');
    if(typeof x==='boolean')out[k]=x;
    else if(typeof x==='string'&&x.length<=240)out[k]=x;
    else if(typeof x==='number'&&Number.isFinite(x)&&Math.abs(x)<=1e9)out[k]=x;
    else if(Array.isArray(x)&&x.length<=32&&x.every(y=>typeof y==='string'&&y.length<=120))out[k]=[...new Set(x)];
    else throw Error('invalid standing value');
  }
  return out;
}
function rating(v){
  if(v==null)return null;
  if(!v||typeof v!=='object'||!Number.isFinite(v.value)||v.value<0||v.value>5)throw Error('invalid rating');
  const out={value:v.value,scale:5};
  for(const k of ['reviews','tasks','jobs','hours'])if(v[k]!=null){if(!Number.isSafeInteger(v[k])||v[k]<0)throw Error('invalid rating count');out[k]=v[k]}
  return out;
}
export function normalize(row){
  if(!row||typeof row!=='object'||Array.isArray(row)||row.schema!=='bonfyre-operator-observation.v1')throw Error('wrong schema');
  const source=clean(row.source,80),source_kind=row.source_kind,profile_url=publicUrl(row.profile_url),display_name=clean(row.display_name,240);
  const workflows=list(row.workflow_families,32),capabilities=list(row.capabilities,96),languages=list(row.languages,24);
  const locality=clean(row.locality,120),region=clean(row.region,80),country=clean(row.country,80);
  if(!source||!SOURCE_KIND.has(source_kind)||!display_name||!workflows.length||workflows.some(x=>!ID.test(x))||
     !locality||!region||!country||typeof row.observed_at!=='string'||!Number.isFinite(Date.parse(row.observed_at)))throw Error('missing operator identity/scope');
  const contact_mode=CONTACT.has(row.contact_mode)?row.contact_mode:'unknown';
  const body_sha256=row.body_sha256==null?'':clean(row.body_sha256,64).toLowerCase();
  if(body_sha256&&!/^[a-f0-9]{64}$/.test(body_sha256))throw Error('invalid body digest');
  const identity=row.identity?clean(row.identity,120):'operator:'+sha([source,profile_url,display_name,locality].join('\x1f')).slice(0,32);
  if(!ID.test(identity))throw Error('invalid identity');
  return {schema:'bonfyre-operator-supply.v1',identity,source,source_kind,profile_url,display_name,
    locality,region,country,workflow_families:workflows,capabilities,languages,rate:rate(row.rate),rating:rating(row.rating),
    availability:clean(row.availability,500),contact_mode,standing:standing(row.standing),observed_at:row.observed_at,body_sha256,
    source_note:clean(row.source_note,1000),cannot_infer:[
      'public profile or labor-market observation -> current availability or willingness to accept this job',
      'listed rate -> accepted quote, final price, reservation, employment relationship or quality guarantee',
      'platform rating -> capability for an unobserved workflow',
      'locality label -> exact travel radius'
    ]};
}
export async function compile(input,{channels,profiles}){
  const seen=new Map(),stats={read:0,admitted:0,invalid:0,duplicates:0};
  const rl=createInterface({input:createReadStream(input,{encoding:'utf8'}),crlfDelay:Infinity});
  for await(const line of rl){
    if(!line.trim())continue;stats.read++;
    try{const x=normalize(JSON.parse(line));const key=x.identity+'\x1f'+x.observed_at+'\x1f'+x.profile_url;if(seen.has(key)){stats.duplicates++;continue}seen.set(key,x);stats.admitted++}
    catch{stats.invalid++}
  }
  const rows=[...seen.values()].sort((a,b)=>a.identity.localeCompare(b.identity)||a.observed_at.localeCompare(b.observed_at));
  const cw=createWriteStream(channels,{flags:'w',mode:0o600}),pw=createWriteStream(profiles,{flags:'w',mode:0o600});
  try{
    for(const x of rows){
      const channel={address:x.profile_url,protocol:new URL(x.profile_url).protocol.slice(0,-1),name:x.source,
        identity:x.identity,kind:x.source_kind,material:x.source_kind.includes('rate')?'operator_market_rate':'operator_profile',
        state:'shadow',truth_level:x.source_kind.includes('rate')?'observed_market_rate':'observed_operator_listing',
        title:x.display_name,census_source:'operator-supply:'+x.workflow_families[0],
        rights:'public_profile_observation',cadence:'daily',discovered_from:'operator-supply:'+x.source,body_sha256:x.body_sha256};
      if(!cw.write(JSON.stringify(channel)+'\n'))await once(cw,'drain');
      if(!pw.write(JSON.stringify(x)+'\n'))await once(pw,'drain');
    }
  }finally{cw.end();pw.end();await Promise.all([once(cw,'close'),once(pw,'close')])}
  return {...stats,channels,profiles,identities:new Set(rows.map(x=>x.identity)).size,
    workflows:new Set(rows.flatMap(x=>x.workflow_families)).size,localities:new Set(rows.map(x=>[x.locality,x.region,x.country].join('|'))).size};
}
export function expansionQueries({cities,workflows,languages=['en'],sources=['indeed','taskrabbit','upwork','bark','thumbtack','craigslist_supply','craigslist_demand']}){
  const es={office_assistance:'asistente administrativo',physical_admin:'asistencia presencial',local_runner:'asistente local',
    bookkeeping:'contabilidad',quickbooks:'QuickBooks',office_finance:'administración financiera',
    b2b_calling:'llamadas B2B',appointment_setting:'concertador de citas',lead_generation:'generación de prospectos',
    video_capture:'videógrafo',podcast_capture:'productor de podcast',video_editing:'editor de video',
    marketplace_listing:'publicaciones marketplace',social_media_sales:'ventas en redes sociales',
    website_build:'diseño web',website_repair:'reparación de sitio web',seo:'SEO',digital_marketing:'marketing digital',
    paid_ads:'publicidad pagada',email_marketing:'marketing por correo',graphic_design:'diseño gráfico',content_writing:'redacción de contenido',
    customer_support:'atención al cliente',transcription:'transcripción',photography:'fotógrafo'};
  const out=[];
  for(const city of cities)for(const workflow of workflows)for(const language of languages)for(const source of sources){
    const term=language==='es'?(es[workflow]??workflow.replaceAll('_',' ')):workflow.replaceAll('_',' ');
    const label=term+' '+city;let query,side='supply';
    if(source==='indeed'){query='site:indeed.com '+label+' pay OR hourly OR salary';side='market_rate';}
    else if(source==='taskrabbit')query='site:taskrabbit.com/locations '+label;
    else if(source==='upwork')query='site:upwork.com/freelancers '+label;
    else if(source==='bark')query='site:bark.com '+label;
    else if(source==='thumbtack')query='site:thumbtack.com '+label;
    else if(source==='craigslist_supply')query='site:craigslist.org '+JSON.stringify(city)+' '+JSON.stringify(term)+' (resume OR services OR available)';
    else if(source==='craigslist_demand'){query='site:craigslist.org '+JSON.stringify(city)+' '+JSON.stringify(term)+' (gig OR job OR wanted OR help)';side='demand';}
    else throw Error('unknown operator query source '+source);
    out.push({schema:'bonfyre-operator-query.v1',city,workflow,language,source,side,query,
      cannot_infer:['translated search phrase -> provider language proficiency','search result -> current availability, rate, authority or customer acceptance']});
  }
  return out;
}
if(import.meta.main){
  try{
    const [verb,...argv]=process.argv.slice(2),args={};
    for(let i=0;i<argv.length;i+=2){if(!argv[i]?.startsWith('--')||argv[i+1]===undefined)throw Error('flag value required');args[argv[i].slice(2)]=argv[i+1]}
    if(verb==='compile'){
      if(!args.input||!existsSync(args.input)||!args.channels||!args.profiles)throw Error('compile --input OBS.jsonl --channels OUT.jsonl --profiles OUT.jsonl');
      console.log(JSON.stringify(await compile(args.input,{channels:args.channels,profiles:args.profiles})));
    }else if(verb==='queries'){
      const rows=expansionQueries({cities:args.cities.split(';'),workflows:args.workflows.split(';'),languages:(args.languages??'en').split(';'),sources:(args.sources??'indeed;taskrabbit;upwork;bark;thumbtack;craigslist_supply;craigslist_demand').split(';')});
      process.stdout.write(rows.map(JSON.stringify).join('\n')+'\n');
    }else throw Error('use compile|queries');
  }catch(e){console.error(e.message);process.exitCode=1}
}
