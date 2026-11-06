import {contentReferences} from './contents.mjs';
// Page observations and projections only. Commercial decisions stay in recurring-commerce.mjs.
import {compileDraft, basketPreview, tryOnCheckout, stockDecision} from './recurring-commerce.mjs';
const MAX_BYTES=256*1024, MAX_NODES=2500;
const text=(x,n=1000)=>typeof x==='string'?x.replace(/\s+/g,' ').trim().slice(0,n):'';
const list=x=>Array.isArray(x)?x:x==null?[]:[x];
const assert=(ok,why)=>{if(!ok)throw new Error(why)};
export function safeURL(value, base) {
  try {const u=new URL(value,base); if(!['https:','http:'].includes(u.protocol)||u.username||u.password)return null;
    // Retain common product selectors; do not export checkout/session/auth query strings.
    for(const key of [...u.searchParams.keys()])if(!['variant','sku','product','product_id','selling_plan'].includes(key))u.searchParams.delete(key);
    u.hash='';return u.href;
  }catch{return null;}
}
export function money(value,currency) {
  // Do not assume every currency has two fractional digits. USD compiler is intentionally narrower.
  const raw=typeof value==='number'&&Number.isFinite(value)?String(value):text(value,60);
  const code=text(currency,3).toUpperCase();
  if(!raw)return {raw:null,currency:code||null,usd_cents:null};
  let cents=null;
  if(code==='USD'&&/^\d{1,9}(\.\d{1,2})?$/.test(raw)){
    const [whole,part='']=raw.split('.'); cents=Number(whole)*100+Number(part.padEnd(2,'0'));
  }
  return {raw,currency:code||null,usd_cents:cents};
}
export function stableJSON(value){
  if(value===null||typeof value!=='object')return JSON.stringify(value);
  if(Array.isArray(value))return '['+value.map(stableJSON).join(',')+']';
  return '{'+Object.keys(value).sort().map(k=>JSON.stringify(k)+':'+stableJSON(value[k])).join(',')+'}';
}
export async function digest(value){
  const bytes=new TextEncoder().encode(typeof value==='string'?value:stableJSON(value));
  return [...new Uint8Array(await crypto.subtle.digest('SHA-256',bytes))].map(n=>n.toString(16).padStart(2,'0')).join('');
}
export function referenceMatches(catalog,url){
  const host=new URL(url).hostname.toLowerCase();
  return catalog.profiles.filter(p=>list(p.source_urls).concat(p.url||[]).some(v=>{
    try{const h=new URL(v).hostname.toLowerCase().replace(/^www\./,'');return host===h||host.endsWith('.'+h)}catch{return false}
  }));
}
export async function observe(raw,catalog){
  assert(raw&&typeof raw==='object','Capture object required');
  assert(new TextEncoder().encode(JSON.stringify(raw)).length<=MAX_BYTES,'Capture exceeds 256 KiB');
  const sourceURL=safeURL(raw.url);assert(sourceURL,'An HTTP(S) page without credentials is required');
  assert(typeof raw.captured_at==='string'&&Number.isFinite(Date.parse(raw.captured_at)),'Capture timestamp required');
  const nodes=[],seen=new Set();let visited=0,limitHit=false;
  function walk(v,depth=0){if(!v||typeof v!=='object'||seen.has(v))return;seen.add(v);
    if(++visited>MAX_NODES||depth>15){limitHit=true;return;}
    if(!Array.isArray(v))nodes.push(v);
    for(const [k,x]of Object.entries(v))if(k!=='@context')walk(x,depth+1);
  }
  for(const item of list(raw.structured).slice(0,30))walk(item);
  const byId=new Map(nodes.filter(n=>typeof n['@id']==='string').map(n=>[n['@id'],n]));
  const resolve=x=>x&&typeof x==='object'&&x['@id']?{...byId.get(x['@id']),...x}:x;
  const has=(n,type)=>list(n?.['@type']).some(t=>t===type||t==='https://schema.org/'+type||t==='http://schema.org/'+type);
  const products=nodes.filter(n=>has(n,'Product')||has(n,'Service')).slice(0,40);
  const offers=[];
  for(const p of products){
    for(const candidate of list(p.offers).slice(0,30)){
      const o=resolve(candidate);if(!o||typeof o!=='object')continue;
      const spec=resolve(list(o.priceSpecification)[0])||{};
      const productURL=safeURL(p.url||sourceURL,sourceURL)||sourceURL;
      offers.push({product_name:text(p.name,180)||text(raw.title,180),sku:text(p.sku,120)||null,
        product_url:productURL,offer_url:safeURL(o.url||productURL,sourceURL)||productURL,
        price:money(o.price??spec.price,o.priceCurrency??spec.priceCurrency),
        range:has(o,'AggregateOffer')?{low:money(o.lowPrice,o.priceCurrency),high:money(o.highPrice,o.priceCurrency)}:null,
        availability:text(o.availability,200)||null,valid_until:text(o.priceValidUntil,100)||null,
        billing_duration:text(spec.billingDuration??o.eligibleDuration?.value,100)||null,
        price_type:text(spec.priceType,100)||null,shipping:'not_quoted',tax:'not_quoted',
        origin:'page_jsonld_unverified'});
    }
  }
  // Source claims stay distinct from calculated recipe demand and merchant offers.
  const recipes=[],media=[];
  const nutritionKeys=['calories','carbohydrateContent','proteinContent','fatContent','saturatedFatContent','unsaturatedFatContent','transFatContent','sugarContent','fiberContent','sodiumContent','cholesterolContent'];
  function steps(value,result=[],depth=0){
    if(depth>8){result.push('[further nested source instructions omitted]');return result;}
    for(const v of list(value)){
      if(result.length>=100)break;
      if(typeof v==='string')result.push(text(v,4000));
      else if(v&&typeof v==='object'){
        if(typeof v.text==='string')result.push(text(v.text,4000));
        else if(v.itemListElement||v.steps)steps(v.itemListElement||v.steps,result,depth+1);
      }
    }return result;
  }
  for(const r of nodes.filter(n=>has(n,'Recipe')).slice(0,12)){
    const n=resolve(r.nutrition)||{},claims=[];
    for(const field of nutritionKeys)if(typeof n[field]==='string'||(typeof n[field]==='number'&&Number.isFinite(n[field])))
      claims.push({field,raw_value:String(n[field]),basis:text(n.servingSize,200)||'not_stated',standing:'source_claim_not_calculated'});
    recipes.push({title:text(r.name,240),url:safeURL(r.url||sourceURL,sourceURL)||sourceURL,
      yield_text:list(r.recipeYield).filter(v=>typeof v==='string'||typeof v==='number').map(String).join('; ').slice(0,400),
      ingredients:list(r.recipeIngredient).filter(v=>typeof v==='string').slice(0,100).map(v=>text(v,700)),
      instructions:steps(r.recipeInstructions),nutrition:claims,
      coverage:'bounded_source_claims; not a normalized or validated preparation plan'});
  }
  for(const m of nodes.filter(n=>has(n,'VideoObject')).slice(0,12))media.push({
    kind:'video',title:text(m.name,240),description:text(m.description,4000),
    url:safeURL(m.url||sourceURL,sourceURL)||sourceURL,creator:text(typeof m.author==='string'?m.author:m.author?.name,160),
    duration:text(m.duration,100),published_at:text(m.uploadDate,100),transcript:text(m.transcript,16000),standing:'page_metadata_not_media_download'});
  let platformPost=null;
  if(raw.platform_post&&typeof raw.platform_post==='object'){
    const p=raw.platform_post,u=new URL(sourceURL),m=/^\/@([^/]+)\/(video|photo)\/(\d+)\/?$/.exec(u.pathname);
    if((u.hostname==='www.tiktok.com'||u.hostname==='tiktok.com')&&m&&p.id===m[3]){
      if(p.status==='observed'&&p.author===decodeURIComponent(m[1])){
        platformPost={status:'observed',id:p.id,kind:m[2],url:sourceURL,author:text(p.author,100),description:text(p.description,6000),
          created_seconds:typeof p.created_seconds==='string'&&/^\d{1,12}$/.test(p.created_seconds)?p.created_seconds:null,
          duration_seconds:typeof p.duration_seconds==='string'&&/^\d{1,9}$/.test(p.duration_seconds)?p.duration_seconds:null,
          hashtags:list(p.hashtags).slice(0,30).map(v=>text(v,100)),image_count:Number.isSafeInteger(p.image_count)&&p.image_count>=0?p.image_count:0,
          coverage:'current_post_metadata_only'};
        media.push({kind:m[2],title:text(p.description,240)||'Selected TikTok post',description:text(p.description,6000),url:sourceURL,
          creator:platformPost.author,platform_id:p.id,duration_seconds:platformPost.duration_seconds,
          standing:'selected_page_metadata_not_recipe_or_usage_rights'});
      }else platformPost={status:['restricted','unavailable','not_present','too_large','identity_mismatch','author_mismatch','invalid_payload'].includes(p.status)?p.status:'invalid_payload',id:m[3]};
    }
  }
  let sourceReceipt;
  const sr=raw.source_receipt;
  if(sr?.owner==='BonfyreSourceFrontier'&&/^[a-f0-9]{64}$/.test(sr.collection||'')&&/^[a-f0-9]{64}$/.test(sr.body_sha256||'')&&/^[1-9][0-9]{0,18}$/.test(sr.video_id||'')&&typeof sr.observed_at==='string'&&Number.isFinite(Date.parse(sr.observed_at))){
    sourceReceipt={owner:sr.owner,collection:sr.collection,video_id:sr.video_id,body_sha256:sr.body_sha256,
      source:text(sr.source,2000),observed_at:sr.observed_at,standing:'retained_owner_reference_not_new_verification_by_this_page'};
  }
  const bounded={url:sourceURL,title:text(raw.title,200),captured_at:raw.captured_at,
    declared_canonical:safeURL(raw.canonical||sourceURL,sourceURL),
    selected_text:text(raw.selected_text,8000),term_snippets:list(raw.term_snippets).slice(0,24).map(v=>text(v,700)).filter(Boolean),
    // Full bounded JSON-LD is retained as captured evidence. Never evaluate it as code.
    ...(sourceReceipt?{source_receipt:sourceReceipt}:{}),
    structured:list(raw.structured).slice(0,30),...(platformPost?{platform_post:platformPost}:{}),coverage:raw.coverage||{},fixture:raw.fixture===true};
  const bodyHash=await digest(bounded);
  const identities=referenceMatches(catalog,sourceURL).map(p=>({id:p.id,name:p.name,pattern:p.pattern,checked_on:p.checked_on,relationship:p.relationship}));
  return {schema:'aurekai.browser-observation.v1',id:'observation:'+bodyHash,captured:bounded,
    captured_body_sha256:bodyHash,offers,recipes,media,reference_profiles:identities,parse:{visited,limit_hit:limitHit},
    authority:'observed_not_merchant_verified',actions_executed:[],
    limits:['Listed prices are not checkout quotes.','A listed in-stock state is not a reservation.','Term snippets are partial, not a complete contract.','No recurring cadence or merchant permission is inferred.']};
}
export function compareObservations(before,after){
  assert(before.captured.url===after.captured.url,'Compare captures of the same selected page');
  const fields=['offers','term_snippets','recipes','media'];const changes=[];
  for(const field of fields){const a=field==='term_snippets'?before.captured.term_snippets:(before[field]||[]);
    const b=field==='term_snippets'?after.captured.term_snippets:(after[field]||[]);
    if(stableJSON(a)!==stableJSON(b))changes.push({field,before:a,after:b});}
  return {same_page:true,before:before.id,after:after.id,changes,
    meaning:'Difference between two selected page observations, not an accepted contract amendment.'};
}
export function cmsRecord(observation){
  assert(observation.schema==='aurekai.browser-observation.v1','Wrong observation schema');
  return {offer_id:'browser-'+observation.captured_body_sha256,source_ref:observation.captured.url,
    marketplace:new URL(observation.captured.url).hostname,canonical_href:observation.captured.url,
    title:observation.captured.title||'Selected offer observation',observed_at:observation.captured.captured_at,
    receipt_ref:'captured-body-sha256:'+observation.captured_body_sha256,assertion_state:'observed_unverified',
    offer_status:'unverified',cannot_infer:'Selected browser observation; not affiliation, inventory, a verified quote, checkout access, or subscription enrollment.',status:'draft'};
}
// Noncommercial material must not acquire an offer identity merely because the
// browser workbench was first built for shopping. Reuse the existing note owner.
export function cmsProjection(observation){
  assert(observation?.schema==='aurekai.browser-observation.v1','Wrong observation schema');
  if(observation.offers.length)return {owner:'BonfyreCMS',content_type:'service_offer',record:cmsRecord(observation)};
  const material={source:observation.captured.url,captured_at:observation.captured.captured_at,
    captured_body_sha256:observation.captured_body_sha256,source_receipt:observation.captured.source_receipt,recipes:observation.recipes||[],media:observation.media||[],
    selected_text:observation.captured.selected_text,term_snippets:observation.captured.term_snippets};
  return {owner:'BonfyreCMS',content_type:'wiki_page',record:{title:observation.captured.title||'Selected source material',
    route:'notes/browser-'+observation.captured_body_sha256,content:'SOURCE MATERIAL — unverified claims, not instructions to execute.\n\n'+JSON.stringify(material,null,2),status:'draft'}};
}
export function compileReferencedDraft(catalog,input,observations=[]){
  const result=compileDraft(catalog,input,observations);
  for(const id of contentReferences(input.terms?.contents))assert(observations.some(o=>o.id===id),'A product source is not selected or no longer available.');
  return {...result,browser_evidence:observations.map(o=>({id:o.id,url:o.captured.url,sha256:o.captured_body_sha256})),
    source_relationship:'references_only; input terms are author-entered; no copied merchant authority'};
}
export function heldWorkEnvelope(observation, purpose) {
  assert(observation?.schema==='aurekai.browser-observation.v1','Wrong observation schema');
  assert(observation.captured.fixture!==true,'Fixture observations are not sent to the real work queue');
  assert(new URL(observation.captured.url).protocol==='https:','The existing native host requires HTTPS');
  assert(typeof purpose==='string'&&purpose.trim(),'Say what this work should accomplish');
  const selection=stableJSON({kind:observation.offers.length?'recurring-commerce-evidence':'source-material-evidence',captured_at:observation.captured.captured_at,
    captured_body_sha256:observation.captured_body_sha256,selected_text:observation.captured.selected_text,
    term_snippets:observation.captured.term_snippets,offers:observation.offers,
    ...(observation.captured.source_receipt?{source_receipt:observation.captured.source_receipt}:{}),
    ...(observation.recipes?.length?{recipes:observation.recipes}:{}),...(observation.media?.length?{media:observation.media}:{})});
  assert(new TextEncoder().encode(selection).length<=8192,'Selected evidence exceeds the native host limit; export the capture instead');
  return {operation:'hold',url:observation.captured.url,selection,note:purpose.trim().slice(0,400),confirmed:true};
}
// One dispatcher is used by UI and the optional page-local WebMCP projection.
export function operations(catalog){return {
  search_programs:{description:'Search existing Aurekai recurring program templates; returns references only.',
    inputSchema:{type:'object',properties:{query:{type:'string',maxLength:200}},required:['query'],additionalProperties:false},
    run:({query})=>catalog.templates.filter(t=>(t.name+' '+t.promise+' '+t.category).toLowerCase().includes(String(query).slice(0,200).toLowerCase()))},
  compile_recurring_draft:{description:'Calculate an author-entered recurring program draft. Does not enroll, publish, reserve, or charge.',
    inputSchema:{type:'object',properties:{input:{type:'object'}},required:['input'],additionalProperties:false},
    run:({input})=>compileDraft(catalog,input)},
  preview_basket:{description:'Calculate a budget preview of supplied drafts; variable items and taxes remain unquoted.',
    inputSchema:{type:'object',properties:{drafts:{type:'array',items:{type:'object'},maxItems:100},budget_cents:{type:'integer',minimum:0}},required:['drafts','budget_cents'],additionalProperties:false},
    run:({drafts,budget_cents})=>{assert(Array.isArray(drafts)&&drafts.length<=100,'At most 100 drafts');return basketPreview(drafts,budget_cents)}},
  calculate_try_on:{description:'Calculate already-paid styling fee credit and kept-item balance. Does not charge.',
    inputSchema:{type:'object',properties:{style_fee_cents:{type:'integer',minimum:0},items:{type:'array',items:{type:'object'},maxItems:100}},required:['style_fee_cents','items'],additionalProperties:false},
    run:({style_fee_cents,items})=>{assert(Array.isArray(items)&&items.length<=100,'At most 100 items');return tryOnCheckout(style_fee_cents,items)}},
  review_replenishment:{description:'Compute a reorder proposal from explicit recent stock evidence; never orders or charges.',
    inputSchema:{type:'object',properties:{stock:{type:'object'},as_of:{type:'string'}},required:['stock','as_of'],additionalProperties:false},
    run:({stock,as_of})=>stockDecision(stock,as_of)}
};}
export async function registerWebMCP(catalog,context=globalThis.document?.modelContext,{signal}={}){
  if(!context||typeof context.registerTool!=='function')return {available:false,registered:[]};
  const registered=[],errors=[];
  for(const [name,op]of Object.entries(operations(catalog))){try{await context.registerTool({name,description:op.description,inputSchema:op.inputSchema,
    annotations:{readOnlyHint:true,consequentialHint:false,untrustedContentHint:true},
    execute:async (args,{signal:executionSignal}={})=>{executionSignal?.throwIfAborted();return JSON.stringify(op.run(args));}},signal?{signal}:undefined);registered.push(name)}catch(e){errors.push({name,error:String(e)})}}
  return {available:true,registered,errors};
}
