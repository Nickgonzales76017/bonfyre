// Account-owned, typed measurement read. Supabase retains a replaceable
// projection; native BonfyreAuth is the only source of the subject.
export const MEASUREMENT_SCHEMA='bonfyre.account.measurement.portfolio.v1';
const str=(x,min,max)=>typeof x==='string'&&x.length>=min&&x.length<=max;
const id=/^[a-z][a-z0-9_]{0,79}$/;
const day=/^\d{4}-\d\d-\d\d$/;
const hash=/^[a-f0-9]{64}$/;
const allowedUnits=new Set(['count','percent','usd','ms','bytes']);
const allowedPeriods=new Set(['day','week','month','custom','lifetime']);
const refuse=(name)=>{throw Error(name);};
export function subjectForVerifiedActor(actor) {
  if(!/^auth-user:[1-9][0-9]{0,9}$/.test(actor||''))refuse('account_measurement_actor_unverified');
  return 'user:'+actor.slice(10);
}
export function validatedMeasurements(rows,{subject,provider=null,limit=100,clock=Date.now()}={}){
  if(!Array.isArray(rows)||rows.length>limit||rows.length>100)refuse('account_measurement_invalid_response');
  const seen=new Set(),current=[];
  for(const r of rows){
    if(!r||typeof r!=='object'||Array.isArray(r)||!id.test(r.provider??'')||
      (provider&&r.provider!==provider)||!str(r.account_ref,1,96)||!id.test(r.metric??'')||
      !allowedUnits.has(r.unit)||!allowedPeriods.has(r.period_kind)||!day.test(r.period_start??'')||
      !day.test(r.period_end??'')||r.period_start>r.period_end||!hash.test(r.source_digest??'')||
      !Number.isFinite(Date.parse(r.source_observed_at))||!Number.isFinite(Date.parse(r.ingested_at)))
      refuse('account_measurement_invalid_response');
    const value=Number(r.value);
    if(!Number.isFinite(value)||value<0||value>1e18)refuse('account_measurement_invalid_response');
    const signature=[r.provider,r.account_ref,r.metric,r.period_start,r.period_end].join('|');
    if(seen.has(signature))refuse('account_measurement_duplicate_coordinate');
    seen.add(signature);
    const observed=Date.parse(r.source_observed_at),ageHours=Math.max(0,Math.floor((clock-observed)/3600000));
    current.push(Object.freeze({provider:r.provider,account:r.account_ref,metric:r.metric,
      unit:r.unit,period:{from:r.period_start,to:r.period_end,kind:r.period_kind},
      value,source_digest:r.source_digest,observed_at:r.source_observed_at,
      ingested_at:r.ingested_at,age_hours:ageHours,stale:ageHours>168}));
  }
  const providers=[...new Set(current.map(r=>r.provider))].sort();
  return Object.freeze({schema:MEASUREMENT_SCHEMA,subject,source:'replaceable_supabase_projection',
    access:'native_account_verified',rows:current,provider_count:providers.length,providers,
    no_external_write:true,current_provider_health:'not_measured',
    more_possible:rows.length===limit,source_rights:'retained_with_source_owner'});
}
export async function readAccountMeasurements(args={}){
  return fetchMeasurements({...args,subject:subjectForVerifiedActor(args.actor)});
}
export async function readInstitutionMeasurements({estates,estate,...args}={}){
  const found=Array.isArray(estates)&&estates.find(v=>v.estate===estate);
  if(!found||!/^[a-z0-9][a-z0-9-]{0,63}$/.test(estate))refuse('institution_measurement_not_in_closure');
  return fetchMeasurements({...args,subject:'institution:'+found.estate});
}
async function fetchMeasurements({subject,provider,limit=100,baseUrl,key,request=fetch,clock=Date.now()}={}){
  if(provider!==undefined&&!/^[a-z][a-z0-9_]{0,47}$/.test(provider))refuse('account_measurement_provider_invalid');
  if(!Number.isSafeInteger(limit)||limit<1||limit>100)refuse('account_measurement_limit_invalid');
  if(!str(baseUrl,12,512)||!/^https:\/\/[a-z0-9.-]+(?::443)?$/.test(baseUrl)||
     !str(key,20,4096))refuse('account_measurement_source_unconfigured');
  let response;
  try{
    response=await request(baseUrl+'/rest/v1/rpc/account_measurement_portfolio',{
      method:'POST',headers:{apikey:key,Authorization:'Bearer '+key,'Content-Type':'application/json',Accept:'application/json'},
      body:JSON.stringify({p_subject:subject,p_provider:provider??null,p_limit:limit}),
      redirect:'error',signal:AbortSignal.timeout(15000)
    });
  }catch{refuse('account_measurement_source_unavailable');}
  if(!response.ok)refuse('account_measurement_source_unavailable');
  if(response.headers?.get?.('content-length')>131072)refuse('account_measurement_invalid_response');
  let rows;
  try{
    const payload=await response.text();
    if(payload.length>131072)refuse('account_measurement_invalid_response');
    rows=JSON.parse(payload);
  }catch{refuse('account_measurement_invalid_response');}
  return validatedMeasurements(rows,{subject,provider:provider??null,limit,clock});
}
