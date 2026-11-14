import {createHash} from 'node:crypto';
import {subjectForVerifiedActor} from '../mcp/connected_measurement.mjs';
// A typed, destination-owned boundary for installed connector readbacks.
// Source collectors supply reported values; BonfyreAuth supplies the actor.
// Do not substitute this for current provider connection admission.
const digest=s=>createHash('sha256').update(s).digest('hex');
const check=(ok,why)=>{if(!ok)throw Error(why)};
const atom=/^[a-z][a-z0-9_]{0,79}$/;
const date=/^\d{4}-\d\d-\d\d$/;
const account=/^[a-z0-9][a-z0-9._:-]{0,95}$/;
const units=new Set(['count','percent','usd','ms','bytes']);
const kinds=new Set(['day','week','month','custom','lifetime']);
const periodDay=s=>date.test(s)&&new Date(s+'T00:00:00Z').toISOString().slice(0,10)===s;
const unique=x=>[...new Set(x)].length===x.length;
const positive=(x)=>typeof x==='number'&&Number.isFinite(x)&&x>=0&&x<1e18;
const observedAt=(x,clock)=>Number.isFinite(Date.parse(x))&&
  Date.parse(x)<=clock+10*60*1000&&Date.parse(x)>=Date.parse('2020-01-01');
export function normalizeConnectedMeasurements({
 actor,source,rows,measures,receiptDigest,sourceObservedAt,clock=Date.now()
}={}){
 const subject=subjectForVerifiedActor(actor);
 check(source&&atom.test(source.provider)&&source.provider.length<=48&&account.test(source.account_ref),
   'connected_source_binding_invalid');
 check(Array.isArray(rows)&&rows.length>=1&&rows.length<=2000,
   'connected_rows_out_of_bounds');
 check(Array.isArray(measures)&&measures.length>=1&&measures.length<=32&&
   measures.every(m=>atom.test(m.field)&&atom.test(m.metric)&&units.has(m.unit)&&
     kinds.has(m.period_kind??'day'))&&unique(measures.map(m=>m.metric)),
   'connected_metric_declaration_invalid');
 check(typeof receiptDigest==='string'&&/^[a-f0-9]{64}$/.test(receiptDigest)&&
   observedAt(sourceObservedAt,clock),'connected_source_receipt_invalid');
 const projected=[],seen=new Set();
 for(const row of rows){
   check(row&&typeof row==='object'&&!Array.isArray(row)&&
     periodDay(row.date),'connected_source_period_invalid');
   for(const {field,metric,unit,period_kind='day'} of measures){
     if(row[field]===undefined||row[field]===null)continue;
     const value=row[field];
     check(positive(value),'connected_source_value_invalid');
     const periodStart=row.period_start??row.date,periodEnd=row.period_end??row.date;
     check(periodDay(periodStart)&&periodDay(periodEnd)&&periodStart<=periodEnd,
       'connected_source_period_invalid');
     const signature=[source.provider,source.account_ref,metric,periodStart,periodEnd].join('|');
     check(!seen.has(signature),'connected_source_duplicate_period');
     seen.add(signature);
     const source_digest=digest(['connected-source',receiptDigest,signature,unit,String(value)].join('\0'));
     const observation_id=digest(['connected-measurement',subject,source_digest].join('\0'));
     projected.push({observation_id,subject,provider:source.provider,account_ref:source.account_ref,
       metric,unit,period_start:periodStart,period_end:periodEnd,period_kind,
       value,source_digest,source_observed_at:sourceObservedAt});
   }
 }
 check(projected.length>0&&projected.length<=2000,'connected_projection_out_of_bounds');
 return Object.freeze({subject,provider:source.provider,source_receipt:receiptDigest,
   rows:projected,admission:'prepared_for_service_owner_only',public:false});
}
// This is a replaceable data projection. It must be called by an admitted
// WorkGraph/connection owner with a sealed service key; not from browser JS.
// An uncertain POST is never silently replayed. The same frozen identities
// may be reconciled through exact readback before any deliberate retry.
export async function submitConnectedMeasurements({
 prepared,baseUrl,serviceKey,request=fetch,dryRun=true
}={}){
 check(prepared?.admission==='prepared_for_service_owner_only'&&Array.isArray(prepared.rows)&&
   prepared.rows.length<=2000&&prepared.rows.every(r=>r.subject===prepared.subject),
   'connected_projection_invalid');
 if(dryRun)return {state:'DRY_RUN',subject:prepared.subject,provider:prepared.provider,
   rows:prepared.rows.length,external_write:false};
 check(typeof baseUrl==='string'&&/^https:\/\/[a-z0-9-]+\.supabase\.co$/.test(baseUrl)&&
   typeof serviceKey==='string'&&serviceKey.length>=20,
   'connected_destination_unconfigured');
 const writes=[];
 for(let i=0;i<prepared.rows.length;i+=200){
   const frozen=prepared.rows.slice(i,i+200),body=JSON.stringify(frozen);
   let response;
   try{
     response=await request(baseUrl+'/rest/v1/account_measurement?on_conflict=observation_id',{
       method:'POST',redirect:'error',
       headers:{apikey:serviceKey,Authorization:'Bearer '+serviceKey,
         'Content-Type':'application/json',Prefer:'resolution=ignore-duplicates,return=minimal'},
       body,signal:AbortSignal.timeout(30000)});
   }catch{throw Error('connected_destination_uncertain_readback_required');}
   if(!response.ok){await response.body?.cancel?.().catch?.(()=>{});
     throw Error('connected_destination_unconfirmed_status_'+response.status);}
   writes.push({first:frozen[0].observation_id,last:frozen.at(-1).observation_id,count:frozen.length});
 }
 return {state:'POST_CONFIRMED',subject:prepared.subject,provider:prepared.provider,
   submitted:prepared.rows.length,batches:writes.length,
   note:'HTTP acceptance only; reconcile the exact projection before claiming durable import',
   external_write:true};
}
