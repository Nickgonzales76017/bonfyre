import {test,expect} from 'bun:test';
import {createHash} from 'node:crypto';
import {normalizeConnectedMeasurements,submitConnectedMeasurements} from './connected_measurement_ingest.mjs';
const src={provider:'google_my_business',account_ref:'aurekai'};
const receipt=createHash('sha256').update('source readback').digest('hex');
const input=()=>({actor:'auth-user:19',source:src,rows:[
 {date:'2026-11-07',impressions:2,website_clicks:0},
 {date:'2026-11-08',impressions:4,website_clicks:1}],
 measures:[{field:'impressions',metric:'impressions',unit:'count'},{field:'website_clicks',metric:'website_clicks',unit:'count'}],
 receiptDigest:receipt,sourceObservedAt:'2026-11-09T20:30:00Z',
 clock:Date.parse('2026-11-09T20:30:00Z')});
test('a reviewed connector read yields stable idempotent per-account dates without raw source bodies',()=>{
 const a=normalizeConnectedMeasurements(input()),b=normalizeConnectedMeasurements(input());
 expect(a).toEqual(b);
 expect(a.rows).toHaveLength(4);
 expect(a.rows.every(row=>row.subject==='user:19'&&row.provider==='google_my_business')).toBe(true);
 expect(new Set(a.rows.map(row=>row.observation_id)).size).toBe(4);
 expect(a.rows[0].period_kind).toBe('day');
 expect(Object.keys(a.rows[0])).not.toContain('raw');
 const another=normalizeConnectedMeasurements({...input(),actor:'auth-user:20'});
 expect(another.rows[0].observation_id).not.toBe(a.rows[0].observation_id);
});
test('unverified subject, duplicate periods and oversized or unmeasured sources are refused',()=>{
 expect(()=>normalizeConnectedMeasurements({...input(),actor:'user:19'})).toThrow();
 expect(()=>normalizeConnectedMeasurements({...input(),rows:[input().rows[0],input().rows[0]]})).toThrow('duplicate_period');
 expect(()=>normalizeConnectedMeasurements({...input(),rows:[{date:'2026-11-07',impressions:-2}]})).toThrow('value_invalid');
 expect(()=>normalizeConnectedMeasurements({...input(),receiptDigest:'bad'})).toThrow('receipt_invalid');
 expect(()=>normalizeConnectedMeasurements({...input(),source:{provider:'google_my_business',account_ref:'../foreign'}})).toThrow();
 expect(()=>normalizeConnectedMeasurements({...input(),rows:Array(2001).fill(input().rows[0])})).toThrow('out_of_bounds');
});
test('dry-run has no network effect and submitted batches use sealed service key with exact identities',async()=>{
 const prepared=normalizeConnectedMeasurements(input());
 let requests=0;
 const request=async(url,options)=>{requests++;expect(url).toContain('/rest/v1/account_measurement?on_conflict=observation_id');
   expect(options.method).toBe('POST');expect(options.headers.Prefer).toContain('ignore-duplicates');
   expect(JSON.parse(options.body)).toEqual(prepared.rows);return{ok:true,status:201}};
 const options={prepared,baseUrl:'https://example.supabase.co',serviceKey:'x'.repeat(40),request};
 expect(await submitConnectedMeasurements(options)).toMatchObject({state:'DRY_RUN',external_write:false});
 expect(requests).toBe(0);
 const done=await submitConnectedMeasurements({...options,dryRun:false});
 expect(done).toMatchObject({state:'POST_CONFIRMED',submitted:4,batches:1});
 expect(requests).toBe(1);
 expect(JSON.stringify(done)).not.toContain('x'.repeat(40));
});
test('an uncertain write never retries automatically and never reports success',async()=>{
 const prepared=normalizeConnectedMeasurements(input());
 let calls=0;
 const request=async()=>{calls++;throw Error('network timeout')};
 await expect(submitConnectedMeasurements({prepared,baseUrl:'https://example.supabase.co',
   serviceKey:'x'.repeat(40),request,dryRun:false})).rejects.toThrow('uncertain_readback_required');
 expect(calls).toBe(1);
});
