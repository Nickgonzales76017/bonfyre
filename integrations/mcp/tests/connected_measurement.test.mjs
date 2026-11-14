import {test,expect} from 'bun:test';
import {subjectForVerifiedActor,validatedMeasurements,readAccountMeasurements,readInstitutionMeasurements,MEASUREMENT_SCHEMA} from '../connected_measurement.mjs';
const digest='a'.repeat(64),at='2026-11-09T20:30:00Z';
const row=(extra={})=>({provider:'google_my_business',account_ref:'aurekai',metric:'impressions',unit:'count',
  period_start:'2026-10-01',period_end:'2026-10-31',period_kind:'month',
  value:'44.0000',source_digest:digest,source_observed_at:at,ingested_at:at,...extra});
test('native actor is the only source of account measurement scope',()=>{
 expect(subjectForVerifiedActor('auth-user:19')).toBe('user:19');
 for(const actor of ['user:19','auth-user:0','auth-user:19:admin','auth-user:-1','institution:foo'])
   expect(()=>subjectForVerifiedActor(actor)).toThrow('actor_unverified');
});
test('account measurement read binds subject, source and period without source authority or live-health claims',async()=>{
 let used;
 const request=async(url,options)=>{used={url,options};return{ok:true,headers:new Headers(),text:async()=>JSON.stringify([row()])}};
 const r=await readAccountMeasurements({actor:'auth-user:19',provider:'google_my_business',
   limit:15,baseUrl:'https://example.supabase.co',key:'x'.repeat(30),
   request,clock:Date.parse('2026-11-09T21:30:00Z')});
 expect(r).toMatchObject({schema:MEASUREMENT_SCHEMA,subject:'user:19',provider_count:1,
   no_external_write:true,current_provider_health:'not_measured'});
 expect(r.rows[0]).toMatchObject({metric:'impressions',value:44,stale:false,age_hours:1});
 expect(JSON.parse(used.options.body)).toEqual({p_subject:'user:19',p_provider:'google_my_business',p_limit:15});
 expect(used.options.headers.Authorization).toBe('Bearer '+('x'.repeat(30)));
 expect(JSON.stringify(r)).not.toContain('Bearer');
 expect(used.url).toContain('/rpc/account_measurement_portfolio');
});
test('boundaries refuse unknown instruments, duplicate cohorts, invalid metrics and unbounded replies',async()=>{
 expect(()=>validatedMeasurements([row(),row()],{subject:'user:19'})).toThrow('duplicate_coordinate');
 expect(()=>validatedMeasurements([row({metric:'views;DROP'})],{subject:'user:19'})).toThrow();
 expect(()=>validatedMeasurements([row({source_digest:'not a digest'})],{subject:'user:19'})).toThrow();
 expect(()=>validatedMeasurements([row({value:-1})],{subject:'user:19'})).toThrow();
 expect(()=>validatedMeasurements([row({provider:'youtube'})],{subject:'user:19',provider:'google_my_business'})).toThrow();
 expect(()=>validatedMeasurements([row({source_observed_at:'unknown'})],{subject:'user:19'})).toThrow();
 const req=async()=>({ok:true,headers:new Headers(),text:async()=>JSON.stringify([row()])});
 await expect(readAccountMeasurements({actor:'auth-user:19',provider:'foo/bar',baseUrl:'https://x.supabase.co',key:'a'.repeat(30),request:req})).rejects.toThrow();
 await expect(readAccountMeasurements({actor:'auth-user:19',limit:101,baseUrl:'https://x.supabase.co',key:'a'.repeat(30),request:req})).rejects.toThrow();
});
test('aging is explicit; an old observation is not classified as current service health',()=>{
 const result=validatedMeasurements([row()],{subject:'user:19',clock:Date.parse('2026-12-09T20:30:00Z')});
 expect(result.rows[0].stale).toBe(true);
 expect(result.current_provider_health).toBe('not_measured');
});
test('institution reads derive tenant subject only from current admitted native closure',async()=>{
 const estates=[{estate:'upper90',scope:'institution:upper90',inherited:false}];
 let sent;
 const request=async(url,options)=>{sent=JSON.parse(options.body);return {ok:true,headers:new Headers(),text:async()=>JSON.stringify([row()])}};
 const input={estates,estate:'upper90',provider:'google_my_business',
   baseUrl:'https://x.supabase.co',key:'x'.repeat(30),request};
 const value=await readInstitutionMeasurements(input);
 expect(value.subject).toBe('institution:upper90');
 expect(sent.p_subject).toBe('institution:upper90');
 await expect(readInstitutionMeasurements({...input,estate:'unrelated'})).rejects.toThrow('not_in_closure');
 await expect(readInstitutionMeasurements({...input,estates:[]})).rejects.toThrow('not_in_closure');
 await expect(readInstitutionMeasurements({...input,estate:'upper90;select'})).rejects.toThrow('not_in_closure');
});
