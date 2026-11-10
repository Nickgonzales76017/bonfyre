#!/usr/bin/env bun
// Authenticated web/companion projection of the existing enrollment Queue.
// This owner binds a reviewed local lane to a current Aurekai destination; it
// neither signs into a provider nor turns a public profile link into OAuth.
import {readFileSync,lstatSync} from 'node:fs';
import {homedir} from 'node:os';
import {join} from 'node:path';
import {createHash} from 'node:crypto';
import {encode,decode} from '../workspace-msgpack.mjs';
import {providerBinding,sameProviderBinding,aurekaiOrigins} from '../provider-contract.mjs';
import {providerAuthCapability} from '../provider-auth-capability.mjs';
import * as enrollment from './enrollment.mjs';
import {loadConnectionCatalog,providerCredentialBinding,connectionFields,extraEnrollmentRows,readOnlySources} from './connection-catalog.mjs';
const sha=value=>createHash('sha256').update(value).digest('hex');
const check=(ok,message,code='invalid_request')=>{if(!ok)throw Object.assign(Error(message),{code});};
const record=value=>value!==null&&typeof value==='object'&&!Array.isArray(value);
const exact=(value,keys)=>record(value)&&Object.keys(value).every(k=>keys.includes(k));
const MAX=512*1024;
export function readAssignments(path){
 check(typeof path==='string'&&path.startsWith('/'),'Your account connection plan has not been assigned.','unavailable');
 const st=lstatSync(path);check(st.isFile()&&!st.isSymbolicLink()&&(st.mode&0o022)===0&&st.size<65536,'The native account-plan assignment is unavailable.','unavailable');
 const value=decode(new Uint8Array(readFileSync(path)));
 check(Array.isArray(value)&&value.length===2&&value[0]===1&&Array.isArray(value[1])&&value[1].length<=100,'The native plan assignment is invalid.');
 for(const row of value[1])check(exact(row,['binding','identities'])&&providerBinding(row.binding)&&Array.isArray(row.identities)&&row.identities.length>0&&row.identities.length<=2&&new Set(row.identities).size===row.identities.length&&row.identities.every(i=>['gmail','proton'].includes(i)),'The account-plan assignment is incomplete.');
 const seen=new Set();for(const row of value[1]){const key=JSON.stringify(row.binding);check(!seen.has(key),'Duplicate account-plan assignment.');seen.add(key);}
 return value[1];
}
export function enrollmentReview(binding,identity,view){
 return {owner:'BonfyreEnrollment',context:binding.scope==='self'?'self':binding.subject,identity,workId:view.item.work_id,
  jobId:sha('bonfyre-enrollment:'+view.item.work_id),revision:view.revision+1,binding};
}
export class AccountConnections{
 constructor(binding,{assignments,config=enrollment.config(),clock=Date.now}={}){
  check(providerBinding(binding),'Current account authority is required.','forbidden');
  this.binding=Object.freeze({...binding});this.clock=clock;
  const matches=(assignments??readAssignments(process.env.BONFYRE_CONNECTION_BINDINGS||join(homedir(),'.bonfyre/onboarding/account-bindings.mpk'))).filter(row=>sameProviderBinding(row.binding,binding));
  check(matches.length===1,'No provider setup plan is assigned to this destination.','forbidden');
  this.identities=[...matches[0].identities];this.catalog=loadConnectionCatalog(config.catalog);
  this.config={...config,resolveProvider:item=>providerCredentialBinding(item,config)};
 }
 lane(identity){check(this.identities.includes(identity),'This provider identity is not assigned to the current account.','forbidden');return identity;}
 entry(id){const row=this.catalog.find(r=>r.id===id);check(row,'This provider is not in the assigned plan.');return row;}
 inspect(identity,workId){
  this.lane(identity);check(typeof workId==='string'&&/^\d{3}-(gmail|proton)$/.test(workId)&&workId.endsWith('-'+identity),'Choose one exact saved provider setup.');
  const view=enrollment.inspect(identity,workId,this.config),entry=this.entry(workId.slice(0,3));
  check(view.item.platform===entry.name&&view.item.signup_url===entry.url,'The native provider entry changed; review its source.','version_conflict');
  const fields=connectionFields(view.item,this.config),review=enrollmentReview(this.binding,identity,view),auth=providerAuthCapability(entry,{credentialFields:fields});
  const providerReady=auth.methods.some(method=>method.available&&['portal','api_key','app_password','personal_access_token','bot_token','client_credentials','signed_token','webhook'].includes(method.strategy));
  return {...view,binding:this.binding,identity,entry:{...entry,auth},review,credential_fields:fields,credential_fields_available:fields.length>0,
   providerReady,identityBrowser:identity==='proton'?'incognito':'regular',
   connectionState:view.missing?.length?'setup_incomplete':'native_milestones_complete',
   canStore:fields.length>0&&view.status==='running',
   cannot_infer:'A provider tab, recorded observation or stored field does not establish valid provider access. The final verification and receiving-operation milestones remain separate.'};
 }
 list(identity){
  this.lane(identity);let cursor='',items=[];
  for(let page=0;page<30;page++){
   const result=enrollment.list(identity,{filter:'all',cursor},this.config);
   if(!result.seeded)break;
   for(const row of result.items){const id=row.item.work_id.slice(0,3),entry=this.entry(id);check(entry.name===row.item.platform,'The native setup identity changed.');
    items.push({id,workId:row.item.work_id,revision:row.revision,status:row.status,resumable:row.resumable,needsDecision:row.needs_decision,updated:row.updated_at_ms});}
   if(!result.more)break;check(result.cursor&&result.cursor!==cursor,'The native setup page did not advance.');cursor=result.cursor;
   if(page===29)throw Error('The native setup list exceeds its page bound.');
  }
  return{binding:this.binding,identity,identities:this.identities.map(id=>({id,label:id==='gmail'?'Gmail · regular window':'Proton · private window',account:enrollment.IDENTITIES[id]})),catalog:this.catalog,items,sourceLinks:readOnlySources,
   totalTargets:this.catalog.length,prepared:items.length,missing:this.catalog.filter(r=>!items.some(i=>i.id===r.id)).map(r=>r.id)};
 }
 reviewed(identity,review){
  check(exact(review,['owner','context','identity','workId','jobId','revision','binding'])&&review.owner==='BonfyreEnrollment'&&sameProviderBinding(review.binding,this.binding),'The account changed after review.','binding_changed');
  const view=this.inspect(identity,review.workId),fresh=view.review;
  check(review.identity===identity&&review.context===fresh.context&&review.jobId===fresh.jobId&&review.revision===fresh.revision,'This setup changed. Read its current progress before another action.','version_conflict');return view;
 }
 provider(identity,workId,revision,challenge,origin){
  const view=this.inspect(identity,workId);check(view.providerReady,'This source has no provider sign-in to continue.','unavailable');
  check(Number.isSafeInteger(revision)&&revision===view.review.revision,'This provider setup changed.','version_conflict');
  check(aurekaiOrigins.includes(origin)&&/^[a-f0-9]{64}$/.test(challenge||''),'Use the current Aurekai document and browser challenge.');
  const at=this.clock();return{purpose:'provider-continuation',challenge,origin,context:view.review.context,binding:this.binding,
   work:{owner:'BonfyreEnrollment',id:view.review.jobId,revision,scope:sha(encode([this.binding.subject,this.binding.scope])),identity,workId},
   provider:{id:'enrollment-'+view.entry.id,name:view.entry.name,url:view.entry.url},issuedAt:at,expiresAt:at+60000,
   returnPath:'/apps/connections?'+new URLSearchParams({context:view.review.context,identity,work:workId})};
 }
 change(identity,payload){
  check(exact(payload,['action','review','stage','note','why','field','secret']),'Use one declared connection operation.');
  const view=this.reviewed(identity,payload.review),expected={work_id:view.item.work_id,revision:view.revision},action=payload.action;
  check(!['retired','public'].includes(view.entry.gate),'This source does not require a new account setup.');
  if(action==='continue')enrollment.continueWork(identity,expected,this.config);
  else if(action==='record'){check(typeof payload.note==='string'&&payload.note.length<=400,'Use a bounded observation, never a password.');enrollment.record(identity,payload.stage,payload.note,this.config,expected);}
  else if(action==='wait')enrollment.wait(identity,payload.why,payload.note||'',this.config,expected);
  else if(action==='decide')enrollment.decide(identity,payload.why,payload.note||'',this.config,expected);
  else if(action==='seal'){
   check(view.credential_fields.some(f=>f.field===payload.field),'The native credential field is not declared.');
   try{enrollment.seal(identity,providerCredentialBinding(view.item,this.config),payload.field,payload.secret,this.config,expected);}catch{throw Object.assign(Error('Credential storage was not confirmed. Reload this exact setup before another attempt.'),{code:'unconfirmed'});}finally{payload.secret='';}
  }else throw Error('This action is not available from account connection setup.');
  return this.inspect(identity,view.item.work_id);
 }
 prepare(){
  // Explicit account-plan preparation only: add missing reviewed recovery items
  // through Queue. No claim, provider request or external account is created.
  const rows=[];for(const identity of this.identities)for(const item of extraEnrollmentRows(identity,enrollment.IDENTITIES[identity]))
   rows.push(['enroll-'+item.work_id,'account.enrollment',JSON.stringify(item),'enrollment-'+identity,500,40,600000].join('\t'));
  const result=Bun.spawnSync([this.config.queue,'enqueue-batch','/dev/stdin','--db',this.config.db],{stdin:Buffer.from(rows.join('\n')+'\n'),stdout:'pipe',stderr:'pipe',timeout:20000});
  check(result.exitCode===0,'The existing native Queue did not confirm plan preparation.','unconfirmed');
  return{binding:this.binding,identities:this.identities,preparedRows:rows.length,networkRequests:0};
 }
}
async function body(){let size=0,chunks=[];for await(const p of Bun.stdin.stream()){size+=p.length;check(size<=65536,'The connection request exceeds its bound.');chunks.push(p);}const data=new Uint8Array(size);let n=0;for(const p of chunks){data.set(p,n);n+=p.length;}const value=decode(data);data.fill(0);check(Array.isArray(value)&&value.length===2&&value[0]===1&&record(value[1]),'Use the bounded connection packet.');return value[1];}
if(import.meta.main){
 let answer;
 try{const args=process.argv.slice(2),input={};for(let i=0;i<args.length;i+=2){const name=args[i]?.slice(2);check(args[i]==='--'+name&&['operation','actor','subject','scope','identity','work','revision','challenge','origin'].includes(name)&&args[i+1]!==undefined&&!Object.hasOwn(input,name),'Invalid native connection coordinates.');input[name]=args[i+1];}
  const owner=new AccountConnections({actor:input.actor,subject:input.subject,scope:input.scope});let result;
  if(input.operation==='list')result=owner.list(input.identity);
  else if(input.operation==='get')result=owner.inspect(input.identity,input.work);
  else if(input.operation==='provider')result=owner.provider(input.identity,input.work,Number(input.revision),input.challenge,input.origin);
  else if(input.operation==='change')result=owner.change(input.identity,await body());
  else if(input.operation==='prepare')result=owner.prepare();else throw Error('Unsupported native connection operation.');
  answer=[1,true,result];
 }catch(error){answer=[1,false,{code:['forbidden','binding_changed','version_conflict','unavailable','unconfirmed'].includes(error.code)?error.code:'invalid_request',message:String(error.message||'The native setup was not confirmed.').slice(0,600)}];}
 await Bun.write(Bun.stdout,encode(answer,{maxBytes:MAX}));
}
