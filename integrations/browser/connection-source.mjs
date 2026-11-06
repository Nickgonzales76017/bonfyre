import {encode,decode} from './workspace-msgpack.mjs';
import {providerBinding,sameProviderBinding,providerContext,providerReview} from './provider-contract.mjs';
import {providerAuthCapability,validProviderAuthCapability} from './provider-auth-capability.mjs';
const object=x=>x!==null&&typeof x==='object'&&!Array.isArray(x);
const text=(x,max=800)=>typeof x==='string'&&x.length<=max&&!/[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]/.test(x);
const check=(v,m='The connection reply is incomplete.')=>{if(!v)throw Object.assign(Error(m),{code:'invalid_reply'});};
const failAccess=e=>e?.status===401||e?.status===403||['forbidden','binding_changed'].includes(e?.code);
const release=body=>{try{body?.cancel()?.catch(()=>{});}catch{}};
function wait(promise,signal){if(signal.aborted)return Promise.reject(signal.reason);return new Promise((yes,no)=>{const abort=()=>{signal.removeEventListener('abort',abort);no(signal.reason);};signal.addEventListener('abort',abort,{once:true});Promise.resolve(promise).then(v=>{signal.removeEventListener('abort',abort);yes(v);},e=>{signal.removeEventListener('abort',abort);no(e);});});}
export async function connectionPacket(context,identity,operation,{work,body,signal,request=fetch}={}){
 check(providerContext(context)&&['gmail','proton'].includes(identity),'Choose an exact current destination and provider identity.');
 check(['list','get','change'].includes(operation)&&((operation==='change')===(body!==undefined)),'Use one declared setup action.');
 if(operation==='get')check(typeof work==='string'&&/^\d{3}-(gmail|proton)$/.test(work)&&work.endsWith('-'+identity),'Choose the exact provider setup.');
 const q=new URLSearchParams({context,identity,operation});if(work)q.set('work',work);
 const control=new AbortController(),abort=()=>control.abort(signal.reason),timer=setTimeout(()=>control.abort(new DOMException('The current setup did not answer.','TimeoutError')),20000);let bytes,reader;
 if(signal?.aborted){clearTimeout(timer);throw signal.reason;}signal?.addEventListener('abort',abort,{once:true});
 try{
  if(body!==undefined)bytes=encode([1,body],{maxBytes:65536});
  const pending=Promise.resolve(request('/apps/_connections?'+q,{method:body===undefined?'GET':'POST',body:bytes,credentials:'same-origin',cache:'no-store',redirect:'error',signal:control.signal,headers:{Accept:'application/msgpack',...(bytes?{'Content-Type':'application/msgpack'}:{})}}));
  pending.then(r=>{if(control.signal.aborted)release(r?.body);},()=>{});const r=await wait(pending,control.signal);
  if(!r.ok){release(r.body);throw Object.assign(Error([401,403].includes(r.status)?'Sign in with current access to this workspace.':'The native connection owner did not confirm this request.'),{status:r.status,code:[401,403].includes(r.status)?'forbidden':'unconfirmed'});}
  const type=r.headers.get('content-type')?.split(';')[0].trim().toLowerCase(),length=r.headers.get('content-length');
  const encoded=r.headers.get('content-encoding'); // Fetch exposes decoded bytes, but Content-Length describes the encoded response.
  if(type!=='application/msgpack'||length!==null&&(!/^\d+$/.test(length)||+length>524288)){release(r.body);check(false,'The connection reply has an unsupported format or size.');}
  reader=r.body?.getReader();check(reader);let count=0;const parts=[];
  for(;;){const p=await wait(reader.read(),control.signal);if(p.done)break;check(p.value instanceof Uint8Array);count+=p.value.length;check(count<=524288);parts.push(p.value);}
  check(count>0&&(length===null||encoded&&encoded!=='identity'||+length===count),'The setup read was interrupted.');const data=new Uint8Array(count);let at=0;for(const p of parts){data.set(p,at);at+=p.length;}const wire=decode(data);check(Array.isArray(wire)&&wire.length===3&&wire[0]===1&&typeof wire[1]==='boolean'&&object(wire[2]));
  if(!wire[1]){const e=wire[2];check(text(e.code,50)&&text(e.message,600));throw Object.assign(Error(e.message),{code:e.code,status:['forbidden','binding_changed'].includes(e.code)?403:e.code==='version_conflict'?409:503});}
  return wire[2];
 }finally{bytes?.fill(0);try{reader?.cancel()?.catch(()=>{});}catch{}try{reader?.releaseLock();}catch{}clearTimeout(timer);signal?.removeEventListener('abort',abort);}
}
function bindingOf(value,context,expected){check(providerBinding(value.binding)&&context===(value.binding.scope==='self'?'self':value.binding.subject));if(expected&&!sameProviderBinding(value.binding,expected))throw Object.assign(Error('The account changed. Reopen the workspace.'),{code:'binding_changed',status:403});}
function entry(row,credentialFields=[]){check(object(row)&&/^\d{3}$/.test(row.id)&&text(row.name,160)&&row.name&&text(row.url,2048)&&text(row.category,100)&&text(row.note,800)&&['portal','instance','access-review','retired','public','existing-only','provider-review'].includes(row.gate));const u=new URL(row.url);check(u.protocol==='https:'&&!u.username&&!u.password&&!u.port);const auth=validProviderAuthCapability(row.auth)?row.auth:providerAuthCapability(row,{credentialFields});check(validProviderAuthCapability(auth));return {...row,auth};}
export async function readConnectionPlan(context,identity,{binding,...options}={}){
 const v=await connectionPacket(context,identity,'list',options);bindingOf(v,context,binding);
 check(v.identity===identity&&Array.isArray(v.catalog)&&v.catalog.length<=500&&Array.isArray(v.items)&&v.items.length<=500&&Array.isArray(v.identities)&&v.identities.length<=2&&Array.isArray(v.sourceLinks));
 const seen=new Set(),catalog=[];for(const row of v.catalog){const normalized=entry(row);check(!seen.has(row.id));seen.add(row.id);catalog.push(normalized);}
 const work=new Set();for(const row of v.items){check(seen.has(row.id)&&row.workId===row.id+'-'+identity&&!work.has(row.workId)&&Number.isSafeInteger(row.revision)&&row.revision>=0&&text(row.status,32));work.add(row.workId);}
 check(v.prepared===v.items.length&&v.totalTargets===v.catalog.length&&Array.isArray(v.missing)&&v.missing.length===v.catalog.length-v.items.length);
 for(const lane of v.identities)check(['gmail','proton'].includes(lane.id)&&text(lane.label,80)&&text(lane.account,320));
 for(const link of v.sourceLinks)check(text(link.title,160)&&text(link.description,500)&&['/discover','/watch','/listen','/sources/web','/s/models/all','/apps'].includes(link.path));return {...v,catalog};
}
function setup(v,context,identity,work,binding){
 bindingOf(v,context,binding);check(v.identity===identity&&v.item?.work_id===work&&v.item.identity===identity&&providerReview(v.review)&&v.review.owner==='BonfyreEnrollment'&&sameProviderBinding(v.binding,v.review.binding)&&v.review.workId===work&&v.review.revision===v.revision+1&&v.review.context===context&&Array.isArray(v.credential_fields)&&v.credential_fields.length<=8&&object(v.stages)&&Array.isArray(v.missing));
 const normalizedEntry=entry(v.entry,v.credential_fields);check(normalizedEntry.id===work.slice(0,3)&&v.item.signup_url===normalizedEntry.url&&v.item.platform===normalizedEntry.name);
 const seen=new Set();for(const f of v.credential_fields){check(text(f.field,64)&&/^[a-z0-9_]+$/.test(f.field)&&!seen.has(f.field)&&text(f.label,160)&&typeof f.sensitive==='boolean'&&typeof f.required==='boolean');seen.add(f.field);}return {...v,entry:normalizedEntry};
}
export async function readConnectionSetup(context,identity,work,{binding,...options}={}){return setup(await connectionPacket(context,identity,'get',{...options,work}),context,identity,work,binding);}
export async function changeConnectionSetup(context,identity,body,options={}){
 check(providerReview(body?.review)&&body.review.owner==='BonfyreEnrollment'&&body.review.identity===identity,'Review this exact setup before changing it.');
 const v=await connectionPacket(context,identity,'change',{...options,body});return setup(v,context,identity,body.review.workId,body.review.binding);
}
export function createConnectionState({context='self',identity='gmail',request=fetch}={}){
 let state={context,identity,plan:null,detail:null,loading:false,busy:false,error:'',uncertain:false,locked:false},epoch=0,controller=null,disposed=false;const listeners=new Set();
 function emit(){const copy={...state};for(const f of listeners)f(copy);}function publish(next){state={...state,...next};emit();}
 function clear(error){publish({plan:null,detail:null,error,locked:true,loading:false,busy:false,uncertain:false});}
 async function read(work){if(disposed||state.locked||state.busy)return;const ticket=++epoch;controller?.abort();controller=new AbortController();const signal=controller.signal;publish({loading:true,error:''});
  try{const value=work?await readConnectionSetup(state.context,state.identity,work,{binding:state.plan?.binding,request,signal}):await readConnectionPlan(state.context,state.identity,{binding:state.plan?.binding,request,signal});if(disposed||ticket!==epoch)return;
   publish(work?{detail:value,loading:false,error:'',uncertain:false}:{plan:value,detail:null,loading:false,error:'',uncertain:false});
  }catch(e){if(disposed||ticket!==epoch||signal.aborted)return;if(failAccess(e))clear(e.message);else publish({loading:false,error:e.message});}
 }
 return{
  get snapshot(){return {...state};},subscribe(fn){listeners.add(fn);fn({...state});return()=>listeners.delete(fn);},refresh(){return read(state.detail?.item.work_id);},load(){return read();},inspect(work){return read(work);},
  async identity(next){if(!['gmail','proton'].includes(next)||disposed||state.busy)return;epoch++;controller?.abort();state={...state,identity:next,plan:null,detail:null,error:'',uncertain:false,busy:false,locked:false};emit();return read();},
  async change(action,values={}){if(disposed||state.locked||state.loading||state.busy||state.uncertain||!state.detail)throw Error('Read the current setup before another change.');const review=state.detail.review,ticket=++epoch;controller?.abort();controller=new AbortController();const signal=controller.signal;publish({busy:true,error:''});
   try{const value=await changeConnectionSetup(state.context,state.identity,{action,review,...values},{request,signal});if(disposed||ticket!==epoch)return;publish({detail:value,busy:false,uncertain:false});return value;}
   catch(e){if(disposed||ticket!==epoch||signal.aborted)return;if(failAccess(e))clear(e.message);else publish({busy:false,uncertain:!['version_conflict','invalid_request'].includes(e.code),error:e.message});throw e;}
   finally{if(Object.hasOwn(values,'secret'))values.secret='';}
  },
  invalidate(message='Current account access must be checked again.'){epoch++;controller?.abort();clear(message);},
  destroy(){disposed=true;epoch++;controller?.abort();state={context,identity,plan:null,detail:null,locked:true};listeners.clear();}
 };
}
