// One native admission vocabulary for the existing SourceFrontier owner and
// BrowserHost. Provider browser state never proves an API-key connection.
export const PROVIDER_ADMISSION_BYTES=65536;
export const PROVIDER_ADMISSION_MS=60000;
export const aurekaiOrigins=Object.freeze(['https://aurekai.work','https://aurekai.life','https://aurekai.live','https://aurekai.shop']);
// Official entry links checked 2026-10-21:
// https://exa.ai/docs/get-started/agent-skills/build-with-exa
// https://docs.firecrawl.dev/introduction (API key link)
export const providerEntries=Object.freeze({
 exa:Object.freeze({id:'exa',name:'Exa',url:'https://dashboard.exa.ai/api-keys'}),
 firecrawl:Object.freeze({id:'firecrawl',name:'Firecrawl',url:'https://www.firecrawl.dev/app/api-keys'})
});
const object=value=>value!==null&&typeof value==='object'&&!Array.isArray(value);
const own=(value,fields)=>object(value)&&Object.keys(value).every(key=>fields.includes(key));
export const providerHex=value=>typeof value==='string'&&/^[a-f0-9]{64}$/.test(value);
export const providerRevision=value=>Number.isSafeInteger(value)&&value>0&&value<=999999999999999;
export const providerContext=value=>value==='self'||typeof value==='string'&&/^(?:institution|household):[a-z0-9-]{1,63}$/.test(value);
export function providerBinding(value){
 if(!own(value,['actor','subject','scope'])||typeof value.actor!=='string'||typeof value.subject!=='string'||!/^user:[1-9][0-9]*$/.test(value.actor)||!['self','institution','household'].includes(value.scope))return false;
 return value.scope==='self'?/^user:[1-9][0-9]*$/.test(value.subject)&&value.subject===value.actor:
  new RegExp('^'+value.scope+':[a-z0-9-]{1,63}$').test(value.subject);
}
export const sameProviderBinding=(a,b)=>providerBinding(a)&&providerBinding(b)&&a.actor===b.actor&&a.subject===b.subject&&a.scope===b.scope;
export function providerReview(value){
 if(value?.owner==='BonfyreEnrollment')return own(value,['owner','context','identity','workId','jobId','revision','binding'])&&['gmail','proton'].includes(value.identity)&&typeof value.workId==='string'&&/^\d{3}-(gmail|proton)$/.test(value.workId)&&value.workId.endsWith('-'+value.identity)&&providerContext(value.context)&&providerHex(value.jobId)&&providerRevision(value.revision)&&providerBinding(value.binding)&&value.context===(value.binding.scope==='self'?'self':value.binding.subject);
 return own(value,['context','jobId','revision','binding'])&&providerContext(value.context)&&providerHex(value.jobId)&&providerRevision(value.revision)&&providerBinding(value.binding)&&value.context===(value.binding.scope==='self'?'self':value.binding.subject);
}
export function providerReturnPath(review){
 if(!providerReview(review))throw Error('Use a current provider review.');
 return review.owner==='BonfyreEnrollment'?'/apps/connections?'+new URLSearchParams({context:review.context,identity:review.identity,work:review.workId}):'/sources/web?'+new URLSearchParams({context:review.context,job:review.jobId});
}
export function providerReturnValid(path,origin,job){
 try{const u=new URL(path,origin);if(u.origin!==origin||u.hash||u.username||u.password||!providerContext(u.searchParams.get('context')))return false;
  if(u.pathname==='/sources/web')return u.searchParams.size===2&&u.searchParams.get('job')===job&&path==='/sources/web?'+new URLSearchParams({context:u.searchParams.get('context'),job});
  const identity=u.searchParams.get('identity'),work=u.searchParams.get('work');
  return u.pathname==='/apps/connections'&&u.searchParams.size===3&&['gmail','proton'].includes(identity)&&/^\d{3}-(gmail|proton)$/.test(work||'')&&work.endsWith('-'+identity)&&path==='/apps/connections?'+new URLSearchParams({context:u.searchParams.get('context'),identity,work});
 }catch{return false;}
}
export function providerCapability({mode,archived,provider,canAdminister=false}){
 const entry=providerEntries[provider],canContinue=!!entry&&mode==='account'&&!archived;
 return {canContinue,canAdminister:canContinue&&canAdminister,modes:canContinue?['provider_tab_sidepanel']:[],reason:canContinue?'':archived?'Restore this run before continuing provider work.':'This run uses public provider access.',provider:entry?{id:entry.id,name:entry.name}:null,credentialInput:canContinue&&canAdminister?'native_api_key':null};
}
export function validateProviderAdmission(value,{origin,review,challenge,now=Date.now()}={}){
 const fail=(message,code)=>{throw Object.assign(Error(message),code?{code}:{});};
 if(!aurekaiOrigins.includes(origin)||!providerReview(review)||!providerHex(challenge))fail('The provider review is not admitted.');
 if(!Array.isArray(value)||value.length!==3||value[0]!==1||value[1]!==true||!own(value[2],['purpose','challenge','origin','context','binding','work','provider','issuedAt','expiresAt','returnPath']))fail('The native provider admission is incomplete.');
 const p=value[2],entry=providerEntries[p.provider?.id];
 if(!sameProviderBinding(p.binding,review.binding))fail('The provider admission belongs to another account.','binding_changed');
 if(p.purpose!=='provider-continuation'||p.challenge!==challenge||p.origin!==origin||p.context!==review.context)fail('The provider admission belongs to another review or account.');
 if(review.owner==='BonfyreEnrollment'){
  if(!own(p.work,['owner','id','revision','scope','identity','workId'])||p.work.owner!==review.owner||p.work.identity!==review.identity||p.work.workId!==review.workId)fail('The enrollment identity changed.');
  if(!own(p.provider,['id','name','url'])||p.provider.id!=='enrollment-'+review.workId.slice(0,3)||typeof p.provider.name!=='string'||!p.provider.name||p.provider.name.length>160)fail('The enrollment provider identity changed.');
  let url;try{url=new URL(p.provider.url);}catch{fail('The enrollment provider entry is invalid.');}
  if(url.protocol!=='https:'||url.username||url.password||url.port||/[\x00-\x20\\]/.test(p.provider.url))fail('The enrollment provider entry is invalid.');
 }else{
  if(!own(p.work,['owner','id','revision','scope'])||p.work.owner!=='BonfyreSourceFrontier')fail('The native work owner changed.');
  if(!entry||!own(p.provider,['id','name','url'])||p.provider.name!==entry.name||p.provider.url!==entry.url)fail('The native provider entry is not supported.');
 }
 if(p.work.id!==review.jobId||p.work.revision!==review.revision||!providerHex(p.work.scope))fail('The native work changed after review.');
 if(!Number.isSafeInteger(p.issuedAt)||!Number.isSafeInteger(p.expiresAt)||p.expiresAt-p.issuedAt!==PROVIDER_ADMISSION_MS||p.issuedAt>now+5000||p.expiresAt<=now)fail('The provider admission expired. Read the work again.');
 const returnPath=providerReturnPath(review);
 if(p.returnPath!==returnPath)fail('The native provider return address changed.');
 return p;
}
