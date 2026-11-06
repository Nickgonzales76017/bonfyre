import {encode,decode} from './workspace-msgpack.mjs';
import {aurekaiOrigins,providerReview,providerHex,providerRevision,providerContext,providerReturnValid,validateProviderAdmission} from './provider-contract.mjs';
const PREFIX='provider-tab-',CURRENT='provider-current-';
const failure=(message,status,code)=>{throw Object.assign(Error(message),[401,403].includes(status)?{status}:{},code==='binding_changed'?{code}:{});};
const random=()=>Array.from(crypto.getRandomValues(new Uint8Array(32)),n=>n.toString(16).padStart(2,'0')).join('');
const record=value=>value!==null&&typeof value==='object'&&!Array.isArray(value);
function senderContext(sender,runtime,privateContext){
 if(sender?.id!==runtime.id||!aurekaiOrigins.includes(sender.origin)||sender.frameId!==0||!sender.documentId||sender.documentLifecycle&&sender.documentLifecycle!=='active')failure('Continue from the current top-level Aurekai page.');
 let address;try{address=new URL(sender.url);}catch{failure('The browser sender has no current address.');}
 if(address.origin!==sender.origin||address.username||address.password||!Number.isInteger(sender.tab?.id)||!Number.isInteger(sender.tab?.windowId)||sender.tab.incognito!==privateContext)failure('The browser context changed. Return to Aurekai and try again.');
 return {origin:sender.origin,tabId:sender.tab.id,windowId:sender.tab.windowId,documentId:sender.documentId,key:sender.tab.id+':'+sender.documentId};
}
function unpack(value){
 if(!Array.isArray(value)||value.length>4096||!value.every(n=>Number.isInteger(n)&&n>=0&&n<=255))return null;
 try{const r=decode(Uint8Array.from(value));if(!Array.isArray(r)||r.length!==11||r[0]!==1||!providerHex(r[1])||!providerHex(r[2])||!providerRevision(r[3])||![r[4],r[5],r[8]].every(n=>Number.isSafeInteger(n)&&n>=0)||!aurekaiOrigins.includes(r[6])||typeof r[9]!=='string'||!r[9]||r[9].length>128||typeof r[10]!=='string'||!/^user:[1-9][0-9]*$/.test(r[10]))return null;
  if(!providerReturnValid(r[7],r[6],r[2]))return null;return r;
 }catch{return null;}
}
// Called in Chrome's isolated world, with the exact document ID pinned by the
// service worker. A web page supplies coordinates only, never a packet or URL.
export async function readProviderInDocument({origin,review,challenge}){
 if(window.top!==window||location.origin!==origin)return {ok:false,reason:'The source document changed.'};
 const enrollment=review.owner==='BonfyreEnrollment';
 const query=new URLSearchParams(enrollment?{operation:'provider',context:review.context,identity:review.identity,work:review.workId,revision:String(review.revision),challenge}:{operation:'provider',context:review.context,job:review.jobId,revision:String(review.revision),challenge});
 const endpoint=enrollment?'/apps/_connections':'/sources/_web';
 const controller=new AbortController(),timer=setTimeout(()=>controller.abort(),15000);
 let reader;
 try{
  const response=await fetch(endpoint+'?'+query,{credentials:'same-origin',cache:'no-store',redirect:'error',headers:{Accept:'application/msgpack'},signal:controller.signal});
  if(!response.ok){await response.body?.cancel();return {ok:false,status:response.status,reason:[401,403].includes(response.status)?'Current destination access was refused.':'The native provider owner is unavailable.'};}
  const type=response.headers.get('content-type')?.split(';')[0].trim().toLowerCase(),length=response.headers.get('content-length');
  if(type!=='application/msgpack'||length!==null&&(!/^\d+$/.test(length)||Number(length)>65536)){await response.body?.cancel();return {ok:false,reason:'The native provider response is not a bounded MessagePack packet.'};}
  reader=response.body?.getReader();if(!reader)return {ok:false,reason:'The native provider response is empty.'};
  const chunks=[];let size=0;for(;;){const part=await reader.read();if(part.done)break;size+=part.value.byteLength;if(size>65536)throw Error('Provider response exceeded its limit.');chunks.push(part.value);}
  if(window.top!==window||location.origin!==origin||!size)return {ok:false,reason:'The source document changed.'};
  const packet=new Uint8Array(size);let at=0;for(const chunk of chunks){packet.set(chunk,at);at+=chunk.length;}return {ok:true,packet:Array.from(packet)};
 }catch{return {ok:false,reason:'The native provider read did not finish. No provider tab was changed.'};}
 finally{clearTimeout(timer);try{await reader?.cancel();}catch{}try{reader?.releaseLock();}catch{}}
}
export function createProviderBridge({chrome,ask,privateContext=false,nonce=random,now=Date.now}={}){
 const epochs=new Map(),serial=new Map(),prepared=new Set();
 const currentEpoch=c=>epochs.get(c.key)||0;
 async function pinned(c){
  const results=await chrome.scripting.executeScript({target:{tabId:c.tabId,documentIds:[c.documentId]},world:'ISOLATED',func:origin=>window.top===window&&location.origin===origin,args:[c.origin]});
  if(results.length!==1||results[0].frameId!==0||results[0].documentId!==c.documentId||results[0].result!==true)failure('The Aurekai document changed. No provider tab was changed.');
 }
 async function context(c){
  const tab=await chrome.tabs.get(c.tabId),w=await chrome.windows.get(c.windowId);
  if(tab.windowId!==c.windowId||tab.incognito!==privateContext||w.incognito!==privateContext||w.type!=='normal')failure('Return to the original browser window.');
  await pinned(c);
 }
 function locked(key,work){const previous=serial.get(key)||Promise.resolve(),task=previous.catch(()=>{}).then(work);serial.set(key,task);task.finally(()=>{if(serial.get(key)===task)serial.delete(key);}).catch(()=>{});return task;}
 async function continueWork(c,review,panel){
  if(review.owner==='BonfyreEnrollment'&&(review.identity==='proton')!==privateContext)failure(review.identity==='proton'?'Open this setup in an incognito window with the companion enabled.':'Open this setup in your regular browser window.');
  const generation=currentEpoch(c),challenge=nonce();if(!providerHex(challenge))failure('A secure browser challenge is unavailable.');
  await context(c);
  const results=await chrome.scripting.executeScript({target:{tabId:c.tabId,documentIds:[c.documentId]},world:'ISOLATED',func:readProviderInDocument,args:[{origin:c.origin,review,challenge}]});
  if(results.length!==1||results[0].frameId!==0||results[0].documentId!==c.documentId||!results[0].result?.ok)failure(results[0]?.result?.reason||'The native provider read was refused.',results[0]?.result?.status);
  const native=await ask('provider_admit',{origin:c.origin,review,challenge,packet:results[0].result.packet,confirmed:true});
  if(!native?.ok)failure(native?.reason||'The native host did not admit provider continuation.',native?.status,native?.code);
  const p=validateProviderAdmission([1,true,native.result],{origin:c.origin,review,challenge,now:now()}),key=PREFIX+p.work.scope+'-'+p.work.id+'-'+p.binding.actor;
  return locked(key,async()=>{
   if(currentEpoch(c)!==generation)failure('This Aurekai presentation was cleared. Read the work again.');
   await context(c);
   if(currentEpoch(c)!==generation||p.expiresAt<=now())failure('The provider admission is no longer current.');
   const values=await chrome.storage.session.get(key),old=unpack(values[key]);
   if(values[key]&&!old)failure('The retained browser reference needs review; no tab was changed.');
   let tab=old?await chrome.tabs.get(old[4]).catch(()=>null):null,created=false;
   if(currentEpoch(c)!==generation)failure('The work presentation changed. No provider tab was changed.');
   if(tab&&(old[1]!==p.work.scope||old[2]!==p.work.id||old[10]!==p.binding.actor||tab.windowId!==c.windowId||tab.incognito!==privateContext))failure('This work has a provider tab in another account or window. Return there; its tab was left untouched.');
   if(!tab){
    // Save the tab identity before any provider navigation. A lost acknowledgment
    // can leave only an inert blank tab, never an untracked second provider tab.
    tab=await chrome.tabs.create({windowId:c.windowId,url:'about:blank',active:true});created=true;
    if(tab.windowId!==c.windowId||tab.incognito!==privateContext){await chrome.tabs.remove(tab.id).catch(()=>{});failure('Chrome created the new tab in another context; it was closed.');}
   }
   const saved=[1,p.work.scope,p.work.id,p.work.revision,tab.id,c.windowId,c.origin,p.returnPath,c.tabId,c.documentId,p.binding.actor];
   try{await chrome.storage.session.set({[key]:Array.from(encode(saved))});}
   catch(error){if(created)await chrome.tabs.remove(tab.id).catch(()=>{});throw error;}
   await context(c);
   if(currentEpoch(c)!==generation||p.expiresAt<=now()){
    if(created)await chrome.tabs.remove(tab.id).catch(()=>{});
    failure('The provider review changed before navigation. No provider work was completed.');
   }
   // A retained redirect or form is never replaced. Only a newly retained blank
   // tab may receive the native owner's admitted initial provider entry.
   if(created||tab.url==='about:blank')await chrome.tabs.update(tab.id,{url:p.provider.url,active:true});
   else await chrome.tabs.update(tab.id,{active:true});
   await chrome.storage.session.set({[CURRENT+c.windowId+'-'+tab.id]:Array.from(encode(saved))});
   return {state:created?'provider_open':'provider_focused',presentation:'provider_tab_sidepanel',companion:await panel?'open':'toolbar_required',binding:p.binding,work:{id:p.work.id,revision:p.work.revision},provider:{id:p.provider.id,name:p.provider.name}};
  });
 }
 function handle(message,sender){
  const c=senderContext(sender,chrome.runtime,privateContext);
  if(!record(message)||message.channel!=='aurekai-provider-v1'||!['capabilities','continue','clear'].includes(message.operation)||Object.keys(message).some(k=>!['channel','operation','review','gesture'].includes(k)))failure('The browser request is not supported.');
  if(message.operation==='capabilities')return chrome.sidePanel.setOptions({path:'panel.html?provider=1',enabled:true}).then(()=>{prepared.add(c.key);return {available:true,modes:['provider_tab_sidepanel']};});
  if(message.operation==='clear'){
   epochs.set(c.key,currentEpoch(c)+1);prepared.delete(c.key);return chrome.storage.session.get(null).then(async values=>{const keys=[];for(const [key,value]of Object.entries(values)){if(!key.startsWith(CURRENT))continue;const current=unpack(value);if(current?.[5]===c.windowId&&current[8]===c.tabId)keys.push(key);}if(keys.length)await chrome.storage.session.remove(keys);return {cleared:true,workUnchanged:true};});
  }
  if(message.gesture!==true||!providerReview(message.review))failure('Choose an exact current work card with a browser gesture.');
  // This call must remain synchronous with the content-script gesture. Opening
  // the companion alone grants no work or provider authority.
  // Chrome consumes this gesture before an awaited setOptions finishes. The
  // harmless provider-only view is prepared by the card's capability read. If
  // the worker restarted meanwhile, prepare it but require the toolbar gesture.
  let panel;try{panel=prepared.has(c.key)?Promise.resolve(chrome.sidePanel.open({windowId:c.windowId})).then(()=>true,()=>false):chrome.sidePanel.setOptions({path:'panel.html?provider=1',enabled:true}).then(()=>{prepared.add(c.key);return false;},()=>false);}catch{panel=Promise.resolve(false);}
  return continueWork(c,message.review,panel);
 }
 async function replaced(added,removed){
  const values=await chrome.storage.session.get(null),tab=await chrome.tabs.get(added).catch(()=>null);if(!tab||tab.incognito!==privateContext)return;
  for(const [key,value]of Object.entries(values)){if(!key.startsWith(PREFIX)&&!key.startsWith(CURRENT))continue;const r=unpack(value);if(r?.[4]!==removed)continue;r[4]=added;r[5]=tab.windowId;const next=key.startsWith(CURRENT)?CURRENT+tab.windowId+'-'+added:key;await chrome.storage.session.set({[next]:Array.from(encode(r))});if(next!==key)await chrome.storage.session.remove(key);}
 }
 async function returnToAurekai(windowId){
  if(!Number.isSafeInteger(windowId)||windowId<0)failure('Use the original browser window.');
  const w=await chrome.windows.get(windowId);if(w.type!=='normal'||w.incognito!==privateContext)failure('Use the original browser window.');
  const active=await chrome.tabs.query({windowId,active:true});
  if(active.length!==1||active[0].incognito!==privateContext)failure('Select the provider tab for this setup before returning to Aurekai.');
  const providerTab=active[0].id,values=await chrome.storage.session.get(null),matches=[];
  // The window's last-opened work can belong to a different provider tab.
  // Resolve the attachment Chrome is showing now and refuse an ambiguous map.
  for(const [key,value]of Object.entries(values)){
   if(!key.startsWith(PREFIX))continue;const saved=unpack(value);
   if(saved?.[4]===providerTab&&saved[5]===windowId){
    if(key!==PREFIX+saved[1]+'-'+saved[2]+'-'+saved[10])failure('The retained browser reference needs review; no tab was changed.');
    matches.push(saved);
   }
  }
  if(matches.length!==1)failure(matches.length?'This provider tab has more than one retained setup. Reopen the exact work card in Aurekai.':'Select the provider tab for this setup before returning to Aurekai.');
  const r=matches[0];
  const currentKey=CURRENT+windowId+'-'+providerTab,presentationKey=Object.hasOwn(values,currentKey)?currentKey:CURRENT+windowId,presentation=unpack(values[presentationKey]);
  if(!presentation||!r.every((value,index)=>presentation[index]===value))failure('Reopen this provider setup from its Aurekai work card before returning.');
  const stillActive=async()=>{const tab=await chrome.tabs.get(providerTab).catch(()=>null);if(!tab||!tab.active||tab.windowId!==windowId||tab.incognito!==privateContext)failure('The active provider tab changed. Select it again before returning to Aurekai.');const latest=await chrome.storage.session.get(presentationKey),current=unpack(latest[presentationKey]);if(!current||!r.every((value,index)=>current[index]===value))failure('This provider presentation changed. Reopen its Aurekai work card.');};
  let source=await chrome.tabs.get(r[8]).catch(()=>null);
  let sourceOrigin;try{sourceOrigin=new URL(source?.url).origin;}catch{}
  if(source&&source.windowId===windowId&&source.incognito===privateContext&&sourceOrigin===r[6]&&await pinned({tabId:source.id,documentId:r[9],origin:r[6]}).then(()=>true,()=>false)){await stillActive();await chrome.tabs.update(source.id,{active:true});return {returned:true,workUnchanged:true};}
  await stillActive();
  const made=await chrome.tabs.create({windowId,url:r[6]+r[7],active:true});if(made.windowId!==windowId||made.incognito!==privateContext){await chrome.tabs.remove(made.id).catch(()=>{});failure('The browser return context changed.');}
  return {returned:true,workUnchanged:true};
 }
 return {handle,replaced,returnToAurekai};
}
