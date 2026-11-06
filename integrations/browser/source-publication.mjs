// SourceCollection/World remains an atomic CMS operation. The browser retains
// only the reviewed request in memory and its opaque recovery address in the URL.
import {encode,decode} from './workspace-msgpack.mjs';
const fail=(v,m)=>{if(!v)throw Error(m);};
const utf8=new TextEncoder(),same=(a,b)=>Array.isArray(a)&&Array.isArray(b)&&a.length===b.length&&a.every((v,i)=>v===b[i]);
const key=v=>typeof v==='string'&&/^[a-f0-9]{64}$/.test(v);
export function sourcePublicationInput(value){
 fail(Array.isArray(value)&&value.length===7&&value[0]===1&&key(value[1]),'Use the exact reviewed publication request.');
 const [,id,subject,source,title,lede,character]=value;
 fail(typeof subject==='string'&&/^[A-Za-z0-9._:@/-]{1,256}$/.test(subject)&&typeof source==='string'&&/^[A-Za-z0-9._:/-]{1,256}$/.test(source),'The selected source has no usable native subject.');
 for(const [text,max]of [[title,960],[lede,7200]])fail(typeof text==='string'&&text.trim()&&utf8.encode(text).length<=max&&!/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/.test(text),'Provide a bounded publication title and introduction.');
 fail(['zine','atlas','scrapbook','dossier'].includes(character),'Choose a supported World presentation.');
 return [1,id,subject,source,title,lede,character];
}
export function selectedPublicationSource(inputs){
 if(!Array.isArray(inputs)||!inputs.length)return null;
 const first=inputs[0];if(!first.sourceSubject||!first.sourceRef)return null;
 if(!inputs.every(s=>s.sourceRef===first.sourceRef&&s.sourceSubject===first.sourceSubject))return null;
 if(!/^[A-Za-z0-9._:@/-]{1,256}$/.test(first.sourceSubject)||!/^[A-Za-z0-9._:/-]{1,256}$/.test(first.sourceRef))return null;
 return {subject:first.sourceSubject,source:first.sourceRef,label:first.title};
}
export function sourcePublicationReceipt(packet,id){
 fail(Array.isArray(packet)&&packet.length===9&&packet[0]===1&&packet[2]===id&&key(id),'The publication receipt names a different request.');
 const [,state,,collection,world,a,b,input,delta]=packet;
 fail(['committed','absent','conflict','refused','source_unavailable','unavailable','invalid'].includes(state)&&typeof delta==='string'&&['NOT_REQUESTED','UNCHANGED','DISABLED','DELIVERED','DELIVERY_FAILED'].includes(delta),'The publication receipt is incomplete.');
 fail(Number.isSafeInteger(a)&&a>=0&&Number.isSafeInteger(b)&&b>=0,'The publication record identities are invalid.');
 if(state==='committed'){
  fail(/^foreign-collection-[a-f0-9]{64}$/.test(collection)&&/^foreign-world-[a-f0-9]{64}$/.test(world)&&collection.slice(-64)===world.slice(-64)&&a>0&&b>0,'The native publication pair is incomplete.');
  sourcePublicationInput(input);fail(input[1]===id,'The saved selection belongs to a different request.');
 }else fail(a===0&&b===0&&input===null,'An incomplete publication cannot supply committed records.');
 return {state,request:id,collection,world,collectionId:a,worldId:b,input,delta};
}
async function exchange(url,options,id,request){
 const r=await request(url,{credentials:'same-origin',cache:'no-store',redirect:'error',signal:AbortSignal.timeout(15000),...options});
 if(r.status===401||r.status===403)throw Error('A current CMS publication grant is required. No account rights were changed.');
 fail(/^application\/msgpack(?:;|$)/i.test(r.headers.get('content-type')||''),'The native publication owner did not return a receipt.');
 const reader=r.body?.getReader();fail(reader,'The publication receipt is missing.');const chunks=[];let bytes=0;
 try{for(;;){const row=await reader.read();if(row.done)break;bytes+=row.value.length;if(bytes>16384){await reader.cancel();throw Error('The publication receipt exceeds its bound.');}chunks.push(row.value);}}finally{reader.releaseLock();}
 const body=new Uint8Array(bytes);let at=0;for(const chunk of chunks){body.set(chunk,at);at+=chunk.length;}
 const result=sourcePublicationReceipt(decode(body),id);
 if(r.ok)fail(['committed','absent'].includes(result.state),'An unsuccessful publication was returned as success.');
 else fail(!['committed','absent'].includes(result.state),'The publication status contradicts its receipt.');
 return result;
}
export function sourcePublicationSession(id,{request=fetch}={}){
 fail(key(id),'Use an exact publication recovery address.');let pending=false,input=null,result=null;
 const read=()=>exchange('/cms/source-publication?request='+id,{headers:{Accept:'application/msgpack'}},id,request);
 const checkResult=value=>{if(value.state==='committed'&&input&&!same(value.input,input))throw Error('This request already names a different saved publication.');result=value;return value;};
 return {
  get pending(){return pending;},get input(){return input?.slice()||null;},get result(){return result;},
  async check(){fail(!pending,'Finish the current publication request first.');pending=true;try{return checkResult(await read());}finally{pending=false;}},
  async publish(proposal){
   fail(!pending,'Finish the current publication request first.');const checked=sourcePublicationInput(proposal);fail(checked[1]===id&&(!input||same(input,checked)),'An interrupted request cannot be changed. Recover it before preparing another publication.');
   input=checked;pending=true;
   try{
    const current=checkResult(await read());if(current.state!=='absent')return current;
    try{const posted=await exchange('/cms/source-publication',{method:'POST',headers:{'Content-Type':'application/msgpack',Accept:'application/msgpack'},body:encode(input)},id,request);return checkResult(posted);}
    catch(error){try{return checkResult(await read());}catch{throw error;}}
   }finally{pending=false;}
  }
 };
}
export function publicationRecoveryId(hash=location.hash){const m=/^#source-publication=([a-f0-9]{64})$/.exec(hash);return m?m[1]:null;}
export async function openSourcePublication(source,{requestId=null,initial=null,request=fetch}={}){
 if(document.querySelector('dialog[data-source-publication]'))return;
 if(!document.querySelector('link[data-source-publication-style]')){
  const style=document.createElement('link');style.rel='stylesheet';style.href=new URL('./source-publication.css',import.meta.url).href;style.dataset.sourcePublicationStyle='';
  document.head.append(style);
 }
 const id=requestId||[...crypto.getRandomValues(new Uint8Array(32))].map(n=>n.toString(16).padStart(2,'0')).join(''),session=sourcePublicationSession(id,{request});
 const el=(tag,text)=>{const n=document.createElement(tag);if(text!==undefined)n.textContent=text;return n;};
 const dialog=el('dialog');dialog.dataset.sourcePublication='';dialog.className='source-publication-dialog';
 const heading=el('h2','Publish a source collection and World'),intro=el('p','Choose one observed source and author its public introduction. The World can display eligible public material from that source, not only the excerpts selected for your private research.');
 const sourceLabel=el('p',source?'Source: '+source.source:'Recovering the original publication…');sourceLabel.className='source-publication-coordinate';
 const title=el('input');title.maxLength=240;title.name='publication-title';const lede=el('textarea');lede.maxLength=1800;lede.name='publication-lede';
 const character=el('select');character.name='publication-character';for(const [value,label]of [['zine','Editorial publication'],['atlas','Catalogue and comparison'],['scrapbook','Place and collection'],['dossier','Research dossier']]){const o=el('option',label);o.value=value;character.append(o);}
 if(initial){title.value=String(initial.title||'').slice(0,240);lede.value=String(initial.lede||'').slice(0,1800);if(['zine','atlas','scrapbook','dossier'].includes(initial.character))character.value=initial.character;}
 const field=(name,control)=>{const label=el('label',name);label.append(control);return label;};
 const status=el('p');status.setAttribute('role','status');status.setAttribute('aria-live','polite');
 const grant=el('p','Requires your current CMS write grant. Your research, findings, external accounts, and source licences remain unchanged.');grant.className='fine';
 const actions=el('div');actions.className='source-publication-actions';const publish=el('button','Publish reviewed source'),recover=el('button','Check saved publication'),close=el('button','Close');close.className='secondary';recover.className='secondary';actions.append(publish,recover,close);
 dialog.append(heading,intro,sourceLabel,field('Publication title',title),field('Public introduction',lede),field('World presentation',character),grant,status,actions);document.body.append(dialog);dialog.showModal();
 let busy=false,locked=!!requestId,standing='ready';
 const address=new URL(location.href);address.hash='source-publication='+id;history.replaceState(null,'',address);
 function controls(){dialog.dataset.state=standing;for(const n of [title,lede,character])n.disabled=busy||locked;publish.disabled=busy||!source||standing==='committed'||standing==='conflict'||standing==='uncertain'||(!session.input&&locked);publish.textContent=session.input?'Retry this reviewed source':'Publish reviewed source';recover.disabled=busy;close.disabled=busy;}
 function show(result){
  standing=result.state;status.replaceChildren();
  if(result.state==='committed'){
   locked=true;title.value=result.input[4];lede.value=result.input[5];character.value=result.input[6];sourceLabel.textContent='Source: '+result.input[3];
   status.append(el('span','Source collection and World are saved together. '));const link=el('a','Open publication →');link.href='/w/'+result.world;link.dataset.publishedWorld='';status.append(link);
   if(['DISABLED','DELIVERY_FAILED'].includes(result.delta))status.append(el('p','The records are committed. Downstream notification delivery was not confirmed; retrying does not create another World.'));
  }else if(result.state==='absent'){standing='ready';status.textContent=session.input?'The owner confirms that this request has no saved pair. Retry sends the same reviewed source and address.':'No publication is saved for this request. '+(source?'Review its title and introduction before publishing.':'Close, select a source and prepare a new publication.');}
  else{standing='conflict';status.textContent='The native owner refused this publication ('+result.state+'). Existing records were not overwritten. Review the source or saved CMS records before preparing another request.';}
 }
 async function run(work){if(busy)return;busy=true;controls();try{show(await work());}catch(e){standing='uncertain';status.textContent=e.message+' Use Check saved publication before retrying.';}finally{busy=false;controls();}}
 publish.onclick=()=>{if(busy)return;let input;try{input=session.input||sourcePublicationInput([1,id,source.subject,source.source,title.value.trim(),lede.value.trim(),character.value]);}catch(error){status.textContent=error.message;return;}locked=true;void run(()=>session.publish(input));};
 recover.onclick=()=>run(()=>session.check());close.onclick=()=>{if(!busy)dialog.close();};dialog.addEventListener('cancel',e=>{if(busy)e.preventDefault();});
 dialog.addEventListener('close',()=>{dialog.remove();if(standing==='committed'||(!session.input&&standing==='ready')){const next=new URL(location.href);if(publicationRecoveryId(next.hash)===id){next.hash='';history.replaceState(null,'',next);}}},{once:true});
 controls();if(requestId)await run(()=>session.check());return dialog;
}
