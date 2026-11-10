import {openDestinationStore} from './workspace-store.mjs';
import {prepareRecord} from './workspace-value.mjs';
import {prepareAdoptionReview,mountAdoptionReview,planAdoptionSelection} from './workspace-adoption-review.mjs';
const allowed=(key,family)=>typeof key==='string'&&(family==='creator-audio'?['draft:creator:']:['draft:catalogue:','fixture-plan:','product-model:']).some(prefix=>key.startsWith(prefix));
const publicSources=[['Work Samples','https://aurekai-work-samples.pages.dev/catalogue/'],['GitHub Spaces','https://nickgonzales76017.github.io/bonfyre/workspace-transfer/']];
const make=(tag,text)=>{const node=document.createElement(tag);if(text)node.textContent=text;return node;};
export function adoptionValue(row){
  // The source submission acknowledgement belongs to its original destination.
  // Preserve the plan and every fixture coordinate, not its send state.
  if(row.key.startsWith('draft:creator:')&&row.value?.files)return {...row.value,permission:'unreviewed',sourcePermission:row.value.permission??'unreviewed'};
  return row.key.startsWith('fixture-plan:')&&row.value&&typeof row.value==='object'&&Object.hasOwn(row.value,'receipt')
    ? {...row.value,receipt:null}:row.value;
}
// Exact reference grammar emitted by the existing productAssetStore. This does not
// infer a room's dependencies from labels or certify model decoding/fitment.
export function adoptionDependencies(row){
  if(!row.key.startsWith('product-model:')||!row.key.includes(':binding:'))return [];
  const match=/^product-model:([a-f0-9]{64}):binding:(.+)$/.exec(row.key),v=row.value;
  if(!match||!v||v.scope!==match[1]||typeof v.productId!=='string'||!v.productId||v.productId.length>160||encodeURIComponent(v.productId)!==match[2]||!/^[a-f0-9]{64}$/.test(v.digest??'')||!['metres','centimetres','millimetres'].includes(v.unit))
    throw Error('A model binding must retain its exact catalogue, product, unit and body reference.');
  return [`product-model:${v.scope}:body:${v.digest}`];
}
function checkedModelBody(row){
  if(!/^product-model:[a-f0-9]{64}:body:[a-f0-9]{64}$/.test(row.key))return;
  const files=row.value?.files;
  if(!Array.isArray(files)||!files.length||files.length>64||files.some(f=>typeof f?.name!=='string'||!f.name||!(f.blob instanceof Blob)))
    throw Error('The selected model body is missing its original named files.');
}
function adoptionBudget(limit){
  let bytes=0,nodes=0;const encoder=new TextEncoder();
  function visit(value,depth=0){
    if(depth>32||++nodes>100000)throw Error('The adoption review is too deeply nested.');
    if(typeof value==='string')bytes+=encoder.encode(value).length;
    else if(value&&typeof value==='object'){
      if(typeof SharedArrayBuffer!=='undefined'&&(value instanceof SharedArrayBuffer||ArrayBuffer.isView(value)&&value.buffer instanceof SharedArrayBuffer))throw Error('Shared mutable memory cannot be reviewed for adoption.');
      if(value instanceof Blob)bytes+=value.size;
      else if(value instanceof ArrayBuffer||ArrayBuffer.isView(value))bytes+=value.byteLength;
      else{
        if(!Array.isArray(value)&&![Object.prototype,null].includes(Object.getPrototypeOf(value)))throw Error('Unsupported adoption value.');
        const descriptors=Object.getOwnPropertyDescriptors(value);
        for(const key of Object.keys(value)){if(!Object.hasOwn(descriptors[key],'value'))throw Error('Computed properties cannot be reviewed for adoption.');bytes+=encoder.encode(key).length;visit(descriptors[key].value,depth+1);}
      }
    }
    if(bytes>limit)throw Error('This adoption exceeds its retained-byte budget. Choose less source and model material.');
  }
  return visit;
}
export async function prepareWorkspaceAdoption(rows,{family='spaces',readVersion,signal}={}){
  if(!['spaces','creator-audio'].includes(family)||typeof readVersion!=='function')throw Error('Choose an existing adoption family and native record reader.');
  if(!Array.isArray(rows)||!rows.length||rows.length>100)throw Error('Choose between one and 100 source records.');
  adoptionBudget(32*1024*1024)(rows);
  const budget=adoptionBudget(64*1024*1024),source=structuredClone(rows),keys=new Set(),required=new Set();let size=0;budget(source);
  for(const row of source){
    if(!row||!allowed(row.key,family)||keys.has(row.key)||!Number.isSafeInteger(row.revision)||row.revision<1)throw Error('Invalid or duplicated source draft.');
    keys.add(row.key);size+=prepareRecord(row.value).bytes;checkedModelBody(row);
    if(size>480*1024)throw Error('The selected draft metadata is too large for one atomic adoption. Choose fewer drafts.');
    for(const key of adoptionDependencies(row))required.add(key);
  }
  const reviewed=[];
  for(const row of source){
    if(signal?.aborted)throw new DOMException('Review cancelled','AbortError');
    const current=await readVersion(row.key);budget(current);
    reviewed.push({row,current:structuredClone(current),immutable:/^product-model:[a-f0-9]{64}:body:[a-f0-9]{64}$/.test(row.key)});
  }
  // Only exact required records are read. A missing body stays a blocker; nothing
  // is fetched, manufactured, or silently accepted as a complete model.
  for(const key of required){
    if(keys.has(key))continue;
    if(signal?.aborted)throw new DOMException('Review cancelled','AbortError');
    const current=await readVersion(key);budget(current);
    if(!current||current.deleted)continue;
    checkedModelBody(current);
    reviewed.push({row:{key,revision:current.revision,value:current.value},current,dependencyOnly:true});
  }
  return prepareAdoptionReview(reviewed,{candidate:adoptionValue,dependencies:adoptionDependencies,signal});
}
export function installWorkspaceAdoption({sources}={}){
  const metadata=document.querySelector('meta[name="bonfyre-workspace-store"]'),navigation=document.querySelector('[data-native-workspace]');
  if(!metadata||!navigation||navigation.querySelector('[data-adopt-browser]'))return;
  const family=document.querySelector('meta[name="bonfyre-workspace-family"]')?.content??'spaces';
  sources??=family==='creator-audio'?[['Work Samples audio','https://aurekai-work-samples.pages.dev/audio/']]:publicSources;
  const store=openDestinationStore(metadata.content),destination=document.querySelector('meta[name="bonfyre-workspace-context"]')?.content??'This workspace';
  const trigger=make('button','Bring browser drafts');trigger.type='button';trigger.dataset.adoptBrowser='';trigger.style.cssText='font:inherit;min-height:44px;padding:8px 12px';navigation.append(trigger);
  trigger.onclick=()=>{
    const dialog=make('dialog');dialog.setAttribute('aria-label','Adopt browser drafts');dialog.style.cssText='max-width:1120px;width:calc(100% - 32px);max-height:85vh;overflow:auto;padding:24px;border:1px solid;border-radius:12px;color:#17201c;background:#fafbf8;font:16px system-ui';
    const requirements=make('div');requirements.dataset.adoptionDependencies='';
    const intro=make('p','Destination: '+destination+'. Choose a browser source, select its drafts, then review each change here. The original browser work stays unchanged.'),choices=make('div'),list=make('div'),status=make('p'),adopt=make('button','Adopt selected drafts'),cancel=make('button','Close');
    status.setAttribute('role','status');status.dataset.adoptionStatus='';adopt.disabled=true;adopt.type=cancel.type='button';let popup=null,sourceOrigin=null,nonce=null,incoming=null,busy=false,prepared=null,reviewView=null;
    for(const button of [adopt,cancel])button.style.cssText='font:inherit;min-height:44px;padding:10px 16px;margin:8px 8px 0 0';
    dialog.append(make('h2','Review existing work'),intro,choices,list,requirements,status,adopt,cancel);document.body.append(dialog);dialog.showModal();
    function close(){if(busy)return;reviewView?.destroy();window.removeEventListener('message',receive);popup?.close();dialog.close();dialog.remove();}
    cancel.onclick=close;dialog.addEventListener('cancel',event=>{event.preventDefault();close();});
    for(const [label,source]of sources){const button=make('button','Open '+label);button.type='button';button.style.cssText='font:inherit;min-height:44px;margin:8px 8px 8px 0';button.onclick=()=>{
      if(busy)return;nonce=crypto.randomUUID();const url=new URL(source);sourceOrigin=url.origin;url.searchParams.set('aurekai-transfer',nonce);url.searchParams.set('aurekai-recipient',location.origin);url.searchParams.set('aurekai-family',family);
      popup?.close();popup=window.open(url.href,'aurekai-transfer-'+nonce,'popup,width=900,height=850');incoming=null;prepared=null;requirements.replaceChildren();reviewView?.destroy();reviewView=null;list.replaceChildren();adopt.disabled=true;
      status.textContent=popup?'In the source window, select the saved drafts and any required model records.':'Allow this user-opened popup, then open the source again.';
    };choices.append(button);}
    async function receive(event){
      if(busy||event.source!==popup||event.origin!==sourceOrigin||event.data?.type!=='bonfyre.workspace.transfer.v1'||event.data?.nonce!==nonce)return;
      busy=true;adopt.disabled=true;
      try{
        incoming=event.data.rows;prepared=null;reviewView?.destroy();reviewView=null;list.replaceChildren();requirements.replaceChildren();status.textContent='Comparing the selected source drafts with this destination…';
        prepared=await prepareWorkspaceAdoption(incoming,{family,readVersion:key=>store.readVersion(key)});
        reviewView=mountAdoptionReview(list,prepared,{onSelection:selected=>selectionChanged(selected)});
        status.textContent='Source versions and product identifiers are retained. Required model bodies are included or checked with the selected bindings. Source submission receipts are not adopted, and recording permissions need a new review. Nothing is published or ordered.';
      }catch(error){status.textContent=error.message;incoming=null;}finally{busy=false;selectionChanged(reviewView?.controller.selectedRows()??[]);}
    }
    function selectionChanged(selected){
      requirements.replaceChildren();adopt.disabled=true;
      if(!incoming||!prepared||!selected.length)return;
      try{
        const plan=planAdoptionSelection(prepared,selected.map(row=>row.id));
        for(const dependency of plan.required){const text=dependency.mode==='check'?'Check unchanged required material: ':'Include required material: ';requirements.append(make('p',text+dependency.key));}
        for(const reason of plan.blockers)requirements.append(make('p',reason));
        if(!plan.blockers.length)requirements.append(make('p',`${plan.writes} records to adopt; ${plan.checks} existing dependencies checked in the same save.`));
        adopt.disabled=busy||plan.blockers.length>0;
      }catch(error){requirements.append(make('p',error.message));}
    }
    window.addEventListener('message',receive);
    adopt.onclick=async()=>{
      if(busy||!incoming)return;const selected=reviewView?.controller.selectedRows().map(item=>item.id)??[];if(!selected.length){status.textContent='Choose the drafts to adopt.';return;}busy=true;adopt.disabled=true;reviewView?.controller.lock();
      try{
        const plan=planAdoptionSelection(prepared,selected);
        if(plan.blockers.length)throw Error(plan.blockers.join(' '));
        const saved=await store.saveLocalBatch(plan.changes);
        status.textContent=plan.writes+' drafts adopted into '+destination+'. '+plan.checks+' existing dependencies checked. Original browser drafts were kept.';
        const release=document.querySelector('meta[name="bonfyre-workspace-release"]')?.content;
        if(/^[a-f0-9]{64}$/.test(release??''))for(const row of saved){
          if(!selected.includes(row.key)||row.key.startsWith('product-model:'))continue;
          const link=make('a','Open saved work · '+row.key);link.dataset.adoptedContinuation='';link.target='_blank';link.rel='noopener noreferrer';
          link.href=metadata.content.replace(/_records$/,'_views')+'?'+new URLSearchParams({key:row.key,revision:String(row.revision),release});dialog.append(link);
        }
        incoming=null;reviewView?.controller.lock();
        const reload=make('button','Reload editor');reload.type='button';reload.style.cssText='font:inherit;min-height:44px;padding:10px';reload.onclick=()=>location.reload();dialog.append(reload);
      }catch(error){status.textContent=error.message+' The source copies remain available. Reopen the source to review current destination revisions.';incoming=null;}
      finally{busy=false;}
    };
  };
}
if(globalThis.document)installWorkspaceAdoption();
