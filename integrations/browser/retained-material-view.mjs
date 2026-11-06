import {openDestinationStore} from './workspace-store.mjs';
import {workspaceElement as node,workspaceButton as action,mountTextSearch} from './interaction-collection.mjs';
import {appendMaterial,localReferences,referenceDescriptor,requirementRange,readMaterial,selectedMaterialRange,highlightMaterial,CONTRACT_PREFIX} from './retained-material.mjs';
export function createMaterialReader({endpoint=document.querySelector('meta[name=bonfyre-workspace-store]')?.content,recordKey=()=>new URL(location.href).searchParams.get('record'),storeFactory,request=fetch}={}){
  const events=new AbortController(),boxes=new Set();let dialog=null,reading=null,generation=0,closed=false;
  if(!document.querySelector('link[data-retained-reader]')){const css=node('link');css.rel='stylesheet';css.href=new URL('./retained-material.css',import.meta.url).href;css.dataset.retainedReader='';document.head.append(css);}
  const storeFor=(signal,address=endpoint)=>storeFactory?storeFactory(signal,address):openDestinationStore(address,{signal,request:(url,options)=>request(url,{...options,signal})});
  function close(){generation++;reading?.abort();reading=null;if(dialog){dialog.close();dialog.remove();dialog=null;}}
  function invalidate(){closed=true;close();for(const box of boxes)box.replaceChildren(node('p','Current account access changed. Reload this workspace.'));boxes.clear();}
  window.addEventListener('bonfyre:account-changed',invalidate,{signal:events.signal});window.addEventListener('pagehide',invalidate,{signal:events.signal});
  async function open(reference){
    if(closed)return;close();const ticket=generation,control=new AbortController();reading=control;const timer=setTimeout(()=>control.abort(new DOMException('The retained source read timed out.','TimeoutError')),20000);
    const d=node('dialog',undefined,'rm-dialog'),head=node('header'),title=node('h2',reference.label),status=node('p','Reading the exact retained source…','rm-status'),body=node('div',undefined,'rm-reader');status.setAttribute('role','status');
    head.append(title,action('Close source',()=>close()));d.append(head,status,body);document.body.append(d);dialog=d;
    d.addEventListener('cancel',event=>{event.preventDefault();close();});d.addEventListener('close',()=>control.abort(),{once:true});d.showModal();
    try{
      const store=storeFor(control.signal);let ref=reference;
      if(ref.kind==='contract'&&!ref.digest){
        if(!new RegExp('^'+CONTRACT_PREFIX+'[a-f0-9]{32}$').test(ref.key))throw Error('Invalid source contract coordinate.');
        const row=await store.readVersion(ref.key),digest=row?.value?.sources?.[0]?.history?.detailSha256;
        if(!/^[a-f0-9]{64}$/.test(digest||'')||ref.key!==CONTRACT_PREFIX+digest.slice(0,32))throw Error('The original contract is not available in this account.');ref={...ref,digest};
      }
      const retained=await readMaterial(ref,{store,signal:control.signal,libraryStore:binding=>storeFor(control.signal,'/apps/'+binding+'/_records')});
      if(closed||ticket!==generation||control.signal.aborted)return;
      status.textContent=retained.material?'Original text matches this research copy · '+retained.material.state+' source · research used revision '+retained.material.revision+' · library revision '+retained.material.currentRevision:'Verified retained source · '+retained.parts.length+' part'+(retained.parts.length===1?'':'s')+' · '+retained.body.length.toLocaleString()+' characters';
      d.dataset.verified='true';d.dataset.digest=retained.sha256;
      const text=node('div',undefined,'rm-source-text');text.tabIndex=0;if(retained.material)text.textContent=retained.body;else appendMaterial(text,retained,{onReference:open});
      const details=node('details',undefined,'rm-source-proof');details.append(node('summary','Source identity and retained parts'),node('code',retained.sha256));
      for(const part of retained.parts){const a=node('a','Open retained part · revision '+part.revision);const url=new URL('../instruments/',import.meta.url);url.searchParams.set('tool','research');url.searchParams.set('record',part.key);url.searchParams.set('revision',String(part.revision));a.href=part.href||url.href;if(part.href)a.textContent='Open source library · current revision '+part.revision;a.target='_blank';a.rel='noopener';details.append(a);}
      body.append(details,text);mountTextSearch(text,{label:'Find in retained source',signal:control.signal});
    }catch(error){if(closed||ticket!==generation)return;body.replaceChildren();d.dataset.verified='false';status.textContent=error.status===401||error.status===403?'Current account access was refused. Reopen the workspace to sign in.':error.message||'This retained source could not be read.';}
    finally{clearTimeout(timer);}
  }
  function mount(box,source){
    if(closed)return box;for(const old of boxes)if(!old.isConnected)boxes.delete(old);boxes.add(box);
    let expanded=false;const content=node('div',undefined,'rm-source-text'),tools=node('div',undefined,'rm-tools'),references=node('div',undefined,'rm-reference-list');
    const range=requirementRange(source),hasWrapper=range.start!==0||range.end!==source.body.length;
    const toggle=action('Show original handoff',()=>{expanded=!expanded;draw();});
    const openSource=ref=>open({...ref,recordKey:recordKey(),sourceId:source.id,sourceSha256:source.sha256});
    const draw=()=>{appendMaterial(content,source,{range:expanded?undefined:range,onReference:openSource});toggle.textContent=expanded?'Show requirement only':'Show original handoff';toggle.setAttribute('aria-expanded',String(expanded));};
    if(hasWrapper)tools.append(node('span','Requirement'),toggle);
    const seen=new Set();for(const token of localReferences(source.body)){const ref=referenceDescriptor(source,token),key=ref.key||ref.locator;if(ref.kind!=='provenance'&&!seen.has(key)){seen.add(key);references.append(action(ref.label,()=>openSource(ref)));}}
    if(source.material)references.append(action('Read original library source',()=>openSource({kind:'material',material:structuredClone(source.material),label:source.title||'Original library source'})));
    const contractKey=source.history?.contractKey;
    if(contractKey&&!seen.has(contractKey)){references.append(action('Shared production contract',()=>open({kind:'contract',key:contractKey,label:'Shared production contract'})));}
    if(references.childNodes.length){references.setAttribute('aria-label','Retained source references');tools.append(references);}
    box.replaceChildren(tools,content);draw();box.__showOriginal=()=>{expanded=true;draw();};return box;
  }
  return {mount,open,selection:(box,source,selection)=>selectedMaterialRange(box,source,selection),highlight(box,source,ref){if(!highlightMaterial(box,source,ref)){box.__showOriginal?.();return highlightMaterial(box,source,ref);}return true;},dispose(){closed=true;close();events.abort();boxes.clear();}};
}
