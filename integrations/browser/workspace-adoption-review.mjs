import {mountCollection} from './interaction-collection.mjs';
import {compareRecords,renderChanges} from './record-comparison.mjs';
import {prepareRecord} from './workspace-value.mjs';
function kind(key){return key.startsWith('fixture-plan:')?'Room plans':key.startsWith('product-model:')?'Model files':key.startsWith('draft:creator:')?'Audio sessions':'Product drafts';}
const titleOf=row=>String(row.value?.plan?.title??row.value?.title??row.value?.productId??row.value?.files?.map(file=>file.name).join(', ')??row.key).slice(0,180);
const integer=n=>Number.isSafeInteger(n)&&n>=0&&n<2147483647;
const keyOK=key=>typeof key==='string'&&key.length>0&&new TextEncoder().encode(key).length<512&&!/[\u0000-\u001f\u007f]/u.test(key);
// Disposable reviewed material, never another store. UI rows cannot change the commit basis.
const reviews=new WeakMap();
export async function prepareAdoptionReview(reviewed,{candidate,dependencies=()=>[],signal}={}){
  if(typeof candidate!=='function'||typeof dependencies!=='function')throw Error('The existing adoption owner must supply the receiving value and dependency binding.');
  if(!Array.isArray(reviewed)||!reviewed.length||reviewed.length>200)throw Error('Review between one and 200 exact records.');
  const seen=new Set();
  // Capture every candidate before the first asynchronous asset comparison.
  const snapshots=reviewed.map(item=>{
    const snapshot=structuredClone(item),{row,current}=snapshot;
    if(!row||!keyOK(row.key)||seen.has(row.key)||!integer(row.revision)||row.revision<1)throw Error('Invalid or repeated adoption source.');
    if(current&&(!integer(current.revision)||current.revision<1||current.key!==row.key||typeof current.deleted!=='boolean'))throw Error('The destination review changed record identity.');
    if(snapshot.dependencyOnly&&(!current||current.deleted))throw Error('A retained dependency must exist in this destination.');
    seen.add(row.key);
    const after=structuredClone(candidate(row)),before=current&&!current.deleted?current.value:undefined;
    prepareRecord(after);if(before!==undefined)prepareRecord(before);
    const requires=dependencies({key:row.key,value:structuredClone(after)});
    if(!Array.isArray(requires)||requires.length>100||requires.some(k=>!keyOK(k))||new Set(requires).size!==requires.length)throw Error('The adoption dependency binding is invalid.');
    return {...snapshot,before,after,requires:[...requires]};
  });
  const result=[],kept=new Map();
  for(const item of snapshots){
    if(signal?.aborted)throw new DOMException('Review cancelled','AbortError');
    let changes=[],failure=null;
    try{changes=await compareRecords(item.before,item.after,{signal});}catch(error){if(error.name==='AbortError')throw error;failure=error.message;}
    const same=!!item.current&&!item.current.deleted&&!failure&&changes.length===0;
    if(item.immutable&&item.current&&!item.current.deleted&&!same)failure='This content-addressed model body differs from the destination. Reconcile the original files; it cannot be replaced by adoption.';
    const status=failure?'Review unavailable':item.dependencyOnly?'Required existing material':same?'Already present':!item.current?'New draft':item.current.deleted?'Restore removed draft':'Conflicting draft';
    const state={...item,same,failure,changes};kept.set(item.row.key,state);
    result.push({id:item.row.key,title:titleOf(item.row),subtitle:`${kind(item.row.key)} · source revision ${item.row.revision}`+(item.current?` · destination revision ${item.current.revision}`:''),
      category:status,status,version:String(item.row.revision)+':'+String(item.current?.revision??0),selectable:!failure&&!same&&!item.dependencyOnly,
      initiallySelected:!failure&&!same&&!item.current&&!item.dependencyOnly,data:structuredClone(state)});
  }
  reviews.set(result,kept);return result;
}
export function planAdoptionSelection(rows,selectedIds){
  const kept=reviews.get(rows);
  if(!kept)throw Error('Reopen the exact adoption review before saving.');
  if(!Array.isArray(selectedIds)||!selectedIds.length||selectedIds.length>100||new Set(selectedIds).size!==selectedIds.length)throw Error('Choose between one and 100 reviewed drafts.');
  const selected=new Set(selectedIds),changes=new Map(),required=new Map(),blockers=[],visiting=new Set(),visited=new Set();
  for(const id of selected){const item=kept.get(id);if(!item||item.failure||item.same||item.dependencyOnly)throw Error('The selected draft is not available in this review.');}
  function visit(id,parent=null){
    if(visiting.has(id)){blockers.push(`Dependency cycle at ${id}.`);return;}
    if(parent){const users=required.get(id)??new Set();users.add(parent);required.set(id,users);}
    if(visited.has(id))return;
    const item=kept.get(id);
    if(!item){blockers.push(`Required material was not supplied or found: ${id}.`);return;}
    if(item.failure){blockers.push(`Required review is unavailable: ${id}.`);return;}
    // Dependencies with local differences or tombstones must be explicitly reviewed.
    if(parent&&!selected.has(id)&&item.current&&!item.same){blockers.push(`Review the conflicting or removed dependency before adopting: ${id}.`);return;}
    visiting.add(id);
    for(const dep of item.requires)visit(dep,id);
    visiting.delete(id);visited.add(id);
    if(item.same)changes.set(id,{key:id,expectedRevision:item.current.revision,checkOnly:true});
    else changes.set(id,{key:id,expectedRevision:item.current?.revision??0,value:structuredClone(item.after)});
  }
  for(const id of selected)visit(id);
  const entries=[...changes.values()];
  return {changes:blockers.length?[]:entries,blockers:[...new Set(blockers)],
    required:[...required].map(([key,users])=>({key,for:[...users],mode:changes.get(key)?.checkOnly?'check':'include'})),
    writes:blockers.length?0:entries.filter(c=>!c.checkOnly).length,checks:blockers.length?0:entries.filter(c=>c.checkOnly).length};
}
export function mountAdoptionReview(host,rows,{onSelection=()=>{}}={}){
  return mountCollection(host,{rows,label:'Drafts to adopt',onSelection,renderInspector(row,detail,{element,signal}){
    const {before,after,changes,failure}=row.data;const urls=[];
    if(failure){detail.append(element('p',failure));return;}
    const note=element('p',row.data.same?'The receiving copy is already identical.':row.data.current&&!row.data.current.deleted?'This would replace the reviewed destination version. It is not selected automatically.':'This creates a separate receiving copy. The original browser work stays unchanged.');note.className='ac-notice';detail.append(note);
    if(row.id.startsWith('draft:creator:'))detail.append(element('p','Recording permission resets to unreviewed in this destination. The players below preview original recordings, not the rendered arrangement.'));
    if(row.id.startsWith('fixture-plan:'))detail.append(element('p','Plan and fixture identities are kept. A source submission receipt does not become an approval in this destination.'));
    for(const [label,value]of [['Current recordings',before],['Proposed recordings',after]]){
      if(!row.id.startsWith('draft:creator:')||!Array.isArray(value?.files))continue;
      const section=element('section');let count=0;
      for(const file of value.files){
        if(signal.aborted)break;
        if(typeof file?.name!=='string'||!file.name.toLowerCase().endsWith('.wav')||!(file.bytes instanceof ArrayBuffer))continue;
        if(count++===8){section.append(element('p','Additional recordings remain listed in the comparison. Open the audio editor to inspect the whole session.'));break;}
        const url=URL.createObjectURL(new Blob([file.bytes],{type:'audio/wav'}));urls.push(url);const player=element('audio');player.controls=true;player.preload='none';player.src=url;player.setAttribute('aria-label',label+': '+file.name);section.append(element('p',file.name),player);
      }
      if(count){detail.append(element('h4',label),section);}
    }
    const cleanup=renderChanges(detail,changes,{element});return ()=>{cleanup();for(const player of detail.querySelectorAll('audio')){player.pause();player.removeAttribute('src');player.load();}for(const url of urls)URL.revokeObjectURL(url);};
  }});
}
