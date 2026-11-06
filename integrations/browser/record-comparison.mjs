// A view of exact candidate values; it creates no approval, schema or stored copy.
const MISSING = Symbol('missing');
const isBytes = value => value instanceof ArrayBuffer || ArrayBuffer.isView(value);
async function binary(value, signal) {
  if (signal?.aborted) throw new DOMException('Inspection cancelled','AbortError');
  const isBlob = typeof Blob !== 'undefined' && value instanceof Blob;
  const bytes = isBlob ? await value.arrayBuffer() : value instanceof ArrayBuffer ? value : value.buffer.slice(value.byteOffset,value.byteOffset+value.byteLength);
  const hash = [...new Uint8Array(await crypto.subtle.digest('SHA-256',bytes))].map(n=>n.toString(16).padStart(2,'0')).join('');
  const name = typeof File !== 'undefined' && value instanceof File ? value.name : '';
  const type = isBlob ? value.type : value.constructor.name;
  const modified = typeof File !== 'undefined' && value instanceof File ? value.lastModified : null;
  return {hash,bytes:bytes.byteLength,name,type,modified};
}
const display = value => value === MISSING ? 'Not present' : value === null ? 'None' : value === '' ? '(empty text)' : typeof value === 'boolean' ? value ? 'Yes' : 'No' : Object.is(value,-0) ? '-0' : typeof value==='object' ? (Array.isArray(value)?'List with '+value.length+' items':'Object') : String(value);
const pathLabel = path => path.length ? path.map(part=>typeof part==='number'?'Item '+(part+1):String(part).replace(/([a-z])([A-Z])/g,'$1 $2').replaceAll('_',' ')).join(' › ') : 'Value';
export async function compareRecords(before,after,{signal,maxNodes=12000}={}) {
  const changes=[];let nodes=0;const hashes=new WeakMap();
  const identity=async value=>{if(!hashes.has(value))hashes.set(value,binary(value,signal));return hashes.get(value);};
  async function walk(a,b,path) {
    if(signal?.aborted)throw new DOMException('Inspection cancelled','AbortError');
    if(++nodes>maxNodes||path.length>40)throw Error('This comparison is larger than the interactive review limit. Open the specialist editor before replacing it.');
    if(Object.is(a,b))return;
    const ab=(typeof Blob!=='undefined'&&a instanceof Blob)||isBytes(a),bb=(typeof Blob!=='undefined'&&b instanceof Blob)||isBytes(b);
    if(ab||bb){
      const av=ab?await identity(a):null,bv=bb?await identity(b):null;
      if(av&&bv&&Object.keys(av).every(key=>av[key]===bv[key]))return;
      const label=v=>v?`${v.name||v.type||'Binary asset'} · ${v.bytes.toLocaleString('en-US')} bytes\nSHA-256 ${v.hash}${v.modified===null?'':'\nFile modified '+v.modified}`:null;
      changes.push({path:pathLabel(path),before:av?label(av):display(a),after:bv?label(bv):display(b),kind:'asset'});return;
    }
    const ao=a!==MISSING&&a!==null&&typeof a==='object',bo=b!==MISSING&&b!==null&&typeof b==='object';
    if((ao||bo)&&(!ao||!bo||Array.isArray(a)===Array.isArray(b))){
      const aa=ao?Object.keys(a):[],ba=bo?Object.keys(b):[],keys=[...new Set([...aa,...ba])];
      if(!keys.length){changes.push({path:pathLabel(path),before:ao?(Array.isArray(a)?'Empty list':'Empty object'):display(a),after:bo?(Array.isArray(b)?'Empty list':'Empty object'):display(b),kind:'value'});return;}
      for(const key of keys){const left=ao&&Object.hasOwn(a,key)?a[key]:MISSING,right=bo&&Object.hasOwn(b,key)?b[key]:MISSING;await walk(left,right,[...path,Array.isArray(bo?b:a)?Number(key):key]);}return;
    }
    changes.push({path:pathLabel(path),before:display(a),after:display(b),kind:a===MISSING?'added':b===MISSING?'removed':'value'});
  }
  await walk(before===undefined?MISSING:before,after===undefined?MISSING:after,[]);return changes;
}
export function renderChanges(host,changes,{element,labelBefore='Current destination',labelAfter='Proposed copy'}={}) {
  const el=element??((tag,value)=>{const node=host.ownerDocument.createElement(tag);if(value!==undefined)node.textContent=String(value);return node;});
  const heading=el('h4',changes.length+' changed '+(changes.length===1?'value':'values')),list=el('dl'),more=el('button','Show more changes');more.type='button';let displayed=0;
  host.append(heading,list,more);
  function page(){const next=changes.slice(displayed,displayed+40);for(const change of next){const block=el('div');block.className='ac-change';const old=el('dd',labelBefore+': '+change.before),updated=el('dd',labelAfter+': '+change.after);old.className='ac-before';updated.className='ac-after';block.append(el('dt',change.path),old,updated);list.append(block);}displayed+=next.length;more.hidden=displayed>=changes.length;more.textContent=`Show more changes (${displayed} of ${changes.length})`;}
  more.onclick=page;page();return ()=>{more.onclick=null;};
}
export function mountSourceReview(host,{drafts,key,item,signal,disabled=()=>false,guard=work=>work(),save,read,labels={}}){
 const events=new AbortController();signal=AbortSignal.any([signal,events.signal]);const el=(tag,text)=>{const n=host.ownerDocument.createElement(tag);if(text!==undefined)n.textContent=text;return n;},button=(text,work)=>{const n=el('button',text);n.type='button';n.onclick=()=>void guard(work);return n;},field=(text,input)=>{const n=el('label',text);input.setAttribute('aria-label',text);n.append(input);return n;};let current=item,draft,cleanup=null,epoch=0,busy=false;
 function sync(value){if(!value||!Number.isSafeInteger(value.revision)||value.revision<1||!['unreviewed','shortlist','discard'].includes(value.decision)||typeof value.notes!=='string'||value.notes.length>2000)throw Error('The saved source review is incomplete.');current=value;draft=drafts.get(key);if(draft?.dirty&&value.revision<draft.revision)throw Error('The saved review is older than this draft. Read its current version again.');if(!draft||!draft.dirty||value.revision>draft.revision&&value.decision===draft.decision&&value.notes===draft.notes){draft={decision:value.decision,notes:value.notes,revision:value.revision,dirty:false,pending:null};drafts.set(key,draft);}else if(draft.pending&&value.revision>=draft.pending.revision)draft.pending=null;}
 sync(item);const decision=el('select'),notes=el('textarea'),status=el('p'),comparison=el('section');for(const [value,title]of [['unreviewed','Not reviewed'],['shortlist','Shortlist'],['discard','Discard']]){const option=el('option',title);option.value=value;decision.append(option);}notes.rows=4;notes.maxLength=2000;status.setAttribute('role','status');status.className='ac-review-status';comparison.className='program-plan-review';
 function render(){if(signal.aborted)return;decision.value=draft.decision;notes.value=draft.notes;const blocked=disabled();decision.disabled=notes.disabled=busy||blocked||!!draft.pending;submit.disabled=busy||blocked||!!draft.pending||!draft.dirty;status.dataset.state=draft.pending?'pending':draft.revision!==current.revision?'conflict':draft.dirty?'dirty':'saved';submit.textContent=draft.revision===current.revision?(labels.save||'Save review'):'Review against saved changes';status.textContent=busy?'Reading or saving this review…':draft.pending?'The save is not confirmed. Check the saved review before editing.':draft.revision!==current.revision?'A newer review is saved. Your draft is kept; compare both versions before saving.':draft.dirty?'Unsaved review · kept while you browse this destination.':'Saved review is current.';for(const n of [decision,notes,submit])n.dataset.sourceUnavailable=String(blocked||!!draft.pending);if(check)check.disabled=busy;}
 async function commit(payload){if(signal.aborted||disabled()||busy||draft.pending)return;draft.pending={...payload};busy=true;render();try{await save(payload);}finally{busy=false;render();}}
 const submit=button(labels.save||'Save review',async()=>{if(signal.aborted||disabled()||busy||draft.pending||!draft.dirty)return;const payload={revision:draft.revision,decision:draft.decision,notes:draft.notes};if(draft.revision!==current.revision){const ticket=++epoch,changes=await compareRecords({decision:current.decision,notes:current.notes},{decision:payload.decision,notes:payload.notes},{signal});if(signal.aborted||ticket!==epoch)return;cleanup?.();comparison.replaceChildren(el('h4','Review your draft against the saved version'));cleanup=renderChanges(comparison,changes,{labelBefore:'Saved review',labelAfter:'Your draft'});const confirm=button(labels.confirm||'Save reviewed change',()=>{if(ticket!==epoch||signal.aborted)return;return commit({...payload,revision:current.revision});});confirm.dataset.sourceWrite='';comparison.append(confirm);}else await commit(payload);});submit.dataset.sourceWrite='';
 const check=read?button('Check saved review',async()=>{if(signal.aborted||busy)return;epoch++;cleanup?.();comparison.replaceChildren();busy=true;render();try{const value=await read();if(signal.aborted)return;sync(value);epoch++;cleanup?.();comparison.replaceChildren();}finally{busy=false;render();}}):null;
 for(const [node,event,name]of [[decision,'change','decision'],[notes,'input','notes']]){node.dataset.sourceEdit='';node.addEventListener(event,()=>{if(signal.aborted||busy||disabled()||draft.pending)return;draft[name]=node.value;draft.dirty=true;epoch++;cleanup?.();comparison.replaceChildren();render();},{signal});}
 const actions=el('div');actions.className='ac-review-actions';actions.append(submit);if(check)actions.append(check);host.append(field(labels.decision||'Review decision',decision),field(labels.notes||'Your note',notes),status,actions,comparison);render();return {draft,get dirty(){return draft.dirty;},destroy(){epoch++;events.abort();cleanup?.();}};
}
