// Account-scoped, review-only Aurekai Warehouse Portfolio interface.
// Imported metadata and installed-app links do not grant access or effects.
import {readWorkspacePacket} from '../bridge/workspace-destinations.mjs';
import {warehouseInstalledTools,collectWarehousePortfolio,warehouseContinuations}
  from '../bridge/warehouse-portfolio.mjs';

const root=document.querySelector('[data-warehouse-studio]');
const context=document.querySelector('meta[name="bonfyre-workspace-context-selector"]')?.content||'self';
const el=(tag,text='',className='')=>{const n=document.createElement(tag);if(text)n.textContent=text;if(className)n.className=className;return n;};
const button=(text,fn)=>{const n=el('button',text);n.type='button';n.addEventListener('click',fn);return n;};
const roles=['sources','research','analysis','finance','connections','programs','campaigns','commerce','creator','logistics'];
const safeText=value=>String(value);
function mount(){
 if(!root)return;
 const abort=new AbortController();let closed=false,tools=null,portfolio={rows:[],complete:false},selected=null,filter='';
 const h=el('h1','Warehouse intelligence'),intro=el('p','Inspect native warehouse metadata, verify pinned snapshots, and continue in the current Aurekai account tools. Original datasets remain in their owning systems.');
 const details=el('p','Read-only review · no SQL execution, record creation, data transfer or publication. A set of portfolio pages is not a globally consistent cross-page snapshot.');
 const chooser=el('input');chooser.type='file';chooser.multiple=true;chooser.accept='.json,application/json';chooser.setAttribute('aria-label','Import native warehouse portfolio pages');
 const fileLabel=el('label','Choose native portfolio pages');fileLabel.append(chooser);
 const state=el('p');state.setAttribute('role','status');state.setAttribute('aria-live','polite');
 const inspectButton=button('Inspect portfolio',()=>void inspect());
 const back=el('a','Source library');back.href='../';
 const actions=el('div');actions.className='wp-actions';actions.append(inspectButton,back);
 const find=el('input');find.type='search';find.maxLength=120;find.placeholder='Find source identity';find.setAttribute('aria-label','Find a warehouse source');
 const list=el('section');list.className='wp-list';list.setAttribute('aria-label','Warehouse source identities');
 const inspector=el('section');inspector.className='wp-detail';inspector.setAttribute('aria-label','Data product review');
 const split=el('div');split.className='wp-split';split.append(list,inspector);
 root.append(h,intro,details,fileLabel,actions,state,find,split);
 const say=text=>{if(!closed)state.textContent=text;};
 function clear(){
   closed=true;abort.abort();tools=null;portfolio={rows:[],complete:false};selected=null;chooser.value='';
   root.replaceChildren(el('h1','Reopen warehouse intelligence'),
     el('p','The account or page context changed. Imported metadata and pending choices were cleared.'));
 }
 addEventListener('bonfyre:account-changed',clear,{signal:abort.signal});
 addEventListener('pagehide',clear,{signal:abort.signal});
 async function readTools(){
   const answer=await readWorkspacePacket('/apps/_bindings?'+new URLSearchParams({context}),
     {signal:abort.signal});
   abort.signal.throwIfAborted();
   return warehouseInstalledTools(answer,context);
 }
 async function inspect(){
   if(closed)return;
   const files=[...chooser.files];
   if(!files.length||files.length>16||files.some(f=>f.size<10||f.size>512*1024)){
     say('Choose one to sixteen native portfolio files, no more than 512 KiB each.');return;
   }
   inspectButton.disabled=true;say('Checking imported source metadata and installed workspace bindings…');
   try{
     const pages=[];
     for(const file of files){
       const body=await file.text();abort.signal.throwIfAborted();
       if(body.length>512*1024)throw Error('A portfolio file exceeds the import bound.');
       pages.push(JSON.parse(body));
     }
     const result=collectWarehousePortfolio(pages);
     let current=null;
     try{current=await readTools();}catch(e){
       if(abort.signal.aborted)return;
       say('Metadata inspected, but current account tools could not be verified. Continuations remain unavailable.');
     }
     abort.signal.throwIfAborted();
     portfolio=result;tools=current;selected=null;paint();
     if(current)say(result.rows.length+' source records reviewed · '+current.filter(x=>x.ready).length+' ready installed tools. No data was transferred.');
   }catch(error){if(!abort.signal.aborted)say('Review refused: '+error.message);}
   finally{if(!closed)inspectButton.disabled=false;}
 }
 function paint(){
   list.replaceChildren();const rows=portfolio.rows.filter(r=>r.semanticKind.toLowerCase().includes(filter));
   list.append(el('h2','Data products'),el('p',rows.length+' visible · '+(portfolio.complete?'complete import':'partial source inventory')));
   const ul=el('ul');
   for(const row of rows.slice(0,1600)){
     const li=el('li'),b=button(row.semanticKind+' · '+(row.state==='pinned'?'Generation '+row.generation:'Unavailable'),
       ()=>{selected=row;paint();});
     b.className='wp-source';b.dataset.state=row.state;
     if(selected?.semanticKind===row.semanticKind)b.setAttribute('aria-current','true');
     li.append(b);ul.append(li);
   }
   list.append(ul);
   inspector.replaceChildren();
   if(!selected){inspector.append(el('h2','Choose a source'),el('p','Review its pinned generation and available owner continuations.'));return;}
   inspector.append(el('h2',selected.semanticKind));
   if(selected.state!=='pinned'){
     inspector.append(el('p','No current pinned physical representation. The source must be repaired or recompiled before use.'));return;
   }
   const dl=el('dl');
   for(const [k,v] of [
    ['Representation',selected.representationId],['Generation',selected.generation],
    ['Digest',selected.digest],['Observed bytes',selected.observedBytes],
    ['Physical realizations',selected.realizationCount]
   ]){dl.append(el('dt',k),el('dd',safeText(v)));}
   inspector.append(dl,el('p','Metadata only. This is not proof of tenant authorization or physical bytes.'));
   if(!tools){inspector.append(el('p','Current installed-app bindings are unavailable. Reopen after account access is restored.'));return;}
   const choices=el('fieldset');choices.append(el('legend','Continue to owner review'));
   const fields=[];
   for(const role of roles){
     const l=el('label'),check=el('input');check.type='checkbox';check.value=role;
     check.checked=['sources','research','analysis'].includes(role);
     l.append(check,document.createTextNode(' '+role));choices.append(l);fields.push(check);
   }
   const outcomes=el('div');
   const open=button('Find current Aurekai tools',()=>{
     const requested=fields.filter(f=>f.checked).map(f=>f.value);
     outcomes.replaceChildren();
     if(!requested.length){outcomes.append(el('p','Choose at least one review domain.'));return;}
     const available=warehouseContinuations(selected,tools,{context,roles:requested});
     outcomes.append(el('h3','Owner continuations'));
     const ul=el('ul');
     for(const c of available){
       const li=el('li');
       if(c.state==='ready'||c.kind==='account'){
         const link=el('a',c.label+' · '+(c.kind==='account'?'Review prerequisites':'Open installed editor'));
         link.href=c.href;li.append(link);
       }else li.append(el('span',c.label+' · '+c.state.replaceAll('_',' ')));
       li.append(el('small',' '+c.boundary));ul.append(li);
     }
     outcomes.append(ul);
   });
   inspector.append(choices,open,outcomes);
 }
 find.addEventListener('input',()=>{filter=find.value.trim().toLowerCase();paint();},{signal:abort.signal});
 paint();
}
mount();
