import {createEditHistory} from './runtime/edit-history.mjs';
import {WorkspaceSession} from './runtime/workspace-composition.mjs';
import {saveLocalBatch,readLocal} from './runtime/store.mjs';
import {WorkingRecord,instrumentKinds} from './working-record.mjs';
import {preparedOpportunity,adoptOpportunity} from './prepared-opportunity.mjs';
import {el,button,modal} from './ui.mjs';

const store={saveLocalBatch,readLocal},url=new URL(location.href);
const tool=Object.hasOwn(instrumentKinds,url.searchParams.get('tool'))?url.searchParams.get('tool'):'traces';
const root=document.querySelector('.application > main'),originalWorkspace=document.querySelector('#workspace');
const primaryFrame=el('section',{class:'linked-pane','data-working-pane':'primary','aria-label':'Primary instrument'});
const stage=el('div',{class:'linked-stage','data-paired':'false'},primaryFrame);
originalWorkspace.replaceWith(stage);primaryFrame.append(originalWorkspace);
document.head.append(el('link',{rel:'stylesheet',href:'./linked-workspace.css'}));
const labels={traces:'Process traces',research:'Research editor',schema:'Schema migration',product:'Product story map'};
const choose=el('select',{'aria-label':'Instrument to open alongside'},el('option',{value:'',text:'Choose another instrument'}),...Object.keys(instrumentKinds).filter(k=>k!==tool).map(k=>el('option',{value:k,text:labels[k]})));
let primary=null,companion=null,focused=null,mounting=false;
const openAlongside=button('Open alongside',()=>{if(choose.value)openBeside(choose.value).catch(e=>primary?.status(e.message,true));});
const jointSave=button('Save connected work',async()=>{
  const panes=[primary,companion].filter(Boolean);
  try{
    if(panes.some(p=>!p.ready()))throw Error('Finish the open instrument operations before saving connected work.');
    const rows=await WorkingRecord.saveTogether(panes.map(p=>p.record));
    panes.forEach((p,i)=>p.status(`Saved in this browser · revision ${rows[i].revision}${p.record.dirty?' · newer edits remain unsaved':''}.`));address();
  }catch(e){primary?.status(e.message,true);}
});
jointSave.hidden=true;
root.insertBefore(el('div',{class:'linked-tools'},choose,openAlongside,jointSave,el('span',{class:'fine muted',text:'Connected editors keep separate records and undo histories. Saves stay in this browser.'})),stage);
function address(){
  if(!primary)return;const next=new URL(location.href);next.searchParams.set('tool',primary.kind);
  for(const [pane,name,rev]of [[primary,'record','revision'],[companion,'withRecord','withRevision']]){
    if(pane?.blocked)continue;
    if(pane?.record?.revision){next.searchParams.set(name,pane.record.key);next.searchParams.set(rev,String(pane.record.revision));}
    else{next.searchParams.delete(name);next.searchParams.delete(rev);}
  }
  if(companion)next.searchParams.set('with',companion.kind);else next.searchParams.delete('with');
  history.replaceState(null,'',next);
}
function revisionParameter(name){const value=url.searchParams.get(name);if(value===null)return null;if(!/^[1-9][0-9]*$/.test(value)||!Number.isSafeInteger(Number(value)))throw Error('This continuation link has an invalid revision.');return Number(value);}
function controlsFor(frame,isPrimary=false){
  if(isPrimary)return {workspace:originalWorkspace,main:document.querySelector('#main-pane'),inspector:document.querySelector('#inspector'),title:document.querySelector('#title'),subtitle:document.querySelector('#subtitle'),status:document.querySelector('#status'),controls:Object.fromEntries(['undo','redo','open','save','export'].map(k=>[k,document.querySelector('#'+k)]))};
  const controls=Object.fromEntries(['undo','redo','open','save','export'].map(k=>[k,button(({open:'Open saved'})[k]??k[0].toUpperCase()+k.slice(1),()=>{})]));controls.save.classList.add('primary');
  const title=el('h2',{text:'Opening connected work…'}),subtitle=el('p',{class:'fine muted'}),status=el('div',{class:'linked-status',role:'status','aria-live':'polite'});
  const main=el('section',{'aria-label':'Connected instrument',id:'companion-main'}),inspector=el('aside',{'aria-label':'Connected inspector',id:'companion-inspector'}),workspace=el('div',{class:'workspace'},main,inspector);
  frame.append(el('header',{class:'linked-head'},el('div',{},el('p',{class:'fine muted',text:'CONNECTED INSTRUMENT'}),title,subtitle),el('div',{class:'commands'},...Object.values(controls),button('Close connected editor',()=>closeCompanion()))),status,workspace);
  return {workspace,main,inspector,title,subtitle,status,controls};
}
async function mountInstrument(kind,frame,elements,request={}){
  let active=null,edits=null,session=null,record=null,disposed=false,blocked=false,refreshPreparation=null;
  const lifetime=new AbortController();
  const status=(message,error=false)=>{elements.status.textContent=message;elements.status.classList.toggle('error',error);};
  const ready=()=>Boolean(!disposed&&!blocked&&active&&active.ready?.()!==false&&!record?.loading);
  const update=()=>{
    for(const id of ['save','export','undo','redo'])elements.controls[id].disabled=!session?.available(id).enabled;
    elements.controls.open.disabled=!record||record.busy||active?.ready?.()===false;
    elements.workspace.inert=Boolean(record?.loading);
    jointSave.disabled=[primary,companion].filter(Boolean).some(p=>!p.ready()||p.record?.busy);
    refreshPreparation?.();
  };
  const ctx={main:elements.main,inspector:elements.inspector,status,refresh:()=>{session?.refresh();update();},render:()=>active?.render(),
    select:id=>{if(session&&!disposed)session.select('instrument',id);},
    commit:fn=>{if(disposed||record?.loading)throw Error('This editor is closed or opening another record.');if(edits){if(edits.transact(fn)){record.changed();status('Unsaved changes · stored only when you choose Save.');}}else fn();session?.refresh();update();},
    async handoff(candidate){
      if(!ready())throw Error('Finish the current instrument operation first.');
      const incoming=await record.handoff(candidate);address();
      // A receiving-view error must never repeat a completed source save.
      try{await openBeside('product',{incoming:incoming.key});}catch(e){showConnectionFailure(incoming.key,e.message);}
      return incoming;
    },inbox:()=>store.readLocal('draft:instruments:incoming:')
  };
  const pane={kind,frame,elements,status,ready,get record(){return record;},get active(){return active;},get ctx(){return ctx;},get blocked(){return blocked;},preparation(fn){refreshPreparation=fn;}};
  function scene(){
    session?.dispose();
    const commands=[
      {id:'save',label:'Save working record',enabled:()=>ready()&&!record.busy,run:async()=>{const row=await record.save();status(`Saved in this browser · revision ${row.revision}${record.dirty?' · newer edits remain unsaved':''}.`);address();return row;}},
      {id:'export',label:'Export current result',enabled:()=>ready()&&!record.busy,run:()=>active.export()},
      {id:'undo',label:'Undo edit',enabled:()=>ready()&&!record.busy&&edits.canUndo,run:()=>{edits.undo();record.changed();update();}},
      {id:'redo',label:'Redo edit',enabled:()=>ready()&&!record.busy&&edits.canRedo,run:()=>{edits.redo();record.changed();update();}}
    ];
    session=new WorkspaceSession({schema:'bonfyre-surface-scene.v1',source_digest:'instruments-linked-20261019',focus:{subject_address:record.key,actor_scope:'browser-owner'},slots:[{slot_id:'main',extent:'workspace',accepts:[kind]},{slot_id:'detail',extent:'section',accepts:[kind+'-inspector']}],bindings:[{binding_id:'instrument',slot_id:'main',application_kind:kind,continuity_key:'selection',action_aperture:'save,export,undo,redo'},{binding_id:'inspector',slot_id:'detail',application_kind:kind+'-inspector',continuity_key:'selection',action_aperture:''}],interaction:{desktop_posture:{layouts:[{id:'instrument-and-inspector',panels:[{id:'instrument'},{id:'inspector',relative:'instrument',direction:'right'}]}]}}},new Map([['instrument',{kind,label:active.title,element:()=>elements.main,commands,version:()=>record.revision,dirty:()=>record.dirty,acceptSelection:active.acceptSelection}],['inspector',{kind:kind+'-inspector',label:'Selected record',element:()=>elements.inspector,commands:[],acceptSelection:active.acceptSelection}]]));session.subscribe(update);
  }
  pane.execute=id=>session?.execute(id).catch(e=>status(e.message,true));
  pane.openKey=async(key,revision=null)=>{
    if(!record||record.busy||active?.ready?.()===false)throw Error('Finish initializing or operating this instrument before opening another record.');
    if(record.dirty&&!confirm('Replace unsaved working edits with this saved revision?'))return false;
    await record.open(key,revision);edits.clear();blocked=false;elements.workspace.hidden=false;frame.querySelector('[data-open-failure]')?.remove();scene();address();status('Opened exact saved revision '+record.revision+'.');update();return true;
  };
  async function openSaved(){
    const rows=(await store.readLocal(`draft:instruments:${kind}:`)).filter(r=>r.value?.kind===kind),body=el('div'),dialog=modal('Open saved '+kind+' work',body);
    for(const row of rows)body.append(button((row.value.title||kind)+' · revision '+row.revision,async()=>{try{if(await pane.openKey(row.key,row.revision))dialog.close();}catch(e){status(e.message,true);}},'save-row'));
    if(!rows.length)body.append(el('p',{text:'No saved work for this instrument yet. Existing catalogue, room and media records are unchanged.'}));
  }
  pane.close=()=>{
    if(record?.busy||active?.ready?.()===false)throw Error('Finish the current instrument operation before closing it.');
    if(record?.dirty&&!confirm('Close this connected editor without saving its current changes?'))return false;
    record?.dispose();disposed=true;refreshPreparation=null;lifetime.abort();session?.dispose();active?.cleanup?.();edits?.clear();return true;
  };
  frame.addEventListener('pointerdown',()=>{focused=pane;},{signal:lifetime.signal});frame.addEventListener('focusin',()=>{focused=pane;},{signal:lifetime.signal});
  for(const id of ['save','export','undo','redo'])elements.controls[id].onclick=()=>{focused=pane;pane.execute(id);};
  elements.controls.open.onclick=()=>{focused=pane;openSaved().catch(e=>status(e.message,true));};
  try{
    if(request.record)elements.workspace.hidden=true;
    const module=await import('./'+instrumentKinds[kind]);active=await module.mount(ctx);
    record=new WorkingRecord({kind,store,capture:active.capture,load:s=>active.load?active.load(s):active.restore(s),onChange:update});
    edits=createEditHistory({capture:active.capture,restore:active.restore,limit:active.historyLimit??12});scene();
    elements.title.textContent=active.title;elements.subtitle.textContent=active.subtitle;
    if(request.record)await pane.openKey(request.record,request.revision??null);
    else{status('Working example loaded. Imports and saves stay in this browser; source searches run only when requested.');active.render();}
    update();
  }catch(error){
    blocked=true;elements.workspace.hidden=true;status(error.message,true);
    frame.append(el('section',{class:'panel panel-body','data-open-failure':''},el('h2',{text:'Requested work could not be opened'}),el('p',{text:error.message}),el('p',{text:'No substitute result was generated. Open a saved record explicitly, or choose another instrument.'})));update();
  }
  return pane;
}
function closeCompanion(){
  if(!companion)return;
  try{if(!companion.close())return;companion.frame.remove();companion=null;focused=primary;stage.dataset.paired='false';jointSave.hidden=true;address();}catch(e){companion.status(e.message,true);}
}
async function openBeside(kind,request={}){
  if(!Object.hasOwn(instrumentKinds,kind))throw Error('Choose an available instrument.');
  let pane=[primary,companion].find(p=>p?.kind===kind);
  if(pane){
    if(request.record&&(pane.record?.key!==request.record||request.revision&&pane.record?.revision!==request.revision))if(!await pane.openKey(request.record,request.revision??null))return pane;
  }else{
    if(mounting)throw Error('Another connected editor is opening.');mounting=true;
    try{
      if(companion){const prior=companion;closeCompanion();if(companion===prior)throw Error('The existing connected work was kept.');}
      const frame=el('section',{class:'linked-pane','data-working-pane':'companion','aria-label':'Connected '+labels[kind]});stage.append(frame);stage.dataset.paired='true';
      pane=await mountInstrument(kind,frame,controlsFor(frame),request);companion=pane;jointSave.hidden=false;address();
    }finally{mounting=false;}
  }
  focused=pane;if(request.incoming)await showIncoming(pane,request.incoming);pane.frame.scrollIntoView({block:'nearest',behavior:'smooth'});return pane;
}
function showConnectionFailure(incoming,message){
  primary?.status('Research and its opportunity were saved; the receiving editor could not open: '+message,true);
  primary?.frame.querySelector('[data-connection-failure]')?.remove();
  primary?.frame.prepend(el('section',{class:'linked-incoming','data-connection-failure':''},el('p',{text:'The saved candidate is intact. Opening its receiving view does not repeat the source save.'}),button('Open saved opportunity',()=>openBeside('product',{incoming}).catch(e=>primary.status(e.message,true)))));
}
async function showIncoming(pane,key){
  if(pane.kind!=='product'||!pane.ready())throw Error('Open a usable product map to review the opportunity.');
  const prepared=await preparedOpportunity(store,key);let pending=false;
  const box=el('section',{class:'linked-incoming','data-incoming':key},el('span',{class:'badge',text:'PREPARED FROM YOUR SAVED RESEARCH'}),el('h3',{text:prepared.candidate.title}),el('p',{class:'fine muted',text:prepared.candidate.researchTitle+' · source revision '+prepared.row.value.sourceRevision}));
  for(const ref of prepared.candidate.evidence)box.append(el('blockquote',{class:'quote',text:ref.quote}),el('p',{class:'fine muted',text:(ref.sourceTitle||ref.sourceId)+' · '+ref.relation+' · original offsets '+ref.start+'–'+ref.end}));
  if(prepared.sourceAdvanced)box.append(el('p',{text:'The source review has a newer saved version. The selected passages still match their retained text and digests.'}));
  box.append(el('p',{class:'fine muted',text:'Reach, impact and confidence remain editable estimates, not facts inferred from a quotation. Existing stories are kept.'}));
  const refresh=()=>{if(!box.isConnected){pane.preparation(null);return;}const exists=pane.active.capture().stories.some(s=>s.id===prepared.candidate.id);add.textContent=exists?'Already in this map':'Add prepared opportunity';add.disabled=exists||pending||pane.record.busy||!pane.ready();};
  const add=button('Add prepared opportunity',async()=>{
    try{
      pending=true;refresh();const latest=await preparedOpportunity(store,key);
      if(latest.row.revision!==prepared.row.revision)throw Error('The candidate changed. Open its preparation again before adopting it.');
      if(!pane.ready()||pane.record.busy)throw Error('The receiving editor is not ready.');
      pane.ctx.commit(()=>pane.active.restore(adoptOpportunity(pane.active.capture(),latest)));
      pane.status('Added the sourced opportunity to this working map. Configure estimates and capacity in its inspector, then save connected work.');
    }catch(e){pane.status(e.message,true);}finally{pending=false;refresh();}
  },'primary');
  box.append(el('div',{class:'toolbar'},add,button('Open originating review',async()=>{try{await openBeside('research',{record:prepared.row.value.sourceRecord,revision:prepared.row.value.sourceRevision});}catch(e){pane.status(e.message,true);}}),button('Dismiss preparation',()=>{box.remove();pane.preparation(null);})));
  pane.frame.querySelector('[data-incoming]')?.remove();pane.elements.workspace.before(box);pane.preparation(refresh);refresh();
}
for(const a of document.querySelectorAll('[data-tool]')){
  if(a.dataset.tool===tool)a.setAttribute('aria-current','page');
  a.addEventListener('click',event=>{if([primary,companion].some(p=>p?.record?.dirty)&&!confirm('Leave these instruments without saving their current changes?'))event.preventDefault();});
}
window.addEventListener('beforeunload',event=>{if([primary,companion].some(p=>p?.record?.dirty||p?.record?.busy)){event.preventDefault();event.returnValue='';}});
document.addEventListener('keydown',event=>{
  if(!(event.ctrlKey||event.metaKey))return;const pane=focused??primary;if(!pane)return;
  if(event.key.toLowerCase()==='s'){event.preventDefault();pane.execute('save');}
  else if(event.key.toLowerCase()==='z'&&!event.target.closest('input,textarea,[contenteditable=true]')){event.preventDefault();pane.execute(event.shiftKey?'redo':'undo');}
});
try{
  primary=await mountInstrument(tool,primaryFrame,controlsFor(primaryFrame,true),{record:url.searchParams.get('record'),revision:revisionParameter('revision')});focused=primary;document.title=primary.elements.title.textContent+' · Aurekai';
  if(url.searchParams.has('with'))await openBeside(url.searchParams.get('with'),{record:url.searchParams.get('withRecord'),revision:revisionParameter('withRevision')});
  if(tool==='product'&&url.searchParams.has('incoming'))await showIncoming(primary,url.searchParams.get('incoming'));
}catch(e){document.querySelector('#status').textContent=e.message;document.querySelector('#status').classList.add('error');}
