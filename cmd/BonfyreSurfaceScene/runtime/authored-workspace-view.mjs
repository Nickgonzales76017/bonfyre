// Mount existing domain controllers, not demo-page iframes. Native CMS owns state.
import {createAuthoredWorkspace,validateAuthoredWorkspace,addInstrument,removeInstrument,moveInstrument,connectInstruments,reuseWorkspaceDesign,prepareAuthoredBatch,validateAuthoredReceipt,authoredWorkspaceScene,blankInstrument,instrumentLabels,productionPrefix} from './production-runtime/authored-workspace.mjs';
import {WorkspaceSession} from './production-runtime/renderers/workspace-composition.mjs';
import {openDesignPublication,instantiatePublicDesign,designFromPacket} from './production-runtime/authored-publication.mjs';
import {decode} from '../bridge/workspace-msgpack.mjs';
import {mountSplitPane,mountTabs,mountCommandPalette} from '../bridge/interaction-collection.mjs';
import {WorkingRecord,instrumentKinds} from './working-record.mjs';
import {createEditHistory} from './runtime/edit-history.mjs';
import {preparedOpportunity,adoptOpportunity} from './prepared-opportunity.mjs';
import {openDestinationStore} from '../bridge/workspace-store.mjs';
import {mountPublicationPicker} from '../bridge/publication-picker.mjs';
import {prepareResearchInputs,researchDigest} from '../bridge/publication-research.mjs';
import {sourceRecord} from './research.mjs';
import {el,button,field,modal} from './ui.mjs';
import {openProductionDestination} from './production-destination.mjs';
import {openRecoveryImport} from './production-recovery.mjs';
const meta=name=>document.querySelector(`meta[name="${name}"]`)?.content;
const endpoint=meta('bonfyre-workspace-store');
if(!/^\/apps\/ws-[a-f0-9]{40}\/_records$/.test(endpoint||''))throw Error('Open this production in its installed Aurekai destination.');
const native=openDestinationStore(endpoint),host=document.querySelector('#production-authoring');
let production=createAuthoredWorkspace(),revision=0,epoch=0,savedEpoch=-1,operation=false,scene=null,inspectorTab='source';
const panes=new Map(),status=el('p',{class:'production-status',role:'status','aria-live':'polite'});
const tell=(message,error=false)=>{status.textContent=message;status.dataset.error=String(error);};
let transition=false,metadataEdited=false,inspectorTabs=null;
async function exclusive(fn){if(transition)throw Error('Wait for the current production transition.');transition=true;host.inert=true;refresh();try{return await fn();}finally{transition=false;host.inert=false;refresh();}}
const run=fn=>async()=>{if(transition)return;try{return await exclusive(fn);}catch(error){tell(error.message,true);}};
const title=el('input',{id:'production-title',value:production.title,maxLength:200,'aria-label':'Production title'});
const description=el('textarea',{id:'production-description',maxLength:4000,placeholder:'What are you making, and what should the next person be able to do?','aria-label':'Production purpose'});
const outline=el('nav',{class:'production-outline','aria-label':'Production outline'}),stage=el('section',{class:'production-stage','aria-label':'Working production canvas'}),inspector=el('aside',{class:'production-inspector','aria-label':'Production configuration'});
const addChoice=el('select',{'aria-label':'Instrument to add'},...Object.entries(instrumentLabels).map(([id,label])=>el('option',{value:id,text:label})));
const saveButton=button('Save production',run(saveProduction),'primary');saveButton.id='save-production';
const bar=el('header',{class:'production-heading'},el('div',{},el('p',{class:'eyebrow',text:'Production studio / native working composition'}),title,description),el('div',{class:'production-actions'},button('New',run(()=>replaceProduction(createAuthoredWorkspace()))),button('Open saved',run(openProduction)),saveButton,button('Recover editable work',run(async()=>{assertIdle();await openRecoveryImport({endpoint,context:meta('bonfyre-workspace-context'),release:meta('bonfyre-workspace-release'),store:native,onOpen:row=>run(()=>loadProduction(row.key,row.revision))()});})),button('Prepare another workspace',run(async()=>{assertIdle();if(hasUnsaved())await saveProduction();await openProductionDestination({sourceEndpoint:endpoint,sourceRelease:meta('bonfyre-workspace-release'),productionKey:productionPrefix+production.id,productionRevision:revision,sourceStore:native});})),button('Reuse this design',run(()=>replaceProduction(reuseWorkspaceDesign(production))))));
const sidebar=el('aside',{class:'production-sidebar'},el('p',{class:'eyebrow',text:'Outline'}),outline,el('div',{class:'production-add'},addChoice,button('Add instrument',run(async()=>{assertIdle();production=addInstrument(production,addChoice.value);changed();const p=production.panels.at(-1);await createPane(p);await activate(p.id);render();}),'primary')),el('p',{class:'fine muted',text:'Up to four bound instruments; two run at once. Parked work keeps its edits and releases workers.'}));
const workarea=el('div',{class:'production-workarea'},stage,inspector),body=el('div',{class:'production-layout production-interactive-layout'},sidebar,workarea);host.append(bar,status,body);
const canvasSplit=mountSplitPane(workarea,{primary:stage,secondary:inspector,label:'Production canvas',ratio:73,min:45,max:82,stackAt:980});
bar.querySelector('.production-actions').append(button('Publish design',run(async()=>{assertIdle();const destination=meta('bonfyre-workspace-context');if(destination!=='self'&&!/^user:[1-9][0-9]*$/.test(destination??''))throw Error('Public design publication currently uses your personal CMS namespace. Institutional and household work stays private; use its reviewed destination handoff.');if(hasUnsaved())await saveProduction();await openDesignPublication(production,{el,button,modal});})));
const productionActions=[...bar.querySelector('.production-actions').querySelectorAll('button')];
const commandButton=button('Commands \u00B7 \u2318K',()=>{});commandButton.dataset.productionCommands='';bar.querySelector('.production-actions').append(commandButton);
const commandPalette=mountCommandPalette(commandButton,{scope:host,label:'Production Studio commands',onError:error=>tell(error.message,true),commands:()=>[
 ...productionActions.map((control,i)=>({id:'production-action-'+i,label:control.textContent,group:'Production',disabled:transition||operation||control.disabled,reason:'Finish the current operation',run:()=>{if(!transition&&!operation&&!control.disabled)control.click();}})),
 ...production.panels.map(binding=>({id:'open-'+binding.id,label:'Open '+binding.label,group:'Bound instruments',disabled:transition||operation,run:run(()=>activate(binding.id))})),
 ...['source','behavior','appearance'].map(key=>({id:'configure-'+key,label:'Configure '+key,group:'Inspector',disabled:transition||operation,run:()=>{inspectorTabs?.select(key);inspector.scrollIntoView({block:'nearest'});inspector.querySelector('[role=tab][aria-selected=true]')?.focus();}}))
]});
const store={readLocal:prefix=>native.readLocal(prefix),saveLocalBatch:async changes=>{
  const at=epoch,prepared=prepareAuthoredBatch(production,revision,[...panes.values()].map(p=>({id:p.id,kind:p.kind,key:p.record.key,revision:p.record.revision})),changes);
  const rows=await native.saveLocalBatch(prepared.changes),accepted=validateAuthoredReceipt(prepared,rows);
  revision=accepted.revision;if(epoch===at){production=prepared.production;savedEpoch=at;}address();
  return changes.map(c=>rows.find(r=>r.key===c.key));
}};
function assertIdle(){if(operation||[...panes.values()].some(p=>p.record?.busy||p.active?.ready?.()===false))throw Error('Finish the current native save or instrument operation first.');}
function changed(){epoch++;refresh();}
function hasUnsaved(){return epoch!==savedEpoch||[...panes.values()].some(p=>p.record.dirty||!p.record.revision);}
function address(){if(!revision)return;const u=new URL(location.href);u.searchParams.delete('design');u.searchParams.set('record',productionPrefix+production.id);u.searchParams.set('revision',String(revision));history.replaceState(null,'',u);}
function refresh(){title.disabled=description.disabled=operation||transition;saveButton.disabled=operation||[...panes.values()].some(p=>p.record?.busy||p.active?.ready?.()===false);host.dataset.dirty=String(hasUnsaved());const current=panes.get(production.focus),coordinate=inspector.querySelector('[data-record-revision]');if(current&&coordinate)coordinate.textContent='Native revision '+current.record.revision+' · '+meta('bonfyre-workspace-context');for(const p of panes.values())if(p.record)p.note.textContent=(p.record.revision?'Saved revision '+p.record.revision:'New working record')+(p.record.dirty?' · unsaved edits':'')+(p.active?'':' · parked');scene?.refresh();}
async function saveProduction(){
  assertIdle();operation=true;body.inert=true;refresh();
  try{const changedRecords=[...panes.values()].filter(p=>p.record.dirty||!p.record.revision).map(p=>p.record);
    if(changedRecords.length)await WorkingRecord.saveTogether(changedRecords);else await store.saveLocalBatch([]);
    tell('Saved production revision '+revision+'. Its arrangement and source bindings were accepted together.');renderOutline();
  }finally{operation=false;body.inert=false;refresh();}
}
async function createPane(binding,registry=panes,attached=true){
  let snapshot=blankInstrument(binding.kind,binding.label),active=null,edits=null;
  const frame=el('article',{class:'production-pane','data-production-pane':binding.id,'data-instrument-kind':binding.kind}),main=el('section',{class:'production-instrument-main'}),detail=el('aside',{class:'production-domain-inspector'}),note=el('p',{class:'fine muted'});
  const p={id:binding.id,kind:binding.kind,frame,main,detail,note,record:null,get active(){return active;}};
  const ctx={main,inspector:detail,status:tell,refresh,select:value=>{if(attached&&scene?.plan.panels.some(x=>x.id===p.id))scene.select(p.id,value);},
    commit:fn=>{if(operation||p.record?.loading)throw Error('Wait for the current production operation.');if(edits){if(edits.transact(fn))p.record.changed();}else fn();refresh();},
    inbox:()=>native.readLocal('draft:instruments:incoming:'),handoff:candidate=>exclusive(()=>handoff(p,candidate))};
  p.record=new WorkingRecord({kind:p.kind,key:binding.recordKey,store,capture:()=>active?active.capture():structuredClone(snapshot),load:async value=>{if(active)await(active.load?active.load(value):active.restore(value));else snapshot=structuredClone(value);},onChange:refresh});
  p.mount=async()=>{if(active)return;if(document.querySelector('dialog[open]'))throw Error('Close the dialog before changing the canvas.');
    const module=await import('./'+instrumentKinds[p.kind]);active=await module.mount(ctx,snapshot);edits=createEditHistory({capture:active.capture,restore:active.restore,limit:active.historyLimit??10});
    if(!p.record.revision||p.kind==='schema'&&!snapshot.bytes)p.record.changed();frame.hidden=false;if(attached)stage.append(frame);refresh();};
  p.park=()=>{if(!active)return;assertIdle();if(document.querySelector('dialog[open]'))throw Error('Close the dialog before parking its editor.');snapshot=active.capture();active.cleanup?.();active=null;edits?.clear();edits=null;main.replaceChildren();detail.replaceChildren();frame.remove();refresh();};
  p.dispose=()=>{p.park();p.record.dispose();};
  p.load=async(key,rev)=>{assertIdle();if([...panes.values()].some(other=>other!==p&&other.record.key===key))throw Error('That record is already bound here.');if(p.record.dirty&&!confirm('Replace this instrument\'s unsaved edits with the selected saved work?'))return;await p.record.open(key,rev);edits?.clear();if(scene?.plan.panels.some(x=>x.id===p.id))scene.select(p.id,null);const b=production.panels.find(x=>x.id===p.id);b.recordKey=p.record.key;b.revision=p.record.revision;changed();render();};
  p.undo=()=>{assertIdle();if(edits?.undo()){p.record.changed();refresh();}};p.redo=()=>{assertIdle();if(edits?.redo()){p.record.changed();refresh();}};p.restore=value=>ctx.commit(()=>active.restore(value));
  const controls=el('div',{class:'commands'},button('Inspect',()=>{production.focus=p.id;renderInspector();}),button('Undo',run(p.undo)),button('Redo',run(p.redo)),button('Export result',run(()=>{assertIdle();return active?.export();})),button('Park',run(()=>{p.park();compileScene();render();})));
  frame.append(el('header',{class:'production-pane-head'},el('div',{},el('span',{class:'eyebrow',text:instrumentLabels[p.kind]}),el('h2',{class:'production-pane-title',text:binding.label}),note),controls),el('div',{class:'production-domain-workspace'},main,detail));
  p.attach=()=>{attached=true;if(active)stage.append(frame);};
  registry.set(p.id,p);if(binding.revision)await p.record.open(binding.recordKey,binding.revision);return p;
}
async function activate(id){assertIdle();const p=panes.get(id);if(!p)throw Error('Choose a bound instrument.');if(!p.active&&[...panes.values()].filter(x=>x.active).length>=2){const other=[...panes.values()].find(x=>x.active&&x.id!==production.focus)||[...panes.values()].find(x=>x.active);other.park();}await p.mount();production.focus=id;compileScene();render();}
function compileScene(){const selections=new Map();if(scene)for(const panel of scene.plan.panels)selections.set(panel.id,scene.context(panel.id).selected);scene?.dispose();scene=null;const adapters=new Map();for(const p of panes.values())if(p.active)adapters.set(p.id,{kind:p.kind,label:production.panels.find(x=>x.id===p.id).label,element:()=>p.frame,version:()=>p.record.revision,dirty:()=>p.record.dirty,acceptSelection:p.active.acceptSelection,commands:[]});
  if(adapters.size)scene=new WorkspaceSession(authoredWorkspaceScene(production,adapters,'authored:'+production.id+':'+epoch),adapters);
  if(scene)for(const [id,selection]of selections)if(selection!==null&&adapters.has(id)){try{scene.select(id,selection);}catch{/* A changed source must explicitly accept its previous selection. */}}
  for(const b of production.panels){const p=panes.get(b.id);if(p?.active)stage.append(p.frame);}
}
function renderOutline(){outline.replaceChildren();for(const [i,b]of production.panels.entries()){const p=panes.get(b.id),row=el('div',{class:'production-outline-row','data-focused':String(production.focus===b.id)});row.append(button(String(i+1).padStart(2,'0')+' / '+b.label,run(()=>activate(b.id))),el('small',{text:b.role+' · '+(p?.active?'open':'parked')}));outline.append(row);}}
function choice(label,values,current,callback){const select=el('select',{'aria-label':label},...values.map(([id,title])=>el('option',{value:id,text:title})));select.value=current;select.onchange=run(()=>callback(select.value));return field(label,select);}
function renderInspector(){
 inspectorTabs?.destroy();inspector.replaceChildren(el('p',{class:'eyebrow',text:'Configure the selected work'}));
 const tabs=el('div',{class:'production-tabs','aria-label':'Production configuration'}),entries=['source','behavior','appearance'].map(key=>({id:key,button:button(key[0].toUpperCase()+key.slice(1),()=>{}),panel:el('section',{class:'production-config-panel'})}));
 tabs.append(...entries.map(e=>e.button));inspector.append(tabs,...entries.map(e=>e.panel));
 for(const entry of entries)renderInspectorContent(entry.panel,entry.id);
 inspectorTabs=mountTabs(tabs,entries,{selected:inspectorTab,onSelect:key=>{inspectorTab=key;}});
}
function renderInspectorContent(target,key){
  const binding=production.panels.find(x=>x.id===production.focus),p=panes.get(binding?.id);
  if(key==='appearance'){
    target.append(choice('Arrangement',[['columns','Side by side'],['stack','Editorial sequence'],['focus','Focus selected editor']],production.layout,value=>{assertIdle();production.layout=value;changed();compileScene();render();}),choice('Canvas',[['paper','Paper'],['night','Night']],production.appearance,value=>{production.appearance=value;changed();render();}),el('p',{class:'fine muted',text:'Presentation changes retain the same editors, sources and unsaved domain work.'}));return;
  }
  if(!p){target.append(el('h2',{text:'Start with your material'}),el('p',{text:'Add an instrument, bring in a source, and connect the next piece of work. No content or account is copied into a new production.'}));return;}
  target.append(el('h2',{text:binding.label}));
  if(key==='source'){
    const label=el('input',{value:binding.label,maxLength:160,'aria-label':'Instrument label'});label.onchange=run(()=>{assertIdle();if(!label.value.trim())throw Error('Name the instrument.');binding.label=label.value;changed();p.frame.querySelector('.production-pane-title').textContent=label.value;renderOutline();});
    if(p.kind==='research')target.append(button('Choose retained sources',run(()=>chooseSources(p))));
    target.append(field('Working title',label),el('p',{class:'production-coordinate',text:p.record.key}),el('p',{class:'fine','data-record-revision':'',text:'Native revision '+p.record.revision+' · '+meta('bonfyre-workspace-context')}),button('Bind saved work',run(()=>openInstrument(p))),button(p.active?'Park this instrument':'Open this instrument',run(()=>p.active?(p.park(),compileScene(),render()):activate(p.id))),el('div',{class:'commands'},button('Move earlier',run(()=>{assertIdle();production=moveInstrument(production,p.id,-1);changed();compileScene();render();})),button('Move later',run(()=>{assertIdle();production=moveInstrument(production,p.id,1);changed();compileScene();render();}))),button('Remove from production',run(()=>{assertIdle();if(p.record.dirty&&!confirm('Remove this unsaved instrument? Saved records are kept.'))return;p.dispose();panes.delete(p.id);production=removeInstrument(production,p.id);changed();compileScene();render();})));
  }else{
    target.append(choice('Production role',[['source','Source material'],['working','Working analysis'],['output','Receiving output']],binding.role,value=>{binding.role=value;changed();renderOutline();}));
    if(p.kind==='research')target.append(choice('Sourced findings go to',[['','No receiving map'],...production.panels.filter(x=>x.kind==='product').map(x=>[x.id,x.label])],production.connections.find(e=>e.from===p.id)?.to??'',value=>{assertIdle();production=connectInstruments(production,p.id,value);changed();render();}),el('p',{class:'fine muted',text:'Select an exact passage, author and review a finding, then Offer as product opportunity. The receiving map gets a reviewable candidate; estimates and commitment remain yours.'}));
    else target.append(el('p',{text:p.kind==='product'?'Research inputs retain their source passages. Effort, reach, confidence, dependencies and capacities remain editable in the working map.':'This instrument performs its own validated operations. No external database or provider is changed by arranging it here.'}));
    target.append(el('a',{href:'/apps?'+new URLSearchParams({context:meta('bonfyre-workspace-context')?.startsWith('user:')?'self':meta('bonfyre-workspace-context')}),text:'Review native destination access ↗'}),el('p',{class:'fine muted',text:'Source, working and output describe the production, not permission grants. Membership and write rights remain with the destination owner.'}));
  }
}
function render(){host.dataset.appearance=production.appearance;stage.dataset.layout=production.layout;stage.dataset.focus=production.focus??'';title.value=production.title;description.value=production.description;
  stage.querySelector('[data-empty-canvas]')?.remove();for(const p of panes.values())p.frame.dataset.focused=String(p.id===production.focus);
  if(![...panes.values()].some(p=>p.active))stage.append(el('div',{class:'production-empty','data-empty-canvas':''},el('p',{class:'eyebrow',text:production.panels.length?'Work is parked':'A blank canvas'}),el('h2',{text:production.panels.length?'Open a piece of work.':'What will you make?'}),el('p',{text:'Combine real source reading, a product map, observed process traces and a database rehearsal. Choose the pieces your production needs.'}),...['research','product'].map(kind=>button('Add '+instrumentLabels[kind],run(async()=>{assertIdle();production=addInstrument(production,kind,{role:kind==='research'?'source':'output'});changed();const b=production.panels.at(-1);await createPane(b);await activate(b.id);}),'primary'))));
  renderOutline();renderInspector();refresh();
}
async function openInstrument(p){const rows=(await native.readLocal('draft:instruments:'+p.kind+':')).filter(r=>r.value?.kind===p.kind),body=el('div'),d=modal('Bind saved '+instrumentLabels[p.kind],body);for(const row of rows)body.append(button((row.value.title||row.key)+' · revision '+row.revision,run(async()=>{d.close();await p.load(row.key,row.revision);})));if(!rows.length)body.append(el('p',{text:'No saved records of this kind in this destination. Import material through the editor.'}));}
async function openProduction(){assertIdle();const rows=(await native.readLocal(productionPrefix)).filter(r=>r.value?.kind==='composition'),body=el('div'),d=modal('Open a saved production',body);for(const row of rows)body.append(button(row.value.title+' · revision '+row.revision,run(async()=>{d.close();await loadProduction(row.key,row.revision);})));if(!rows.length)body.append(el('p',{text:'This destination has no saved productions yet.'}));}
async function replaceProduction(next,rev=0){assertIdle();const candidate=structuredClone(validateAuthoredWorkspace(next,{saved:rev>0}));if(hasUnsaved()&&(production.panels.length||metadataEdited)&&!confirm('Leave current unsaved production edits? Saved work is unchanged.'))return;
  const prepared=new Map();
  try{for(const b of candidate.panels)await createPane(b,prepared,false);for(const b of candidate.panels.slice(0,2))await prepared.get(b.id).mount();}
  catch(error){for(const p of prepared.values()){try{p.dispose();}catch{p.record.dispose();p.frame.remove();}}throw error;}
  for(const p of panes.values())p.dispose();panes.clear();scene?.dispose();scene=null;production=candidate;metadataEdited=false;revision=rev;epoch++;savedEpoch=rev?epoch:-1;
  for(const [id,p]of prepared){panes.set(id,p);p.attach();}compileScene();render();if(rev)address();else{const u=new URL(location.href);u.search='';history.replaceState(null,'',u);}return true;
}
async function chooseSources(p){
  assertIdle();await activate(p.id);const key=p.record.key,version=p.record.revision,initial=p.active.capture(),before=await researchDigest(initial);
  const content=el('section',{class:'production-source-picker'}),dialog=modal('Choose sources for '+production.panels.find(x=>x.id===p.id).label,content);
  const picker=mountPublicationPicker(content,{endpoint,onApply:async inputs=>{
    assertIdle();if(!p.active||p.record.key!==key||p.record.revision!==version||await researchDigest(p.active.capture())!==before)throw Error('This research draft changed. Close source selection and review the current draft before adding material.');
    const prepared=await prepareResearchInputs({inputs,title:initial.title,existing:{key,revision:version,value:initial},sourceRecord});
    if(!dialog.open||p.record.key!==key||p.record.revision!==version||await researchDigest(p.active.capture())!==before)throw Error('The receiving research changed while preparing sources.');
    if(!prepared.added.length){tell('These excerpts already match the working research sources.');dialog.close();return;}
    if(!prepared.change.value.selectedSource)prepared.change.value.selectedSource=prepared.added[0];
    p.restore(prepared.change.value);dialog.close();tell(prepared.added.length+' retained excerpts added to the working research. Review the material, then save the production.');
  }});
  dialog.addEventListener('close',()=>picker.dispose(),{once:true});
}
async function loadProduction(key,expected=null){
  assertIdle();if(!key.startsWith(productionPrefix)||key.length>240)throw Error('Choose an exact production record.');const rows=(await native.readLocal(key)).filter(r=>r.key===key);if(rows.length!==1)throw Error('The requested production is unavailable. No example was substituted.');const row=rows[0];validateAuthoredWorkspace(row.value,{saved:true});if(productionPrefix+row.value.id!==key||expected!==null&&row.revision!==expected)throw Error('The production revision changed. Open saved work to review its current version.');
  for(const b of row.value.panels){const sources=(await native.readLocal(b.recordKey)).filter(r=>r.key===b.recordKey);if(sources.length!==1||sources[0].revision!==b.revision||sources[0].value?.kind!==b.kind)throw Error('A bound source changed: '+b.label+'. Review its current record before rebinding.');}
  if(await replaceProduction(row.value,row.revision))tell('Opened production revision '+revision+' with its exact source records.');
}
async function handoff(source,candidate){
  assertIdle();const edge=production.connections.find(x=>x.from===source.id),target=panes.get(edge?.to);if(!target)throw Error('Choose a receiving product map in Behavior before offering this finding.');
  await saveProduction();operation=true;body.inert=true;let incoming;try{incoming=await source.record.handoff(candidate);}finally{operation=false;body.inert=false;refresh();}
  try{await activate(target.id);const prepared=await preparedOpportunity(native,incoming.key),box=el('section',{class:'production-incoming','data-incoming':incoming.key},el('p',{class:'eyebrow',text:'Prepared from your saved research'}),el('h3',{text:prepared.candidate.title}));
    for(const ref of prepared.candidate.evidence)box.append(el('blockquote',{text:ref.quote}),el('small',{text:(ref.sourceTitle||ref.sourceId)+' · exact offsets '+ref.start+'–'+ref.end}));
    box.append(el('p',{text:'Review the candidate. Reach, effort and confidence were not inferred from the quotation.'}),button('Add prepared opportunity',run(async()=>{assertIdle();const latest=await preparedOpportunity(native,incoming.key);if(latest.row.revision!==prepared.row.revision)throw Error('The candidate changed; reopen its preparation.');target.restore(adoptOpportunity(target.active.capture(),latest));box.remove();tell('Prepared opportunity added. Edit its estimates, then save the connected production.');}),'primary'),button('Dismiss',()=>box.remove()));target.frame.querySelector('.production-incoming')?.remove();target.frame.insertBefore(box,target.frame.querySelector('.production-domain-workspace'));tell('The source and candidate are retained; review the prepared receiving map.');
  }catch(error){tell('Source and candidate saved. The receiving editor could not open: '+error.message,true);}return incoming;
}
for(const control of[title,description])control.addEventListener('input',()=>{if(operation||transition)return;metadataEdited=true;production[control===title?'title':'description']=control.value;changed();});
window.addEventListener('beforeunload',event=>{if(hasUnsaved()){event.preventDefault();event.returnValue='';}});
render();const initialURL=new URL(location.href),key=initialURL.searchParams.get('record'),expected=initialURL.searchParams.get('revision');
const publicDesign=initialURL.searchParams.get('design');
if(publicDesign&&key)tell('Choose a saved production or a public design, not both.',true);
else if(publicDesign){void run(async()=>{
  if(initialURL.searchParams.getAll('design').length!==1||!/^[A-Za-z0-9_-]{1,127}\/design-[a-f0-9]{64}$/.test(publicDesign))throw Error('Use the exact published design address.');
  const response=await fetch('/productions/'+publicDesign+'/design',{credentials:'omit',cache:'no-store',redirect:'error',signal:AbortSignal.timeout(15000)});
  if(!response.ok||!/^application\/msgpack(?:;|$)/i.test(response.headers.get('content-type')||''))throw Error('This design is no longer published or supported. No example was substituted.');
  const bytes=new Uint8Array(await response.arrayBuffer());if(bytes.length>30000)throw Error('The public design exceeds its allowed size.');
  await replaceProduction(instantiatePublicDesign(designFromPacket(decode(bytes))));
  tell('Public design prepared with new working identities. Bring your own sources, then save this production. No private content or authority was copied.');
})();}
else if(key){if(expected!==null&&!/^[1-9][0-9]*$/.test(expected))tell('Invalid production revision.',true);else void run(()=>loadProduction(key,expected===null?null:Number(expected)))();}
else tell('A blank native production. Add only the instruments and sources you need.');

addEventListener('pagehide',event=>{if(!event.persisted){commandPalette.destroy();inspectorTabs?.destroy();canvasSplit.destroy();}});
