import {mountWorkbookAnalysis} from './workbook-analysis-view.mjs';
import {createScenarioBook,addScenarioDriver,removeScenarioDriver,currentDriver,saveAssumptionSet,previewAssumptionSet,acceptAssumptionPreview} from './workbook-scenarios.mjs';
import {parseCellAddress} from './workbook-lineage.mjs';
import {calculateScenario} from './workbook-scenario-calculation.mjs';

export function mountWorkbookScenarios(host,{getSnapshot,getScenarios,setScenarios,changed,runTask,applySnapshot}){
 const make=(tag,text)=>{const n=document.createElement(tag);if(text!==undefined)n.textContent=text;return n;};
 const control=(tag,label)=>{const n=make(tag);n.setAttribute('aria-label',label);return n;};
 const button=(label,key)=>{const n=make('button',label);n.type='button';if(key)n.dataset.scenarioAction=key;return n;};
 const field=(label,input)=>{const n=make('label',label);n.append(input);return n;};
 const status=make('p'),driverForm=make('form'),name=control('input','Driver name'),sheet=control('select','Driver worksheet'),address=control('input','Driver cell'),add=button('Add driver','add-driver');
 const drivers=make('div'),setForm=make('form'),setName=control('input','Assumption set name'),save=button('Save new set','save-set'),sets=control('select','Saved assumption set'),preview=button('Preview set','preview'),removeSet=button('Remove set','remove-set'),results=make('div'),apply=button('Apply to workbook','apply');
 host.className='finance-scenarios';host.dataset.workbookScenarios='';status.setAttribute('role','status');status.dataset.scenarioStatus='';results.dataset.scenarioResults='';drivers.dataset.scenarioDrivers='';
 name.maxLength=100;setName.maxLength=100;address.maxLength=10;address.value='A1';add.type='submit';save.type='submit';apply.hidden=true;
 driverForm.append(field('Driver name',name),field('Worksheet',sheet),field('Cell',address),add);
 setForm.append(field('Set name',setName),save);
 const choose=make('div');choose.className='fs-choose';choose.append(field('Saved assumption set',sets),preview,removeSet);
 host.append(make('h2','Assumption sets'),make('p','Name numeric input cells, save alternative values, then preview their effect on formulas. Drivers use worksheet addresses; check their meaning after moving rows. Sets are saved with Save review. XLSX downloads contain the applied cell values.'),driverForm,drivers,setForm,choose,status,results,apply);
 let active=false,disposed=false,busy=false,ticket=0,pending=null,controller=null,draftValues=false,analysisView;
 const book=()=>getScenarios()??createScenarioBook(getSnapshot());
 const notice=text=>{status.textContent=text;};
 function controls(){for(const n of host.querySelectorAll('button,input,select'))n.disabled=busy||disposed;preview.disabled=busy||!sets.value||draftValues;removeSet.disabled=busy||!sets.value;apply.disabled=busy||!pending;analysisView?.setBusy(busy);}
 function invalidate(){analysisView?.invalidate();ticket++;controller?.abort();controller=null;pending=null;results.replaceChildren();apply.hidden=true;if(active)notice('Preview a saved set to see its effect on the current workbook.');controls();}
 function choices(){const snapshot=getSnapshot(),selected=sheet.value;sheet.replaceChildren(...snapshot.sheetOrder.map(id=>{const n=make('option',snapshot.sheets[id].name);n.value=id;return n;}));if(snapshot.sheetOrder.includes(selected))sheet.value=selected;}
 function renderDrivers(){
  draftValues=false;drivers.replaceChildren();const snapshot=getSnapshot(),data=book(),selected=data.sets.find(s=>s.id===sets.value);
  if(!data.drivers.length){drivers.append(make('p','Add an input cell to start. Formula cells cannot be drivers.'));return;}
  for(const d of data.drivers){
   const row=make('div');row.className='fs-driver';const input=control('input',d.name+' value');input.type='number';input.step='any';input.dataset.driverId=d.id;input.required=true;input.oninput=()=>{draftValues=true;invalidate();notice('Save these values as a new set before previewing, or choose a saved set to restore its values.');};
   let text;try{const current=currentDriver(snapshot,d);text=current.address+' · Current '+current.value;input.value=String(selected?.values.find(v=>v.driverId===d.id)?.value??current.value);}catch(error){text=error.message;}
   const remove=button('Remove '+d.name);remove.onclick=()=>run(()=>update(removeScenarioDriver(getSnapshot(),book(),d.id)));
   row.append(field(d.name,input),make('span',text),remove);drivers.append(row);
  }
 }
 function refresh(){
  choices();const value=sets.value;sets.replaceChildren();
  const none=make('option','Choose a saved set');none.value='';sets.append(none);
  for(const s of book().sets){const n=make('option',s.name);n.value=s.id;sets.append(n);}
  sets.value=book().sets.some(s=>s.id===value)?value:'';renderDrivers();controls();analysisView?.refresh();
 }
 function update(next){setScenarios(next);changed();invalidate();refresh();}
 async function run(fn){
  if(busy||disposed||!active)return;
  try{await runTask(fn);}catch(error){if(active&&!disposed)notice(error.message);}
 }
 driverForm.onsubmit=event=>{event.preventDefault();run(()=>{
  const snapshot=getSnapshot(),cell=parseCellAddress(address.value,snapshot.sheets[sheet.value]);
  update(addScenarioDriver(snapshot,book(),{name:name.value,sheetId:sheet.value,...cell}));name.value='';notice('Driver added. Save review to retain it.');
 });};
 setForm.onsubmit=event=>{event.preventDefault();run(()=>{
  const data=book(),values=[...drivers.querySelectorAll('[data-driver-id]')].map(n=>{if(!n.value.trim())throw Error('Enter a value for every driver.');return{driverId:n.dataset.driverId,value:Number(n.value)};});
  const next=saveAssumptionSet(getSnapshot(),data,{name:setName.value,values});update(next);sets.value=next.sets.at(-1).id;renderDrivers();setName.value='';notice('Assumption set added. Save review to retain it.');
 });};
 sets.onchange=()=>{invalidate();renderDrivers();controls();};
 removeSet.onclick=()=>run(()=>{const next=structuredClone(book());next.sets=next.sets.filter(s=>s.id!==sets.value);update(next);notice('Set removed from this draft. Save review to retain the change.');});
 function table(title,rows){
  results.append(make('h3',title));const wrap=make('div'),t=make('table'),head=make('thead'),tr=make('tr'),body=make('tbody');wrap.className='fs-table';
  for(const label of ['Cell or driver','Current','Scenario']){const h=make('th',label);h.scope='col';tr.append(h);}head.append(tr);
  for(const r of rows){const line=make('tr');line.append(make('td',(r.name?r.name+' · ':'')+r.address),make('td',String(r.before??'')),make('td',String(r.after)));body.append(line);}
  t.append(head,body);wrap.append(t);results.append(wrap);
 }
 preview.onclick=()=>run(async()=>{
  if(draftValues)throw Error('Save the edited values as a new set before previewing.');
  invalidate();const attempt=ticket,abort=new AbortController();controller=abort;notice('Calculating this set in a separate workbook…');
  const result=await previewAssumptionSet(getSnapshot(),book(),sets.value,snapshot=>calculateScenario(snapshot,{signal:abort.signal}));
  if(disposed||!active||ticket!==attempt)return;
  pending=result;table('Input changes',result.changes);table('Formula results · '+result.outputCount+' changed',result.outputs);
  if(result.outputCount>result.outputs.length)results.append(make('p','Showing the first 100 changed formulas.'));
  notice('Preview ready. The current workbook has not changed.');apply.hidden=false;controls();
 });
 apply.onclick=()=>run(async()=>{
  const next=acceptAssumptionPreview(getSnapshot(),book(),pending),label=pending.name;
  await applySnapshot(next);invalidate();refresh();notice('Applied '+label+'. Save review to retain these values.');
 });
 const analysisPanel=make('section');host.append(analysisPanel);analysisView=mountWorkbookAnalysis(analysisPanel,{getSnapshot,getScenarios:book,runTask,saveBook:next=>{update(next);sets.value=next.sets.at(-1).id;renderDrivers();controls();}});
 return{
  activate(){active=true;refresh();analysisView.activate();notice('Choose a saved set to preview.');},
  deactivate(){active=false;analysisView.deactivate();invalidate();},
  invalidate,refresh(){if(active){invalidate();refresh();}},
  setBusy(value){busy=value;controls();},
  dispose(){disposed=true;active=false;analysisView.dispose();invalidate();host.replaceChildren();}
 };
}
