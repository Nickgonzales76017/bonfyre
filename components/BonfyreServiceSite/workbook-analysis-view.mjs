import {calculateWorkbookAnalysis} from './workbook-scenario-calculation.mjs';
import {currentDriver,saveAssumptionSet} from './workbook-scenarios.mjs';
import {parseCellAddress} from './workbook-lineage.mjs';
export function mountWorkbookAnalysis(host,{getSnapshot,getScenarios,runTask,saveBook}){
 const make=(tag,text)=>{const n=document.createElement(tag);if(text!==undefined)n.textContent=text;return n;};
 const control=(tag,label,value)=>{const n=make(tag);n.setAttribute('aria-label',label);if(value!==undefined)n.value=String(value);return n;};
 const numeric=(label,value)=>{const n=control('input',label,value);n.type='number';n.step='any';n.required=true;return n;};
 const field=(label,input)=>{const n=make('label',label);n.append(input);return n;};
 const button=(label,key)=>{const n=make('button',label);n.type='button';n.dataset.analysisAction=key;return n;};
 const details=make('details'),summary=make('summary','Sensitivity and target values'),form=make('form'),driver=control('select','Analysis driver'),sheet=control('select','Analysis result worksheet'),cell=control('input','Analysis result cell','A1'),minimum=numeric('Minimum input',0),maximum=numeric('Maximum input',100),count=numeric('Sensitivity samples',5),goal=numeric('Target result',0),tolerance=numeric('Result tolerance',0.001);
 const sweep=button('Compare inputs','sensitivity'),seek=button('Find input for target','goal'),status=make('p'),results=make('div'),save=button('Save result as an assumption set','save-goal'),setName=control('input','Goal assumption set name'),saveForm=make('div');
 count.min='2';count.max='15';count.step='1';setName.maxLength=100;cell.maxLength=10;status.setAttribute('role','status');status.dataset.analysisStatus='';results.dataset.analysisResults='';saveForm.hidden=true;saveForm.className='fs-choose';saveForm.append(field('Set name',setName),save);
 form.append(field('Named driver',driver),field('Result worksheet',sheet),field('Formula cell',cell),field('Minimum input',minimum),field('Maximum input',maximum),field('Sample count',count),sweep,field('Target result',goal),field('Result tolerance',tolerance),seek);
 details.append(summary,make('p','Vary one named input and inspect a formula result. Target search checks a finite interval for up to 24 steps. It reports a calculated match within your result tolerance; it does not establish that a model is realistic or that the match is unique.'),form,status,results,saveForm);host.append(details);
 let active=false,busy=false,disposed=false,ticket=0,controller=null,pending=null,selectedDriver=null;
 const controls=()=>{for(const n of host.querySelectorAll('button,input,select'))n.disabled=busy||disposed;sweep.disabled=busy||!driver.value;seek.disabled=busy||!driver.value;save.disabled=busy||!pending?.result.solution;};
 function invalidate(){ticket++;controller?.abort();controller=null;pending=null;results.replaceChildren();saveForm.hidden=true;if(active)status.textContent='Choose an input and formula cell to analyze.';controls();}
 function bounds(){
  const d=getScenarios().drivers.find(d=>d.id===driver.value);if(!d)return;
  try{const {value}=currentDriver(getSnapshot(),d),a=value*.8,b=value*1.2;minimum.value=String(a===b?value-1:Math.min(a,b));maximum.value=String(a===b?value+1:Math.max(a,b));}catch(error){status.textContent=error.message;}
 }
 function refresh(){
  const data=getScenarios(),snapshot=getSnapshot(),old=driver.value,oldSheet=sheet.value;
  driver.replaceChildren(...data.drivers.map(d=>{const n=make('option',d.name);n.value=d.id;return n;}));if(data.drivers.some(d=>d.id===old))driver.value=old;
  sheet.replaceChildren(...snapshot.sheetOrder.map(id=>{const n=make('option',snapshot.sheets[id].name);n.value=id;return n;}));if(snapshot.sheetOrder.includes(oldSheet))sheet.value=oldSheet;
  if(driver.value!==selectedDriver){selectedDriver=driver.value;bounds();}controls();
 }
 driver.onchange=()=>{invalidate();selectedDriver=driver.value;bounds();};
 for(const n of [sheet,cell,minimum,maximum,count,goal,tolerance])n.addEventListener('input',invalidate);
 form.onsubmit=event=>event.preventDefault();
 function value(input){if(!input.value.trim()||!Number.isFinite(Number(input.value)))throw Error('Enter a finite number in '+input.getAttribute('aria-label')+'.');return Number(input.value);}
 async function run(fn){if(!active||busy||disposed)return;try{await runTask(fn);}catch(error){if(active&&!disposed)status.textContent=error.message;}}
 function table(rows){
  const wrap=make('div'),table=make('table'),head=make('thead'),tr=make('tr'),body=make('tbody');wrap.className='fs-table';
  for(const text of ['Input','Formula result','Change from current']){const th=make('th',text);th.scope='col';tr.append(th);}head.append(tr);
  for(const row of rows){const line=make('tr');for(const n of [row.input,row.result,Number.isFinite(row.result-pending.result.baseline.result)?row.result-pending.result.baseline.result:'Outside numeric range'])line.append(make('td',String(n)));body.append(line);}
  table.append(head,body);wrap.append(table);results.append(wrap);
 }
 function analyze(kind){return run(async()=>{
  invalidate();const snapshot=getSnapshot(),book=getScenarios(),low=value(minimum),high=value(maximum);if(low>=high)throw Error('Minimum input must be less than maximum input.');
  const target={sheetId:sheet.value,...parseCellAddress(cell.value,snapshot.sheets[sheet.value])},request={kind,book,driverId:driver.value,target};
  if(kind==='sensitivity'){const steps=value(count);if(!Number.isInteger(steps)||steps<2||steps>15)throw Error('Choose two to 15 samples.');request.values=Array.from({length:steps},(_,i)=>low*(1-i/(steps-1))+high*(i/(steps-1)));}
  else Object.assign(request,{minimum:low,maximum:high,goal:value(goal),tolerance:value(tolerance)});
  const attempt=ticket,abort=new AbortController();controller=abort;status.textContent='Calculating in an isolated workbook…';
  const result=await calculateWorkbookAnalysis(snapshot,request,{signal:abort.signal});if(!active||disposed||ticket!==attempt)return;
  pending={result,base:JSON.stringify(snapshot),book:JSON.stringify(book)};results.append(make('h3',result.target.address),make('p','Current input '+result.baseline.input+' → result '+result.baseline.result));
  if(kind==='sensitivity'){table(result.rows);status.textContent='Compared '+result.rows.length+' inputs. No workbook cells changed.';}
  else{table([result.solution]);results.append(make('p','Difference from target: '+result.solution.residual+' · Allowed difference: '+result.tolerance));setName.value='Target '+result.goal;saveForm.hidden=false;status.textContent='A calculated input met the result tolerance. Save it as a set to review its other effects.';}
  controls();
 });}
 sweep.onclick=()=>analyze('sensitivity');seek.onclick=()=>analyze('goal');
 save.onclick=()=>run(()=>{
  const snapshot=getSnapshot(),book=getScenarios();if(!pending?.result.solution||JSON.stringify(snapshot)!==pending.base||JSON.stringify(book)!==pending.book)throw Error('The workbook or assumptions changed. Run the analysis again.');
  const result=pending.result,values=book.drivers.map(d=>({driverId:d.id,value:d.id===result.driver.id?result.solution.input:currentDriver(snapshot,d).value}));
  const next=saveAssumptionSet(snapshot,book,{name:setName.value,values});saveBook(next);invalidate();status.textContent='Set added. Preview it above before applying it; Save review retains the set.';
 });
 return{activate(){active=true;refresh();},deactivate(){active=false;invalidate();},refresh(){if(active){invalidate();refresh();}},invalidate,setBusy(value){busy=value;controls();},dispose(){disposed=true;active=false;invalidate();host.replaceChildren();}};
}
