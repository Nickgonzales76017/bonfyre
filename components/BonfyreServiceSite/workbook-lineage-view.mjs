import {cellAddress,parseCellAddress,readWorkbookDependencies,inspectWorkbookDependencies} from './workbook-lineage.mjs';
export function mountWorkbookLineage(host,{getSnapshot,getFormula,getGeneration,runTask,openCell}){
 const make=(tag,text)=>{const n=document.createElement(tag);if(text!==undefined)n.textContent=text;return n;};
 host.className='finance-lineage';host.dataset.workbookLineage='';
 const heading=make('h2','Formula lineage'),help=make('p','Inspect the references used by a formula and the formulas affected by a cell. References include possible branches of conditional formulas.'),form=make('form'),sheet=make('select'),address=make('input'),inspect=make('button','Inspect cell'),status=make('p'),results=make('div');
 const sheetLabel=make('label','Worksheet'),cellLabel=make('label','Cell');sheetLabel.append(sheet);cellLabel.append(address);sheet.setAttribute('aria-label','Lineage worksheet');address.setAttribute('aria-label','Lineage cell');address.value='A1';address.maxLength=10;inspect.type='submit';inspect.dataset.lineageInspect='';status.setAttribute('role','status');status.dataset.lineageStatus='';results.dataset.lineageResults='';form.append(sheetLabel,cellLabel,inspect);host.append(heading,help,form,status,results);
 let active=false,disposed=false,ticket=0,busy=false;
 const controls=()=>{for(const n of [sheet,address,inspect])n.disabled=busy||disposed;};
 function choices(){const snapshot=getSnapshot(),selected=sheet.value;sheet.replaceChildren(...snapshot.sheetOrder.map(id=>{const option=make('option',snapshot.sheets[id].name);option.value=id;return option;}));if(snapshot.sheetOrder.includes(selected))sheet.value=selected;}
 function invalidate(){ticket++;results.replaceChildren();status.textContent='The workbook changed. Inspect a cell to refresh its dependencies.';}
 function link(sheetId,row,column,label){const button=make('button',label);button.type='button';const sheet=getSnapshot().sheets[sheetId];if(!sheet||row>=sheet.rowCount||column>=sheet.columnCount){button.disabled=true;button.title='This reference is outside the displayed worksheet.';}else button.onclick=()=>openCell(sheetId,row,column);return button;}
 function render(graph,view){
  const names=new Map(graph.sheets.map(s=>[s.id,s.name])),title=make('h3',view.cell.label),formula=make('code',view.formula||'This cell contains a value, not a formula.');formula.dataset.lineageFormula='';results.replaceChildren(title,formula);
  const inputs=make('ul');inputs.dataset.lineageInputs='';
  for(const r of view.inputs.slice(0,200)){const first=cellAddress(r.startRow,r.startColumn),last=cellAddress(r.endRow,r.endColumn),item=make('li');item.append(link(r.sheetId,r.startRow,r.startColumn,names.get(r.sheetId)+'!'+first+(first===last?'':':'+last)));inputs.append(item);}
  results.append(make('h3','Reads from'),inputs);if(!view.inputs.length)results.append(make('p','No cell references.'));
  if(view.inputs.length>200)results.append(make('p','Showing the first 200 of '+view.inputs.length+' reference ranges.'));
  for(const [label,rows,kind]of [['Directly affected formulas',view.directDependents,'direct'],['Affected through other formulas',view.indirectDependents,'indirect']]){
   const list=make('ul');list.dataset.lineageDependents=kind;results.append(make('h3',label+' · '+rows.length),list);
   for(const row of rows.slice(0,200)){const li=make('li');li.append(link(row.sheetId,row.row,row.column,names.get(row.sheetId)+'!'+cellAddress(row.row,row.column)),make('code',row.formula));list.append(li);}
   if(rows.length>200)results.append(make('p','Showing the first 200 affected formulas.'));
  }
  status.textContent='Inspected '+graph.nodes.length+' formulas in the current workbook. No cells were changed.';
 }
 form.onsubmit=async event=>{
  event.preventDefault();if(busy||disposed||!active)return;const attempt=++ticket;results.replaceChildren();
  try{await runTask(async()=>{
   const snapshot=getSnapshot(),generation=getGeneration(),sheetId=sheet.value,cell=parseCellAddress(address.value,snapshot.sheets[sheetId]);status.textContent='Reading formula dependencies…';
   const graph=await readWorkbookDependencies({snapshot,formula:getFormula(),generation,currentGeneration:getGeneration});
   if(disposed||!active||ticket!==attempt)return;render(graph,inspectWorkbookDependencies(graph,{sheetId,...cell}));
  });}catch(error){if(!disposed&&active&&ticket===attempt)status.textContent=error.message;}
 };
 return{activate(){active=true;choices();status.textContent='Choose a worksheet and cell to inspect.';},deactivate(){active=false;ticket++;},invalidate,setBusy(value){busy=value;controls();},dispose(){disposed=true;active=false;ticket++;host.replaceChildren();}};
}
