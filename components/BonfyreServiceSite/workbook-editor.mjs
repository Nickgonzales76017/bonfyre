import { createUniver } from '@univerjs/presets';
import { UniverSheetsCorePreset, CalculationMode } from '@univerjs/preset-sheets-core';
import UniverPresetSheetsCoreEnUS from '@univerjs/preset-sheets-core/locales/en-US';
import '@univerjs/preset-sheets-core/lib/index.css';
import './workbook-presentation.css';
import { validateWorkbook, calculationErrors, exportWorkbookSnapshot, hasFormulas } from './workbook-model.mjs';
import {createWorkbookLifetime,openWorkbookStore,guardWorkbookStore,clearWorkbookHost} from './workbook-lifetime.mjs';
import { workbookKey, readWorkbookReview, workbookReviewValue } from './workbook-record.mjs';
import { mountWorkbookReview } from './workbook-review-view.mjs';
import {mountWorkbookScenarios} from './workbook-scenarios-view.mjs';
import { mountWorkbookLineage } from './workbook-lineage-view.mjs';
import { withReviewSheet, setReviewCell } from './workbook-review.mjs';

// The caller supplies the record identity and a source-bearing initial model.
// The shared store resolves browser drafts or the native account binding.
export async function mountWorkbookEditor(host, { key, snapshot, source, store, lifetime=createWorkbookLifetime() }) {
  lifetime.assert();workbookKey(key);
  host.classList.add('finance-workbook'); host.dataset.view='workbook';
  validateWorkbook(snapshot);
  let revision = 0, generation = 0, savedGeneration = 0, busy = false, disposed = false, lastCalculation = -1;
  let review = null, scenarios=null, reviewView, lineageView, scenariosView,univer,univerAPI,workbook,changed,beforeUnload;
  const element = (tag, text) => { const node = document.createElement(tag); if (text) node.textContent = text; return node; };
  const toolbar = element('div'), title = element('strong', snapshot.name), status = element('p', 'Opening workbook…'), canvas = element('div'), issues = element('ul');
  toolbar.className = 'finance-workbook-toolbar'; canvas.className = 'finance-workbook-canvas'; status.setAttribute('role', 'status'); issues.setAttribute('aria-label', 'Calculation errors');
  const button = label => { const node = element('button', label); node.type = 'button'; toolbar.append(node); return node; };
  toolbar.append(title); const calculate = button('Recalculate'), save = button('Save review'), download = button('Download XLSX'), reload = button('Reload saved');
  const reviewButton = button('Transaction review'), workbookButton = button('Workbook'), reviewPanel = element('section'); reviewPanel.hidden = true;
  const lineageButton=button('Formula lineage'),lineagePanel=element('section');lineagePanel.hidden=true;
  const scenariosButton=button('Assumption sets'),scenariosPanel=element('section');scenariosPanel.hidden=true;scenariosButton.dataset.workbookAction='scenarios';scenariosButton.setAttribute('aria-pressed','false');
  const actions=element('div'),tabs=element('div'),sourceLine=element('p');actions.className='finance-workbook-actions';tabs.className='finance-workbook-tabs';tabs.setAttribute('role','group');tabs.setAttribute('aria-label','Working view');sourceLine.className='finance-workbook-source';status.className='finance-workbook-status';issues.className='finance-workbook-issues';
  for(const [control,name]of [[calculate,'calculate'],[save,'save'],[download,'export'],[reload,'reload']]){control.dataset.workbookAction=name;actions.append(control);}
  workbookButton.setAttribute('aria-pressed','true');reviewButton.setAttribute('aria-pressed','false');lineageButton.setAttribute('aria-pressed','false');lineageButton.dataset.workbookAction='lineage';tabs.append(workbookButton,reviewButton,lineageButton,scenariosButton);toolbar.append(actions,tabs);
  host.replaceChildren(toolbar,sourceLine,status,issues,canvas,reviewPanel,lineagePanel,scenariosPanel);

  const downloads=new Set();let retained;
  function close(){
    if(disposed)return;disposed=true;
    for(const n of host.querySelectorAll('input,textarea'))n.value='';for(const n of host.querySelectorAll('button,input,textarea,select'))n.disabled=true;for(const url of downloads)URL.revokeObjectURL(url);downloads.clear();
    try{for(const resource of [changed,reviewView,lineageView,scenariosView,univer]){try{resource?.dispose();}catch{}}}
    finally{if(beforeUnload)globalThis.removeEventListener('beforeunload',beforeUnload);source=null;review=null;scenarios=null;snapshot=null;retained=null;workbook=null;clearWorkbookHost(host);}
  }
  lifetime.onClose(close);
  store=store?guardWorkbookStore(store,lifetime):await openWorkbookStore(lifetime);lifetime.assert();
  try { retained = (await store.readLocal(key)).find(row => row.key === key); }
  catch (error) { if(!lifetime.closed)status.textContent = error.message; throw error; }
  if (retained) {
    const opened=await readWorkbookReview(retained);lifetime.assert();({ snapshot, source, revision, review, scenarios } = opened);
  }
  lifetime.assert();title.textContent = snapshot.name;
  if (!retained) generation++;
  const original = source?.original instanceof Blob ? button('Download original') : null;
  if(original)actions.append(original);
  const showSource=()=>{sourceLine.textContent=(source?.name||'Workbook')+' · '+snapshot.sheetOrder.length+' worksheets'+(source?.original instanceof Blob?' · Original file retained':'');};showSource();
  const downloadBlob = (blob, name) => {
    lifetime.assert();
    const url = URL.createObjectURL(blob), link = element('a'); downloads.add(url);link.href = url; link.download = name; link.click();
    setTimeout(() => {URL.revokeObjectURL(url);downloads.delete(url);}, 1000);
  };
  if (original) original.onclick = () => downloadBlob(source.original, source.name || 'original.xlsx');
  ({ univer, univerAPI } = createUniver({ locale: 'enUS', locales: { enUS: UniverPresetSheetsCoreEnUS },
    presets: [UniverSheetsCorePreset({ container: canvas, formula: { initialFormulaComputing: CalculationMode.FORCED } })] }));
  workbook = univerAPI.createWorkbook(structuredClone(snapshot));
  const controls = value => { for (const node of [calculate, save, download, reload, reviewButton, workbookButton,lineageButton,scenariosButton]) node.disabled = value; reviewView?.setBusy(value);lineageView?.setBusy(value);scenariosView?.setBusy(value); };
  const noteChange = () => { lifetime.assert();generation++; lastCalculation = -1; status.textContent = 'Unsaved changes';lineageView?.invalidate();scenariosView?.invalidate(); };
  changed = univerAPI.addEvent(univerAPI.Event.CommandExecuted, command => {
    if (command.id.startsWith('sheet.mutation.') && !command.options?.applyFormulaCalculationResult && !busy) {
      noteChange(); reviewView?.refresh();
    }
  });
  async function recalculate() {
    lifetime.assert();validateWorkbook(workbook.save());
    if (!hasFormulas(workbook.save())) { lastCalculation = generation; issues.replaceChildren(); return workbook.save(); }
    const formula = univerAPI.getFormula(), pending = formula.onCalculationResultApplied(10000);
    formula.executeCalculation(); await pending;lifetime.assert();
    const next = workbook.save(), errors = calculationErrors(next), circular = workbook.getCircularReferences();
    issues.replaceChildren(...errors.map(error => element('li', `${error.sheet}!${error.cell}: ${error.error}`)));
    if (circular.length) issues.append(element('li', 'Resolve circular references before saving or exporting.'));
    if (errors.length || circular.length) throw Error('Calculation needs review.');
    lastCalculation = generation; return next;
  }
  async function exclusive(fn) {
    lifetime.assert();if (busy || disposed) throw Error('Wait for the current workbook operation to finish.'); busy = true; controls(true); canvas.inert = true;
    try { const result=await fn();lifetime.assert();return result; }
    finally { busy = false; if(!disposed){controls(false); canvas.inert = false;} }
  }
  async function action(fn) { try { await exclusive(fn); } catch (error) { if(!disposed)status.textContent = error.message; } }
  const hideScenarios=()=>{scenariosPanel.hidden=true;scenariosButton.setAttribute('aria-pressed','false');scenariosView?.deactivate();};
  const showWorkbook = () => {hideScenarios(); lineagePanel.hidden=true;lineageButton.setAttribute('aria-pressed','false');lineageView?.deactivate();reviewPanel.hidden = true; canvas.hidden = false; host.dataset.view='workbook';workbookButton.setAttribute('aria-pressed','true');reviewButton.setAttribute('aria-pressed','false');reviewView.deactivate(); };
  reviewView = mountWorkbookReview(reviewPanel, { getSnapshot: () => workbook.save(), getSource: () => source, getReview: () => review,
    setReview: value => { lifetime.assert();review = value; }, changed: noteChange, recalculate, runTask: exclusive,
    editCell: async (row, role, value) => { setReviewCell(workbook, review, row, role, value); noteChange(); await recalculate(); },
    openCell: (sheetId, row, col) => { showWorkbook(); const sheet = workbook.getSheetBySheetId(sheetId); workbook.setActiveSheet(sheet); sheet.setActiveRange(sheet.getRange(row, col)); }
  });
  lineageView=mountWorkbookLineage(lineagePanel,{getSnapshot:()=>workbook.save(),getFormula:()=>univerAPI.getFormula(),getGeneration:()=>generation,runTask:exclusive,
    openCell:(sheetId,row,col)=>{showWorkbook();const sheet=workbook.getSheetBySheetId(sheetId);workbook.setActiveSheet(sheet);sheet.setActiveRange(sheet.getRange(row,col));}});
  scenariosView=mountWorkbookScenarios(scenariosPanel,{getSnapshot:()=>workbook.save(),getScenarios:()=>scenarios,setScenarios:value=>{lifetime.assert();scenarios=value;},changed:noteChange,runTask:exclusive,applySnapshot:async next=>{const previous=workbook.save();univerAPI.disposeUnit(workbook.getId());try{workbook=univerAPI.createWorkbook(next);}catch(error){workbook=univerAPI.createWorkbook(previous);throw error;}noteChange();await reviewView.refresh();}});
  scenariosButton.onclick=()=>{canvas.hidden=true;reviewPanel.hidden=true;lineagePanel.hidden=true;scenariosPanel.hidden=false;host.dataset.view='scenarios';for(const b of [workbookButton,reviewButton,lineageButton])b.setAttribute('aria-pressed','false');scenariosButton.setAttribute('aria-pressed','true');reviewView.deactivate();lineageView.deactivate();scenariosView.activate();};
  lineageButton.onclick=()=>{hideScenarios();canvas.hidden=true;reviewPanel.hidden=true;lineagePanel.hidden=false;host.dataset.view='lineage';workbookButton.setAttribute('aria-pressed','false');reviewButton.setAttribute('aria-pressed','false');lineageButton.setAttribute('aria-pressed','true');reviewView.deactivate();lineageView.activate();};
  workbookButton.onclick = showWorkbook;
  reviewButton.onclick = async () => {hideScenarios();lineagePanel.hidden=true;lineageButton.setAttribute('aria-pressed','false');lineageView.deactivate(); canvas.hidden = true; reviewPanel.hidden = false;host.dataset.view='transactions';workbookButton.setAttribute('aria-pressed','false');reviewButton.setAttribute('aria-pressed','true'); await reviewView.activate(); };
  calculate.onclick = () => action(async () => { await recalculate(); status.textContent = 'Calculated'; });
  save.onclick = () => action(async () => {
    const next = await recalculate(), version = generation;lifetime.assert();
    const result = await store.saveLocal(key, workbookReviewValue(next, source, review, scenarios), revision);
    lifetime.assert();revision = result.revision; savedGeneration = version; status.textContent = `Saved review · revision ${revision}`;
  });
  download.onclick = () => action(async () => {
    const next = await recalculate(), bytes = exportWorkbookSnapshot(await withReviewSheet(next, review));
    downloadBlob(new Blob([bytes], { type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' }), 'financial-review.xlsx');
    status.textContent = 'Review workbook downloaded';
  });
  reload.onclick = () => action(async () => {
    if (generation !== savedGeneration && !confirm('Discard unsaved edits and reload the saved review?')) return;
    const row = (await store.readLocal(key)).find(row => row.key === key);
    lifetime.assert();if (!row) throw Error('No saved review was found.');
    const retained = await readWorkbookReview(row);lifetime.assert();
    univerAPI.disposeUnit(workbook.getId()); workbook = univerAPI.createWorkbook(retained.snapshot);
    snapshot=retained.snapshot;source = retained.source; review = retained.review;scenarios=retained.scenarios; revision = retained.revision; title.textContent = retained.snapshot.name; generation++; savedGeneration = generation; lastCalculation = -1;
    lineageView.invalidate();scenariosView.refresh();showSource();await recalculate(); await reviewView.refresh(); status.textContent = `Opened revision ${revision}`;
  });
  beforeUnload = event => { if (generation !== savedGeneration || busy) { event.preventDefault(); event.returnValue = ''; } };
  globalThis.addEventListener('beforeunload', beforeUnload);
  status.textContent = revision ? `Opened revision ${revision}` : 'New review · not saved';
  return { snapshot: () => {lifetime.assert();return workbook.save();}, recalculate: () => exclusive(recalculate),
    get state() { return { revision, dirty: generation !== savedGeneration, calculated: lastCalculation === generation }; },
    dispose() { lifetime.close();close(); } };
}
