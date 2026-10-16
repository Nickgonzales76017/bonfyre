import {createEditHistory, sameEdit} from './edit-history.mjs';
import {createRecordGrid} from './record-grid.mjs';
import {createReviewSheet, escapeText as h} from './review-sheet.mjs';

/** Rich collection interaction, extracted from the catalogue's working editor.
 * `binding` owns records, validation, source identity, domain commands and saves.
 * `presentation` supplies terminology, columns, facets and selected-record detail.
 * The renderer owns no storage and grants no permission.
 */
export function createCollectionWorkbench(host, {binding, presentation, loadLibrary}) {
  for (const name of ['load','rows','capture','restore','edit','changes','prepareBulk','prepareSave','persist','acceptSave','discard']) {
    if (typeof binding?.[name] !== 'function') throw new TypeError(`Missing collection binding: ${name}`);
  }
  if (!host || typeof loadLibrary !== 'function' || typeof presentation?.columns !== 'function') {
    throw new TypeError('Collection renderer requires a host, library and column presentation.');
  }
  let state, api, grid, scope, generation = 0, version = 0, active = false, busy = false,
    filter = 'all', mode = presentation.modes[0].id, selected = null, review,
    lifetime, editing = false, currentRows = [], lastInspector;
  const $ = name => host.querySelector(`[data-grid="${name}"]`);
  const actionButton = (text, action, primary = false) => `<button type="button" class="cw-button ${primary ? 'cw-button-primary' : ''}" data-grid-action="${h(action)}">${h(text)}</button>`;
  const history = createEditHistory({capture: () => binding.capture(state), restore: value => {state = binding.restore(state, value);}});
  const notice = (message = '', bad = false) => {
    if (!$('notice')) return;
    $('notice').textContent = message; $('notice').classList.toggle('bad', bad);
    $('notice').setAttribute('role', bad ? 'alert' : 'status');
  };
  const editable = () => active && !busy && (binding.canEdit?.(state) ?? true);
  const dirty = () => active && (editing || (state !== undefined && binding.changes(state).length > 0));
  const selectedRows = () => api?.getSelectedRows() ?? [];
  const notify = () => binding.onState?.({dirty:dirty(), busy, editing, scope});
  function selection() {
    if (!api) return;
    const count = selectedRows().length;
    $('selection').textContent = `${count} selected`;
    $('bulk').hidden = !count || !editable() || !presentation.bulk.length;
    presentation.onSelection?.(selectedRows());
  }
  function inspect(row, force = false) {
    if (!row) {cleanupInspector();selected = null; lastInspector = null; $('inspector').hidden = true; return;}
    selected = row.key;
    if (!force && sameEdit(lastInspector, row)) return;
    lastInspector = structuredClone(row);
    const detail = presentation.inspect?.(state, row);
    if (!detail) {$('inspector').hidden = true; return;}
    $('inspector').hidden = false;
    // The presentation supplies trusted markup; source strings are escaped there.
    // Custom interactive previews use the mount hook and its registered cleanup.
    cleanupInspector();
    $('inspector').innerHTML = detail.html;
    inspectorCleanup = detail.mount?.($('inspector')) ?? null;
    $('inspector').querySelector('img')?.addEventListener('error', e => e.target.remove(), {once:true});
  }
  let inspectorCleanup = null;
  function cleanupInspector() { inspectorCleanup?.(); inspectorCleanup = null; }
  function sync() {
    if (!api || !active) return;
    const rows = binding.rows(state, mode), keys = new Set();
    for (const row of rows) {
      if (typeof row.key !== 'string' || !row.key || keys.has(row.key)) throw Error('Collection rows require unique stable identities.');
      keys.add(row.key);
    }
    currentRows = rows; grid.setRows(rows);
    grid.setFilter($('search').value, filter==='all'?null:filter);
    for (const metric of presentation.metrics(state)) {
      const node = host.querySelector(`[data-grid-metric="${metric.id}"]`);
      if (node) node.textContent = String(metric.value);
    }
    const warning = presentation.warning?.(state) ?? '';
    $('blocked').textContent = warning; $('blocked').hidden = !warning;
    for (const [name, disabled] of [['undo',!history.canUndo],['redo',!history.canRedo],['review',!binding.changes(state).length],['reset',!binding.changes(state).length]]) {
      host.querySelector(`[data-grid-action="${name}"]`).disabled = disabled || !editable();
    }
    selection(); if (selected) inspect(rows.find(row => row.key === selected)); notify();
  }
  function transact(change) {
    if (!editable()) throw Error('This working copy is read-only or a save is in progress.');
    if (history.transact(change)) version++;
    sync();
  }
  function edit(row, field, raw) {
    try { transact(() => {const next = binding.edit(state,row,field,raw); if(next !== undefined) state = next;}); notice(); }
    catch (error) {notice(error.message,true);}
    return false; // AG Grid never becomes a second record owner.
  }
  function markup() {
    const words = presentation.words, titleId = `collection-review-${crypto.randomUUID()}`;
    return `<div class="cw-inventory-head"><div><span class="cw-overline">${h(presentation.scopeLabel(scope))}</span><h1>${h(words.title)}</h1></div><div class="cw-inventory-actions">${actionButton('↶ Undo','undo')}${actionButton('↷ Redo','redo')}${actionButton('Discard edits','reset')}${actionButton('Review changes','review',true)}</div></div>
      <div class="cw-inventory-metrics">${presentation.metricDefinitions.map(metric=>`<button data-grid-filter="${h(metric.filter)}"><strong data-grid="${h(metric.id)}" data-grid-metric="${h(metric.id)}">—</strong> ${h(metric.label)}</button>`).join('')}</div>
      <div class="cw-grid-tools"><div class="cw-grid-filters cw-grid-mode" aria-label="Row layout">${presentation.modes.map(view=>`<button data-grid-action="${h(view.id)}" data-grid-mode="${h(view.id)}" aria-pressed="${view.id===mode}">${h(view.label)}</button>`).join('')}</div><label class="cw-grid-search"><input type="search" data-grid="search" placeholder="${h(words.search)}" aria-label="${h(words.searchLabel)}"></label><div class="cw-grid-filters" aria-label="Collection views">${presentation.filters.map(view=>`<button data-grid-filter="${h(view.id)}" aria-pressed="${view.id==='all'}">${h(view.label)}</button>`).join('')}</div><details class="cw-column-menu"><summary>Columns</summary><div data-grid="columns"></div></details></div>
      <div class="cw-bulk-bar" data-grid="bulk" hidden><strong data-grid="selection">0 selected</strong><label><select data-grid="bulk-action" aria-label="Bulk action">${presentation.bulk.map(operation=>`<option value="${h(operation.id)}">${h(operation.label)}</option>`).join('')}</select></label><label><input data-grid="bulk-value" aria-label="Bulk value" placeholder="${h(presentation.bulk[0]?.placeholder ?? '')}"></label>${actionButton('Preview changes','preview')}${actionButton('Clear selection','clear')}</div>
      <p data-grid="blocked" class="cw-errors" hidden></p><div class="cw-grid-stage"><div class="cw-data-grid" data-grid="table" aria-label="${h(words.tableLabel)}"></div><aside class="cw-product-inspector" data-grid="inspector" hidden></aside></div>
      <footer class="cw-grid-footer"><span data-grid="visible"></span><span>Double-click to edit · Tab to move · ⌘/Ctrl Z to undo</span><span>${h(words.storage)}</span></footer><p class="cw-grid-notice" data-grid="notice" role="status" aria-live="polite">${h(words.loading)}</p>
      <dialog class="cw-bulk-dialog" data-grid="dialog" aria-labelledby="${titleId}"><header><h2 id="${titleId}" data-grid="dialog-title"></h2>${actionButton('Close','close-dialog')}</header><div data-grid="dialog-body"></div><footer><span data-grid="dialog-status" role="status"></span><button class="cw-button cw-button-primary" data-grid="dialog-confirm"></button></footer></dialog>`;
  }
  function close() {
    active = false; generation++; lifetime?.abort(); review?.destroy(); cleanupInspector();
    grid?.destroy();grid=null;api = null; state = undefined; currentRows = []; history.clear();
    selected = null; lastInspector = null; editing = false; busy = false;
    delete host.dataset.ready;
  }
  async function activate(nextScope) {
    close(); scope = nextScope; host.dataset.store = String(scope);
    const ticket = generation; active = true; mode = presentation.modes[0].id; filter = 'all'; version = 0;
    lifetime = new AbortController(); const {signal} = lifetime;
    host.innerHTML = markup(); host.dataset.component = 'collection-workbench';
    review = createReviewSheet({dialog:$('dialog'),title:$('dialog-title'),body:$('dialog-body'),confirm:$('dialog-confirm'),status:$('dialog-status'),currentVersion:()=>version});
    host.addEventListener('click', onClick, {signal});
    host.addEventListener('keydown', event => {
      if (!active || busy || review.open || event.target.closest('input,textarea,select,[contenteditable="true"]')) return;
      if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === 'z') {
        event.preventDefault(); host.querySelector(`[data-grid-action="${event.shiftKey?'redo':'undo'}"]`)?.click();
      }
    }, {signal});
    try {
      const [lib, loaded] = await Promise.all([loadLibrary(),binding.load(scope,{signal})]);
      if (!active || ticket !== generation || signal.aborted) return;
      state = loaded;
      const ports = {edit, editable, state:()=>state, mode:()=>mode, host, inspect:row=>inspect(row)};
      const columns = presentation.columns(ports);
      grid = createRecordGrid($('table'),{
        library:lib,key:row=>row.key,columns:()=>columns,
        canEdit:()=>editable(),label:presentation.words.tableLabel,
        onSelection:selection,onEditingChange:value=>{editing=value;notify();},
        onError:error=>notice(error.message,true),
        matches:(row,criterion)=>presentation.filters.find(value=>value.id===criterion)?.test(state,row)??true,
        options:{
          ...(presentation.gridOptions?.(lib,ports) ?? {}),
          selectionColumnDef:{pinned:'left',width:44},
          onCellClicked:p=>{if(p.colDef.field)inspect(p.data);},
          onModelUpdated:()=>{if(api&&$('visible'))$('visible').textContent=`${api.getDisplayedRowCount()} ${presentation.modes.find(view=>view.id===mode)?.label.toLowerCase()} shown`;},
          onGridSizeChanged:()=>{if(api)presentation.resize?.(api,host);}
        }
      });
      api=grid.api;
      for(const column of columns.filter(value=>value.field)) {
        const label=document.createElement('label'),input=document.createElement('input');
        input.type='checkbox';input.checked=!column.hide;
        input.addEventListener('change',()=>api?.setColumnsVisible([column.field],input.checked),{signal});
        label.append(input,document.createTextNode(column.headerName));$('columns').append(label);
      }
      $('search').addEventListener('input',()=>grid.setFilter($('search').value,filter==='all'?null:filter),{signal});
      $('bulk-action').addEventListener('change',()=>{
        $('bulk-value').value='';$('bulk-value').placeholder=presentation.bulk.find(value=>value.id===$('bulk-action').value)?.placeholder??'';
      },{signal});
      sync();notice();host.dataset.ready='true';
    } catch(error) {
      if(!active || ticket!==generation)return;
      notice(error.message,true);
      $('table').innerHTML=`<div class="cw-empty">${actionButton(presentation.words.retry,'retry')}</div>`;
      binding.onLoadFailure?.(error);
    }
  }
  async function onClick(event) {
    const control=event.target.closest('[data-grid-action],[data-grid-filter]');
    if(!control || !host.contains(control) || busy || !active)return;
    const action=control.dataset.gridAction;
    if(control.dataset.gridFilter) {
      filter=control.dataset.gridFilter;
      host.querySelectorAll('[data-grid-filter]').forEach(button=>button.setAttribute('aria-pressed',String(button.dataset.gridFilter===filter)));
      grid?.setFilter($('search').value,filter==='all'?null:filter);return;
    }
    try {
      if(action==='retry'){await activate(scope);return;}
      if(action==='close-dialog'){review.close();return;}
      if(action==='close-inspector'){cleanupInspector();inspect(null);return;}
      if(!api)return;
      if(control.dataset.gridMode) {
        const next=control.dataset.gridMode;
        if(!presentation.modes.some(view=>view.id===next))throw Error('Unknown row layout.');
        api.stopEditing();mode=next;api.deselectAll();cleanupInspector();inspect(null);
        host.querySelectorAll('[data-grid-mode]').forEach(button=>button.setAttribute('aria-pressed',String(button.dataset.gridMode===mode)));
        sync();return;
      }
      if(action==='clear'){api.deselectAll();return;}
      if(action==='undo' || action==='redo') {
        if(!editable())return;api.stopEditing();if(history[action]())version++;sync();return;
      }
      if(action==='reset') {
        if(!editable())return;
        if(globalThis.confirm(presentation.words.discard))transact(()=>{const next=binding.discard(state);if(next!==undefined)state=next;});
        return;
      }
      if(action==='preview') {
        if(!editable())return;api.stopEditing();
        const proposal=binding.prepareBulk(state,selectedRows(),mode,$('bulk-action').value,$('bulk-value').value);
        review.show({heading:`Review ${proposal.changes.length} changes`,changes:proposal.changes,action:'apply',label:presentation.words.apply,payload:proposal});return;
      }
      if(action==='apply') {
        const proposal=review.take('apply');
        transact(()=>{state=proposal.value;});review.close();notice(presentation.words.applied);return;
      }
      if(action==='review') {
        if(!editable())return;api.stopEditing();const proposal=binding.prepareSave(state);
        if(!proposal.changes.length)throw Error('There are no changed values to save.');
        review.show({heading:proposal.heading,changes:proposal.changes,action:'save',label:presentation.words.save,payload:proposal.payload});return;
      }
      if(action==='save') {
        if(!editable())return;
        const payload=review.take('save'),ticket=generation,sheet=review,original=state;
        busy=true;sheet.pending(true,'Saving…');notify();
        try {
          const result=await binding.persist(payload);
          if(!active || ticket!==generation)return;
          const next=binding.acceptSave(original,payload,result);if(next!==undefined)state=next;
          history.clear();version++;sheet.pending(false);sheet.close();notice(presentation.words.saved(result));
          // A downstream refresh failure must not turn a confirmed write into an unsaved edit.
          try{await binding.afterSave?.(result);}catch(error){if(active&&ticket===generation)notice(`Saved; refresh needs attention: ${error.message}`,true);}
        } catch(error) {if(active&&ticket===generation){sheet.message(error.message);notice(error.message,true);}}
        finally {if(active&&ticket===generation){busy=false;sheet.pending(false);sync();}}
        return;
      }
      if(presentation.actions?.[action]) {
        api.stopEditing();const row=currentRows.find(value=>value.key===selected);
        if(row)await presentation.actions[action](state,row);
      }
    } catch(error) {notice(error.message,true);}
  }
  return {activate,close,dirty,busy:()=>busy,refresh:sync};
}
