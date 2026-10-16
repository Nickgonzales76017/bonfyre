/** Virtualized record projection used by collection workbenches and manifest
 * editors. Selection, editors and row reconciliation share one implementation.
 * Values, units, validation, permissions and persistence stay with the binding.
 */
export function createRecordGrid(host, {
  library, rows = [], key = row => row.id, copyRows = value => value.map(row => ({...row})),
  columns, options = {}, onEdit = () => false, onSelection = () => {},
  onInspect = () => {}, onError = () => {}, onEditingChange = () => {},
  canEdit = () => true, matches = () => true, label = 'Record collection'
}) {
  if (!host || typeof library?.createGrid !== 'function' || typeof columns !== 'function') {
    throw new TypeError('A record grid needs its host, renderer library and domain columns.');
  }
  let api, destroyed = false, updating = false, criteria = null, selection = new Set();
  const id = row => {const value = key(row);if(typeof value!=='string'||!value)throw Error('Record identity must be a nonempty string.');return value;};
  const checkedRows = source => {
    if(!Array.isArray(source) || source.length>100000)throw Error('Record collection exceeds its row bound.');
    const next=copyRows(source),seen=new Set();
    if(!Array.isArray(next)||next.length!==source.length)throw Error('Record projection changed the collection length.');
    for(let i=0;i<next.length;i++){const value=id(next[i]);if(value!==id(source[i])||seen.has(value))throw Error('Record identities changed or repeat.');seen.add(value);}
    return next;
  };
  const report = error => {if(!destroyed)onError(error instanceof Error?error:Error(String(error)));};
  const selectedIds = () => !api||destroyed?[]:api.getSelectedRows().map(id);
  const hasPendingEdit = () => !!api&&!destroyed&&api.getEditingCells().length>0;
  const editingChanged = () => {if(!destroyed&&api)try{onEditingChange(hasPendingEdit());}catch(error){report(error);}};
  const emitSelection = () => {
    if(destroyed||updating||!api)return;
    const ids=selectedIds(),next=new Set(ids);
    if(next.size===selection.size && [...next].every(value=>selection.has(value)))return;
    selection=next;try{onSelection(ids);}catch(error){report(error);}
  };
  const setter = (field,parse=value=>value) => params => {
    if(destroyed||updating||!params.data||!canEdit(params.data,field))return false;
    try{const value=parse(params.newValue);if(!Object.is(value,params.data[field]))onEdit(params.data,field,value);}
    catch(error){report(error);}
    return false;
  };
  const inspect = row => {if(!destroyed)try{onInspect(id(row),row);}catch(error){report(error);}};
  const textCell = params => {const span=host.ownerDocument.createElement('span');span.textContent=params.valueFormatted??params.value??'';return span;};
  const colDefs=columns({setter,inspect,textCell,editable:canEdit});
  const userResize=options.onGridSizeChanged,userModel=options.onModelUpdated,userClick=options.onCellClicked;
  try {
    api=library.createGrid(host,{
      ...options,rowData:checkedRows(rows),getRowId:params=>id(params.data),
      defaultColDef:{sortable:true,resizable:true,filter:'agTextColumnFilter',minWidth:110,cellRenderer:textCell,cellDataType:false,...options.defaultColDef},
      rowSelection:options.rowSelection??{mode:'multiRow',selectAll:'filtered',enableClickSelection:false},
      selectionColumnDef:options.selectionColumnDef??{pinned:null,width:44,minWidth:44,maxWidth:44,resizable:false},
      animateRows:false,stopEditingWhenCellsLoseFocus:true,suppressDragLeaveHidesColumns:true,
      columnDefs:colDefs,isExternalFilterPresent:()=>criteria!==null,
      doesExternalFilterPass:node=>matches(node.data,criteria),
      onSelectionChanged:emitSelection,onCellEditingStarted:editingChanged,onCellEditingStopped:editingChanged,
      onGridSizeChanged:event=>{if(!destroyed&&api)userResize?.(event);},
      onModelUpdated:event=>{if(!destroyed)userModel?.(event);},
      onCellClicked:event=>{if(!destroyed)userClick?.(event);}
    });
    api.setGridAriaProperty('label',label);host.dataset.ready='true';host.dataset.component='record-grid';
  } catch(error) {api?.destroy();host.dataset.ready='error';throw error;}
  return {
    // Advanced presentation composition may use the renderer's column APIs.
    // The collection owner still uses setRows for identity-checked reconciliation.
    get api(){return destroyed?null:api;},
    setRows(nextRows){
      if(destroyed)return;
      const next=checkedRows(nextRows),ids=new Set(selectedIds());updating=true;
      try{api.setGridOption('rowData',next);api.forEachNode(node=>{const value=ids.has(id(node.data));if(node.isSelected()!==value)node.setSelected(value);});selection=new Set(selectedIds());}
      finally{updating=false;}
    },
    setFilter(text,nextCriteria=null){
      if(destroyed)return;criteria=nextCriteria;api.setGridOption('quickFilterText',String(text??''));api.onFilterChanged();
    },
    selectedIds,selectedRows:()=>!destroyed?api.getSelectedRows():[],hasPendingEdit,
    clearSelection(){if(destroyed)return;updating=true;try{api.deselectAll();}finally{updating=false;}emitSelection();},
    stopEditing(cancel=false){if(!destroyed)api.stopEditing(cancel);},
    destroy(){if(destroyed)return;destroyed=true;api.destroy();api=null;selection.clear();delete host.dataset.ready;delete host.dataset.component;}
  };
}
