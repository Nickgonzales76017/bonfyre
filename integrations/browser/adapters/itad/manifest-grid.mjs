import { cents, dollars, money, boundedInt } from '../journey-kit/room.mjs';
import {createRecordGrid} from './renderers/record-grid.mjs';
const GRADES = ['unknown', 'working', 'untested', 'parts'];
let libraryPromise, loadAttempt = 0;
function loadLibrary() {
  if (globalThis.agGrid?.createGrid && globalThis.agGrid?.themeQuartz) return Promise.resolve(globalThis.agGrid);
  if (libraryPromise) return libraryPromise;
  libraryPromise = new Promise((resolve, reject) => {
    const script = document.createElement('script');
    const url = new URL('./vendor/ag-grid-community-36.2.0/ag-grid-community.min.js', import.meta.url);
    if (loadAttempt++) url.searchParams.set('retry', String(loadAttempt));
    script.src = url.href;script.async = true;let finished = false;
    const finish = error => {if(finished)return;finished=true;clearTimeout(timeout);script.onload=script.onerror=null;if(error){libraryPromise=null;script.remove();reject(error);}else resolve(globalThis.agGrid);};
    const timeout = setTimeout(() => finish(Error('The manifest table took too long to load. Retry the table.')), 20000);
    script.onload = () => finish(globalThis.agGrid?.createGrid && globalThis.agGrid?.themeQuartz ? null : Error('The manifest table could not start. Retry the table.'));
    script.onerror = () => finish(Error('The manifest table could not load. Retry the table.'));document.head.append(script);
  });return libraryPromise;
}
const copyRows = rows => rows.map(({id,model,qty,grade,recovery})=>({id,model,qty,grade,recovery}));
class RecoveryEditor {
  init(params) {
    this.input = document.createElement('input');this.input.className = 'ag-input-field-input ag-text-field-input';this.input.type = 'text';this.input.inputMode = 'decimal';
    this.input.setAttribute('aria-label', 'Modeled resale per unit in dollars');this.input.placeholder = 'Unpriced';this.input.value = params.eventKey?.length === 1 ? params.eventKey : dollars(params.value);
    this.input.style.cssText = 'width:100%;height:100%;box-sizing:border-box;padding:0 12px;';
  }
  getGui() { return this.input; }
  afterGuiAttached() { this.input.focus(); this.input.select(); }
  getValue() { return this.input.value; }
}
export async function createManifestGrid(host, {rows=[],onEdit=()=>false,onSelection=()=>{},onInspect=()=>{},onError=()=>{},onEditingChange=()=>{}}={}) {
  if (!(host instanceof HTMLElement)) throw Error('A manifest table host is required.');
  host.dataset.ready='loading';let lib;try{lib=await loadLibrary();}catch(error){host.dataset.ready='error';throw error;}
  let pinned=window.innerWidth>800 && host.clientWidth>800;
  const grid=createRecordGrid(host,{
    library:lib,rows,key:row=>row.id,copyRows,
    label:'Equipment manifest. Select lines with checkboxes. Double-click or press Enter to edit.',
    onEdit:(row,field,value)=>onEdit({ids:[row.id],patch:{[field]:value}}),onSelection,onInspect,onError,onEditingChange,
    matches:(row,grade)=>row.grade===grade,
    options:{
      theme:lib.themeQuartz.withParams({accentColor:'var(--manifest-accent, #f39c57)',backgroundColor:'var(--manifest-background, #102521)',foregroundColor:'var(--manifest-text, #edf0e4)',borderColor:'var(--manifest-border, #355047)',headerBackgroundColor:'var(--manifest-header, #19352e)',headerTextColor:'var(--manifest-muted, #c2d1c4)',fontFamily:'var(--body, "DM Sans", system-ui, sans-serif)',fontSize:13,headerFontSize:11,rowHeight:48,headerHeight:42,wrapperBorderRadius:8,selectedRowBackgroundColor:'var(--manifest-selected, #354638)',rowHoverColor:'var(--manifest-hover, #203d33)',browserColorScheme:'dark'}),
      onGridSizeChanged:({api})=>{const next=window.innerWidth>800&&host.clientWidth>800;if(next!==pinned){pinned=next;api.applyColumnState({state:[{colId:'model',pinned:pinned?'left':null}]});}},
      overlayNoRowsTemplate:'<span role="status">No manifest lines match this view.</span>'
    },
    columns:({setter,inspect})=>[
      {colId:'inspect',headerName:'',cellStyle:{paddingLeft:'8px',paddingRight:'8px',textOverflow:'clip'},width:76,minWidth:76,maxWidth:76,sortable:false,filter:false,resizable:false,editable:false,cellRenderer:params=>{const button=document.createElement('button');button.type='button';button.textContent='Open';button.className='manifest-open-line';button.setAttribute('aria-label',`Open line: ${params.data.model}`);button.addEventListener('click',event=>{event.stopPropagation();inspect(params.data);});return button;}},
      {field:'model',headerName:'Model / family',headerTooltip:'Seller-reported model or equipment family',pinned:pinned?'left':null,flex:1,minWidth:220,editable:true,cellEditor:'agTextCellEditor',cellEditorParams:{maxLength:100},valueSetter:setter('model',value=>{const model=String(value??'').trim();if(!model||model.length>100)throw Error('Enter a model or family using 1 to 100 characters.');return model;})},
      {field:'qty',headerName:'Quantity',headerTooltip:'Seller quantity',width:115,editable:true,filter:'agNumberColumnFilter',cellEditor:'agTextCellEditor',valueSetter:setter('qty',value=>boundedInt(value,{min:1,max:50000})),cellStyle:{textAlign:'right',fontVariantNumeric:'tabular-nums'}},
      {field:'grade',headerName:'Condition',headerTooltip:'Seller-reported condition',width:140,editable:true,cellEditor:'agSelectCellEditor',cellEditorParams:{values:GRADES},valueSetter:setter('grade',value=>{if(!GRADES.includes(value))throw Error('Choose unknown, working, untested, or parts.');return value;}),cellRenderer:params=>{const span=document.createElement('span');span.className='manifest-condition';span.dataset.grade=GRADES.includes(params.value)?params.value:'unknown';span.textContent=params.value??'unknown';return span;}},
      {field:'recovery',headerName:'Resale / unit',headerTooltip:'Modeled recovery per unit in USD. Blank means unpriced.',width:155,editable:true,filter:'agNumberColumnFilter',cellEditor:RecoveryEditor,useFormatterForExport:true,valueFormatter:params=>params.value==null?'Unpriced':money(params.value),valueGetter:params=>params.data.recovery,valueSetter:setter('recovery',value=>cents(String(value??'').trim(),{empty:true})),cellStyle:{textAlign:'right',fontVariantNumeric:'tabular-nums'}},
      {colId:'extendedRecovery',headerName:'Extended resale',headerTooltip:'Quantity × modeled resale per unit',width:165,editable:false,filter:'agNumberColumnFilter',valueGetter:params=>params.data.recovery==null?null:params.data.qty*params.data.recovery,valueFormatter:params=>params.value==null?'Unpriced':money(params.value),cellStyle:{textAlign:'right',fontVariantNumeric:'tabular-nums',fontWeight:'600'}}
    ]
  });
  return {setRows:grid.setRows,setFilter:(text,grade='all')=>grid.setFilter(text,GRADES.includes(grade)?grade:null),selectedIds:grid.selectedIds,hasPendingEdit:grid.hasPendingEdit,clearSelection:grid.clearSelection,stopEditing:grid.stopEditing,destroy:grid.destroy};
}
