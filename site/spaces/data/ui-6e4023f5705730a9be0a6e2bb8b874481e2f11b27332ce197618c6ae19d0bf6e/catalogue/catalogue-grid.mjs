import {composeCollectionWorkspace} from '../../../shared/interaction-98777b6f3a622c96dbfe/collection-composition.mjs';
import {loadWorkspaceDocking} from './workspace-dependencies.mjs';
import {catalogueSession,inventoryRows,inventoryProductRows,editInventoryCell,bulkInventoryProposal,inventoryChanges,inventorySaveBatch,inventoryDelta} from './catalogue-grid-model.mjs';
import {draftKey} from './catalogue-model.mjs';
import {saveLocalBatch} from './store.mjs';
import {escapeHTML as h} from './workspace-navigation.mjs';
import {createCollectionWorkbench} from '../../../shared/interaction-98777b6f3a622c96dbfe/collection-workbench.mjs';

let library;
function loadGrid(){
  if(!library)library=new Promise((resolve,reject)=>{
    const script=document.createElement('script');script.src=new URL('./ag-grid-community.min.js',import.meta.url);
    script.onload=()=>{if(globalThis.agGrid)resolve(globalThis.agGrid);else{library=null;script.remove();reject(Error('The table library did not provide its renderer.'));}};
    script.onerror=()=>{library=null;script.remove();reject(Error('The product table could not load. Retry or open the product editor.'));};document.head.append(script);
  });return library;
}
const button=(text,action,primary=false)=>`<button type="button" class="cw-button ${primary?'cw-button-primary':''}" data-grid-action="${action}">${text}</button>`;
export function inventoryWorkspace(host,{config,fetchProduct,records,openProduct,saved,inspectProduct}){
  const binding={
    async load(store){return catalogueSession(await Promise.all(config.entries.filter(p=>p.store===store).map(fetchProduct)),records());},
    rows:(state,mode)=>mode==='products'?inventoryProductRows(state):inventoryRows(state),
    capture:state=>[...state].map(([id,s])=>[id,s.edit]),
    restore(state,value){for(const[id,edit]of value){const row=state.get(id);if(!row)throw Error('The catalogue changed. Reopen it before restoring edits.');row.edit=structuredClone(edit);}return state;},
    edit:editInventoryCell,
    changes:inventoryChanges,
    prepareBulk(state,picked,mode,operation,value){
      const ids=new Set(picked.map(r=>r.productId)),rows=mode==='products'?inventoryRows(state).filter(r=>ids.has(r.productId)):picked;
      const proposal=bulkInventoryProposal(state,rows,operation,value);return {value:proposal.copies,changes:proposal.changes};
    },
    prepareSave(state){const changed=inventoryChanges(state);return {heading:`Save ${changed.length} product drafts`,changes:changed.flatMap(inventoryDelta),payload:inventorySaveBatch(state)};},
    persist:saveLocalBatch,
    acceptSave(state,batch,results){
      const byKey=new Map([...state.values()].map(s=>[draftKey(s.product),s]));
      const expected=new Set(batch.map(item=>item.key)),seen=new Set();
      if(!Array.isArray(results)||results.length!==batch.length)throw Error('The save response is incomplete. Reopen saved drafts before retrying.');
      for(const result of results){if(!expected.has(result.key)||seen.has(result.key)||!byKey.has(result.key)||!Number.isSafeInteger(result.revision)||result.revision<1)throw Error('The save response does not match the reviewed catalogue.');seen.add(result.key);}
      for(const result of results){const s=byKey.get(result.key);s.revision=result.revision;s.saved=structuredClone(s.edit);}
      return state;
    },
    afterSave:saved,
    discard(state){for(const s of state.values())s.edit=structuredClone(s.saved);return state;}
  };
  const presentation={
    compose:ports=>composeCollectionWorkspace({...ports,loadDocking:loadWorkspaceDocking,sourceDigests:()=>[...ports.state().values()].map(s=>s.product.source_sha256),changes:()=>inventoryChanges(ports.state()).flatMap(inventoryDelta)}),
    scopeLabel:store=>config.stores.find(s=>s.id===store)?.name??store,
    words:{title:'Catalogue',search:'Search products, options, SKUs or tags',searchLabel:'Search catalogue',tableLabel:'Catalogue options',storage:'Browser drafts',loading:'Loading catalogue…',retry:'Retry catalogue',discard:'Discard all unsaved catalogue edits?',apply:'Apply to drafts',applied:'Applied to drafts. Review changes to save.',save:'Save all drafts',saved:results=>`${results.length} product drafts saved. Open Saved drafts to send them for review.`},
    modes:[{id:'products',label:'Products'},{id:'options',label:'Options'}],
    metricDefinitions:[{id:'products',label:'Products',filter:'all'},{id:'options',label:'Options',filter:'all'},{id:'missing',label:'Missing SKUs',filter:'missing'},{id:'changes',label:'Edited products',filter:'changed'}],
    metrics(state){const rows=inventoryRows(state);return [{id:'products',value:state.size},{id:'options',value:rows.length},{id:'missing',value:rows.filter(r=>r.missing).length},{id:'changes',value:inventoryChanges(state).length}];},
    warning:state=>{const n=[...state.values()].filter(s=>s.error).length;return n?`${n} products need their saved drafts checked in the product editor.`:'';},
    filters:[{id:'all',label:'All'},{id:'missing',label:'Missing SKUs',test:(_,r)=>r.missing>0},{id:'changed',label:'Edited',test:(_,r)=>r.changed},{id:'saved',label:'Saved drafts',test:(s,r)=>s.get(r.productId).revision>0}],
    bulk:[{id:'percent',label:'Adjust price by %',placeholder:'e.g. 10 or -15'},{id:'price',label:'Set price',placeholder:'e.g. 24.95'},{id:'tag',label:'Add product tag',placeholder:'e.g. Summer collection'}],
    gridOptions:lib=>({theme:lib.themeQuartz.withParams({accentColor:'var(--cw-green)',backgroundColor:'var(--cw-paper)',foregroundColor:'var(--cw-ink)',borderColor:'var(--cw-rule)',headerBackgroundColor:'var(--cw-panel)',fontFamily:'var(--body)',fontSize:13,headerFontSize:11,rowHeight:62,headerHeight:44,wrapperBorderRadius:8,selectedRowBackgroundColor:'color-mix(in srgb,var(--cw-green) 12%,var(--cw-paper))',rowHoverColor:'var(--cw-panel)',browserColorScheme:'dark'})}),
    columns({edit,editable,state,mode}){
      const setter=field=>p=>edit(p.data,field,p.newValue);
      return [
        {field:'title',headerName:'Product',pinned:host.clientWidth<700?null:'left',width:host.clientWidth<700?230:360,minWidth:180,editable:p=>editable()&&!p.data.error,valueSetter:setter('title'),getQuickFilterText:p=>p.value+' '+(p.data.search||''),cellRenderer:p=>{const wrap=document.createElement('div');wrap.className='cw-grid-product';if(p.data.image){const img=document.createElement('img');img.src=p.data.image;img.alt='';img.loading='lazy';img.referrerPolicy='no-referrer';img.onerror=()=>img.remove();wrap.append(img);}const name=document.createElement('span');name.textContent=p.value;wrap.append(name);return wrap;}},
        {field:'option',headerName:'Option',width:175},
        {field:'sku',headerName:'SKU',width:150,editable:p=>mode()==='options'&&editable()&&!p.data.error,valueSetter:setter('sku'),cellClassRules:{'cw-cell-missing':p=>p.data.missing>0,'cw-cell-edited':p=>mode()==='options'&&p.value!==state().get(p.data.productId).saved.variants.find(v=>v.id===p.data.variantId).sku}},
        {field:'price',headerName:'Price',width:150,editable:p=>mode()==='options'&&editable()&&!p.data.error,valueSetter:setter('price'),comparator:(a,b)=>parseFloat(a)-parseFloat(b),cellClassRules:{'cw-cell-edited':p=>mode()==='options'&&p.value!==state().get(p.data.productId).saved.variants.find(v=>v.id===p.data.variantId).price}},
        {field:'status',headerName:'Draft',width:125,cellRenderer:p=>{const span=document.createElement('span');span.className='cw-grid-status '+(p.data.changed?'is-edited':'');span.textContent=p.value;return span;}},
        {field:'tags',headerName:'Tags',width:240,editable:p=>editable()&&!p.data.error,valueSetter:setter('tags')},
        {field:'kind',headerName:'Type',width:110,hide:true}
      ];
    },
    resize(api){const small=host.clientWidth<700;api.applyColumnState({state:[{colId:'title',pinned:small?null:'left',width:small?230:360}]});},
    inspect(state,row){const s=state.get(row.productId),source=s.product.variants.find(v=>v.id===row.variantId);return {html:`<div class="cw-inspector-top"><span>PRODUCT DETAIL</span>${button('Close','close-inspector')}</div>${row.image?`<img src="${h(row.image)}" alt="" referrerpolicy="no-referrer">`:''}<span class="cw-tag">${h(row.status)}</span><h2>${h(row.title)}</h2><p>${h(row.option)}</p><dl><div><dt>Source price</dt><dd>${h(source.price)}</dd></div><div><dt>Draft price</dt><dd>${h(row.price)}</dd></div><div><dt>SKU</dt><dd>${h(row.sku||'Missing')}</dd></div><div><dt>Product ID</dt><dd>${h(row.productId)}</dd></div></dl><a class="cw-button" href="${h(s.product.source_url)}" target="_blank" rel="noopener noreferrer">Original listing ↗</a>${button('Open product editor','edit-product')}${inspectProduct?button('Configure & place in room','inspect-3d',true):''}<p class="cw-grid-muted">${h(row.error||'Source captured '+new Date(s.product.observed_at).toLocaleDateString())}</p>`};},
    actions:{'edit-product':(_,row)=>openProduct(row.productId),'inspect-3d':(state,row)=>inspectProduct?.(state.get(row.productId).product,row.variantId)}
  };
  return createCollectionWorkbench(host,{binding,presentation,loadLibrary:loadGrid});
}
