import {catalogueSession,inventoryRows,inventoryProductRows,editInventoryCell,bulkInventoryProposal,inventoryChanges,inventorySaveBatch,inventoryDelta} from './catalogue-grid-model.mjs';
import {draftKey} from './catalogue-model.mjs';
import {saveLocalBatch} from './store.mjs';
import {escapeHTML as h} from './workspace-navigation.mjs';

let library;
function loadGrid(){
  if(!library)library=new Promise((resolve,reject)=>{
    const script=document.createElement('script');script.src=new URL('./ag-grid-community.min.js',import.meta.url);
    script.onload=()=>resolve(globalThis.agGrid);script.onerror=()=>{library=null;script.remove();reject(Error('The product table could not load. Retry or open the product editor.'));};document.head.append(script);
  });return library;
}
const button=(text,action,primary=false)=>`<button type="button" class="cw-button ${primary?'cw-button-primary':''}" data-grid-action="${action}">${text}</button>`;
export function inventoryWorkspace(host,{config,fetchProduct,records,openProduct,saved,inspectProduct}){
  let session=new Map(),api=null,generation=0,busy=false,filter='all',history=[],future=[],proposal=null,selected=null,active=false,mode='products',inspectorKey='';
  const $=name=>host.querySelector(`[data-grid="${name}"]`);
  const dirty=()=>inventoryChanges(session).length>0;
  const notice=(message,bad=false)=>{if(!$('notice'))return;$('notice').textContent=message;$('notice').classList.toggle('bad',bad);$('notice').setAttribute('role',bad?'alert':'status');};
  const snapshot=()=>[...session].map(([id,s])=>[id,structuredClone(s.edit)]);
  const checkpoint=()=>{history.push(snapshot());if(history.length>60)history.shift();future=[];};
  const restore=state=>{for(const[id,edit]of state)session.get(id).edit=structuredClone(edit);};
  function sync(){
    if(!api)return;
    const selectedKeys=new Set(api.getSelectedRows().map(r=>r.key));
    const options=inventoryRows(session),rows=mode==='products'?inventoryProductRows(session):options;api.setGridOption('rowData',rows);
    api.forEachNode(n=>n.setSelected(selectedKeys.has(n.data.key)));
    api.onFilterChanged();
    const changed=inventoryChanges(session),errors=[...session.values()].filter(s=>s.error).length;
    $('products').textContent=session.size;$('options').textContent=options.length;
    $('missing').textContent=options.filter(r=>r.missing).length;
    $('changes').textContent=changed.length;$('blocked').hidden=!errors;
    $('blocked').textContent=`${errors} products need their saved drafts checked in the product editor.`;
    host.querySelector('[data-grid-action="undo"]').disabled=!history.length||busy;
    host.querySelector('[data-grid-action="redo"]').disabled=!future.length||busy;
    host.querySelector('[data-grid-action="review"]').disabled=!changed.length||busy;
    host.querySelector('[data-grid-action="reset"]').disabled=!changed.length||busy;
    selection();if(selected)inspect(rows.find(r=>r.key===selected));
  }
  function selection(){if(!api||!$('selection'))return;const n=api.getSelectedRows().length;$('selection').textContent=`${n} selected`;$('bulk').hidden=!n;}
  function inspect(row){
    const nextKey=JSON.stringify(row);if(row&&nextKey===inspectorKey&&!$('inspector').hidden)return;inspectorKey=nextKey;
    selected=row?.key??null;if(!row)return;
    const s=session.get(row.productId),source=s.product.variants.find(v=>v.id===row.variantId);
    $('inspector').hidden=false;
    $('inspector').innerHTML=`<div class="cw-inspector-top"><span>PRODUCT DETAIL</span>${button('Close','close-inspector')}</div>${row.image?`<img src="${h(row.image)}" alt="" referrerpolicy="no-referrer">`:''}<span class="cw-tag">${h(row.status)}</span><h2>${h(row.title)}</h2><p>${h(row.option)}</p><dl><div><dt>Source price</dt><dd>${h(source.price)}</dd></div><div><dt>Draft price</dt><dd>${h(row.price)}</dd></div><div><dt>SKU</dt><dd>${h(row.sku||'Missing')}</dd></div><div><dt>Product ID</dt><dd>${h(row.productId)}</dd></div></dl><a class="cw-button" href="${h(s.product.source_url)}" target="_blank" rel="noopener noreferrer">Original listing ↗</a>${button('Open product editor','edit-product')}${inspectProduct?button('Configure & place in room','inspect-3d',true):''}<p class="cw-grid-muted">${h(row.error||'Source captured '+new Date(s.product.observed_at).toLocaleDateString())}</p>`;
    $('inspector').querySelector('img')?.addEventListener('error',e=>e.target.remove());
  }
  function openDialog(title,body,action,label){
    $('dialog-title').textContent=title;$('dialog-body').innerHTML=body;
    const confirm=$('dialog-confirm');confirm.textContent=label;confirm.dataset.gridAction=action;confirm.disabled=false;
    if(!$('dialog').open)$('dialog').showModal();
  }
  const changeRows=changes=>`<div class="cw-bulk-diffs">${changes.map(c=>`<div class="cw-bulk-diff"><div><strong>${h(c.title)}</strong><small>${h(c.option)} · ${h(c.field)}</small></div><del>${h(c.before||'Empty')}</del><ins>${h(c.after||'Empty')}</ins></div>`).join('')}</div>`;
  async function activate(store){
    const serial=++generation;active=true;api?.destroy();api=null;session=new Map();history=[];future=[];selected=null;filter='all';proposal=null;mode='products';inspectorKey='';
    host.innerHTML=`<div class="cw-inventory-head"><div><span class="cw-overline">${h(config.stores.find(s=>s.id===store).name)}</span><h1>Catalogue</h1></div><div class="cw-inventory-actions">${button('↶ Undo','undo')}${button('↷ Redo','redo')}${button('Discard edits','reset')}${button('Review changes','review',true)}</div></div>
      <div class="cw-inventory-metrics"><button data-grid-filter="all"><strong data-grid="products">—</strong> Products</button><button data-grid-filter="all"><strong data-grid="options">—</strong> Options</button><button data-grid-filter="missing"><strong data-grid="missing">—</strong> Missing SKUs</button><button data-grid-filter="changed"><strong data-grid="changes">0</strong> Edited products</button></div>
      <div class="cw-grid-tools"><div class="cw-grid-filters cw-grid-mode" aria-label="Row layout"><button data-grid-action="products" aria-pressed="true">Products</button><button data-grid-action="options" aria-pressed="false">Options</button></div><label class="cw-grid-search"><span class="sr-only">Search catalogue</span><input type="search" data-grid="search" placeholder="Search products, options, SKUs or tags" aria-label="Search catalogue"></label><div class="cw-grid-filters" aria-label="Catalogue views"><button data-grid-filter="all" aria-pressed="true">All</button><button data-grid-filter="missing" aria-pressed="false">Missing SKUs</button><button data-grid-filter="changed" aria-pressed="false">Edited</button><button data-grid-filter="saved" aria-pressed="false">Saved drafts</button></div><details class="cw-column-menu"><summary>Columns</summary><div data-grid="columns"></div></details></div>
      <div class="cw-bulk-bar" data-grid="bulk" hidden><strong data-grid="selection">0 selected</strong><label><span class="sr-only">Bulk action</span><select data-grid="bulk-action" aria-label="Bulk action"><option value="percent">Adjust price by %</option><option value="price">Set price</option><option value="tag">Add product tag</option></select></label><label><span class="sr-only">Bulk value</span><input data-grid="bulk-value" aria-label="Bulk value" placeholder="e.g. 10 or -15"></label>${button('Preview changes','preview')}${button('Clear selection','clear')}</div>
      <p data-grid="blocked" class="cw-errors" hidden></p><div class="cw-grid-stage"><div class="cw-data-grid" data-grid="table" aria-label="Catalogue options"></div><aside class="cw-product-inspector" data-grid="inspector" hidden></aside></div>
      <footer class="cw-grid-footer"><span data-grid="visible"></span><span>Double-click to edit · Tab to move · ⌘/Ctrl Z to undo</span><span>Browser drafts</span></footer><p class="cw-grid-notice" data-grid="notice" role="status" aria-live="polite">Loading catalogue…</p>
      <dialog class="cw-bulk-dialog" data-grid="dialog" aria-labelledby="catalogue-bulk-title"><header><h2 id="catalogue-bulk-title" data-grid="dialog-title"></h2>${button('Close','close-dialog')}</header><div data-grid="dialog-body"></div><footer><span data-grid="dialog-status" role="status">Store unchanged</span><button class="cw-button cw-button-primary" data-grid="dialog-confirm"></button></footer></dialog>`;
    try{
      const entries=config.entries.filter(p=>p.store===store);
      const [lib,products]=await Promise.all([loadGrid(),Promise.all(entries.map(fetchProduct))]);if(serial!==generation)return;
      session=catalogueSession(products,records());
      const rowText=p=>{const span=document.createElement('span');span.textContent=p.value??'';return span;};
      const setter=field=>p=>{if(busy)return false;try{checkpoint();editInventoryCell(session,p.data,field,p.newValue);sync();notice('');return false;}catch(e){history.pop();notice(e.message,true);return false;}};
      api=lib.createGrid($('table'),{
        theme:lib.themeQuartz.withParams({accentColor:'var(--cw-green)',backgroundColor:'var(--cw-paper)',foregroundColor:'var(--cw-ink)',borderColor:'var(--cw-rule)',headerBackgroundColor:'var(--cw-panel)',fontFamily:'var(--body)',fontSize:13,headerFontSize:11,rowHeight:62,headerHeight:44,wrapperBorderRadius:8,selectedRowBackgroundColor:'color-mix(in srgb,var(--cw-green) 12%,var(--cw-paper))',rowHoverColor:'var(--cw-panel)',browserColorScheme:'dark'}),
        rowData:[],getRowId:p=>p.data.key,defaultColDef:{sortable:true,resizable:true,filter:'agTextColumnFilter',minWidth:100,cellRenderer:rowText},
        rowSelection:{mode:'multiRow',selectAll:'filtered'},selectionColumnDef:{pinned:'left',width:44},animateRows:false,
        columnDefs:[
          {field:'title',headerName:'Product',pinned:host.clientWidth<700?null:'left',width:host.clientWidth<700?230:360,minWidth:180,editable:p=>!busy&&!p.data.error,valueSetter:setter('title'),getQuickFilterText:p=>p.value+' '+(p.data.search||''),cellRenderer:p=>{const wrap=document.createElement('div');wrap.className='cw-grid-product';if(p.data.image){const img=document.createElement('img');img.src=p.data.image;img.alt='';img.loading='lazy';img.referrerPolicy='no-referrer';img.onerror=()=>img.remove();wrap.append(img);}const name=document.createElement('span');name.textContent=p.value;wrap.append(name);return wrap;}},
          {field:'option',headerName:'Option',width:175},
          {field:'sku',headerName:'SKU',width:150,editable:p=>mode==='options'&&!busy&&!p.data.error,valueSetter:setter('sku'),cellClassRules:{'cw-cell-missing':p=>p.data.missing>0,'cw-cell-edited':p=>mode==='options'&&p.value!==session.get(p.data.productId).saved.variants.find(v=>v.id===p.data.variantId).sku}},
          {field:'price',headerName:'Price',width:150,editable:p=>mode==='options'&&!busy&&!p.data.error,valueSetter:setter('price'),comparator:(a,b)=>parseFloat(a)-parseFloat(b),cellClassRules:{'cw-cell-edited':p=>mode==='options'&&p.value!==session.get(p.data.productId).saved.variants.find(v=>v.id===p.data.variantId).price}},
          {field:'status',headerName:'Draft',width:125,cellRenderer:p=>{const span=document.createElement('span');span.className='cw-grid-status '+(p.data.changed?'is-edited':'');span.textContent=p.value;return span;}},
          {field:'tags',headerName:'Tags',width:240,editable:p=>!busy&&!p.data.error,valueSetter:setter('tags')},
          {field:'kind',headerName:'Type',width:110,hide:true},
        ],
        isExternalFilterPresent:()=>filter!=='all',doesExternalFilterPass:n=>filter==='missing'?n.data.missing>0:filter==='changed'?n.data.changed:session.get(n.data.productId).revision>0,
        onSelectionChanged:selection,onCellClicked:p=>{if(p.colDef.field)inspect(p.data);},
        onModelUpdated:()=>{if(api&&$('visible'))$('visible').textContent=`${api.getDisplayedRowCount()} ${mode} shown`;},
        onGridSizeChanged:()=>{if(api){const small=host.clientWidth<700;api.applyColumnState({state:[{colId:'title',pinned:small?null:'left',width:small?230:360}]});}},
      });
      for(const c of api.getColumnDefs().filter(c=>c.field)){
        const label=document.createElement('label'),input=document.createElement('input');input.type='checkbox';input.checked=!c.hide;
        input.addEventListener('change',()=>api.setColumnsVisible([c.field],input.checked));label.append(input,document.createTextNode(c.headerName));$('columns').append(label);
      }
      $('search').oninput=()=>api.setGridOption('quickFilterText',$('search').value);
      $('bulk-action').onchange=()=>{$('bulk-value').value='';$('bulk-value').placeholder=$('bulk-action').value==='tag'?'e.g. Summer collection':$('bulk-action').value==='price'?'e.g. 24.95':'e.g. 10 or -15';};
      $('dialog').addEventListener('cancel',e=>{if(busy)e.preventDefault();});
      sync();notice('');host.dataset.ready='true';
    }catch(error){if(serial===generation){notice(error.message,true);$('table').innerHTML=`<div class="cw-empty">${button('Retry catalogue','retry')}</div>`;}}
  }
  host.addEventListener('click',async event=>{
    const control=event.target.closest('[data-grid-action],[data-grid-filter]');if(!control||busy)return;
    const action=control.dataset.gridAction;
    if(control.dataset.gridFilter){filter=control.dataset.gridFilter;host.querySelectorAll('[data-grid-filter]').forEach(b=>b.setAttribute('aria-pressed',String(b.dataset.gridFilter===filter)));api?.onFilterChanged();return;}
    try{
      if(action==='products'||action==='options'){mode=action;api.deselectAll();$('inspector').hidden=true;selected=null;host.querySelectorAll('.cw-grid-mode button').forEach(b=>b.setAttribute('aria-pressed',String(b.dataset.gridAction===mode)));sync();return;}
      if(action==='retry'){await activate(host.dataset.store);return;}
      if(action==='undo'&&history.length){future.push(snapshot());restore(history.pop());sync();}
      if(action==='redo'&&future.length){history.push(snapshot());restore(future.pop());sync();}
      if(action==='reset'&&confirm('Discard all unsaved catalogue edits?')){checkpoint();for(const s of session.values())s.edit=structuredClone(s.saved);sync();}
      if(action==='clear')api.deselectAll();
      if(action==='close-inspector'){$('inspector').hidden=true;selected=null;}
      if(action==='inspect-3d'&&selected){const row=api.getRowNode(selected).data;await inspectProduct(session.get(row.productId).product,row.variantId);}
      if(action==='edit-product'&&selected){await openProduct(api.getRowNode(selected).data.productId);}
      if(action==='close-dialog')$('dialog').close();
      if(action==='preview'){
        api.stopEditing();const picked=api.getSelectedRows(),ids=new Set(picked.map(r=>r.productId));proposal=bulkInventoryProposal(session,mode==='products'?inventoryRows(session).filter(r=>ids.has(r.productId)):picked,$('bulk-action').value,$('bulk-value').value);
        openDialog(`Review ${proposal.changes.length} changes`,changeRows(proposal.changes),'apply','Apply to drafts');
      }
      if(action==='apply'&&proposal){checkpoint();session=proposal.copies;proposal=null;$('dialog').close();sync();notice('Applied to drafts. Review changes to save.');}
      if(action==='review'){
        api.stopEditing();const changed=inventoryChanges(session);
        const changes=changed.flatMap(inventoryDelta);
        openDialog(`Save ${changed.length} product drafts`,changeRows(changes),'save','Save all drafts');
      }
      if(action==='save'){
        const batch=inventorySaveBatch(session);busy=true;$('dialog-confirm').disabled=true;$('dialog-status').textContent='Saving…';
        try{const results=await saveLocalBatch(batch);for(const r of results){const s=[...session.values()].find(s=>draftKey(s.product)===r.key);s.revision=r.revision;s.saved=structuredClone(s.edit);}history=[];future=[];await saved(results);$('dialog').close();notice(`${results.length} product drafts saved. Open Saved drafts to send them for review.`);}
        catch(error){$('dialog-status').textContent=error.message;throw error;}
        finally{busy=false;$('dialog-confirm').disabled=false;sync();}
      }
    }catch(error){notice(error.message,true);}
  });
  document.addEventListener('keydown',event=>{
    if(!active||busy||$('dialog')?.open||event.target.closest('input,textarea,[contenteditable="true"]'))return;
    if((event.ctrlKey||event.metaKey)&&event.key.toLowerCase()==='z'){event.preventDefault();host.querySelector(`[data-grid-action="${event.shiftKey?'redo':'undo'}"]`)?.click();}
  });
  return {activate:async store=>{host.dataset.store=store;delete host.dataset.ready;await activate(store);},dirty,busy:()=>busy,close(){active=false;++generation;api?.destroy();api=null;session=new Map();}};
}
