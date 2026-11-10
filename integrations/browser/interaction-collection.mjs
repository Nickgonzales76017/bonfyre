export const workspaceElement=(tag,text,cls)=>{const n=document.createElement(tag);if(text!==undefined)n.textContent=String(text);if(cls)n.className=cls;return n;};
export const workspaceButton=(text,action)=>{const n=workspaceElement('button',text);n.type='button';if(action)n.onclick=action;return n;};
export const workspaceLink=(text,href)=>{const n=workspaceElement('a',text);n.href=href;return n;};
export function mountTextSearch(content,{label='Find in source',signal}={}){
  const doc=content.ownerDocument,events=new AbortController(),el=(tag,value)=>{const n=doc.createElement(tag);if(value!==undefined)n.textContent=value;return n;};interactionStyle(doc);
  if(content.textContent.length>4*1024*1024)throw Error('This text exceeds the reader limit.');signal?.throwIfAborted();
  const tools=el('div'),search=el('input'),next=el('button','Find next'),message=el('span');tools.className='ac-text-search';search.type='search';search.maxLength=512;search.placeholder=label;search.setAttribute('aria-label',label);next.type='button';message.setAttribute('role','status');tools.append(search,next,message);content.before(tools);content.classList.add('ac-reading-text');content.tabIndex=0;let from=0;
  function clear(){from=0;message.textContent='';const selected=doc.getSelection();if(selected?.rangeCount&&content.contains(selected.getRangeAt(0).commonAncestorContainer))selected.removeAllRanges();}
  function find(){if(events.signal.aborted)return;if(!search.value){clear();return;}const pattern=new RegExp(search.value.replace(/[.*+?^${}()|[\]\\]/g,'\\$&'),'giu'),spans=[...content.querySelectorAll('[data-original-start]')],nodes=spans.length?spans.map(n=>n.firstChild):[content.firstChild];let wrapped=false;
    for(let attempt=0;attempt<2;attempt++){let start=0;for(const node of nodes){if(node?.nodeType!==3)continue;pattern.lastIndex=Math.max(0,from-start);const match=pattern.exec(node.data);if(match){const range=doc.createRange();range.setStart(node,match.index);range.setEnd(node,match.index+match[0].length);const selected=doc.getSelection();selected.removeAllRanges();selected.addRange(range);from=start+match.index+match[0].length;const rect=range.getBoundingClientRect(),box=content.getBoundingClientRect();content.scrollTop+=rect.top-box.top-content.clientHeight/2;content.scrollIntoView({block:'nearest',inline:'nearest'});message.textContent=wrapped?'Wrapped to the first match.':'Match selected in source text.';return;}start+=node.length;}from=0;wrapped=true;}clear();message.textContent='No match in readable source text.';
  }
  search.addEventListener('input',clear,{signal:events.signal});search.addEventListener('keydown',event=>{if(event.key==='Enter'){event.preventDefault();find();}},{signal:events.signal});next.addEventListener('click',find,{signal:events.signal});
  function destroy(){clear();events.abort();signal?.removeEventListener('abort',destroy);tools.remove();}signal?.addEventListener('abort',destroy,{once:true});return {destroy};
}
/* Working selection and inspection only. Domain objects, permissions, mutations
 * and durable state stay with the caller's existing owners. */
const text = value => String(value ?? '');
export function collectionSelection(initialRows = [], {maxSelected=1000}={}) {
  if(!Number.isInteger(maxSelected)||maxSelected<1||maxSelected>1000)throw Error('Invalid selection limit.');
  let rows = new Map(), order = [], selected = new Set(), anchor = null, query = '', category = '', frozen = false, sort='source', selectedOnly=false;
  const listeners = new Set();
  const visible = () => {const shown=order.filter(id => {
    const row = rows.get(id);
    return (!selectedOnly||selected.has(id)) && (!category || row.category === category) && (!query || `${row.title} ${row.subtitle ?? ''} ${row.search ?? ''}`.toLocaleLowerCase().includes(query));
  });if(sort!=='source')shown.sort((a,b)=>sort==='newest'?(Number(rows.get(b).at)||0)-(Number(rows.get(a).at)||0):rows.get(a).title.localeCompare(rows.get(b).title)*(sort==='title-desc'?-1:1));return shown;};
  const emit = () => { for (const fn of listeners) fn(snapshot()); };
  function snapshot() {
    const shown = visible(), shownSet = new Set(shown), chosen = order.filter(id => selected.has(id));
    return {rows: order.map(id => rows.get(id)), visible: shown, selected: chosen,
      hiddenSelected: chosen.filter(id => !shownSet.has(id)).length,
      query, category, frozen, anchor, sort, selectedOnly, maxSelected};
  }
  function replace(next, {selectNew = false} = {}) {
    if (frozen) throw Error('Finish the current operation before replacing its working set.');
    if (!Array.isArray(next) || next.length > 1000) throw Error('Choose a bounded working set of up to 1000 objects.');
    const updated = new Map(), removed = [];
    for (const row of next) {
      if (!row || typeof row.id !== 'string' || !row.id || updated.has(row.id) || typeof row.title !== 'string' || typeof row.version !== 'string') throw Error('Every working object needs a unique identity, title and reviewed version.');
      updated.set(row.id, {...row});
    }
    const nextSelected=new Set(selected);
    for(const id of selected)if(!updated.has(id)||updated.get(id).selectable===false||updated.get(id).version!==rows.get(id)?.version){nextSelected.delete(id);removed.push(id);}
    if(selectNew)for(const[id,row]of updated)if(!rows.has(id)&&row.selectable!==false&&row.initiallySelected)nextSelected.add(id);
    if(nextSelected.size>maxSelected)throw Error('Too many initially selected objects.');
    rows=updated;order=[...rows.keys()];selected=nextSelected;if(!rows.has(anchor))anchor=null;
    emit(); return removed;
  }
  function toggle(id, {range = false, checked} = {}) {
    if (frozen || !rows.has(id) || rows.get(id).selectable === false) return;
    const shown = visible(), start = shown.indexOf(anchor), end = shown.indexOf(id), add = checked ?? !selected.has(id);
    const affected = range && start >= 0 && end >= 0 ? shown.slice(Math.min(start, end), Math.max(start, end) + 1) : [id];
    if(add&&new Set([...selected,...affected.filter(key=>rows.get(key).selectable!==false)]).size>maxSelected)throw Error('Choose at most '+maxSelected+' objects for this operation.');
    for (const key of affected) if (rows.get(key).selectable !== false) add ? selected.add(key) : selected.delete(key);
    if (!range || start < 0) anchor = id;
    emit();
  }
  replace(initialRows, {selectNew: true});
  return {
    snapshot, replace, toggle,
    subscribe(fn) { listeners.add(fn); return () => listeners.delete(fn); },
    filter(value = '', group = '') { if (frozen) return; query = text(value).trim().toLocaleLowerCase(); category = text(group); emit(); },
    selectVisible(checked = true) { if (frozen) return;const shown=visible().filter(id=>rows.get(id).selectable!==false);if(checked&&new Set([...selected,...shown]).size>maxSelected)throw Error('Choose at most '+maxSelected+' objects for this operation.');for(const id of shown)checked?selected.add(id):selected.delete(id);emit(); },
    sortBy(value='source'){if(frozen)return;if(!['source','title-asc','title-desc','newest'].includes(value))throw Error('Unsupported ordering.');sort=value;emit();},
    showSelected(value=true){if(frozen)return;selectedOnly=!!value;emit();},
    clear() { if (frozen) return; selected.clear(); anchor = null; emit(); },
    lock(value = true) { if(frozen===!!value)return;frozen = !!value; emit(); },
    selectedRows() { return Object.freeze(order.filter(id => selected.has(id)).map(id => Object.freeze({...rows.get(id)}))); }
  };
}
const stylesheet = `
.ac-collection{--ac-line:#bdc9c0;--ac-ink:#172a21;--ac-paper:#fbfcf8;--ac-soft:#eaf0e9;--ac-accent:#1d5945;color:var(--ac-ink);background:var(--ac-paper);border:1px solid var(--ac-line);border-radius:12px;font:15px/1.45 system-ui;min-width:0;overflow:hidden;container-type:inline-size}
.ac-collection *{box-sizing:border-box}.ac-collection [hidden]{display:none!important}.ac-collection[data-selection=false]>.ac-body>.ac-list .ac-check{display:none!important}.ac-collection[data-selection=false]>.ac-body>.ac-list .ac-row{grid-template-columns:minmax(0,1fr)}
.ac-collection button,.ac-collection input{font:inherit}.ac-collection button{cursor:pointer;min-height:44px;border:1px solid var(--ac-line);border-radius:6px;padding:8px 12px;background:var(--ac-paper);color:inherit}.ac-collection button:disabled{cursor:default;opacity:.55}.ac-collection :focus-visible{outline:3px solid var(--ac-accent);outline-offset:2px}
.ac-toolbar{display:flex;align-items:center;gap:8px;flex-wrap:wrap;padding:12px;border-bottom:1px solid var(--ac-line)}.ac-search{flex:1;min-width:160px;display:grid;gap:4px}.ac-search span{font-size:12px;font-weight:600}.ac-search input{width:100%;min-height:44px;border:1px solid var(--ac-line);border-radius:6px;padding:8px;background:var(--ac-paper);color:inherit}
.ac-filters{display:flex;gap:6px;flex-wrap:wrap;padding:0 12px 12px}.ac-filters button[aria-pressed=true]{background:var(--ac-accent);color:#fff;border-color:var(--ac-accent)}
.ac-summary{margin:0!important;padding:10px 12px;background:var(--ac-soft);font-size:13px;max-width:none!important}.ac-body{display:grid;grid-template-columns:minmax(240px,1fr) minmax(260px,1fr);min-height:260px;max-height:620px}.ac-list{overflow:auto;overscroll-behavior:contain;padding:6px;border-right:1px solid var(--ac-line);min-width:0}.ac-row{display:grid;grid-template-columns:44px minmax(0,1fr);gap:4px;border:1px solid transparent;border-radius:8px;padding:4px;margin-bottom:3px;min-width:0}.ac-row[data-selected=true]{background:var(--ac-soft);border-color:var(--ac-line)}.ac-row[data-inspected=true]{box-shadow:inset 3px 0 var(--ac-accent)}.ac-check{display:flex!important;align-items:center;justify-content:center;margin:0!important;min-height:44px;cursor:pointer}.ac-check input{appearance:auto!important;min-height:0!important;width:20px!important;height:20px!important;margin:0!important;padding:0!important;accent-color:var(--ac-accent)}
.ac-title{border:0!important;text-align:left;display:block;width:100%;background:transparent!important;min-width:0;overflow-wrap:anywhere}.ac-title strong{display:block;font-size:15px}.ac-title small{display:block;font-size:13px;margin-top:3px;opacity:.85}.ac-tag{display:inline-block;font-size:12px;padding:2px 6px;margin-top:5px;border:1px solid var(--ac-line);border-radius:4px}
.ac-inspector{padding:16px;overflow:auto;min-width:0}.ac-inspector h3{font-size:21px;line-height:1.25;margin:0 0 8px;overflow-wrap:anywhere}.ac-inspector h4{font-size:15px;margin:16px 0 8px}.ac-inspector p{max-width:none;margin:8px 0}.ac-inspector audio{max-width:100%;width:100%}.ac-inspect-nav{display:flex;justify-content:space-between;gap:8px;margin-bottom:12px}.ac-detail-empty{padding:24px 12px}.ac-change{padding:10px 0;border-top:1px solid var(--ac-line);overflow-wrap:anywhere}.ac-change dt{font-weight:650;margin-bottom:6px}.ac-change dd{margin:0;white-space:pre-wrap}.ac-before{color:#704137}.ac-after{color:#1a583f}.ac-notice{padding:10px;background:var(--ac-soft);border-radius:6px}
.ac-inspect-nav{align-items:center;flex-wrap:wrap}.ac-inspect-position{font-size:12px;color:var(--ac-ink);margin-left:auto}.ac-inspector[aria-busy=true]{cursor:progress}.ac-inspector:focus-visible{outline-offset:-3px}
.ac-collection{border-radius:18px;box-shadow:0 14px 40px #142b1b0c}.ac-toolbar{padding:16px;gap:10px;background:color-mix(in srgb,var(--ac-soft) 35%,var(--ac-paper))}.ac-toolbar label:not(.ac-search){display:grid;gap:4px;font-size:12px;font-weight:600}.ac-toolbar select{min-height:44px;border:1px solid var(--ac-line);border-radius:9px;background:var(--ac-paper);color:var(--ac-ink);padding:8px 10px;font:inherit}.ac-search input,.ac-collection button{border-radius:9px}.ac-filters{padding:4px 16px 14px}.ac-filters button{border-radius:999px;min-height:36px;font-size:12px;font-weight:600}.ac-summary{padding:11px 16px!important;font-size:12px;font-weight:600;color:var(--ac-ink)}.ac-list{padding:10px}.ac-row{border-radius:12px;margin-bottom:6px}.ac-row:hover{background:color-mix(in srgb,var(--ac-soft) 65%,var(--ac-paper))}.ac-row[data-inspected=true]{background:var(--ac-soft);border-color:var(--ac-line);box-shadow:inset 4px 0 var(--ac-accent)}.ac-title{padding:12px!important}.ac-title strong{font-size:15px;line-height:1.3}.ac-title small{line-height:1.45}.ac-tag{border:0;border-radius:999px;background:var(--ac-soft);color:var(--ac-accent);font-size:11px;font-weight:650;padding:4px 9px}.ac-row-icon:not([hidden]){float:left;display:grid;place-items:center;width:38px;height:38px;margin:0 12px 4px 0;border-radius:12px;background:color-mix(in srgb,var(--ac-accent) 11%,var(--ac-paper));color:var(--ac-accent)}.ac-row-icon svg{width:19px;height:19px}.ac-inspector{background:color-mix(in srgb,var(--ac-soft) 23%,var(--ac-paper));padding:20px}.ac-pagination{display:flex;align-items:center;justify-content:flex-end;gap:10px;padding:12px 16px;border-top:1px solid var(--ac-line);font-size:12px}.ac-pagination[hidden]{display:none}
@container(max-width:679px){.ac-body{grid-template-columns:1fr;max-height:none}.ac-list{max-height:420px;border-right:0}.ac-toolbar{align-items:stretch}.ac-search{flex-basis:100%}.ac-collection:not([data-details=true])>.ac-body>.ac-inspector{display:none}.ac-collection[data-details=true]>.ac-toolbar,.ac-collection[data-details=true]>.ac-filters,.ac-collection[data-details=true]>.ac-summary,.ac-collection[data-details=true]>.ac-pagination,.ac-collection[data-details=true]>.ac-body>.ac-list,.ac-collection[data-details=true]>.ac-body>.ak-split-handle{display:none!important}.ac-collection[data-details=true]>.ac-body{height:auto!important;min-height:0!important;max-height:none!important}.ac-collection[data-details=true]>.ac-body>.ac-inspector{max-height:none!important;overflow:visible}.ac-inspect-position{flex-basis:100%;order:1;margin:0}}
@media(prefers-reduced-motion:reduce){.ac-collection *{scroll-behavior:auto!important}}`;
export function mountCollection(host, {rows = [], label = 'Working objects', renderInspector, onSelection = () => {}, maxSelected=1000, pageSize=50, selection=true,ratio=55,renderRowIcon} = {}) {
  if(!Number.isInteger(pageSize)||pageSize<1||pageSize>100)throw Error('Use a page size from 1 to 100.');
  if (!host?.ownerDocument) throw Error('The working collection needs a document host.');
  const doc = host.ownerDocument, controller = collectionSelection(rows,{maxSelected}), listeners = new AbortController();
  const el = (tag, value) => { const n = doc.createElement(tag); if (value !== undefined) n.textContent = text(value); return n; };
  const root = el('section'); root.className = 'ac-collection';root.dataset.selection=String(selection); root.setAttribute('aria-label', label);
  const style = el('style', stylesheet), toolbar = el('div'); toolbar.className = 'ac-toolbar';
  const search = el('label'); search.className = 'ac-search'; const input = el('input'); input.type = 'search'; input.placeholder = 'Search this working set'; search.append(el('span', 'Find in ' + label.toLocaleLowerCase()), input);
  const choose = el('button','Select filtered'), clear = el('button','Clear selection'); choose.type = clear.type = 'button';
  const orderLabel=el('label','Order'),order=el('select');order.setAttribute('aria-label','Order '+label.toLowerCase());for(const[value,name]of [['source','Source order'],['title-asc','Title A–Z'],['title-desc','Title Z–A'],['newest','Newest first']]){const o=el('option',name);o.value=value;order.append(o);}orderLabel.append(order);
  const selectedFilter=el('button','Selected only');selectedFilter.type='button';selectedFilter.setAttribute('aria-pressed','false');
  toolbar.append(search,orderLabel,selectedFilter,choose,clear);choose.hidden=clear.hidden=selectedFilter.hidden=!selection; const filters = el('div'); filters.className = 'ac-filters'; filters.setAttribute('aria-label','Filter '+label.toLocaleLowerCase());
  const summary = el('p'); summary.className = 'ac-summary'; summary.setAttribute('role','status'); summary.setAttribute('aria-live','polite');
  const body = el('div'); body.className = 'ac-body'; const list = el('div'); list.className = 'ac-list'; list.setAttribute('role','group'); list.setAttribute('aria-label',label);
  const inspector = el('section'); inspector.className = 'ac-inspector'; inspector.id=freshId('details');inspector.setAttribute('aria-label','Object details');
  body.append(list,inspector); root.append(style,toolbar,filters,summary,body); host.replaceChildren(root);
  const split=mountSplitPane(body,{primary:list,secondary:inspector,label,ratio,stackAt:680});
  const pagination=el('nav');pagination.className='ac-pagination';pagination.setAttribute('aria-label',label+' pages');const pageBack=el('button','Previous page'),pageNext=el('button','Next page'),pageStatus=el('span');pageBack.type=pageNext.type='button';pagination.append(pageBack,pageStatus,pageNext);root.append(pagination);
  let pageIndex=0,lastCut='',inspected = null, detailAbort = null, detailCleanup = null, destroyed = false, viewEpoch = 0;
  function perform(fn){try{fn();}catch(error){summary.textContent=error.message;const state=controller.snapshot();for(const[id,part]of rendered)part.checkbox.checked=state.selected.includes(id);}}
  const rendered = new Map();
  function returnToList(){const id=inspected,state=controller.snapshot(),at=state.visible.indexOf(id);inspected=null;root.dataset.details='false';if(at>=0)pageIndex=Math.floor(at/pageSize);render(state);(rendered.get(id)?.title||input).focus();}
  function inspect(id, {focus = false} = {}) {
    const state = controller.snapshot(), row = state.rows.find(r => r.id === id);
    if (!row || destroyed) return;
    inspected = id;root.dataset.details='true';inspector.setAttribute('aria-busy',String(!!renderInspector));const epoch = ++viewEpoch; detailAbort?.abort(); detailCleanup?.(); detailCleanup = null; detailAbort = new AbortController();
    for (const [key,n] of rendered){n.node.dataset.inspected = String(key === id);n.title.setAttribute('aria-expanded',String(key===id));}
    inspector.replaceChildren(); const nav = el('div'); nav.className = 'ac-inspect-nav';
    const previous = el('button','Previous'), next = el('button','Next'); previous.type = next.type = 'button';
    const index = state.visible.indexOf(id); previous.disabled = index <= 0; next.disabled = index < 0 || index >= state.visible.length-1;
    previous.dataset.inspectStep='-1';next.dataset.inspectStep='1';const step=direction=>{const current=controller.snapshot(),at=current.visible.indexOf(inspected);if(at>=0)inspect(current.visible[at+direction],{focus:true});};previous.onclick=()=>step(-1);next.onclick=()=>step(1);const back=el('button','Back to list'),position=el('span',index<0?'Outside current filter':(index+1)+' of '+state.visible.length);back.type='button';back.dataset.inspectBack='';back.onclick=returnToList;position.className='ac-inspect-position';nav.append(back,position,previous,next);
    const heading = el('h3',row.title); heading.tabIndex = -1; inspector.append(nav,heading);
    if (row.subtitle) inspector.append(el('p',row.subtitle));
    const content = el('div'); inspector.append(content);
    const inspectionSignal=detailAbort.signal;
    if (renderInspector) {
      Promise.resolve().then(()=>renderInspector(row,content,{signal:inspectionSignal,element:el,selected:()=>controller.snapshot().selected.includes(id)}))
        .then(cleanup => { if (typeof cleanup !== 'function') return; if (epoch !== viewEpoch || destroyed) cleanup(); else detailCleanup = cleanup; })
        .catch(error => { if (epoch === viewEpoch && !destroyed && error.name !== 'AbortError') content.replaceChildren(el('p','Could not inspect this object: '+error.message)); }).finally(()=>{if(epoch===viewEpoch&&!destroyed)inspector.setAttribute('aria-busy','false');});
    } else content.append(el('p',row.description ?? row.status ?? 'Select this object to include it in your working set.'));
    if (focus) heading.focus();
  }
  function render(state) {
    if (destroyed) return;
    const focused=doc.activeElement,cut=state.query+'\0'+state.category+'\0'+state.sort+'\0'+state.selectedOnly;if(cut!==lastCut){pageIndex=0;lastCut=cut;}
    pageIndex=Math.min(pageIndex,Math.max(0,Math.ceil(state.visible.length/pageSize)-1));const pageIds=state.visible.slice(pageIndex*pageSize,(pageIndex+1)*pageSize);
    const byId = new Map(state.rows.map(r => [r.id,r])), shown = new Set(pageIds), selected = new Set(state.selected);
    if(focused!==input)input.value=state.query;order.value=state.sort;selectedFilter.setAttribute('aria-pressed',String(state.selectedOnly));
    for (const [id,part] of rendered) if (!shown.has(id)) {part.node.remove(); rendered.delete(id);}
    for (const row of pageIds.map(id=>byId.get(id))) {
      let part = rendered.get(row.id);
      if (!part) {
        const node = el('div'); node.className = 'ac-row'; node.dataset.objectId = row.id;
        const selectLabel = el('label'); selectLabel.className = 'ac-check'; const checkbox = el('input'); checkbox.type = 'checkbox'; checkbox.value = row.id; selectLabel.append(checkbox);
        const title = el('button'); title.type = 'button'; title.className = 'ac-title'; const icon=el('span');icon.className='ac-row-icon';icon.setAttribute('aria-hidden','true');const strong = el('strong'), sub = el('small'), tag = el('span'); tag.className = 'ac-tag';title.append(icon,strong,sub,tag);node.append(selectLabel,title);
        checkbox.addEventListener('click',event => perform(()=>controller.toggle(row.id,{checked:checkbox.checked,range:event.shiftKey})),{signal:listeners.signal});
        title.addEventListener('click',() => inspect(row.id,{focus:true}),{signal:listeners.signal});
        part={node,checkbox,title,icon,strong,sub,tag,iconCategory:null};rendered.set(row.id,part);list.append(node);
      }
      if(renderRowIcon&&part.iconCategory!==row.category){part.icon.replaceChildren();const visual=renderRowIcon(row);if(visual)part.icon.append(visual);part.iconCategory=row.category;}part.icon.hidden=!part.icon.childNodes.length;part.strong.textContent=row.title;part.sub.textContent=text(row.subtitle);part.sub.hidden=!row.subtitle;part.tag.textContent=text(row.status);part.tag.hidden=!row.status;
      part.checkbox.setAttribute('aria-label','Select '+row.title);part.checkbox.disabled=state.frozen||row.selectable===false;part.checkbox.checked=selected.has(row.id);
      part.node.dataset.selected=String(selected.has(row.id));part.node.dataset.inspected=String(inspected===row.id);part.title.setAttribute('aria-controls',inspector.id);part.title.setAttribute('aria-expanded',String(inspected===row.id));part.node.hidden=!shown.has(row.id);list.append(part.node);
    }
    pageBack.disabled=state.frozen||pageIndex===0;pageNext.disabled=state.frozen||(pageIndex+1)*pageSize>=state.visible.length;pagination.hidden=state.visible.length<=pageSize;pageStatus.textContent='Page '+(pageIndex+1)+' of '+Math.max(1,Math.ceil(state.visible.length/pageSize));
    const categories = [...new Set(state.rows.map(r=>r.category).filter(Boolean))];
    const focusedCategory=filters.contains(doc.activeElement)?doc.activeElement.dataset.category:null;filters.replaceChildren();
    for(const group of ['',...categories]){const count=group?state.rows.filter(r=>r.category===group).length:state.rows.length,b=el('button',`${group||'All'} · ${count}`);b.type='button';b.dataset.category=group;b.setAttribute('aria-pressed',String(state.category===group));b.disabled=state.frozen;b.onclick=()=>controller.filter(input.value,group);filters.append(b);if(focusedCategory===group)b.focus();}
    summary.textContent=state.visible.length+' shown of '+state.rows.length+' loaded'+(selection?' · '+state.selected.length+' selected'+(state.hiddenSelected?' · '+state.hiddenSelected+' selected outside this filter':''):'');
    choose.disabled=state.frozen||!state.visible.some(id=>byId.get(id).selectable!==false);clear.disabled=state.frozen||!state.selected.length;input.disabled=order.disabled=selectedFilter.disabled=state.frozen;
    if(!inspected||!byId.has(inspected)){const lost=inspector.contains(focused);inspected=null;root.dataset.details='false';viewEpoch++;detailAbort?.abort();detailCleanup?.();detailCleanup=null;inspector.setAttribute('aria-busy','false');inspector.replaceChildren(el('p','Open an object to inspect it without changing your selection.'));if(lost)input.focus();}
    if(!state.visible.length){if(!list.querySelector('.ac-detail-empty')){const empty=el('p','No objects match these filters. Your other selections are kept.');empty.className='ac-detail-empty';list.append(empty);}}else list.querySelector('.ac-detail-empty')?.remove();
    for(const b of inspector.querySelectorAll('[data-inspect-step]')){const at=state.visible.indexOf(inspected);b.disabled=at<0||!state.visible[at+Number(b.dataset.inspectStep)];}const position=inspector.querySelector('.ac-inspect-position'),at=state.visible.indexOf(inspected);if(position)position.textContent=at<0?'Outside current filter':(at+1)+' of '+state.visible.length;
    if(focused?.isConnected&&root.contains(focused))focused.focus({preventScroll:true});
    onSelection(controller.selectedRows(),state);
  }
  input.addEventListener('input',()=>controller.filter(input.value,controller.snapshot().category),{signal:listeners.signal});choose.onclick=()=>perform(()=>controller.selectVisible());clear.onclick=()=>controller.clear();order.onchange=()=>controller.sortBy(order.value);selectedFilter.onclick=()=>controller.showSelected(!controller.snapshot().selectedOnly);
  pageBack.onclick=()=>{pageIndex--;render(controller.snapshot());};pageNext.onclick=()=>{pageIndex++;render(controller.snapshot());};
  inspector.addEventListener('keydown',event=>{if(event.key==='Escape'&&!event.defaultPrevented&&!event.target.isContentEditable&&!event.target.closest('input,textarea,select')){event.preventDefault();event.stopPropagation();returnToList();}},{signal:listeners.signal});
  list.addEventListener('keydown',event=>{
    const node=event.target.closest('.ac-row');if(!node)return;const state=controller.snapshot(),index=state.visible.indexOf(node.dataset.objectId);
    if(selection&&(event.ctrlKey||event.metaKey)&&event.key.toLocaleLowerCase()==='a'){event.preventDefault();perform(()=>controller.selectVisible());return;}
    if(event.key==='ArrowDown'||event.key==='ArrowUp'){const target=state.visible[index+(event.key==='ArrowDown'?1:-1)];if(target){event.preventDefault();if(event.shiftKey)perform(()=>controller.toggle(target,{range:true,checked:true}));pageIndex=Math.floor(state.visible.indexOf(target)/pageSize);render(controller.snapshot());rendered.get(target).title.focus();rendered.get(target).node.scrollIntoView({block:'nearest'});}}
    if(selection&&event.key===' '&&event.target.classList.contains('ac-title')){event.preventDefault();perform(()=>controller.toggle(node.dataset.objectId,{range:event.shiftKey}));}
  },{signal:listeners.signal});
  const unsubscribe=controller.subscribe(render);render(controller.snapshot());
  return {controller,inspect,root,refresh(next){const dropped=controller.replace(next);if(inspected&&next.some(r=>r.id===inspected))inspect(inspected);return dropped;},destroy(){destroyed=true;viewEpoch++;detailAbort?.abort();detailCleanup?.();unsubscribe();listeners.abort();split.destroy();root.remove();}};
}

// Presentation state only. Consumers supply their current permitted actions.
let interactionId=0;
const freshId=prefix=>prefix+'-'+(++interactionId);
const interactionCSS=`.ak-split-handle{width:12px;min-width:12px;cursor:col-resize;touch-action:none;background:transparent;position:relative;outline-offset:-3px}.ak-split-handle::after{content:'';position:absolute;left:5px;top:12px;bottom:12px;width:2px;border-radius:2px;background:#b3c4a5}.ak-split-handle:hover::after,.ak-split-handle:focus-visible::after{background:#376141;width:3px}.ak-split-handle:focus-visible{outline:2px solid #376141}.ak-split[data-split-mode=stacked]{gap:16px!important}.ac-pagination{display:flex;justify-content:center;align-items:center;gap:14px;padding:12px;border-top:1px solid var(--ac-line);font-size:13px}.ac-toolbar>label:not(.ac-search){display:grid;gap:4px;font-size:12px}.ac-toolbar select{min-height:44px;max-width:180px;border:1px solid var(--ac-line);border-radius:6px;padding:8px;background:var(--ac-paper);font:14px system-ui;color:inherit}.ac-toolbar button[aria-pressed=true]{background:var(--ac-soft);border-color:var(--ac-accent)}.ac-inspector [role=tablist]{display:flex;gap:5px;border-bottom:1px solid var(--ac-line);padding:0 0 10px;margin:16px 0}.ac-inspector [role=tab][aria-selected=true]{background:var(--ac-soft);border-color:var(--ac-accent)}.ac-inspector blockquote{white-space:pre-wrap;overflow-wrap:anywhere;margin:16px 0;font:17px/1.7 Georgia,serif;padding:0 0 0 18px;border-left:3px solid #b6ca9b}.ac-inspector dl{display:grid;grid-template-columns:auto minmax(0,1fr);gap:12px;font-size:13px}.ac-inspector dt{font-weight:650}.ac-inspector dd{margin:0;overflow-wrap:anywhere}.ak-command-dialog{box-sizing:border-box;width:min(680px,calc(100vw - 24px));padding:24px;border:1px solid #abc399;border-radius:20px;color:#203c2a;background:#f8fbf2;max-height:85dvh;box-shadow:0 30px 100px #142b1d55;font:15px/1.5 system-ui}.ak-command-dialog::backdrop{background:#10231bb0;backdrop-filter:blur(4px)}.ak-command-dialog h2{font:400 30px Georgia,serif;margin:0 0 16px}.ak-command-dialog input{box-sizing:border-box;width:100%;min-height:48px;padding:12px;border:1px solid #a9be97;border-radius:10px;font:inherit;background:#fff}.ak-command-dialog [role=listbox]{max-height:45vh;overflow:auto;margin:14px 0}.ak-command-dialog [role=option]{padding:12px;border-radius:10px;margin:4px 0;cursor:pointer;display:flex;justify-content:space-between;gap:14px}.ak-command-dialog [aria-selected=true]{background:#e4efd6;outline:1px solid #acc294}.ak-command-dialog [aria-disabled=true]{opacity:.55;cursor:default}.ak-command-dialog [role=option] small{font-size:12px}.ak-command-dialog button{min-height:42px;padding:8px 16px;border:1px solid #b3c7a3;border-radius:999px;background:#fffef6;color:inherit;font:inherit}.ak-command-dialog footer{display:flex;align-items:center;justify-content:space-between;gap:12px;font-size:12px}.ak-command-dialog [role=status]{min-height:20px;font-size:13px}.ac-review-actions{display:flex;flex-wrap:wrap;gap:8px;margin:12px 0}.ac-review-status{padding:10px 12px;border-left:3px solid #739270;background:#edf2e8;font-size:13px}.ac-review-status[data-state=pending],.ac-review-status[data-state=conflict]{border-color:#856833;background:#fbf3de}.program-plan-review dl{display:block}.program-plan-review:not(:empty){margin:16px 0;padding:14px;border:1px solid #b8c8bf;border-radius:8px;background:#f8faf6}`;
const readerCSS=`.ac-text-search{display:flex;flex-wrap:wrap;align-items:center;gap:8px;padding:12px;background:#eaf0e9;border:1px solid #bdc9c0;border-radius:10px 10px 0 0;font:14px/1.4 system-ui;color:#172a21;min-width:0}.ac-text-search input{flex:1 1 160px;width:0;min-width:100px;min-height:44px;padding:9px 12px;border:1px solid #bdc9c0;border-radius:6px;background:#fff;color:inherit;font:inherit}.ac-text-search button{min-height:44px;padding:8px 12px;border:1px solid #bdc9c0;border-radius:6px;background:#fff;color:inherit;font:inherit;cursor:pointer}.ac-text-search [role=status]{flex-basis:100%;font-size:12px;min-height:17px}.ac-text-search :focus-visible,.ac-reading-text:focus-visible{outline:3px solid #1d5945;outline-offset:2px}.ac-reading-text{white-space:pre-wrap;overflow-wrap:anywhere;word-break:normal;max-height:420px;overflow:auto;overscroll-behavior:contain;padding:20px;margin:0 0 16px;border:1px solid #bdc9c0;border-top:0;border-radius:0 0 10px 10px;background:#fffef9;color:#172a21;font:16px/1.75 Georgia,serif;tab-size:4}.ac-reading-text::selection,.ac-reading-text ::selection{background:#d5e6a9;color:#122719}`;
function interactionStyle(doc){for(const[name,css]of [['layout',interactionCSS],['reader',readerCSS]])if(!doc.querySelector('style[data-interaction-'+name+']')){const style=doc.createElement('style');style.setAttribute('data-interaction-'+name,'');style.textContent=css;doc.head.append(style);}}
export function mountSplitPane(host,{primary,secondary,label='Working list',ratio=55,min=25,max=75,stackAt=720}={}){
 if(!host?.ownerDocument||primary?.parentNode!==host||secondary?.parentNode!==host||!(min>=10&&max<=90&&min<max))throw Error('Use two existing panes and bounded split settings.');
 const doc=host.ownerDocument,win=doc.defaultView,events=new AbortController(),oldStyle=host.getAttribute('style'),hadClass=host.classList.contains('ak-split');interactionStyle(doc);
 const handle=doc.createElement('div');handle.className='ak-split-handle';handle.tabIndex=0;handle.setAttribute('role','separator');handle.setAttribute('aria-orientation','vertical');handle.setAttribute('aria-label',label);handle.setAttribute('aria-valuemin',String(min));handle.setAttribute('aria-valuemax',String(max));
 const originalId=primary.id;if(!primary.id)primary.id=freshId('pane');handle.setAttribute('aria-controls',primary.id);host.insertBefore(handle,secondary);host.classList.add('ak-split');let value=Math.max(min,Math.min(max,ratio)),pointer=null,dead=false;
 function layout(){if(dead)return;const stacked=host.getBoundingClientRect().width<stackAt,single=secondary.hidden;handle.hidden=stacked||single;host.dataset.splitMode=single?'single':stacked?'stacked':'split';host.style.display='grid';host.style.gridTemplateColumns=stacked||single?'minmax(0,1fr)':`minmax(0,${value}fr) 12px minmax(0,${100-value}fr)`;host.style.columnGap='0px';handle.setAttribute('aria-valuenow',String(Math.round(value)));handle.setAttribute('aria-valuetext',Math.round(value)+' percent');}
 function set(next){if(!Number.isFinite(next))return;value=Math.max(min,Math.min(max,next));layout();}
 handle.addEventListener('keydown',event=>{let next;if(event.key==='ArrowLeft')next=value-(event.shiftKey?10:2);if(event.key==='ArrowRight')next=value+(event.shiftKey?10:2);if(event.key==='Home')next=min;if(event.key==='End')next=max;if(next!==undefined){event.preventDefault();set(next);}},{signal:events.signal});
 handle.addEventListener('pointerdown',event=>{if(event.button!==0)return;pointer=event.pointerId;handle.setPointerCapture(pointer);event.preventDefault();handle.focus();},{signal:events.signal});
 handle.addEventListener('pointermove',event=>{if(event.pointerId!==pointer)return;const rect=host.getBoundingClientRect();if(rect.width>0)set((event.clientX-rect.left)/rect.width*100);},{signal:events.signal});
 const release=event=>{if(event.pointerId===pointer)pointer=null;};handle.addEventListener('pointerup',release,{signal:events.signal});handle.addEventListener('pointercancel',release,{signal:events.signal});handle.addEventListener('lostpointercapture',release,{signal:events.signal});
 handle.addEventListener('dblclick',()=>set(ratio),{signal:events.signal});const resize=new win.ResizeObserver(layout),hidden=new win.MutationObserver(layout);resize.observe(host);hidden.observe(secondary,{attributes:true,attributeFilter:['hidden']});layout();
 return {handle,get value(){return value;},set,destroy(){dead=true;events.abort();resize.disconnect();hidden.disconnect();handle.remove();if(!hadClass)host.classList.remove('ak-split');delete host.dataset.splitMode;if(oldStyle===null)host.removeAttribute('style');else host.setAttribute('style',oldStyle);if(!originalId)primary.removeAttribute('id');}};
}
export function mountTabs(tablist,entries,{selected=entries?.[0]?.id,onSelect=()=>{}}={}){
 if(!tablist?.ownerDocument||!Array.isArray(entries)||!entries.length||new Set(entries.map(e=>e.id)).size!==entries.length||entries.some(e=>!e.button||!e.panel))throw Error('Tabs need distinct existing controls and panels.');
 const events=new AbortController();tablist.setAttribute('role','tablist');if(!tablist.hasAttribute('aria-label'))tablist.setAttribute('aria-label','Working view');let active=selected;
 for(const e of entries){if(!e.button.id)e.button.id=freshId('tab');if(!e.panel.id)e.panel.id=freshId('tabpanel');e.button.type='button';e.button.setAttribute('role','tab');e.button.setAttribute('aria-controls',e.panel.id);e.button.removeAttribute('aria-pressed');e.panel.setAttribute('role','tabpanel');e.panel.setAttribute('aria-labelledby',e.button.id);e.panel.tabIndex=0;}
 function render(){for(const e of entries){e.button.setAttribute('aria-selected',String(e.id===active));e.button.tabIndex=e.id===active?0:-1;e.panel.hidden=e.id!==active;}}
 function select(id,{notify=true}={}){const e=entries.find(e=>e.id===id);if(!e||e.button.disabled)return false;active=id;render();if(notify)onSelect(id);return true;}
 for(const e of entries)e.button.addEventListener('click',()=>select(e.id),{signal:events.signal});
 tablist.addEventListener('keydown',event=>{const available=entries.filter(e=>!e.button.disabled),at=available.findIndex(e=>e.button===event.target);if(at<0||!available.length)return;let next;if(event.key==='ArrowRight')next=(at+1)%available.length;if(event.key==='ArrowLeft')next=(at+available.length-1)%available.length;if(event.key==='Home')next=0;if(event.key==='End')next=available.length-1;if(next!==undefined){event.preventDefault();available[next].button.focus();}},{signal:events.signal});
 if(!entries.some(e=>e.id===active))active=entries[0].id;render();return {select,get selected(){return active;},destroy(){events.abort();}};
}
export function mountCommandPalette(trigger,{scope=trigger?.ownerDocument?.body,label='Workspace commands',commands,onError=()=>{}}={}){
 if(!trigger?.ownerDocument||typeof commands!=='function')throw Error('Commands must come from the current workspace.');const doc=trigger.ownerDocument,events=new AbortController();interactionStyle(doc);
 const el=(tag,value)=>{const n=doc.createElement(tag);if(value!==undefined)n.textContent=String(value);return n;},dialog=el('dialog');dialog.className='ak-command-dialog';const heading=el('h2',label),input=el('input'),list=el('div'),status=el('p'),footer=el('footer'),close=el('button','Close');heading.id=freshId('commands');dialog.setAttribute('aria-labelledby',heading.id);input.type='search';input.placeholder='Find an action';input.setAttribute('aria-label',label);input.setAttribute('role','combobox');input.setAttribute('aria-autocomplete','list');input.setAttribute('aria-expanded','true');list.id=freshId('command-results');list.setAttribute('role','listbox');list.setAttribute('aria-label','Matching commands');input.setAttribute('aria-controls',list.id);status.setAttribute('role','status');close.type='button';footer.append(el('span','↑ ↓ choose · Enter run · Esc close'),close);dialog.append(heading,input,list,status,footer);doc.body.append(dialog);let shown=[],index=-1,previous=null;
 function current(){const rows=commands();if(!Array.isArray(rows)||rows.length>100||new Set(rows.map(r=>r.id)).size!==rows.length||rows.some(r=>typeof r.id!=='string'||typeof r.label!=='string'||typeof r.run!=='function'))throw Error('The workspace command list is invalid.');return rows;}
 function mark(){for(const[n,row]of [...list.children].entries())row.setAttribute('aria-selected',String(n===index));if(index>=0){input.setAttribute('aria-activedescendant',list.children[index].id);list.children[index].scrollIntoView({block:'nearest'});}else input.removeAttribute('aria-activedescendant');}
 function draw(){try{shown=current().filter(row=>(row.label+' '+(row.group||'')).toLowerCase().includes(input.value.toLowerCase()));list.replaceChildren();for(const row of shown){const option=el('div');option.id=freshId('command');option.setAttribute('role','option');option.setAttribute('aria-disabled',String(!!row.disabled));option.append(el('span',row.label),el('small',row.disabled?(row.reason||'Unavailable now'):(row.group||'')));option.onclick=()=>execute(row.id);list.append(option);}index=shown.findIndex(r=>!r.disabled);status.textContent=shown.length?shown.length+' matching actions':'No matching actions';mark();}catch(e){status.textContent=e.message;}}
 function execute(id){try{const row=current().find(r=>r.id===id);if(!row||row.disabled){draw();return;}dialog.close();Promise.resolve().then(()=>row.run()).catch(onError);}catch(e){onError(e);}}
 function open(event){if(dialog.open)return;previous=event?.currentTarget===trigger?trigger:doc.activeElement;if(!previous||previous===doc.body)previous=trigger;previous.focus({preventScroll:true});input.value='';draw();dialog.showModal();input.focus();}
 trigger.addEventListener('click',open,{signal:events.signal});input.addEventListener('input',draw,{signal:events.signal});close.onclick=()=>dialog.close();dialog.addEventListener('close',()=>{if(previous?.isConnected&&(doc.activeElement===doc.body||dialog.contains(doc.activeElement)))previous.focus();},{signal:events.signal});
 input.addEventListener('keydown',event=>{if(event.key==='Escape'){event.preventDefault();event.stopPropagation();dialog.close();if(previous?.isConnected)previous.focus();return;}if(event.key==='Enter'){event.preventDefault();if(index>=0)execute(shown[index].id);return;}if(!['ArrowDown','ArrowUp','Home','End'].includes(event.key))return;event.preventDefault();const available=shown.map((r,i)=>r.disabled?-1:i).filter(i=>i>=0);if(!available.length)return;const at=available.indexOf(index);index=event.key==='Home'?available[0]:event.key==='End'?available.at(-1):available[(at+(event.key==='ArrowDown'?1:available.length-1))%available.length];mark();},{signal:events.signal});
 scope?.addEventListener('keydown',event=>{if((event.ctrlKey||event.metaKey)&&event.key.toLowerCase()==='k'&&!event.defaultPrevented){event.preventDefault();open();}},{signal:events.signal});return {open,dialog,destroy(){events.abort();dialog.remove();}};
}
