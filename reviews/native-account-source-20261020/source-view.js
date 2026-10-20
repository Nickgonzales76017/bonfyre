// Replaces retrieval and list orchestration inside the existing link.html closure.
// Existing detail, readiness and native owner functions remain the receiving path.
function reviewRows() {
  const rows=twins.filter(show).sort((a,b)=>host(a.url).localeCompare(host(b.url))), box=$('list');
  const oldFocus=document.activeElement?.dataset?.sourceKey, oldAction=document.activeElement?.dataset?.sourceAction;
  box.replaceChildren();
  $('shown').textContent=rows.length+' shown · '+twins.length+' loaded / '+total+(total!==estateTotal?' matches · '+estateTotal+' total':'');
  if(cut.message){const alert=E('p','notice',cut.message);alert.setAttribute('role','alert');box.append(alert);}
  if(cut.state==='loading'||cut.state==='waiting')box.append(E('p','empty','Reading the selected source cut…'));
  for(const source of rows){
    const key=cut.key(source),wrap=E('div','source-row'),checkbox=E('input'),button=E('button','row'+(source===picked?' sel':''));
    checkbox.type='checkbox';checkbox.checked=cut.selected.has(key);checkbox.disabled=cut.selectionStale||!['ready','failed-page'].includes(cut.state);
    checkbox.dataset.sourceKey=key;checkbox.dataset.sourceAction='select';checkbox.setAttribute('aria-label','Compare '+host(source.url)+' · '+B(source).source);
    checkbox.onchange=()=>{try{cut.toggle(key);}catch(error){checkbox.checked=cut.selected.has(key);$('source-selection-note').textContent=error.message;}};
    button.type='button';button.dataset.sourceKey=key;button.dataset.sourceAction='inspect';button.setAttribute('aria-pressed',String(source===picked));
    button.append(E('b','',host(source.url)),E('code','',B(source).source+' · '+(B(source).family||'external')));
    const dots=E('span','dots');dots.setAttribute('aria-label','Admission '+S(source).frontier.state+'; archive '+S(source).archive.state+'; publication '+S(source).public_program.state);
    for(const state of [S(source).frontier.state,S(source).archive.state,S(source).public_program.state])dots.append(dot(state));button.append(dots);
    button.onclick=()=>cut.inspect(key);wrap.append(checkbox,button);box.append(wrap);
  }
  if(!rows.length&&cut.state==='ready')box.append(E('p','empty','No source contracts match this cut.'));
  if(['failed','changed'].includes(cut.state)){const retry=E('button','ghost','Refresh source cut');retry.type='button';retry.onclick=()=>cut.search(searchTerm);box.append(retry);}
  else if(nextOffset!==null){const more=E('button','ghost',loadingMore?'Loading…':cut.state==='failed-page'?'Retry next page':'Load next 200 sources');more.type='button';more.disabled=loadingMore;more.onclick=()=>cut.page();box.append(more);}
  if(oldFocus){const match=[...box.querySelectorAll('[data-source-key]')].find(node=>node.dataset.sourceKey===oldFocus&&node.dataset.sourceAction===oldAction);match?.focus({preventScroll:true});}
  renderComparison();
}
function renderComparison(){
  const tray=$('source-comparison'),note=$('source-selection-note');if(!tray)return;tray.replaceChildren();
  const entries=[...cut.selected.values()];
  $('export-source-selection').disabled=!entries.length||cut.selectionStale||!['ready','failed-page'].includes(cut.state);
  $('clear-source-selection').disabled=!entries.length;
  note.textContent=cut.selectionStale?'The selected sources belong to an older release. Clear and reselect before using them.':entries.length+' of '+cut.maxSelected+' sources selected for comparison.';
  if(!entries.length){tray.append(E('p','hint','Select sources from one or more searches to compare their actual retained material and publication status.'));return;}
  const table=E('table','source-compare-table'),head=E('tr');
  for(const title of ['Source','Family','Retained items','Publication','Refresh'])head.append(E('th','',title));
  const thead=E('thead');thead.append(head);table.append(thead);const body=E('tbody');
  for(const {row:source}of entries){const tr=E('tr'),b=B(source),s=S(source);for(const value of [host(source.url)+' · '+b.source,b.family||'Unstated',s.archive.items,s.public_program.delivery_state||'not_verified',b.cadence_seconds?b.cadence_seconds+' s':'Unstated'])tr.append(E('td','',String(value)));body.append(tr);}
  table.append(body);tray.append(table);
}
function sourceRead(path,{signal}={}){
  return (async()=>{
    const response=await fetch(path,{credentials:'same-origin',cache:'no-store',redirect:'error',headers:{Accept:'application/json'},signal});
    if(!response.ok)return {ok:false,status:response.status};
    if(!/^application\/json(?:;|$)/i.test(response.headers.get('content-type')||''))throw Error('The source owner returned an incompatible representation.');
    const reader=response.body.getReader(),chunks=[];let size=0;
    try{for(;;){const part=await reader.read();if(part.done)break;size+=part.value.byteLength;if(size>4*1024*1024){await reader.cancel();throw Error('The source page exceeds its 4 MiB limit.');}chunks.push(part.value);}}finally{reader.releaseLock();}
    const bytes=new Uint8Array(size);let at=0;for(const chunk of chunks){bytes.set(chunk,at);at+=chunk.length;}
    return {ok:true,status:response.status,body:JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(bytes))};
  })();
}
const cut=new SourceCut({get:sourceRead,changed:state=>{
  twins=state.rows;total=state.total;estateTotal=state.estateTotal;nextOffset=state.next;catalogueGeneration=state.generation;
  loadingMore=['loading','paging','waiting'].includes(state.state);picked=state.rows.find(row=>state.key(row)===state.active)||null;
  reviewRows();detail();plan();
  const summary=state.summary||{},readiness=summary.readiness||{};
  $('sources').textContent=summary.source_contracts??'—';$('frontier').textContent=summary.frontier_current??'—';$('archive').textContent=summary.archive_current??'—';
  $('paths').textContent=(readiness.spatial_ready_for_extraction||0)+(readiness.media_ready_for_extraction||0)+(readiness.rights_ready_for_extraction||0)+(readiness.public_program_input_bound||0);
}});
function list(){reviewRows();}
function loadMore(){return cut.page();}
function reloadSearch(){return cut.search(searchTerm);}
function cataloguePath(offset){const q=new URLSearchParams();if(searchTerm)q.set('q',searchTerm);if(offset&&catalogueGeneration)q.set('snapshot',catalogueGeneration);return '/foreign/twins'+(offset?'/page/'+offset:'')+(q.size?'?'+q:'');}
function exportSourceSelection(){
  let selected;try{selected=cut.selection();}catch(error){$('source-selection-note').textContent=error.message;return;}
  const escape=value=>'"'+String(value??'').replaceAll('"','""')+'"';
  const lines=[['source','subject','url','catalogue_generation'],...selected.sources.map(source=>[source.source,source.subject,source.url,selected.generation])];
  const url=URL.createObjectURL(new Blob([lines.map(row=>row.map(escape).join('\t')).join('\n')+'\n'],{type:'text/tab-separated-values;charset=utf-8'}));
  const link=E('a');link.href=url;link.download='reviewed-source-selection.tsv';link.click();setTimeout(()=>URL.revokeObjectURL(url),1000);
}
