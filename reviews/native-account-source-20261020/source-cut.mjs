// A bounded, transient view of the existing ForeignTwin catalogue. No persistence,
// source admission, discovery verdict, credential or publication authority is owned here.
export class SourceCut {
  constructor({get, changed=()=>{}, maxRows=2000, maxSelected=12, timeoutMillis=20000}) {
    if(typeof get!=='function'||!Number.isSafeInteger(maxRows)||maxRows<200||maxRows>10000)throw Error('Invalid catalogue reader.');
    if(!Number.isSafeInteger(maxSelected)||maxSelected<1||maxSelected>32||!Number.isSafeInteger(timeoutMillis)||timeoutMillis<10||timeoutMillis>60000)throw Error('Invalid catalogue bounds.');
    this.timeoutMillis=timeoutMillis; this.get=get; this.changed=changed; this.maxRows=maxRows; this.maxSelected=maxSelected;
    this.epoch=0; this.flight=null; this.disposed=false; this.rows=[]; this.selected=new Map();
    this.query=''; this.generation=''; this.total=0; this.estateTotal=0; this.next=null;
    this.state='idle'; this.message=''; this.active=null; this.summary={}; this.selectionStale=false;
  }
  key(row) {
    const source=row?.release?.source_contract?.source, subject=row?.subject_id, url=row?.url;
    if(typeof source!=='string'||!source||source.length>512||typeof subject!=='string'||!subject||subject.length>512||typeof url!=='string'||url.length>2048)throw Error('Incomplete source identity.');
    const parsed=new URL(url); if(!['http:','https:'].includes(parsed.protocol)||parsed.username||parsed.password)throw Error('Unsafe source address.');
    return [source,subject,url].map(value=>value.length+':'+value).join('|');
  }
  emit(){if(!this.disposed)this.changed(this);}
  invalidate(){this.epoch++; this.flight?.controller.abort(); this.flight=null;}
  prepare(query) {
    if(this.disposed)return;
    if(typeof query!=='string'||query.length>160||/[\u0000-\u001f\u007f]/.test(query))throw Error('Use at most 160 printable characters.');
    this.invalidate(); this.query=query.trim(); this.rows=[]; this.active=null; this.next=null;
    this.total=0; this.state='waiting'; this.message=''; this.emit();
  }
  search(query=this.query){this.prepare(query);return this.load(0);}
  page(){if(this.disposed||this.flight||this.next===null||!['ready','failed-page'].includes(this.state))return Promise.resolve(false);return this.load(this.next);}
  parse(body,offset) {
    if(body?.state==='snapshot_mismatch')throw Object.assign(Error('The source release changed. Refresh before using this selection.'),{changed:true});
    if(body?.schema!=='bonfyre.foreign.twins.v2'||body.state!=='current'||!Array.isArray(body.twins)||body.twins.length>200||body.offset!==offset||!/^[a-zA-Z0-9._-]{1,96}$/.test(body.catalogue_generation||''))throw Error('The source owner returned an incompatible page.');
    for(const value of [body.total,body.estate_total])if(!Number.isSafeInteger(value)||value<0)throw Error('Invalid source counts.');
    if(body.total>body.estate_total||body.twins.length>body.total)throw Error('Inconsistent source counts.');
    if(body.next_offset!==null&&(!Number.isSafeInteger(body.next_offset)||body.next_offset<=offset||body.next_offset>=body.total))throw Error('Invalid source continuation.');
    const keys=new Set();
    for(const row of body.twins){const key=this.key(row);if(keys.has(key))throw Error('Repeated source identity inside a page.');keys.add(key);if(!row.release.stages)throw Error('Source stages are missing.');}
    return body;
  }
  async load(offset) {
    if(this.disposed)return false;
    if(offset&&this.rows.length>=this.maxRows){this.message='Refine this selection before loading more than '+this.maxRows+' sources.';this.emit();return false;}
    const job={epoch:this.epoch,query:this.query,controller:new AbortController(),timedOut:false};this.flight=job;
    const deadline=setTimeout(()=>{job.timedOut=true;job.controller.abort();},this.timeoutMillis);
    this.state=offset?'paging':'loading';this.message='';this.emit();
    const query=new URLSearchParams();if(job.query)query.set('q',job.query);if(offset)query.set('snapshot',this.generation);
    const path='/foreign/twins'+(offset?'/page/'+offset:'')+(query.size?'?'+query:'');
    try {
      const response=await this.get(path,{signal:job.controller.signal});
      if(this.disposed||job.epoch!==this.epoch)return false;
      if(job.timedOut)throw Error('The source read timed out. Retry this page.');
      if(job.controller.signal.aborted)return false;
      if(!response.ok)throw Error(response.status===401||response.status===403?'This source cut needs account access.':'The source page could not be read. Retry this page.');
      const body=this.parse(response.body,offset);
      if(offset&&body.catalogue_generation!==this.generation)throw Object.assign(Error('The source release changed. Refresh before using this selection.'),{changed:true});
      if(!offset&&this.generation&&body.catalogue_generation!==this.generation&&this.selected.size)this.selectionStale=true;
      const rows=offset?[...this.rows]:[], byKey=new Map(rows.map(row=>[this.key(row),row]));
      for(const row of body.twins){const key=this.key(row);if(byKey.has(key))throw Error('A later page repeated a source from the same release.');const copy=structuredClone(row);freezeSource(copy);byKey.set(key,copy);rows.push(copy);}
      if(rows.length>this.maxRows)throw Error('Refine the source cut before loading more records.');
      this.rows=rows;this.total=body.total;this.estateTotal=body.estate_total;this.next=body.next_offset;
      this.generation=body.catalogue_generation;this.summary=structuredClone(body.release_summary||{});this.state='ready';return true;
    } catch(error) {
      if(this.disposed||job.epoch!==this.epoch||(job.controller.signal.aborted&&!job.timedOut))return false;
      if(job.timedOut)error=Error('The source read timed out. Retry this page.');
      if(error.changed){this.rows=[];this.active=null;this.next=null;this.selectionStale=!!this.selected.size;this.state='changed';}
      else this.state=offset?'failed-page':'failed';
      this.message=error.message;return false;
    } finally {clearTimeout(deadline);if(this.flight===job){this.flight=null;this.emit();}}
  }
  inspect(key){const row=this.rows.find(row=>this.key(row)===key);if(!row)throw Error('Select a source in the current page.');this.active=key;this.emit();return row;}
  toggle(key) {
    if(!['ready','failed-page'].includes(this.state)||this.selectionStale)throw Error('Refresh and review the source release before selecting.');
    if(this.selected.has(key)){this.selected.delete(key);this.emit();return;}
    if(this.selected.size>=this.maxSelected)throw Error('Select at most '+this.maxSelected+' sources for this comparison.');
    const row=this.rows.find(row=>this.key(row)===key);if(!row)throw Error('Source is outside the current cut.');
    this.selected.set(key,{generation:this.generation,row});this.emit();
  }
  clearSelection(){this.selected.clear();this.selectionStale=false;this.emit();}
  selection() {
    if(this.selectionStale||!['ready','failed-page'].includes(this.state)||[...this.selected.values()].some(value=>value.generation!==this.generation))throw Error('This selection must be reviewed against the current source release.');
    return freezeSource({generation:this.generation,sources:[...this.selected.values()].map(value=>({source:value.row.release.source_contract.source,subject:value.row.subject_id,url:value.row.url}))});
  }
  dispose(){this.disposed=true;this.invalidate();this.rows=[];this.selected.clear();this.active=null;}
}
function freezeSource(value){if(value&&typeof value==='object'&&!Object.isFrozen(value)){Object.freeze(value);for(const child of Object.values(value))freezeSource(child);}return value;}
