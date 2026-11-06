// An authored arrangement of existing instruments. Durable material, revisions,
// grants and effects remain with their current CMS workspace and domain owners.
import {compileWorkspace} from './renderers/workspace-composition.mjs';
import {blankDataChecks} from './authored-data-checks.mjs';
export const productionPrefix = 'draft:instruments:composition:';
export const instrumentLabels = Object.freeze({research:'Research editor',product:'Product story map',traces:'Process traces',schema:'SQLite migration',checks:'Data checks'});
const coordinate = s => typeof s === 'string' && /^[A-Za-z0-9:._-]{1,240}$/.test(s);
const integer = n => Number.isSafeInteger(n) && n >= 0 && n < 2147483647;
const recordKey = (kind,key) => coordinate(key) && key.startsWith('draft:instruments:'+kind+':') && key.length > ('draft:instruments:'+kind+':').length;
const requireValue = (value,message) => {if(!value)throw Error(message);};
// Recovery records only source coordinates and hashes. It is private provenance,
// not a grant, an execution receipt, or material for public reusable designs.
export function validateRecoveryOrigin(origin) {
  const keys=['schema','archive_sha256','recovery_key','conversation_id','message_ids','source_sha256'];
  const anchor=s=>typeof s==='string'&&/^[A-Za-z0-9][A-Za-z0-9:._-]{0,239}$/.test(s);
  requireValue(origin&&[Object.prototype,null].includes(Object.getPrototypeOf(origin))&&Object.keys(origin).length===keys.length&&keys.every(k=>Object.hasOwn(origin,k))&&origin.schema==='bonfyre.authored-workspace-recovery-origin.v1','Unsupported recovery provenance.');
  requireValue(/^[a-f0-9]{64}$/.test(origin.archive_sha256)&&/^[a-f0-9]{64}$/.test(origin.source_sha256)&&/^[a-z0-9][a-z0-9-]{0,119}$/.test(origin.recovery_key)&&anchor(origin.conversation_id),'Use exact bounded recovery source coordinates and hashes.');
  requireValue(Array.isArray(origin.message_ids)&&origin.message_ids.length>0&&origin.message_ids.length<=32&&origin.message_ids.every(anchor)&&new Set(origin.message_ids).size===origin.message_ids.length&&origin.message_ids.every((id,i)=>!i||origin.message_ids[i-1]<id),'Retain distinct sorted provider message IDs or stable session-line anchors.');
  return origin;
}
export function createAuthoredWorkspace(title='Untitled production') {
  return {kind:'composition',schema:'bonfyre.authored-workspace.v1',id:crypto.randomUUID(),title,
    description:'',layout:'columns',appearance:'paper',panels:[],connections:[],focus:null};
}
export function validateAuthoredWorkspace(p,{saved=false}={}) {
  requireValue(p?.kind==='composition'&&p.schema==='bonfyre.authored-workspace.v1'&&coordinate(p.id),'Invalid authored workspace.');
  requireValue(typeof p.title==='string'&&p.title.trim()&&p.title.length<=200,'Give the production a title of up to 200 characters.');
  requireValue(typeof p.description==='string'&&p.description.length<=4000,'Keep the production description under 4000 characters.');
  requireValue(['columns','stack','focus'].includes(p.layout)&&['paper','night'].includes(p.appearance),'Choose a supported workspace presentation.');
  if(Object.hasOwn(p,'recoveryOrigin'))validateRecoveryOrigin(p.recoveryOrigin);
  requireValue(Array.isArray(p.panels)&&p.panels.length<=4&&Array.isArray(p.connections)&&p.connections.length<=8,'A workspace can bind up to four instruments and eight declared handoffs.');
  const ids=new Set(),keys=new Set();
  for(const pane of p.panels){
    requireValue(coordinate(pane.id)&&!ids.has(pane.id)&&Object.hasOwn(instrumentLabels,pane.kind),'Every instrument needs a distinct supported binding.');
    requireValue(typeof pane.label==='string'&&pane.label.trim()&&pane.label.length<=160,'Name the working instrument.');
    requireValue(['source','working','output'].includes(pane.role),'Choose the instrument\'s production role.');
    requireValue(recordKey(pane.kind,pane.recordKey)&&!keys.has(pane.recordKey),'Bind distinct records from the selected instrument family.');
    requireValue(integer(pane.revision)&&(!saved||pane.revision>0),'Retain every selected record before saving its production binding.');
    ids.add(pane.id);keys.add(pane.recordKey);
  }
  requireValue(p.focus===null||ids.has(p.focus),'The focused instrument must belong to this production.');
  const edges=new Set();
  for(const edge of p.connections){
    const a=p.panels.find(x=>x.id===edge.from),b=p.panels.find(x=>x.id===edge.to),key=edge.from+'>'+edge.to;
    requireValue(edge.operation==='research-opportunity'&&a?.kind==='research'&&b?.kind==='product'&&!edges.has(key),'This handoff requires a research source and a product map.');
    requireValue(!p.connections.some(e=>e!==edge&&e.from===edge.from),'Choose one receiving map for each research instrument.');
    edges.add(key);
  }
  return p;
}
export function addInstrument(p,kind,{label=instrumentLabels[kind],role='working'}={}) {
  const next=structuredClone(validateAuthoredWorkspace(p));
  requireValue(Object.hasOwn(instrumentLabels,kind),'Choose a supported instrument.');
  const id=crypto.randomUUID();next.panels.push({id,kind,label,role,recordKey:'draft:instruments:'+kind+':'+crypto.randomUUID(),revision:0});
  next.focus=id;return validateAuthoredWorkspace(next);
}
export function removeInstrument(p,id) {
  const next=structuredClone(validateAuthoredWorkspace(p));
  next.panels=next.panels.filter(x=>x.id!==id);next.connections=next.connections.filter(e=>e.from!==id&&e.to!==id);
  if(next.focus===id)next.focus=next.panels[0]?.id??null;
  return validateAuthoredWorkspace(next);
}
export function moveInstrument(p,id,offset) {
  requireValue(offset===1||offset===-1,'Move by one position.');
  const next=structuredClone(validateAuthoredWorkspace(p)),at=next.panels.findIndex(x=>x.id===id),to=at+offset;
  requireValue(at>=0&&to>=0&&to<next.panels.length,'The requested position is outside the outline.');
  [next.panels[at],next.panels[to]]=[next.panels[to],next.panels[at]];return next;
}
export function connectInstruments(p,from,to) {
  const next=structuredClone(validateAuthoredWorkspace(p));next.connections=next.connections.filter(e=>e.from!==from);
  if(to)next.connections.push({from,to,operation:'research-opportunity'});
  return validateAuthoredWorkspace(next);
}
export function reuseWorkspaceDesign(p) {
  validateAuthoredWorkspace(p);const next=createAuthoredWorkspace(p.title+' — new work'),ids=new Map();
  next.description=p.description;next.layout=p.layout;next.appearance=p.appearance;
  for(const panel of p.panels){const id=crypto.randomUUID();ids.set(panel.id,id);next.panels.push({...panel,id,recordKey:'draft:instruments:'+panel.kind+':'+crypto.randomUUID(),revision:0});}
  next.connections=p.connections.map(e=>({...e,from:ids.get(e.from),to:ids.get(e.to)}));next.focus=ids.get(p.focus)??null;
  return validateAuthoredWorkspace(next);
}
// The full reviewed arrangement and each source revision enter one existing
// workspace transaction. Unchanged sources are checked, not rewritten.
export function prepareAuthoredBatch(production,productionRevision,records,changes) {
  const next=structuredClone(validateAuthoredWorkspace(production));
  requireValue(integer(productionRevision)&&integer(productionRevision+1)&&Array.isArray(records)&&Array.isArray(changes),'Use reviewed native revisions.');
  requireValue(records.length===next.panels.length&&changes.length<=next.panels.length+1,'The save must contain exactly the bound instrument records and at most one reviewed handoff.');
  const current=new Map(),byId=new Map();
  for(const record of records){
    const panel=next.panels.find(p=>p.id===record?.id);
    requireValue(panel&&record.kind===panel.kind&&recordKey(record.kind,record.key)&&record.key===panel.recordKey&&integer(record.revision)&&!byId.has(record.id)&&!current.has(record.key),'An instrument returned missing, repeated or different source coordinates.');
    current.set(record.key,record);byId.set(record.id,record);
  }
  const offered=structuredClone(changes),byKey=new Map();let incoming=null;
  for(const change of offered){
    requireValue(change&&coordinate(change.key)&&integer(change.expectedRevision)&&integer(change.expectedRevision+1)&&!byKey.has(change.key),'Invalid or repeated record in the reviewed save.');
    requireValue(!Object.hasOwn(change,'remove')&&!Object.hasOwn(change,'checkOnly'),'A reviewed instrument save cannot delete or supply its own dependency checks.');
    const record=current.get(change.key);
    if(record)requireValue(change.expectedRevision===record.revision&&change.value?.kind===record.kind,'The submitted record differs from the reviewed instrument.');
    else {
      const value=change.value,candidate=value?.candidate;
      requireValue(!incoming&&change.expectedRevision===0&&candidate&&typeof candidate.id==='string'&&/^[A-Za-z0-9._-]{1,180}$/.test(candidate.id)&&change.key==='draft:instruments:incoming:'+candidate.id&&Object.keys(value).length===3&&Object.hasOwn(value,'sourceRecord')&&Object.hasOwn(value,'sourceRevision'),'The production cannot overwrite an unbound record.');
      incoming=change;
    }
    byKey.set(change.key,change);
  }
  if(incoming){
    const value=incoming.value,source=current.get(value.sourceRecord),sourceChange=byKey.get(value.sourceRecord);
    requireValue(source?.kind==='research'&&sourceChange&&value.sourceRevision===source.revision+1&&next.connections.some(edge=>edge.from===source.id&&edge.operation==='research-opportunity'),'A new opportunity must accompany its bound research revision and declared receiving map.');
  }
  for(const panel of next.panels){
    const record=byId.get(panel.id),change=byKey.get(record.key);
    if(change)panel.revision=record.revision+1;
    else {
      requireValue(record.revision>0,'Save all newly added instruments together before deriving an output.');
      panel.revision=record.revision;offered.push({key:record.key,expectedRevision:record.revision,checkOnly:true});
    }
  }
  validateAuthoredWorkspace(next,{saved:true});
  offered.push({key:productionPrefix+next.id,value:next,expectedRevision:productionRevision});
  return {production:next,changes:offered};
}
export function authoredWorkspaceScene(p,adapters,sourceDigest) {
  validateAuthoredWorkspace(p);requireValue(adapters instanceof Map,'Use explicitly mounted native instrument adapters.');
  const active=p.panels.filter(x=>adapters.has(x.id));
  requireValue(active.length>0&&active.length<=2,'Keep one or two live instruments; other records remain parked.');
  const scene={schema:'bonfyre-surface-scene.v1',source_digest:sourceDigest,
    focus:{subject_address:productionPrefix+p.id,actor_scope:'current-native-destination'},
    slots:active.map((x,i)=>({slot_id:x.id,extent:'workspace',accepts:[x.kind],ordinal:i})),
    bindings:active.map(x=>({binding_id:x.id,slot_id:x.id,application_kind:x.kind,continuity_key:x.id,
      action_aperture:adapters.get(x.id).commands.map(c=>c.id).join(',')})),
    interaction:{desktop_posture:{initial_layout:p.layout,layouts:[{id:p.layout,panels:active.map((x,i)=>i?{id:x.id,relative:active[0].id,direction:p.layout==='columns'?'right':'below'}:{id:x.id})}]}}};
  compileWorkspace(scene,adapters);return scene;
}
// Compare the supported instrument material, including SQLite bytes. A receipt
// with the right coordinates but different content cannot make a draft clean.
function sameMaterial(a,b,depth=0,budget={nodes:0}) {
  if(depth>32||++budget.nodes>50000)return false;
  if(Object.is(a,b))return true;
  if(!a||!b||typeof a!=='object'||typeof b!=='object')return false;
  if(a instanceof ArrayBuffer||b instanceof ArrayBuffer)return a instanceof ArrayBuffer&&b instanceof ArrayBuffer&&sameMaterial(new Uint8Array(a),new Uint8Array(b),depth+1,budget);
  if(a instanceof Uint8Array||b instanceof Uint8Array)return a instanceof Uint8Array&&b instanceof Uint8Array&&a.byteLength===b.byteLength&&a.every((value,index)=>value===b[index]);
  if(Array.isArray(a)||Array.isArray(b))return Array.isArray(a)&&Array.isArray(b)&&a.length===b.length&&a.every((value,index)=>sameMaterial(value,b[index],depth+1,budget));
  if(![Object.prototype,null].includes(Object.getPrototypeOf(a))||![Object.prototype,null].includes(Object.getPrototypeOf(b)))return false;
  const keys=Object.keys(a);return keys.length===Object.keys(b).length&&keys.every(key=>Object.hasOwn(b,key)&&sameMaterial(a[key],b[key],depth+1,budget));
}
export function validateAuthoredReceipt(prepared,rows) {
  requireValue(Array.isArray(rows)&&rows.length===prepared.changes.length,'The native save receipt is incomplete. Reopen the production before repeating a write.');
  for(const change of prepared.changes){
    const match=rows.filter(r=>r?.key===change.key),revision=change.expectedRevision+(change.checkOnly?0:1);
    requireValue(match.length===1&&match[0].revision===revision,'A save receipt names a different record revision. Reconcile with the native owner.');
    const row=match[0],panel=prepared.production.panels.find(p=>p.recordKey===change.key);
    requireValue(!row.deleted&&(change.checkOnly?panel&&row.value?.kind===panel.kind:sameMaterial(change.value,row.value)),'The saved content differs from the reviewed production. Keep the draft and reconcile with the native owner.');
  }
  return rows.find(r=>r.key===productionPrefix+prepared.production.id);
}
export function blankInstrument(kind,title=instrumentLabels[kind]) {
  if(kind==='checks')return blankDataChecks(title);
  if(kind==='research')return {kind,title,sources:[],findings:[],selectedSource:null,selectedFinding:null,tab:'sources'};
  if(kind==='product')return {kind,title,stories:[],releases:[],selected:null};
  if(kind==='traces')return {kind,title,text:'case_id,activity,start,end,resource,source_url\n',mapping:{case:'case_id',activity:'activity',start:'start',end:'end',resource:'resource',source:'source_url'},selectedCase:null,selectedEvent:null};
  // The schema instrument creates its explicitly labelled rehearsal database.
  if(kind==='schema')return {kind,title:'SQLite rehearsal — replace with your database',bytes:null,selected:'tasks',positions:{},acceptedSQL:[]};
  throw Error('Unsupported instrument.');
}
