// Source-to-experience routing, compiled from existing native contracts.
export const SOURCE_ROUTING_VERSION = 1;
export const ROUTING_DECISIONS = Object.freeze({
  admitted: 'eligible_for_review',
  refused: 'permission_or_capability_refused',
  unmatched: 'no_authored_semantic_route'
});
import {readFileSync,existsSync} from 'node:fs';
import {resolve,join} from 'node:path';
import {createHash} from 'node:crypto';
const ROOT = resolve(import.meta.dir,'../../..');
const CONTRACT=join(ROOT,'cmd/BonfyreExternalEstate/source-experience-routes.yaff');
const STAGE=join(ROOT,'cmd/BonfyreExperienceStage/experience-maturities.yaff');
const ATLAS=join(ROOT,'architecture/atlas.organs.json');
const NAME=/^[a-z][a-z0-9-]{0,79}$/;
const REF=/^[A-Za-z0-9._:-]{1,180}$/;
const FIELDS=['record','semantic','destination','capability','institution','priority'];
function refuse(message){throw Error('source route refused: '+message);}
function stageEntries(text){
  const known=new Map();let slug=null;
  for(const line of text.split(/\r?\n/)){
    const m=line.match(/^maturity \d+ ([a-z0-9-]+)$/);
    if(m){slug=m[1];known.set(slug,null);continue;}
    const entry=line.match(/^  entry (\S+)$/);
    if(entry&&slug)known.set(slug,entry[1]==='-'?null:entry[1]);
  }
  return known;
}
export function readRouteContracts({
  routeText=readFileSync(CONTRACT,'utf8'),maturityText=readFileSync(STAGE,'utf8')
}={}){
  const known=stageEntries(maturityText),routes=[],used=new Set();let current=null;
  for(const [n,line] of routeText.split(/\r?\n/).entries()){
    if(!line.trim())continue;
    if(line.startsWith('route ')){
      const id=line.slice(6);
      if(!NAME.test(id)||used.has(id))refuse('invalid/duplicate route '+(n+1));
      used.add(id);routes.push(current={id});continue;
    }
    if(line.startsWith('  ')){
      const m=line.match(/^  ([a-z_]+) (\S+)$/);
      if(!current||!m||!FIELDS.includes(m[1])||Object.hasOwn(current,m[1]))refuse('invalid route field '+(n+1));
      current[m[1]]=m[2];continue;
    }
    if(current||!/^(pack|generation|authority|invariant) \S/.test(line))refuse('invalid contract header '+(n+1));
  }
  if(!routes.length||routes.length>64)refuse('route count out of range');
  for(const route of routes){
    if(FIELDS.some(key=>!Object.hasOwn(route,key)))refuse('incomplete route '+route.id);
    if(!['publication_occurrence','household_opportunity'].includes(route.record))refuse('invalid record kind');
    if(!NAME.test(route.semantic)||!NAME.test(route.destination)||!NAME.test(route.institution))refuse('invalid typed route');
    if(!known.has(route.destination))refuse('unknown ExperienceStage destination');
    route.entry=known.get(route.destination);
    if(!/^\d{1,3}$/.test(route.priority)||Number(route.priority)>100)refuse('invalid priority');
    route.priority=Number(route.priority);
    route.capabilities=route.capability.split(',');
    if(!route.capabilities.length||route.capabilities.length>8||
       new Set(route.capabilities).size!==route.capabilities.length)refuse('invalid capability list');
    if(route.capabilities.some(name=>!/^Bonfyre[A-Z][A-Za-z0-9]+$/.test(name)))refuse('invalid capability');
    delete route.capability;Object.freeze(route.capabilities);Object.freeze(route);
  }
  return Object.freeze(routes.sort((a,b)=>b.priority-a.priority||a.id.localeCompare(b.id)));
}
const binaryName=name=>'bonfyre-'+name.slice(7).replace(/([a-z0-9])([A-Z])/g,'$1-$2').toLowerCase();
export function nativeCapabilities({atlas=JSON.parse(readFileSync(ATLAS,'utf8')),commandRoot=join(ROOT,'cmd')}={}){
  return name=>Boolean(atlas[name]&&atlas[name].maturity!=='architectural'&&
    existsSync(join(commandRoot,name,binaryName(name))));
}
export function sourceFingerprint(row){
  const hash=createHash('sha256');
  for(const key of ['kind','namespace','id','semantic','source_ref','receipt_ref','observed_at',
    'rights_basis','visibility','assertion_state','canonical_href','title','summary','status']){
    const value=String(row[key]??'');
    hash.update(key+' '+Buffer.byteLength(value)+'\n'+value+'\n');
  }
  return hash.digest('hex');
}
const publicationSQL="SELECT p.namespace,p.occurrence_id AS id,p.source_ref,p.receipt_ref,p.observed_at,p.rights_basis,p.visibility,p.assertion_state,p.canonical_href,p.title,p.summary,p.status,COALESCE(s.family,'') AS semantic FROM publication_occurrence p LEFT JOIN internet_source s ON s.namespace=p.namespace AND s.source=p.source_ref AND s.status='published' WHERE p.namespace=? AND p.status='published'";
const householdSQL="SELECT namespace,opportunity_id AS id,source_ref,receipt_ref,observed_at,canonical_href,title,description AS summary,service_kinds,status FROM household_opportunity WHERE namespace=? AND status='published'";
export function readImportedRecords(db,{namespace='root',kind=null,ids=null,limit=100}={}){
  if(!REF.test(namespace)||!Number.isInteger(limit)||limit<1||limit>200)refuse('invalid CMS selection');
  if(kind!==null&&!['publication_occurrence','household_opportunity'].includes(kind))refuse('invalid CMS owner');
  if(ids!==null&&(!Array.isArray(ids)||ids.length>200||ids.some(id=>!REF.test(id))))refuse('invalid source ids');
  const rows=[];
  for(const [type,table,sql,idField] of [
    ['publication_occurrence','publication_occurrence',publicationSQL,'p.occurrence_id'],
    ['household_opportunity','household_opportunity',householdSQL,'opportunity_id']]){
    if(kind&&kind!==type)continue;
    const tableExists=db.query("SELECT name FROM sqlite_master WHERE type='table' AND name=?").get(table);
    if(!tableExists)continue;
    if(type==='publication_occurrence'&&!db.query("SELECT name FROM sqlite_master WHERE type='table' AND name='internet_source'").get())refuse('missing source owner');
    const selected=ids===null?db.query(sql+' ORDER BY id LIMIT ?').all(namespace,limit):
      ids.flatMap(id=>db.query(sql+' AND '+idField+'=? LIMIT 1').all(namespace,id));
    for(const row of selected){
      row.kind=type;if(type==='household_opportunity')row.semantic='household-opportunity';rows.push(row);
    }
  }
  return rows.sort((a,b)=>a.kind.localeCompare(b.kind)||a.id.localeCompare(b.id)).slice(0,limit);
}
export function planSourceRoutes(records,{routes=readRouteContracts(),hasCapability=nativeCapabilities(),auth=null,token=null,select=null,reason=null}={}){
  if(!Array.isArray(records)||records.length>200)refuse('source batch exceeds bound');
  if(select!==null&&(!NAME.test(select)||records.length!==1))refuse('override requires one source and route ID');
  if(select!==null&&(typeof reason!=='string'||reason.trim().length<8||reason.length>240||/[\r\n]/.test(reason)))refuse('override reason required');
  const actor=token&&auth?auth.principal(token):null;
  if(actor&&(!actor.actor||!Number.isSafeInteger(actor.userId)))refuse('invalid native actor');
  const items=records.map(record=>{
    const valid=REF.test(record.namespace)&&REF.test(record.id)&&REF.test(record.source_ref)&&
      REF.test(record.receipt_ref)&&typeof record.observed_at==='string'&&record.observed_at.length>=10&&
      record.observed_at.length<=64&&record.status==='published'&&
      (record.kind!=='publication_occurrence'||record.visibility==='public');
    const matches=routes.filter(route=>route.record===record.kind&&route.semantic===record.semantic);
    const candidates=matches.map(route=>{
      const unavailable=route.capabilities.filter(name=>!hasCapability(name));
      let permission=false;
      if(actor&&!unavailable.length)try{
        permission=auth.allowed(token,route.institution,'write')===true&&
          (record.namespace==='root'||auth.allowed(token,record.namespace,'read')===true);
      }catch{/* a native refusal cannot become an allow */}
      const state=!valid?'source_not_admissible':unavailable.length?'capability_unavailable':
        !actor?'native_session_required':!permission?'institution_permission_refused':'eligible_for_review';
      return {route:route.id,destination:route.destination,entry:route.entry,scope:route.institution,
        capabilities:route.capabilities,missing_capabilities:unavailable,priority:route.priority,state,
        reason:'exact authored '+record.kind+' / '+record.semantic+' binding'};
    });
    const chosen=select?candidates.find(c=>c.route===select):
      candidates.find(c=>c.state==='eligible_for_review');
    if(select&&(!chosen||chosen.state!=='eligible_for_review'))refuse('selected destination is not authorized');
    return {source:{kind:record.kind,namespace:record.namespace,id:record.id,semantic:record.semantic,
      source_ref:record.source_ref,receipt_ref:record.receipt_ref,observed_at:record.observed_at,
      fingerprint:sourceFingerprint(record)},candidates,chosen:chosen?.state==='eligible_for_review'?chosen:null,
      state:!valid?'source_not_admissible':!matches.length?'no_authored_semantic_route':
        chosen?.state==='eligible_for_review'?'ready_for_native_review_handoff':'held'};
  });
  return {items,actor,decision:select?'reviewer_override':'automatic',reason:select?reason.trim():null,
    boundary:'Native review only; not accepted evidence, rights, purchase, publication, or effect.'};
}
