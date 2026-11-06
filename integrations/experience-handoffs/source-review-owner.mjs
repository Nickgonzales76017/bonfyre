import {Database} from 'bun:sqlite';
import {join,resolve} from 'node:path';
import {pathToFileURL} from 'node:url';
import {accountAuth} from '../../components/BonfyreServiceSite/account-auth.mjs';
import {readImportedRecords,readRouteContracts,planSourceRoutes,sourceFingerprint,nativeCapabilities} from '../../cmd/BonfyreExternalEstate/tools/source_experience_routing.mjs';
import {nativeHandoffQueue} from '../handoff-fabric/native-owner.mjs';
import {artifacts} from './artifacts.mjs';
import {line,decode,one,sha256,validId,validSha} from './contract.mjs';

const FAMILY='experience-source-review',TYPE='external.source.review';
const mission=id=>'source-review-'+id.slice(0,48);
const refuse=(message,status=409)=>Object.assign(Error(message),{status});

function sourcePacket(s,route,plan,actor){
  return 'handoff aurekai.handoff.v1\n'+
    line('format','bonfyre.source-review.v1')+line('institution',route.institution)+
    line('route',route.id)+line('destination',route.destination)+line('entry',route.entry??'')+
    line('kind',s.kind)+line('namespace',s.namespace)+line('source_id',s.id)+
    line('semantic',s.semantic)+line('source_ref',s.source_ref)+
    line('receipt_ref',s.receipt_ref)+line('observed_at',s.observed_at)+
    line('fingerprint',s.fingerprint)+line('actor_digest',sha256(actor))+
    line('selection',plan.decision)+line('reason',plan.reason??'')+
    route.capabilities.map(c=>line('capability',c)).join('')+
    line('authority','none')+line('disposition','destination_review_required');
}

export function sourceReviewOwner({cmsDb,authDb,auth,home,binary,
    hasCapability=nativeCapabilities()}={}) {
  if(!cmsDb||!home||!binary||(!auth&&!authDb))
    throw refuse('Native CMS, Auth and WorkGraph owners required.',503);
  const access=auth??accountAuth(authDb);
  const files=artifacts(join(resolve(home),'source-review'));
  const queue=nativeHandoffQueue(join(resolve(home),'source-review.db'),{binary});
  function readCMS(fn){
    const db=new Database(resolve(cmsDb),{readonly:true,create:false});
    try{return fn(db);}finally{db.close();}
  }
  function granted(token,institution,namespace,role){
    const principal=access.principal(token);
    if(!validId(institution)||!access.allowed(token,institution,role)||
       (namespace!=='root'&&!access.allowed(token,namespace,'read')))
      throw refuse('Current institutional grants required.',403);
    return principal;
  }
  function sourceNow({kind,namespace,id,fingerprint}){
    return readCMS(db=>{
      const rows=readImportedRecords(db,{kind,namespace,ids:[id],limit:1});
      if(rows.length!==1||sourceFingerprint(rows[0])!==fingerprint)
        throw refuse('Source publication or receipt frontier changed.');
      return rows[0];
    });
  }
  function packet(id,institution){
    if(!validSha(id)||!validId(institution))throw refuse('Invalid handoff identity.',400);
    const bytes=files.get(id,'yaff'),body=decode(bytes.toString('utf8'));
    if(one(body,'format')!=='bonfyre.source-review.v1'||
       one(body,'institution')!==institution||one(body,'authority')!=='none'||
       one(body,'disposition')!=='destination_review_required')
      throw refuse('Handoff identity or authorization boundary changed.',503);
    const source={kind:one(body,'kind'),namespace:one(body,'namespace'),
      id:one(body,'source_id'),fingerprint:one(body,'fingerprint')};
    if(!validSha(source.fingerprint))throw refuse('Source digest invalid.',503);
    const uri=pathToFileURL(join(files.root,'objects',id.slice(0,2),id+'.yaff')).href;
    return {body,bytes,source,uri};
  }
  function work(id,uri){
    const row=queue.inspect(mission(id));
    if(row.node_id!=='job'||row.family!==FAMILY||
       row.operator_id!=='queue.'+TYPE||row.input_uri!==uri)
      throw refuse('Native WorkGraph source binding changed.',503);
    return row;
  }
  function current(token,institution,id,role){
    const p=packet(id,institution),principal=granted(token,institution,p.source.namespace,role);
    sourceNow(p.source);
    return {...p,principal,native:work(id,p.uri)};
  }
  function stage(token,query={}){
    if(!Array.isArray(query.ids)||!query.ids.length||query.ids.length>64)
      throw refuse('Stage requires 1 to 64 exact IDs.',400);
    const actor=access.principal(token),namespace=query.namespace??'root';
    if(!actor?.actor||(namespace!=='root'&&!access.allowed(token,namespace,'read')))
      throw refuse('Current source read grant required.',403);
    const records=readCMS(db=>readImportedRecords(db,{namespace,
      kind:query.kind??null,ids:query.ids,limit:64}));
    if(records.length!==query.ids.length)throw refuse('Source missing or withheld.');
    const routes=readRouteContracts();
    const plan=planSourceRoutes(records,{routes,auth:access,token,hasCapability,
      select:query.select??null,reason:query.reason??null});
    const items=plan.items.map(item=>item.chosen?admit(token,item,plan,routes):{
      source:[item.source.kind,item.source.namespace,item.source.id].join(':'),
      state:item.state,candidates:item.candidates.map(x=>({route:x.route,state:x.state}))});
    return {decision:plan.decision,items,boundary:
      'Review admission only; no proof, payment, publication or effect.'};
  }
  function admit(token,item,plan,routes){
    const route=routes.find(r=>r.id===item.chosen.route),s=item.source;
    const principal=granted(token,route.institution,s.namespace,'write');
    sourceNow({kind:s.kind,namespace:s.namespace,id:s.id,fingerprint:s.fingerprint});
    const content=sourcePacket(s,route,plan,principal.actor);
    const saved=files.put(content,'yaff'),uri=pathToFileURL(saved.path).href;
    queue.enqueue(mission(saved.sha256),FAMILY,TYPE,uri);
    const state=work(saved.sha256,uri);
    return {id:saved.sha256,mission:mission(saved.sha256),
      source:[s.kind,s.namespace,s.id].join(':'),destination:route.destination,
      scope:route.institution,entry:route.entry,native_status:state.status,state:'admitted_for_native_review'};
  }
  function inspect(token,institution,id){
    const p=current(token,institution,id,'read');
    return {status:p.native.status,mission:mission(id),source:p.source,
      destination:one(p.body,'destination'),artifact_uri:p.uri};
  }
  function route(token,institution,id,note){
    if(typeof note!=='string'||note.trim().length<12||note.length>2000)
      throw refuse('A bounded review note is required.',400);
    const before=current(token,institution,id,'write');
    const destination=one(before.body,'destination');
    const routeId=one(before.body,'route');
    const declared=readRouteContracts().find(rule=>rule.id===routeId);
    if(!declared||declared.destination!==destination||declared.institution!==institution||
       (declared.entry??'')!==one(before.body,'entry')||
       declared.record!==before.source.kind||
       declared.semantic!==one(before.body,'semantic')||
       declared.capabilities.some(capability=>!hasCapability(capability))||
       declared.capabilities.join(',')!==(before.body.get('capability')??[]).map(row=>row[0]).join(','))
      throw refuse('Current authored destination or native capability changed.');
    const inbox=files.envelopePath(institution,destination,id);
    const reviewed=before.bytes.toString('utf8')+line('review_action','route_for_review')+line('review_note',note.trim());
    const reviewer=line('review_actor_digest',sha256(before.principal.actor));
    const payload=Buffer.from(reviewed+reviewer,'utf8');
    const output=pathToFileURL(inbox).href;
    if(['complete','completed'].includes(before.native.status)){
      if(before.native.output_uri!==output||!files.read(inbox).equals(payload))
        throw refuse('Native completion differs from the reviewed artifact.');
      return {id,destination,output_uri:output,state:'routed_for_review',replayed:true};
    }
    if(before.native.status==='running'&&before.native.lease_expires_at_ms<=Date.now())
      queue.reap(mission(id));
    const worker='source-review-'+sha256(before.principal.actor).slice(0,32);
    const claim=queue.claim(mission(id),FAMILY,worker);
    const after=current(token,institution,id,'write');
    if(after.principal.actor!==before.principal.actor)throw refuse('Reviewer changed.',403);
    throw refuse('Destination review action not yet available.',503);
  }
  return {stage,inspect};
}
