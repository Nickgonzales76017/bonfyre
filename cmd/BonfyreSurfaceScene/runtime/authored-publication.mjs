// Public reusable design, not publication of private instrument material.
// Durable publication is the existing CMS page aggregate; reuse returns to the
// existing SurfaceScene controllers and destination-owned record transaction.
import {validateAuthoredWorkspace,createAuthoredWorkspace,addInstrument,connectInstruments,instrumentLabels} from './authored-workspace.mjs';
const fail=(v,m)=>{if(!v)throw Error(m);};
const text=(v,n)=>typeof v==='string'&&v.length<=n&&!/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/.test(v);
const exact=(v,keys)=>v&&Object.getPrototypeOf(v)===Object.prototype&&Object.keys(v).length===keys.length&&keys.every(k=>Object.hasOwn(v,k));
export function validatePublicDesign(d){
  fail(exact(d,['schema','title','description','layout','appearance','panels','connections'])&&d.schema==='bonfyre.public-production-design.v1','Unsupported public production design.');
  fail(text(d.title,200)&&d.title.trim()&&text(d.description,4000),'Use a bounded title and purpose.');
  fail(['columns','stack','focus'].includes(d.layout)&&['paper','night'].includes(d.appearance),'Unsupported presentation.');
  fail(Array.isArray(d.panels)&&d.panels.length>0&&d.panels.length<=4&&Array.isArray(d.connections)&&d.connections.length<=4,'Choose one to four real instruments.');
  for(const p of d.panels)fail(exact(p,['kind','label','role'])&&Object.hasOwn(instrumentLabels,p.kind)&&text(p.label,160)&&p.label.trim()&&['source','working','output'].includes(p.role),'Invalid public instrument.');
  const from=new Set();
  for(const e of d.connections){
    fail(exact(e,['from','to','operation'])&&Number.isInteger(e.from)&&Number.isInteger(e.to)&&e.from>=0&&e.to>=0&&e.from<d.panels.length&&e.to<d.panels.length&&e.operation==='research-opportunity'&&d.panels[e.from].kind==='research'&&d.panels[e.to].kind==='product'&&!from.has(e.from),'Unsupported or ambiguous receiving operation.');
    from.add(e.from);
  }
  return d;
}
export function publicProductionDesign(production){
  validateAuthoredWorkspace(production);
  const positions=new Map(production.panels.map((p,i)=>[p.id,i]));
  return validatePublicDesign({schema:'bonfyre.public-production-design.v1',title:production.title,description:production.description,layout:production.layout,appearance:production.appearance,
    panels:production.panels.map(p=>({kind:p.kind,label:p.label,role:p.role})),connections:production.connections.map(e=>({from:positions.get(e.from),to:positions.get(e.to),operation:e.operation}))});
}
export function instantiatePublicDesign(input){
  const d=structuredClone(validatePublicDesign(input));let p=createAuthoredWorkspace(d.title);
  p.description=d.description;p.layout=d.layout;p.appearance=d.appearance;
  for(const panel of d.panels)p=addInstrument(p,panel.kind,{label:panel.label,role:panel.role});
  for(const edge of d.connections)p=connectInstruments(p,p.panels[edge.from].id,p.panels[edge.to].id);
  return p;
}
export function designFromPacket(packet){
  fail(Array.isArray(packet)&&packet.length===7&&packet[0]===1&&Array.isArray(packet[5])&&Array.isArray(packet[6]),'The public design packet is incomplete.');
  return validatePublicDesign({schema:'bonfyre.public-production-design.v1',title:packet[1],description:packet[2],layout:packet[3],appearance:packet[4],panels:packet[5].map(p=>{fail(Array.isArray(p)&&p.length===3,'Invalid instrument packet.');return {kind:p[0],label:p[1],role:p[2]};}),connections:packet[6].map(e=>{fail(Array.isArray(e)&&e.length===3,'Invalid handoff packet.');return {from:e[0],to:e[1],operation:e[2]};})});
}
export function publicationPayload(design,route,snapshot,published=true){
  validatePublicDesign(design);fail(/^\/design-[a-f0-9]{64}$/.test(route),'Invalid publication address.');
  fail(snapshot?.schema==='bonfyre.page-snapshot.v1'&&snapshot.snapshot?.route===route&&/^[a-f0-9]{64}$/.test(snapshot.digest)&&Number.isSafeInteger(snapshot.id)&&snapshot.id>=0,'Reopen the CMS publication baseline.');
  if(snapshot.id)fail(snapshot.snapshot.page?.machine_view==='production-design-v1','This address belongs to a different kind of page.');
  const status=published?'published':'draft';
  return {operation:snapshot.id?'replace':'create',route,expected_id:snapshot.id,expected_digest:snapshot.digest,
    page:{route,title:design.title,heading:design.title,eyebrow:'Reusable production',lede:design.description,machine_view:'production-design-v1',status},
    fragments:[{coordinate:route.slice(1),page:route,slot:'body',audience:'anonymous',kind:'production-design',position:0,heading:design.title,body:design.description,status,projection_contract:{html:true,htmx:true,design:structuredClone(design)}}]};
}
export async function publicationRoute(productionId){
  fail(text(productionId,240)&&productionId,'Missing production identity.');
  const bytes=await crypto.subtle.digest('SHA-256',new TextEncoder().encode(productionId));
  return '/design-'+Array.from(new Uint8Array(bytes),b=>b.toString(16).padStart(2,'0')).join('');
}
// A native snapshot is compared to the exact reviewed operation, never to a
// success-shaped reply. The same comparison handles publication and withdrawal.
const ordered=value=>Array.isArray(value)?value.map(ordered):value&&typeof value==='object'?Object.fromEntries(Object.keys(value).sort().map(k=>[k,ordered(value[k])])):value;
const equal=(a,b)=>JSON.stringify(ordered(a))===JSON.stringify(ordered(b));
export function reconcilePublication(payload,baseline,current){
  const valid=s=>s?.schema==='bonfyre.page-snapshot.v1'&&Number.isSafeInteger(s.id)&&s.id>=0&&/^[a-f0-9]{64}$/.test(s.digest)&&s.snapshot?.route===payload.route&&/^[A-Za-z0-9_-]{1,127}$/.test(s.snapshot?.namespace??'')&&Array.isArray(s.snapshot.fragments);
  fail(valid(baseline)&&valid(current)&&current.snapshot.namespace===baseline.snapshot.namespace,'The publication readback does not belong to this reviewed destination.');
  const metadata=new Set(['id','namespace','created_at','updated_at','published_at']);
  const rowMatches=(wanted,row)=>{
    if(!row||row.namespace!==current.snapshot.namespace)return false;
    for(const [key,value]of Object.entries(wanted)){
      let actual=row[key];if(key==='projection_contract'&&typeof actual==='string'){try{actual=JSON.parse(actual);}catch{return false;}}
      if(!equal(actual,value))return false;
    }
    return Object.keys(row).every(k=>Object.hasOwn(wanted,k)||metadata.has(k)||row[k]===null||row[k]==='');
  };
  const matches=current.id>0&&current.snapshot.page?.id===current.id&&(!payload.expected_id||current.id===payload.expected_id)&&
    rowMatches(payload.page,current.snapshot.page)&&current.snapshot.fragments.length===payload.fragments.length&&
    payload.fragments.every((f,i)=>rowMatches(f,current.snapshot.fragments[i]));
  if(matches)return 'committed';
  if(current.id===baseline.id&&current.digest===baseline.digest)return 'unchanged';
  return 'conflict';
}
// The modal belongs to the existing production editor. Pending writes retain
// their original payload and identity until native readback settles the outcome.
export async function openDesignPublication(production,{el,button,modal,request=fetch}){
  const design=publicProductionDesign(production),route=await publicationRoute(production.id);
  async function read(){const r=await request('/cms/compose/page?route='+encodeURIComponent(route),{credentials:'same-origin',cache:'no-store',redirect:'error',headers:{Accept:'application/json'},signal:AbortSignal.timeout(15000)});if(!r.ok)throw Error(r.status===403?'Publishing requires a current CMS write grant. Your native production is unchanged.':'The publication owner could not be read.');return r.json();}
  let baseline=await read(),pending=false,attempt=null,standing='ready';
  const body=el('section',{},el('p',{class:'eyebrow',text:'Public design review'}),el('h3',{text:design.title}),el('p',{text:design.description}),el('p',{text:'Only this title, purpose, editor arrangement and declared handoffs will be public. No source text, findings, database bytes, people, credentials, grants, saved record addresses or commitments are included.'}));
  const list=el('ol');for(const p of design.panels)list.append(el('li',{text:p.label+' · '+instrumentLabels[p.kind]+' · '+p.role}));body.append(list);
  for(const e of design.connections)body.append(el('p',{text:design.panels[e.from].label+' → '+design.panels[e.to].label+' · reviewed research opportunity'}));
  const notice=el('p',{role:'status','aria-live':'polite'}),actions=el('div',{class:'commands'});body.append(notice,actions);
  const dialog=modal('Publish a reusable production design',body);
  const publish=button('Publish reviewed design',()=>write(true),'primary'),withdraw=button('Withdraw public design',()=>write(false));
  const recover=button('Check saved publication',()=>recoverAttempt()),close=button('Close',()=>{if(!pending)dialog.close();});
  actions.append(publish,withdraw,recover,close);
  function controls(){publish.disabled=pending||!!attempt;withdraw.disabled=pending||!!attempt||!baseline.id||baseline.snapshot.page?.status!=='published';recover.hidden=!attempt;recover.disabled=pending;close.disabled=pending;recover.textContent=standing==='unchanged'?'Retry this reviewed publication':'Check saved publication';body.dataset.publicationState=standing;}
  function complete(current,published,recovered=false){
    baseline=current;attempt=null;standing='committed';const ns=current.snapshot.namespace;
    notice.replaceChildren(el('span',{text:published?'Public design published. Visitors can inspect and reuse its configuration. ':'Public design withdrawn. Saved production work is unchanged. '}));
    if(recovered)notice.append(el('span',{text:'The original save was recovered from native readback; no second write was sent. '}));
    if(published)notice.append(el('a',{href:'/productions/'+ns+'/'+route.slice(1),target:'_blank',rel:'noopener',text:'Open published design ↗'}));
  }
  async function check(){
    const current=await read(),result=reconcilePublication(attempt.payload,attempt.baseline,current);standing=result;
    if(result==='committed')complete(current,attempt.published,true);
    else if(result==='unchanged')notice.textContent='The saved publication still matches the original baseline. Retry will send this same reviewed operation and address.';
    else notice.textContent='The public page changed after review. Nothing was overwritten. Close and reopen publication to review its current baseline.';
  }
  async function submit(){
    standing='uncertain';
    try{
      const r=await request('/cms/compose/page',{method:'POST',credentials:'same-origin',cache:'no-store',redirect:'error',headers:{'Content-Type':'application/json'},body:JSON.stringify(attempt.payload),signal:AbortSignal.timeout(15000)});
      const reply=await r.json();if(!r.ok||!reply.committed)throw Error(reply.error||'The publication was not accepted.');
      const current=await read();if(reconcilePublication(attempt.payload,attempt.baseline,current)!=='committed')throw Error('The publication changed before readback.');
      complete(current,attempt.published);
    }catch(error){
      try{await check();}catch{standing='uncertain';notice.textContent='The save outcome is not yet confirmed. Check saved publication to recover this exact operation before attempting another write.';}
    }
  }
  async function write(published){
    if(pending||attempt)return;const payload=publicationPayload(design,route,baseline,published);
    if(reconcilePublication(payload,baseline,baseline)==='committed'){complete(baseline,published);controls();return;}
    attempt={payload,baseline:structuredClone(baseline),published};pending=true;controls();notice.textContent='Saving the reviewed publication through the CMS…';
    try{await submit();}finally{pending=false;controls();}
  }
  async function recoverAttempt(){
    if(pending||!attempt)return;pending=true;controls();
    try{if(standing==='unchanged')await submit();else await check();}
    catch{standing='uncertain';notice.textContent='The native publication cannot be read yet. The reviewed operation is retained here; another write has not been sent.';}
    finally{pending=false;controls();}
  }
  dialog.addEventListener('cancel',e=>{if(pending)e.preventDefault();});controls();return dialog;
}
