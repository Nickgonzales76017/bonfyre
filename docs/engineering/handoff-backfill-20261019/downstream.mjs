#!/usr/bin/env bun
// Project the existing source ledgers and retained briefs into the existing
// operator goal owner. This is explicit intake, not a scheduler or a new ledger.
import assert from 'node:assert/strict';
import {readFileSync,lstatSync,existsSync,mkdirSync,writeFileSync} from 'node:fs';
import {resolve,join,relative} from 'node:path';
import {homedir} from 'node:os';
import {pathToFileURL} from 'node:url';
import {createHash} from 'node:crypto';
import {encode} from '../../../integrations/browser/workspace-msgpack.mjs';
import {createOperatorStore} from '../../../integrations/mcp/operator_store.mjs';
export const sha=body=>createHash('sha256').update(body).digest('hex');
export const PREFIX='chat-history:';
const handoff='docs/engineering/handoff-backfill-20261019/';
const cohesion='docs/engineering/aurekai-experience-cohesion-20261015.md';
export function tsv(text){
 const lines=text.replace(/\r\n/g,'\n').split('\n');while(lines.at(-1)==='')lines.pop();
 const headings=lines.shift().split('\t');
 assert(headings.length>1&&new Set(headings).size===headings.length,'Invalid ledger header');
 return lines.map((raw,index)=>{const cells=raw.split('\t');assert.equal(cells.length,headings.length,'Malformed ledger row '+(index+2));return{line:index+2,raw,fields:Object.fromEntries(headings.map((key,n)=>[key,cells[n]]))};});
}
export function sections(text,level=1){
 const lines=text.split('\n'),result=[];let current=null,fence=null;
 for(let i=0;i<lines.length;i++){
  const marker=/^\s*(`{3,}|~{3,})/.exec(lines[i]);if(marker){if(!fence)fence=marker[1][0];else if(marker[1][0]===fence)fence=null;continue;}
  if(fence)continue;
  const match=/^(#{1,6})\s+(.+)$/.exec(lines[i]);if(!match||match[1].length!==level)continue;
  if(current){current.end=i;current.text=lines.slice(current.start-1,i).join('\n');result.push(current);}
  current={heading:match[2],start:i+1};
 }
 if(current){current.end=lines.length;current.text=lines.slice(current.start-1).join('\n');result.push(current);}
 return result;
}
export function heading(text,name){
 const lines=text.split('\n'),at=lines.findIndex(line=>line.replace(/^#{1,6}\s+/,'')===name&&/^#/.test(line));
 assert(at>=0,'Missing source heading: '+name);const level=/^#+/.exec(lines[at])[0].length;
 let end=at+1;for(;end<lines.length;end++){const m=/^(#{1,6})\s/.exec(lines[end]);if(m&&m[1].length<=level)break;}
 return{start:at+1,end,text:lines.slice(at,end).join('\n')};
}
export function programmes(text){
 const lines=text.split('\n'),rows=[];
 for(let i=0;i<lines.length;i++){const m=/^maturity (\d+) ([a-z0-9-]+)$/.exec(lines[i]);if(!m)continue;let end=i+1;while(end<lines.length&&!/^maturity /.test(lines[end]))end++;
 const block=lines.slice(i,end).join('\n'),fields={id:m[1],slug:m[2]};for(const line of lines.slice(i+1,end)){const f=/^  ([a-z_]+) (.*)$/.exec(line);if(f){assert(!Object.hasOwn(fields,f[1]),'Repeated programme field');fields[f[1]]=f[2];}}
 for(const key of ['class','owner','entry','depends','stages','release_gate','witness'])assert(fields[key],'Missing programme '+key);
 rows.push({start:i+1,end,text:block,fields});i=end-1;}
 assert.equal(rows.length,30);assert.equal(new Set(rows.map(r=>r.fields.id)).size,30);
 const byId=new Map(rows.map(r=>[r.fields.id,r])),done=new Set();
 function visit(id,trail=[]){assert(!trail.includes(id),'Programme dependency cycle');assert(byId.has(id),'Unknown programme prerequisite');if(done.has(id))return;for(const dep of byId.get(id).fields.depends==='-'?[]:byId.get(id).fields.depends.split(','))visit(dep,[...trail,id]);done.add(id);}
 rows.forEach(r=>visit(r.fields.id));return rows;
}
function tableRows(source,test){return source.text.split('\n').flatMap((line,i)=>test(line)?[{start:i+1,end:i+1,text:line,cells:line.slice(1,-1).split('|').map(s=>s.trim())}]:[]);}
export function buildDelivery({root=resolve(import.meta.dir,'../../..'),home=homedir()}={}){
 const sources=[],byPath=new Map(),items=[],references=[];
 function source(path,label=path){
  if(byPath.has(path))return byPath.get(path);
  const stat=lstatSync(path);assert(stat.isFile()&&!stat.isSymbolicLink()&&stat.size<=16*1024*1024,'Bounded regular source required');
  const bytes=readFileSync(path),text=new TextDecoder('utf-8',{fatal:true}).decode(bytes);assert(!text.includes('\0'),'NUL in source');
  const row={path,label,sha256:sha(bytes),bytes:bytes.length,text};sources.push(row);byPath.set(path,row);return row;
 }
 const local=path=>source(join(root,path),path),c=local(cohesion);
 const contracts=[
  heading(c.text,'Buildable product-composition contract, October 18'),
  heading(c.text,'Operator-to-consumer backfill across modalities'),
  heading(c.text,'Complete productions inside feeds and addressed details — revised target, October 19'),
  heading(c.text,'Deep source reading: production relationships in the October 19 pair'),
  heading(c.text,'Wider target connections: institutional worlds, editorial products and native closure'),
  {text:local(handoff+'skills-conversion-contract.md').text},
 ].map(r=>r.text).join('\n\n');
 const contractHash=sha(contracts),materialDir=join(home,'.bonfyre/operator/history-material');
 const contractURI=pathToFileURL(join(materialDir,contractHash+'.md')).href;
 const shared='Required before implementation: read the retained complete-production and authoring contract at '+contractURI+'.\n'+
  'Use existing native owners. Keep individual source obligations, unknown inputs, source/selection/inclusion/configuration/result identities, batch and per-item overrides, actual previews, populated receiving workspaces, account continuation, selective corrections and an unrelated second consumer. Do not replace the target with a catalogue, report-only UI, empty editor, fixture, or the currently shipped subset.\n'+
  'Reconcile current source, exact package and live evidence before changing code. Dated observations are not current status. A blank or PARTIAL milestone is not complete. Planned prerequisites below are review requirements; this goal store does not enforce a WorkGraph dependency edge. No paid access, credential change, signup, outreach, purchase, experiment or publication is authorized merely by importing this work.';
 function add(kind,key,title,origin,unit,{priority=75,context='',related=[]}={}){
  assert(unit.text&&title&&key);const dedupe_key=PREFIX+kind+':'+sha(origin.label+'\n'+key).slice(0,32);
  assert(!items.some(r=>r.goal.dedupe_key===dedupe_key),'Duplicate original source identity');
  const details=shared+'\n\nOriginal identity: '+origin.label+' / '+key+'\nRequirement SHA-256: '+sha(unit.text)+'\n\n'+unit.text+
   (context?'\n\nReceiving context:\n'+context:'')+(related.length?'\n\nRelated original obligations (not substitute completion):\n'+related.join('\n'):'');
  assert(Buffer.byteLength(details)<=262144,'Goal exceeds owner bound');
  items.push({kind,key,source:origin.label,start:unit.start,end:unit.end,source_sha256:origin.sha256,unit_sha256:sha(unit.text),
   goal:{title,details,priority,source:'user-history-downstream-20261021',dedupe_key,recurring:false,interval_seconds:0}});
 }
 const portfolio=local('cmd/BonfyreExperienceStage/experience-maturities.yaff');
 const productMap=tableRows(c,l=>/^\| \d{2} \| /.test(l));assert.equal(productMap.length,30);
 for(const row of programmes(portfolio.text)){
  const mapping=productMap.find(r=>r.cells[0]===row.fields.id.padStart(2,'0')&&r.cells[1]===row.fields.slug);assert(mapping);
  add('programme',row.fields.id,'Deliver '+row.fields.id.padStart(2,'0')+' · '+row.fields.slug,portfolio,row,{priority:90,context:mapping.text,
   related:row.fields.depends==='-'?[]:row.fields.depends.split(',').map(id=>'programme:'+id)});
 }
 function ledger(path,{count,kind,key,title,priority=75,context=''}){
  const s=local(path),rows=tsv(s.text);assert.equal(rows.length,count,'Unexpected original membership: '+path);const identities=new Set();
  for(const row of rows){const id=key(row.fields);assert(id&&!identities.has(id),'Ambiguous ledger identity');identities.add(id);
   add(kind,id,title(row.fields),s,{start:row.line,end:row.line,text:s.text.split('\n')[0]+'\n'+row.raw},{priority,context});}
  return rows;
 }
 const skillRows=ledger(handoff+'skills-native-demo-scope.tsv',{count:388,kind:'skill',key:r=>r.source_path,title:r=>'Complete donor workflow · '+r.source_domain+' / '+r.source_name,priority:80,
  context:'Read the exact pinned donor package, companion scripts, examples, exclusions and tests. Retain specimen/alias/package roles; repeated Markdown is not package equivalence. The seven stage fields above remain independent and retain their exact recorded values.'});
 const pathDigest=sha(skillRows.map(r=>r.fields.source_path).sort().join('\n')+'\n');assert.equal(pathDigest,'063a11c3599ae8ba19753fdd47d7f0ece6c4566904c582199255fb261dbb7cb8');
 ledger(handoff+'platforms-100.tsv',{count:100,kind:'platform',key:r=>r.source_number,title:r=>'Connect platform operations · '+r.source_number+' · '+r.source_name,priority:77,
  context:'Preserve the seven operation columns separately: public discovery, account read, embed/link, import/export, author/publish, membership/commerce, distribution/settlement. Supplied provider claims are historical intake, not verified access or current terms. Reuse the existing provider enrollment and recovery work, not duplicate signups.'});
 ledger(handoff+'demo-instruments.tsv',{count:10,kind:'instrument',key:r=>r.id,title:r=>'Finish promised instrument · '+r.title,priority:88,
  context:'This promise may overlap a programme or donor skill. Preserve its distinctive controls and reconcile both implementations before reuse. Check the later live-rollout record; the October 19 state is historical. Do not declare either source complete from the other branch.'});
 local('docs/engineering/live-rollout-20261021.md');
 for(const[name,count]of [['erpnext',74],['hrms',53],['other',2]])ledger('docs/engineering/capability-backfill-20261011-'+name+'.tsv',{count,kind:'controller',key:r=>r.application+'/'+r.module+'/'+r.doctype,
  title:r=>'Complete native controller · '+r.application+' / '+r.doctype,priority:74,
  context:'Continue through the existing Frappe/domain controller owners. Verify the actual precondition, authorized transition, named hooks and resulting ledger/work/stock/payroll effects and negative paths. Metadata, route presence and a related owner do not establish this controller. Preserve submit/cancel/amend distinctions.'});
 local('docs/engineering/capability-backfill-20261011.md');local('docs/engineering/capability-controller-hooks-20261011.tsv');local(handoff+'skills-domain-allocation.tsv');
 const content=heading(c.text,'Low-cost source supply for authored products, October 18');
 const baseFamilies=tableRows(c,l=>/^\| \d{2} [^|]+\|/.test(l)).filter(row=>row.start>=content.start&&row.end<=content.end);assert.equal(baseFamilies.length,25);
 const expanded=tableRows(c,l=>/^\| (News and editorial|Recipes and cooking|Events and going out|Screen culture and reading|Music, podcasts and radio|Games, tabletop and collecting|Creators, live communities and expression|Sports and motorsport|Fashion, making and everyday commerce|Other specialist interests and the original twenty-five families) \|/.test(l));assert.equal(expanded.length,10);
 for(const row of [...baseFamilies,...expanded])add('source-family',row.cells[0],'Deliver source world · '+row.cells[0],c,row,{priority:85,context:content.text});
 const bundleRows=tableRows(c,l=>/^\| P\d\d \|/.test(l));assert.equal(bundleRows.length,12);
 for(const row of bundleRows)add('composition',row.cells[0],'Close receiving bundle · '+row.cells[0]+' · '+row.cells[1],c,row,{priority:97,context:heading(c.text,'Eight demanding product proofs').text});
 const proofRows=c.text.split('\n').flatMap((line,i)=>/^C[1-8]: /.test(line)?[{start:i+1,end:i+1,text:line}]:[]);assert.equal(proofRows.length,8);
 references.push({kind:'cross-product-acceptance',source:c.label,sha256:c.sha256,items:proofRows});
 // Enrollment already has its own identity-specific Queue and checkpoints.
 // Keep every row as source, but do not turn import into a second signup queue.
 for(const[name,count]of [['onboarding-enrollment.tsv',400],['onboarding-recovery-p0.tsv',34]]){const s=local(handoff+name),rows=tsv(s.text);assert.equal(rows.length,count);references.push({kind:'existing-enrollment-owner',source:s.label,sha256:s.sha256,count,rows:rows.map(({line,raw})=>({line,raw}))});}
 const browser=source(join(home,'Downloads/Aurekai Browser + Compute Maturity.md'),'retained:Aurekai Browser + Compute Maturity.md');
 const browserRows=sections(browser.text).filter(s=>/^[IVXLCDM]+\. /.test(s.heading));assert.equal(browserRows.length,50);
 for(const row of browserRows)add('browser-compute',row.heading.split('.')[0],'Bind browser/compute behavior · '+row.heading,browser,row,{priority:73,
  context:'Historical design requirement. Reuse currently implemented browser/numerical owners and their existing lowerers. Reconcile support with the current Atlas and the source contract before implementing missing receiving behavior. Preserve CPU/Wasm/browser-vGPU/Node-vGPU distinctions; no capability or speedup is proven by this brief.'});
 const protocols=source(join(home,'Downloads/AUREKAI_MACHINE_PROTOCOLS_CONTINUATION_V3.md'),'retained:AUREKAI_MACHINE_PROTOCOLS_CONTINUATION_V3.md');
 const protocolRows=sections(protocols.text,2).filter(s=>/^\d+\. /.test(s.heading));assert.equal(protocolRows.length,43);
 for(const row of protocolRows)add('protocol-family',row.heading.split('.')[0],'Bind protocol family · '+row.heading,protocols,row,{priority:72,
  context:'Preserve every protocol row in this original family, including operation/participant/commitment/recovery requirements. A protocol label is not working interoperability; inspect the current exact owners and bind a real independent receiving operation. These are historical proposals, not current support claims or authority to contact services/devices.'});
 const master=source(join(home,'Downloads/Bonfyre_Aurekai_4_Feedback_Lossless_Master_2026-08-29.md'),'retained:Bonfyre_Aurekai_4_Feedback_Lossless_Master_2026-08-29.md');
 const lines=master.text.split('\n'),corpus=lines.findIndex(l=>l==='# VERBATIM SOURCE CORPUS');assert(corpus>=0,'Missing verbatim corpus');
 const starts=lines.flatMap((l,i)=>i>corpus&&/^# Source [A-E] — /.test(l)?[i]:[]);assert.equal(starts.length,5);
 for(let i=0;i<starts.length;i++){const at=starts[i],end=starts[i+1]??lines.length,row={start:at+1,end,text:lines.slice(at,end).join('\n')};
  add('historical-feedback',lines[at].slice(2),'Close retained feedback · '+lines[at].slice(2),master,row,{priority:76,
   context:'This original contains proposals and dated implementation claims. Preserve all nested lists and maps. Reconcile their current owners and receiving gaps; do not recreate a foundation already implemented or turn old reported numbers into current measurements. Five source corpora are retained in this particular copy despite its older filename.'});}
 const missing=heading(c.text,'Deeper historical lists and receiving work');
 add('recovery-gap','56-groups-575-entries','Recover the remaining 56-group item map and unexpanded original tails',c,missing,{priority:89,
  context:'The handoff reports 575 overlapping within-list entries in 56 groups, not 575 distinct features. The original per-entry derived file is not among the sources recovered here. Recover its actual attachment or retained artifact and reconcile every original row before asserting full historical coverage. This goal is an explicit coverage gap, not a replacement list.'});
 const counts={};for(const item of items)counts[item.kind]=(counts[item.kind]??0)+1;
 const result={schema:'bonfyre.history-delivery.v1',sources,contracts,contractHash,contractURI,materialDir,items,references,counts,
  priority_basis:'Execution ordering chosen for this intake, not historical dates, durations or proven dependencies.',
  cannot_infer:'These are source-scoped delivery obligations, with overlaps retained. Imported goals do not count as completed products or an exhaustive search of all chats.'};
 assert(items.length<=2048);assert(Buffer.byteLength(JSON.stringify(items.map(i=>i.goal)))<=16*1024*1024);return result;
}
export function verifySources(plan){for(const source of plan.sources){const stat=lstatSync(source.path);assert(stat.isFile()&&!stat.isSymbolicLink(),'Source path changed');assert.equal(sha(readFileSync(source.path)),source.sha256,'Source changed: '+source.label);}}
function retain(path,body){if(existsSync(path)){assert.equal(sha(readFileSync(path)),sha(body),'Retained material conflict');return;}writeFileSync(path,body,{flag:'wx',mode:0o600});}
export function retainDelivery(plan){
 verifySources(plan);mkdirSync(plan.materialDir,{recursive:true,mode:0o700});assert(!lstatSync(plan.materialDir).isSymbolicLink());
 retain(join(plan.materialDir,plan.contractHash+'.md'),plan.contracts);
 const bytes=encode(plan,{maxBytes:16*1024*1024}),digest=sha(bytes),path=join(plan.materialDir,digest+'.mpk');retain(path,bytes);return{path,digest,bytes:bytes.length};
}
if(import.meta.main){
 const [verb,expected,...extra]=process.argv.slice(2);assert(['plan','apply'].includes(verb)&&!extra.length,'Use downstream.mjs plan OR apply EXACT_PLAN_SHA256');
 const plan=buildDelivery(),material=retainDelivery(plan);if(verb==='apply')assert.equal(expected,material.digest,'Review this exact source-bound plan before application');else assert(!expected,'Unexpected plan argument');
 const home=join(homedir(),'.bonfyre/operator');assert(existsSync(join(home,'operator.sqlite3')),'Use the existing operator owner');
 assert(existsSync(join(home,'STOP')),'This intake requires the existing operator pause; it never changes STOP');
 const store=createOperatorStore({home});let result;
 try{verifySources(plan);result=store.importGoals(plan.items.map(i=>i.goal),{apply:verb==='apply'});}finally{store.close();}
 const receipt={schema:'bonfyre.history-delivery-receipt.v1',source_digest:material.digest,material:material.path,counts:plan.counts,...result};
 if(verb==='apply')retain(join(plan.materialDir,sha(encode(receipt))+'.receipt.mpk'),encode(receipt));
 console.log('SOURCE_PLAN',material.digest,material.path);console.log('OBLIGATIONS',plan.items.length,plan.counts);console.log('OWNER_RESULT',{applied:result.applied,created:result.created,unchanged:result.unchanged,missing:result.missing});console.log('PRESERVED_REFERENCES',plan.references.map(r=>({kind:r.kind,rows:r.count??r.items?.length})));console.log('STOP_UNCHANGED',existsSync(join(home,'STOP')));console.log(plan.cannot_infer);
}
