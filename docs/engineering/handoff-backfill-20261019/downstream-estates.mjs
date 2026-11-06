#!/usr/bin/env bun
// Source-preserving supplemental intake and read-only views of the existing goal
// owner. Neither a new work store nor an executor; historical commands are data.
import assert from 'node:assert/strict';
import {readFileSync,writeFileSync,existsSync,lstatSync,mkdirSync,readdirSync} from 'node:fs';
import {join,resolve,basename} from 'node:path';
import {homedir} from 'node:os';
import {Database} from 'bun:sqlite';
import {encode,decode} from '../../../integrations/browser/workspace-msgpack.mjs';
import {createOperatorStore} from '../../../integrations/mcp/operator_store.mjs';
import {sha,tsv,heading} from './downstream.mjs';
export const FIRST='01f4c06b4a58dee2f6ff213d7be34bac10b67e9cb03bf419f38f11cad1e2d8cb';
export const RECOVERED='706aa1fd49f227b5a22b908e20cc180ad9c7958d4d8103c3850f8816d9484cd6';
export const BOUND={maxBytes:16*1024*1024};
const briefs=[
 ['0b37564b01e8af1c04d141f3258e896600399a92e6a5974da0b27b68817b01e8','Institutional worlds and operating modes'],
 ['2faffdbf8b9fe09a0782c4cc930e482acee519b84e1813c42b0b217811b23b75','Use partner mechanisms, not more partner catalogs'],
 ['716702d627f877722b59bd3a88c5d50c9109c54bbdb1a4d67f3b7ae5e24c9356','Shared-world addressing and public operation'],
 ['7546562b100b830d58071f6ec5f631ef6f3b6f9840675e29e84457ccb942c801','Institutional interactions and need-driven discovery'],
 ['d943d2b0332c7f11fbe9a3c7a3c16ae0c19a7717d637b0be7d2fde83acca9793','Compiled closures and bounded institutional publication'],
 ['12c0db6570f1b66f3f1cc019e1d1b5f15f77d241c9ae446a107b0e87e421c3de','Numerical, relational and physical composition'],
 ['4887da57e6ab4deffcee3366143fc4b6a94e402e88556fea41de15843e090f51','Dormant capacity and useful activation'],
 ['ee59f8fbe6c7935f3a26bc1b60b20044cab4158cf1c051c58e408096171bfdee','Physical carriers, QR and mobile continuation'],
 ['2b08979b1a4e0ebb51d7792e20e075f8850355fa9ae2cb38b0b42d55b43f86b3','CMS authoring, native operations and complete institutions'],
];
const contracts=[
 'architecture/institutional/namespaces.yaff','architecture/institutional/delegation-model.yaff',
 'architecture/institutional/relationship-types.yaff','docs/experiences/profile-sites.md',
 'docs/engineering/public-value-business-lifecycle-20261016.md','docs/commercial/white-label-operating-products-20261011.md',
];
// Each routing note is an implementation allocation, not a claim that a named
// actor holds authority, a receiving application is ready, or an edge executes.
const lanes={
 institutions:{label:'Institution setup and continuing operations',bundles:['P01','P04','P07','P09','P11','P12'],owners:['Auth','Authority','ProductContract','PackGraph','CMS','WorkGraph'],finish:'Public utility or blank organization start -> configured offering -> bounded invitations -> customer/participant action -> operated work -> permitted result and day-two controls.'},
 partners:{label:'Partner mechanism into a real receiving operation',bundles:['P02','P06','P10','P11','P12'],owners:['PartnerCommons','CapabilityProgram','ProviderGraph','Bridge','ProtocolMachine'],finish:'Identify the declared mechanism and current implementation; bind one needed operation to a real consumer, use its output, inspect failure/recovery and verify a second compatible receiver. A catalog entry is not completion.'},
 upper90:{label:'Upper90, JRSS and AI Loss operating continuity',bundles:['P01','P05','P07','P09','P11','P12'],owners:['ProductContracts','Authority','WorkGraph','Finance','existing client controllers'],finish:'Keep each client grammar and report meaning, scheduled business behavior, source lineage, current operator/customer roles and explicit delegation. Reconcile existing work before changes; never restart old migrations or modify client records from this intake.'},
 services:{label:'Independent branded operating products',bundles:['P03','P04','P06','P07','P09','P12'],owners:['SitePresentation','CMS','CommerceChain','Commitment','Finance','Pay','WorkGraph'],finish:'Configure the named business from empty customer state; preserve platform/operator/merchant/legal-seller distinctions; exercise the same offering, customer work and fulfilled result across permitted views.'},
 reuse:{label:'Reusable native, browser and host-native delivery',bundles:['P02','P03','P05','P08','P10','P11','P12'],owners:['SurfaceScene','native domain owners','Artifact','PackGraph'],finish:'Recover the exact source candidate, current implementation and complete dependency package; deliver an independently usable operation and its intended carrier, replace eligible callers, and prove actual downstream use.'},
 history:{label:'Historical requirement into current receiving workflows',bundles:['P01','P04','P05','P06','P07','P08','P09','P10','P11','P12'],owners:['current Atlas owners named by the original requirement'],finish:'Preserve every nested requirement. Reconcile later implementations and retired mechanisms; close the missing receiving behavior rather than recreate infrastructure or shorten the target to an exported report.'},
 continuity:{label:'Cross-workspace and source-to-production continuity',bundles:['P01','P05','P07','P11','P12'],owners:['SurfaceScene','CMS','WorkGraph','RetentionGC','native domain owners'],finish:'Preserve selected source parts, variants, edits and actual material through installed workspaces, shared/personal scope, save/reopen, downstream operation and selective correction.'},
};
function regular(path,max=16*1024*1024){const s=lstatSync(path);assert(s.isFile()&&!s.isSymbolicLink()&&s.size<=max,'Bounded regular file required: '+path);return readFileSync(path);}
function linesOf(text){return text.match(/[^\n]*\n|[^\n]+$/g)||[];}
function cut(text,starts){const lines=linesOf(text),points=[0,...starts.filter(i=>i>0),lines.length];return [...new Set(points)].slice(0,-1).map((at,i,a)=>{const end=[...new Set(points)][i+1];return {start:at+1,end,text:lines.slice(at,end).join('')};});}
export function chapters(text,level=1){
 const lines=linesOf(text),starts=[];let fence=null;
 for(let i=0;i<lines.length;i++){const marker=/^\s*(`{3,}|~{3,})/.exec(lines[i]);if(marker){if(!fence)fence=marker[1][0];else if(marker[1][0]===fence)fence=null;continue;}if(fence)continue;const m=/^(#{1,6})\s+(.+)/.exec(lines[i]);if(m&&m[1].length===level)starts.push(i);}
 const units=cut(text,starts);assert.equal(units.map(u=>u.text).join(''),text,'Section partition lost source text');
 return units.map((u,i)=>({...u,key:/^#{1,6}\s+([^\r\n]+)/.exec(u.text)?.[1]||'source-framing',title:/^#{1,6}\s+([^\r\n]+)/.exec(u.text)?.[1]||'Source framing',ordinal:i}));
}
export function declarations(text,tags){
 const lines=linesOf(text),starts=[];for(let i=0;i<lines.length;i++){const m=/^([a-z_][a-z_-]*) ([^\s]+)(.*)/.exec(lines[i]);if(m&&tags.includes(m[1]))starts.push(i);}
 if(!starts.length)return {preamble:text,units:[]};const preamble=lines.slice(0,starts[0]).join('');
 const units=starts.map((at,i)=>{const end=starts[i+1]??lines.length,m=/^([a-z_-]+) ([^\s]+)(.*)/.exec(lines[at]);return{start:at+1,end,key:m[1]+' '+m[2],title:m[1]+' '+m[2],text:lines.slice(at,end).join('')};});
 assert.equal(preamble+units.map(u=>u.text).join(''),text);assert.equal(new Set(units.map(u=>u.key)).size,units.length,'Repeated declaration key needs explicit reconciliation');return{preamble,units};
}
export function verify(plan){for(const s of plan.sources)assert.equal(sha(regular(s.path)),s.sha256,'Changed source: '+s.label);}
export function buildEstateDelivery({root=resolve(import.meta.dir,'../../..'),home=homedir()}={}){
 const materialDir=join(home,'.bonfyre/operator/history-material'),sources=[],items=[],references=[],aliases=[];const byPath=new Map(),byKey=new Map(),bodies=new Map();
 const source=(path,label=path)=>{if(byPath.has(path))return byPath.get(path);const bytes=regular(path),text=new TextDecoder('utf-8',{fatal:true}).decode(bytes);assert(!text.includes('\0'));const s={path,label,sha256:sha(bytes),text,bytes:bytes.length};sources.push(s);byPath.set(path,s);return s;};
 const local=p=>source(join(root,p),p);
 const readPacket=(name,suffix='.mpk')=>{const path=join(materialDir,name+suffix),bytes=regular(path);assert.equal(sha(bytes),name,'Wrong retained packet');return decode(bytes,BOUND);};
 const first=readPacket(FIRST),recovered=readPacket(RECOVERED,'.recovered.mpk');assert.equal(first.items.length,803);assert.equal(recovered.items.length,25);
 const originalKeys=new Set(first.items.map(i=>i.goal.dedupe_key));
 const contexts=contracts.map(local),shared=first.contracts+'\n\n'+contexts.map(s=>'SOURCE '+s.label+'\n'+s.text).join('\n\n');
 const contractHash=sha(shared),contractPath=join(materialDir,contractHash+'.md');
 const common='Current user-directed task: deliver the previously planned behavior through the existing Aurekai/Bonfyre owners. Historical statements, commands, limits, endpoints, status, rights and dates below are retained input, NOT present execution instructions or verified current state. Read the shared source contract at '+contractPath+'.\n'+
 'Do not resume paused execution, launch retired infrastructure/models, change credentials, create memberships, contact counterparties, apply for funding, spend, publish or operate physical equipment just because an old brief describes it. Retrieve current state and effect authority at the owning boundary. This intake does not grant them.\n'+
 'A requirement is not finished at a catalog, fetched response, explanatory screen, generic form, empty editor or export. Retain exact source/selection/inclusion/production/result identities, batch and granular controls, actual previews, prepared receiving workspaces, account/role continuity, failure recovery and selective correction. Preserve live mature behavior and replace duplicated callers only after parity.';
 function add(kind,key,title,s,unit,lane,{context='',related=[],classification='source_requirement'}={}){
  assert(lanes[lane]&&unit.text.trim()&&key);const identity=kind+'\n'+s.label+'\n'+key,dedupe_key='chat-history-estates:'+sha(identity).slice(0,40);assert(!originalKeys.has(dedupe_key)&&!byKey.has(dedupe_key),'Repeated source identity');
  const contentHash=sha(unit.text),exact=bodies.get(kind+'\n'+contentHash);if(exact){aliases.push({source:s.label,start:unit.start,end:unit.end,key,target:exact,reason:'Exact same selected source text; both provenances retained.'});return;}
  const route=lanes[lane],bundleRefs=route.bundles.map(p=>first.items.find(i=>i.kind==='composition'&&i.key===p)?.goal.dedupe_key);assert(bundleRefs.every(Boolean));
  const details=common+'\n\nSource identity: '+s.label+' / '+key+'\nSource SHA-256: '+s.sha256+'\nSelected requirement SHA-256: '+contentHash+'\nSource kind: '+classification+'\n\n'+unit.text+
   '\n\nReceiving allocation (implementation analysis, not granted relationships or proven readiness):\n'+route.finish+'\nExisting owner navigation: '+route.owners.join('; ')+
   '\nExisting composition goals: '+bundleRefs.join(', ')+(related.length?'\nRelated source obligations: '+related.join('; '):'')+(context?'\n\nOriginal surrounding context:\n'+context:'');
  assert(Buffer.byteLength(details)<=262144,'Goal detail exceeds owner bound');
  const item={kind,key,lane,source:s.label,source_sha256:s.sha256,start:unit.start??0,end:unit.end??0,unit_sha256:contentHash,classification,related,composition_goals:bundleRefs,
   goal:{title:title.slice(0,380),details,priority:lane==='institutions'?94:lane==='continuity'?93:lane==='upper90'?86:lane==='services'?88:lane==='partners'?84:lane==='reuse'?78:81,source:'user-estate-downstream-20261021',dedupe_key,recurring:false,interval_seconds:0}};
  items.push(item);byKey.set(dedupe_key,item);bodies.set(kind+'\n'+contentHash,dedupe_key);
 }
 // Recover all nine complete authored briefs, including their preambles. An
 // unformatted earlier correction stays separate, not silently rewritten.
 for(const[id,title]of briefs){const retained=recovered.items.find(s=>s.sha256===id);assert(retained);const s=source(retained.path,'native-user-message:'+id);assert.equal(s.sha256,id);references.push({kind:'original-user-message',source:s.label,references:retained.references});
  for(const unit of chapters(s.text))add('historical-workflow',unit.key,title+' · '+unit.title,s,unit,'history',{context:'Original user-message references are retained with this source. Old claims of implementation must be reconciled against the current Atlas, exact source and release. This chapter may overlap another brief without being an independent product.'});
 }
 // Keep cumulative operating handoffs as references, not sixteen duplicate
 // executable instructions to restart retired infrastructure.
 const cumulative=recovered.items.filter(i=>!briefs.some(([id])=>id===i.sha256));
 for(const row of cumulative){const s=source(row.path,'native-user-message:'+row.sha256);assert.equal(s.sha256,row.sha256);references.push({kind:'historical-operating-handoff-reference',source:s.label,references:row.references});}
 const setup=byPath.get(join(materialDir,'20e1c90eda22f5ee091c64156a5311f778014b00dc6231b833af27ea40f6405d.source.md'));
 const fleet=setup.text.slice(setup.text.indexOf('Part F — Fill existing setup boards'),setup.text.indexOf('Part G — MCP execution matrix'));assert(fleet&&fleet.includes('Public Good / Civic:'));
 const fleetNames=['RewardsOps','Website Landlord / AI SEO','GovCon','JRSS','Cultural Commerce / SPUNE','SHOUTOUT','Public Good / Civic','Household / HOA / Business / Network','Oracle Setup / Deployment','Connector/API/MCP Infrastructure','Model Runtime Infrastructure'];
 for(let i=0;i<fleetNames.length;i++){const at=setup.text.indexOf(fleetNames[i]+':',setup.text.indexOf('Part F —')),end=i+1<fleetNames.length?setup.text.indexOf(fleetNames[i+1]+':',at):setup.text.indexOf('Part G —',at);assert(at>=0&&end>at);
  add('setup-fleet',fleetNames[i],'Continue original setup · '+fleetNames[i],setup,{start:setup.text.slice(0,at).split('\n').length,end:setup.text.slice(0,end).split('\n').length,text:setup.text.slice(at,end)},'institutions',{classification:'historical_setup_requirement',context:'Preserve original numerical targets as historical targets, not current counts or authorization to scrape. Reconcile the presently active deployment/resource/provider owner; do not revive Oracle or duplicate boards. The complete handoff remains retained.'});}
 // Historical mechanism records are distinct from maintained relationships.
 const partners=local('.bonfyre/run8-r2c/20260823T032652Z-27525/control/partner-commons-reconstruction.json'),catalog=JSON.parse(partners.text);assert.equal(catalog.count,112);assert.equal(catalog.records.length,112);assert.equal(new Set(catalog.records.map(r=>r.identity)).size,112);
 const canonical='.bonfyre/run8/canonical/estate/Bonfyre_Aurekai_CANONICAL_INTEGRATED_V8_1_FULL_LINEAGE_KERNEL_CONVERGENCE_2026-08-20/';
 const profileDir=canonical+'15_OPEN_WORLD/PARTNER_COMMONS/PROFILES/',profileRule=local(profileDir+'00_ROLE_OF_THESE_SEED_PROFILES.md');
 const profileFiles=readdirSync(join(root,profileDir)).filter(n=>n.endsWith('.yaml')).sort();assert.equal(profileFiles.length,102);
 for(const row of catalog.records){let support=profileRule.text;const n=basename(row.preserved_candidate||'');if(n.endsWith('.yaml')&&profileFiles.includes(n)){const p=local(profileDir+n);support+='\n\n'+p.label+'\n'+p.text;}
  add('partner-mechanism',row.identity,'Use declared partner mechanism · '+row.identity,partners,{start:0,end:0,text:JSON.stringify(row,null,2)},'partners',{classification:'historical_mechanism_record',context:support+'\nHistorical reconstruction observed_at: '+catalog.observed_at+'. Preserve cataloged-not-invoked, ownership boundaries and missing adapters. An archived Python path is a discovery lead, not a direction to restore it.'});}
 references.push({kind:'partner-populations',reconstruction:112,seed_profile_files:102,reason:'Different historical populations; neither count proves an installed integration or relationship.'});
 // Domain declarations keep their complete adjacent notes, defects and refusal
 // requirements; report semantics never collapse merely because mechanics do.
 const packSpecs=[
 ['work.yaff',['program','work']],['capabilities.yaff',['capability']],['providers.yaff',['provider']],
 ['bindings.yaff',['binding_set']],['products.yaff',['product','report_contract']],
 ['role-products.yaff',['product_basis','workspace','workspace_role']],['delegation.yaff',['envelope']],
 ['product-contract-jrss.yaff',['product_contract','downstream_contract']],['product-contract-ai-loss.yaff',['product_contract','downstream_contract','change_process']],
 ['schedule-bindings.yaff',['schedule_mechanism','schedule_binding']],['reporting-basis.yaff',['basis','binding_note','composite_note','builder_note','anti_target']],
 ];
 for(const[name,tags]of packSpecs){const s=local('packs/upper90/'+name),parsed=declarations(s.text,tags);for(const unit of parsed.units)add('upper90-receiver',unit.key,'Continue institutional product · '+unit.title,s,unit,'upper90',{classification:'dated_domain_contract',context:parsed.preamble});}
 for(const path of ['packs/upper90/relationships.yaff','packs/upper90/actors.yaff','packs/upper90/circuit-quality.yaff','packs/upper90/demo-leads.yaff']){const s=local(path);references.push({kind:'institution-boundary-and-relationship-context',source:s.label});}
 const quality=byPath.get(join(root,'packs/upper90/circuit-quality.yaff'));add('operating-circuit','jrss-quality','Close the existing JRSS quality-to-delivery circuit',quality,{start:1,end:linesOf(quality.text).length,text:quality.text},'upper90');
 const fleming=local('packs/fleming-equity-group/work.yaff');add('institution-continuity','fleming','Continue Fleming operating work without reopening completed work',fleming,{start:1,end:linesOf(fleming.text).length,text:fleming.text},'institutions',{context:'The source includes work_closed, work_open and PARTIAL/SCOPED observations. Reconcile the existing native WorkGraph and current proof before deciding any residual. Do not requeue every historical work line, contact owners, submit forms or make property offers.'});
 for(const[path,tags]of [['packs/technical-institutions/watches.yaff',['watch']],['packs/institutional-opportunities/opportunities.yaff',['opportunity']],['packs/utd-vdc/work.yaff',['work']]]){const s=local(path),parsed=declarations(s.text,tags);for(const unit of parsed.units)add('institution-relationship',unit.key,'Continue institutional relationship work · '+unit.title,s,unit,'institutions',{classification:'dated_relationship_work',context:parsed.preamble+'\nInspect current native work and actual correspondence/issue state before further action. This is not a recurring watch, email, application, or external submission.'});}
 for(const p of ['packs/technical-institutions/actors.yaff','packs/collaborations/namespace.yaff','packs/utd-vdc/actors.yaff','docs/commercial/partner-erp-20261010.requirements.tsv','docs/commercial/partner-erp-20261010.opportunities.tsv']){const s=local(p);references.push({kind:'relationship-source-not-new-authority',source:s.label});}
 const white=byPath.get(join(root,'docs/commercial/white-label-operating-products-20261011.md')),whiteRows=white.text.split('\n').flatMap((text,i)=>/^\| (Revenue Cloud|Service Network Cloud|Creator & Rights Cloud|Commons & Funding Cloud|Property & Claims Cloud|Territory & Hospitality Cloud|Trade & Supply Cloud|Contractor Cloud|Civic Operations Cloud) \|/.test(text)?[{start:i+1,end:i+1,text,key:text.split('|')[1].trim()}]:[]);assert.equal(whiteRows.length,9);
 for(const unit of whiteRows)add('white-label-product',unit.key,'Complete branded operation · '+unit.key,white,unit,'services',{context:heading(white.text,'The three flagship demonstrations').text});
 const life=byPath.get(join(root,'docs/engineering/public-value-business-lifecycle-20261016.md'));
 for(const name of ['Public and authenticated experience','Business configuration, not single-business cloning','First-time organization path','SHOUTOUT acceptance activity','Resources and ongoing operations','Interaction requirements retained from the October 16 uploads','October 21: social, business and grant-derived production flows','October 21: references are chosen, not memorized']){const unit=heading(life.text,name);add('institution-setup-flow',name,'Deliver complete setup/operation · '+name,life,unit,'institutions',{context:heading(life.text,'Release acceptance requirements').text});}
 const tests=life.text.split('\n').filter(l=>/^\| G\d\d \|/.test(l));assert(tests.length>=15);references.push({kind:'normal-apex-to-day-two-acceptance',source:life.label,rows:tests});
 const remain=local('docs/engineering/remaining-workspace-adaptations-20261015.md');for(const unit of chapters(remain.text,2))if(unit.key!=='source-framing')add('workspace-continuation',unit.key,'Complete connected workspace · '+unit.title,remain,unit,'continuity');
 const candidates=source(join(home,'Downloads/Bonfyre_Product_Candidate_Matrix_2026-10-10_Refined.tsv'),'retained:Bonfyre_Product_Candidate_Matrix_2026-10-10_Refined.tsv');
 const market=source(join(home,'Downloads/Bonfyre_Estate_Wide_Library_and_Market_Study_2026-10-10_Refined.md'),'retained:Bonfyre_Estate_Wide_Library_and_Market_Study_2026-10-10_Refined.md');
 const candidateRows=tsv(candidates.text);assert.equal(candidateRows.length,163);assert.equal(new Set(candidateRows.map(r=>r.fields.Candidate)).size,163);
 for(const row of candidateRows)add('reuse-candidate',row.fields.Candidate,'Deliver reusable candidate · '+row.fields.Candidate,candidates,{start:row.line,end:row.line,text:candidates.text.split('\n')[0]+'\n'+row.raw},'reuse',{classification:'historical_market_candidate',context:'Read the retained complete study '+market.label+' (SHA-256 '+market.sha256+'). H/X/N denote the source evidence classification, not a release or readiness status. Reuse existing implementations before extracting a new package; preserve the named target buyer, carrier and original relative priority.'});
 const counts={};for(const i of items)counts[i.kind]=(counts[i.kind]??0)+1;
 assert(items.length>0&&items.length<=2048,'Unexpected supplemental scope');assert(Buffer.byteLength(JSON.stringify(items.map(i=>i.goal)))<=16*1024*1024);
 return {schema:'bonfyre.estate-history-delivery.v1',root,home,materialDir,previous:FIRST,recovered:RECOVERED,contractHash,contractPath,shared,sources,items,references,aliases,lanes,counts,
  boundary:'Source-linked obligations and receiving allocations, not executable dependency edges, complete products, current partner status, account-rail visibility or exhaustive recovery of all sessions.'};
}
function retain(path,bytes){if(existsSync(path)){assert.equal(sha(regular(path)),sha(bytes),'Retained material conflict');return;}writeFileSync(path,bytes,{flag:'wx',mode:0o600});}
export function retainPlan(plan){verify(plan);mkdirSync(plan.materialDir,{recursive:true,mode:0o700});assert(!lstatSync(plan.materialDir).isSymbolicLink());retain(plan.contractPath,plan.shared);const bytes=encode(plan,BOUND),digest=sha(bytes),path=join(plan.materialDir,digest+'.estates.mpk');retain(path,bytes);return{digest,path,bytes:bytes.length};}
export function readPlan(home,digest){assert(/^[a-f0-9]{64}$/.test(digest),'Exact source-plan digest required');const path=join(home,'.bonfyre/operator/history-material',digest+'.estates.mpk'),bytes=regular(path);assert.equal(sha(bytes),digest);const plan=decode(bytes,BOUND);assert.equal(plan.schema,'bonfyre.estate-history-delivery.v1');return plan;}
export function ownerView(plan,{database=join(plan.home,'.bonfyre/operator/operator.sqlite3'),query='',limit=50}={}){
 assert(typeof query==='string'&&query.length<=300&&Number.isInteger(limit)&&limit>0&&limit<=200);
 const db=new Database(database,{readonly:true}),rows=[];try{db.exec('BEGIN');const get=db.query('SELECT id,state,attempts,last_error,updated_at FROM goals WHERE dedupe_key=?');for(const item of plan.items){const native=get.get(item.goal.dedupe_key);rows.push({kind:item.kind,key:item.key,lane:item.lane,title:item.goal.title,source:item.source,source_lines:[item.start,item.end],definition_matches:native?(()=>{const saved=db.query('SELECT details,title FROM goals WHERE id=?').get(native.id);return saved.details===item.goal.details&&saved.title===item.goal.title;})():null,...(native??{id:null,state:'not_imported',attempts:0}),dedupe_key:item.goal.dedupe_key});}}finally{db.close();}
 const states={},byLane={};for(const row of rows){states[row.state]=(states[row.state]??0)+1;byLane[row.lane]??={};byLane[row.lane][row.state]=(byLane[row.lane][row.state]??0)+1;}
 const terms=query.toLowerCase().trim().split(/\s+/).filter(Boolean),texts=new Map(plan.items.map(item=>[item.goal.dedupe_key,item.goal.details.toLowerCase()]));
 const matches=rows.filter(row=>terms.every(term=>term.startsWith('kind:')?row.kind===term.slice(5):term.startsWith('lane:')?row.lane===term.slice(5):
  [row.title,row.key,row.kind,row.lane,row.source,row.id||'',texts.get(row.dedupe_key)].join(' ').toLowerCase().includes(term)));
 return{total:rows.length,states,byLane,matched:matches.length,rows:matches.slice(0,limit),more:matches.length>limit,boundary:plan.boundary};
}
if(import.meta.main){
 const[verb,digest,query,...extra]=process.argv.slice(2);assert(!extra.length&&['plan','apply','status','show'].includes(verb),'Use plan | apply DIGEST | status DIGEST [QUERY] | show DIGEST GOAL_ID');const home=homedir(),owner=join(home,'.bonfyre/operator');
 if(verb==='status'||verb==='show'){const plan=readPlan(home,digest);if(verb==='status')console.log(ownerView(plan,{query:query||''}));else{assert(query,'Goal ID required');const view=ownerView(plan,{query,limit:2});assert.equal(view.rows.length,1,'Select one exact goal ID');assert.equal(view.rows[0].id,query);const item=plan.items.find(i=>i.goal.dedupe_key===view.rows[0].dedupe_key);console.log(view.rows[0]);console.log(item.goal.details);} }
 else{assert(!query);const plan=buildEstateDelivery(),saved=retainPlan(plan);if(verb==='apply')assert.equal(digest,saved.digest,'Review the exact source plan');else assert(!digest);assert(existsSync(join(owner,'operator.sqlite3'))&&existsSync(join(owner,'STOP')),'Existing paused owner required');const stop=sha(regular(join(owner,'STOP'))),store=createOperatorStore({home:owner});let result;try{verify(plan);result=store.importGoals(plan.items.map(i=>i.goal),{apply:verb==='apply'});}finally{store.close();}assert.equal(sha(regular(join(owner,'STOP'))),stop);if(verb==='apply'){const receipt={schema:'bonfyre.estate-history-intake-receipt.v1',plan:saved.digest,...result};const bytes=encode(receipt,BOUND);retain(join(plan.materialDir,sha(bytes)+'.receipt.mpk'),bytes);}console.log('SOURCE_PLAN',saved);console.log('SCOPE',plan.counts,'aliases',plan.aliases.length,'sources',plan.sources.length);console.log('OWNER_RESULT',{created:result.created,unchanged:result.unchanged,missing:result.missing,applied:result.applied});console.log(plan.boundary);}
}
