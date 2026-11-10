#!/usr/bin/env bun
// Project the existing portfolio into PackGraph. No app registry or user state.
import {readFileSync} from 'node:fs';
import {resolve,join} from 'node:path';
import {fileURLToPath} from 'node:url';
import {createHash} from 'node:crypto';
import {execFileSync} from 'node:child_process';
const root=resolve(fileURLToPath(new URL('..',import.meta.url)));
const hash=value=>createHash('sha256').update(value).digest('hex');
export function portfolio(text){
  const rows=[];let current;
  for(const line of text.split(/\r?\n/)){
    const start=/^maturity (\d+) ([a-z0-9-]+)$/.exec(line);
    if(start){current={id:Number(start[1]),slug:start[2]};rows.push(current);continue;}
    const field=/^  (class|owner|entry|depends|stages|release_gate|witness) (.+)$/.exec(line);
    if(current&&field){if(Object.hasOwn(current,field[1]))throw Error('Duplicate portfolio field.');current[field[1]]=field[2];}
  }
  if(rows.length!==30||new Set(rows.map(r=>r.id)).size!==30)throw Error('Expected the thirty-program portfolio.');
  for(const row of rows)for(const key of ['class','owner','entry','depends','stages','release_gate','witness'])if(!row[key])throw Error('Incomplete portfolio entry.');
  return rows;
}
export function projectSetups(rows,ids){
  const selected=new Map(),byId=new Map(rows.map(r=>[r.id,r]));
  function visit(id,trail=[]){
    if(trail.includes(id))throw Error('Portfolio dependency cycle.');
    if(selected.has(id))return;
    const row=byId.get(id);if(!row)throw Error('Unknown portfolio prerequisite.');
    for(const dep of row.depends==='-'?[]:row.depends.split(',').map(Number))visit(dep,[...trail,id]);
    selected.set(id,row);
  }
  ids.forEach(id=>visit(id));
  const records=[];
  for(const row of selected.values()){
    const pack='experience.'+row.slug,label=row.slug.split('-').map(w=>w==='itad'?'ITAD':w[0].toUpperCase()+w.slice(1)).join(' ');
    const digest=hash(Object.entries(row).sort().map(([k,v])=>k+'\t'+v).join('\n'));
    const version=digest.slice(0,24),common={pack,status:'published'};
    records.push({type:'pack_artifact',key:{pack},value:{...common,canonical_name:label,publisher:'aurekai',layer:'official',kind:'workflow-plan',purpose:'Workflow plan: '+row.stages.replaceAll('|',' → ')+'. Product release requirements remain separate from configuration installation.',governed_by:'operator-authored-program-candidates',cannot_infer:'Installation does not provision the application backend or finish the portfolio release requirements.'}});
    records.push({type:'pack_version',key:{pack,version},value:{...common,version,root:digest,changed:'Portfolio definition '+String(row.id).padStart(2,'0'),proof:row.owner}});
    if(row.depends!=='-')for(const id of row.depends.split(',').map(Number)){
      const depends_on='experience.'+byId.get(id).slug;
      records.push({type:'pack_dependency',key:{pack,depends_on},value:{...common,depends_on,channel:'stable',why:'Declared portfolio prerequisite; resource availability is checked by its owner.'}});
    }
    const demands=[['release-check',row.release_gate,'Release requirement','requirement'],['native-owner',row.owner,'Existing implementation owner','source']];
    if(row.entry!=='-'){
      const url=new URL(row.entry);
      if(url.protocol!=='https:'||url.username||url.password||url.search||url.hash)throw Error('Unsafe portfolio entry.');
      const guide=row.class.includes('guide')||row.class==='case-study';
      demands.push(['public-preview',url.href,label,guide?'guide':'demo']);
    }
    for(const [demand_kind,needs,preferred_sources,fields] of demands)records.push({type:'pack_demand',key:{pack,demand_kind,needs},value:{...common,demand_kind,needs,preferred_sources,fields,body_regime:'REMOTE',reheat_when:'explicit reviewed user action',optional:'true'}});
    // Heads publish last, after the selected definitions and dependencies exist.
    records.push({type:'pack_head',key:{pack,channel:'stable'},value:{...common,channel:'stable',version,advanced_by:'aurekai-portfolio-publisher'}});
  }
  return records.sort((a,b)=>(a.type==='pack_head')-(b.type==='pack_head'));
}
export function projectCreatorWorkspaceSetup(release){
  if(!/^[a-f0-9]{64}$/.test(release??''))throw Error('Choose a retained creator release.');
  const pack='experience.native-creator-studio.workspace',family='creator-audio',entry='audio/',version=hash(['creator-workspace-v1',release,family,entry].join('\n')).slice(0,24),common={pack,status:'published'};
  const records=[
    {type:'pack_artifact',key:{pack},value:{...common,canonical_name:'Audio Studio — shared workspace',publisher:'aurekai',layer:'official',kind:'application',purpose:'Arrange recordings in the existing audio editor with destination-owned session drafts and exact WAV sources.',governed_by:'native-workspace-installation',cannot_infer:'Saving or adopting an audio session does not establish recording rights, approve publication or distribute it.'}},
    {type:'pack_version',key:{pack,version},value:{...common,version,root:release,changed:'Native audio session storage and original WAV references',proof:'cmd/BonfyreWebAddress/tests/creator_workspace_test.mjs'}},
    {type:'pack_dependency',key:{pack,depends_on:'experience.native-creator-studio'},value:{...common,depends_on:'experience.native-creator-studio',channel:'stable',why:'Existing audio editor and native/Wasm mix behavior.'}},
    {type:'pack_demand',key:{pack,demand_kind:'workspace',needs:family},value:{...common,demand_kind:'workspace',needs:family,preferred_sources:release,fields:entry}},
    {type:'pack_demand',key:{pack,demand_kind:'workspace-record',needs:family},value:{...common,demand_kind:'workspace-record',needs:family,fields:'draft:creator:'}},{type:'capability_program',key:{program_id:'feed.text.narration'},value:{program_id:'feed.text.narration',application_kind:family,action:'feed-intake',consumes:['text','publication'],produces:{mode:'narration',prefix:'draft:creator:',endpoint:'_narrate'},carrier_candidates:['workspace-binding'],capability_requirements:{pack,record_prefix:'draft:creator:'},authority_requirement:'current-destination-write',transition_conditions:{review_required:true,save_session:false},failure_projection:'Show narration as unavailable when the installed workspace release cannot serve its native narration owner.',status:'published'}},
    {type:'pack_demand',key:{pack,demand_kind:'public-preview',needs:'https://aurekai-work-samples.pages.dev/audio/'},value:{...common,demand_kind:'public-preview',needs:'https://aurekai-work-samples.pages.dev/audio/',preferred_sources:'Public audio arrangement sample',fields:'demo'}},
    {type:'pack_head',key:{pack,channel:'stable'},value:{...common,channel:'stable',version,advanced_by:'aurekai-workspace-publisher'}}
  ];
  return records;
}

export function projectWorkspaceSetups(rows,release){
  if(!/^[a-f0-9]{64}$/.test(release??''))throw Error('Choose the exact verified native editor package.');
  const records=[];
  for(const [id,entry,label]of [[1,'product-room/','Room Planning — shared workspace'],[2,'catalogue/','Catalogue — shared workspace']]){
    const row=rows.find(r=>r.id===id);if(!row)throw Error('The existing portfolio product is missing.');
    const base='experience.'+row.slug,pack=base+'.workspace',common={pack,status:'published'};
    const digest=hash('spaces-workspace-v2\n'+release+'\n'+Object.entries(row).sort().map(([k,v])=>k+'\t'+v).join('\n')),version=digest.slice(0,24);
    records.push({type:'pack_artifact',key:{pack},value:{...common,canonical_name:label,publisher:'aurekai',layer:'official',kind:'application',
      purpose:'Open the specialized catalogue and room editors with destination-owned drafts, verified model files and explicit adoption of existing browser work.',
      governed_by:'native-content-and-workspace-authority',cannot_infer:'No supplier purchase, merchant publication, new organization grant or household membership is created. Public demo originals remain separate until explicitly adopted.'}});
    records.push({type:'pack_version',key:{pack,version},value:{...common,version,root:digest,changed:'Connected native editor package '+release,proof:row.owner}});
    records.push({type:'pack_dependency',key:{pack,depends_on:base},value:{...common,depends_on:base,channel:'stable',why:'The existing portfolio product and its declared prerequisite behavior.'}});
    records.push({type:'pack_demand',key:{pack,demand_kind:'workspace',needs:'spaces'},value:{...common,demand_kind:'workspace',needs:'spaces',preferred_sources:release,fields:entry,body_regime:'LOCAL',reheat_when:'explicit reviewed installation',optional:'false'}});
    for(const fields of ['draft:catalogue:','review:catalogue:','release:catalogue:','fixture-plan:','product-model:','procurement-draft:','procurement-rfq:','procurement-quote:','procurement-comparison:','procurement-allocation:','procurement-approval:','procurement-order-intent:'])records.push({type:'pack_demand',key:{pack,demand_kind:'workspace-record',needs:'spaces',fields},value:{...common,demand_kind:'workspace-record',needs:'spaces',fields,body_regime:'LOCAL',reheat_when:'authorized editor read or write',optional:'false'}});
    records.push({type:'pack_head',key:{pack,channel:'stable'},value:{...common,channel:'stable',version,advanced_by:'aurekai-workspace-publisher'}});
  }
  return records.sort((a,b)=>(a.type==='pack_head')-(b.type==='pack_head'));
}

// Connected releases do not rewrite the previously published workflow plans.
// Both projections use the same portfolio and native PackGraph owner.

// The source/program compiler is already a ProgramFabric owner; this definition
// binds its reviewed editor package to the same destination-owned record service.
export function projectProgramWorkspaceSetup(release){
  if(!/^[a-f0-9]{64}$/.test(release??''))throw Error('Choose the exact verified program editor package.');
  const pack='experience.source-programs.workspace',common={pack,status:'published'},digest=hash('source-program-workspace\n'+release),version=digest.slice(0,24);
  return [
    {type:'pack_artifact',key:{pack},value:{...common,canonical_name:'Sources and recurring programs',publisher:'aurekai',layer:'official',kind:'application',purpose:'Build recurring product selections, customize individual cycles, compare upcoming programs and review source-price changes across saved programs in the selected destination.',governed_by:'native-content-and-workspace-authority',cannot_infer:'A saved program is a proposal. Installation does not grant merchant access, authorize an order, create a subscription or execute a charge.'}},
    {type:'pack_version',key:{pack,version},value:{...common,version,root:digest,changed:'Native source and program editor package '+release,proof:'scripts/recurring-commerce/draft.mjs; integrations/browser/tests/contents-native.mjs'}},
    {type:'pack_demand',key:{pack,demand_kind:'workspace',needs:'recurring-programs'},value:{...common,demand_kind:'workspace',needs:'recurring-programs',preferred_sources:release,fields:'programs/',body_regime:'LOCAL',reheat_when:'explicit reviewed installation',optional:'false'}},
    ...['draft:','observation:'].map(fields=>({type:'pack_demand',key:{pack,demand_kind:'workspace-record',needs:'recurring-programs',fields},value:{...common,demand_kind:'workspace-record',needs:'recurring-programs',fields,body_regime:'LOCAL',reheat_when:'authorized source and program read or write',optional:'false'}})),
    {type:'pack_head',key:{pack,channel:'stable'},value:{...common,channel:'stable',version,advanced_by:'aurekai-workspace-publisher'}}
  ];
}
export function projectFinanceWorkspaceSetup(release){
  if(!/^[a-f0-9]{64}$/.test(release??''))throw Error('Choose the exact verified financial workbook package.');
  const pack='experience.finance-workbooks.workspace',family='financial-workbooks',common={pack,status:'published'},digest=hash('financial-workbook-workspace\n'+release),version=digest.slice(0,24);
  return [
    {type:'pack_artifact',key:{pack},value:{...common,canonical_name:'Financial workbook review',publisher:'aurekai',layer:'official',kind:'application',purpose:'Open Excel review workbooks, edit assumptions, recalculate formulas and retain reviews in the selected account or organization.',governed_by:'native-content-and-workspace-authority',cannot_infer:'A saved review is not a ledger posting, completed reconciliation, financial approval or payment.'}},
    {type:'pack_version',key:{pack,version},value:{...common,version,root:digest,changed:'Financial workbook editor package '+release,proof:'components/BonfyreServiceSite/tests/workbook-native.test.mjs'}},
    {type:'pack_demand',key:{pack,demand_kind:'workspace',needs:family},value:{...common,demand_kind:'workspace',needs:family,preferred_sources:release,fields:'finance/',body_regime:'LOCAL',reheat_when:'explicit reviewed installation',optional:'false'}},
    {type:'pack_demand',key:{pack,demand_kind:'workspace-record',needs:family,fields:'finance-workbook:'},value:{...common,demand_kind:'workspace-record',needs:family,fields:'finance-workbook:',body_regime:'LOCAL',reheat_when:'authorized financial review read or write',optional:'false'}},
    {type:'pack_demand',key:{pack,demand_kind:'workspace-record',needs:family,fields:'procurement-order-intent:'},value:{...common,demand_kind:'workspace-record',needs:family,fields:'procurement-order-intent:',body_regime:'LOCAL',reheat_when:'authorized read-only downstream procurement review; never ledger posting, payment or purchase authority',optional:'false'}},
    {type:'pack_demand',key:{pack,demand_kind:'workspace-record',needs:family,fields:'crm-finance:'},value:{...common,demand_kind:'workspace-record',needs:family,fields:'crm-finance:',body_regime:'LOCAL',reheat_when:'exact CRM-source review and current financial destination write',optional:'false'}},
    {type:'pack_head',key:{pack,channel:'stable'},value:{...common,channel:'stable',version,advanced_by:'aurekai-workspace-publisher'}}
  ];
}
// The skill instruments (process simulator, research desk, schema workspace, planner, pricing desk) as one pack:
// one editor release, one record prefix per instrument.
export function projectInstrumentsWorkspaceSetup(release){
  if(!/^[a-f0-9]{64}$/.test(release??''))throw Error('Choose the exact verified instruments package.');
  const pack='experience.skill-instruments.workspace',family='skill-instruments',common={pack,status:'published'},digest=hash('skill-instruments-workspace\n'+release),version=digest.slice(0,24);
  const prefixes=['process-model:','review-project:','schema-plan:','plan:','price-work:','opportunity:'];
  return [
    {type:'pack_artifact',key:{pack},value:{...common,canonical_name:'Planning and analysis instruments',publisher:'aurekai',layer:'official',kind:'application',purpose:'Simulate queued work and staffing, review literature with exact quotations, change database structure safely, plan dependencies, price capabilities and answer buyers\' requirements, with work saved in the selected account or organization.',governed_by:'native-content-and-workspace-authority',cannot_infer:'A saved instrument draft is not a hiring decision, a published finding, a migrated database, an agreed schedule or a price in the service book.'}},
    {type:'pack_version',key:{pack,version},value:{...common,version,root:digest,changed:'Skill instruments editor package '+release,proof:'components/BonfyreServiceSite/tests/instruments-native.test.mjs'}},
    {type:'pack_demand',key:{pack,demand_kind:'workspace',needs:family},value:{...common,demand_kind:'workspace',needs:family,preferred_sources:release,fields:'instruments/',body_regime:'LOCAL',reheat_when:'explicit reviewed installation',optional:'false'}},
    {type:'capability_program',key:{program_id:'feed.jobs.relationship'},value:{program_id:'feed.jobs.relationship',application_kind:family,action:'feed-intake',consumes:['jobs','people'],produces:{mode:'relationship',prefix:'opportunity:'},carrier_candidates:['workspace-binding'],capability_requirements:{pack,record_prefix:'opportunity:'},authority_requirement:'current-destination-write',transition_conditions:{review_required:true,identity:'unresolved-until-reviewed',source_fingerprint_required:true},failure_projection:'Show the relationship handoff as unavailable when the installed workspace cannot accept the existing opportunity record family.',status:'published'}},...prefixes.map(fields=>({type:'pack_demand',key:{pack,demand_kind:'workspace-record',needs:family,fields},value:{...common,demand_kind:'workspace-record',needs:family,fields,body_regime:'LOCAL',reheat_when:'authorized instrument read or write',optional:'false'}})),
    {type:'pack_head',key:{pack,channel:'stable'},value:{...common,channel:'stable',version,advanced_by:'aurekai-workspace-publisher'}}
  ];
}
// The separately published research/product/schema/trace implementation shares
// one native destination so its reviewed cross-instrument handoffs stay atomic.
export function projectConnectedInstrumentsSetup(release){
  if(!/^[a-f0-9]{64}$/.test(release??''))throw Error('Choose the exact verified connected-instrument package.');
  const pack='experience.connected-instruments.workspace',family='connected-instruments',common={pack,status:'published'};
  const digest=hash('connected-instruments-workspace\n'+release),version=digest.slice(0,24);
  return [
    {type:'pack_artifact',key:{pack},value:{...common,canonical_name:'Connected research and planning',publisher:'aurekai',layer:'official',kind:'application',purpose:'Open research, product planning, process traces and SQLite migration side by side. Keep source-linked opportunities and connected edits in the selected destination.',governed_by:'native-content-and-workspace-authority',cannot_infer:'Saving connected drafts does not publish a finding, approve a product commitment, change a production database or predict process performance.'}},
    {type:'pack_version',key:{pack,version},value:{...common,version,root:digest,changed:'Native connected-instrument package '+release,proof:'scripts/tests/connected-instruments-native.test.mjs'}},
    {type:'pack_demand',key:{pack,demand_kind:'workspace',needs:family},value:{...common,demand_kind:'workspace',needs:family,preferred_sources:release,fields:'instruments/',body_regime:'LOCAL',reheat_when:'explicit reviewed installation',optional:'false'}},
    {type:'capability_program',key:{program_id:'feed.publication.research'},value:{program_id:'feed.publication.research',application_kind:family,action:'feed-intake',consumes:['publication','text'],produces:{mode:'research',prefix:'draft:instruments:research:'},carrier_candidates:['workspace-binding','source-review'],capability_requirements:{pack,record_prefix:'draft:instruments:research:'},authority_requirement:'current-destination-write',transition_conditions:{review_required:true,source_fingerprint_required:true,direct_write:false},failure_projection:'Open the exact retained source review and revalidate the selected research destination before any research draft can be saved.',status:'published'}},...['traces','research','schema','product','incoming'].map(kind=>{const fields='draft:instruments:'+kind+':';return {type:'pack_demand',key:{pack,demand_kind:'workspace-record',needs:family,fields},value:{...common,demand_kind:'workspace-record',needs:family,fields,body_regime:'LOCAL',reheat_when:'authorized editor read or write',optional:'false'}};}),
    {type:'pack_head',key:{pack,channel:'stable'},value:{...common,channel:'stable',version,advanced_by:'aurekai-workspace-publisher'}}
  ];
}

export function publishSetups({records,binary,db,schemas,apply=false}){
  if(!db||!binary||!schemas)throw Error('Explicit native owner coordinates required.');
  const run=args=>{
    const output=execFileSync(binary,[...args,'--db',db,'--schemas',schemas],{encoding:'utf8',stdio:['pipe','pipe','pipe'],maxBuffer:8*1024*1024,timeout:20000});
    return JSON.parse(output);
  };
  const types=[...new Set(records.map(r=>r.type))],existing=new Map();
  for(const type of types){
    const result=run(['entry','list',type,'--ns','root','--limit','5000']);
    if(!Array.isArray(result.data)||result.data.length>=5000||Number(result.meta?.total)>result.data.length)throw Error('Incomplete CMS enumeration.');
    existing.set(type,result.data.map(r=>({id:r.id,...(r.attributes??r)})));
  }
  const pending=[];
  for(const record of records){
    const hits=existing.get(record.type).filter(r=>Object.entries(record.key).every(([k,v])=>String(r[k])===String(v)));
    if(hits.length>1)throw Error('Duplicate native configuration identity.');
    if(hits.length&&Object.entries(record.value).some(([k,v])=>String(hits[0][k]??'')!==String(v)))throw Error('Existing definition differs; review an upgrade through its owner.');
    if(!hits.length)pending.push(record);
  }
  if(apply)for(const row of pending)run(['entry','create',row.type,JSON.stringify(row.value),'--ns','root']);
  return {mode:apply?'applied':'plan',configurations:new Set(records.map(r=>r.value.pack)).size,rows:records.length,missing:pending.length,created:apply?pending.length:0};
}
const workspaceSelectors=['workspace','finance-workspace','instruments-workspace','connected-instruments-workspace'];
export function setupOptions(args){
  if(!Array.isArray(args))throw Error('Expected setup arguments.');
  const options={},allowed=new Set(['db','binary','schemas','portfolio','ids',...workspaceSelectors]);
  for(let i=0;i<args.length;i++){
    const arg=args[i];
    if(typeof arg!=='string'||!arg.startsWith('--'))throw Error('Use named setup options.');
    const name=arg.slice(2);
    if(Object.hasOwn(options,name))throw Error('Repeated setup option: '+arg);
    if(name==='apply'){options.apply=true;continue;}
    if(!allowed.has(name))throw Error('Unknown setup option: '+arg);
    const value=args[++i];
    if(typeof value!=='string'||!value.trim()||value.startsWith('--'))throw Error('Missing value for '+arg);
    options[name]=value;
  }
  const selected=workspaceSelectors.filter(name=>Object.hasOwn(options,name));
  if(selected.length>1)throw Error('Choose one workspace family to publish.');
  if(selected.length&&options.ids)throw Error('--ids selects portfolio configurations, not a workspace family.');
  if(selected.length&&selected[0]!=='workspace'&&options.portfolio)throw Error('This workspace family does not read a portfolio.');
  return options;
}
export function setupRecords(options){
  const selected=workspaceSelectors.filter(name=>Object.hasOwn(options,name));
  if(selected.length>1)throw Error('Choose one workspace family to publish.');
  // Exact package families do not depend on the unrelated portfolio file.
  if(selected[0]==='connected-instruments-workspace')return projectConnectedInstrumentsSetup(options[selected[0]]);
  if(selected[0]==='instruments-workspace')return projectInstrumentsWorkspaceSetup(options[selected[0]]);
  if(selected[0]==='finance-workspace')return projectFinanceWorkspaceSetup(options[selected[0]]);
  const ids=(options.ids??'1,2,5,6,17,18').split(',').map(Number);
  if(ids.some(n=>!Number.isInteger(n)||n<1||n>30))throw Error('Invalid portfolio selection.');
  const rows=portfolio(readFileSync(options.portfolio??join(root,'cmd/BonfyreExperienceStage/experience-maturities.yaff'),'utf8'));
  return selected[0]==='workspace'?projectWorkspaceSetups(rows,options.workspace):projectSetups(rows,ids);
}
if(import.meta.main){
  const options=setupOptions(process.argv.slice(2)),records=setupRecords(options);
  const result=publishSetups({...options,records});
  for(const [key,value] of Object.entries(result))console.log(key+'\t'+value);
}
