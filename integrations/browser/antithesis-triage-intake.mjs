// Review Antithesis triage-action's 0.1 findings.json as untrusted input.
// Exact artifact digests pin provenance; no commands, patches, reports or
// workflow text are executed, exposed, persisted, or treated as verified.
import {prepareAntithesisFinding,collectFindingCandidates} from './antithesis-reliability.mjs';
const allow=(v,m)=>{if(!v)throw Error('Antithesis triage intake refused: '+m);};
const oid=v=>v&&typeof v==='object'&&!Array.isArray(v);
const keys=(v,list)=>oid(v)&&Object.keys(v).every(x=>list.includes(x));
const ident=v=>typeof v==='string'&&/^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$/.test(v);
const sha=v=>typeof v==='string'&&/^[a-f0-9]{64}$/.test(v);
const repository=v=>typeof v==='string'&&/^antithesishq\/[A-Za-z0-9_.-]{1,120}$/.test(v);
const commit=v=>typeof v==='string'&&/^[a-f0-9]{40}$/.test(v);
const str=v=>typeof v==='string'&&v.length<=8192;
const top=['schema_version','status','summary','selection','grouping','runs','skipped_runs','failures','known_issues_not_seen','ignored','not_investigated','run_health','errors'];
const runKeys=['run_id','created_at','group','status','commit','triage_report_url','properties_failed','properties_passed','instrumentation','notes'];
const findingKeys=['property','run_group','classification','failing_runs','baseline_runs','rate','known_issue','evidence','root_cause','confidence','category','source_refs','suggested_fix','patch_file','recommended_debug_session','reproduce_commands'];
const digest=async bytes=>Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',bytes)),b=>b.toString(16).padStart(2,'0')).join('');
export async function prepareAntithesisTriageIntake(body,{
  repository:repo,git_commit,source_sha256
}={}){
 allow(repository(repo)&&commit(git_commit)&&sha(source_sha256),'source identity');
 const bytes=typeof body==='string'?new TextEncoder().encode(body):body;
 allow(bytes instanceof Uint8Array&&bytes.length>=2&&bytes.length<=256*1024,'bounded findings.json');
 let parsed;
 try{parsed=JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(bytes));}
 catch{throw Error('Antithesis triage intake refused: invalid UTF-8 JSON');}
 allow(keys(parsed,top)&&top.every(k=>Object.hasOwn(parsed,k))&&
  parsed.schema_version==='0.1'&&['complete','partial'].includes(parsed.status)&&
  str(parsed.summary)&&keys(parsed.selection,['applied','runs_considered'])&&
  str(parsed.selection.applied)&&Number.isSafeInteger(parsed.selection.runs_considered)&&
  parsed.selection.runs_considered>=0&&
  keys(parsed.grouping,['applied','inferred'])&&
  str(parsed.grouping.applied)&&typeof parsed.grouping.inferred==='boolean'&&
  Array.isArray(parsed.runs)&&parsed.runs.length<=128&&
  Array.isArray(parsed.failures)&&parsed.failures.length<=100&&
  ['skipped_runs','known_issues_not_seen','ignored','not_investigated','run_health','errors']
  .every(k=>Array.isArray(parsed[k])&&parsed[k].length<=128),
  'unsupported or incomplete triage artifact');
 const runSet=new Set();
 for(const run of parsed.runs){
  allow(keys(run,runKeys)&&runKeys.every(k=>Object.hasOwn(run,k))&&
    ident(run.run_id)&&!runSet.has(run.run_id)&&
    (run.commit===null||commit(run.commit))&&
    typeof run.group==='string'&&run.group.length<=128&&
    Number.isSafeInteger(run.properties_failed)&&run.properties_failed>=0&&
    Number.isSafeInteger(run.properties_passed)&&run.properties_passed>=0&&
    ['ok','missing','unknown'].includes(run.instrumentation),
    'ambiguous triage run');
  runSet.add(run.run_id);
 }
 const report_sha256=await digest(bytes),findings=[];
 for(const failure of parsed.failures){
  allow(keys(failure,findingKeys)&&findingKeys.every(k=>Object.hasOwn(failure,k))&&
    typeof failure.property==='string'&&failure.property.length<=191&&
    ['new','ongoing','known_issue','unclear'].includes(failure.classification)&&
    Array.isArray(failure.failing_runs)&&failure.failing_runs.length>0&&failure.failing_runs.length<=128&&
    Array.isArray(failure.evidence)&&failure.evidence.length<=100&&
    Array.isArray(failure.reproduce_commands)&&failure.reproduce_commands.length<=50&&
    (failure.patch_file===null||typeof failure.patch_file==='string'),
    'invalid or untrusted triage failure');
  const seen=new Set(),runIds=[];
  for(const id of failure.failing_runs){
   allow(ident(id)&&runSet.has(id)&&!seen.has(id),'unmatched or duplicated failing run');
   seen.add(id);runIds.push(id);
   const run=parsed.runs.find(r=>r.run_id===id);
   allow(run.commit===git_commit,'triage run commit differs from selected source');
  }
  for(const item of failure.evidence)
   allow(keys(item,['run_id','input_hash','vtime','container','excerpt'])&&
     ident(item.run_id)&&seen.has(item.run_id),'evidence run identity mismatch');
  for(const id of runIds){
   // The original report is pinned in the digest, but only fixed explanatory
   // text is promoted. Untrusted suggested commands and secret-bearing logs
   // stay in the original file, outside the WorkGraph/LLM prompt.
   findings.push(await prepareAntithesisFinding({
     repository:repo,git_commit,run_id:id,property_name:failure.property,
     branch_ref:'artifact-v0.1',source_sha256,report_sha256,
     observation:'Antithesis triage-action reported a '+failure.classification+
       ' failed property; verify the original report and exact run.'
   }));
  }
 }
 const cohort=collectFindingCandidates(findings);
 return Object.freeze({schema:'aurekai.antithesis.triage-intake.v1',
  state:'review_unverified',repository:repo,git_commit,
  source_sha256,report_sha256,report_bytes:bytes.length,
  run_count:runSet.size,reported_failure_count:parsed.failures.length,
  candidate_count:cohort.count,candidates:cohort.items,
  suggestedCommandsExecuted:0,patchesApplied:0,sourcePublished:false,
  reportAuthenticated:false,accountGranted:false,
  nextStep:'Review exact raw artifact, independently authenticate run and source, then use the current EvidenceGraph owner.'});
}
