// Antithesis -> Aurekai review boundary. This module never calls the platform.
// A Snouty plan is a bounded candidate, not a credential grant or an executed CLI.
// Imported failures remain unverified until native source/runner evidence is admitted.
const fail=(condition,message)=>{if(!condition)throw Error(message);};
const hex=/^[a-f0-9]{64}$/;
const repo=/^antithesishq\/[A-Za-z0-9_.-]{1,120}$/;
const commit=/^[a-f0-9]{40}$/;
const runId=/^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$/;
const property=/^[A-Za-z0-9][A-Za-z0-9_.:/ -]{0,191}$/;
const fullBinding=/^ws-[a-f0-9]{40}$/;
const fingerprint=async text=>Array.from(new Uint8Array(await crypto.subtle.digest(
  'SHA-256',new TextEncoder().encode(text))),x=>x.toString(16).padStart(2,'0')).join('');
const freeze=value=>Object.freeze(value);
const mintedFindings=new WeakSet();
export const reliabilityLanes=freeze([
 freeze({id:'public-ui',priority:'P0',owner:'BonfyreStitch',
  assets:freeze(['bombadil','bombadil-zoo']),assertion:'authenticated_route_property'}),
 freeze({id:'workgraph',priority:'P0',owner:'BonfyreQueue',
  assets:freeze(['antithesis-sdk-cpp','antithesis-sdk-rust','glitch-grid']),assertion:'no_repeat_effect'}),
 freeze({id:'database',priority:'P0',owner:'BonfyreWarehouse',
  assets:freeze(['sqlancer','postgres-workload']),assertion:'head_cas_integrity'}),
 freeze({id:'distributed',priority:'P0',owner:'BonfyreWebAddress',
  assets:freeze(['S3-Compatible-Workload','examples']),assertion:'snapshot_recovery'}),
 freeze({id:'connection',priority:'P1',owner:'ConnectionStudio',
  assets:freeze(['snouty','antithesis-trigger-action']),assertion:'actor_scoped_read'}),
 freeze({id:'evidence',priority:'P1',owner:'EvidenceGraph',
  assets:freeze(['antithesis-triage-action','antithesis-skills']),assertion:'unverified_failure_review'})
]);
// Exact read-only Snouty subcommands; no arbitrary argv, URLs, credentials,
// run cancellation, launching, debugger control or event-set DSL evaluation.
export function prepareSnoutyRead({binding,context='self',command,run_id}={}){
 fail(fullBinding.test(binding??'')&&context==='self','Choose a signed-in account-owned Connection Studio destination.');
 const allowed={
  doctor:['doctor'],runs:['runs','list'],
  show:['runs','show'],properties:['runs','properties']
 };
 fail(Object.hasOwn(allowed,command),'Only the declared read-only Snouty verbs may be reviewed.');
 const needsRun=['show','properties'].includes(command);
 fail(needsRun?runId.test(run_id??''):run_id===undefined,
  'Choose an exact run identity only for a run-scoped read.');
 const argv=freeze([...allowed[command],...(needsRun?[run_id]:[])]);
 return freeze({schema:'aurekai.antithesis.snouty.read-plan.v1',state:'candidate_unexecuted',
  command,argv,context,binding,tool:'snouty',
  providerInstalled:false,credentialsVerified:false,executionAuthorized:false,
  networkAccess:false,mutationAuthorized:false});
}
// Normalize a human-submitted incident coordinate, not an Antithesis REST
// object or an authoritative assertion about an executed Antithesis run.
export async function prepareAntithesisFinding({
 repository,git_commit,run_id,property_name,branch_ref,source_sha256,report_sha256,
 status='failed',observation
}={}){
 fail(repo.test(repository??'')&&commit.test(git_commit??'')&&
  runId.test(run_id??'')&&property.test(property_name??'')&&
  runId.test(branch_ref??'')&&hex.test(source_sha256??'')&&
  hex.test(report_sha256??''),'Provide exact GitHub, run, branch and digest coordinates.');
 fail(status==='failed'&&typeof observation==='string'&&observation.trim()&&
  observation.length<=500&& !/[\r\n\u0000-\u001f]/.test(observation),
  'Only bounded reported failures can enter the review queue.');
 const source=freeze({repository,git_commit,run_id,property_name,branch_ref,
  source_sha256,report_sha256});
 const key='antithesis-review:'+await fingerprint([
  repository,git_commit,run_id,property_name,branch_ref,source_sha256,report_sha256
 ].join('\n'));
 const finding=freeze({schema:'aurekai.antithesis.finding-review.v1',
  key,source,status:'unverified_candidate',statement:observation.trim(),
  effectClass:'none',reportVerified:false,commitVerified:false,providerRunVerified:false,
  sourceRightsVerified:false,workgraphExecuted:false,patchApplied:false,
  suggestedContinuation:'EvidenceGraph.review',
  nextStep:'Verify source bytes, Git commit, Antithesis report and current actor grant before saving or acting.'});
 mintedFindings.add(finding);
 return finding;
}
// Stable idempotence across identical upstream failure reports; changes in
// branch, tested commit or report digest yield distinct candidate identities.
export function collectFindingCandidates(candidates){
 fail(Array.isArray(candidates)&&candidates.length<=100,'Use a bounded review cohort.');
 const seen=new Set(),items=[];
 for(const item of candidates){
  fail(mintedFindings.has(item)&&item?.schema==='aurekai.antithesis.finding-review.v1'&&
    Object.isFrozen(item)&&item.status==='unverified_candidate'&&item.effectClass==='none'&&
    item.reportVerified===false&&item.commitVerified===false&&
    item.providerRunVerified===false&&item.sourceRightsVerified===false&&
    item.workgraphExecuted===false&&item.patchApplied===false&&
    typeof item.key==='string'&&/^antithesis-review:[a-f0-9]{64}$/.test(item.key),
    'Only native review candidates are eligible.');
  if(seen.has(item.key))continue;
  seen.add(item.key);items.push(item);
 }
 return freeze({state:'review_only',items:freeze(items),count:items.length,
  writes:0,confirmedAntithesisRuns:0,approvedPatches:0});
}
