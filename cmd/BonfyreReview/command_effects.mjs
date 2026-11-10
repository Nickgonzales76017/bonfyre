import { createHash } from 'node:crypto';
import { constants, openSync, fstatSync, readFileSync, closeSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { refuse, validId, validDigest } from './authority.mjs';

// Schema-validated capability_program rows are the only source for native
// command effect bindings. CLI discovery and API scopes are NOT effect grants.
const IDENT=/^[a-z0-9](?:[a-z0-9-]{0,62}[a-z0-9])?$/;
const HASH=/^[a-f0-9]{64}$/;
const bad=message=>refuse(403,message);
export function commandBindings(rows) {
  if(!Array.isArray(rows)||rows.length>512)throw Error('Bounded CMS capability program rows required');
  const map=new Map();
  for(const row of rows) {
    const p=row?.attributes??row;
    if(!p||p.application_kind!=='native-command'||p.status!=='published')continue;
    const pack=p.capability_requirements?.installed_pack, family=p.capability_requirements?.workspace_family;
    const review=p.transition_conditions?.review;
    const carrier=p.carrier_candidates;
    const names=[p.program_id,p.action,pack,family,review?.experience,review?.institution,
      review?.approved_family];
    if(names.some(n=>typeof n!=='string'||!IDENT.test(n))||
       p.authority_requirement!=='actor+pack+independent-review'||
       !Array.isArray(carrier)||carrier.length!==1||
       carrier[0]?.kind!=='native-workgraph'||!IDENT.test(carrier[0]?.type??'')||
       carrier[0].operator_id!==`queue.${carrier[0].type}`||
       carrier[0].type!==p.action||
       review?.executor!=='BonfyreQueue'||
       review?.artifact_kind!=='immutable-review-decision')
       throw Error('Invalid command capability/review/executor declaration');
    if(map.has(p.action))throw Error('Ambiguous native command capability binding');
    map.set(p.action,Object.freeze({command:p.action,program:p.program_id,pack,family,
      experience:review.experience,institution:review.institution,
      approvedFamily:review.approved_family,operator:carrier[0].operator_id,
      type:carrier[0].type,executor:'BonfyreQueue'}));
  }
  return map;
}
const digest=parts=>createHash('sha256').update(
  parts.map(x=>`${Buffer.byteLength(String(x))}:${x}`).join('')
).digest('hex');

// The reviewed source is the exact effect intent, not a mutable request body.
// A decision for one pack, account, program or operator cannot be repurposed.
const INTENT_FIELDS=['command','program','pack','pack_digest','subject',
  'workspace_binding','workspace_release','operator','input_sha256'];
export function readCommandIntent(uri,expectedSha256) {
  validDigest(expectedSha256);
  let fd;
  try {
    const address=new URL(uri);
    if(address.protocol!=='file:'||address.hostname||address.search||address.hash)
      throw Error('Not a local owner artifact');
    fd=openSync(fileURLToPath(address),constants.O_RDONLY|constants.O_NOFOLLOW);
    const st=fstatSync(fd);
    if(!st.isFile()||st.size<32||st.size>4096)throw Error('Invalid source size');
    const bytes=readFileSync(fd);
    if(createHash('sha256').update(bytes).digest('hex')!==expectedSha256)
      throw Error('Source digest changed');
    const content=bytes.toString('utf8');
    if(content.includes('\r')||content.includes('\0')||content.includes('\uFFFD'))
      throw Error('Invalid source encoding');
    const lines=content.trimEnd().split('\n');
    if(lines.length!==1+INTENT_FIELDS.length||lines[0]!=='command_effect_intent v1')
      throw Error('Invalid effect source grammar');
    const intent={};
    INTENT_FIELDS.forEach((key,i)=>{
      const prefix='  '+key+' ';
      const line=lines[i+1];
      if(!line.startsWith(prefix)||!line.slice(prefix.length)||
         /[\x00-\x1f\x7f]/.test(line))throw Error('Invalid intent field '+key);
      intent[key]=line.slice(prefix.length);
    });
    if(!HASH.test(intent.pack_digest)||!HASH.test(intent.workspace_release)||
       !HASH.test(intent.input_sha256)||
       !/^ws-[a-f0-9]{40}$/.test(intent.workspace_binding))
      throw Error('Invalid pinned intent digests');
    return Object.freeze(intent);
  }catch {
    throw bad('Reviewed source intent is absent, changed or invalid');
  }finally{if(fd!==undefined)closeSync(fd);}
}

// The execution request contains IDs, never shell argv or user-supplied
// operator names. Each proof is re-read from its actual native owner.
export function reviewedCommandEffects({
  programs, review, auth, packCurrent, workspaceCurrent, workerCurrent,
  queue, queueDb
}) {
  const bindings=commandBindings(programs);
  if(!review||typeof review.read!=='function'||typeof review.reconcile!=='function'||
     !auth||typeof auth.principal!=='function'||typeof auth.allowed!=='function'||
     typeof packCurrent!=='function'||typeof workspaceCurrent!=='function'||
     typeof workerCurrent!=='function'||typeof queue!=='function'||
     typeof queueDb!=='string'||!queueDb.startsWith('/'))
    throw Error('Native owners for actor, pack, workspace, executor, review and queue are required');

  function dispatch({token,command,program,context,reviewId,sourceSha256}) {
    const binding=bindings.get(validId(command,'command'));
    if(!binding||binding.program!==program)throw bad('No declared capability program for command');
    const principal=auth.principal(token);
    if(!Number.isSafeInteger(principal?.userId)||principal.userId<1||
       typeof principal.actor!=='string'||!principal.actor.trim())
      throw bad('Current native ActorClosure required');
    const own='user:'+principal.userId;
    const subject=context==='self'?own:context;
    const scope=context==='self'?'self':'institution';
    if(scope==='institution'&&
       (context!==('institution:'+binding.institution)||
        auth.allowed(token,binding.institution,'write')!==true))
      throw bad('Current institution destination required');
    if(scope==='self'&&subject!==own)throw bad('Account identity changed');
    const installed=packCurrent({token,pack:binding.pack,subject,scope,actor:own});
    if(installed?.state!=='active'||!HASH.test(installed.digest??'')||
       installed.digest!==installed.pinned_digest)
      throw bad('Installed PackGraph is not current and pinned');
    const workspace=workspaceCurrent({token,pack:binding.pack,subject,scope,
      family:binding.family,digest:installed.digest});
    if(workspace?.status!=='active'||!/^ws-[a-f0-9]{40}$/.test(workspace.binding??'')||
       !HASH.test(workspace.release??''))
      throw bad('Destination-owned installed application binding is unavailable');
    const worker=workerCurrent({command,program,operator:binding.operator,
       executor:binding.executor,artifactKind:'immutable-review-decision',
       subject,packDigest:installed.digest,workspaceBinding:workspace.binding});
    if(worker?.state!=='ready'||worker.operator_id!==binding.operator||
       worker.program_id!==program||worker.pack_digest!==installed.digest||
       worker.workspace_binding!==workspace.binding||!HASH.test(worker.binary_sha256??''))
      throw bad('No attested native worker for this exact effect');
      throw bad('No attested native worker for this exact effect');
    const r=review.read({token,reviewId:validId(reviewId,'review ID')});
    const latest=r.history?.at(-1),effect=r.downstream?.at(-1);
    if(r.state!=='approved'||r.experience!==binding.experience||
       r.institution!==binding.institution||
       r.authorUserId!==principal.userId||
       !latest||latest.action!=='approve'||latest.state!=='approved'||
       latest.actorUserId===r.authorUserId||
       !latest.artifact?.startsWith('file://')||
       !HASH.test(latest.sha256??'')||
       r.source?.sha256!==validDigest(sourceSha256)||
       !effect||effect.revision!==r.revision||
       effect.family!==binding.approvedFamily||!effect.admitted)
      throw bad('Independent source-bound review approval is required');
    const intent=readCommandIntent(r.source.uri,r.source.sha256);
    if(intent.command!==command||intent.program!==program||intent.pack!==binding.pack||
       intent.pack_digest!==installed.digest||intent.subject!==subject||
       intent.workspace_binding!==workspace.binding||
       intent.workspace_release!==workspace.release||
       intent.operator!==binding.operator)
      throw bad('Reviewed effect is bound to a different native command or destination');
    // The review's WorkGraph decision MUST exist exactly as recorded.
    const decision=queue(['mission',effect.mission],queueDb);
    if(decision.node_id!=='job'||decision.operator_id!=='queue.review-decision'||
       decision.family!==binding.approvedFamily||
       decision.input_uri!==latest.artifact)
      throw bad('Review decision WorkGraph receipt has changed');
    // Recheck volatile owners immediately before committing Queue admission.
    // A stale review read or lost worker binding must not become a lease.
    const again=auth.principal(token);
    if(again?.userId!==principal.userId||again.actor!==principal.actor||
       (scope==='institution'&&auth.allowed(token,binding.institution,'write')!==true))
      throw bad('Actor authority changed before Queue admission');
    const packAgain=packCurrent({token,pack:binding.pack,subject,scope,actor:own});
    const workspaceAgain=workspaceCurrent({token,pack:binding.pack,subject,scope,
      family:binding.family,digest:installed.digest});
    const workerAgain=workerCurrent({command,program,operator:binding.operator,
      executor:binding.executor,artifactKind:'immutable-review-decision',
      subject,packDigest:installed.digest,workspaceBinding:workspace.binding});
    if(packAgain?.state!=='active'||packAgain.digest!==installed.digest||
       packAgain.pinned_digest!==installed.digest||
       workspaceAgain?.status!=='active'||workspaceAgain.binding!==workspace.binding||
       workspaceAgain.release!==workspace.release||
       workerAgain?.state!=='ready'||workerAgain.operator_id!==binding.operator||
       workerAgain.program_id!==program||workerAgain.pack_digest!==installed.digest||
       workerAgain.workspace_binding!==workspace.binding||
       workerAgain.binary_sha256!==worker.binary_sha256)
      throw bad('Installed capability or execution worker changed before admission');

    const mission='command-'+digest([program,command,subject,installed.digest,
      reviewId,r.revision,latest.sha256]).slice(0,48);
    // Queue's --if-absent protects replay, and the next read verifies all
    // identity-bearing fields. No direct binary execution occurs in this gate.
    queue(['enqueue',binding.type,latest.artifact,'--mission',mission,
      '--family',binding.approvedFamily,'--if-absent','--retries','3'],queueDb);
    const node=queue(['mission',mission],queueDb);
    if(node.node_id!=='job'||node.operator_id!==binding.operator||
       node.family!==binding.approvedFamily||node.input_uri!==latest.artifact)
      throw bad('Command WorkGraph executor identity changed');
    return Object.freeze({command,program,pack:binding.pack,pack_digest:installed.digest,
      subject,workspace_binding:workspace.binding,review_id:reviewId,
      review_revision:r.revision,review_mission:effect.mission,mission_id:mission,
      operator_id:node.operator_id,workgraph_status:node.status,
      admitted:true,executed:node.status==='complete'});
  }
  return Object.freeze({dispatch,
    catalogue:()=>[...bindings.values()].map(binding=>({...binding,admitted:false}))
  });
}
