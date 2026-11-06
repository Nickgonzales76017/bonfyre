import { resolve } from 'node:path';
import { reviewState } from './state.mjs';
import { reviewAuthority, refuse, validId, validReason, intentDigest } from './authority.mjs';
import { reviewEffects } from './effects.mjs';

const ACTIONS=new Set(['approve','reject','revise','delegate','escalate','resubmit']);
const NEEDS_REASON=new Set(['reject','revise','delegate','escalate']);
const NEXT=Object.freeze({approve:'approved',reject:'rejected',
  revise:'revision_requested',delegate:'delegated',escalate:'escalated'});

// Endpoints may call this shared engine from any registered experience. Native
// account scope, a bounded source owner and the existing WorkGraph Queue are
// required by construction. Browser state never becomes canonical authority.
export function nativeReview({home,authDb,experiences,auth,queue,clock=()=>new Date().toISOString()}) {
  if(typeof home!=='string'||!home.startsWith('/'))throw Error('Owned absolute review home required');
  home=resolve(home);
  const authority=reviewAuthority(authDb,experiences,auth);
  const state=reviewState(home,authority.policies);
  const downstream=reviewEffects(home,queue);
  const actor=authority.principal,permit=authority.permit;
  function view({token,reviewId,reconcile=false}) {
    const checked=state.verify(state.record(validId(reviewId,'review ID')));
    actor(token);permit(token,checked.policy.institution,'read');
    const pending=reconcile?downstream.reconcile(state,reviewId,clock):
      checked.effects.filter(effect=>!effect.admitted_at).map(effect=>effect.revision);
    const row=state.record(reviewId);
    return {id:row.id,experience:row.experience,institution:row.institution,
      state:row.state,revision:row.revision,authorUserId:row.author_user,
      assignedUserId:row.assigned_user,source:{uri:row.source_uri,sha256:row.source_sha256},
      history:state.history(reviewId).map(e=>({revision:e.revision,action:e.action,
        state:e.state,actorUserId:e.actor_user,assignedUserId:e.assigned_user,
        reason:e.reason,at:e.at,sha256:e.digest,artifact:e.artifact_uri})),
      downstream:state.outbox(reviewId).map(e=>({revision:e.revision,mission:e.mission,
        family:e.family,admitted:Boolean(e.admitted_at)})),pending};
  }
  function submit({token,experience,reviewId,sourceUri,sourceSha256,requestKey}) {
    validId(reviewId,'review ID');validId(requestKey,'request key');
    const policy=authority.policies.get(validId(experience,'experience'));
    if(!policy)throw refuse(404,'This experience has no declared native review owner');
    const principal=actor(token);permit(token,policy.institution,'write');
    authority.source(policy,sourceUri,sourceSha256);
    const intent=intentDigest(['submit',experience,reviewId,sourceUri,sourceSha256,principal.userId]);
    state.transaction(()=>{
      const existing=state.record(reviewId);
      if(existing){
        state.verify(existing);
        const first=state.history(reviewId)[0];
        if(existing.experience!==experience||existing.author_user!==principal.userId||
          first.request_key!==requestKey||first.intent_digest!==intent)
          throw refuse(409,'Review ID is already bound to different material');
        return;
      }
      permit(token,policy.institution,'write');
      authority.source(policy,sourceUri,sourceSha256);
      state.create(reviewId,policy,principal,sourceUri,sourceSha256,clock());
      state.append(state.record(reviewId),policy,principal,'submit','pending',null,'',
        requestKey,intent,sourceUri,sourceSha256,clock());
    });
    return view({token,reviewId,reconcile:true});
  }
  function decide({token,reviewId,action,expectedRevision,reason='',
    targetEmail=null,sourceUri=null,sourceSha256=null,requestKey}) {
    validId(reviewId,'review ID');validId(requestKey,'request key');
    if(!ACTIONS.has(action)||!Number.isSafeInteger(expectedRevision)||expectedRevision<1)
      throw refuse(400,'Use a supported action and current review revision');
    reason=validReason(reason);
    if(NEEDS_REASON.has(action)&&!reason)throw refuse(400,'An explanation is required');
    const principal=actor(token),first=state.verify(state.record(reviewId));
    permit(token,first.policy.institution,'write');
    let assigned=null;
    if(action==='delegate'){
      permit(token,first.policy.institution,'admin');
      assigned=authority.delegate(token,first.policy,targetEmail);
      if(assigned===null)throw refuse(403,'Target has no current native reviewer grant');
    }else if(targetEmail!==null)throw refuse(400,'Only delegation may name a reviewer');
    if(action==='resubmit'){
      if(typeof sourceUri!=='string')throw refuse(400,'Supply the revised source');
      authority.source(first.policy,sourceUri,sourceSha256);
    }else if(sourceUri!==null||sourceSha256!==null)
      throw refuse(400,'Only the author resubmission may change the source');
    const intent=intentDigest([action,expectedRevision,reason,assigned,sourceUri,sourceSha256,principal.userId]);
    state.transaction(()=>{
      const {row,policy,records}=state.verify(state.record(reviewId));
      if(row.revision!==expectedRevision){
        const replay=records.find(e=>e.request_key===requestKey);
        if(replay&&replay.intent_digest===intent&&replay.revision===row.revision&&
          replay.actor_user===principal.userId)return;
        throw refuse(409,'A newer review revision must be read before acting');
      }
      if(row.state==='approved'||row.state==='rejected')throw refuse(409,'Review is already final');
      const fresh=actor(token);
      if(fresh.userId!==principal.userId||fresh.actor!==principal.actor)
        throw refuse(403,'Actor binding changed during review');
      permit(token,policy.institution,action==='delegate'||
        row.state==='escalated'&&action!=='resubmit'?'admin':'write');
      if(action==='resubmit'){
        if(row.state!=='revision_requested'||row.author_user!==principal.userId||
          row.source_sha256===sourceSha256)
          throw refuse(403,'Only its author can return changed work for review');
        authority.source(policy,sourceUri,sourceSha256);
        state.append(row,policy,principal,action,'pending',null,reason,
          requestKey,intent,sourceUri,sourceSha256,clock());
        return;
      }
      if(row.author_user===principal.userId)throw refuse(403,'Authors cannot decide their own work');
      if(row.state==='revision_requested')throw refuse(409,'The author must revise the work');
      if(row.assigned_user!==null&&row.assigned_user!==principal.userId&&action!=='delegate')
        throw refuse(403,'Review belongs to a different delegated reviewer');
      if(action==='delegate'&&(assigned===row.author_user||assigned===principal.userId||
        authority.delegate(token,policy,targetEmail)!==assigned))
        throw refuse(403,'The delegated reviewer is invalid or lost authority');
      state.append(row,policy,principal,action,NEXT[action],
        action==='delegate'?assigned:null,reason,requestKey,intent,
        row.source_uri,row.source_sha256,clock());
    });
    return view({token,reviewId,reconcile:true});
  }
  return Object.freeze({submit,decide,read:({token,reviewId})=>view({token,reviewId}),
    reconcile:({token,reviewId})=>view({token,reviewId,reconcile:true}),
    close:()=>state.close()});
}
