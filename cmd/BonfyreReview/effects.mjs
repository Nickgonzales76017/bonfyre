import { join } from 'node:path';

// Queue is an injected, existing native WorkGraph owner, not an alternative
// simulated implementation. The review table remains the durable outbox.
export function reviewEffects(home,queue) {
  if(typeof queue!=='function')throw Error('Native Queue admission owner is required');
  const db=join(home,'work.db');
  function reconcile(store,review,clock=()=>new Date().toISOString()) {
    const current=store.verify(store.record(review)),pending=[];
    for(const effect of current.effects) {
      if(effect.admitted_at)continue;
      try {
        queue(['enqueue','review-decision',effect.input_uri,
          '--mission',effect.mission,'--family',effect.family,'--if-absent','--retries','3'],db);
        const node=queue(['mission',effect.mission],db);
        if(node.node_id!=='job'||node.operator_id!=='queue.review-decision'||
          node.family!==effect.family||node.input_uri!==effect.input_uri)
          throw Error('Native mission identity changed');
        store.markAdmitted(review,effect.revision,clock());
      }catch{pending.push(effect.revision);}
    }
    return pending;
  }
  return Object.freeze({reconcile,db});
}
