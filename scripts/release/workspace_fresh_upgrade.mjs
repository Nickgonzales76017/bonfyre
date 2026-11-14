// A new native review after a *different* installed workspace changes.
 // Preserve previous immutable journals; never reuse a stale snapshot or replay
 // an unconfirmed write. The shared Pack lifecycle still owns every effect.
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {existsSync} from 'node:fs';
import {join} from 'node:path';
import {homedir} from 'node:os';
import {reviewWorkspaceUpgrade,applyWorkspaceUpgrade,nativeWorkspaceUpgradeAdapter} from './workspace_upgrade.mjs';
import {sourceLibraryKnownUpgradeSpec} from './source_library_known_upgrade.mjs';
import {financeCrmUpgradeSpec} from './crm_finance_workspace_upgrade.mjs';
const choices={source:sourceLibraryKnownUpgradeSpec,finance:financeCrmUpgradeSpec};
const sha=value=>createHash('sha256').update(JSON.stringify(value)).digest('hex');
const steps=['version','head','demand','install','activate'];
function noUncertainOldJournal(spec,candidate){
 const dir=join(homedir(),'.bonfyre/release-handoff',spec.id,candidate);
 for(const step of steps)
  if(existsSync(join(dir,step+'-intent.json'))&&!existsSync(join(dir,step+'-done.json')))
   throw Error('Older '+step+' dispatch has no confirmed readback. Reconcile it before another review.');
}
export async function freshUpgrade(mode,which,candidate){
 assert(['review','advance','apply'].includes(mode)&&choices[which],
  'Choose review|advance|apply source|finance.');
 const original=choices[which];
 const preflight=nativeWorkspaceUpgradeAdapter({spec:original});
 let baseline;
 try{baseline=await preflight.inspect({});}finally{preflight.close();}
 const snapshot={
  binding:baseline.binding.release,
  installation:baseline.installationState.revision,
  stable:baseline.head.version,
  peer:baseline.preserved.otherBindings,
  definitions:baseline.preserved.definitions,
  records:baseline.preserved.records,
  owner:baseline.preserved.owner
 };
 const id=original.id+'-state-'+sha(snapshot).slice(0,14);
 assert(id.length<=79,'Review journal identity exceeds native contract.');
 const spec=Object.freeze({...original,id,
  sourcePaths:[...new Set([...original.sourcePaths,'scripts/release/workspace_fresh_upgrade.mjs'])]});
 const adapter=nativeWorkspaceUpgradeAdapter({spec});
 try{
  const material=await adapter.materialize();
  noUncertainOldJournal(original,material.id);
  if(mode==='apply'){
   assert(/^[a-f0-9]{64}$/.test(candidate||'')&&candidate===material.id,
    'Apply exactly the reviewed immutable package.');
   return await applyWorkspaceUpgrade(adapter,candidate,spec);
  }
  assert(!candidate,'Review does not accept a caller-chosen package.');
  const reviewed=await reviewWorkspaceUpgrade(adapter,spec);
  if(mode==='review'||reviewed.state==='already_verified')return reviewed;
  return await applyWorkspaceUpgrade(adapter,reviewed.candidate,spec);
 }finally{adapter.close();}
}
if(import.meta.main){
 const [mode,which,candidate,...extra]=process.argv.slice(2);
 try{assert(!extra.length);console.log(JSON.stringify(await freshUpgrade(mode,which,candidate)));}
 catch(error){console.log(JSON.stringify({state:'stopped',reason:error.message,
   next_step:'Preserve all earlier journals. Reconcile native state, then take an exact fresh review.'}));process.exitCode=1;}
}
