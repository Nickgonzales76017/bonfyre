// Compatibility entry point; shared upgrade journal and native owners.
import assert from 'node:assert/strict';
import {readWorkspaceFile} from '../workspace_material.mjs';
import {connectionWorkspaceMaterial,connectionModules,connectionPack} from '../materialize_connections_workspace.mjs';
import {reviewWorkspaceUpgrade,applyWorkspaceUpgrade,nativeWorkspaceUpgradeAdapter,parseWorkspaceLifecycle} from './workspace_upgrade.mjs';
export {upgradeJournal,workspaceEntrySnapshot as connectionEntrySnapshot} from './workspace_upgrade.mjs';
export const connectionUpgradeTarget=Object.freeze({pack:connectionPack,subject:'user:19',scope:'self',binding:'ws-c50141659f5365ba76eed886c44635ed06791966',family:'connections',entry:'connections/'});
export const connectionUpgradeSpec=Object.freeze({
 id:'connection-studio-upgrade',schema:'bonfyre.connection-studio-upgrade.v1',target:connectionUpgradeTarget,
 sourcePaths:['scripts/release/connection_studio_upgrade.mjs','scripts/release/tests/connection_studio_upgrade.test.mjs','scripts/materialize_connections_workspace.mjs','scripts/materialize_workspace_package.mjs','scripts/workspace_material.mjs','scripts/workspace_continuation.mjs',...connectionModules.map(name=>'integrations/browser/'+name)],
 baseRoot:release=>release,
 versionFor:(candidate,base)=>({pack:connectionPack,version:'connections-'+candidate.slice(0,40),root:candidate,parent_root:base,changed:'Account sign-in card and provider continuation; package '+candidate,proof:'integrations/browser/tests/connection-studio-signin.browser.test.mjs; scripts/release/tests/connection_studio_upgrade.test.mjs',status:'published'}),
 materialize:connectionWorkspaceMaterial,
 verify:async({root,id})=>assert((await readWorkspaceFile(root,id,'connections/index.html')).toString().includes('../bridge/connection-studio.mjs'),'The package does not mount its retained Connection Studio.')
});
export const reviewConnectionStudioUpgrade=adapter=>reviewWorkspaceUpgrade(adapter,connectionUpgradeSpec);
export const applyConnectionStudioUpgrade=(adapter,candidate)=>applyWorkspaceUpgrade(adapter,candidate,connectionUpgradeSpec);
export const parseConnectionLifecycle=raw=>parseWorkspaceLifecycle(raw,connectionPack);
export const nativeConnectionUpgradeAdapter=(options={})=>nativeWorkspaceUpgradeAdapter({...options,spec:connectionUpgradeSpec});
if(import.meta.main){
 let adapter;try{const [mode,candidate,...extra]=process.argv.slice(2);assert(!extra.length&&['review','apply'].includes(mode)&&(mode==='review'?!candidate:/^[a-f0-9]{64}$/.test(candidate)),'Use review, then apply EXACT_REVIEWED_PACKAGE_SHA256.');
  adapter=nativeConnectionUpgradeAdapter();const result=mode==='review'?await reviewConnectionStudioUpgrade(adapter):await applyConnectionStudioUpgrade(adapter,candidate);console.log(JSON.stringify(result));
 }catch(error){console.log(JSON.stringify({state:'stopped',reason:error.message,next_step:'Keep the review and journal. Resolve the reported stage and rerun for native readback; do not edit hashes, delete receipts, or replay an unconfirmed write.'}));process.exitCode=1;}finally{adapter?.close();}
}
