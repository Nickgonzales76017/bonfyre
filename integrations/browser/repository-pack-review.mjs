// RepositoryFabric and PackGraph remain separate owners. This is an account-scoped
// review projection only: it cannot retain bytes, publish or execute provider calls.
import {readRepositoryCutReceipt,repositoryCutContinuations} from './repository-cuts.mjs';
import {repositoryCutPackDemand} from './repository-pack-intake.mjs';

const permittedRoles=Object.freeze(['sources','research','analysis','finance',
  'connections','programs','campaigns','commerce','creator','logistics']);

export async function prepareRepositoryPackReview(receiptText,installedTools,{
  context='self',pack='experience.source-library.workspace',roles=permittedRoles
}={}){
  // Always re-verify original immutable YAFF before accepting a proposed PackGraph
  // demand. A caller-supplied object with a convincing digest shape is insufficient.
  const cut=await readRepositoryCutReceipt(receiptText);
  const proposal=repositoryCutPackDemand(cut,{pack});
  const choices=repositoryCutContinuations(cut,installedTools,{context,roles});
  const receiving=choices.find(row=>row.kind==='workspace'&&
    row.pack===pack&&row.state==='ready'&&row.workspace?.ready===true);
  if(!receiving)throw Error('The selected pack is not installed and ready in this account.');
  const continuations=Object.freeze(choices.map(choice=>Object.freeze({...choice})));
  return Object.freeze({
    schema:'bonfyre.repository-pack-review.v1',scope:context,
    state:'review_only',source_cut_digest:cut.cutDigest,
    demand:proposal,receiving_pack:pack,receiving_editor:receiving.href,
    continuations,provider_connected:false,source_bytes_retained:false,
    pack_compiled:false,pack_installed_by_review:false,
    downstream_effect_authorized:false,publication_authorized:false
  });
}
