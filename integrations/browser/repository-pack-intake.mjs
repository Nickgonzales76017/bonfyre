// Read-only RepositoryFabric -> canonical BonfyreCMS pack_demand proposal.
// A self-consistent Git cut does not prove source rights, bytes, or installation.
const hex=/^[0-9a-f]{64}$/;
const oid=/^(?:[0-9a-f]{40}|[0-9a-f]{64})$/;
const packId=/^[a-z0-9][a-z0-9._-]{0,94}$/;
const id=/^[A-Za-z0-9][A-Za-z0-9._-]{0,94}$/;
export function repositoryCutPackDemand(cut,{pack}={}) {
 if(!packId.test(pack||''))throw Error('Choose an existing canonical pack identifier.');
 if(cut?.kind!=='repository_cut'||cut.state!=='receipt_self_consistent'||
    cut.sourceOwner!=='BonfyreRepositoryFabric'||cut.provider!=='forgejo'||
    !id.test(cut.org||'')||!id.test(cut.repository||'')||
    !oid.test(cut.virtualCommit||'')||!oid.test(cut.virtualTree||'')||
    !hex.test(cut.cutDigest||'')||
    !((cut.filter==='none')||(typeof cut.filter==='string'&&cut.filter.startsWith(':')&&!cut.filter.includes('..')&&/^:[A-Za-z0-9_.\/:=,\-]{1,199}$/.test(cut.filter)))||
    cut.canonicalMapping!==(cut.filter==='none'?'direct-git':'unverified')||
    cut.dataBytesVerified!==false||cut.remoteRepositoryVerified!==false||
    cut.sourceReadAuthorized!==false||cut.installAuthorized!==false||
    cut.publicationAuthorized!==false)
   throw Error('Refuse unverified or authority-claiming source cut.');
 if(typeof cut.requestedRef!=='string'||!/^[A-Za-z0-9][A-Za-z0-9._/-]{0,159}$/.test(cut.requestedRef)||cut.requestedRef.includes('..'))
   throw Error('Invalid source reference.');
 const source=Object.freeze({provider:cut.provider,organization:cut.org,repository:cut.repository,
   requested_ref:cut.requestedRef,josh_filter:cut.filter,
   virtual_commit:cut.virtualCommit,virtual_tree:cut.virtualTree,
   source_cut_digest:cut.cutDigest,canonical_mapping:cut.canonicalMapping});
 const attributes=Object.freeze({pack,demand_kind:'source',
   needs:'repository-cut:'+cut.cutDigest,preferred_sources:'repository-cut:'+cut.cutDigest,
   fields:'BonfyreRepositoryFabric/'+cut.cutDigest,reheat_when:'source-revision-review',
   optional:'false',status:'draft'});
 return Object.freeze({schema:'bonfyre.repository-pack-demand-proposal.v1',
   owner:'BonfyreCMS/PackGraph',content_type:'pack_demand',namespace:'root',
   source,attributes,readiness:'review_required',
   source_bytes_verified:false,license_verified:false,compiled_pack_root:null,
   pack_version_created:false,pack_existence_verified:false,source_demand_published:false,
   install_authorized:false,activation_authorized:false,
   publication_authorized:false,
   cannot_infer:'A repository cut is not compiled PackIR, a canonical JOSH mapping, a license or authority to install and publish.'});
}
export function repositoryCutPackDemands(cuts,options={}) {
 if(!Array.isArray(cuts)||!cuts.length||cuts.length>24)throw Error('Select 1–24 repository cuts.');
 const seen=new Set();
 return Object.freeze(cuts.map(cut=>{
  const row=repositoryCutPackDemand(cut,options);
  if(seen.has(row.source.source_cut_digest))throw Error('Duplicate source cut.');
  seen.add(row.source.source_cut_digest);return row;
 }));
}
