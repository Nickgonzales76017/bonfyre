import {test,expect} from 'bun:test';
import {createHash} from 'node:crypto';
import {warehouseInstalledTools} from '../warehouse-portfolio.mjs';
import {prepareRepositoryPackReview} from '../repository-pack-review.mjs';

const sha=s=>createHash('sha256').update(s).digest('hex');
function receipt(filter=':/notes'){
  const commit='a'.repeat(40),tree='b'.repeat(40);
  const digest=sha(['bonfyre.repository-cut.v1','forgejo','aurekai',
    'repository-fabric-proof','HEAD',filter==='none'?'':filter,commit,tree,''].join('\n'));
  return ['schema: bonfyre.repository-cut.v1','source_provider: forgejo',
    'source_org: aurekai','source_repository: repository-fabric-proof',
    'requested_ref: HEAD','josh_filter: '+filter,'virtual_commit: '+commit,
    'virtual_tree: '+tree,'cut_digest: '+digest,
    'canonical_mapping: '+(filter==='none'?'direct-git':'unverified'),
    'pack_root: uncompiled','install_authority: none',
    'publication_authority: none',''].join('\n');
}
function tools(){
 const packs=['experience.source-library.workspace','experience.connected-instruments.workspace'];
 return warehouseInstalledTools([1,'user:19','self',packs.map((pack,i)=>
    ['ws-'+String(i).repeat(40),pack,'sources/','c'.repeat(64),pack,true])],'self');
}
test('exact receipt proposes PackGraph demand with installed account continuations',async()=>{
 const review=await prepareRepositoryPackReview(receipt(),tools(),{roles:['sources','research']});
 expect(review).toMatchObject({state:'review_only',scope:'self',
   receiving_pack:'experience.source-library.workspace',
   provider_connected:false,pack_compiled:false,publication_authorized:false});
 expect(review.demand.attributes).toMatchObject({demand_kind:'source',status:'draft'});
 expect(review.demand.source.canonical_mapping).toBe('unverified');
 expect(review.continuations.some(x=>x.kind==='workspace'&&x.state==='ready')).toBe(true);
});
test('uninstalled pack, tampered digest, and forged authority cannot become reviewable packs',async()=>{
 await expect(prepareRepositoryPackReview(receipt(),tools(),
  {pack:'experience.uninstalled'})).rejects.toThrow('not installed');
 await expect(prepareRepositoryPackReview(receipt().replace('virtual_tree: '+ 'b'.repeat(40),
  'virtual_tree: '+ 'f'.repeat(40)),tools())).rejects.toThrow();
 await expect(prepareRepositoryPackReview(receipt().replace('install_authority: none',
  'install_authority: admin'),tools())).rejects.toThrow();
});
test('different native cuts retain distinct digests and canonical mapping states',async()=>{
 const a=await prepareRepositoryPackReview(receipt(),tools());
 const b=await prepareRepositoryPackReview(receipt('none'),tools());
 expect(a.source_cut_digest).not.toBe(b.source_cut_digest);
 expect(b.demand.source.canonical_mapping).toBe('direct-git');
 expect(a.downstream_effect_authorized).toBe(false);
});
