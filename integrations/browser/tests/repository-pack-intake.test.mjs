import {test,expect} from 'bun:test';
import {createHash} from 'node:crypto';
import {readRepositoryCutReceipt} from '../repository-cuts.mjs';
import {repositoryCutPackDemand,repositoryCutPackDemands} from '../repository-pack-intake.mjs';
const sha=x=>createHash('sha256').update(x).digest('hex');
const commit='a'.repeat(40),tree='b'.repeat(40);
function receipt(filter=':/notes'){
 const digest=sha(['bonfyre.repository-cut.v1','forgejo','aurekai','example','HEAD',
  filter==='none'?'':filter,commit,tree,''].join('\n'));
 return ['schema: bonfyre.repository-cut.v1','source_provider: forgejo',
  'source_org: aurekai','source_repository: example','requested_ref: HEAD',
  'josh_filter: '+filter,'virtual_commit: '+commit,'virtual_tree: '+tree,
  'cut_digest: '+digest,'canonical_mapping: '+(filter==='none'?'direct-git':'unverified'),
  'pack_root: uncompiled','install_authority: none','publication_authority: none',''].join('\n');
}
test('native source receipt projects a non-authoritative canonical demand draft',async()=>{
 const cut=await readRepositoryCutReceipt(receipt());
 const x=repositoryCutPackDemand(cut,{pack:'existing-pack'});
 expect(x.content_type).toBe('pack_demand');
 expect(x.attributes).toMatchObject({pack:'existing-pack',status:'draft',demand_kind:'source',
  needs:'repository-cut:'+cut.cutDigest,preferred_sources:'repository-cut:'+cut.cutDigest});
 expect(x.source).toMatchObject({virtual_commit:commit,virtual_tree:tree,canonical_mapping:'unverified'});
 expect(x.compiled_pack_root).toBeNull();
 expect(x.pack_existence_verified).toBe(false);
 expect(x.source_demand_published).toBe(false);
 expect([x.source_bytes_verified,x.license_verified,x.install_authorized,
  x.activation_authorized,x.publication_authorized]).toEqual([false,false,false,false,false]);
});
test('deduplicates cuts and preserves JOSH mapping distinction',async()=>{
 const a=await readRepositoryCutReceipt(receipt()),b=await readRepositoryCutReceipt(receipt('none'));
 const rows=repositoryCutPackDemands([a,b],{pack:'existing-pack'});
 expect(rows.map(x=>x.source.canonical_mapping)).toEqual(['unverified','direct-git']);
 expect(()=>repositoryCutPackDemands([a,a],{pack:'existing-pack'})).toThrow();
});
test('rejects forged install authority and malformed coordinates',async()=>{
 const x=await readRepositoryCutReceipt(receipt());
 expect(()=>repositoryCutPackDemand({...x,installAuthorized:true},{pack:'existing-pack'})).toThrow();
 expect(()=>repositoryCutPackDemand({...x,virtualTree:'fake'},{pack:'existing-pack'})).toThrow();
 expect(()=>repositoryCutPackDemand(x,{pack:'../bad'})).toThrow();
});

test('published native PackImage source admission expects exactly the same preferred_sources selector',async()=>{
 const cut=await readRepositoryCutReceipt(receipt());
 const proposal=repositoryCutPackDemand(cut,{pack:'studio'});
 const {readFileSync}=await import('node:fs');
 const {resolve}=await import('node:path');
 const source=readFileSync(resolve(import.meta.dir,'../../../cmd/BonfyreCMS/src/pack_image.h'),'utf8');
 const witness=readFileSync(resolve(import.meta.dir,'../../../cmd/BonfyreCMS/tests/pack_image_test.sh'),'utf8');
 expect(source).toContain('"repository-cut:%s"');
 expect(source).toContain("AND pack=?1 AND demand_kind='source' AND preferred_sources=?2");
 expect(witness).toContain('preferred_sources');
 expect(proposal.attributes).toMatchObject({
  preferred_sources:'repository-cut:'+cut.cutDigest,
  status:'draft'
 });
 expect(proposal.source.canonical_mapping).toBe('unverified');
 expect(proposal.install_authorized).toBe(false);
});
