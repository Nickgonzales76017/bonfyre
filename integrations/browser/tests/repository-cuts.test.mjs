import {test,expect} from 'bun:test';
import {createHash} from 'node:crypto';
import {readRepositoryCutReceipt,readRepositoryCutCollection,repositoryCutContinuations}
 from '../repository-cuts.mjs';
import {warehouseInstalledTools} from '../warehouse-portfolio.mjs';

const sha=b=>createHash('sha256').update(b).digest('hex');
const commit='a'.repeat(40),tree='b'.repeat(40);
function cut({org='aurekai',repo='repository-fabric-proof',ref='HEAD',
 filter=':/notes',canonical=filter==='none'?'direct-git':'unverified',
 fields={},digest}={}){
 const values={schema:'bonfyre.repository-cut.v1',source_provider:'forgejo',
   source_org:org,source_repository:repo,requested_ref:ref,josh_filter:filter,
   virtual_commit:commit,virtual_tree:tree,
   cut_digest:digest??sha(['bonfyre.repository-cut.v1','forgejo',org,repo,ref,
     filter==='none'?'':filter,commit,tree,''].join('\n')),
   canonical_mapping:canonical,pack_root:'uncompiled',
   install_authority:'none',publication_authority:'none',...fields};
 return Object.entries(values).map(([k,v])=>k+': '+v+'\n').join('');
}
const install=(i,pack)=>['ws-'+i.repeat(40),pack,'editor/','c'.repeat(64),pack,true];
test('accepts and verifies the actual immutable JOSH source-cut digest',async()=>{
 const body=cut();
 const row=await readRepositoryCutReceipt(body);
 expect(row).toMatchObject({kind:'repository_cut',org:'aurekai',repository:'repository-fabric-proof',
   filter:':/notes',virtualCommit:commit,virtualTree:tree,canonicalMapping:'unverified',
   remoteRepositoryVerified:false,sourceReadAuthorized:false,
   installAuthorized:false,publicationAuthorized:false});
 expect(row.cutDigest).toBe(sha(['bonfyre.repository-cut.v1','forgejo','aurekai',
   'repository-fabric-proof','HEAD',':/notes',commit,tree,''].join('\n')));
 expect(Object.isFrozen(row)).toBe(true);
});
test('plain Git receipts remain direct-git; JOSH cannot claim a canonical mapping',async()=>{
 const plain=await readRepositoryCutReceipt(cut({filter:'none'}));
 expect(plain.filter).toBe('none');
 expect(plain.canonicalMapping).toBe('direct-git');
 await expect(readRepositoryCutReceipt(cut({canonical:'direct-git'}))).rejects.toThrow();
 await expect(readRepositoryCutReceipt(cut({filter:'none',canonical:'unverified'}))).rejects.toThrow();
});
test('digest tampering, hostile filters, forged powers and extra fields fail closed',async()=>{
 for(const body of [
  cut({fields:{virtual_tree:'f'.repeat(40)}}),
  cut({fields:{pack_root:'claimed'}}),
  cut({fields:{install_authority:'admin'}}),
  cut({fields:{publication_authority:'granted'}}),
  cut({fields:{source_provider:'github'}}),
  cut({filter:'--upload-pack'}),
  cut({filter:':/notes;sh'}),
  cut({filter:':/../../private'}),
  cut({ref:'refs/../../HEAD'}),
  cut({digest:'0'.repeat(64)}),
  cut()+'token: stolen\\n',
  cut().replace('source_org: aurekai'+String.fromCharCode(10),
    'source_org: aurekai'+String.fromCharCode(10)+'source_org: another'+String.fromCharCode(10))
 ])await expect(readRepositoryCutReceipt(body)).rejects.toThrow();
});
test('multiple cuts dedupe and do not merge different virtual trees',async()=>{
 const a=cut({filter:':/notes'}),b=cut({filter:'none'});
 const rows=await readRepositoryCutCollection([a,b]);
 expect(rows).toHaveLength(2);
 expect(new Set(rows.map(r=>r.cutDigest)).size).toBe(2);
 await expect(readRepositoryCutCollection([a,a])).rejects.toThrow();
});
test('source cut opens 11 installed owner families without inferring Git or data authority',async()=>{
 const packs=['experience.source-library.workspace','experience.connected-instruments.workspace',
  'experience.skill-instruments.workspace','experience.finance-workbooks.workspace',
  'experience.native-creator-studio.workspace','experience.multi-merchant-catalogue.workspace',
  'experience.retail-procurement.workspace','experience.shipment-archive.workspace',
  'experience.source-programs.workspace','experience.source-campaign.workspace',
  'experience.connection-studio.workspace'];
 const tools=warehouseInstalledTools([1,'user:19','self',
   packs.map((pack,i)=>install('0123456789a'[i],pack))],'self');
 const row=await readRepositoryCutReceipt(cut());
 const choices=repositoryCutContinuations(row,tools,{roles:[
   'sources','research','analysis','finance','connections','programs','campaigns',
   'commerce','creator','logistics'
 ]});
 const ready=new Set(choices.filter(x=>x.kind==='workspace'&&x.state==='ready').map(x=>x.pack));
 expect(ready.size).toBe(10);
 expect(choices.some(x=>x.kind==='account'&&x.label==='Connections')).toBe(true);
 expect(choices.every(x=>!x.dataTransferAuthorized&&!x.queryExecuted&&!x.recordCreated&&
   !x.sourceReadAuthorized&&!x.gitWriteAuthorized&&!x.sourceBytesVerified)).toBe(true);
 expect(new Set(choices.map(x=>x.sourceCoordinate.cutDigest))).toEqual(new Set([row.cutDigest]));
 await expect(readRepositoryCutReceipt(cut({digest:'1'.repeat(64)}))).rejects.toThrow();
});
