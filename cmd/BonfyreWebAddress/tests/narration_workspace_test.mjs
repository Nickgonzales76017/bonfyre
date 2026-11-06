import {test,expect,beforeAll,afterAll} from 'bun:test';
import {copyFileSync,mkdtempSync,rmSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {materializeCreatorWorkspace} from '../../../scripts/materialize_creator_workspace.mjs';
import {workspaceFixture,root} from './workspace_fixture.mjs';
import {projectCreatorWorkspaceSetup,publishSetups} from '../../../scripts/publish_experience_setups.mjs';
import {encode,decode} from '../../../integrations/browser/workspace-msgpack.mjs';
import {createProduction,reviewScripts,renderRequests,acceptTake,mixerSession} from '../../../integrations/browser/narration-production.mjs';
import {decodeWav} from '../../../components/BonfyreServiceSite/creator-import.mjs';
let f,binding,packageRoot;
const prior={...process.env};
beforeAll(async()=>{
 packageRoot=mkdtempSync(join(tmpdir(),'aurekai-narration-packages-'));
 const {id:release}=await materializeCreatorWorkspace({root:packageRoot});
 process.env.BONFYRE_WORKSPACE_PACKAGES=packageRoot;
 process.env.BONFYRE_WORKSPACE_RELEASE=release;
 process.env.BONFYRE_NARRATE_BINARY=join(root,'cmd/BonfyreNarrate/bonfyre-narrate');
 f=await workspaceFixture();f.pack('experience.native-creator-studio','Audio Studio');
 publishSetups({records:projectCreatorWorkspaceSetup(release),binary:f.cms,db:f.db,schemas:join(f.home,'schemas'),apply:true});
 for(const type of ['publication_occurrence','internet_source'])copyFileSync(join(root,'cmd/BonfyreCMS/content-types',type+'.json'),join(f.home,'schemas',type+'.json'));
 f.run(f.cms,['schema','migrate','--db',f.db,'--schemas',join(f.home,'schemas')]);
 binding=await f.install('experience.native-creator-studio.workspace','institution:workspace-team');
},60000);
afterAll(async()=>{await f?.close();if(packageRoot)rmSync(packageRoot,{recursive:true,force:true});for(const key of ['BONFYRE_WORKSPACE_RELEASE','BONFYRE_NARRATE_BINARY','BONFYRE_WORKSPACE_PACKAGES'])if(prior[key]===undefined)delete process.env[key];else process.env[key]=prior[key];});
const endpoint=()=>'/apps/'+binding.id+'/_narrate';
function render(user,packet=[1,'A real recording for an editable production.\n\nA second reviewed paragraph stays in the same narration request.','Samantha',180],headers={}){
 return f.raw(endpoint(),user,{method:'POST',headers:{'Content-Type':'application/msgpack',...headers},body:encode(packet)});
}
test('read access and cross-origin requests cannot invoke narration',async()=>{
 expect((await render(f.reader)).status).toBe(403);
 expect((await render(f.other)).status).toBe(403);
 expect((await render(null)).status).toBe(403);
 expect((await render(f.owner,undefined,{Origin:'https://unrelated.example'})).status).toBe(403);
 expect((await render(f.owner,[1,'Text','Samantha',301])).status).toBe(400);
 expect((await render(f.owner,[1,'Text','../voice',180])).status).toBe(400);
 expect((await f.raw(endpoint())).status).toBe(405);
},20000);
test('a reviewed passage renders real audio, persists through CMS, and prepares measured mixer input',async()=>{
 let p=createProduction({id:'native-edition',title:'Native production',defaults:{voice:'Samantha'},
   items:[{id:'source-1',title:'Authored test passage',source:{href:'https://example.test/authored',sha256:'a'.repeat(64)},
   segments:[{id:'passage-1',text:'A real recording for an editable production.'}]}]});
 p=await reviewScripts(p,['passage-1@en']);const [request]=await renderRequests(p,['passage-1@en']);
 const response=await render(f.owner,[1,request.script,request.voice,request.rate]);
 expect(response.status).toBe(200);expect(response.headers.get('content-type')).toBe('audio/wav');
 expect(response.headers.get('cache-control')).toBe('private, no-store');
 const bytes=await response.arrayBuffer(),audio=decodeWav(bytes);
 expect(audio.frames).toBeGreaterThan(22050);
 expect(audio.samples.some(n=>Math.abs(n)>.005)).toBe(true);
 p=await acceptTake(p,request,{id:'take-1',bytes,decode:decodeWav});
 const store=f.store(binding),key='draft:creator:production:native-edition';
 const row=await store.saveLocal(key,p,0);
 const reopened=await f.store(binding,f.writer).readVersion(key);
 expect(reopened.revision).toBe(row.revision);
 expect(reopened.value.cells[0].takes[0].bytes).toEqual(bytes);
 const session=await mixerSession(reopened.value,'en');
 expect(session.measured.frames).toBe(audio.frames);
 expect(session.clips[0].frames).toBe(audio.frames);
 expect(session.files[0].bytes).toEqual(bytes);
 const personal=await f.install('experience.native-creator-studio.workspace','self');
 expect(await f.store(personal).readLocal('draft:creator:production:')).toEqual([]);
},60000);

test('source selection retains public observed excerpts and receipts with bounded continuation',async()=>{
 f.add('internet_source',{source:'fixture-feed',family:'rss',label:'Fixture feed',says:'Fixture publisher',state:'observed',kind:'rss',url:'https://example.test/feed',status:'published'});
 const add=(i,extra={})=>f.add('publication_occurrence',{occurrence_id:'fixture-'+i,source_twin:'twin:fixture',source_ref:'fixture-feed',canonical_href:'https://example.test/'+i,title:'Production passage '+i,summary:'Retained source excerpt '+i,observed_at:'2026-10-20',receipt_ref:'receipt:'+i,rights_basis:'Observed public excerpt; reuse requires review',retention:'excerpt',visibility:'public',assertion_state:'OBSERVED',cannot_infer:'Not publication permission.',status:'published',...extra});
 for(let i=0;i<26;i++)add(i);
 add('draft',{status:'draft'});add('private',{visibility:'private'});add('unobserved',{assertion_state:'UNVERIFIED'});add('unbound',{source_ref:'absent'});
 const route='/apps/'+binding.id+'/_publication_inputs';
 const first=await f.raw(route+'?q=Production',f.reader);expect(first.status).toBe(200);
 const packet=decode(new Uint8Array(await first.arrayBuffer()));expect(packet[1]).toHaveLength(24);expect(packet[2]).toBeGreaterThan(0);
 expect(packet[1][0].slice(0,6)).toEqual(['fixture-25','Production passage 25','Retained source excerpt 25','https://example.test/25','fixture-feed','receipt:25']);
 const second=await f.raw(route+'?q=Production&cursor='+packet[2],f.reader),rest=decode(new Uint8Array(await second.arrayBuffer()));
 expect(rest[1]).toHaveLength(2);expect(rest[2]).toBe(0);
 expect((await f.raw(route+'?q=x&q=y')).status).toBe(400);
 expect((await f.raw(route+'?cursor=-1')).status).toBe(400);
 expect((await f.raw(route,f.other)).status).toBe(403);
 expect((await f.raw(route,f.owner,{method:'POST'})).status).toBe(405);
 const empty=await f.raw(route+'?q=not-found');expect(decode(new Uint8Array(await empty.arrayBuffer()))).toEqual([1,[],0]);
});
