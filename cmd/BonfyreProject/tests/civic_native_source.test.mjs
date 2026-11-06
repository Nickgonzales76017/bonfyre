import {test,expect} from 'bun:test';
import {consumeNativeObservation,observeCivicSource,run} from '../tools/civic_sync.mjs';
import {observePublic} from '../../BonfyreSourceFrontier/tools/foreign_observation.mjs';
import {createHash} from 'node:crypto';
import {mkdtempSync,writeFileSync,readFileSync,rmSync,symlinkSync,unlinkSync,existsSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,resolve} from 'node:path';
const hash=bytes=>createHash('sha256').update(bytes).digest('hex');
const root=resolve(import.meta.dir,'../../..');
function fixture(){
  const dir=mkdtempSync(join(tmpdir(),'civic-source-reuse-')),bodyPath=join(dir,'source.body'),receiptPath=join(dir,'source.receipt');
  const bytes=Buffer.from('<html>Retained source</html>');writeFileSync(bodyPath,bytes);
  const source={source:'fixture-source',url:'https://fixture.invalid/retained'};
  const receipt={schema:'bonfyre-foreign-observation.v1',twin_id:source.source,source_url:source.url,
    observe_right:'public',policy_decision:'allow',observation_kind:'source_observed',http_status:200,
    content_sha256:hash(bytes),body_bytes:bytes.length,body_path:bodyPath,observed_at:'2026-10-15T12:00:00Z'};
  writeFileSync(receiptPath,JSON.stringify(receipt)+'\n');
  return {dir,source,bytes,observation:{receipt,bodyPath,receiptPath,exitCode:0},close:()=>rmSync(dir,{recursive:true,force:true})};
}
test('retained body and exact native receipt supply one parser input',()=>{
  const f=fixture();try{const r=consumeNativeObservation(f.observation,f.source);expect(r.bytes).toEqual(f.bytes);expect(r.digest).toBe(hash(f.bytes));expect(r.proof.receipt_path).toBe(f.observation.receiptPath);}finally{f.close();}
});
test('changed or missing retained bodies refuse instead of triggering another source read',()=>{
  const f=fixture();try{writeFileSync(f.observation.bodyPath,'changed');expect(()=>consumeNativeObservation(f.observation,f.source)).toThrow();unlinkSync(f.observation.bodyPath);expect(()=>consumeNativeObservation(f.observation,f.source)).toThrow();}finally{f.close();}
});
test('receipt redirects, samples, policy refusal and wrong source identity do not become parseable input',()=>{
  const f=fixture();try{for(const changed of [{body_path:'/tmp/unrelated'},{source_url:'https://other.invalid/'},{twin_id:'other'},{observation_kind:'sample_observed'},{policy_decision:'robots_disallow'},{body_bytes:999}])expect(()=>consumeNativeObservation({...f.observation,receipt:{...f.observation.receipt,...changed}},f.source)).toThrow();}finally{f.close();}
});
test('receipt and native exit must agree; a replacement receipt cannot silently validate earlier bytes',()=>{
  const f=fixture();try{expect(()=>consumeNativeObservation({...f.observation,exitCode:1},f.source)).toThrow();writeFileSync(f.observation.receiptPath,JSON.stringify({...f.observation.receipt,observed_at:'changed'})+'\n');expect(()=>consumeNativeObservation(f.observation,f.source)).toThrow('receipt changed');}finally{f.close();}
});
test('bounds and non-regular retained paths fail without a whole-file fallback',()=>{
  const f=fixture();try{expect(()=>consumeNativeObservation(f.observation,f.source,{limit:2})).toThrow();expect(()=>consumeNativeObservation(f.observation,f.source,{limit:0})).toThrow();const target=join(f.dir,'original');writeFileSync(target,f.bytes);unlinkSync(f.observation.bodyPath);symlinkSync(target,f.observation.bodyPath);expect(()=>consumeNativeObservation(f.observation,f.source)).toThrow();}finally{f.close();}
});
test('a valid 304 refers to the prior retained bytes, not a new empty body',()=>{
  const f=fixture();try{const receipt={...f.observation.receipt,http_status:304,observation_kind:'repeat_observed',body_bytes:0,conditional:true,unchanged_from_content_sha256:hash(f.bytes)};writeFileSync(f.observation.receiptPath,JSON.stringify(receipt)+'\n');const r=consumeNativeObservation({...f.observation,receipt,exitCode:3},f.source);expect(r.bytes).toEqual(f.bytes);expect(r.proof.revalidated).toBe(true);}finally{f.close();}
});
test('the shared observer rejects contradictory full/range contracts before executing a provider',async()=>{
  for(const options of [{fullMaxBytes:-1},{fullMaxBytes:12000001},{fullMaxBytes:100,sampleBytes:50},{fullMaxBytes:100,rangeStart:2}])await expect(observePublic('/missing-native-owner','/unused','fixture','https://fixture.invalid',options)).rejects.toThrow('bounded full');
});
test('actual native ForeignTwin observes one local fixture body and a denied source produces no body request',async()=>{
  const dir=mkdtempSync(join(tmpdir(),'civic-source-native-')),binary=join(root,'cmd/BonfyreProject/bonfyre-project');expect(existsSync(binary)).toBe(true);
  let denied=false;const requests=[];const text='<html><body>Native owner fixture</body></html>';
  const server=Bun.serve({hostname:'127.0.0.1',port:0,fetch(request){const p=new URL(request.url).pathname;requests.push(p);return p==='/robots.txt'?new Response(denied?'User-agent: *\nDisallow: /source\n':'User-agent: *\nAllow: /\n'):new Response(text,{headers:{'content-type':'text/html'}});}});
  const source={source:'native-read-fixture',url:'http://127.0.0.1:'+server.port+'/source'};
  try{const observation=await observeCivicSource(binary,source,{foreignRoot:join(dir,'foreign'),cmsDb:join(dir,'cms.db')});const parsed=consumeNativeObservation(observation,source);expect(parsed.bytes.toString()).toBe(text);expect(observation.receipt.observation_kind).toBe('source_observed');expect(requests).toEqual(['/robots.txt','/source']);denied=true;await expect(observeCivicSource(binary,source,{foreignRoot:join(dir,'foreign'),cmsDb:join(dir,'cms.db')})).rejects.toThrow();expect(requests).toEqual(['/robots.txt','/source','/robots.txt']);}finally{server.stop(true);rmSync(dir,{recursive:true,force:true});}
},20000);
test('apply mode reuses the shared adapter, performs one read per source and sends one native CMS batch',async()=>{
  // Provider process is a fixture; CMS commands and source parsing are real native integration.
  const dir=mkdtempSync(join(tmpdir(),'civic-apply-native-')),cms=join(root,'cmd/BonfyreCMS/bonfyre-cms'),schemas=join(root,'cmd/BonfyreCMS/content-types'),db=join(dir,'cms.db');
  const call=args=>{const r=Bun.spawnSync([cms,...args,'--db',db,'--schemas',schemas],{cwd:root,stdout:'pipe',stderr:'pipe',timeout:20000});if(r.exitCode!==0)throw Error(r.stderr.toString()||r.stdout.toString());return JSON.parse(r.stdout.toString());};
  const provider=join(dir,'fixture-provider'),calls=join(dir,'calls'),mode=join(dir,'mode');writeFileSync(mode,'original');
  const agendaSource={source:'dallas-council-20261015-legistar',family:'civic-meeting',url:'https://cityofdallas.legistar.com/MeetingDetail.aspx?ID=123'},videoSource={source:'dallas-council-20261015-swagit',family:'civic-meeting-video',url:'https://dallastx.new.swagit.com/videos/123'};
  const fixtureCode=`#!${process.execPath}\nimport {writeFileSync,appendFileSync,readFileSync} from 'node:fs';import {createHash} from 'node:crypto';const a=process.argv.slice(2),value=k=>a[a.indexOf(k)+1],twin=a[2],url=a[3];if(value('--full-max-bytes')!=='4000000')throw Error('full bound missing');appendFileSync(${JSON.stringify(calls)},url+'\\n');const title=readFileSync(${JSON.stringify(mode)},'utf8');const text=url.includes('legistar')?'<table id="gridMain_ctl00"><tr><td>26-2200A</td><td></td><td>1</td><td></td><td></td><td>'+title+'</td></tr></table>':'<a class="playerControl" data-ts="100" data-end-ts="200" data-title="26-2200A chapter" href="/play/123/100">Watch</a>';const body=value('--body'),receipt=value('--receipt');writeFileSync(body,text);writeFileSync(receipt,JSON.stringify({schema:'bonfyre-foreign-observation.v1',twin_id:twin,source_url:url,observe_right:'public',policy_decision:'allow',observation_kind:'source_observed',http_status:200,body_path:body,body_bytes:Buffer.byteLength(text),content_sha256:createHash('sha256').update(text).digest('hex'),observed_at:'2026-10-15T12:00:00Z'})+'\\n');\n`;
  writeFileSync(provider,fixtureCode,{mode:0o755});const originalFetch=globalThis.fetch;
  try{
    for(const name of ['internet_source','source_collection','publication_occurrence'])call(['schema','apply',join(schemas,name+'.json')]);
    for(const source of [agendaSource,videoSource])call(['entry','create','internet_source','--ns','civic-test',JSON.stringify({...source,label:'Fixture',says:'Fixture only',state:'observed',status:'published'})]);
    for(const id of ['fixture-one','fixture-two'])call(['entry','create','source_collection','--ns','civic-test',JSON.stringify({collection_id:id,subject_address:'fixture:'+id,source_selector:{source_refs:[agendaSource.source,videoSource.source]},material_kind:['civic'],refresh_policy:'manual',status:'published'})]);
    const args=['--apply','--cms-db',db,'--schemas',schemas,'--cms-bin',cms,'--project-bin',provider,'--state',join(dir,'state'),'--foreign-root',join(dir,'foreign'),'--ns','civic-test'];
    globalThis.fetch=()=>{throw Error('duplicate JavaScript provider fetch');};
    const first=await run(args);expect(first).toMatchObject({paired_meetings:1,applied:2,native_observation_calls:2,source_read_attempts:2});expect(first.cms_batch.imported).toBe(2);expect(readFileSync(calls,'utf8').trim().split('\n')).toHaveLength(2);
    const second=await run(args);expect(second.applied).toBe(0);expect(second.meetings[0]).toMatchObject({unchanged:2,changed:0,new_row_count:0});expect(readFileSync(calls,'utf8').trim().split('\n')).toHaveLength(4);
    writeFileSync(mode,'corrected');const third=await run(args);expect(third.applied).toBe(0);expect(third.meetings[0]).toMatchObject({unchanged:1,changed:1});expect(third.meetings[0].review[0].proposed_fields.title).toBe('26-2200A corrected');
    console.log(JSON.stringify({fixtureProvider:true,nativeCms:true,collections:2,pairedMeetings:1,sourceCallsPerRun:2,duplicateJavaScriptFetches:0,initialCreates:2,replayCreates:0,correctedRecordsHeld:1}));
  }finally{globalThis.fetch=originalFetch;rmSync(dir,{recursive:true,force:true});}
},20000);
