import {test,expect} from 'bun:test';
import {mkdtempSync,rmSync,readFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {knownSourcePage,prepareLink,checkSource,recipeCorpusResults} from '../../../cmd/BonfyreSourceFrontier/tools/source_material.mjs';
import {materializeKnownSourceLibrary,sourceLibraryKnownUpgradeSpec} from '../source_library_known_upgrade.mjs';
import {readWorkspaceFile,verifyWorkspacePackage} from '../../workspace_material.mjs';
const generation='foreign-v1-bounded';
const row=(ref='feed-work')=>({url:'https://publisher.example/feed',release:{source_contract:{source:ref,label:'Publisher observation',family:'news'},stages:{archive:{items:12}}}});
const packet=(twins=[row()])=>({schema:'bonfyre.foreign.twins.v2',state:'current',catalogue_generation:generation,total:twins.length,next_offset:null,twins});

test('known-source page is a typed, bounded review-only projection',()=>{
 const page=knownSourcePage(packet());
 expect(page).toEqual({generation,total:1,next:null,rows:[{sourceRef:'feed-work',url:'https://publisher.example/feed',title:'Publisher observation',family:'news',archived:12}]});
 for(const corrupt of [
  {...packet(),state:'snapshot_changed'}, {...packet(),catalogue_generation:'x/?'},
  {...packet(),twins:[row(),row()]}, {...packet(),twins:[{...row(),url:'http://localhost/private'}]},
  {...packet(),twins:[{...row(),url:'https://publisher.example/?access_token=secret'}]},
  {...packet(),twins:[] ,next_offset:-1}
 ])expect(()=>knownSourcePage(corrupt)).toThrow();
});
test('known URL can only become an ordinary reviewed address, not verified content',async()=>{
 const prepared=await prepareLink('https://publisher.example/feed',{title:'Publisher observation',collection:'news',tags:['news']});
 expect(prepared.expectedRevision).toBe(0);expect(checkSource(prepared.value).kind).toBe('link');
 expect(prepared.value.original).toBeUndefined();expect(prepared.value.observed).toBeUndefined();
 expect(prepared.key).toMatch(/^source-material:[a-f0-9]{64}$/);
});
test('source library package is content-addressed and adds a non-persistent catalogue lane',async()=>{
 const root=mkdtempSync(join(tmpdir(),'bonfyre-known-library-'));
 try{
  const first=await materializeKnownSourceLibrary({root}),second=await materializeKnownSourceLibrary({root});
  expect(first.id).toMatch(/^[a-f0-9]{64}$/);expect(second.id).toBe(first.id);
  const verified=await verifyWorkspacePackage(root,first.id);expect(verified.files).toBeGreaterThan(8);
  await sourceLibraryKnownUpgradeSpec.verify({root,id:first.id});
  const html=(await readWorkspaceFile(root,first.id,'sources/index.html')).toString();
  expect(html).toContain('src="./known-sources.mjs"');
  expect(html).toContain('src="./library.mjs"');
  const script=(await readWorkspaceFile(root,first.id,'sources/known-sources.mjs')).toString();
  expect(script).toContain('Save reviewed bookmark');
  expect(script).toContain('saveLocal(change.key,change.value,0)');
  expect(script).not.toContain('saveLocalBatch(');
  const provenance=(await readWorkspaceFile(root,first.id,'SOURCE.tsv')).toString();
  expect(provenance).toContain('integrations/browser/source-library-known.mjs');
  expect(readFileSync(join(root,first.id,'MANIFEST.tsv'),'utf8')).toContain('sources/known-sources.mjs');
 }finally{rmSync(root,{recursive:true,force:true});}
});
test('native recipe search carrier preserves exact bounded result identity',()=>{
 const data={query:'pasta',count:1,hits:[{key:'ffts-lg33961.31000kz-0700-f61d99d1df3da63f',title:'Retained pasta preparation'}]};
 const packet={jsonrpc:'2.0',result:{skill:'recipe-search',execution:'free',data}};
 expect(recipeCorpusResults(packet,'pasta')).toEqual(data.hits);
 for(const bad of [
  {...packet,result:{...packet.result,execution:'paid'}},
  {...packet,result:{...packet.result,data:{...data,query:'other'}}},
  {...packet,result:{...packet.result,data:{...data,hits:[...data.hits,...data.hits],count:2}}},
  {...packet,result:{...packet.result,data:{...data,hits:[{key:'../etc/passwd',title:'Bad key'}]}}}
 ])expect(()=>recipeCorpusResults(bad,'pasta')).toThrow();
});
