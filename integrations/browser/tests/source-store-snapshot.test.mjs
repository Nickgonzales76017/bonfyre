// Exercise the shipped facade, substituting only its native import with a transport probe.
import {test,expect} from 'bun:test';
import {readFileSync,writeFileSync,mkdtempSync,rmSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {pathToFileURL} from 'node:url';
const file=new URL('../extension/store.mjs',import.meta.url);
let seq=0;
async function facade(){
  const calls=[],oldDoc=globalThis.document;
  const name='__facadeProbe'+(++seq);
  globalThis[name]={saveLocal(...args){calls.push(args);return args;},saveLocalBatch(...args){calls.push(args);return args;}};
  const source=readFileSync(file,'utf8');
  if(source.split("import('/apps/workspace-store.mjs')").length!==2)throw Error('Native facade import changed; update the probe deliberately.');
  const probe=source.replace("import('/apps/workspace-store.mjs')",`Promise.resolve({openDestinationStore:()=>globalThis[${JSON.stringify(name)}]})`);
  const dir=mkdtempSync(join(tmpdir(),'bonfyre-source-store-probe-'));
  let mod;
  try{const path=join(dir,'probe.mjs');writeFileSync(path,probe);mod=await import(pathToFileURL(path).href);}
  catch(error){delete globalThis[name];throw error;}
  finally{rmSync(dir,{recursive:true,force:true});}
  globalThis.document={querySelector:()=>({content:'/apps/ws-'+ 'a'.repeat(40)+'/_records'})};
  return{mod,calls,close(){if(oldDoc===undefined)delete globalThis.document;else globalThis.document=oldDoc;delete globalThis[name];}};
}
test('facade captures reviewed values before awaiting native destination selection',async()=>{
  const f=await facade();try{
    const v={segments:[{text:'reviewed'}],bytes:new Uint8Array([1,2])};
    const pending=f.mod.saveLocal('draft:one',v,4);v.segments[0].text='later';v.bytes[0]=9;
    await pending;expect(f.calls[0][1].segments[0].text).toBe('reviewed');expect([...f.calls[0][1].bytes]).toEqual([1,2]);expect(f.calls[0][2]).toBe(4);
  }finally{f.close();}
});
test('facade captures the whole batch including check-only identity and revision',async()=>{
  const f=await facade();try{
    const changes=[{key:'source:one',expectedRevision:2,checkOnly:true},{key:'draft:one',expectedRevision:0,value:{name:'reviewed'}}];
    const pending=f.mod.saveLocalBatch(changes);changes[0].checkOnly=false;changes[0].value={name:'unreviewed'};changes[1].key='draft:wrong';changes[1].expectedRevision=8;changes[1].value.name='later';changes.push({key:'extra',expectedRevision:0,value:{}});
    await pending;expect(f.calls[0][0]).toEqual([{key:'source:one',expectedRevision:2,checkOnly:true},{key:'draft:one',expectedRevision:0,value:{name:'reviewed'}}]);
  }finally{f.close();}
});
test('uncloneable candidate refuses before any native owner call',async()=>{
  const f=await facade();try{await expect(f.mod.saveLocal('draft:one',{handler:()=>{}},0)).rejects.toThrow();expect(f.calls.length).toBe(0);}finally{f.close();}
});
