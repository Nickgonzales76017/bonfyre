import {test,expect} from 'bun:test';
import {mkdtempSync,writeFileSync,rmSync,readdirSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {resolve,join} from 'node:path';
import {pathToFileURL} from 'node:url';

test('browser helper exits after replies, protocol errors and work failures without retained timers or profiles',()=>{
  const root=mkdtempSync(join(tmpdir(),'browser-lifecycle-'));
  try{
    const helper=process.env.BONFYRE_BROWSER_SESSION_TEST_MODULE||resolve(import.meta.dir,'../capture/chrome-session.mjs');
    const script=join(root,'lifecycle.mjs');
    writeFileSync(script,`
import {withChrome} from ${JSON.stringify(pathToFileURL(helper).href)};
const cases=[];
for(const kind of ['reply','protocol-error','work-error','disconnect']){
 let error=null;
 try{await withChrome(async({open,send})=>{
  const {page}=await open();
  if(kind==='disconnect'){
   const pending=page('Runtime.evaluate',{expression:'new Promise(()=>{})',awaitPromise:true}).then(()=>null,e=>e.message);
   await send('Browser.close');
   const message=await pending;
   if(!message)throw Error('pending command unexpectedly succeeded');
   throw Error(message);
  }
  if(kind==='work-error')throw Error('expected work failure');
  if(kind==='protocol-error'){await page('Bonfyre.nonexistentCommand');return;}
  const r=await page('Runtime.evaluate',{expression:'20+22',returnByValue:true});
  if(r.result.value!==42)throw Error('wrong browser result');
 });}catch(e){error=e.message;}
 cases.push({kind,error});
}
console.log(JSON.stringify(cases));
`);
    const start=performance.now();
    const result=Bun.spawnSync([process.execPath,script],{env:{...process.env,TMPDIR:root},timeout:20000,maxBuffer:1024*1024});
    expect(result.exitCode,result.stderr.toString()).toBe(0);
    const cases=JSON.parse(result.stdout.toString());
    expect(cases[0].error).toBeNull();
    expect(cases[1].error).toContain('nonexistentCommand');
    expect(cases[2].error).toBe('expected work failure');
    expect(cases[3].error).toMatch(/closed|detached/i);
    expect(readdirSync(root).filter(name=>name.startsWith('bonfyre-chrome-'))).toEqual([]);
    console.log(JSON.stringify({session_cases:cases.length,elapsed_ms:Math.round(performance.now()-start)}));
  }finally{rmSync(root,{recursive:true,force:true});}
},30000);
