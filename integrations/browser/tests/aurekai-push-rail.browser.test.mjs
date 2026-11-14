import {afterAll,beforeAll,expect,test} from 'bun:test';
import {launchFixtureChromium,loopbackServer} from './browser-fixture.mjs';
import {readFile} from 'node:fs/promises';
import {resolve} from 'node:path';
const root=resolve(import.meta.dir,'..');let browser,server;
beforeAll(async()=>{
  browser=await launchFixtureChromium({module:'playwright'});
  server=loopbackServer({async fetch(request){
    const url=new URL(request.url);
    if(url.pathname==='/apps/fixture')return new Response(`<!doctype html><html><head><link rel="stylesheet" href="/apps/aurekai-activity.css"></head><body>
      <header class="ak-bar"><span id="fixture-account"><span class="ak-session" data-session-state="signed-in">Account</span></span></header>
      <main><h1>Push account lifecycle fixture</h1></main><script type="module">
      import {mountAurekaiActivity} from '/apps/aurekai-activity.mjs';
      document.getElementById('fixture-account').setAttribute('data-account-slot','');
      window.fixture={actor:'user:1',paused:false,enableCalls:[],resumeCalls:[],statusActors:[],mode:'pending'};
      const f=window.fixture,binding=context=>({actor:f.actor,subject:context==='self'?f.actor:context,scope:context==='self'?'self':context.split(':')[0]});
      const research=async context=>({context,binding:binding(context),activities:[],outputs:[],more:false,cursor:0,catalogHref:'/s/models/all?context='+encodeURIComponent(context)});
      const native=async context=>({source:'fabric',context,binding:binding(context),revision:f.actor,tasks:[],outputs:[],notifications:[],unread:0,more:false});
      window.rail=mountAurekaiActivity({intervalMs:3600000,tasks:null,inbox:null,destinations:async()=>[{context:'self',label:'Personal',canWrite:true},{context:'institution:studio',label:'Studio',canWrite:true}],
        activity:research,accountActivity:native,pushDevice:async()=>null,
        pushStatus:async context=>{f.statusActors.push(f.actor);return {source:'fabric',context,binding:binding(context),configured:true,paused:f.paused,publicKey:'B'+'a'.repeat(86),keyId:'a'.repeat(64),reason:'',subscriptions:[],counts:{queued:0,accepted:0,shown:0,opened:0,uncertain:0}};},
        pushEnable:async(context,status,options)=>{f.enableCalls.push({context,actor:status.binding.actor,trusted:options.event.isTrusted});f.enableSignal=options.signal;
          if(f.mode==='pause'){f.paused=true;return;}await new Promise(resolve=>f.enableResolve=resolve);},
        pushResume:async(location,options)=>{f.resumeCalls.push(f.actor);if(f.actor==='user:1'){f.resumeSignal=options.signal;return new Promise(resolve=>f.resumeResolve=resolve);}return {state:'access_required'};}
      });
      f.switchActor=()=>{f.actor='user:2';window.dispatchEvent(new Event('bonfyre:account-changed'));};
      </script></body></html>`,{headers:{'Content-Type':'text/html'}});
    if(/^\/apps\/(?:aurekai-activity(?:-source)?|aurekai-push|aurekai-inbox-source|workspace-msgpack)\.mjs$/.test(url.pathname)||url.pathname==='/apps/aurekai-activity.css')
      return new Response(await readFile(resolve(root,url.pathname.slice(6))),{headers:{'Content-Type':url.pathname.endsWith('.css')?'text/css':'text/javascript'}});
    return new Response('No external action exists in this fixture',{status:404});
  }});
});
afterAll(async()=>{await browser?.close();server?.stop(true);},30000);
async function visit(extra=''){
  const context=await browser.newContext({viewport:{width:1280,height:900}}),page=await context.newPage();page.setDefaultTimeout(3000);const errors=[];page.on('pageerror',error=>errors.push(error.message));
  await page.goto(server.url+'apps/fixture?context=institution%3Astudio'+extra);
  try{await page.waitForFunction(()=>window.fixture?.statusActors.length>0,null,{timeout:3000});}catch(error){const status=await page.locator('.ak-rail-status').textContent();await context.close();throw new Error('Fixture failed to become ready: '+errors.join('; ')+' '+status);}
  await page.locator('.ak-rail-push summary').click();await page.locator('.ak-rail-push button').waitFor({state:'visible'});
  return {context,page};
}
test('same-organization account change aborts pending permission ownership and ignores its late completion',async()=>{
  const {context,page}=await visit();try{
    await page.locator('.ak-rail-push button').click();await page.waitForFunction(()=>window.fixture.enableCalls.length===1);
    await page.evaluate(()=>window.fixture.switchActor());await page.waitForFunction(()=>window.fixture.statusActors.includes('user:2'));
    expect(await page.evaluate(()=>window.fixture.enableSignal.aborted)).toBe(true);
    await page.evaluate(()=>window.fixture.enableResolve());await page.waitForFunction(()=>!document.querySelector('.ak-rail-push button').disabled);
    expect(await page.evaluate(()=>window.fixture.enableCalls)).toEqual([{context:'institution:studio',actor:'user:1',trusted:true}]);
    expect(await page.locator('.ak-rail-push button').textContent()).toBe('Enable on this device');expect(await page.locator('.ak-rail-push summary').textContent()).toBe('Device notifications');
  }finally{await context.close();}
});
test('post-enable refresh that reports paused remains disabled after the old action settles',async()=>{
  const {context,page}=await visit();try{
    await page.evaluate(()=>window.fixture.mode='pause');await page.locator('.ak-rail-push button').click();
    await page.waitForFunction(()=>document.querySelector('.ak-rail-push p').textContent==='Background delivery is paused.');
    expect(await page.locator('.ak-rail-push button').isDisabled()).toBe(true);expect(await page.evaluate(()=>window.fixture.enableCalls.length)).toBe(1);
  }finally{await context.close();}
});
test('late push continuation from an old account cannot navigate or reopen its work in the new account',async()=>{
  const {context,page}=await visit('&aurekai_push='+ 'b'.repeat(24)+'&aurekai_actor=user%3A1');try{
    await page.waitForFunction(()=>window.fixture.resumeResolve);await page.evaluate(()=>window.fixture.switchActor());
    await page.waitForFunction(()=>window.fixture.statusActors.includes('user:2'));
    expect(await page.evaluate(()=>window.fixture.resumeSignal.aborted)).toBe(true);
    await page.evaluate(()=>window.fixture.resumeResolve({state:'ready',binding:{actor:'user:1',subject:'institution:studio',scope:'institution'},taskId:'c'.repeat(24),href:'/apps?context=institution%3Astudio#fabric-task-'+ 'c'.repeat(24)}));
    await page.waitForFunction(()=>document.querySelector('.ak-rail-push p').textContent.includes('Switch to the account'));
    expect(new URL(page.url()).hash).toBe('');expect(await page.locator('.ak-rail-detail').isVisible()).toBe(false);
  }finally{await context.close();}
});
