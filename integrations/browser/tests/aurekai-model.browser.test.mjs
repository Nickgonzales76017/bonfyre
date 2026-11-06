// Real browser module and MessagePack HTTP boundary; all model replies below
// are loopback fixtures. These tests never invoke inference or a payment owner.
import {afterAll,beforeAll,beforeEach,expect,test} from 'bun:test';
import {createHash} from 'node:crypto';
import {readFile} from 'node:fs/promises';
import {mkdirSync} from 'node:fs';
import {resolve} from 'node:path';
import {encode,decode} from '../workspace-msgpack.mjs';

const root=resolve(import.meta.dir,'..'),taskId='a'.repeat(24),output='Retained output <img src=x onerror="window.modelInjected=true">\n';
const hash=value=>createHash('sha256').update(value).digest('hex');
const deferred=()=>{let resolve;const promise=new Promise(done=>resolve=done);return {promise,resolve};};
const gate=()=>({entered:deferred(),release:deferred()});
const packet=(value,status=200)=>new Response(encode(value),{status,headers:{'Content-Type':'application/msgpack','Cache-Control':'no-store'}});
const binding=(context,actor)=>({actor,subject:context==='self'?actor:context,scope:context==='self'?'self':context.split(':')[0]});
const model=(id,label,catalogId)=>({id,label,catalogId,provider:'bonfyre-moe',execution:'local',available:true,maxTokens:2048,maxTimeoutMs:120000});
let browser,server,actor,calls,reviews,reviewGate,cancelGate,loseSubmit,runState,localTransferAvailable,loseTransfer,badTransferDigest,runOutput,usageAlreadyRecorded;
beforeEach(()=>{actor='user:1';calls=[];reviews=new Map();reviewGate=cancelGate=null;loseSubmit=false;runState='completed';localTransferAvailable=loseTransfer=badTransferDigest=usageAlreadyRecorded=false;runOutput=null;});
function run(owner){
  const completed=runState==='completed',retained=completed||runState==='uncertain',shown=runOutput??output;return {binding:owner,taskId,state:runState,phase:runState==='running'?'active':runState,cancellationAllowed:runState==='running',reconciliationAllowed:runState==='uncertain',localTransferAvailable,
    model:{id:'local:exact',label:'Exact local model',provider:'bonfyre-moe'},error:null,
    output:retained?{id:(runOutput?'d':'c').repeat(24),text:shown,digest:hash(shown),bytes:Buffer.byteLength(shown)}:null,
    usage:completed||usageAlreadyRecorded?{state:'recorded',receiptId:'meter-receipt-one',attemptRef:'attempt-one',durationMs:320,outputBytes:Buffer.byteLength(output),pricing:{state:'unknown'}}:
      ['running','uncertain'].includes(runState)?{state:'pending',attemptRef:'attempt-one',durationMs:retained?320:null,outputBytes:retained?Buffer.byteLength(output):null,pricing:{state:'unknown'}}:null};
}
beforeAll(async()=>{
  const {chromium}=await import(process.env.BONFYRE_PLAYWRIGHT_MODULE||'playwright');
  browser=await chromium.launch({headless:true,...process.env.BONFYRE_CHROMIUM_BINARY?{executablePath:process.env.BONFYRE_CHROMIUM_BINARY}:{}});
  server=Bun.serve({hostname:'127.0.0.1',port:0,async fetch(request){
    const url=new URL(request.url);
    if(url.pathname==='/fixture')return new Response(`<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><link rel="stylesheet" href="/apps/aurekai-activity.css"><link rel="stylesheet" href="/apps/aurekai-model.css">
      <style>body{margin:0;background:#f2f4ed;color:#25342b;font:14px/1.6 system-ui,sans-serif}body>header{padding:22px 30px;border-bottom:1px solid #dce3d5;background:#fbfcf8}main{padding:24px;max-width:1100px}main>a{margin-right:16px;color:#31593d}#fixture-account{float:right}body>header strong{font:26px Georgia,serif}</style></head><body>
      <header><strong>Aurekai</strong><span id="fixture-account"><span class="ak-session" data-session-state="signed-in">Studio account</span></span></header>
      <main><a id="catalog-link" href="/s/models/all">Catalogue</a><a id="model-link" href="/s/model/fixture">Model page</a><section id="model" class="ak-model-workbench" data-model-name="Fixture catalogue model"></section></main>
      <script type="module">import {mountModelCommons} from '/apps/aurekai-model.mjs';import {mountAurekaiActivity} from '/apps/aurekai-activity.mjs';
      const target=document.getElementById('model');target.dataset.aurekaiModel=new URL(location.href).searchParams.get('catalog')||'';
      window.fixtureModel=mountModelCommons(target);
      if(new URL(location.href).searchParams.has('rail')){document.getElementById('fixture-account').dataset.accountSlot='';window.fixtureRail=mountAurekaiActivity({intervalMs:3600000,tasks:null,inbox:null});}
      </script></body></html>`,{headers:{'Content-Type':'text/html'}});
    if(url.pathname==='/apps/_destinations')return packet([1,[['self','Personal',true],['institution:studio','Studio',true]]]);
    if(url.pathname==='/sources/_web')return packet([1,true,{binding:binding(url.searchParams.get('context'),actor),jobs:[],more:false,cursor:0}]);
    if(url.pathname==='/apps/_fabric'){
      const context=url.searchParams.get('context'),operation=url.searchParams.get('operation'),owner=binding(context,actor);
      const body=request.method==='POST'?decode(new Uint8Array(await request.arrayBuffer()))[1]:null;
      calls.push({operation,context,actor,body,method:request.method});
      if(body&&JSON.stringify(body.expectedBinding)!==JSON.stringify(owner))return packet([1,false,{code:'conflict',message:'The current account changed.'}],409);
      if(operation==='read'){
        const href='/apps?'+new URLSearchParams({context})+'#fabric-task-'+taskId,at=Date.now();
        return packet([1,true,{source:'fabric',context,binding:owner,revision:'fixture-'+runState,unread:1,more:false,
          tasks:[{id:'task:'+taskId,taskId,parentTaskId:null,kind:'task',workKind:'model',title:'Retained fixture model run',state:'completed',phase:'completed',revision:1,created:at,updated:at,reconciliationRequired:false,href}],
          outputs:[{id:'receipt:fixture-output',taskId,title:'Model output',kind:'model-output',digest:hash(output),created:at,href}],
          notifications:[{id:'notice:fixture-output',taskId,title:'A model output is ready',kind:'output_ready',state:'unread',revision:1,created:at,updated:at,allowedActions:['read','dismiss'],href}]}]);
      }
      if(operation==='push-status')return packet([1,true,{source:'fabric',binding:owner,configured:false,paused:false,publicKey:null,keyId:null,reason:'Local fixture has no carrier identity.',subscriptions:[],counts:{queued:0,accepted:0,shown:0,opened:0,uncertain:0}}]);
      if(operation==='model-capabilities')return packet([1,true,{binding:owner,models:[model('local:lookalike','Lookalike local model','elsewhere'),model('local:exact','Exact local model','fixture')],limits:{maxPromptBytes:32768,maxTokens:2048,maxTimeoutMs:120000}}]);
      if(operation==='model-review'){
        const row={id:hash(body.requestId).slice(0,48),requestId:body.requestId,modelId:body.modelId,label:body.modelId==='local:exact'?'Exact local model':'Lookalike local model',promptSha256:hash(body.prompt),promptBytes:Buffer.byteLength(body.prompt),maxTokens:body.maxTokens,timeoutMs:body.timeoutMs,expiresAt:Date.now()+60000,pricing:{state:'unknown'},execution:'local',calls:1};
        reviews.set(row.id,{row,body});const waiting=reviewGate;if(waiting){reviewGate=null;waiting.entered.resolve();await waiting.release.promise;}return packet([1,true,{binding:owner,review:row}]);
      }
      if(operation==='model-submit'){
        if(!reviews.has(body.reviewId))return packet([1,false,{code:'invalid',message:'This review is missing.'}],409);
        const submitted=calls.filter(row=>row.operation==='model-submit').length;
        if(loseSubmit){loseSubmit=false;return packet([1,false,{code:'unavailable',message:'The accepted response was lost.'}],503);}
        return packet([1,true,{binding:owner,taskId,state:runState,replayed:submitted>1}]);
      }
      if(operation==='model-read')return packet([1,true,run(owner)]);
      if(operation==='model-verify-transfer'){
        if(loseTransfer){loseTransfer=false;return packet([1,false,{code:'unavailable',message:'The retained transfer acknowledgement was lost.'}],503);}
        return packet([1,true,{binding:owner,taskId,outputId:body.outputId,requestId:body.requestId,
          replayed:calls.filter(row=>row.operation==='model-verify-transfer').length>1,
          transfer:{state:'verified',id:'e'.repeat(32),transport:'loopback-tcp',localOnly:true,verificationOnly:true,
            sha256:badTransferDigest?'0'.repeat(64):hash(output),bytes:Buffer.byteLength(output),
            senderConfirmed:true,receiverConfirmed:true,receiptId:'f'.repeat(24)}}]);
      }
      if(operation==='model-reconcile'){runState='completed';return packet([1,true,run(owner)]);}
      if(operation==='model-cancel'){
        const waiting=cancelGate;if(waiting){cancelGate=null;waiting.entered.resolve();await waiting.release.promise;return packet([1,false,{code:'unavailable',message:'Cancellation was not confirmed.'}],503);}
        runState='cancelled';return packet([1,true,run(owner)]);
      }
      return new Response('Unknown fixture action',{status:404});
    }
    if(/^\/apps\/(?:aurekai-model|aurekai-activity(?:-source)?|aurekai-push|aurekai-inbox-source|workspace-msgpack)\.mjs$/.test(url.pathname)||['/apps/aurekai-activity.css','/apps/aurekai-model.css'].includes(url.pathname))
      return new Response(await readFile(resolve(root,url.pathname.slice(6))),{headers:{'Content-Type':url.pathname.endsWith('.css')?'text/css':'text/javascript'}});
    return new Response('No external action exists in this fixture',{status:404});
  }});
});
afterAll(async()=>{reviewGate?.release.resolve();cancelGate?.release.resolve();await browser?.close();server?.stop(true);});
async function visit(catalog='fixture',{viewport={width:1280,height:1000},rail=false}={}){
  const context=await browser.newContext({viewport}),page=await context.newPage();page.setDefaultTimeout(4000);
  await page.goto(server.url+'fixture?context=institution%3Astudio&catalog='+encodeURIComponent(catalog)+(rail?'&rail=1':''));
  await page.waitForFunction(()=>window.fixtureModel&&document.querySelector('.ak-model-status').textContent!=='Checking execution for this account…');
  if(rail)await page.waitForFunction(()=>document.querySelector('.ak-rail-native-status').textContent.includes('1 task'));
  return {context,page};
}
async function review(page,prompt='Private reviewed instructions'){
  await page.getByLabel('Model instructions').fill(prompt);await page.getByRole('button',{name:'Review run',exact:true}).click();
  await page.getByRole('button',{name:'Start run',exact:true}).waitFor({state:'visible'});
}
async function start(page){await review(page);await page.getByRole('button',{name:'Start run',exact:true}).click();await page.getByRole('button',{name:'Refresh run',exact:true}).waitFor({state:'visible'});}

test('real model review, submit and retained result use the exact catalogue mapping and scoped MessagePack actions',async()=>{
  const {context,page}=await visit();try{
    await review(page,'Private exact model instructions');expect(await page.locator('.ak-model-review').textContent()).toContain('Studio · Exact local model');
    expect(await page.locator('.ak-model-review').textContent()).toContain('Pricing unavailable');expect(calls.filter(row=>row.operation==='model-submit')).toHaveLength(0);
    const reviewCall=calls.find(row=>row.operation==='model-review');expect(reviewCall.body.modelId).toBe('local:exact');expect(reviewCall.body.expectedBinding).toEqual(binding('institution:studio','user:1'));
    await page.getByRole('button',{name:'Start run',exact:true}).click();await page.getByLabel('Retained model output').waitFor();
    expect(await page.getByLabel('Retained model output').textContent()).toBe(output);expect(await page.locator('.ak-model-result img').count()).toBe(0);expect(await page.evaluate(()=>window.modelInjected)).toBeUndefined();
    expect(await page.locator('.ak-model-result').textContent()).toContain('Usage recorded.');
    expect(await page.getByRole('link',{name:'Reopen this run from Activity'}).getAttribute('href')).toBe('/apps?context=institution%3Astudio#fabric-task-'+taskId);
    expect(await page.locator('#model-link').getAttribute('href')).toBe('/s/model/fixture?context=institution%3Astudio');
    expect(calls.filter(row=>row.operation==='model-submit')).toHaveLength(1);expect(await page.evaluate(()=>localStorage.length+sessionStorage.length)).toBe(0);
  }finally{await context.close();}
  const missing=await visit('unmapped');try{
    expect(await missing.page.getByRole('button',{name:'Review run',exact:true}).isDisabled()).toBe(true);
    expect(await missing.page.locator('.ak-model-status').textContent()).toContain('no currently admitted local execution');
  }finally{await missing.context.close();}
});

test('a same-organization actor switch clears private instructions and rejects a late old-account review',async()=>{
  const {context,page}=await visit(),waiting=gate();try{
    reviewGate=waiting;await page.getByLabel('Model instructions').fill('Private account one draft');await page.getByRole('button',{name:'Review run',exact:true}).click();await waiting.entered.promise;
    actor='user:2';await page.evaluate(()=>window.dispatchEvent(new Event('bonfyre:account-changed')));
    expect(await page.getByLabel('Model instructions').inputValue()).toBe('');expect(await page.locator('.ak-model-review').isVisible()).toBe(false);
    await page.waitForFunction(()=>!document.querySelector('textarea').disabled);waiting.release.resolve();
    await review(page,'Account two current draft');expect(await page.locator('.ak-model-review').textContent()).toContain('Studio');
    const inputs=calls.filter(row=>row.operation==='model-review');expect(inputs.map(row=>row.body.expectedBinding.actor)).toEqual(['user:1','user:2']);
    expect(inputs[1].body.prompt).toBe('Account two current draft');expect(calls.filter(row=>row.operation==='model-submit')).toHaveLength(0);
    expect(await page.locator('#model').textContent()).not.toContain('Private account one draft');
  }finally{waiting.release.resolve();await context.close();}
});

test('a lost submission reply exposes explicit recovery with the exact same admitted request body',async()=>{
  const {context,page}=await visit();try{
    loseSubmit=true;await review(page);await page.getByRole('button',{name:'Start run',exact:true}).click();await page.getByRole('button',{name:'Recover this run',exact:true}).waitFor({state:'visible'});
    expect(calls.filter(row=>row.operation==='model-submit')).toHaveLength(1);expect(await page.getByLabel('Model instructions').isDisabled()).toBe(true);
    expect(await page.getByRole('button',{name:'Review run',exact:true}).isDisabled()).toBe(true);
    await page.getByRole('button',{name:'Recover this run',exact:true}).click();await page.getByLabel('Retained model output').waitFor();
    const attempts=calls.filter(row=>row.operation==='model-submit');expect(attempts).toHaveLength(2);expect(attempts[1].body).toEqual(attempts[0].body);
    expect(calls.filter(row=>row.operation==='model-review')).toHaveLength(1);expect(await page.getByLabel('Retained model output').textContent()).toBe(output);
  }finally{await context.close();}
});

test('pending cancellation blocks competing refresh and suspension requires a fresh read before another cancel',async()=>{
  runState='running';const {context,page}=await visit(),waiting=gate();try{
    await start(page);await page.getByRole('button',{name:'Cancel run',exact:true}).waitFor({state:'visible'});cancelGate=waiting;
    await page.getByRole('button',{name:'Cancel run',exact:true}).click();await waiting.entered.promise;
    expect(await page.getByRole('button',{name:'Refresh run',exact:true}).isDisabled()).toBe(true);expect(await page.getByRole('button',{name:'Cancel run',exact:true}).isDisabled()).toBe(true);
    const reads=calls.filter(row=>row.operation==='model-read').length;
    await page.evaluate(()=>{Object.defineProperty(document,'hidden',{configurable:true,value:true});document.dispatchEvent(new Event('visibilitychange'));});
    expect(await page.getByRole('button',{name:'Cancel run',exact:true}).isVisible()).toBe(false);waiting.release.resolve();
    await page.evaluate(()=>{Object.defineProperty(document,'hidden',{configurable:true,value:false});document.dispatchEvent(new Event('visibilitychange'));});
    await page.waitForFunction(()=>[...document.querySelectorAll('button')].some(node=>node.textContent==='Cancel run'&&!node.hidden&&!node.disabled));
    expect(calls.filter(row=>row.operation==='model-read').length).toBeGreaterThan(reads);
    await page.getByRole('button',{name:'Cancel run',exact:true}).click();await page.locator('.ak-model-result').filter({hasText:'Cancelled'}).waitFor();
    expect(calls.filter(row=>row.operation==='model-cancel')).toHaveLength(2);expect(await page.getByRole('button',{name:'Cancel run',exact:true}).isVisible()).toBe(false);
    expect(await page.getByRole('button',{name:'Refresh run',exact:true}).isDisabled()).toBe(false);
  }finally{waiting.release.resolve();await context.close();}
});

test('active usage may be unknown and explicit usage reconciliation never submits another inference request',async()=>{
  runState='running';const {context,page}=await visit();try{
    await start(page);await page.locator('.ak-model-result').filter({hasText:'Usage receipt is pending.'}).waitFor();
    const active=await page.locator('.ak-model-result').textContent();expect(active).not.toContain('NaN');expect(active).not.toContain('0 seconds');
    expect(await page.getByRole('button',{name:'Finish usage receipt',exact:true}).isVisible()).toBe(false);
    runState='uncertain';await page.getByRole('button',{name:'Refresh run',exact:true}).click();await page.getByRole('button',{name:'Finish usage receipt',exact:true}).waitFor({state:'visible'});
    expect(await page.getByLabel('Retained model output').textContent()).toBe(output);expect(calls.filter(row=>row.operation==='model-submit')).toHaveLength(1);
    await page.getByRole('button',{name:'Finish usage receipt',exact:true}).click();await page.locator('.ak-model-result').filter({hasText:'Usage recorded.'}).waitFor();
    const recovery=calls.filter(row=>row.operation==='model-reconcile');expect(recovery).toHaveLength(1);
    expect(recovery[0].body).toMatchObject({expectedBinding:binding('institution:studio','user:1'),taskId});
    expect(calls.filter(row=>row.operation==='model-submit')).toHaveLength(1);expect(calls.filter(row=>row.operation==='model-review')).toHaveLength(1);
    expect(await page.getByRole('button',{name:'Finish usage receipt',exact:true}).isVisible()).toBe(false);
  }finally{await context.close();}
});

test('retained usage still permits native-admitted completion of an interrupted run record without another inference request',async()=>{
  runState='uncertain';usageAlreadyRecorded=true;const {context,page}=await visit();try{
    await start(page);await page.getByRole('button',{name:'Finish run record',exact:true}).waitFor({state:'visible'});
    expect(await page.getByLabel('Retained model output').textContent()).toBe(output);
    expect(await page.locator('.ak-model-result').textContent()).toContain('Usage recorded.');
    await page.getByRole('button',{name:'Finish run record',exact:true}).click();
    await page.locator('.ak-model-result').filter({hasText:'Completed'}).waitFor();
    const recovery=calls.filter(row=>row.operation==='model-reconcile');expect(recovery).toHaveLength(1);
    expect(recovery[0].body).toMatchObject({expectedBinding:binding('institution:studio','user:1'),taskId});
    expect(calls.filter(row=>row.operation==='model-submit')).toHaveLength(1);expect(calls.filter(row=>row.operation==='model-review')).toHaveLength(1);
    expect(await page.getByRole('button',{name:'Finish run record',exact:true}).isVisible()).toBe(false);
  }finally{await context.close();}
});

test('local transfer is explicit, recovers the same request after lost acknowledgement and remains bound to its output',async()=>{
  const {context,page}=await visit();try{
    await start(page);await page.getByLabel('Retained model output').waitFor();
    expect(await page.getByRole('button',{name:'Verify local transfer',exact:true}).isVisible()).toBe(false);
    expect(calls.filter(row=>row.operation==='model-verify-transfer')).toHaveLength(0);
    localTransferAvailable=true;await page.getByRole('button',{name:'Refresh run',exact:true}).click();
    await page.getByRole('button',{name:'Verify local transfer',exact:true}).waitFor({state:'visible'});
    expect(await page.locator('.ak-model-result').textContent()).toContain('two peers on this device');
    expect(calls.filter(row=>row.operation==='model-verify-transfer')).toHaveLength(0);

    loseTransfer=true;await page.getByRole('button',{name:'Verify local transfer',exact:true}).click();
    await page.getByRole('button',{name:'Recover transfer receipt',exact:true}).waitFor({state:'visible'});
    expect(calls.filter(row=>row.operation==='model-verify-transfer')).toHaveLength(1);
    await page.getByRole('button',{name:'Refresh run',exact:true}).click();
    await page.waitForFunction(()=>[...document.querySelectorAll('button')].some(node=>node.textContent==='Recover transfer receipt'&&!node.disabled));
    expect(calls.filter(row=>row.operation==='model-verify-transfer')).toHaveLength(1);

    badTransferDigest=true;await page.getByRole('button',{name:'Recover transfer receipt',exact:true}).click();
    await page.waitForFunction(()=>[...document.querySelectorAll('button')].some(node=>node.textContent==='Recover transfer receipt'&&!node.disabled));
    expect(calls.filter(row=>row.operation==='model-verify-transfer')).toHaveLength(2);
    expect(await page.getByRole('button',{name:'Local transfer verified',exact:true}).isVisible()).toBe(false);
    badTransferDigest=false;await page.getByRole('button',{name:'Recover transfer receipt',exact:true}).click();
    await page.getByRole('button',{name:'Local transfer verified',exact:true}).waitFor({state:'visible'});
    expect(await page.getByRole('button',{name:'Local transfer verified',exact:true}).isDisabled()).toBe(true);
    expect(await page.locator('.ak-model-result').textContent()).toContain('Both local peers confirmed the exact output');
    const attempts=calls.filter(row=>row.operation==='model-verify-transfer');expect(attempts).toHaveLength(3);
    expect(attempts[1].body).toEqual(attempts[0].body);expect(attempts[2].body).toEqual(attempts[0].body);
    expect(attempts[0].body).toMatchObject({expectedBinding:binding('institution:studio','user:1'),taskId,outputId:'c'.repeat(24)});
    expect(Object.keys(attempts[0].body).sort()).toEqual(['expectedBinding','outputId','requestId','taskId']);

    runOutput='A changed retained output needs its own verification.';
    await page.getByRole('button',{name:'Refresh run',exact:true}).click();
    await page.waitForFunction(()=>document.querySelector('[aria-label="Retained model output"]')?.textContent==='A changed retained output needs its own verification.');
    expect(await page.getByRole('button',{name:'Local transfer verified',exact:true}).isVisible()).toBe(false);
    expect(calls.filter(row=>row.operation==='model-verify-transfer')).toHaveLength(3);
    expect(calls.filter(row=>row.operation==='model-submit')).toHaveLength(1);
    expect(await page.evaluate(()=>localStorage.length+sessionStorage.length)).toBe(0);
  }finally{await context.close();}
});

test('shared rail and Model Commons fit desktop and mobile and expose keyboard-operated controls',async()=>{
  const directory=process.env.BONFYRE_BROWSER_EVIDENCE_DIR;if(directory)mkdirSync(directory,{recursive:true});
  for(const [name,viewport]of [['desktop',{width:1440,height:1100}],['mobile',{width:390,height:844}]]){
    const {context,page}=await visit('fixture',{viewport,rail:true});try{
      await review(page,'Summarize the account-scoped design decisions and retain a concise output for Studio.');
      const metrics=await page.evaluate(()=>({width:innerWidth,scrollWidth:document.documentElement.scrollWidth,main:document.querySelector('.ak-model-workbench').getBoundingClientRect().toJSON(),rail:document.querySelector('.ak-work-rail').getBoundingClientRect().toJSON()}));
      expect(metrics.scrollWidth).toBeLessThanOrEqual(metrics.width);expect(metrics.main.left).toBeGreaterThanOrEqual(0);expect(metrics.main.right).toBeLessThanOrEqual(metrics.width);
      expect(metrics.rail.right).toBeLessThanOrEqual(metrics.width);expect(metrics.rail.left).toBeGreaterThanOrEqual(0);
      if(directory)await page.screenshot({path:resolve(directory,'aurekai-model-rail-'+name+'.png'),fullPage:true});
      await page.getByLabel('Model instructions').focus();await page.keyboard.press('Tab');expect(await page.evaluate(()=>document.activeElement.getAttribute('aria-label'))).toBe('Maximum output tokens');
      await page.locator('.ak-rail-launch').focus();if(await page.locator('.ak-rail-launch').getAttribute('aria-expanded')==='false')await page.keyboard.press('Enter');
      expect(await page.locator('.ak-rail-launch').getAttribute('aria-expanded')).toBe('true');
      await page.keyboard.press('Escape');expect(await page.locator('.ak-rail-launch').getAttribute('aria-expanded')).toBe('false');expect(await page.locator('.ak-rail-launch').evaluate(node=>node===document.activeElement)).toBe(true);
      if(name==='mobile'){
        await page.keyboard.press('Enter');expect(await page.locator('.ak-rail-launch').getAttribute('aria-expanded')).toBe('true');
        const bounds=await page.locator('.ak-rail-cards').boundingBox();expect(bounds.x).toBeGreaterThanOrEqual(0);expect(bounds.y).toBeGreaterThanOrEqual(0);expect(bounds.x+bounds.width).toBeLessThanOrEqual(viewport.width);
        if(directory)await page.screenshot({path:resolve(directory,'aurekai-model-rail-mobile-expanded.png'),fullPage:true});
      }
    }finally{await context.close();}
  }
});
