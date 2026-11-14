// Real program owner, compiler, SQLite and browser over a loopback HTTP boundary.
// Account admission is a fixture; the C gateway suite covers native auth and roles.
import {beforeAll,beforeEach,afterAll,expect,test} from 'bun:test';
import {deferred,gate,packet,loopbackServer} from './browser-fixture.mjs';
import {Database} from 'bun:sqlite';
import {mkdtempSync,readFileSync,writeFileSync,rmSync,mkdirSync} from 'node:fs';
import {join,resolve} from 'node:path';
import {tmpdir} from 'node:os';
import {createHash} from 'node:crypto';
import {operate} from '../../../scripts/program-workspace-owner.mjs';
import {operateProgramSourceWork} from '../../../scripts/program-source-work.mjs';
import {run as compileOwner} from '../../../scripts/compile-program-fabric.mjs';
import {WebWork} from '../../../cmd/BonfyreSourceFrontier/tools/web_work.mjs';
import {encode,decode} from '../workspace-msgpack.mjs';
import {withChrome} from '../capture/chrome-session.mjs';

const repo=resolve(import.meta.dir,'../../..'),assets=process.env.BONFYRE_TEST_PUBLIC_BROWSER||resolve(import.meta.dir,'..');
const types=['program_template','program_instance','semantic_slot','application_binding','program_binding','source_collection','program_requirement','funding_work_package'];
const keys=['template_id','instance_id','slot_id','binding_id','binding_id','collection_id','requirement_id','package_id'];
const hash=value=>createHash('sha256').update(value).digest('hex');
const workspace='ws-'+'a'.repeat(40),recordNamespace='workspace-'+'d'.repeat(40),recordKey='draft:learning:1';
const referenceManifest='view\tlabel\tentry\tprefix\ttitle\tquery\nplan\tWorking plan\tsources/\tdraft:\ttitle\trecord=$key\n';
const referencePage='<html><head><meta name="bonfyre-workspace-store"></head><body>Source editor fixture</body></html>';
const packageManifest='path\tsha256\tbytes\nCONTINUATIONS.tsv\t'+hash(referenceManifest)+'\t'+Buffer.byteLength(referenceManifest)+'\nsources/index.html\t'+hash(referencePage)+'\t'+Buffer.byteLength(referencePage)+'\n',release=hash(packageManifest);
const admission={admissionEnv:{BONFYRE_EXTERNAL_READS_ENABLED:'1',BONFYRE_RESOURCE_MAX_CRAWL_PAGES_PER_RUN:'10'}};
const retainedItem=(suffix='one')=>{const body='A retained fixture source about community learning. '+suffix;return {id:hash('program-browser-item-'+suffix),url:'https://example.org/learning/'+suffix,title:'Community learning source '+suffix,kind:'page',author:'Fixture author',publishedAt:'2026-10-01T00:00:00Z',body,bodySHA256:hash(body),truncated:false};};
const providerReply=spec=>({items:[retainedItem(),retainedItem('two')],refused:0,requestId:'provider-request',reportedCredits:null,rawSHA256:hash('fixture-provider-reply'),raw:'fixture-provider-reply',observedAt:'2026-10-22T01:00:00Z',provider:spec.provider,tool:spec.provider==='exa'?'web_search_advanced_exa':'firecrawl_search',notices:[],noticesTruncated:false});
const emptyAddress=argv=>({exitCode:0,stderr:'',stdout:JSON.stringify({candidate_kind:argv[argv.indexOf('--kind')+1],truth:'eligible_for_further_consideration',considered:0,eligible:0,returned:0,has_more:false,survivors:[]})});
let server,estate,db,actor,denied,toolsDenied,toolsUnavailable,losePrepare,readUnavailable,replyActor,reviewGate,calls;
let loseChange,loseSourcePlan,loseSourceStart,loseSourceDecision,stopPlanBeforeNative,recordRevision,recordPageSize,metadataLeadRecord,providerEffects,providerGate,sourceItemGate,sourceDecisionGate,sourceAdmission;
function binding(context){return {actor,subject:context==='self'?actor:context,scope:context==='self'?'self':context.split(':')[0]};}
const recordBody=()=>encode([2,{title:'Workshop plan',kind:'plan',content:'Retained editor material'},[]]);
function nativeRecord(context){const b=binding(context),owner=[1,workspace,b.subject,b.scope,'experience.source-library.workspace',release,'sources/','source-library',recordNamespace];return {exitCode:0,stdout:encode([1,owner,[recordKey,recordRevision,recordBody()],[[owner,'Source library']]])};}
function owner(operation,context,body,instance=''){
 return operate({operation,...binding(context),db:join(estate,'cms.db'),addressIndex:join(estate,'address.db'),cmsBinary:process.execPath,workspacePackages:join(estate,'packages'),instance},body,{addressRunFn:emptyAddress,cmsRunFn:()=>nativeRecord(context)});
}
async function sourceOwner(operation,context,body){return operateProgramSourceWork({operation,binding:binding(context),db,request:decode(body)[1],sourceFrontierDB:join(estate,'frontier.db'),webConfig:sourceAdmission,createWebWork:(path,options)=>{const work=new WebWork(path,{...options,execute:async spec=>{providerEffects++;if(providerGate){const wait=providerGate;providerGate=null;wait.entered.resolve();await wait.release.promise;wait.finished.resolve();}return providerReply(spec);}});if(stopPlanBeforeNative&&operation==='source-plan'){stopPlanBeforeNative=false;work.plan=()=>{throw Error('Fixture stopped before native plan');};}return work;}});}
function prepare(context='self'){
 const expectedBinding=binding(context),review=owner('review',context,encode([1,{template_id:'learning',expectedBinding}]));
 return owner('prepare',context,encode([1,{template_id:'learning',review_digest:review.review_digest,expectedBinding}])).instance;
}
beforeEach(()=>{
 db?.close();if(estate)rmSync(estate,{recursive:true,force:true});estate=mkdtempSync(join(tmpdir(),'program-browser-'));db=new Database(join(estate,'cms.db'));
 types.forEach((type,index)=>{const schema=JSON.parse(readFileSync(join(repo,'cmd/BonfyreCMS/content-types',type+'.json'),'utf8'));
  const columns=new Set(['namespace','created_at','updated_at','published_at','status',...Object.keys(schema.attributes)]);
  db.exec(`CREATE TABLE "${type}"(id INTEGER PRIMARY KEY,${[...columns].map(name=>`"${name}" ${schema.attributes[name]?.type==='integer'?'INTEGER':'TEXT'}`).join(',')},UNIQUE(namespace,"${keys[index]}"))`);
 });
 const packs=join(estate,'fixture.json');writeFileSync(packs,JSON.stringify([{program_id:'learning',name:'Community learning',archetype:'education',
  description:'Prepare sources, mentoring and a recurring learning program. <img src=x onerror="window.injected=true">',subject_kinds:['person','institution','household'],semantic_slots:['sources','mentoring','community','connections'],capability_families:['source_intake'],grant_stack:['Education stream']}]));
 compileOwner({verb:'load',estate,packs});actor='user:19';denied=null;toolsDenied=toolsUnavailable=losePrepare=readUnavailable=false;replyActor=null;reviewGate=null;calls=[];
 new Database(join(estate,'frontier.db')).close();mkdirSync(join(estate,'packages',release,'sources'),{recursive:true});writeFileSync(join(estate,'packages',release,'MANIFEST.tsv'),packageManifest);writeFileSync(join(estate,'packages',release,'CONTINUATIONS.tsv'),referenceManifest);writeFileSync(join(estate,'packages',release,'sources/index.html'),referencePage);
 loseChange=loseSourcePlan=loseSourceStart=loseSourceDecision=stopPlanBeforeNative=metadataLeadRecord=false;recordRevision=3;recordPageSize=1000;providerEffects=0;providerGate=sourceItemGate=sourceDecisionGate=null;sourceAdmission=admission;
});
beforeAll(async()=>{
 server=loopbackServer({async fetch(request){
  const url=new URL(request.url),context=url.searchParams.get('context')||'self';
  if(url.pathname==='/programs')return new Response(`<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><link rel="stylesheet" href="/apps/program-workspace.css"><style>body{margin:0;background:#f4f5f0;color:#23342b;font:15px/1.55 system-ui,sans-serif}header{padding:18px 28px;border-bottom:1px solid #d5ddcf;background:#fafbf7}header strong{font:26px Georgia,serif}main{padding:26px}h1{font:36px Georgia,serif}a{color:#315e41}button,input,select{font:inherit}button{cursor:pointer}</style></head><body><header><strong>Aurekai</strong> · Browser verification fixture</header><main><section class="program-workspace" data-program-workspace data-context="${context==='self'?'self':'institution:studio'}" data-instance="${url.searchParams.get('instance')||''}" data-template="${(url.searchParams.get('template')||'').replace(/[^A-Za-z0-9._-]/g,'')}" data-can-write="${url.searchParams.get('readonly')?'false':'true'}"><h1>Programs</h1><p>Prepare work for an account or organization, then continue in its tools.</p><label>Destination <select id="program-context"><option>${context==='self'?'Personal':'Studio organization'}</option></select></label><div data-program-app></div></section></main><script type="module" src="/apps/program-workspace.mjs"></script></body></html>`,{headers:{'Content-Type':'text/html'}});
  if(url.pathname==='/apps/_bindings'){
   if(toolsDenied)return new Response('Current access required',{status:403});
   if(toolsUnavailable)return new Response('Temporarily unavailable',{status:503});
   const b=binding(context);return packet([1,b.subject,b.scope,[[workspace,'experience.source-library.workspace','sources/',release,'Source library',true]]]);
  }
  if(url.pathname==='/apps/'+workspace+'/CONTINUATIONS.tsv')return new Response(referenceManifest,{headers:{'Content-Type':'text/tab-separated-values'}});
  if(url.pathname==='/apps/'+workspace+'/_records'){
   if(toolsDenied)return new Response('Current access required',{status:403});const prefix=url.searchParams.get('prefix')||'',cursor=url.searchParams.get('cursor')||'';
   const records=[...(metadataLeadRecord?[['aaa:raw',1,encode([2,{title:'Raw metadata without a view'},[]])]]:[]),[recordKey,recordRevision,recordBody()],['untyped:record',1,encode([2,{title:'Unsupported editor record'},[]])]].filter(row=>row[0].startsWith(prefix)&&row[0]>cursor),rows=records.slice(0,recordPageSize);
   return packet([1,recordRevision,records.length>rows.length,rows.at(-1)?.[0]||cursor,rows]);
  }
  if(url.pathname==='/apps/_programs'){
   const operation=url.searchParams.get('operation'),body=request.method==='POST'?new Uint8Array(await request.arrayBuffer()):undefined;
   calls.push({operation,context,body:body?decode(body)[1]:null,instance:url.searchParams.get('instance')||''});
   if(denied===operation)return new Response('Current access required',{status:403});
   if(['read','change-receipt'].includes(operation)&&readUnavailable)return packet([1,false,{code:'owner_unavailable',message:'Saved work is temporarily unavailable.'}],503);
   try{
    if(operation==='source-decision'&&sourceDecisionGate){const wait=sourceDecisionGate;sourceDecisionGate=null;wait.entered.resolve();await wait.release.promise;wait.finished.resolve();}
    const value=operation.startsWith('source-')?await sourceOwner(operation,context,body):owner(operation,context,body,url.searchParams.get('instance')||'');
    if(operation==='source-item'&&sourceItemGate){const wait=sourceItemGate;sourceItemGate=null;wait.entered.resolve();await wait.release.promise;wait.finished.resolve();}
    if(replyActor===operation)value.binding={...value.binding,actor:'user:20',...(context==='self'?{subject:'user:20'}:{})};
    if(operation==='review'&&reviewGate){const wait=reviewGate;reviewGate=null;wait.entered.resolve();await wait.release.promise;wait.finished.resolve();}
    if(operation==='prepare'&&losePrepare){losePrepare=false;return packet([1,false,{code:'owner_unavailable',message:'The preparation acknowledgement was lost.'}],503);}
    if(operation==='change'&&loseChange){loseChange=false;return packet([1,false,{code:'owner_unavailable',message:'The change acknowledgement was lost.'}],503);}
    if(operation==='source-plan'&&loseSourcePlan){loseSourcePlan=false;return packet([1,false,{code:'owner_unavailable',message:'The source plan acknowledgement was lost.'}],503);}
    if(operation==='source-start'&&loseSourceStart){loseSourceStart=false;return packet([1,false,{code:'owner_unavailable',message:'The source start acknowledgement was lost.'}],503);}
    if(operation==='source-decision'&&loseSourceDecision){loseSourceDecision=false;return packet([1,false,{code:'owner_unavailable',message:'The source review acknowledgement was lost.'}],503);}
    return packet([1,true,value]);
   }catch(error){return packet([1,false,{code:error.code||'owner_unavailable',message:error.message}],error.exitCode===4?409:503);}
  }
  const asset=url.pathname.slice(6);
  if(url.pathname.startsWith('/apps/')&&['program-workspace.mjs','program-service-continuations.mjs','program-service-policy.mjs','program-source-work.mjs','program-workspace.css','aurekai-activity-source.mjs','workspace-msgpack.mjs','interaction-collection.mjs','record-comparison.mjs','workspace-store.mjs','workspace-value.mjs','workspace-continuation.mjs'].includes(asset))
   return new Response(readFileSync(join(assets,asset)),{headers:{'Content-Type':asset.endsWith('.css')?'text/css':'text/javascript'}});
  return new Response('No outside actions in this fixture',{status:404});
 }});
});
afterAll(async()=>{for(const wait of [reviewGate,providerGate,sourceItemGate,sourceDecisionGate])wait?.release.resolve();server?.stop(true);db?.close();if(estate)rmSync(estate,{recursive:true,force:true});});
// The same installed-browser owner used by native Activity tests owns each
// throwaway profile. These helpers use real pointer input and host-side waits;
// no downloaded browser driver or package is needed by the release gate.
const node=selector=>'document.querySelector('+JSON.stringify(selector)+')';
const named=(selector,name)=>'[...document.querySelectorAll('+JSON.stringify(selector)+')].find(el=>'+
 (name instanceof RegExp?name+'.test(el.innerText.trim().replace(/\\s+/g," "))':'el.innerText.trim().replace(/\\s+/g," ")==='+JSON.stringify(name))+')';
const button=name=>named('button',name),heading=name=>named('h1,h2,h3,h4,h5,h6',name),link=name=>named('a[href]',name);
const visible=expression=>'(()=>{const el='+expression+';if(!el)return false;const r=el.getBoundingClientRect(),style=getComputedStyle(el);return r.width>0&&r.height>0&&style.visibility!=="hidden"&&style.display!=="none";})()';
async function waitForGate(promise,label){
 let timer;
 try{return await Promise.race([promise,new Promise((_,reject)=>{timer=setTimeout(()=>reject(Error('Programs gate did not finish: '+label)),8000);})]);}
 finally{clearTimeout(timer);}
}
function chromeTest(name,work){
 test(name,()=>withChrome(async({open,send})=>{
  async function visit(query='',viewport={width:1360,height:1000}){
   const tab=await open();
   await tab.page('Emulation.setDeviceMetricsOverride',{...viewport,deviceScaleFactor:1,mobile:false});
   const run=async expression=>{const result=await tab.page('Runtime.evaluate',{expression,awaitPromise:true,returnByValue:true});if(result.exceptionDetails)throw Error(result.exceptionDetails.exception?.description||result.exceptionDetails.text);return result.result.value;};
   const wait=async expression=>{
    const deadline=Date.now()+8000;
    while(Date.now()<deadline){if(await run('(()=>{try{return Boolean('+expression+');}catch(error){if(error.name!=="TypeError")throw error;return false;}})()'))return;await Bun.sleep(20);}
    const notices=await run('[...document.querySelectorAll(".program-status,.program-draft-state,.program-source-status")].map(node=>node.textContent).join(" | ")');throw Error('Programs condition not reached: '+expression+'\nCurrent notices: '+notices);
   };
   const click=async expression=>{
    await wait(visible(expression)+'&&!('+expression+').matches(":disabled")');
    await run('(()=>{('+expression+').scrollIntoView({block:"center"});return new Promise(done=>requestAnimationFrame(()=>requestAnimationFrame(done)));})()');
    const point=await run('(()=>{const el='+expression+';const r=el.getBoundingClientRect(),point={x:r.x+r.width/2,y:r.y+r.height/2};if(!el.contains(document.elementFromPoint(point.x,point.y)))throw Error("Programs control is covered");return point;})()');
    await tab.page('Input.dispatchMouseEvent',{type:'mouseMoved',...point});
    await tab.page('Input.dispatchMouseEvent',{type:'mousePressed',...point,button:'left',clickCount:1});
    await tab.page('Input.dispatchMouseEvent',{type:'mouseReleased',...point,button:'left',clickCount:1});
   };
   const fill=async(expression,value)=>{await wait(visible(expression)+'&&!('+expression+').matches(":disabled")');await run('(()=>{const input='+expression+';input.focus();input.value='+JSON.stringify(value)+';input.dispatchEvent(new Event("input",{bubbles:true}));input.dispatchEvent(new Event("change",{bubbles:true}));})()');};
   const page={tab,run,wait,click,fill,evaluate:fn=>run('('+fn+')()'),text:selector=>run(node(selector)+'.textContent'),close:()=>send('Target.closeTarget',{targetId:tab.targetId})};
   await tab.go(server.url+'programs?'+query);
   await wait(node('.program-status')+'&&!'+node('.program-status')+'.textContent.startsWith("Reading")');
   return page;
  }
  await work(visit);
 },{timeoutMs:60000,window:'1360,1000'}),70000);
}
async function choose(page){await page.click(button(/^Templates/));await page.click(button(/^Community learning education(?: |$)/));}
async function review(page){await choose(page);await page.click(button('Review setup'));await page.wait(visible(button(/^(Prepare this program|Open prepared program)$/)));}
async function prepared(page){await review(page);await page.click(button(/^(Prepare this program|Open prepared program)$/));await page.wait(visible(heading('Requirements')));}
const count=()=>db.query('SELECT COUNT(*) n FROM program_instance').get().n;
async function cleared(page){await page.wait(node('.program-status')+'.dataset.error==="true"');expect(await page.text('.program-list')).not.toContain('Community learning');expect(await page.text('.program-detail')).toBe('');}

chromeTest('browser prepares and reopens the real private program with current installed workspace links',async visit=>{
 const page=await visit();try{
  await review(page);expect(await page.text('.program-review')).toContain('Personal');expect(count()).toBe(0);
  expect(await page.run('document.querySelectorAll(".program-detail img").length')).toBe(0);expect(await page.evaluate(()=>window.injected)).toBeUndefined();
  await page.click(button('Prepare this program'));await page.wait(visible(heading('Requirements')));
  expect(count()).toBe(1);expect(db.query('SELECT owner_subject,status FROM program_instance').get()).toEqual({owner_subject:'user:19',status:'draft'});
  expect(await page.run(link('Source library')+".getAttribute('href')")).toBe('/apps/'+workspace+'/sources/');
  expect(await page.run(link('Community')+".getAttribute('href')")).toBe('/community');
  expect(await page.run(link('Connections')+".getAttribute('href')")).toBe('/apps/connections?context=self');
  expect(await page.run('document.querySelector("[data-continuation-kind=surface]").dataset.continuationState')).toBe('available');
  expect(await page.run('document.querySelector("[data-continuation-kind=account]").dataset.continuationState')).toBe('available');
  expect(await page.text('.program-tools')).toContain('No typed Aurekai continuation is declared for this program facet yet.');
  expect(await page.text('.program-tools')).toContain('Each continuation keeps its own owner');
  expect(await page.run(link('Model Commons')+".getAttribute('href')")).toBe('/s/models/all?context=self');
  expect(await page.text('.program-detail')).toContain('Parts awaiting an editor connection');
  const address=await page.run('location.href');db.exec("UPDATE program_requirement SET state='met'");await page.tab.go(await page.run('location.href'));await page.wait(visible(heading('Requirements')));
  expect(await page.run('location.href')).toBe(address);expect(await page.text('.program-summary')).toContain('0 requirements still open');expect(count()).toBe(1);
  expect(await page.evaluate(()=>localStorage.length+sessionStorage.length)).toBe(0);
 }finally{await page.close();}
});

chromeTest('lost preparation acknowledgement and a failed read retain the exact recovery address without a second write',async visit=>{
 const page=await visit();try{
  await review(page);losePrepare=true;await page.click(button('Prepare this program'));await page.wait(visible(button('Check saved program')));
  expect(count()).toBe(1);readUnavailable=true;await page.click(button('Check saved program'));await page.wait(visible(named('p','The saved outcome could not be read. You can check the same program again.')));
  readUnavailable=false;await page.click(button('Check saved program'));await page.wait(visible(heading('Requirements')));
  expect(count()).toBe(1);expect(calls.filter(row=>row.operation==='prepare')).toHaveLength(1);
  const reads=calls.filter(row=>row.operation==='read'&&row.instance);expect(reads).toHaveLength(2);expect(reads[0].instance).toBe(reads[1].instance);
 }finally{await page.close();}
});

chromeTest('access revocation on saved reads, reviews and preparation clears all prior private program content',async visit=>{
 prepare();
 for(const operation of ['read','review','prepare']){
  const page=await visit();try{
   if(operation==='read'){denied='read';await page.click(button(/^Community learning education(?: |$)/));}
   else {await choose(page);if(operation==='prepare'){await page.click(button('Review setup'));await page.wait(visible(button('Open prepared program')));denied='prepare';await page.click(button('Open prepared program'));}
    else {denied='review';await page.click(button('Review setup'));}}
   await cleared(page);expect(count()).toBe(1);
  }finally{denied=null;await page.close();}
 }
});

chromeTest('installed tools access refusal clears private programs while ordinary tool unavailability remains explicit',async visit=>{
 prepare();toolsDenied=true;const blocked=await visit();try{await cleared(blocked);}finally{await blocked.close();}
 toolsDenied=false;toolsUnavailable=true;const degraded=await visit();try{
  expect(await degraded.text('.program-status')).toContain('Installed tools could not be checked');
  await degraded.click(button(/^Community learning education(?: |$)/));await degraded.wait(visible(heading('Requirements')));
  expect(await degraded.text('.program-tools')).toContain('Installed editor availability could not be checked');
 }finally{await degraded.close();}
});

chromeTest('same-organization actor mismatch and late account responses cannot retain private state',async visit=>{
 prepare('institution:studio');const mismatched=await visit('context=institution%3Astudio');try{
  replyActor='read';await mismatched.click(button(/^Community learning education(?: |$)/));await cleared(mismatched);
 }finally{replyActor=null;await mismatched.close();}
 const page=await visit('context=institution%3Astudio'),wait=gate();try{
  await choose(page);reviewGate=wait;await page.click(button('Review setup'));await waitForGate(wait.entered.promise,'review request entered');
  actor='user:20';await page.evaluate(()=>window.dispatchEvent(new Event('bonfyre:account-changed')));
  expect(await page.text('.program-detail')).toBe('');expect(await page.text('.program-list')).toBe('');
  wait.release.resolve();await waitForGate(wait.finished.promise,'review response released');await page.evaluate(()=>new Promise(done=>requestAnimationFrame(()=>requestAnimationFrame(done))));
  expect(await page.text('.program-detail')).toBe('');expect(await page.run('Number(Boolean('+button('Open prepared program')+'))')).toBe(0);
 }finally{wait.release.resolve();await page.close();}
});

chromeTest('reviewed template drift refuses preparation and read-only members cannot start it',async visit=>{
 const page=await visit();try{
  await review(page);db.exec("UPDATE program_template SET description='Updated setup'");await page.click(button('Prepare this program'));await page.wait(visible(button('Check saved program')));expect(count()).toBe(0);
  expect(await page.text('.program-status')).toContain('template or destination changed');
 }finally{await page.close();}
 const readonly=await visit('readonly=1');try{await choose(readonly);expect(await readonly.run(button('Review setup')+'.disabled')).toBe(true);}finally{await readonly.close();}
 db.query('UPDATE program_template SET subject_kinds=?').run('["facility"]');const domain=await visit();try{await choose(domain);expect(await domain.run(button('Review setup')+'.disabled')).toBe(true);expect(await domain.text('.program-detail')).toContain('This template needs a destination of this kind: facility.');}finally{await domain.close();}
});

chromeTest('a template link opens the reviewed template unprepared, then only this account\'s prepared program',async visit=>{
 const fresh=await visit('template=learning');try{await fresh.wait(visible(button('Review setup')));expect(await fresh.run('Boolean('+heading('Community learning')+')')).toBe(true);expect(await fresh.run('Number(Boolean('+heading('Requirements')+'))')).toBe(0);}finally{await fresh.close();}
 const instance=prepare(),kept=await visit('template=learning');try{await kept.wait(visible(heading('Requirements')));expect(await kept.run('location.search')).toContain('instance='+instance.instance_id);}finally{await kept.close();}
 actor='user:20';const other=await visit('template=learning'),missing=await visit('template=absent-template');try{await other.wait(visible(button('Review setup')));expect(await other.run('Number(Boolean('+heading('Requirements')+'))')).toBe(0);await missing.wait(visible(named('p','That template is not offered in this destination.')));expect(calls.filter(row=>['review','prepare','change','source-plan','source-start','source-decision'].includes(row.operation))).toEqual([]);}finally{await other.close();await missing.close();}
});

chromeTest('shared rail invalidation clears the page immediately and cached-page recovery rechecks the account',async visit=>{
 const instance=prepare(),page=await visit('context=self&instance='+instance.instance_id);try{
  await page.wait(visible(heading('Requirements')));
  await page.evaluate(()=>window.dispatchEvent(new CustomEvent('bonfyre:activity-context-invalidated',{detail:{context:'self'}})));
  expect(await page.text('.program-detail')).toBe('');expect(await page.text('.program-list')).toBe('');
  actor='user:20';await page.evaluate(()=>window.dispatchEvent(new CustomEvent('bonfyre:activity-context-changed',{detail:{context:'self',binding:{actor:'user:20',subject:'user:20',scope:'self'}}})));
  await page.wait(visible(named('p','That program is not available in this destination.')));expect(await page.run('Number(Boolean('+heading('Requirements')+'))')).toBe(0);
  expect(await page.run(button('Your programs')+'.textContent')).toBe('Your programs');
  await page.evaluate(()=>window.dispatchEvent(new Event('pagehide')));const before=calls.length;
  await page.evaluate(()=>window.dispatchEvent(new CustomEvent('bonfyre:activity-context-changed',{detail:{context:'self'}})));
  expect(calls.length).toBe(before);expect(await page.text('.program-detail')).toBe('');
  await page.evaluate(()=>window.dispatchEvent(new Event('pageshow')));await page.wait(visible(named('p','That program is not available in this destination.')));expect(calls.length).toBeGreaterThan(before);
  await page.evaluate(()=>{window.dispatchEvent(new CustomEvent('bonfyre:activity-context-invalidated',{detail:{context:'institution:studio'}}));window.dispatchEvent(new CustomEvent('bonfyre:activity-context-changed',{detail:{context:'institution:studio'}}));});
  await page.wait("location.pathname+location.search==='/programs?context=institution%3Astudio'");await page.wait(visible(heading('Choose a starting point')));expect(count()).toBe(1);
 }finally{await page.close();}
});

chromeTest('desktop and phone layout keep the prepared requirements and keyboard controls usable',async visit=>{
 for(const [name,viewport] of [['desktop',{width:1360,height:1000}],['phone',{width:390,height:844}]]){
  const page=await visit('',viewport);try{
   await prepared(page);
   expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);
   expect(await page.run(visible(heading('Requirements')))).toBe(true);
   await page.run(button('Refresh')+'.focus()');expect(await page.evaluate(()=>document.activeElement.textContent)).toBe('Refresh');
   if(process.env.BONFYRE_BROWSER_ARTIFACT_DIR){mkdirSync(process.env.BONFYRE_BROWSER_ARTIFACT_DIR,{recursive:true});const {cssContentSize}=await page.tab.page('Page.getLayoutMetrics');const {data}=await page.tab.page('Page.captureScreenshot',{format:'png',captureBeyondViewport:true,clip:{...cssContentSize,scale:1}});writeFileSync(join(process.env.BONFYRE_BROWSER_ARTIFACT_DIR,'programs-'+name+'.png'),Buffer.from(data,'base64'));}
  }finally{await page.close();}
 }
});

const field=label=>node('[aria-label="'+label+'"]');
const area=id=>'[data-program-area="'+id+'"]';
const workRow=id=>node(area('work')+' .ac-row[data-object-id="'+id+'"] .ac-title');
const resultRow=id=>node('.program-source-current .ac-row[data-object-id="'+id+'"] .ac-title');
const sourceCall=(instance,operation,extra={})=>sourceOwner(operation,'self',encode([1,{expectedBinding:binding('self'),instance_id:instance.instance_id,...extra}]));
async function sourceSeed(instance,id='seed-source',query='community learning sources',start=true){const state=await sourceCall(instance,'source-status'),plan=await sourceCall(instance,'source-plan',{collection_id:state.collection.collection_id,collection_revision:state.collection.revision,demand_id:state.collection.demands[0].id,request_id:id,provider:'exa',mode:'public',query,limit:3,characters:5000});return start?sourceCall(instance,'source-start',{job_id:plan.job.id,digest:plan.job.digest,revision:plan.job.revision}):plan;}
async function openSourceRun(page,id){await page.click(button('Sources'));await page.click(button('Saved runs'));await page.click(node('.program-source-work .ac-row[data-object-id="'+id+'"] .ac-title'));await page.click(button('Open saved source run'));await page.wait(visible(node('.program-source-current h4')));}
async function reviewSource(page,id){await page.click(resultRow(id));await page.wait(visible(button('Review and provenance')));await page.click(button('Review and provenance'));await page.wait(visible(field('Review note')));}
async function screenshot(page,name){if(!process.env.BONFYRE_BROWSER_ARTIFACT_DIR)return;mkdirSync(process.env.BONFYRE_BROWSER_ARTIFACT_DIR,{recursive:true});const {cssContentSize}=await page.tab.page('Page.getLayoutMetrics'),{data}=await page.tab.page('Page.captureScreenshot',{format:'png',captureBeyondViewport:true,clip:{...cssContentSize,scale:1}});writeFileSync(join(process.env.BONFYRE_BROWSER_ARTIFACT_DIR,name+'.png'),Buffer.from(data,'base64'));}

chromeTest('deadline drafts survive tabs and requirement inspection, then save through the native owner and reopen',async visit=>{
 const instance=prepare(),[first,second]=instance.requirements,page=await visit('instance='+instance.instance_id);try{
  await page.wait(visible(field('Working deadline')));await page.fill(field('Working deadline'),'2026-12-15');
  await page.click(button('Funding'));await page.click(button('Work'));expect(await page.run(field('Working deadline')+'.value')).toBe('2026-12-15');
  await page.click(workRow(second.requirement_id));await page.wait(field('Working deadline')+'.value===""');await page.click(workRow(first.requirement_id));await page.wait(field('Working deadline')+'.value==="2026-12-15"');
  await page.click(button('Review deadline'));await page.wait(visible(button('Save reviewed deadline')));expect(owner('read','self',undefined,instance.instance_id).instances[0].requirements.find(row=>row.requirement_id===first.requirement_id).deadline).toBe('');
  await page.click(button('Save reviewed deadline'));await page.wait(node('.program-status')+'.textContent==="Working deadline saved."');
  expect(owner('read','self',undefined,instance.instance_id).instances[0].requirements.find(row=>row.requirement_id===first.requirement_id)).toMatchObject({deadline:'2026-12-15',state:'open',evidence_ref:'',satisfied_by:''});
  await screenshot(page,'programs-mature-work-desktop');const address=await page.run('location.href');await page.tab.go(address);await page.wait(field('Working deadline')+'.value==="2026-12-15"');expect(await page.evaluate(()=>localStorage.length+sessionStorage.length)).toBe(0);
 }finally{await page.close();}
});

chromeTest('funding scope and cost codes are reviewed and saved, with keyboard tabs preserving the same editor nodes',async visit=>{
 const instance=prepare(),page=await visit('instance='+instance.instance_id);try{
  await page.click(button('Funding'));await page.fill(field('Scope'),'Weekly workshops\nMentor coordination');await page.fill(field('Cost codes'),'LEARN-101\nLEARN-102');
  await page.run('window.retainedFundingInput='+field('Scope'));await page.click(button('Work'));await page.click(button('Funding'));expect(await page.run('window.retainedFundingInput==='+field('Scope'))).toBe(true);
  await page.click(button('Review funding plan'));await page.wait(visible(button('Save reviewed funding plan')));expect(JSON.parse(db.query('SELECT scope FROM funding_work_package').get().scope)).toEqual([]);
  await page.click(button('Save reviewed funding plan'));await page.wait(node('.program-status')+'.textContent==="Funding scope and cost codes saved."');
  const saved=db.query('SELECT scope,cost_codes,status FROM funding_work_package').get();expect(JSON.parse(saved.scope)).toEqual(['Weekly workshops','Mentor coordination']);expect(JSON.parse(saved.cost_codes)).toEqual(['LEARN-101','LEARN-102']);expect(saved.status).toBe('draft');
  await page.run(button('Funding')+'.focus()');await page.tab.page('Input.dispatchKeyEvent',{type:'keyDown',key:'ArrowRight'});expect(await page.evaluate(()=>document.activeElement.textContent)).toBe('Overview');expect(await page.run(button('Funding')+'.getAttribute("aria-selected")')).toBe('true');
  await screenshot(page,'programs-mature-funding-desktop');
 }finally{await page.close();}
});

chromeTest('lost change acknowledgement retains one exact request across unavailable and successful receipt reads',async visit=>{
 const instance=prepare(),page=await visit('instance='+instance.instance_id);try{
  await page.fill(field('Working deadline'),'2026-12-16');await page.click(button('Review deadline'));loseChange=true;await page.click(button('Save reviewed deadline'));await page.wait(visible(button('Check saved change')));
  expect(db.query('SELECT COUNT(*) n FROM _program_workspace_change').get().n).toBe(1);expect(await page.run(button('Review deadline')+'.matches(":disabled")')).toBe(true);
  readUnavailable=true;await page.click(button('Check saved change'));await page.wait(node('.program-status')+'.dataset.error==="true"');readUnavailable=false;await page.click(button('Check saved change'));await page.wait(node('.program-status')+'.textContent==="Save confirmed. The current saved program is open."');
  const changes=calls.filter(row=>row.operation==='change'),receipts=calls.filter(row=>row.operation==='change-receipt');expect(changes).toHaveLength(1);expect(receipts).toHaveLength(2);expect(new Set([changes[0].body.request_id,...receipts.map(row=>row.body.request_id)]).size).toBe(1);expect(await page.run(field('Working deadline')+'.value')).toBe('2026-12-16');
 }finally{await page.close();}
});

chromeTest('concurrent saved changes preserve the draft and require a fresh comparison before the next CAS',async visit=>{
 const instance=prepare(),id=instance.requirements[0].requirement_id,page=await visit('instance='+instance.instance_id);try{
  await page.fill(field('Working deadline'),'2026-12-17');await page.click(button('Review deadline'));await page.wait(visible(button('Save reviewed deadline')));db.query('UPDATE program_requirement SET deadline=? WHERE requirement_id=?').run('2026-12-18',id);
  await page.click(button('Save reviewed deadline'));await page.wait(visible(button('Read current program')));expect(db.query('SELECT deadline FROM program_requirement WHERE requirement_id=?').get(id).deadline).toBe('2026-12-18');
  await page.click(button('Read current program'));await page.wait(field('Working deadline')+'.value==="2026-12-17"');await page.click(button('Review deadline'));await page.wait(visible(button('Save reviewed deadline')));expect(await page.text('.program-plan-review')).toContain('2026-12-18');
  await page.click(button('Save reviewed deadline'));await page.wait(node('.program-status')+'.textContent==="Working deadline saved."');expect(db.query('SELECT deadline FROM program_requirement WHERE requirement_id=?').get(id).deadline).toBe('2026-12-17');const saves=calls.filter(row=>row.operation==='change');expect(saves).toHaveLength(2);expect(saves[0].body.expected_revision).not.toBe(saves[1].body.expected_revision);
 }finally{await page.close();}
});

chromeTest('saved-record attachment uses the native declared view, detects newer work, updates and detaches without deleting it',async visit=>{
 const instance=prepare(),page=await visit('instance='+instance.instance_id);try{
  await page.click(button('Attach saved work'));await page.wait(visible(named('.program-record-picker .ac-title',/^Workshop plan/)));expect(await page.text('.program-record-picker')).toContain('1 without a declared view');expect(await page.text('.program-record-picker')).not.toContain('Unsupported editor record');
  await page.click(named('.program-record-picker .ac-title',/^Workshop plan/));await page.click(button('Attach this saved version'));await page.wait(node('.program-status')+'.textContent.startsWith("Saved work attached")');
  let saved=owner('read','self',undefined,instance.instance_id).instances[0];expect(saved.work_links).toHaveLength(1);expect(saved.work_links[0].record_revision).toBe(3);expect(await page.run(link('Workshop plan')+'.getAttribute("href")')).toBe(saved.work_links[0].href);expect(saved.requirements.every(row=>row.state==='open')).toBe(true);
  recordRevision=4;await page.click(button('Check current version'));await page.wait(node('.program-records small')+'.textContent.includes("version 4")');expect(await page.run(node('.program-record-title')+'.hasAttribute("href")')).toBe(false);
  await page.click(button('Update saved version'));await page.wait(visible(button('Attach this saved version')));await page.click(button('Attach this saved version'));await page.wait(node('.program-status')+'.textContent.startsWith("Saved work attached")');saved=owner('read','self',undefined,instance.instance_id).instances[0];expect(saved.work_links).toHaveLength(1);expect(saved.work_links[0].record_revision).toBe(4);
  await page.click(button('Detach from program'));await page.click(button('Detach this reference'));await page.wait(node('.program-status')+'.textContent==="Saved-work reference detached."');expect(owner('read','self',undefined,instance.instance_id).instances[0].work_links).toEqual([]);expect(recordRevision).toBe(4);
 }finally{await page.close();}
});

chromeTest('saved-record paging retains the native epoch and reaches later declared views after an explicit reload',async visit=>{
 recordPageSize=1;metadataLeadRecord=true;const instance=prepare(),page=await visit('instance='+instance.instance_id);try{
  await page.click(button('Attach saved work'));await page.wait(node('.program-record-state')+'.textContent.startsWith("Page 1")');expect(await page.text('.program-record-picker')).toContain('This page has no records with a declared editor view.');expect(await page.run(link('Open Source library')+'.getAttribute("href")')).toBe('/apps/'+workspace+'/sources/');expect(await page.run(link('Open Source library')+'.target')).toBe('_blank');expect(await page.run(button('Previous saved records')+'.disabled')).toBe(true);
  await page.click(button('Next saved records'));await page.wait(visible(named('.program-record-picker .ac-title',/^Workshop plan/)));expect(await page.text('.program-record-state')).toContain('Page 2');await page.click(button('Previous saved records'));await page.wait(node('.program-record-state')+'.textContent.startsWith("Page 1")');
  recordRevision=4;await page.click(button('Next saved records'));await page.wait(node('.program-record-state')+'.textContent.includes("The library changed while reading")');expect(await page.run('document.querySelectorAll(".program-record-picker .ac-row").length')).toBe(0);expect(owner('read','self',undefined,instance.instance_id).instances[0].work_links).toEqual([]);
  await page.click(button('Read saved records'));await page.wait(node('.program-record-state')+'.textContent.startsWith("Page 1")');await page.click(button('Next saved records'));await page.wait(visible(named('.program-record-picker .ac-title',/^Workshop plan/)));await page.click(named('.program-record-picker .ac-title',/^Workshop plan/));await page.click(button('Attach this saved version'));await page.wait(node('.program-status')+'.textContent.startsWith("Saved work attached")');const links=owner('read','self',undefined,instance.instance_id).instances[0].work_links;expect(links).toHaveLength(1);expect(links[0]).toMatchObject({record_key:recordKey,record_revision:4});
 }finally{await page.close();}
});

chromeTest('source search plans, explicit execution, retained text review and source-library continuation use real owners',async visit=>{
 const instance=prepare(),page=await visit('instance='+instance.instance_id);try{
  await page.click(button('Sources'));await page.fill(field('Search query'),'community learning local partners');await page.click(button('Save source plan'));await page.wait(visible(button('Start reviewed source run')));expect(providerEffects).toBe(0);expect(calls.filter(row=>row.operation==='source-plan')).toHaveLength(1);
  await page.click(button('Start reviewed source run'));await page.wait(visible(resultRow(retainedItem().id)));expect(providerEffects).toBe(1);
  await reviewSource(page,retainedItem().id);await page.fill(field('Research review'),'shortlist');await page.fill(field('Review note'),'Useful evidence for planning the workshops.');await page.click(button('Save source review'));await page.wait(node('.program-source-status')+'.textContent==="Research review saved to this retained result."');
  const relation=db.query('SELECT job_id FROM _program_web_work').get(),current=await sourceCall(instance,'source-read',{job_id:relation.job_id});expect(current.job.items[0]).toMatchObject({decision:'shortlist',notes:'Useful evidence for planning the workshops.',revision:2});expect(db.query("SELECT COUNT(*) n FROM program_requirement WHERE state<>'open'").get().n).toBe(0);
  await page.click(node('.program-source-current input[type=checkbox][value="'+retainedItem().id+'"]'));const continuation=await page.run(link('Continue selected in source library')+'.getAttribute("href")');expect(continuation).toContain('job='+relation.job_id);expect(continuation).toContain('items='+retainedItem().id);expect(await page.run(link('Continue selected in source library')+'.target')).toBe('_blank');
  await page.click(button('Read saved source outcome'));await page.wait(visible(resultRow(retainedItem().id)));expect(providerEffects).toBe(1);await screenshot(page,'programs-mature-sources-desktop');
 }finally{await page.close();}
});

chromeTest('lost source-plan and source-start acknowledgements recover exact native identities without replaying provider work',async visit=>{
 const instance=prepare(),page=await visit('instance='+instance.instance_id);try{
  await page.click(button('Sources'));await page.fill(field('Search query'),'community learning source recovery');loseSourcePlan=true;await page.click(button('Save source plan'));await page.wait(visible(button('Check saved source plan')));await page.click(button('Check saved source plan'));await page.wait(visible(button('Start reviewed source run')));expect(providerEffects).toBe(0);expect(calls.filter(row=>row.operation==='source-plan')).toHaveLength(1);
  loseSourceStart=true;await page.click(button('Start reviewed source run'));await page.wait(visible(button('Check original source run')));expect(providerEffects).toBe(1);expect(await page.run(button('Start reviewed source run')+'.disabled')).toBe(true);await page.click(button('Check original source run'));await page.wait(visible(resultRow(retainedItem().id)));expect(calls.filter(row=>row.operation==='source-start')).toHaveLength(1);expect(providerEffects).toBe(1);
 }finally{await page.close();}
});

chromeTest('a retained source intent without a native plan offers same-request recovery and retry',async visit=>{
 const instance=prepare(),page=await visit('instance='+instance.instance_id);try{
  await page.click(button('Sources'));await page.fill(field('Search query'),'recover an interrupted source plan');stopPlanBeforeNative=true;await page.click(button('Save source plan'));await page.wait(visible(button('Check saved source plan')));await page.click(button('Check saved source plan'));await page.wait(visible(button('Retry the same source plan')));expect(db.query('SELECT job_id FROM _program_web_work').get().job_id).toBeNull();
  await page.click(button('Retry the same source plan'));await page.wait(visible(button('Start reviewed source run')));const plans=calls.filter(row=>row.operation==='source-plan');expect(plans).toHaveLength(2);expect(plans[0].body).toEqual(plans[1].body);expect(db.query('SELECT COUNT(*) n FROM _program_web_work').get().n).toBe(1);expect(providerEffects).toBe(0);
 }finally{await page.close();}
});

chromeTest('stale source collection recovery can return to a current plan without losing the edited query',async visit=>{
 const instance=prepare(),page=await visit('instance='+instance.instance_id);try{
  await page.click(button('Sources'));await page.fill(field('Search query'),'retain this research question');db.query('UPDATE source_collection SET updated_at=?').run('changed-after-source-review');await page.click(button('Save source plan'));await page.wait(visible(button('Check saved source plan')));await page.click(button('Check saved source plan'));await page.wait(visible(button('Review a new source plan')));await page.click(button('Review a new source plan'));await page.wait('!'+button('Save source plan')+'.disabled');expect(await page.run(field('Search query')+'.value')).toBe('retain this research question');
  await page.click(button('Save source plan'));await page.wait(visible(button('Start reviewed source run')));const plans=calls.filter(row=>row.operation==='source-plan');expect(plans).toHaveLength(2);expect(plans[0].body.request_id).not.toBe(plans[1].body.request_id);expect(plans[0].body.collection_revision).not.toBe(plans[1].body.collection_revision);expect(providerEffects).toBe(0);
 }finally{await page.close();}
});

chromeTest('dirty source notes are compared with a newer owner revision before an explicit reviewed save',async visit=>{
 const instance=prepare(),seed=await sourceSeed(instance),page=await visit('instance='+instance.instance_id);try{
  await openSourceRun(page,'seed-source');await reviewSource(page,retainedItem().id);await page.fill(field('Research review'),'shortlist');await page.fill(field('Review note'),'My unsaved research note.');await page.click(resultRow(retainedItem('two').id));await page.wait(visible(button('Retained text')));
  await sourceCall(instance,'source-decision',{job_id:seed.job.id,item_id:retainedItem().id,revision:1,decision:'discard',notes:'Another editor reviewed this source.'});await reviewSource(page,retainedItem().id);expect(await page.run(field('Review note')+'.value')).toBe('My unsaved research note.');await page.click(button('Review against saved changes'));await page.wait(visible(button('Save reviewed source change')));expect(await page.text('.program-source-current .program-plan-review')).toContain('Another editor reviewed this source.');
  expect((await sourceCall(instance,'source-read',{job_id:seed.job.id})).job.items[0].revision).toBe(2);await page.click(button('Save reviewed source change'));await page.wait(node('.program-source-status')+'.textContent==="Research review saved to this retained result."');const after=await sourceCall(instance,'source-read',{job_id:seed.job.id});expect(after.job.items[0]).toMatchObject({revision:3,decision:'shortlist',notes:'My unsaved research note.'});expect(calls.filter(row=>row.operation==='source-decision')[0].body.revision).toBe(2);
 }finally{await page.close();}
});

chromeTest('an uncertain source review stays attached to its original job when another run is opened',async visit=>{
 const instance=prepare(),first=await sourceSeed(instance,'first-source'),second=await sourceSeed(instance,'second-source','another source question',false),page=await visit('instance='+instance.instance_id),wait=gate();try{
  await openSourceRun(page,'first-source');await reviewSource(page,retainedItem().id);await page.fill(field('Review note'),'Keep this original review.');sourceDecisionGate=wait;await page.click(button('Save source review'));await waitForGate(wait.entered.promise,'source decision entered');expect(await page.run(field('Review note')+'.disabled')).toBe(true);await page.click(button('Stop waiting'));await page.wait(visible(button('Check original source run')));
  await page.click(button('Saved runs'));await page.click(node('.program-source-work .ac-row[data-object-id="second-source"] .ac-title'));await page.click(button('Open saved source run'));await page.wait(visible(button('Start reviewed source run')));expect(await page.run(button('Start reviewed source run')+'.disabled')).toBe(true);expect(await page.run(visible(button('Check original source run')))).toBe(true);
  wait.release.resolve();await waitForGate(wait.finished.promise,'source decision released');await page.click(button('Check original source run'));await page.wait(visible(resultRow(retainedItem().id)));expect(await page.run(visible(button('Check original source run')))).toBe(false);expect((await sourceCall(instance,'source-read',{job_id:first.job.id})).job.items[0].notes).toBe('Keep this original review.');expect((await sourceCall(instance,'source-read',{job_id:second.job.id})).job.state).toBe('planned');expect(providerEffects).toBe(1);
 }finally{wait.release.resolve();await page.close();}
});

chromeTest('late source text clears on account change and read-only members cannot edit retained reviews',async visit=>{
 const instance=prepare();await sourceSeed(instance);const page=await visit('instance='+instance.instance_id),wait=gate();try{
  await openSourceRun(page,'seed-source');sourceItemGate=wait;await page.click(resultRow(retainedItem().id));await waitForGate(wait.entered.promise,'source item retained');actor='user:20';await page.evaluate(()=>window.dispatchEvent(new Event('bonfyre:account-changed')));expect(await page.text('.program-detail')).toBe('');expect(await page.text('.program-list')).toBe('');wait.release.resolve();await waitForGate(wait.finished.promise,'source item released');await page.evaluate(()=>new Promise(done=>requestAnimationFrame(()=>requestAnimationFrame(done))));expect(await page.text('.program-detail')).toBe('');
 }finally{wait.release.resolve();await page.close();}
 actor='user:19';const readonly=await visit('instance='+instance.instance_id+'&readonly=1');try{await openSourceRun(readonly,'seed-source');await reviewSource(readonly,retainedItem().id);expect(await readonly.run(field('Review note')+'.disabled')).toBe(true);expect(await readonly.run(field('Research review')+'.disabled')).toBe(true);expect(await readonly.run(button('Save source review')+'.disabled')).toBe(true);expect(await readonly.run(button('Save source plan')+'.matches(":disabled")')).toBe(true);}finally{await readonly.close();}
});

chromeTest('phone source work retains usable forms, split collections and exact saved results without horizontal overflow',async visit=>{
 const instance=prepare();await sourceSeed(instance);const page=await visit('instance='+instance.instance_id,{width:390,height:844});try{
  await page.wait(visible(field('Working deadline')));expect(await page.run(visible(node('.program-index')))).toBe(false);await page.click(button('Browse programs'));expect(await page.run(visible(node('.program-index')))).toBe(true);await page.click(button('Browse programs'));await screenshot(page,'programs-mature-work-phone');await openSourceRun(page,'seed-source');await page.click(resultRow(retainedItem().id));await page.wait(visible(button('Retained text')));expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);expect(await page.run(visible(field('Search query')))).toBe(false);await screenshot(page,'programs-mature-sources-phone');
 }finally{await page.close();}
});
