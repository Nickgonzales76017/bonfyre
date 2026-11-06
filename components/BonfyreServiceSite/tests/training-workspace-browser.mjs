import assert from 'node:assert/strict';
import {execFileSync} from 'node:child_process';
import {mkdtempSync,rmSync,mkdirSync,writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,resolve} from 'node:path';
import {withChrome} from '../../../integrations/browser/capture/chrome-session.mjs';
import {trainingMaterial,trainingHandler} from '../training-workspace.mjs';
import {requestStore} from '../training-requests.mjs';
import {creatorAuth,creatorInstitution} from '../training-creator.mjs';

const home=mkdtempSync(join(tmpdir(),'training-workspace-')),db=join(home,'auth.db');
const out=resolve(process.env.WORKSPACE_BROWSER_PROOF??'packs/no-simulation/product-maturity-20261015/workspace');mkdirSync(out,{recursive:true});
const material=trainingMaterial('packs/aurekai-fitness-studio'),requests=requestStore(join(home,'requests'),material),auth=creatorAuth(db);
const native=(command,args=[],input='')=>JSON.parse(execFileSync('cmd/BonfyreAuth/bonfyre-auth',['--db',db,command,...args],{input,encoding:'utf8',stdio:'pipe'}));
const account=(email,name)=>{native('http-signup',[],JSON.stringify({email,name,password:'workspace-test-password'}));const login=native('http-login',[],JSON.stringify({email,password:'workspace-test-password'}));native('actor-bind',['--user-id',String(login.user.id),'--actor','actor:'+name]);return login.user.id;};
let handler;const server=Bun.serve({hostname:'127.0.0.1',port:0,fetch:r=>handler(r)});handler=trainingHandler(material,server.port,{auth,requests});
const origin=`http://127.0.0.1:${server.port}`,report={origin,checks:[],status:'running'};
const check=(name,condition)=>{assert(condition,name);report.checks.push(name);console.log('PASS '+name);};
try{
  const owner=account('owner@example.test','owner'),member=account('member@example.test','member');
  native('institution-grant',['--user-id',String(owner),'--institution',creatorInstitution('forge'),'--role','admin']);
  native('institution-grant',['--user-id',String(owner),'--institution',creatorInstitution('still'),'--role','read']);
  for(const studio of ['forge','still'])requests.submit({id:crypto.randomUUID(),creatorId:studio,programId:material.creators.find(c=>c.id===studio).routines[0].id,title:studio==='forge'?'Travel strength':'Evening mobility',details:'Prepare a short routine for my next session.'});
  await withChrome(async({open,events})=>{
    const tab=await open();
    const evaluate=async expression=>{const r=await tab.page('Runtime.evaluate',{expression,awaitPromise:true,returnByValue:true});if(r.exceptionDetails)throw Error(JSON.stringify(r.exceptionDetails));return r.result.value;};
    const wait=expression=>evaluate(`new Promise((resolve,reject)=>{const start=Date.now();const poll=()=>{if(${expression})return resolve(true);if(Date.now()-start>15000)return reject(Error('Readiness timeout: '+${JSON.stringify(expression)}));setTimeout(poll,30)};poll()})`);
    const click=async selector=>{await evaluate(`document.querySelector(${JSON.stringify(selector)}).click()`);await wait(`document.querySelector('#connection').textContent!=='Working…'`);};
    const login=async email=>{await wait(`Boolean(document.querySelector('#creator-login'))`);await evaluate(`(()=>{const f=document.querySelector('#creator-login');f.elements.email.value=${JSON.stringify(email)};f.elements.password.value='workspace-test-password';f.requestSubmit()})()`);await wait(`document.querySelector('#sign-out').hidden===false`);await wait(`document.querySelector('#connection').textContent!=='Working…'`);};
    await tab.go(origin+'/creator/forge/requests');await login('owner@example.test');
    check('deep link opens the requested authorized studio',await evaluate(`location.pathname==='/creator/forge/requests'&&document.querySelectorAll('#studio option').length===2`));
    check('request inbox excludes the other studio',await evaluate(`document.querySelector('#creator-view').textContent.includes('Travel strength')&&!document.querySelector('#creator-view').textContent.includes('Evening mobility')`));
    await click('[data-action="requests.filter"][data-filter="mine"]');check('assigned-to-me view has an honest empty state',await evaluate(`document.querySelector('.empty').textContent.includes('No requests match')`));
    await click('[data-action="requests.filter"][data-filter="all"]');await click('[data-action="accept"]');
    await click('[data-action="requests.filter"][data-filter="mine"]');check('native assignment appears in the personal queue',await evaluate(`Boolean(document.querySelector('[data-action="build"]'))`));
    await click('[data-action="mode.account"]');
    check('account screen shows both studios and distinct permissions',await evaluate(`location.pathname==='/creator/forge/account'&&document.querySelectorAll('.access-card').length===2&&document.querySelector('#creator-view').textContent.includes('Administrator')&&document.querySelector('#creator-view').textContent.includes('Can view')`));
    await tab.go(origin+'/creator/forge/account');await wait(`Boolean(document.querySelector('.access-card'))`);check('account route survives a full reload',true);
    await click('[data-workspace="still"]');check('workspace switch retains the current task view',await evaluate(`location.pathname==='/creator/still/account'&&document.querySelector('#studio').value==='still'`));
    await click('[data-action="mode.requests"]');check('read-only studio hides mutation and administration actions',await evaluate(`document.querySelector('#creator-view').textContent.includes('Evening mobility')&&!document.querySelector('[data-action="accept"]')&&!document.querySelector('[data-action="mode.team"]')`));
    await evaluate('history.back()');await wait(`location.pathname==='/creator/still/account'&&Boolean(document.querySelector('.access-card'))`);check('browser Back restores the prior task view',true);
    await click('[data-workspace="forge"]');await click('[data-action="mode.team"]');await wait(`Boolean(document.querySelector('#team-add'))`);
    await evaluate(`(()=>{const f=document.querySelector('#team-add');f.elements.email.value='member@example.test';f.elements.role.value='write';f.requestSubmit()})()`);await wait(`Boolean(document.querySelector('[data-action="team.confirm"]'))`);
    check('access change requires review before native mutation',!auth.allowed(auth.login({email:'member@example.test',password:'workspace-test-password'}).token,'forge','read'));
    await click('[data-action="team.confirm"]');await wait(`Boolean(document.querySelector('[data-member="member@example.test"]'))`);
    check('confirmed team change persists in native Auth',true);
    for(const width of [1440,390]){await tab.page('Emulation.setDeviceMetricsOverride',{width,height:960,deviceScaleFactor:1,mobile:false});await evaluate('document.fonts.ready');check(`team view fits ${width}px`,await evaluate('document.documentElement.scrollWidth<=innerWidth'));const shot=await tab.page('Page.captureScreenshot',{format:'png'});writeFileSync(join(out,`team-${width}.png`),Buffer.from(shot.data,'base64'));}
    await click('#sign-out');await login('member@example.test');await click('[data-action="mode.requests"]');check('new member can open the granted workspace after signing in',await evaluate(`document.querySelectorAll('#studio option').length===1&&!document.querySelector('[data-action="mode.team"]')`));
    native('institution-revoke',['--user-id',String(member),'--institution',creatorInstitution('forge')]);await click('[data-action="refresh"]');await wait(`Boolean(document.querySelector('#creator-login'))`);check('revocation removes private data from an existing signed-in session',await evaluate(`!document.querySelector('.creator-request')`));
    check('no browser runtime exceptions',events.filter(e=>e.method==='Runtime.exceptionThrown').length===0);
  },{timeoutMs:90000});
  report.status='passed';
}catch(error){report.status='failed';report.error=error.stack;process.exitCode=1;}
finally{server.stop(true);rmSync(home,{recursive:true,force:true});writeFileSync(join(out,'verification.json'),JSON.stringify(report,null,2)+'\n');console.log(JSON.stringify(report));}
