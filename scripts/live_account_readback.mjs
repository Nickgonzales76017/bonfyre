// Read-only live QA in one disposable tab of an already signed-in Chrome window.
// No password/cookie reads, new accounts, signup, logout, grants or customer writes.
import {mkdirSync,writeFileSync} from 'node:fs';
import {homedir} from 'node:os';
import {join} from 'node:path';
import {encode} from '../integrations/browser/workspace-msgpack.mjs';
if(!process.argv.includes('--run'))throw Error('Use --run for a bounded live browser verification.');
const deadline=Date.now()+180000,results=[];let windowId=0,tabId=0,principal=null,passed=false;
const apple=lines=>{
 const r=Bun.spawnSync(['/usr/bin/osascript','-'],{stdin:new TextEncoder().encode(lines.join('\n')),stdout:'pipe',stderr:'pipe',timeout:12000});
 if(r.exitCode)throw Error(r.stderr.toString().slice(0,400));return r.stdout.toString().trim();
};
const js=code=>apple(['tell application "Google Chrome"',`tell window id ${windowId}`,`set targetTab to first tab whose id is ${tabId}`,`return execute targetTab javascript ${JSON.stringify(code)}`,'end tell','end tell']);
async function wait(predicate,label){
 const until=Math.min(deadline,Date.now()+20000);
 while(Date.now()<until){try{const text=js('JSON.stringify('+predicate+')');if(text&&text!=='missing value'){const value=JSON.parse(text);if(value)return value;}}catch{}await Bun.sleep(250);}
 throw Error('Timed out: '+label);
}
async function inspect(host,path='/'){
 await wait(`location.hostname===${JSON.stringify(host)}&&location.pathname===${JSON.stringify(path)}&&document.readyState==='complete'&&document.querySelector('.ak-session')?.dataset.sessionState==='signed-in'`,'signed-in '+host+path);
 js("window.__bonfyreReadQA={pending:true};void (async()=>{try{const r=await fetch('/account',{credentials:'same-origin',cache:'no-store',headers:{Accept:'application/json'},signal:AbortSignal.timeout(10000)}),body=await r.json();window.__bonfyreReadQA={status:r.status,valid:body.valid===true,principal:Number(body.user?.id)||null};}catch{window.__bonfyreReadQA={error:'same-origin account read failed'};}})();'started'");
 const account=await wait('window.__bonfyreReadQA&&!window.__bonfyreReadQA.pending&&window.__bonfyreReadQA','account read '+host);
 if(account.status!==200||!account.valid||!account.principal)throw Error('Live account verification refused at '+host);
 if(principal===null)principal=account.principal;if(principal!==account.principal)throw Error('Territory changed the principal');
 const dom=await wait("(()=>{if(document.querySelector('.ak-session')?.dataset.sessionState!=='signed-in')return false;return {host:location.hostname,path:location.pathname,session:document.querySelector('.ak-session')?.dataset.sessionState,profile:!!document.querySelector('[data-public-profile]'),visibleAuthLinks:[...document.querySelectorAll('a[href]')].filter(a=>{const u=new URL(a.href),r=a.getBoundingClientRect(),s=getComputedStyle(a);return u.origin===location.origin&&['/login','/signup'].includes(u.pathname)&&r.width>0&&r.height>0&&s.visibility!=='hidden'&&s.display!=='none'}).map(a=>({path:new URL(a.href).pathname,label:a.textContent.trim().slice(0,70)})),overflow:document.documentElement.scrollWidth>innerWidth};})()",'stable account menu '+host);
 const result={...dom,samePrincipal:true,networkAccountStatus:account.status};results.push(result);
 if(dom.visibleAuthLinks.length)throw Error('Signed-in page retains visible login/signup at '+host+path);
 console.log('PASS live signed-in '+host+path);return result;
}
try{
 const created=apple([
  'if application "Google Chrome" is not running then return "no-session"','tell application "Google Chrome"',
  'repeat with w in windows','repeat with t in tabs of w','if URL of t starts with "https://aurekai.work/" then',
  'set found to execute t javascript "String(document.querySelector(\'.ak-session\')?.dataset.sessionState===\'signed-in\')"',
  'if found is "true" then','set qaTab to make new tab at end of tabs of w with properties {URL:"https://aurekai.work/"}',
  'return (id of w as text) & ":" & (id of qaTab as text)','end if','end if','end repeat','end repeat','return "no-signed-in-work-tab"','end tell'
 ]);
 if(!/^\d+:\d+$/.test(created))throw Error('No existing signed-in WORK browser tab');
 [windowId,tabId]=created.split(':').map(Number);await inspect('aurekai.work');
 // Euler walk covers each of the twelve directed territory pairs once.
 const only=process.argv.find(a=>a.startsWith('--only='))?.slice(7);
 if(only&&!['life','live','shop'].includes(only))throw Error('Unsupported target territory.');
 for(const to of only?[only,'work']:['life','work','live','work','shop','life','live','life','shop','live','shop','work']){
  const from=results.at(-1).host;
  const selected=js(`(()=>{const a=[...document.querySelectorAll('.ak-bar a[href]')].find(a=>{try{return new URL(a.dataset.territoryHref||a.getAttribute('href'),location.origin).hostname==='aurekai.${to}'}catch{return false}});if(!a)throw Error('Ordinary territory link absent');setTimeout(()=>a.click(),40);return 'selected';})()`);
  if(selected!=='selected')throw Error('Cannot select territory '+to);const dom=await inspect('aurekai.'+to);dom.from=from;
 }
 for(const path of only?[]:['/account','/apps?context=self','/packs/setup','/cms/']){
  js(`setTimeout(()=>location.assign(${JSON.stringify('https://aurekai.work'+path)}),40);'selected'`);
  await inspect('aurekai.work',path.split('?')[0]);
 }
 passed=true;
}catch(error){
 try{if(tabId)results.push({failure:JSON.parse(js("JSON.stringify({host:location.hostname,path:location.pathname,ready:document.readyState,title:document.title,errorCode:document.querySelector('.error-code')?.textContent||null,heading:document.querySelector('h1')?.textContent?.slice(0,160),session:document.querySelector('.ak-session')?.dataset.sessionState||'absent',navigationRefresh:document.querySelector('[data-account-slot]')?.dataset.accountRefresh||'absent',authEntryLabels:[...document.querySelectorAll('form button')].map(n=>n.textContent.slice(0,80))})"))});}catch{}
 try{if(tabId){const address=new URL(apple(['tell application "Google Chrome"',`tell window id ${windowId}`,`return URL of (first tab whose id is ${tabId})`,'end tell','end tell']));results.push({failedAddress:{host:address.hostname,port:address.port,path:address.pathname}});}}catch{}
 console.error('Live browser verification: '+error.message);process.exitCode=1;
}
finally{
 if(tabId&&windowId){try{apple(['tell application "Google Chrome"',`tell window id ${windowId}`,`close (first tab whose id is ${tabId})`,'end tell','end tell']);}catch(error){passed=false;console.error('QA tab cleanup: '+error.message);process.exitCode=1;}}
 const proof={schema:'bonfyre.live-signed-in-qa.v1',at:new Date().toISOString(),passed,principal,results,createdAccounts:0,customerCredentialsRead:false,realPublicHTTPS:true};
 const out=process.env.BONFYRE_LIVE_QA_PROOF||join(homedir(),'.bonfyre/releases/live-signed-in-20261020');mkdirSync(out,{recursive:true,mode:0o700});writeFileSync(join(out,'browser-readback.msgpack'),encode(proof),{mode:0o600});
 console.log(JSON.stringify(proof));
}
