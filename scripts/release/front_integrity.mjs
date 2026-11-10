// Rendered integrity of the territory fronts, read in a real Chrome. Each class
// here was once found by hand on aurekai.*: a section drawn with no sheet, a
// byline run together, the bar pushed under the dock, a collapsed body, a page
// wider than the phone, a public front linking an outreach sample, a slow load.
import {readFileSync,readdirSync} from 'node:fs';
import {join,dirname} from 'node:path';
import {fileURLToPath} from 'node:url';
const root=join(dirname(fileURLToPath(import.meta.url)),'../..');
export const hosts=['aurekai.life','aurekai.live','aurekai.shop','aurekai.work'];
export const budgets={ttfb:+(process.env.BONFYRE_FRONT_TTFB_MS||800),load:+(process.env.BONFYRE_FRONT_LOAD_MS||4000),resource:+(process.env.BONFYRE_FRONT_RESOURCE_MS||1500)};
// Classes some house sheet styles; one used on a page whose sheets never mention it was drawn bare.
export function styledClasses(dir=join(root,'site/matrix')){
 const names=new Set();
 for(const file of readdirSync(dir).filter(name=>name.endsWith('.css')))for(const m of readFileSync(join(dir,file),'utf8').matchAll(/\.(-?[A-Za-z_][\w-]*)/g))names.add(m[1]);
 return [...names];
}
function probe({styled,budgets}){
 const out=[],add=(kind,detail,hard=true)=>out.push({kind,detail:String(detail).slice(0,200),hard});
 const loaded=new Set();const walk=rules=>{for(const r of rules){if(r.selectorText)for(const m of r.selectorText.matchAll(/\.(-?[A-Za-z_][\w-]*)/g))loaded.add(m[1]);if(r.cssRules)walk(r.cssRules);}};
 for(const s of document.styleSheets){try{walk(s.cssRules);}catch{}}
 const known=new Set(styled),bare=new Map(),main=document.querySelector('main')||document.body;
 for(const el of main.querySelectorAll('[class]'))for(const c of el.classList)if(known.has(c)&&!loaded.has(c))bare.set(c,(bare.get(c)||0)+1);
 const unstyled=[...bare].filter(([,n])=>n>=2);if(unstyled.length)add('unstyled',unstyled.map(([c,n])=>c+'×'+n).join(' '));
 for(const el of main.querySelectorAll('*')){
  const kids=[...el.children].filter(k=>k.textContent.trim()&&getComputedStyle(k).display.startsWith('inline'));if(kids.length<2)continue;
  for(let i=1;i<kids.length;i++){const a=kids[i-1],b=kids[i];let n=a.nextSibling,space=false;while(n&&n!==b){if(n.nodeType===3&&/\s/.test(n.textContent))space=true;n=n.nextSibling;}
   const ra=a.getBoundingClientRect(),rb=b.getBoundingClientRect();
   if(!space&&ra.width&&rb.width&&Math.abs(ra.top-rb.top)<2&&rb.left-ra.right<1.5&&getComputedStyle(el).display!=='flex'){add('run-together',a.textContent.trim()+'|'+b.textContent.trim());break;}}
  if(out.filter(f=>f.kind==='run-together').length>=5)break;
 }
 for(const el of main.querySelectorAll('section,[class*="-body"]')){const r=el.getBoundingClientRect(),cs=getComputedStyle(el);
  if(cs.display!=='none'&&cs.visibility!=='hidden'&&el.textContent.trim().length>40&&r.height<24&&!el.closest('[hidden],details:not([open])'))add('collapsed',(el.className||el.tagName)+' '+Math.round(r.height)+'px');}
 const bar=document.querySelector('.ak-bar,header');if(bar){const r=bar.getBoundingClientRect();if(r.top<-1||r.left<-1||r.right>innerWidth+1)add('bar-offscreen',Math.round(r.left)+','+Math.round(r.top)+' '+Math.round(r.right)+'/'+innerWidth);
  const head=main.querySelector('h1,h2');if(head&&scrollY===0&&getComputedStyle(bar).position!=='static'){const h=head.getBoundingClientRect();if(h.top<r.bottom-1&&h.bottom>r.top)add('under-bar',head.textContent.trim());}}
 if(document.documentElement.scrollWidth>innerWidth+1)add('overflow',document.documentElement.scrollWidth+'>'+innerWidth);
 const arrival=document.querySelector('[data-front-arrival]');if(!arrival)add('arrival-missing','shared estate arrival layer');else{const doors=arrival.querySelectorAll('[data-arrival-door]').length,icons=arrival.querySelectorAll('[data-arrival-icon] svg').length;if(doors!==7||icons!==7)add('arrival-contract',doors+' doors / '+icons+' icons');
  const account=[...arrival.querySelectorAll('header nav a')].map(a=>a.getAttribute('href'));if(JSON.stringify(account)!==JSON.stringify(['/profile','/apps?context=self','/programs?context=self']))add('arrival-account',account.join(' '));}
 const outreach=[...document.querySelectorAll('a[href]')].map(a=>a.href).filter(h=>/\.pages\.dev\/|aurekai-work-samples/.test(h));if(outreach.length)add('outreach-link',[...new Set(outreach)].join(' '),false);
 const nav=performance.getEntriesByType('navigation')[0];
 if(nav){if(nav.responseStart>budgets.ttfb)add('slow-ttfb',Math.round(nav.responseStart)+'ms',false);if(nav.loadEventEnd>budgets.load)add('slow-load',Math.round(nav.loadEventEnd)+'ms',false);}
 const slow=performance.getEntriesByType('resource').filter(r=>r.duration>budgets.resource&&new URL(r.name).origin===location.origin);if(slow.length)add('slow-resource',slow.map(r=>new URL(r.name).pathname+' '+Math.round(r.duration)+'ms').join(' '),false);
 return out;
}
// port: read a candidate on 127.0.0.1:port under the public names; otherwise the public sites.
export async function frontIntegrity({port=null,chrome=process.env.BONFYRE_CHROMIUM_BINARY,targets=hosts,viewports=[[1440,900],[390,844]],styled=styledClasses()}={}){
 const {chromium}=await import(process.env.BONFYRE_PLAYWRIGHT_MODULE||'@playwright/test');
 const args=port?['--host-resolver-rules=MAP '+targets.join(' 127.0.0.1:'+port+', MAP ')+' 127.0.0.1:'+port]:[];
 const browser=await chromium.launch({headless:true,args,...chrome?{executablePath:chrome}:{}});
 const findings=[];
 try{for(const host of targets)for(const [width,height] of viewports){
  const page=await browser.newPage({viewport:{width,height}});
  try{const url=host.includes('://')?host:(port?'http://':'https://')+host+'/',reply=await page.goto(url,{waitUntil:'load',timeout:60000});
   if(!url.startsWith('file:')&&(!reply||reply.status()!==200))findings.push({host,width,kind:'status',detail:String(reply?.status()),hard:true});
   else{await page.waitForTimeout(1500);for(const f of await page.evaluate(probe,{styled,budgets}))findings.push({host,width,...f});}}
  catch(error){findings.push({host,width,kind:'unreachable',detail:error.message.split('\n')[0],hard:true});}
  finally{await page.close();}
 }}finally{await browser.close();}
 // A named, known defect can be carried as a note while its fix lands; the name is the owner's decision.
 const advisory=new Set((process.env.BONFYRE_FRONT_ADVISORY||'').split(',').filter(Boolean));
 return findings.map(f=>advisory.has(f.kind)?{...f,hard:false}:f);
}
if(import.meta.main){
 const flag=name=>{const at=process.argv.indexOf(name);return at>0?process.argv[at+1]:null;},url=flag('--url');
 const findings=await frontIntegrity({port:flag('--candidate'),...url?{targets:[url]}:{}});
 for(const f of findings)console.log([f.hard?'FAIL':'NOTE',f.host,f.width,f.kind,f.detail].join('\t'));
 console.log('FRONT_INTEGRITY '+JSON.stringify({hard:findings.filter(f=>f.hard).length,notes:findings.filter(f=>!f.hard).length}));
 process.exit(findings.some(f=>f.hard)?1:0);
}
