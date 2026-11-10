import {test,expect} from 'bun:test';
import {readFileSync,writeFileSync,existsSync} from 'node:fs';
import {join,resolve} from 'node:path';
import {homedir} from 'node:os';
import {createHash} from 'node:crypto';
import {withChrome} from '../../integrations/browser/capture/chrome-session.mjs';
import {renderSite,validate} from '../../components/BonfyreServiceSite/build.mjs';
const dir=process.env.BONFYRE_FLEMING_CANDIDATE??join(homedir(),'Downloads/aurekai-atlas-reconcile/fleming-preview');
const file=join(dir,'content.json'),dist=join(dir,'dist');
const sha=p=>createHash('sha256').update(readFileSync(p)).digest('hex');
const here=existsSync(file)&&existsSync(join(dir,'content-before.json'));
const content=here?JSON.parse(readFileSync(file,'utf8')):{};
const original=here?JSON.parse(readFileSync(join(dir,'content-before.json'),'utf8')):{};
const oldForm=id=>[...original.sections,...original.pages.flatMap(p=>p.sections)].find(s=>s.form?.id===id).form;
test.skipIf(!here)('the revision is a record in the existing proton site, not a second application',()=>{
 expect(validate(content)).toBe('2149005577');
 for(const key of ['site_id','pages_project','cloud_account','leads','preview'])expect(content[key]).toBe(original[key]);
 expect(content.cloud_account).toBe('proton');expect(content.pages.map(p=>p.path)).toEqual(['buyers']);
 expect(content.contact).toEqual(original.contact);expect(content.business.logo).toBe(original.business.logo);
 const rendered=renderSite(content);expect(Object.keys(rendered.forms)).toEqual(['seller','buyer']);
 for(const id of ['seller','buyer'])expect(rendered.forms[id].required).toEqual(renderSite(original).forms[id].required);
 expect(content.sections[0].form).toBeUndefined();expect(content.sections[0].image.src).toBe(original.sections[0].image.src);
 expect(content.sections.find(s=>s.kind==='steps').items).toHaveLength(6);
 expect(content.theme.accent).toBe('#9cff00');expect(content.theme.second).toBe('#00aaff');
 expect(content.contact.portrait).toBe('');expect(existsSync(join(dist,'market/index.html'))).toBe(false);
 expect(JSON.stringify(content)).not.toMatch(/Tarrant|Fort Worth|DFW|Collin|Denton|certified corporate|guaranteed cash|24 hours|7 days|100%/i);
 expect(content.footer.notice.text).toContain('written transaction terms');expect(content.footer.notice.text).toContain('No outcome is guaranteed');
});
test.skipIf(!here)('the actual kit renders the revision on phones and both forms retain their native intake',async()=>{
 const before=sha(file),kept=new Map(),counts=new Map();
 const worker=(await import(join(dist,'_worker.js'))).default;
 const env={LEADS:{get:async key=>counts.get(key)??null,put:async(key,value,options)=>{if(key.startsWith('count:'))counts.set(key,value);else kept.set(key,{value:JSON.parse(value),options})}},ASSETS:{fetch:async request=>{
   const u=new URL(request.url),path=decodeURIComponent(u.pathname),f=resolve(dist,'.'+(path.endsWith('/')?path+'index.html':path));
   if(!f.startsWith(resolve(dist)+'/'))return new Response('not here',{status:404});
   const body=Bun.file(f);return await body.exists()?new Response(body):new Response(Bun.file(join(dist,'404.html')),{status:404,headers:{'content-type':'text/html'}});
 }}};
 const server=Bun.serve({hostname:'127.0.0.1',port:0,fetch:request=>worker.fetch(request,env)}),origin='http://127.0.0.1:'+server.port;
 const observed=[];
 try{await withChrome(async({open,events})=>{
  for(const path of ['/','/buyers/'])for(const width of [320,390,768,1024,1280]){
   const {page,go,sessionId}=await open();
   await page('Emulation.setDeviceMetricsOverride',{width,height:900,deviceScaleFactor:1,mobile:width<600});
   await go(origin+path);await page('Runtime.evaluate',{expression:'document.fonts.ready',awaitPromise:true});
   const answer=await page('Runtime.evaluate',{returnByValue:true,expression:`(()=>{
    const w=document.documentElement.clientWidth,visible=e=>{const b=e.getBoundingClientRect();return b.width>1&&b.height>1&&getComputedStyle(e).visibility!=='hidden'};
    return {h1:document.querySelector('h1').innerText,overflow:document.documentElement.scrollWidth>w,
     offscreen:[...document.querySelectorAll('main *,header *,footer *')].filter(e=>visible(e)&&!e.closest('.sr-only')&&e.getBoundingClientRect().right>w+1).map(e=>e.tagName+':'+e.innerText?.slice(0,35)),
     images:[...document.images].every(i=>i.complete&&i.naturalWidth>0),robots:document.querySelector('meta[name=robots]').content,
     dangling:[...document.querySelectorAll('a[href^="#"]')].filter(a=>!document.getElementById(a.hash.slice(1))).map(a=>a.hash),
     scripts:[...document.scripts].filter(s=>s.src).map(s=>s.src),form:document.querySelector('.ask-form')?.dataset,
     hasBuyer:[...document.querySelectorAll('a')].some(a=>a.getAttribute('href')==='/buyers/'),
     text:document.querySelector('main').innerText,
     targets:[...document.querySelectorAll('button,summary,input:not([type=radio])')].filter(e=>visible(e)&&!e.closest('.sr-only')&&e.getBoundingClientRect().height<44).map(e=>e.outerHTML.slice(0,60))};})()`});
   expect(answer.exceptionDetails).toBeUndefined();const view=answer.result.value;
   expect(view.overflow).toBe(false);expect(view.offscreen).toEqual([]);expect(view.dangling).toEqual([]);
   expect(view.images).toBe(true);expect(view.scripts).toEqual([]);expect(view.robots).toBe('noindex, nofollow');expect(view.targets).toEqual([]);
   expect(view.text).not.toMatch(/Tarrant|Fort Worth|DFW|certified corporate|100%|7 days/i);
   if(path==='/'){expect(view.h1).toContain('real families.');expect(view.hasBuyer).toBe(true);expect(view.text).toContain('No out-of-pocket cost to the family');}
   const errors=events.filter(e=>e.sessionId===sessionId&&e.method==='Runtime.exceptionThrown');expect(errors).toEqual([]);
   observed.push({path,width,overflow:view.overflow,script_errors:errors.length});
   if(path==='/'&&[390,1280].includes(width)){
    const shot=await page('Page.captureScreenshot',{format:'png',captureBeyondViewport:false});writeFileSync(join(dir,'preview-'+width+'.png'),Buffer.from(shot.data,'base64'));
   }
  }
  for(const id of ['seller','buyer']){
   counts.clear();const {page,go}=await open();await page('Emulation.setDeviceMetricsOverride',{width:390,height:900,deviceScaleFactor:1,mobile:true});await go(origin+(id==='seller'?'/':'/buyers/'));
   const answer=await page('Runtime.evaluate',{returnByValue:true,awaitPromise:true,expression:`(async()=>{
    const form=document.querySelector('.ask-form'),box=form.parentNode,set=(label,value)=>{const e=form.querySelector('input[data-label="'+label+'"]');if(e)e.value=value},pick=(label,value)=>form.querySelector('fieldset[data-label="'+label+'"] input[value="'+value+'"]').click();
    if(${JSON.stringify(id)}==='seller'){
      set('Property Address','1 Fixture Street, Dallas');pick('Property Type','House');form.querySelector('[data-next]').click();
      pick('Condition','Needs updating');form.querySelector('[data-next]').click();pick('How soon','Within a month');set('Phone Number','555 010 0199');set('Your Name','Test Family');
    }else{set('Your Name','Test Builder');set('Phone Number','555 010 0199');set('Email Address','builder@example.test');pick('What you buy','Land and lots');}
    form.requestSubmit();for(let i=0;i<100;i++){if(!box.querySelector('.ask-sent').classList.contains('hidden'))break;await new Promise(r=>setTimeout(r,30));}
    return {sent:!box.querySelector('.ask-sent').classList.contains('hidden'),words:box.querySelector('.ask-sent').textContent};})()`});
   expect(answer.exceptionDetails).toBeUndefined();expect(answer.result.value.sent).toBe(true);
   expect(answer.result.value.words).toContain('ready for Bryston to review');
  }
 },{timeoutMs:120000});
 expect(kept.size).toBe(2);const rows=[...kept.values()].map(r=>r.value);
 expect(rows.map(r=>r.form).sort()).toEqual(['buyer','seller']);expect(rows.every(r=>r.site_id===content.site_id)).toBe(true);
 expect(rows.find(r=>r.form==='seller').fields).toContainEqual({label:'Property Address',value:'1 Fixture Street, Dallas'});
 expect(rows.find(r=>r.form==='buyer').fields).toContainEqual({label:'What you buy',value:'Land and lots'});
 expect(sha(file)).toBe(before);expect((await fetch(origin+'/market/')).status).toBe(404);
 writeFileSync(join(dir,'BROWSER_VERIFICATION.json'),JSON.stringify({pages:observed,local_form_submissions:2,live_form_submissions:0,source_unchanged:true},null,2));
 }finally{server.stop(true)}
},150000);
