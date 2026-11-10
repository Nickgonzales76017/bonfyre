import {test,expect,afterAll} from 'bun:test';
import {readFileSync,writeFileSync,mkdtempSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
const scratch=mkdtempSync(join(tmpdir(),'service-site-worker-'));
afterAll(()=>rmSync(scratch,{recursive:true,force:true}));
let serial=0;
import {renderSite} from '../build.mjs';
import {localHref,siteHref} from '../navigation.mjs';
import {metadata,discovery,publicOrigin} from '../seo.mjs';
import {workerSource} from '../lead-worker.mjs';
const sample=()=>JSON.parse(readFileSync(new URL('./fixtures/sample-lawn/content.json',import.meta.url),'utf8'));
const forms={request:{page:'/',fields:{Phone:true,Kind:['House','Land'],Message:true},required:['Phone']}};
async function fixture(){const path=join(scratch,'worker-'+serial+++'.mjs');writeFileSync(path,workerSource('test-site',forms));const module=await import(path);const values=new Map();const env={LEADS:{get:async k=>values.get(k)??null,put:async(k,v)=>values.set(k,v)}};return{module,env,values};}
const payload=(value='555-010-1234')=>({form:'request',page:'/',fields:[{label:'Phone',value}]});
const req=(data,origin='https://example.test',headers={})=>new Request('https://example.test/api/lead',{method:'POST',headers:{origin,'content-type':'application/json',...headers},body:typeof data==='string'?data:JSON.stringify(data)});

test('authored local links reject active, remote, malformed and missing destinations',()=>{
 for(const v of ['javascript:alert(1)','//other.test/','/../private','/a%2Fb/','https://other.test/'])expect(()=>localHref(v)).toThrow();
 expect(localHref('/buyers/#ask')).toBe('/buyers/#ask');expect(siteHref('https://docs.aurekai.work/')).toBe('https://docs.aurekai.work/');expect(()=>siteHref('https://other.test/')).toThrow();expect(()=>siteHref('https://aurekai.work.other.test/')).toThrow();const c=sample();c.navigation=[{label:'Missing',href:'/missing/'}];expect(()=>renderSite(c)).toThrow('Dead site link');
});
test('CTA destinations and service links are projected by the existing components',()=>{
 const c=sample();c.navigation=[{label:'Home',href:'/'},{label:'Ask',href:'/#ask'}];c.sections.at(-1).href='/#ask';c.sections[0].secondary={label:'Read the work',href:'#work'};
 const html=renderSite(c).pages[0].html;expect(html).toContain('href="/#ask"');expect(html).toContain('data-form=');expect(html).not.toContain('javascript:');
});
test('metadata preserves identity and preview noindex without fictitious business proof',()=>{
 const c=sample();c.public_origin='https://example.test';c.business.name='Example </script><script>bad()</script>';c.preview=true;
 const html=metadata(c,{path:''});expect(html).not.toContain('</script><script>bad()');expect(html).toContain('application/ld+json');expect(html).not.toContain('aggregateRating');
 const d=discovery(c,[{path:''},{path:'buyers'}]);expect(d.robots).toContain('Disallow: /');expect(d.sitemap).toContain('https://example.test/buyers/');
 for(const u of ['http://example.test','https://user:pass@example.test','https://example.test/a'])expect(()=>publicOrigin({...c,public_origin:u})).toThrow();
});
test('select and multiline message are genuine typed form fields',()=>{
 const c=sample(),f=c.sections.find(s=>s.form).form;f.fields.push({label:'Kind',type:'select',options:['House','Land'],optional:true},{label:'Message',type:'textarea',optional:true});
 const r=renderSite(c);expect(r.pages[0].html).toContain('<textarea');expect(r.pages[0].html).toContain('<select');expect(r.forms[f.id].fields.Kind).toEqual(['House','Land']);
});
test('whitespace, unlisted choices, duplicate labels and prototype names store nothing',async()=>{
 const{module,env,values}=await fixture();const inputs=[payload('   '),{...payload(),fields:[{label:'Phone',value:'x'},{label:'Phone',value:'y'}]},{...payload(),fields:[{label:'__proto__',value:'x'}]},{...payload(),fields:[{label:'Phone',value:'x'},{label:'Kind',value:'Boat'}]},{...payload(),fields:[{label:'Phone',value:'x'.repeat(301)}]},{...payload(),form:'__proto__'},{...payload(),page:'/buyers/'}];
 for(const p of inputs)expect((await module.takeLead(req(p),env)).status).toBe(400);expect(values.size).toBe(0);
});
test('malformed origins and wrong schemes are refused before a request is stored',async()=>{
 const{module,env,values}=await fixture();for(const o of ['not a url','http://example.test','https://other.test'])expect((await module.takeLead(req(payload(),o),env)).status).toBe(403);expect(values.size).toBe(0);
});
test('streaming body limit is bytes, including when content length is absent',async()=>{
 const{module,env,values}=await fixture();expect((await module.takeLead(req('é'.repeat(5000)),env)).status).toBe(413);
 let cancelled=false;const stream=new ReadableStream({start(c){c.enqueue(new Uint8Array(8001));},cancel(){cancelled=true;}});const r=new Request('https://example.test/api/lead',{method:'POST',headers:{origin:'https://example.test','content-type':'application/json'},body:stream,duplex:'half'});
 expect((await module.takeLead(r,env)).status).toBe(413);expect(cancelled).toBe(true);expect(values.size).toBe(0);
});
test('ordinary intake, canonical page, and existing five-per-hour limit remain',async()=>{
 const{module,env,values}=await fixture();for(let i=0;i<5;i++)expect((await module.takeLead(req({...payload(),page:undefined}),env)).status).toBe(201);
 expect((await module.takeLead(req(payload()),env)).status).toBe(429);const leads=[...values].filter(([k])=>k.startsWith('lead:')).map(([,v])=>JSON.parse(v));expect(leads.length).toBe(5);expect(leads.every(v=>v.page==='/'&&v.site_id==='test-site')).toBe(true);
});
