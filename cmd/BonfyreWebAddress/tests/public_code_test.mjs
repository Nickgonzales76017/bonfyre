import {test,expect,beforeAll,afterAll} from 'bun:test';
import {createRequire} from 'node:module';
import {join} from 'node:path';
import {fixture,root} from './auth_continuation_fixture.mjs';
let f,browser;
beforeAll(async()=>{f=await fixture();},30000);
afterAll(async()=>{await browser?.close();await f?.close();});
test('the native public item exposes its own QR, with no external generator or account payload',async()=>{
 const page=await f.request(f.path,{host:'aurekai.life'});expect(page.status).toBe(200);expect(page.body).toContain('Open on another device');expect(page.body).toContain(f.path+'.qr.svg');
 const qr=await f.request(f.path+'.qr.svg',{host:'aurekai.life'});expect(qr.status).toBe(200);expect(qr.headers.get('content-type')).toContain('image/svg+xml');expect(qr.headers.get('content-security-policy')).toContain("default-src 'none'");
 expect(qr.body).toContain('<desc>https://aurekai.life'+f.path+'</desc>');expect(qr.body).not.toMatch(/<script|<foreignObject|<image|href=|token|password|session=/i);
 expect(qr.body).toContain('shape-rendering="crispEdges"');expect(qr.body).toContain('fill="#fff"');expect(qr.body).toContain('fill="#000"');
});
test('unknown item keys and source membership changes cannot manufacture a public QR',async()=>{
 const missing=await f.request('/o/newsdesk/'+'0'.repeat(24)+'.qr.svg');expect(missing.status).toBe(404);expect(missing.body).not.toContain('<svg');
 f.run(f.cms,['entry','update','internet_source','1','{"status":"draft"}','--ns','root','--db',f.db,'--schemas',join(f.home,'schemas')]);
 try{const hidden=await f.request(f.path+'.qr.svg');expect(hidden.status).toBe(404);expect(hidden.body).not.toContain('<svg');}
 finally{f.run(f.cms,['entry','update','internet_source','1','{"status":"published"}','--ns','root','--db',f.db,'--schemas',join(f.home,'schemas')]);}
});
test('the same item on two hosts encodes its own exact public host, never an account continuation',async()=>{
 const a=await f.request(f.path+'.qr.svg',{host:'aurekai.life'}),b=await f.request(f.path+'.qr.svg',{host:'aurekai.work'});
 expect(a.status).toBe(200);expect(b.status).toBe(200);expect(a.body).not.toBe(b.body);expect(b.body).toContain('<desc>https://aurekai.work'+f.path+'</desc>');expect(b.body).not.toContain('/login');
});
test('Apple Vision independently decodes the actual native SVG raster to the public item address',async()=>{
 if(process.platform!=='darwin')throw Error('Run this explicit browser witness on the Bonfyre Mac with Vision.');
 const qr=await f.request(f.path+'.qr.svg',{host:'aurekai.life'});expect(qr.status).toBe(200);
 const pp=createRequire(join(root,'tools/creator-ingest/package.json'))('puppeteer');browser=await pp.launch({headless:true,executablePath:'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',args:['--no-sandbox','--disable-dev-shm-usage']});const page=await browser.newPage();
 await page.setViewport({width:800,height:800});await page.setContent('<img width="512" height="512" src="data:image/svg+xml;base64,'+Buffer.from(qr.body).toString('base64')+'">');await page.waitForFunction(()=>document.querySelector('img').complete&&document.querySelector('img').naturalWidth>0);
 const png=join(f.home,'public-code.png');await (await page.$('img')).screenshot({path:png});
 const decoded=Bun.spawnSync(['/usr/bin/swift',join(import.meta.dir,'read_public_code.swift'),png],{stdout:'pipe',stderr:'pipe',timeout:60000});
 expect(decoded.exitCode,decoded.stderr.toString()).toBe(0);expect(decoded.stdout.toString().trim()).toBe('https://aurekai.life'+f.path);
 await page.close();
},90000);
test('published QR output is bounded and remains SVG without client execution',async()=>{
 const qr=await f.request(f.path+'.qr.svg');expect(Buffer.byteLength(qr.body)).toBeLessThan(128*1024);expect(qr.body.startsWith('<svg ')).toBe(true);expect(qr.body.trimEnd().endsWith('</svg>')).toBe(true);
 const supplied=new Set([...qr.body.matchAll(/https:\/\/[^<\s]+/g)].map(x=>x[0]));expect([...supplied]).toEqual(['https://aurekai.life'+f.path]);
});
