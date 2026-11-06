import {test, expect} from 'bun:test';
import {Database} from 'bun:sqlite';
import {mkdtempSync,writeFileSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {basename,join,resolve} from 'node:path';
import {withChrome} from '../../../integrations/browser/capture/chrome-session.mjs';

const root=resolve(import.meta.dir,'../../..');
const binary=process.env.BONFYRE_WEB_ADDRESS_TEST_BINARY||join(root,'cmd/BonfyreWebAddress/bonfyre-web-address');
async function siteFixture(fn){
  const home=mkdtempSync(join(tmpdir(),'site-page-replace-'));
  const reservation=Bun.serve({hostname:'127.0.0.1',port:0,fetch:()=>new Response('reserved')});
  const port=reservation.port;await reservation.stop(true);
  const env={...process.env,BONFYRE_CMS_BINARY:process.env.BONFYRE_CMS_TEST_BINARY||join(root,'cmd/BonfyreCMS/bonfyre-cms'),BONFYRE_CMS_SCHEMAS:join(root,'cmd/BonfyreCMS/content-types'),BONFYRE_SITE_ROOT:join(root,'site/matrix')};
  const prefix=basename(binary)==='bonfyre-site'?[]:['site'];
  const run=(...args)=>{const p=Bun.spawnSync([binary,...prefix,...args,'--home',home,'--port',String(port),'--no-open'],{env,timeout:20000});return {code:p.exitCode,stdout:p.stdout.toString(),stderr:p.stderr.toString()};};
  const snapshot=()=>{const db=new Database(join(home,'cms.db'),{readonly:true});try{return {pages:db.query('SELECT * FROM site_page ORDER BY id').all(),fragments:db.query('SELECT * FROM site_fragment ORDER BY id').all()};}finally{db.close();}};
  const page=async()=>{const r=await fetch(`http://127.0.0.1:${port}/guide`,{headers:{Host:'replace-test.localhost',Accept:'text/html'}});return {status:r.status,body:await r.text()};};
  try{
    expect(run('new','replace-test','--title','Replacement test').code).toBe(0);
    expect(run('page','replace-test','--route','/guide','--title','Original guide','--group','Reference','--text','Original instructions.').code).toBe(0);
    await fn({home,run,snapshot,page,port});
  }finally{run('stop');rmSync(home,{recursive:true,force:true});}
}

test('a missing replacement file leaves every page record and the served guide unchanged',()=>siteFixture(async({home,run,snapshot,page})=>{
  const before=snapshot(),served=await page();
  const result=run('page','replace-test','--route','/guide','--title','New guide','--replace','--file',join(home,'missing.md'));
  expect(result.code).not.toBe(0);expect(snapshot()).toEqual(before);expect(await page()).toEqual(served);
}));

test('long code fences retain embedded Markdown through import and native HTML rendering',()=>siteFixture(async({run,snapshot,page,port})=>{
  const code='Literal example.\n```\n## This heading is code\n<script>example()</script>\n````not a closing fence\n## Still code\n';
  const source='# Reference\n\nA public fixture.\n\n## Result\n\n````text In\n'+code+'`````\n\n## Next step\n\nVisible instructions.';
  const result=run('page','replace-test','--route','/guide','--title','Reference','--replace','--text',source);
  expect(result.code).toBe(0);
  expect(snapshot().fragments.filter(f=>f.page==='/guide').map(f=>f.heading)).toEqual(['Result','Next step']);
  const rendered=await page();
  expect(rendered.status).toBe(200);
  expect(rendered.body).toContain('data-lang="text"');
  expect(rendered.body).toContain('<pre><code>'+code.replaceAll('<','&lt;').replaceAll('>','&gt;')+'</code></pre>');
  expect(rendered.body).not.toContain('<script>example()');
  expect(rendered.body).toContain('Visible instructions.');
  await withChrome(async({open})=>{
    const {page:cdp,go}=await open();
    await go(`http://replace-test.localhost:${port}/guide`);
    for(const width of [1440,390]){
      await cdp('Emulation.setDeviceMetricsOverride',{width,height:900,deviceScaleFactor:1,mobile:false});
      const result=await cdp('Runtime.evaluate',{expression:'({code:document.querySelector("pre code")?.textContent,fits:document.documentElement.scrollWidth<=innerWidth+1})',returnByValue:true});
      expect(result.result.value.code).toBe(code);expect(result.result.value.fits).toBe(true);
      if(process.env.BONFYRE_FENCE_SCREENSHOT_DIR){const image=await cdp('Page.captureScreenshot',{format:'png',captureBeyondViewport:false});writeFileSync(join(process.env.BONFYRE_FENCE_SCREENSHOT_DIR,`fence-${width}.png`),Buffer.from(image.data,'base64'));}
    }
  },{flags:['--host-resolver-rules=MAP replace-test.localhost 127.0.0.1','--no-proxy-server']});
}),30000);

test('empty, whitespace, binary and directory inputs are refused before replacement',()=>siteFixture(async({home,run,snapshot,page})=>{
  const before=snapshot();
  for(const [name,body] of [['empty',''],['blank',' \n\t\r\n'],['binary','Good opening\0hidden text']]){
    const path=join(home,name);writeFileSync(path,body);
    expect(run('page','replace-test','--route','/guide','--title','New guide','--replace','--file',path).code).not.toBe(0);
    expect(snapshot()).toEqual(before);
  }
  expect(run('page','replace-test','--route','/guide','--title','New guide','--replace','--file',home).code).not.toBe(0);
  expect(run('page','replace-test','--route','/guide','--title','New guide','--replace','--text','  \n').code).not.toBe(0);
  expect(run('page','replace-test','--route','/guide','--replace','--remove').code).not.toBe(0);
  expect(snapshot()).toEqual(before);expect((await page()).body).toContain('Original instructions.');
}));

test('valid replacement keeps the page identity, creation time and navigation group',()=>siteFixture(async({run,snapshot,page})=>{
  const original=snapshot().pages.find(p=>p.route==='/guide');
  expect(run('page','replace-test','--route','/next','--title','Next guide','--text','Another page.').code).toBe(0);
  const result=run('page','replace-test','--route','/guide','--title','Updated guide','--replace','--text','# Updated guide\n\nA revised summary.\n\n## Next step\n\nRevised instructions.');
  expect(result.code).toBe(0);
  const after=snapshot(),updated=after.pages.find(p=>p.route==='/guide');
  expect(updated.id).toBe(original.id);expect(updated.created_at).toBe(original.created_at);expect(updated.eyebrow).toBe('Reference');
  expect(after.pages).toHaveLength(2);expect(after.fragments.filter(f=>f.page==='/guide')).toHaveLength(1);
  const html=(await page()).body;expect(html).toContain('Revised instructions.');expect(html).not.toContain('Original instructions.');
  expect(run('page','replace-test','--route','/guide','--title','Updated guide','--replace','--group','Revised group','--text','Second revision.').code).toBe(0);
  expect(snapshot().pages.find(p=>p.route==='/guide').eyebrow).toBe('Revised group');
  expect(snapshot().pages.find(p=>p.route==='/guide').id).toBe(original.id);
  expect(run('page','replace-test','--route','/guide','--remove').code).toBe(0);
  expect((await page()).status).toBe(404);expect(snapshot().pages.map(p=>p.route)).toEqual(['/next']);
}));


test('a storage failure during replacement, creation or removal preserves the served page',()=>siteFixture(async({home,run,snapshot,page})=>{
  const db=new Database(join(home,'cms.db'));
  db.exec("CREATE TRIGGER refuse_fragment BEFORE INSERT ON site_fragment WHEN new.body='Refuse this section.' BEGIN SELECT RAISE(ABORT,'injected failure'); END");
  const before=snapshot(),served=await page();
  try{
    for(const [route,extra] of [['/guide',['--replace']],['/new',[]]]){
      const result=run('page','replace-test','--route',route,'--title','Changed',...extra,'--text','First section succeeds.\n\nRefuse this section.');
      expect(result.code).not.toBe(0);expect(result.stderr).toContain('previous content is intact');
      expect(snapshot()).toEqual(before);expect(await page()).toEqual(served);
    }
    db.exec("CREATE TRIGGER refuse_page_delete BEFORE DELETE ON site_page BEGIN SELECT RAISE(ABORT,'injected failure'); END");
    expect(run('page','replace-test','--route','/guide','--remove').code).not.toBe(0);
    expect(snapshot()).toEqual(before);expect(await page()).toEqual(served);
  }finally{db.close();}
}));
