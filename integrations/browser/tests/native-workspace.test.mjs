import { test, expect } from 'bun:test';
import { Database } from 'bun:sqlite';
import { copyFileSync, existsSync, mkdirSync, mkdtempSync, rmSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { tmpdir } from 'node:os';
import { spawnSync } from 'node:child_process';

const repo = process.env.BONFYRE_TEST_REPO || resolve(import.meta.dir, '../../..');
const web = process.env.BONFYRE_TEST_WEB_BINARY || join(repo, 'cmd/BonfyreWebAddress/bonfyre-web-address');
const cms = join(repo, 'cmd/BonfyreCMS/bonfyre-cms');
const auth = join(repo, 'cmd/BonfyreAuth/bonfyre-auth');
const queue = join(repo, 'cmd/BonfyreQueue/bonfyre-queue');
const { createHost } = await import(join(repo, 'integrations/browser/host/bonfyre-browser-host.mjs'));
const { withChrome, findChrome } = await import(join(repo, 'integrations/browser/capture/chrome-session.mjs'));
const available = [web,cms,auth,queue].every(existsSync) && findChrome();
function command(binary, args) {
  const r = spawnSync(binary, args, { encoding:'utf8', timeout:20000 });
  if (r.status !== 0) throw new Error(binary + ' refused: ' + r.stderr);
  return r.stdout;
}

test.skipIf(!available)('browser-held material continues through the real Desk and CMS form, with institution access preserved', async () => {
  const dir = mkdtempSync(join(tmpdir(),'bf-native-workspace-'));
  let server;
  try {
    const authdb=join(dir,'auth.db'), cmsdb=join(dir,'cms.db'), queuedb=join(dir,'queue.db'), matrix=join(dir,'matrix.db');
    const schemas=join(dir,'schemas'); mkdirSync(schemas);
    copyFileSync(join(repo,'cmd/BonfyreCMS/content-types/wiki_page.json'),join(schemas,'wiki_page.json'));
    command(cms,['schema','migrate','--db',cmsdb,'--schemas',schemas]);
    command(web,['bootstrap-owned','--db',matrix]);
    const account = (name, admin=false) => {
      command(auth,['--db',authdb,'signup','--email',name+'@example.test','--name',name,'--password','workspace-fixture-only','--tier','pro']);
      const login=JSON.parse(command(auth,['--db',authdb,'login','--email',name+'@example.test','--password','workspace-fixture-only']));
      const token=login.session.token;
      const user=JSON.parse(command(auth,['--db',authdb,'verify','--token',token])).user;
      if(admin) command(auth,['--db',authdb,'institution-grant','--user-id',String(user.id),'--institution','bonfyre','--role','admin']);
      return {token,id:user.id};
    };
    const reader=account('writer'), admin=account('admin',true);
    const selection='Four tins, then a community tasting. '+ 'Complete retained material. '.repeat(285) + '</textarea><script>globalThis.sourceExecuted=true</script> END OF SELECTED MATERIAL';
    const host=createHost({env:{...process.env,BONFYRE_QUEUE_BINARY:queue,BONFYRE_BROWSER_QUEUE_DB:queuedb}});
    const request={operation:'hold',url:'https://example.com/tin-program',selection,note:'Prepare our neighborhood tasting programme',confirmed:true};
    const held=await host.answer(request); expect(held.ok).toBe(true);
    expect((await host.answer(request)).result.mission).toBe(held.result.mission);
    const id=held.result.mission;
    const q=new Database(queuedb,{readonly:true});
    const before=q.query('SELECT * FROM workgraph_nodes ORDER BY mission_id,node_id').all();
    expect(before).toHaveLength(1);
    const reservation=Bun.serve({hostname:'127.0.0.1',port:0,fetch:()=>new Response('reserved')});
    const port=reservation.port; reservation.stop(true);
    const origin='http://127.0.0.1:'+port;
    server=Bun.spawn([web,'serve','--db',matrix,'--auth-db',authdb,'--port',String(port),'--site-root',join(repo,'site/matrix')],
      {env:{...process.env,BONFYRE_AUTH_BINARY:auth,BONFYRE_CMS_BINARY:cms,BONFYRE_CMS_DB:cmsdb,BONFYRE_CMS_SCHEMAS:schemas,BONFYRE_BROWSER_QUEUE_DB:queuedb},stdout:'ignore',stderr:'pipe'});
    let ready=false;
    for(let i=0;i<100;i++){try{const r=await fetch(origin+'/healthz');if(r.ok){ready=true;break}}catch{} await Bun.sleep(50)}
    expect(ready).toBe(true);
    const get=(path,token)=>fetch(origin+path,{headers:{accept:'text/html',...(token?{authorization:'Bearer '+token}:{})}});
    expect((await get('/make?held_selection='+id)).status).toBe(403);
    expect((await get('/make?held_selection='+id,reader.token)).status).toBe(403);
    expect(await (await get('/studio/',reader.token)).text()).not.toContain(id);
    const desk=await (await get('/studio/',admin.token)).text();
    expect(desk).toContain('/make?held_selection='+id);
    expect(desk).toContain(request.note);
    expect(desk).toContain('button.ap-act{width:100%;text-align:left');
    const page=await (await get('/make?held_selection='+id,admin.token)).text();
    expect(page).toContain('END OF SELECTED MATERIAL');
    expect(page).toContain('&lt;script&gt;globalThis.sourceExecuted=true&lt;/script&gt;');
    expect(page).not.toContain('<script>globalThis.sourceExecuted');
    expect((await get('/make?held_selection=page-selection-000000000000000000000000',admin.token)).status).toBe(404);
    await withChrome(async ({open})=>{
      const tab=await open();
      await tab.page('Network.setCookie',{name:'bonfyre_session',value:admin.token,url:origin,path:'/',httpOnly:true});
      await tab.go(origin+'/studio/');
      const evaluate=async expression=>{
        const r=await tab.page('Runtime.evaluate',{expression,awaitPromise:true,returnByValue:true});
        if(r.exceptionDetails)throw Error(r.exceptionDetails.exception?.description || r.exceptionDetails.text);return r.result.value;
      };
      const until=async(expression,check)=>{for(let i=0;i<150;i++){const r=await evaluate(expression).catch(()=>null);if(r!=null&&check(r))return r;await Bun.sleep(100)}throw Error('native UI never reached '+expression+'; location='+await evaluate('location.href')+'; body='+(await evaluate('document.body?.textContent')||'').slice(-3000))};
      const href='/make?held_selection='+id;
      await until('Array.from(document.querySelectorAll("a")).some(a=>a.getAttribute("href")==='+JSON.stringify(href)+')',Boolean);
      await evaluate('Array.from(document.querySelectorAll("a")).find(a=>a.getAttribute("href")==='+JSON.stringify(href)+').click()');
      await until('document.body.textContent',s=>s.includes('END OF SELECTED MATERIAL'));
      expect(await evaluate('globalThis.sourceExecuted===true')).toBe(false);
      await evaluate('Array.from(document.querySelectorAll("a")).find(a=>a.getAttribute("href")?.startsWith("/make/form?type=Note")).click()');
      const content=await until('document.querySelector("textarea[name=content]")?.value',s=>typeof s==='string'&&s.includes('END OF SELECTED MATERIAL'));
      expect(content).toContain(selection);expect(content).toContain(request.url);expect(content).toContain(id);
      expect(await evaluate('document.querySelector("form.mk-commit").getAttribute("action")')).toBe('/cms/v1/api/wiki_page');
      await evaluate('document.querySelector("form.mk-commit").requestSubmit()');
      await until('document.body.textContent',s=>s.includes('Created in the native CMS'));
    },{timeoutMs:60000});
    const records=new Database(cmsdb,{readonly:true});
    const notes=records.query('SELECT id,namespace,route,content,status FROM wiki_page').all();
    expect(notes).toHaveLength(1);expect(notes[0].status).toBe('draft');expect(notes[0].namespace).not.toBe('root');
    expect(notes[0].content).toContain(selection);expect(notes[0].content).toContain(id);expect(notes[0].route).toBe('notes/'+id);
    expect(await (await get('/studio/',reader.token)).text()).not.toContain('notes/'+id);
    for(const path of ['/room/note/'+id,'/s/note/'+id]) {
      const other=await (await get(path,reader.token)).text();
      expect(other).not.toContain('END OF SELECTED MATERIAL');
      expect(other).not.toContain('Working note from a selected page');
      expect(await (await get(path,admin.token)).text()).toContain('Working note from a selected page');
    }
    expect(notes[0].content.length).toBeGreaterThan(8192);
    const editor=await (await get('/cms/parts/edit?type=wiki_page&id='+notes[0].id,admin.token)).text();
    expect(editor).toContain('END OF SELECTED MATERIAL');
    expect(editor).toContain('&lt;script&gt;globalThis.sourceExecuted=true&lt;/script&gt;');
    const formWrite=(body)=>fetch(origin+'/cms/v1/api/wiki_page/'+notes[0].id,{method:'PUT',headers:{authorization:'Bearer '+admin.token,'content-type':'application/x-www-form-urlencoded',accept:'text/html'},body});
    const optional='meta_description=';
    await formWrite(new URLSearchParams({content:notes[0].content,meta_description:'Remove this'}).toString());
    await formWrite(optional);
    expect(records.query('SELECT meta_description FROM wiki_page WHERE id=?').get(notes[0].id).meta_description).toBe('');
    const kept=records.query('SELECT content FROM wiki_page WHERE id=?').get(notes[0].id).content;
    for(const invalid of ['content=%00','content=first&content=second','content=%zz','content='+ 'x'.repeat(65536),'content='+ 'x'.repeat(256*1024)]) {
      const response=await formWrite(invalid);
      const answer=await response.text();
      if(invalid.length>256*1024) expect(response.status).toBe(413);
      else expect(answer).toContain('could not be validated');
      expect(records.query('SELECT content FROM wiki_page WHERE id=?').get(notes[0].id).content).toBe(kept);
    }
    records.close();
    expect(q.query('SELECT * FROM workgraph_nodes ORDER BY mission_id,node_id').all()).toEqual(before);
    command(auth,['--db',authdb,'institution-revoke','--user-id',String(admin.id),'--institution','bonfyre']);
    expect((await get('/make?held_selection='+id,admin.token)).status).toBe(403);
    const revokedDesk=await (await get('/studio/',admin.token)).text();
    expect(revokedDesk).not.toContain('/make?held_selection='+id);
    expect(revokedDesk).not.toContain('Selections brought from the browser');
    expect(revokedDesk).toContain('notes/'+id);
    q.close();
  } finally {
    if(server){server.kill();await server.exited;console.error((await new Response(server.stderr).text()).slice(-12000));}
    rmSync(dir,{recursive:true,force:true});
  }
},120000);
