import {test,expect} from 'bun:test';
import {loopbackServer} from './browser-fixture.mjs';
import {pathToFileURL} from 'node:url';
import {resolve} from 'node:path';
import {mkdtempSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
const helper=process.env.BONFYRE_BROWSER_SESSION_TEST_MODULE||resolve(import.meta.dir,'../capture/chrome-session.mjs');
const {withChrome}=await import(pathToFileURL(helper).href);

test('reused tabs wait for each document, delayed resources and redirects, and support fragment navigation',async()=>{
  const downloads=mkdtempSync(resolve(tmpdir(),'bonfyre-navigation-download-'));
  const served=[];
  const server=loopbackServer({hostname:'127.0.0.1',port:0,async fetch(request){
    const path=new URL(request.url).pathname;
    if(path==='/redirect')return Response.redirect(new URL('/third',request.url));
    if(path==='/download')return new Response('fixture',{headers:{'content-type':'application/octet-stream','content-disposition':'attachment; filename="fixture.txt"'}});
    if(path==='/delayed-script'){
      await Bun.sleep(250);served.push(path);
      return new Response('window.pageReady="second-ready"',{headers:{'content-type':'text/javascript'}});
    }
    served.push(path);
    return new Response(`<h1>${path}</h1><div id="target">Target</div>${path==='/second'?'<script src="/delayed-script"></script>':''}`,{headers:{'content-type':'text/html'}});
  }});
  try{await withChrome(async({open,send})=>{
    const tab=await open(),origin=`http://127.0.0.1:${server.port}`;
    const state=async()=>{const r=await tab.page('Runtime.evaluate',{expression:'({path:location.pathname,hash:location.hash,heading:document.querySelector("h1")?.textContent,ready:window.pageReady,state:document.readyState})',returnByValue:true});return r.result.value;};
    const first=await tab.go(origin+'/first');
    expect(await state()).toMatchObject({path:'/first',heading:'/first',state:'complete'});
    expect(await tab.go(origin+'/second')).toBe(first);
    expect(await state()).toMatchObject({path:'/second',heading:'/second',ready:'second-ready',state:'complete'});
    expect(served).toContain('/delayed-script');
    expect(await tab.go(origin+'/redirect')).toBe(first);
    expect(await state()).toMatchObject({path:'/third',heading:'/third',state:'complete'});
    expect(await tab.go(origin+'/third#target')).toBe(first);
    expect(await state()).toMatchObject({path:'/third',hash:'#target',heading:'/third'});
    expect(await tab.go(origin+'/third#target')).toBe(first);
    expect(await state()).toMatchObject({path:'/third',hash:'#target',heading:'/third'});
    await send('Browser.setDownloadBehavior',{behavior:'allow',downloadPath:downloads});
    // Some installed Chrome builds close their connection for this attachment.
    // Either response must reject: an attachment is not a loaded document.
    let downloadFailure;
    try{await tab.go(origin+'/download');}catch(error){downloadFailure=error.message;}
    expect(downloadFailure).toMatch(/navigation failed|download|the browser connection closed/);
    console.info(JSON.stringify({download_failure:downloadFailure}));
  });}finally{await server.stop(true);rmSync(downloads,{recursive:true,force:true});}
},30000);

test('a browser navigation error cannot borrow a prior successful load',async()=>{
  await withChrome(async({open})=>{
    const tab=await open();
    await tab.go('data:text/html,<h1>First document</h1>');
    await expect(tab.go('http://127.0.0.1:1/')).rejects.toThrow(/ERR_UNSAFE_PORT|navigation/i);
  },{timeoutMs:10000});
},15000);
