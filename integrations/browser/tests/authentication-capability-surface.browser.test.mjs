import {test,expect} from 'bun:test';
import {readFileSync} from 'node:fs';
import {join,resolve} from 'node:path';
import {withChrome} from '../capture/chrome-session.mjs';
const root=resolve(import.meta.dir,'../../..');
const files={'/interaction-collection.mjs':readFileSync(join(root,'integrations/browser/interaction-collection.mjs')),
 '/authentication-capability-surface.mjs':readFileSync(join(root,'integrations/browser/authentication-capability-surface.mjs')),
 '/provider-auth-capability.mjs':readFileSync(join(root,'integrations/browser/provider-auth-capability.mjs'))
};
function html(){
 return `<!doctype html><html><head><meta charset="utf-8"><style>body{font:14px system-ui}.pac-methods{display:flex;gap:6px}.pac-panel{padding:12px}</style></head><body><main id="app"></main><script type="module">
 import {mountAuthCapabilitySurface} from '/authentication-capability-surface.mjs';
 import {providerAuthCapability} from '/provider-auth-capability.mjs';
 window.events=[];window.current=null;
 window.mountProfile=(entry,credentialFields=[])=>{window.current?.destroy?.();window.events.length=0;const capability=providerAuthCapability(entry,{credentialFields});window.current=mountAuthCapabilitySurface(document.querySelector('#app'),{capability,onMethod:(strategy,meta)=>window.events.push(['method',strategy,!!meta?.selected]),onAction:value=>window.events.push(['action',value.strategy,value.value||''])});return capability;};
 window.mountProfile({id:'001',name:'Bluesky / AT Protocol',url:'https://bsky.app/',gate:'instance',note:'fixture'});
 </script></body></html>`;
}
async function fixture(work){
 const server=Bun.serve({port:0,fetch(request){const url=new URL(request.url);if(url.pathname==='/')return new Response(html(),{headers:{'content-type':'text/html'}});if(files[url.pathname])return new Response(files[url.pathname],{headers:{'content-type':'text/javascript'}});return new Response('not found',{status:404});}});
 try{await withChrome(async({open})=>{const tab=await open();await tab.go('http://127.0.0.1:'+server.port+'/');const run=async expression=>{const result=await tab.page('Runtime.evaluate',{expression,awaitPromise:true,returnByValue:true});if(result.exceptionDetails)throw Error(result.exceptionDetails.exception?.description||result.exceptionDetails.text);return result.result.value;};for(let n=0;n<80;n++){if(await run("!!document.querySelector('.pac-surface')"))break;await Bun.sleep(25);}await work(run);},{timeoutMs:20000,window:'900,700'});}finally{server.stop(true);}
}
test('isolated Chrome renders Bluesky as capability-specific auth rather than a generic sign-in card',async()=>fixture(async run=>{
 expect(await run("[...document.querySelectorAll('[role=tab]')].map(n=>n.textContent)")).toEqual(['Continue with Bluesky','Use an app password','Use public AT Protocol']);
 expect(await run("document.querySelector('[role=tab][aria-selected=true]').textContent")).toBe('Continue with Bluesky');
 expect(await run("document.querySelector('[aria-label=\"Handle or DID\"]').autocomplete")).toBe('username');
 expect(await run("document.querySelector('.pac-panel .pac-primary').disabled")).toBe(true);
 expect(await run("document.body.innerText.includes('native owner for this flow is not installed')")).toBe(true);
 expect(await run("document.querySelectorAll('.fsi-card').length")).toBe(0);
 await run("[...document.querySelectorAll('[role=tab]')].find(n=>n.textContent==='Use public AT Protocol').click()");
 expect(await run("document.querySelector('.pac-link').getAttribute('href')")).toBe('/sources/web');
 expect(await run("document.body.innerText.includes('Public reading grants no posting')")).toBe(true);
}));
test('isolated Chrome preserves signer, multi-grant, plan, and typed-token variations',async()=>fixture(async run=>{
 await run("window.mountProfile({id:'003',name:'Nostr Networks',url:'https://nostr.com/clients',gate:'instance',note:'fixture'})");
 expect(await run("[...document.querySelectorAll('[role=tab]')].map(n=>n.textContent)")).toEqual(['Use browser signer','Use remote signer','Read public Nostr']);
 expect(await run("document.querySelector('.pac-panel .pac-primary').textContent")).toBe('No browser signer detected');
 expect(await run("document.querySelector('.pac-panel .pac-primary').disabled")).toBe(true);
 await run("window.mountProfile({id:'051',name:'Shopify',url:'https://accounts.shopify.com/store-login',gate:'portal',note:'fixture'})");
 expect(await run("[...document.querySelectorAll('[role=tab]')].map(n=>n.textContent)")).toEqual(['Shopify-managed installation','Embedded app token exchange','Authorization code grant','Client credentials']);
 expect(await run("document.querySelector('.pac-features summary').textContent")).toBe('What this flow needs');
 await run("document.querySelector('.pac-features').open=true");expect(await run("document.body.innerText.includes('Managed Scopes')")).toBe(true);
 await run("window.mountProfile({id:'038',name:'Hashnode',url:'https://hashnode.com/settings/developer',gate:'portal',note:'fixture'},[{field:'token',kind:'token'}])");
 expect(await run("[...document.querySelectorAll('[role=tab]')].map(n=>n.textContent)")).toEqual(['Use Hashnode PAT','Use Hashnode connected-app login','Check Hashnode Pro API entitlement','Read public Hashnode content']);
 expect(await run("document.querySelector('.pac-panel .pac-owner').textContent")).toBe('Native credential');
 await run("[...document.querySelectorAll('[role=tab]')].find(n=>n.textContent==='Check Hashnode Pro API entitlement').click()");
 expect(await run("document.querySelector('.pac-panel .pac-owner').textContent")).toBe('Human');
 expect(await run("document.body.innerText.includes('Success proof')")).toBe(true);
 await run("window.mountProfile({id:'132',name:'Pixabay',url:'https://pixabay.com/accounts/login/',gate:'portal',note:'fixture'},[{field:'api_key',kind:'api_key'}])");
 expect(await run("[...document.querySelectorAll('[role=tab]')].map(n=>n.textContent)")).toEqual(['Use Pixabay API key','Respect Pixabay API use rules']);
 expect(await run("document.querySelector('.pac-panel .pac-owner').textContent")).toBe('Native credential');
}));
