#ifndef BONFYRE_PRODUCTION_HOME_H
#define BONFYRE_PRODUCTION_HOME_H
/* A supporting home section over the published production projection. The
 * territory's own editorial opening stays first; the catalogue remains whole. */
#define BF_PUBLIC_PRODUCTION_HOME \
"<script>(function(){if(location.pathname!=='/'||!/^aurekai\\.(work|life|live|shop)$/.test(location.hostname)||document.querySelector('script[data-production-home]'))return;const s=document.createElement('script');s.src='/_surface/production-home.js';s.defer=true;s.dataset.productionHome='';document.head.append(s);})();</script>"
#define BF_PUBLIC_PRODUCTION_HOME_SCRIPT \
"(function(){\n" \
"'use strict';if(location.pathname!=='/'||!/^aurekai\\.(work|life|live|shop)$/.test(location.hostname))return;\n" \
"let slot=null,observer=null,pending=null,suspended=false,ready=false;\n" \
"const homeStyle='.production-gallery[data-home-showcase]{max-width:none;margin:56px 0;padding:0;background:none;color:var(--w-ink,#eef3e4);border:0;border-radius:0}.production-gallery[data-home-showcase]>h2{font:400 clamp(40px,6vw,88px)/.95 var(--w-face-display,Instrument Serif,Georgia,serif);letter-spacing:-.03em;text-transform:none;max-width:none;margin:0 0 12px}.production-gallery[data-home-showcase]>h2+p{max-width:60ch;margin:0 0 20px;opacity:.8}.production-gallery[data-home-showcase]>.production-editions{display:block;margin:0;counter-reset:made}.production-gallery[data-home-showcase] .production-edition{display:grid;grid-template-columns:minmax(0,5fr) minmax(0,6fr);gap:clamp(18px,4vw,56px);align-items:start;padding:28px 0;border:0;border-top:1px solid var(--w-rule,#2b3a2f);border-radius:0;background:none!important;box-shadow:none;animation:none;counter-increment:made}.production-gallery[data-home-showcase] .production-edition:last-child{border-bottom:1px solid var(--w-rule,#2b3a2f)}.production-gallery[data-home-showcase] .production-edition>header{padding:0;min-height:0;background:none!important;color:inherit}.production-gallery[data-home-showcase] .production-edition>header::before{content:counter(made,decimal-leading-zero);display:block;font:600 12px/1 var(--w-face-mono,DM Mono,monospace);color:var(--w-accent,#b5df32);margin:0 0 10px}.production-gallery[data-home-showcase] .production-edition>header::after{display:none}.production-gallery[data-home-showcase] .production-edition h3{font:400 clamp(28px,3vw,44px)/1.05 var(--w-face-display,Instrument Serif,Georgia,serif);letter-spacing:-.02em;margin:0;max-width:none;color:inherit}.production-gallery[data-home-showcase] .production-copy{padding:0;background:none!important;color:inherit}.production-gallery[data-home-showcase] .production-copy>p:first-child{margin:0 0 10px;line-height:1.55;opacity:.85}.production-gallery[data-home-showcase] details>summary{cursor:pointer;padding:8px 0;font:600 13px/1.5 var(--w-face-body,system-ui)}.production-gallery[data-home-showcase] .production-outline-preview{margin:10px 0}.production-gallery[data-home-showcase] a{display:inline-block;margin-top:8px;padding:9px 18px;border-radius:999px;border:1px solid var(--w-accent,#b5df32);color:inherit;text-decoration:none;white-space:normal;overflow-wrap:anywhere}.production-gallery[data-home-showcase] a:hover{background:var(--w-accent,#b5df32);color:#111913}.production-gallery[data-home-showcase] :focus-visible{outline:3px solid var(--w-accent,#b5df32);outline-offset:3px}@media(max-width:720px){.production-gallery[data-home-showcase] .production-edition{grid-template-columns:1fr;gap:12px;padding:22px 0}.production-gallery[data-home-showcase]{margin:36px 0}}';\n" \
"function compact(section){\n" \
" section.removeAttribute('style');section.dataset.homeShowcase='';\n" \
" const title=section.querySelector(':scope > h2');if(title)title.textContent='Made with Aurekai';\n" \
" const intro=section.querySelector(':scope > h2 + p');if(intro)intro.textContent='Explore a published arrangement, then continue with your own material.';\n" \
" const kicker=section.querySelector(':scope > .production-kicker');if(kicker)kicker.remove();\n" \
" const cards=[...section.querySelectorAll('[data-production-design]')];\n" \
" for(const [index,card]of cards.entries()){\n" \
"  if(index>=3){card.remove();continue;}\n" \
"  const copy=card.querySelector('.production-copy');if(!copy)continue;\n" \
"  const details=document.createElement('details'),summary=document.createElement('summary');summary.textContent='How this is arranged';details.append(summary);\n" \
"  for(const node of [...copy.querySelectorAll('.production-outline-preview,.production-handoff,.production-scope')])details.append(node);\n" \
"  const link=copy.querySelector('a');link?link.before(details):copy.append(details);\n" \
"  for(const label of card.querySelectorAll('header .production-kicker'))label.remove();\n" \
" }return section;\n" \
"}\n" \
"async function load(){\n" \
" if(ready||suspended||pending||!slot?.isConnected)return;const request=new AbortController();pending=request;const timer=setTimeout(()=>request.abort(),10000);\n" \
" try{\n" \
"  const r=await fetch('/_surface/productions',{credentials:'omit',cache:'no-store',redirect:'error',headers:{Accept:'text/html'},signal:request.signal});\n" \
"  if(!r.ok||!/^text\\/html(?:;|$)/i.test(r.headers.get('content-type')||''))return;\n" \
"  const reader=r.body?.getReader();if(!reader)return;let size=0;const chunks=[];\n" \
"  try{for(;;){const x=await reader.read();if(x.done)break;size+=x.value.length;if(size>262144){await reader.cancel();return;}chunks.push(x.value);}}finally{reader.releaseLock();}\n" \
"  if(request.signal.aborted||pending!==request||suspended||!slot.isConnected)return;\n" \
"  const bytes=new Uint8Array(size);let at=0;for(const b of chunks){bytes.set(b,at);at+=b.length;}\n" \
"  const doc=new DOMParser().parseFromString(new TextDecoder('utf-8',{fatal:true}).decode(bytes),'text/html'),section=doc.querySelector('[data-public-productions]');\n" \
"  if(!section?.querySelector('[data-production-design]')){slot.remove();ready=true;return;}\n" \
"  const kept=compact(document.importNode(section,true));kept.querySelectorAll('script,iframe,form').forEach(n=>n.remove());\n" \
"  const css=doc.querySelector('style');if(!document.getElementById('public-production-css')){const style=document.createElement('style');style.id='public-production-css';style.textContent=(css?.textContent||'')+homeStyle;document.head.append(style);}\n" \
"  slot.replaceWith(kept);slot=kept;ready=true;observer?.disconnect();\n" \
" }catch(_){}finally{clearTimeout(timer);if(pending===request)pending=null;}\n" \
"}\n" \
"function observe(){\n" \
" if(ready||suspended||!slot?.isConnected)return;observer?.disconnect();\n" \
" if(typeof IntersectionObserver!=='function'){void load();return;}\n" \
" observer=new IntersectionObserver(entries=>{if(entries.some(x=>x.isIntersecting)){observer.disconnect();void load();}},{rootMargin:'600px'});observer.observe(slot);\n" \
"}\n" \
"function open(){\n" \
" if(document.querySelector('[data-public-productions],[data-home-production-slot]'))return;const main=document.querySelector('body>main')||document.querySelector('main');if(!main)return;\n" \
" slot=document.createElement('aside');slot.dataset.homeProductionSlot='';slot.setAttribute('aria-label','Published production examples');\n" \
" const tools=main.querySelector(':scope > [data-work-entrance]'),footer=main.querySelector(':scope > footer');\n" \
" if(tools)tools.after(slot);else if(footer)footer.before(slot);else main.append(slot);observe();\n" \
"}\n" \
"window.addEventListener('pagehide',()=>{suspended=true;observer?.disconnect();const old=pending;pending=null;old?.abort();});\n" \
"window.addEventListener('pageshow',event=>{suspended=false;if(event.persisted)observe();});\n" \
"if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',open,{once:true});else open();\n" \
"})();"
#endif
