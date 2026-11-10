#ifndef BONFYRE_ACCOUNT_HOME_CONTINUATION_H
#define BONFYRE_ACCOUNT_HOME_CONTINUATION_H
/* HTML-only, session-gated presentation of the existing private installed-app projection. */
#define BF_HOME_CONTINUATION \
"<script>\n" \
"(function(){\n" \
"'use strict';\n" \
"if(location.pathname!=='/')return;\n" \
"const bar=document.currentScript.previousElementSibling;\n" \
"const slot=bar?.querySelector('[data-account-slot]');\n" \
"if(!slot)return;\n" \
"let version=0,lastIdentity='',current=null,done=false;\n" \
"function clear(){version++;current?.remove();current=null;done=false;document.getElementById('ak-home-product-css')?.remove();}\n" \
"async function sync(force){\n" \
"const session=slot.querySelector('.ak-session');\n" \
"const signed=session?.dataset.sessionState==='signed-in';\n" \
"const fingerprint=signed?session.innerHTML:'';\n" \
"if(!signed){if(current||lastIdentity)clear();lastIdentity='';return;}\n" \
"if(!force&&fingerprint===lastIdentity&&(current||done))return;\n" \
"lastIdentity=fingerprint;clear();const generation=++version;\n" \
"try{\n" \
" const response=await fetch('/apps/_cards?context=self',{credentials:'same-origin',cache:'no-store',redirect:'error',headers:{Accept:'text/html'},signal:AbortSignal.timeout(8500)});\n" \
" if(!response.ok||!/^text\\/html(?:;|$)/i.test(response.headers.get('content-type')||''))return;\n" \
" const declared=response.headers.get('content-length');if(declared&&Number(declared)>262144)return;\n" \
" const html=await response.text();if(html.length>262144||generation!==version)return;\n" \
" const documentPart=new DOMParser().parseFromString(html,'text/html'),sourceCards=documentPart.querySelector('[data-account-tools]');\n" \
" const all=[...sourceCards?.querySelectorAll('.pack-tool')||[]];\n" \
" const profile=session.querySelector('[data-public-profile-menu],[data-public-profile]');\n" \
" if(!all.length&&!profile)return;\n" \
" const main=document.querySelector('body > main')||document.querySelector('main');if(!main)return;\n" \
" const wrap=document.createElement('section');wrap.className='ak-home-continuation';wrap.setAttribute('aria-label','Your existing Aurekai work');\n" \
" const head=document.createElement('div');head.className='ak-home-continuation-head';\n" \
" const copy=document.createElement('div');const eyebrow=document.createElement('p');eyebrow.className='ak-home-continuation-kicker';eyebrow.textContent='Your Aurekai / Continue';\n" \
" const title=document.createElement('h2');title.textContent='Pick up what you made yours.';\n" \
" const explanation=document.createElement('p');explanation.textContent='Your real installed tools, saved work, and public presence—without starting an editor until you open it.';\n" \
" copy.append(eyebrow,title,explanation);\n" \
" const links=document.createElement('nav');links.className='ak-home-continuation-links';links.setAttribute('aria-label','Personal continuation');\n" \
" const open=document.createElement('a');open.href='/apps?context=self';open.textContent='All your tools ↗';links.append(open);\n" \
" if(profile){const href=profile.getAttribute('href');if(href&&href.startsWith('https://')){const a=document.createElement('a');a.href=href;a.textContent='Your public profile ↗';links.append(a);}}\n" \
" head.append(copy,links);wrap.append(head);\n" \
" if(all.length){\n" \
"  const shelf=document.createElement('div');shelf.className='ak-home-continuation-grid pack-workspace';\n" \
"  const grid=document.createElement('div');grid.className='pack-cards';\n" \
"  for(const item of all.slice(0,3)){const clone=document.importNode(item,true);clone.querySelectorAll('script,style,form').forEach(n=>n.remove());grid.append(clone);}\n" \
"  shelf.append(grid);wrap.append(shelf);\n" \
"  const css=[...documentPart.querySelectorAll('style')].find(s=>s.textContent.includes('.pack-tool'));\n" \
"  if(css&&!document.getElementById('ak-home-product-css')){const style=document.createElement('style');style.id='ak-home-product-css';style.textContent=css.textContent;document.head.append(style);}\n" \
" }\n" \
" if(generation!==version)return;\n" \
" const first=main.children[0];first?first.after(wrap):main.prepend(wrap);current=wrap;done=true;\n" \
"}catch(_){if(generation===version)done=false;}\n" \
"}\n" \
"const style=document.createElement('style');style.textContent='.ak-home-continuation{max-width:1460px;margin:clamp(32px,5vw,80px) auto;padding:clamp(24px,4vw,50px);border-radius:28px;background:linear-gradient(130deg,#172f24,#375d40);color:#ecf8df;box-sizing:border-box}.ak-home-continuation-head{display:flex;align-items:end;justify-content:space-between;flex-wrap:wrap;gap:20px}.ak-home-continuation h2{font:400 clamp(33px,5vw,67px)/1.02 Georgia,serif;letter-spacing:-.05em;margin:8px 0 14px}.ak-home-continuation-kicker{font:600 11px monospace;text-transform:uppercase;letter-spacing:.13em;color:#c6e7ac}.ak-home-continuation-head p{max-width:68ch;line-height:1.5}.ak-home-continuation-links{display:flex;flex-wrap:wrap;gap:12px}.ak-home-continuation-links a{color:#eaf8dc;border:1px solid #d4ebbc9a;border-radius:99px;padding:10px 15px;text-decoration:none;font-weight:600}.ak-home-continuation-links a:hover{background:#ffffff19}.ak-home-continuation-grid .pack-cards{margin:30px 0 0;grid-template-columns:repeat(3,minmax(0,1fr))}.ak-home-continuation-grid .pack-tool{min-width:0}@media(max-width:850px){.ak-home-continuation{margin:20px 12px;padding:24px;border-radius:19px}.ak-home-continuation-grid .pack-cards{grid-template-columns:1fr}}@media(prefers-reduced-motion:reduce){.ak-home-continuation *{animation:none!important;transition:none!important}}';document.head.append(style);\n" \
"const observe=new MutationObserver(()=>{void sync()});observe.observe(slot,{childList:true,subtree:true});window.addEventListener('pageshow',()=>{void sync(true)});window.addEventListener('focus',()=>{void sync(true)});document.addEventListener('visibilitychange',()=>{if(!document.hidden)void sync(true)});document.addEventListener('DOMContentLoaded',()=>{void sync()},{once:true});void sync();\n" \
"})();\n" \
"</script>\n"
#endif
