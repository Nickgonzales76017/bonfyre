(function () {
  'use strict';
  const script=document.currentScript, toolbar=script.previousElementSibling;
  const run=()=>{
    const root=toolbar?.closest('.pack-workspace');
    if(!root||toolbar.dataset.browserReady)return;toolbar.dataset.browserReady='true';
    const cards=[...root.querySelectorAll('.pack-tool')].filter(card=>card.closest('.pack-workspace')===root);
    const input=toolbar.querySelector('[data-pack-filter]'),kind=toolbar.querySelector('[data-pack-kind]'),sort=toolbar.querySelector('[data-pack-sort]');
    const count=toolbar.querySelector('[data-pack-count]'),empty=toolbar.querySelector('[data-pack-empty]'),reset=toolbar.querySelector('[data-pack-reset]');
    const abort=new AbortController(),signals={signal:abort.signal};
    const entries=cards.map((card,order)=>({card,order,parent:card.parentElement,name:card.querySelector('h3')?.textContent.trim()||'Tool',kind:card.querySelector('.pack-kind')?.textContent.trim()||'Tool',text:card.textContent.normalize('NFKC').toLocaleLowerCase()}));
    const kinds=[...new Set(entries.map(entry=>entry.kind))];for(const value of kinds)kind.add(new Option(value,value));kind.parentElement.hidden=kinds.length<2;
    const groups=[...new Set(entries.map(entry=>entry.parent.closest('[data-setup-kind]')).filter(Boolean))];
    function filter(){const words=input.value.normalize('NFKC').trim().toLocaleLowerCase().split(/\s+/).filter(Boolean);let shown=0;
      for(const e of entries){e.card.hidden=!(words.every(word=>e.text.includes(word))&&(!kind.value||e.kind===kind.value));if(!e.card.hidden)shown++;}
      for(const group of groups)group.hidden=!entries.some(e=>group.contains(e.card)&&!e.card.hidden);
      count.textContent=shown+' of '+cards.length+' shown';empty.hidden=shown!==0;reset.hidden=!input.value&&!kind.value;}
    input.addEventListener('input',filter,signals);kind.addEventListener('change',filter,signals);
    reset.addEventListener('click',()=>{input.value='';kind.value='';filter();input.focus();},signals);
    sort.addEventListener('change',()=>{for(const parent of new Set(entries.map(e=>e.parent))){const rows=entries.filter(e=>e.parent===parent);rows.sort((a,b)=>sort.value==='name'?a.name.localeCompare(b.name)||a.order-b.order:a.order-b.order);for(const row of rows)parent.append(row.card);}},signals);
    for(const button of toolbar.querySelectorAll('[data-pack-view]'))button.addEventListener('click',()=>{root.dataset.packView=button.dataset.packView;for(const peer of toolbar.querySelectorAll('[data-pack-view]'))peer.setAttribute('aria-pressed',String(peer===button));},signals);
    root.addEventListener('keydown',event=>{if((event.ctrlKey||event.metaKey)&&event.key.toLowerCase()==='k'&&!event.altKey){event.preventDefault();input.focus();input.select();}},signals);
    let dialog=null,trigger=null,current=null,index=0,serial=0;const cleanup=[];
    const make=(tag,cls,text)=>{const node=document.createElement(tag);if(cls)node.className=cls;if(text)node.textContent=text;return node;};
    const button=(text,key)=>{const node=make('button','',text);node.type='button';if(key)node.dataset[key]='';return node;};
    const safeImage=raw=>{try{const u=new URL(raw,location.href);return u.origin===location.origin&&!u.search&&!u.hash&&/^\/apps\/material\/[a-f0-9]{64}\/assets\/(?:[A-Za-z0-9_-][A-Za-z0-9_.-]*\/)*[A-Za-z0-9_-][A-Za-z0-9_.-]*\.(?:png|jpg|webp)$/.test(u.pathname)&&!u.pathname.includes('..')?u.href:null;}catch{return null;}};
    function close(restore=true){serial++;const opener=trigger;trigger=null;current=null;for(const fn of cleanup.splice(0))fn();if(dialog){if(dialog.open)dialog.close();dialog.remove();dialog=null;}if(restore&&opener?.isConnected&&!opener.closest('[hidden]'))opener.focus({preventScroll:true});}
    function inspect(entry,opener){
      const screens=[...entry.card.querySelectorAll('.pack-preview')].map(i=>({url:safeImage(i.getAttribute('src')),caption:i.alt||'Application preview'})).filter(i=>i.url);
      if(!screens.length||typeof HTMLDialogElement==='undefined'||!HTMLDialogElement.prototype.showModal)return;
      close(false);current=entry;trigger=opener;index=0;
      dialog=make('dialog','bf-pack-inspector');dialog.dataset.packInspector='';dialog.setAttribute('aria-label',entry.name+' previews');
      const heading=make('header','bf-preview-heading'),name=make('h2','',entry.name),dismiss=button('Close','previewClose');
      heading.append(make('div','bf-preview-title'),dismiss);heading.firstChild.append(make('p','bf-preview-eyebrow','Application preview'),name);
      const body=make('div','bf-preview-body'),left=make('section','bf-preview-visual'),divider=make('div','bf-preview-divider'),info=make('aside','bf-preview-info');
      divider.tabIndex=0;divider.setAttribute('role','separator');divider.setAttribute('aria-orientation','vertical');divider.setAttribute('aria-label','Preview width');divider.setAttribute('aria-valuemin','45');divider.setAttribute('aria-valuemax','78');
      let share=68;const setShare=value=>{share=Math.min(78,Math.max(45,value));body.style.setProperty('--bf-preview-share',share+'%');divider.setAttribute('aria-valuenow',String(Math.round(share)));divider.setAttribute('aria-valuetext',Math.round(share)+' percent preview');};setShare(share);
      const tools=make('div','bf-preview-tools'),previous=button('Previous','previewPrevious'),next=button('Next','previewNext'),number=make('span','bf-preview-number'),fit=button('Fit','previewFit'),actual=button('100%','previewActual');
      fit.setAttribute('aria-pressed','true');actual.setAttribute('aria-pressed','false');const zoom=make('div','bf-preview-segmented');zoom.setAttribute('role','group');zoom.setAttribute('aria-label','Image scale');zoom.append(fit,actual);tools.append(previous,number,next,zoom);
      const viewport=make('div','bf-preview-viewport'),image=make('img'),status=make('p','bf-preview-status');viewport.tabIndex=0;viewport.setAttribute('role','region');viewport.setAttribute('aria-label','Scrollable application screenshot');image.decoding='async';image.referrerPolicy='no-referrer';viewport.append(image);status.setAttribute('role','status');status.setAttribute('aria-live','polite');
      const thumbnails=make('div','bf-preview-thumbnails');thumbnails.setAttribute('role','group');thumbnails.setAttribute('aria-label','Available screenshots');const caption=make('p','bf-preview-caption');
      info.append(make('p','bf-preview-eyebrow',entry.kind),make('h3','','About this tool'));const purpose=entry.card.querySelector('.pack-purpose')?.textContent.trim();if(purpose)info.append(make('p','',purpose));info.append(caption,make('p','bf-preview-disclosure','These are retained screenshots. Open the editor or demo to use its controls.'));
      const actions=make('nav','bf-preview-actions');actions.setAttribute('aria-label','Continue with '+entry.name);const seen=new Set();
      for(const original of entry.card.querySelectorAll('.pack-card-actions a[href]')){let u;try{u=new URL(original.getAttribute('href'),location.href);}catch{continue;}if(!['http:','https:'].includes(u.protocol)||u.username||u.password||seen.has(u.href))continue;seen.add(u.href);const link=make('a','',original.textContent.trim());link.href=u.href;if(u.origin!==location.origin)link.rel='noopener noreferrer';actions.append(link);}
      info.append(actions);left.append(tools,viewport,status,thumbnails);body.append(left,divider,info);dialog.append(heading,body);
      const thumbButtons=screens.map((screen,i)=>{const b=button(String(i+1),'previewScreen');b.setAttribute('aria-label','Screenshot '+(i+1)+': '+screen.caption);const thumb=make('img');thumb.src=screen.url;thumb.alt='';thumb.loading='lazy';thumb.decoding='async';b.prepend(thumb);b.addEventListener('click',()=>show(i));thumbnails.append(b);return b;});
      function show(i){if(!dialog||current!==entry)return;index=i;const ticket=++serial;image.hidden=true;viewport.setAttribute('aria-busy','true');status.textContent='Loading screenshot…';image.alt=screens[i].caption;caption.textContent=screens[i].caption;number.textContent=(i+1)+' / '+screens.length;previous.disabled=i===0;next.disabled=i+1===screens.length;for(let n=0;n<thumbButtons.length;n++)thumbButtons[n].setAttribute('aria-pressed',String(n===i));
        const loaded=()=>{if(ticket!==serial||!dialog)return;viewport.removeAttribute('aria-busy');image.hidden=false;status.textContent='Screenshot '+(i+1)+' of '+screens.length;};
        const failed=()=>{if(ticket!==serial||!dialog)return;viewport.removeAttribute('aria-busy');image.hidden=true;status.textContent='This screenshot could not be loaded. The editor and setup links remain available.';};
        image.onload=loaded;image.onerror=failed;image.src=screens[i].url;if(image.complete){if(image.naturalWidth)loaded();else failed();}viewport.scrollTo(0,0);}
      const scale=large=>{viewport.dataset.scale=large?'actual':'fit';fit.setAttribute('aria-pressed',String(!large));actual.setAttribute('aria-pressed',String(large));viewport.scrollTo(0,0);};fit.onclick=()=>scale(false);actual.onclick=()=>scale(true);previous.onclick=()=>{if(index>0)show(index-1);};next.onclick=()=>{if(index+1<screens.length)show(index+1);};dismiss.onclick=()=>close();dialog.addEventListener('cancel',event=>{event.preventDefault();close();});
      dialog.addEventListener('keydown',event=>{
        if(event.key!=='Tab'||!dialog)return;
        const stops=[...dialog.querySelectorAll('button,a[href],input,select,[tabindex]')].filter(n=>!n.disabled&&n.tabIndex>=0&&n.getClientRects().length&&getComputedStyle(n).visibility!=='hidden');
        const first=stops[0],last=stops.at(-1);
        if(!first){event.preventDefault();dismiss.focus();return;}
        if(event.shiftKey&&document.activeElement===first){event.preventDefault();last.focus();}
        else if(!event.shiftKey&&document.activeElement===last){event.preventDefault();first.focus();}
      });
      divider.addEventListener('keydown',event=>{let value=share;if(event.key==='ArrowLeft')value-=event.shiftKey?10:2;else if(event.key==='ArrowRight')value+=event.shiftKey?10:2;else if(event.key==='Home')value=45;else if(event.key==='End')value=78;else return;event.preventDefault();setShare(value);});
      let dragging=false;divider.addEventListener('pointerdown',event=>{if(event.button!==0)return;dragging=true;divider.setPointerCapture(event.pointerId);divider.focus();event.preventDefault();});divider.addEventListener('pointermove',event=>{if(!dragging)return;const rect=body.getBoundingClientRect();if(rect.width)setShare((event.clientX-rect.left)/rect.width*100);});for(const type of ['pointerup','pointercancel','lostpointercapture'])divider.addEventListener(type,()=>{dragging=false;});
      document.body.append(dialog);dialog.showModal();dismiss.focus();show(0);cleanup.push(()=>{image.onload=image.onerror=null;image.removeAttribute('src');});
    }
    if(typeof HTMLDialogElement!=='undefined'&&HTMLDialogElement.prototype.showModal)for(const entry of entries){if(![...entry.card.querySelectorAll('.pack-preview')].some(i=>safeImage(i.getAttribute('src'))))continue;const b=button('Inspect previews','packInspect');b.className='bf-pack-inspect';b.setAttribute('aria-label','Inspect previews of '+entry.name);b.setAttribute('aria-haspopup','dialog');b.addEventListener('click',()=>inspect(entry,b),signals);entry.card.querySelector('.pack-art').append(b);}
    function suspend(){close(false);}window.addEventListener('pagehide',suspend,signals);document.addEventListener('visibilitychange',()=>{if(document.hidden)suspend();},signals);window.addEventListener('bonfyre:account-changed',suspend,signals);
    const observer=new MutationObserver(()=>{if(!root.isConnected)dispose();else if(current&&!root.contains(current.card))close(false);});observer.observe(document.body,{childList:true,subtree:true});
    function dispose(){close(false);abort.abort();observer.disconnect();}
    document.addEventListener('htmx:beforeCleanupElement',event=>{const node=event.detail?.elt;if(node&&(node===root||node.contains(root)))dispose();},signals);
    filter();toolbar.hidden=false;
  };
  if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',run,{once:true});else queueMicrotask(run);
})();
