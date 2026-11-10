/* Shared presentation of published product previews. Records and editor destinations remain owner supplied. */
(function(global){
  'use strict';
  const safeURL=(value,base)=>{
    if(typeof value!=='string'||value.length>4096||/[\\\u0000-\u001f\u007f]/.test(value))return null;
    try{const url=new URL(value,base);return url.protocol==='https:'&&!url.username&&!url.password?url.href:null;}catch{return null;}
  };
  const node=(document,tag,className,text)=>{const element=document.createElement(tag);element.className=className;if(text!==undefined)element.textContent=text;return element;};
  function readDirectory(list,base){
    const seen=new Set();return Array.from(list.querySelectorAll('a[data-demo]')).flatMap(source=>{
      const href=safeURL(source.getAttribute('href'),base),title=source.querySelector('strong')?.textContent.trim();
      if(!href||!title||seen.has(href))return [];seen.add(href);
      return [{href,title,summary:source.querySelector('.detail')?.textContent.trim()||'',kind:source.dataset.kind||'Interactive workspace',
        image:source.dataset.shot?safeURL(source.dataset.shot,base):null,alt:source.dataset.shotAlt||'',action:source.dataset.action||'Open workspace'}];
    });
  }
  function createCard(document,item,{className='showcase-card',base=document.baseURI}={}){
    const href=safeURL(item.href,base);if(!href||typeof item.title!=='string'||!item.title.trim())throw Error('A preview needs an exact permitted destination and title.');
    const card=node(document,'a',className);card.href=href;
    const imageURL=safeURL(item.image,base);
    if(imageURL&&item.alt){const image=node(document,'img','');image.src=imageURL;image.alt=item.alt;image.loading='lazy';image.decoding='async';image.referrerPolicy='no-referrer';image.addEventListener('error',()=>image.remove(),{once:true});card.append(image);}
    const body=node(document,'div','card-body');
    body.append(node(document,'span','card-type',item.kind||'Application'),node(document,'strong','heading',item.title),node(document,'span','summary',item.summary||''));
    // State is optional and provided by the current installation owner. The image never establishes it.
    if(item.state){if(!['installed','setup','workflow','public-preview'].includes(item.state))throw Error('Unknown application presentation state.');body.append(node(document,'span','ak-application-state',({installed:'Installed in this workspace',setup:'Review application setup',workflow:'Workflow configuration','public-preview':'Public example'})[item.state]));}
    if(item.contextLabel)body.append(node(document,'span','ak-application-context',item.contextLabel));
    body.append(node(document,'span','cta',(item.action||'Open workspace')+' ↗'));card.append(body);return card;
  }
  /** In a native list, pass already-authorized records including their actual per-destination href.
   * Preview metadata supplies only imagery/copy; it cannot replace an installed editor URL. */
  function joinPublishedPreview(record,preview){
    if(!record||!preview||typeof record.previewIdentity!=='string'||record.previewIdentity!==preview.identity)throw Error('Exact published-preview identity is required.');
    if(record.previewRevision&&record.previewRevision!==preview.revision)throw Error('The requested preview revision does not match.');
    return {...record,image:preview.image,alt:preview.alt,summary:record.summary||preview.summary,kind:record.kind||preview.kind};
  }
  function mountCollection(root,records,{document=root.ownerDocument,title='All applications',replace=false}={}){
    if(!Array.isArray(records)||records.length>1000)throw Error('A bounded owner-supplied list is required.');
    const original=[...root.childNodes],cards=records.map(item=>createCard(document,item,{className:'ak-application-card'}));
    const section=node(document,'section','ak-application-collection'),controls=node(document,'div','ak-application-controls'),label=node(document,'label','ak-application-search');
    const input=node(document,'input','');input.type='search';input.placeholder='Find an application';input.setAttribute('aria-label','Find an application');
    label.append(node(document,'span','',title),input);
    const modes=node(document,'div','ak-application-modes');modes.setAttribute('aria-label','Directory view');
    const gridButton=node(document,'button','','Visual'),listButton=node(document,'button','','Compact');
    const grid=node(document,'div','ak-application-grid');grid.setAttribute('role','list');
    const count=node(document,'p','ak-application-count');count.setAttribute('role','status');count.setAttribute('aria-live','polite');
    const empty=node(document,'p','ak-application-empty','No matching applications. Change your search to see the full list.');empty.hidden=true;
    let mode='grid';
    const view=value=>{mode=value;grid.dataset.layout=value;gridButton.setAttribute('aria-pressed',String(value==='grid'));listButton.setAttribute('aria-pressed',String(value==='list'));};
    for(const [button,value]of [[gridButton,'grid'],[listButton,'list']]){button.type='button';button.onclick=()=>view(value);modes.append(button);}
    const wrappers=cards.map(card=>{const wrapper=node(document,'div','ak-application-cell');wrapper.setAttribute('role','listitem');wrapper.append(card);grid.append(wrapper);return wrapper;});
    const filter=()=>{const terms=input.value.trim().toLocaleLowerCase().split(/\s+/).filter(Boolean);let visible=0;records.forEach((record,i)=>{const haystack=[record.title,record.summary,record.kind,record.contextLabel].join(' ').toLocaleLowerCase();const show=terms.every(term=>haystack.includes(term));wrappers[i].hidden=!show;if(show)visible++;});count.textContent=visible+' of '+records.length+' applications';empty.hidden=visible!==0;};
    input.addEventListener('input',filter);controls.append(label,modes);section.append(controls,count,grid,empty);view('grid');filter();
    if(replace)root.replaceChildren(section);else root.append(section);
    return Object.freeze({element:section,dispose(){input.removeEventListener('input',filter);section.remove();if(replace)root.replaceChildren(...original);},get layout(){return mode;}});
  }
  const api=Object.freeze({safeURL,readDirectory,createCard,joinPublishedPreview,mountCollection});
  global.AurekaiApplicationPreviews=api;
  if(typeof module==='object'&&module.exports)module.exports=api;
  if(typeof document==='undefined')return;
  const list=document.querySelector('#experiences [data-demo-list]');
  if(!list||document.getElementById('demo-showcase'))return;
  const entries=readDirectory(list,document.baseURI);if(!entries.length)return;
  const n=(tag,cls,text)=>node(document,tag,cls,text);
  const section=n('section','showcase');section.id='demo-showcase';section.setAttribute('aria-label','Featured application previews');
  const head=n('div','showcase-head'),intro=n('div','showcase-intro');
  intro.append(n('div','eyebrow','Featured work / public previews'),n('p','','Open an application or browse the complete list below.'));
  const adoption=n('a','action secondary','Review Aurekai setup');adoption.id='aurekai-adoption';adoption.href='https://aurekai.work/packs/setup';adoption.setAttribute('aria-describedby','aurekai-adoption-scope');
  const scope=n('p','','Keep and share a versioned workflow configuration in your Aurekai account. Demo drafts and application backends remain separate.');scope.id='aurekai-adoption-scope';intro.append(adoption,scope);
  const controls=n('div','showcase-actions'),previous=n('button','','←'),next=n('button','','→');
  for(const [button,label]of [[previous,'Previous preview'],[next,'Next preview']]){button.type='button';button.setAttribute('aria-label',label);button.setAttribute('aria-controls','demo-preview-track');}
  controls.append(previous,next);head.append(intro,controls);section.append(head);
  const track=n('div','showcase-scroll');track.id='demo-preview-track';track.tabIndex=0;track.setAttribute('role','region');track.setAttribute('aria-label','Application previews');
  entries.forEach(item=>track.append(createCard(document,item)));
  section.append(track,n('p','showcase-note','Screenshots appear only where a published capture is available. Preview edits do not authorize purchases, payments or external publication.'));
  list.parentNode.insertBefore(section,list);
  const updateControls=()=>{const maximum=Math.max(0,track.scrollWidth-track.clientWidth);previous.disabled=track.scrollLeft<=1;next.disabled=track.scrollLeft>=maximum-1;};
  const scroll=direction=>{const reduced=window.matchMedia?.('(prefers-reduced-motion: reduce)').matches;const distance=(track.firstElementChild?.getBoundingClientRect().width||350)+16;track.scrollBy({left:direction*distance,behavior:reduced?'auto':'smooth'});updateControls();};
  previous.addEventListener('click',()=>scroll(-1));next.addEventListener('click',()=>scroll(1));track.addEventListener('keydown',event=>{if(event.target!==track||event.altKey||event.ctrlKey||event.metaKey||event.shiftKey)return;if(event.key==='ArrowLeft'||event.key==='ArrowRight'){event.preventDefault();scroll(event.key==='ArrowLeft'?-1:1);}});
  track.addEventListener('scroll',updateControls,{passive:true});window.addEventListener('resize',updateControls,{passive:true});updateControls();
  // Upgrade the full list only when its scoped stylesheet has loaded; failure keeps the authored links.
  if(document.head&&typeof global.requestAnimationFrame==='function'){
    const sheet=document.createElement('link');sheet.rel='stylesheet';sheet.href=new URL('application-previews.css',document.currentScript?.src||new URL('assets/showcase.js',document.baseURI)).href;
    sheet.addEventListener('load',()=>global.requestAnimationFrame(()=>{
      if(list.dataset.previewEnhanced)return;
      const container=document.createElement('div');list.parentNode.insertBefore(container,list);
      try {mountCollection(container,entries,{document,title:'Explore every application'});list.hidden=true;list.dataset.previewEnhanced='true';}
      catch {container.remove();list.hidden=false;}
    }),{once:true});document.head.append(sheet);
  }
})(typeof window==='object'?window:globalThis);
