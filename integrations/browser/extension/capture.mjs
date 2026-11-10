// Serializable function for chrome.scripting.executeScript in its default isolated world.
export function capturePage(){
  if(!/^https?:$/.test(location.protocol))throw Error('Select an ordinary HTTP(S) page');
  const structured=[];let parseErrors=0,total=0;
  for(const el of [...document.querySelectorAll('script[type="application/ld+json"]')].slice(0,30)){
    const s=el.textContent||'';if(total+s.length>120000)break;total+=s.length;
    try{structured.push(JSON.parse(s))}catch{parseErrors++}
  }
  // Read visible contract-related prose, not input values, cookies, storage or form credentials.
  const terms=[];let inspected=0;
  for(const el of document.querySelectorAll('p,li,summary,[data-subscription-terms]')){
    if(++inspected>1200||terms.length>=24)break;
    if(el.closest('input,textarea,[contenteditable="true"],form'))continue;
    const s=(el.innerText||'').replace(/\s+/g,' ').trim();
    if(!s||s.length>700||!/(renew|cancel|return|refund|ship|delivery|styling|subscription|billed|trial)/i.test(s))continue;
    if(!el.getClientRects().length||getComputedStyle(el).visibility==='hidden')continue;
    if(!terms.includes(s))terms.push(s);
  }
  // Read only the current post's public fields. Never retain the full hydration
  // state: it can include session/account material unrelated to this selection.
  let platform_post=null;
  const pageURL=new URL(location.href),match=/^\/@([^/]+)\/(video|photo)\/(\d+)\/?$/.exec(pageURL.pathname);
  if ((pageURL.hostname==='www.tiktok.com'||pageURL.hostname==='tiktok.com')&&match) {
    const id=match[3],handle=decodeURIComponent(match[1]);
    const clean=(v,n)=>typeof v==='string'?v.slice(0,n):'';
    const integer=v=>(Number.isSafeInteger(v)&&v>=0)?String(v):(typeof v==='string'&&/^\d{1,30}$/.test(v)?v:null);
    for (const name of ['__UNIVERSAL_DATA_FOR_REHYDRATION__','SIGI_STATE']) {
      const el=document.getElementById(name);if(!el)continue;
      const body=el.textContent||'';
      if(body.length>1048576){platform_post={status:'too_large',id};continue;}
      try {
        const data=JSON.parse(body),detail=data?.__DEFAULT_SCOPE__?.['webapp.video-detail'];
        const item=name==='SIGI_STATE'?data?.ItemModule?.[id]:detail?.itemInfo?.itemStruct;
        if(detail?.statusCode!=null && detail.statusCode!==0 && detail.statusCode!=='0'){platform_post={status:'unavailable',id};continue;}
        if(!item){platform_post={status:'not_present',id};continue;}
        if(integer(item.id)!==id){platform_post={status:'identity_mismatch',id};continue;}
        if(item.secret===true||item.forFriend===true||item.privateItem===true){platform_post={status:'restricted',id};break;}
        const author=typeof item.author==='string'?item.author:item.author?.uniqueId;
        if(author!==handle){platform_post={status:'author_mismatch',id};continue;}
        platform_post={status:'observed',id,kind:match[2],url:pageURL.origin+pageURL.pathname,
          author:clean(author,100),description:clean(item.desc,6000),created_seconds:integer(item.createTime),
          duration_seconds:integer(item.video?.duration),image_count:Array.isArray(item.imagePost?.images)?item.imagePost.images.length:0,
          hashtags:(Array.isArray(item.challenges)?item.challenges:[]).slice(0,30).map(v=>clean(v?.title,100)).filter(Boolean),
          counts:Object.fromEntries(['playCount','diggCount','shareCount','commentCount'].map(k=>[k,integer(item.stats?.[k])])),
          origin:'selected_page_hydration',coverage:'current_post_metadata_only'};
        break;
      }catch{platform_post={status:'invalid_payload',id};}
    }
  }
  const active=document.activeElement;
  const selected=active?.matches('input,textarea,[contenteditable="true"]')?'':String(window.getSelection()||'').slice(0,8000);
  return {url:location.href,title:document.title,canonical:document.querySelector('link[rel="canonical"]')?.href||location.href,
    captured_at:new Date().toISOString(),structured,platform_post,selected_text:selected,term_snippets:terms,
    fixture:document.documentElement.dataset.fixture==='true',coverage:{jsonld_blocks:structured.length,jsonld_parse_errors:parseErrors,inspected_text_elements:inspected,kind:'bounded_user_initiated_selection'}};
}
