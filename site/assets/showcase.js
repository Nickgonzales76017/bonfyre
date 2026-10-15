/* Exact published experience destinations. Internal docs are not playable demos. */
(function () {
  const anchor=document.querySelector('#experiences .items');
  if (!anchor) return;
  const entries=[
    {title:'Acquisition Brief Studio',kind:'Buyer intelligence · interactive',href:'https://nickgonzales76017.github.io/pages-procurement-memory/brief-studio/',image:'assets/shots/brief-studio-preview.svg',alt:'Illustrated visual study, not a screenshot: acquisition questions and costed buyer brief',summary:'Compose ITAD, property, civic and supplier reviews, compare proof costs and export a proposal.',action:'Open the studio'},
    {title:'Dallas Council Media',kind:'Civic media · interactive',href:'https://nickgonzales76017.github.io/pages-town-box/civic-council/',image:'assets/shots/council-media-preview.svg',alt:'Illustrated visual study, not a screenshot: exact agenda record joined to video chapter markers',summary:'Explore exact agenda-to-video joins and carry an official file into Brief Studio.',action:'View council records'},
    {title:'Shift Continuity Studio',kind:'Operator handoff · interactive',href:'https://nickgonzales76017.github.io/pages-shift-handoff/continuity/',image:'assets/shots/shift-continuity-preview.svg',alt:'Illustration of the Shift Continuity source-linked six-item workboard and Alex-to-Morgan handoff; not a screenshot',summary:'Claim work, resolve blockers, review critical incidents and require exact next-shift acknowledgement.',action:'Open continuity workbench'},
    {title:'Funding & Impact Studio',kind:'Funding operations · interactive',href:'https://nickgonzales76017.github.io/pages-grant-evidence/impact-studio/',image:'assets/shots/funding-impact-preview.svg',alt:'Illustrated editable $48K request, $24K declared match and source-linked $72K impact proposal; not a screenshot',summary:'Balance grants and matching costs, compare public programs, review original sources and track milestone evidence.',action:'Open funding studio'},
    {title:'Retail Fixture Planner',kind:'Design and costing · interactive',href:'https://aurekai-work-samples.pages.dev/red3-planner/',image:'https://aurekai-work-samples.pages.dev/assets/shots/planner.jpg',alt:'Actual retail planner screenshot with movable fixtures and running costs',summary:'Arrange sourced RED3 fixtures, compare fit and prepare a reviewable supplier brief.',action:'Try retail planning'},
    {title:'Catalogue Workbench',kind:'Product operations · interactive',href:'https://aurekai-work-samples.pages.dev/catalogue/',image:'https://aurekai-work-samples.pages.dev/assets/shots/catalogue.jpg',alt:'Actual product editor screenshot with variant details',summary:'Inspect a source catalogue, edit variants and compare drafts without publishing.',action:'Try product editing'},
    {title:'Multitrack Creator Studio',kind:'Native audio · production workstation',href:'https://nickgonzales76017.github.io/pages-podcast-plant/studio/',image:'assets/shots/creator-studio-preview.svg',alt:'Faithful illustrated arrangement view of Creator Studio with four edited voice, music, room and field tracks; not a screenshot',summary:'Arrange imported audio with source trims, fades, pan, markers, native C/WASM rendering, WAV exports and podcast handoff.',action:'Open multitrack studio'},
    {title:'Narrated Quick Mix',kind:'Voice production · interactive',href:'https://nickgonzales76017.github.io/pages-podcast-plant/studio/quick/',image:'assets/shots/creator-quick-mix-preview.svg',alt:'Illustrated real-audio Quick Mix with four licensed recordings, level controls and dialogue-aware music ducking',summary:'Mix real voice, CC0 music, location sound and credited Foley; create a voiced opener, compare ducking and export a cut.',action:'Try a real voice mix'}
  ];
  const section=document.createElement('section');section.className='showcase';section.setAttribute('aria-label','Published interactive demos');
  const head=document.createElement('div');head.className='showcase-head';
  const left=document.createElement('div'),eyebrow=document.createElement('div');eyebrow.className='eyebrow';eyebrow.textContent='Featured work / live previews';
  const subtitle=document.createElement('p');subtitle.textContent='Open working software. Docs, guides and unbuilt concepts are not counted as playable demos.';left.append(eyebrow,subtitle);
  const actions=document.createElement('div');actions.className='showcase-actions';
  const prev=document.createElement('button'),next=document.createElement('button');prev.type=next.type='button';prev.setAttribute('aria-label','Scroll to previous demo');next.setAttribute('aria-label','Scroll to next demo');prev.textContent='←';next.textContent='→';actions.append(prev,next);head.append(left,actions);section.append(head);
  const track=document.createElement('div');track.className='showcase-scroll';track.tabIndex=0;track.setAttribute('aria-label','Interactive demo carousel');
  entries.forEach(item=>{
    const a=document.createElement('a');a.className='showcase-card';a.href=item.href;
    const img=document.createElement('img');img.src=item.image;img.alt=item.alt;img.loading='lazy';img.decoding='async';
    const body=document.createElement('div');body.className='card-body';
    for(const [cls,value,tag] of [['card-type',item.kind,'span'],['heading',item.title,'strong'],['summary',item.summary,'span'],['cta',item.action+' ↗','span']]){
      const node=document.createElement(tag);node.className=cls;node.textContent=value;body.append(node);
    }
    a.append(img,body);track.append(a);
  });
  section.append(track);
  const note=document.createElement('p');note.className='showcase-note';note.textContent='Illustrated previews are labelled and linked to the applications. Working demos do not imply production integrations, validated external source facts or payments.';section.append(note);
  anchor.parentNode.insertBefore(section,anchor);
  const scroll=direction=>track.scrollBy({left:direction*((track.firstElementChild?.getBoundingClientRect().width||350)+16),behavior:'smooth'});
  prev.addEventListener('click',()=>scroll(-1));next.addEventListener('click',()=>scroll(1));
})();
