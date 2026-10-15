/* Exact published experience destinations. Internal docs are not playable demos. */
(function () {
  const anchor=document.querySelector('#experiences .items');
  if (!anchor) return;
  const entries=[
    {title:'Acquisition Brief Studio',kind:'Buyer intelligence · interactive',href:'https://nickgonzales76017.github.io/pages-procurement-memory/brief-studio/',image:'assets/shots/brief-studio-preview.svg',alt:'Illustrated visual study, not a screenshot: acquisition questions and costed buyer brief',summary:'Compose ITAD, property, civic and supplier reviews, compare proof costs and export a proposal.',action:'Open the studio'},
    {title:'Dallas Council Media',kind:'Civic media · interactive',href:'https://nickgonzales76017.github.io/pages-town-box/civic-council/',image:'assets/shots/council-media-preview.svg',alt:'Illustrated visual study, not a screenshot: exact agenda record joined to video chapter markers',summary:'Explore exact agenda-to-video joins and carry an official file into Brief Studio.',action:'View council records'},
    {title:'Retail Fixture Planner',kind:'Design and costing · interactive',href:'https://aurekai-work-samples.pages.dev/red3-planner/',image:'https://aurekai-work-samples.pages.dev/assets/shots/planner.jpg',alt:'Actual retail planner screenshot with movable fixtures and running costs',summary:'Arrange sourced RED3 fixtures, compare fit and prepare a reviewable supplier brief.',action:'Try retail planning'},
    {title:'Catalogue Workbench',kind:'Product operations · interactive',href:'https://aurekai-work-samples.pages.dev/catalogue/',image:'https://aurekai-work-samples.pages.dev/assets/shots/catalogue.jpg',alt:'Actual product editor screenshot with variant details',summary:'Inspect a source catalogue, edit variants and compare drafts without publishing.',action:'Try product editing'},
    {title:'Multitrack Creator Studio',kind:'Native audio · production workstation',href:'https://nickgonzales76017.github.io/pages-podcast-plant/studio/',image:'assets/shots/creator-studio-preview.svg',alt:'Faithful illustrated arrangement view of Creator Studio with four edited voice, music, room and field tracks; not a screenshot',summary:'Arrange imported audio with source trims, fades, pan, markers, native C/WASM rendering, WAV exports and podcast handoff.',action:'Open multitrack studio'},
    {title:'Creator Audio Mixer',kind:'Native audio · interactive',href:'https://aurekai-work-samples.pages.dev/audio/',image:'https://aurekai-work-samples.pages.dev/assets/shots/audio.png',alt:'Actual Aurekai audio mixer showing track gain controls',summary:'Mix three tracks, play and stop audio, save levels and export a WAV.',action:'Try the audio mixer'}
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
  const note=document.createElement('p');note.className='showcase-note';note.textContent='Creator Studio, Brief Studio and Council illustrations represent their real interfaces; images are labelled visual studies, not screenshots. All destinations link to released applications.';section.append(note);
  anchor.parentNode.insertBefore(section,anchor);
  const scroll=direction=>track.scrollBy({left:direction*((track.firstElementChild?.getBoundingClientRect().width||350)+16),behavior:'smooth'});
  prev.addEventListener('click',()=>scroll(-1));next.addEventListener('click',()=>scroll(1));
})();
