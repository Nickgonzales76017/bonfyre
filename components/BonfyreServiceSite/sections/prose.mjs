// Shared editorial reading flow for site pages and authored profiles.
// Published answers remain visible rather than hiding inside disclosures.
export const needs=['title','items'];
export function render(s,site,h){
 const content=s.items.map((item,index)=>
  '<article id="'+h.esc(s.id)+'-reading-'+(index+1)+'" class="grid grid-cols-1 sm:grid-cols-[52px_minmax(0,1fr)] gap-3 sm:gap-7 border-t border-surface-container-high py-8 sm:py-10">'+
  '<span class="font-mono text-xs tracking-widest text-secondary pt-2" aria-hidden="true">'+String(index+1).padStart(2,'0')+'</span>'+
  '<div class="min-w-0"><h3 class="font-headline font-semibold text-2xl sm:text-3xl text-display tracking-tight leading-tight">'+h.esc(item.title)+'</h3>'+
  '<p class="mt-4 max-w-[72ch] whitespace-pre-line text-base sm:text-lg leading-[1.85] text-on-surface-variant">'+h.esc(item.text)+'</p></div></article>').join('');
 const links=s.links?.length?'<nav aria-label="'+h.esc(s.title)+'" class="mt-9 flex flex-wrap gap-x-8 gap-y-3 border-t border-surface-container-high pt-7">'+
  s.links.map(l=>'<a class="inline-flex min-h-[44px] items-center font-semibold text-secondary underline underline-offset-4" href="'+h.esc(h.localHref(l.href))+'">'+h.esc(l.label)+' &rarr;</a>').join('')+'</nav>':'';
 return '<section id="'+h.esc(s.id)+'" class="scroll-mt-24 border-b border-surface-container-high bg-surface py-14 sm:py-24" data-editorial-reading="'+(s.questions?'questions':'prose')+'">'+
  '<div class="mx-auto max-w-7xl px-4 sm:px-6 lg:px-8"><div class="grid grid-cols-1 lg:grid-cols-12 gap-x-12 gap-y-10">'+
  '<header class="lg:col-span-4 lg:self-start lg:sticky lg:top-24">'+h.heading(s,'mb-4')+'</header>'+
  '<div class="lg:col-span-8 min-w-0">'+content+links+'</div></div></div></section>';
}
