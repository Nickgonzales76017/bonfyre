// Source-owned situations compose as editorial features in both sites and profiles.
// All words, links and rights-checked imagery remain with the existing owner.
export const needs = ["title", "items"];
export function render(s, site, h) {
  const caseItem=(entry,index)=>{
    const wide=index===0,hasImage=Boolean(entry.picture);
    const picture=hasImage?'<figure class="'+(wide?'lg:col-span-7 ':'')+'overflow-hidden rounded-xl">'+
      '<img class="case-picture w-full aspect-[16/10] object-cover" src="'+h.src(entry.picture.src)+'" alt="'+h.esc(entry.picture.alt??'')+'" loading="lazy" decoding="async"></figure>':'';
    const number='<span class="font-mono text-xs tracking-widest text-secondary" aria-hidden="true">'+String(index+1).padStart(2,'0')+'</span>';
    const icon=entry.icon?'<span aria-hidden="true">'+h.icon(entry.icon,h.tone(entry.tone))+'</span>':'';
    const link=entry.href?'<a class="mt-6 inline-flex min-h-[44px] items-center gap-2 font-semibold text-secondary underline underline-offset-4" href="'+h.esc(h.siteHref(entry.href))+'">'+h.esc(entry.button??'Explore options')+' &rarr;</a>':'';
    const article='<div class="'+(wide&&hasImage?'lg:col-span-5 ':'')+(hasImage?'pt-5 lg:pt-0':'')+'">'+
      '<p class="mb-5 flex items-center gap-3">'+number+icon+'</p>'+
      '<h3 class="font-headline '+(wide?'text-3xl sm:text-5xl':'text-2xl sm:text-3xl')+' font-semibold leading-tight tracking-tight text-display">'+h.esc(entry.title)+'</h3>'+
      '<p class="mt-4 max-w-[68ch] text-base sm:text-lg leading-[1.8] text-on-surface-variant">'+h.esc(entry.text)+'</p>'+link+'</div>';
    return '<article class="'+(wide?'lg:col-span-12':'lg:col-span-6')+' min-w-0 border-t border-surface-container-high pt-7 sm:pt-9">'+
      '<div class="'+(wide&&hasImage?'grid grid-cols-1 lg:grid-cols-12 gap-8 lg:gap-14 items-center':'')+'">'+picture+article+'</div></article>';
  };
  return '<section id="'+h.esc(s.id)+'" class="scroll-mt-20 py-16 sm:py-24 border-b border-surface-container-high bg-surface-container-lowest" data-editorial-cases>'+
    '<div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8"><div class="max-w-4xl">'+h.heading(s,'mb-10 sm:mb-14')+'</div>'+
    '<div'+(s.pictures==='shown'?' data-pictures="shown"':'')+' class="grid grid-cols-1 lg:grid-cols-12 gap-x-10 gap-y-5">'+
    s.items.map(caseItem).join('\n')+'</div></div></section>';
}
