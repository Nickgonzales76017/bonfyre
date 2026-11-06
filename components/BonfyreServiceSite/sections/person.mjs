// Who you will actually be dealing with, and how they work.
export const needs = ["text"];
export function render(s, site, h) {
  const c = site.contact;
  const portrait = c.portrait
    ? `<img src="${h.src(c.portrait)}" alt="${h.esc(c.name)}" class="w-full h-full object-cover" />`
    : s.brand_mark && site.business.logo ? `<div class="w-full h-full flex items-center p-4"><img src="${h.src(site.business.logo)}" alt="${h.esc(site.business.name)}" class="w-full h-auto" /></div>` : `<div class="w-full h-full flex items-center justify-center text-5xl font-headline font-extrabold text-display" aria-hidden="true">${h.esc(site.initials)}</div>`;
  return `<section id="${h.esc(s.id)}" class="scroll-mt-20 py-16 sm:py-20 border-b border-surface-container-high bg-surface">
    <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
      <div class="bg-surface-container border border-surface-container-highest rounded-3xl p-8 sm:p-12 shadow-2xl grid grid-cols-1 lg:grid-cols-12 gap-10 items-center">
        <div class="lg:col-span-4 flex flex-col items-center">
          <div class="relative w-48 h-48 sm:w-60 sm:h-60 rounded-2xl overflow-hidden border-2 border-surface-container-highest shadow-2xl bg-surface-container-high">
            ${portrait}
          </div>
          <div class="mt-4 text-center">
            <h2 class="font-headline font-bold text-lg text-display">${h.esc(c.name)}</h2>
            <span class="text-xs font-mono text-secondary uppercase tracking-wider block">${h.esc(c.role ?? "")}</span>
            <span class="text-xs text-on-surface-variant">${h.esc(c.line ?? "")}</span>
          </div>
        </div>
        <div class="lg:col-span-8 flex flex-col">
          <div class="inline-flex items-center gap-2 text-secondary text-xs font-mono uppercase tracking-wider mb-3">
            ${h.icon("handshake", "text-sm")}
            <span>${h.esc(s.overline ?? "How we work")}</span>
          </div>
          ${s.title ? `<h2 class="font-headline font-bold text-3xl text-display mb-4">${h.esc(s.title)}</h2>` : ""}
          <p class="text-base sm:text-lg text-display font-normal leading-relaxed">${h.esc(s.text)}</p>
          <div class="flex flex-wrap items-center gap-6 mt-8 pt-6 border-t border-surface-container-high">
            ${site.phone_tel ? `<a href="tel:${site.phone_tel}" class="inline-flex items-center gap-2 min-h-[44px] bg-surface-container-high hover:bg-surface-bright px-4 py-2.5 rounded-lg text-display transition-colors border border-surface-container-highest">
              ${h.icon("call", "text-secondary text-lg")}
              <span class="font-mono text-sm font-bold">${h.esc(s.line_label ?? "Direct Line:")} ${site.phone_display}</span>
            </a>` : ""}
            ${c.location_line ? `<div class="flex items-center gap-2 text-xs font-mono text-on-surface-variant">
              ${h.icon("location_on", "text-primary text-base")}
              <span>${h.esc(c.location_line)}</span>
            </div>` : ""}
          </div>
        </div>
      </div>
    </div>
  </section>`;
}
