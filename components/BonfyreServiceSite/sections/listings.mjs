// What is on offer right now, one entry each. An entry shows only if the record
// gives its place and its price; with none on file the section is absent. The
// one derived number is plain subtraction and says so.
export const needs = ["title"];
const whole = items => (items ?? []).filter(i => i.name && i.place && Number.isFinite(i.price));
export const present = s => whole(s.items).length > 0;
export function render(s, site, h) {
  const money = n => "$" + Math.round(n).toLocaleString("en-US");
  return `<section id="${h.esc(s.id)}" class="scroll-mt-20 py-16 sm:py-20 border-b border-surface-container-high bg-surface-container-lowest">
    <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
      ${h.heading(s, "mb-10 sm:mb-14")}
      <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
${whole(s.items).map(item => { const spread = Number.isFinite(item.after_repair) && Number.isFinite(item.repairs) ? item.after_repair - item.price - item.repairs : null; return `        <article class="bg-surface-container border border-surface-container-high rounded-2xl overflow-hidden flex flex-col">
          ${item.image ? `<img src="${h.src(item.image)}" alt="${h.esc(item.name)}" loading="lazy" class="w-full aspect-[16/10] object-cover" />` : ""}
          <div class="p-5 flex flex-col gap-3 grow">
            <div><span class="text-xs font-mono uppercase tracking-wider text-secondary">${h.esc(item.place)}</span>
            <h3 class="font-headline font-bold text-lg text-display">${h.esc(item.name)}</h3></div>
            <dl class="grid grid-cols-3 gap-2 text-xs">
              <div><dt class="text-on-surface-variant">Price</dt><dd class="font-mono font-bold text-display text-sm">${money(item.price)}</dd></div>
              ${Number.isFinite(item.after_repair) ? `<div><dt class="text-on-surface-variant">After repair</dt><dd class="font-mono font-bold text-display text-sm">${money(item.after_repair)}</dd></div>` : ""}
              ${Number.isFinite(item.repairs) ? `<div><dt class="text-on-surface-variant">Repairs, est.</dt><dd class="font-mono font-bold text-display text-sm">${money(item.repairs)}</dd></div>` : ""}
            </dl>
            ${spread !== null ? `<p class="text-xs text-on-surface-variant">After repair less price and repairs: <strong class="font-mono text-secondary">${money(spread)}</strong>. An estimate, not a promise.</p>` : ""}
            ${(item.facts ?? []).length ? `<ul class="text-xs text-on-surface-variant space-y-1">${item.facts.map(f => `<li>• ${h.esc(f)}</li>`).join("")}</ul>` : ""}
            ${item.note ? `<p class="text-sm text-on-surface-variant leading-relaxed">${h.esc(item.note)}</p>` : ""}
            <a href="${site.ask}" class="mt-auto text-center min-h-[44px] flex items-center justify-center bg-secondary hover:bg-secondary-dark text-surface font-headline font-bold text-xs uppercase tracking-wider px-4 rounded-lg transition-colors">${h.esc(s.button ?? "Ask about this one")}</a>
          </div>
        </article>`; }).join("\n")}
      </div>
    </div>
  </section>`;
}
