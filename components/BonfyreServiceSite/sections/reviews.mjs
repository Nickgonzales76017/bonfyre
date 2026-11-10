// What clients said. A review shows only when it is on file with who said it;
// with none on file the section, and its menu link, are absent.
export const needs = [];
export const present = s => (s.items ?? []).some(r => r.quote && r.name);
export function render(s, site, h) {
  const reviews = (s.items ?? []).filter(r => r.quote && r.name);
  return `<section id="${h.esc(s.id)}" class="scroll-mt-20 py-16 sm:py-20 border-b border-surface-container-high bg-surface-container-lowest">
    <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
      <div class="text-center max-w-3xl mx-auto mb-12"><h2 class="font-headline text-3xl sm:text-4xl font-extrabold text-display">${h.esc(s.title ?? "What clients say")}</h2></div>
      <div class="grid grid-cols-1 ${reviews.length >= 3 ? "md:grid-cols-3" : reviews.length === 2 ? "md:grid-cols-2 max-w-4xl mx-auto" : "max-w-xl mx-auto"} gap-6">${reviews.map(r => `
        <figure class="bg-surface-container border border-surface-container-high rounded-2xl p-6">
          <blockquote class="text-on-surface text-sm leading-relaxed">“${h.esc(r.quote)}”</blockquote>
          <figcaption class="mt-4"><span class="block text-display font-bold text-sm">${h.esc(r.name)}</span>
          <span class="text-xs text-on-surface-variant">${h.esc(r.where ?? "")}</span></figcaption>
        </figure>`).join("")}
      </div>
    </div>
  </section>`;
}
