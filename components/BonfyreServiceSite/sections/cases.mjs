// The situations people arrive in, each answered in a sentence or two.
// An item may carry a picture of the thing it leads to; a section whose
// pictures are "shown" lays each one out beside its words.
export const needs = ["title", "items"];
export function render(s, site, h) {
  return `<section id="${h.esc(s.id)}" class="scroll-mt-20 py-16 sm:py-20 border-b border-surface-container-high bg-surface-container-lowest">
    <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
      ${h.heading(s, "mb-16")}
      <div${s.pictures === "shown" ? ' data-pictures="shown"' : ""} class="grid grid-cols-1 md:grid-cols-2 ${s.items.length % 3 === 0 || s.items.length > 4 ? "lg:grid-cols-3" : "lg:grid-cols-2 max-w-4xl mx-auto"} gap-6">
${s.items.map(item => `        <div class="bg-surface-container border border-surface-container-high p-6 rounded-2xl">
          <div class="w-10 h-10 rounded-lg bg-surface-container-high flex items-center justify-center ${h.tone(item.tone)} mb-4">
            ${h.icon(item.icon ?? "check")}
          </div>
          <h3 class="font-headline font-bold text-lg text-display mb-2">${h.esc(item.title)}</h3>
          <p class="text-on-surface-variant text-sm leading-relaxed">${h.esc(item.text)}</p>
          ${item.href ? `<a class="min-h-[48px] mt-4 inline-flex items-center gap-2 font-semibold text-secondary" href="${h.esc(h.siteHref(item.href))}">${h.esc(item.button ?? "Explore options")} &rarr;</a>` : ""}
          ${item.picture ? `<img class="case-picture mt-4 w-full rounded-xl border border-surface-container-high" src="${h.src(item.picture.src)}" alt="${h.esc(item.picture.alt ?? "")}" loading="lazy" decoding="async" />` : ""}
        </div>`).join("\n")}
      </div>
    </div>
  </section>`;
}
