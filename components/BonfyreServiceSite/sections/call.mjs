// The closing ask: the form again, or the phone.
export const needs = ["title"];
export function render(s, site, h) {
  return `<section class="py-16 bg-gradient-to-r from-surface-container-low via-surface to-surface-container-low border-b border-surface-container-high">
    <div class="max-w-5xl mx-auto px-4 sm:px-6 lg:px-8 text-center flex flex-col items-center">
      <h2 class="font-headline font-extrabold text-3xl sm:text-4xl text-display">${h.esc(s.title)}</h2>
      ${s.text ? `<p class="text-on-surface-variant text-base mt-3 max-w-xl">${h.esc(s.text)}</p>` : ""}
      <div class="flex flex-col sm:flex-row items-center gap-4 mt-8">
        <a href="${h.esc(h.localHref(s.href ?? site.ask))}" class="w-full sm:w-auto bg-secondary hover:bg-secondary-dark text-surface font-headline font-bold text-sm uppercase tracking-wider px-8 py-4 rounded-lg shadow-xl shadow-secondary/20 transition-all hover:scale-105 active:scale-95">${h.esc(s.button ?? site.action)}</a>
        ${site.phone_tel ? `<a href="tel:${site.phone_tel}" class="w-full sm:w-auto bg-surface-container-high hover:bg-surface-bright border border-surface-container-highest text-display font-mono text-sm font-semibold px-8 py-4 rounded-lg transition-colors flex items-center justify-center gap-2">
          ${h.icon("call", "text-secondary")}
          <span>Call ${site.phone_display}</span>
        </a>` : ""}
      </div>
    </div>
  </section>`;
}
