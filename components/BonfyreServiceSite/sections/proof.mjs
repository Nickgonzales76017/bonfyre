// A short strip of plain facts about how the business works.
export const needs = ["items"];
export function render(s, site, h) {
  return `<section class="bg-surface border-b border-surface-container-high py-8">
    <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
      <div class="grid grid-cols-2 md:grid-cols-4 gap-6 text-center divide-y md:divide-y-0 md:divide-x divide-surface-container-high">
${s.items.map(item => `        <div class="flex flex-col items-center pt-3 md:pt-0">
          ${h.icon(item.icon, "text-secondary text-2xl mb-1")}
          <span class="text-sm font-bold text-display">${h.esc(item.title)}</span>
          <span class="text-xs text-on-surface-variant font-mono">${h.esc(item.sub ?? "")}</span>
        </div>`).join("\n")}
      </div>
    </div>
  </section>`;
}
