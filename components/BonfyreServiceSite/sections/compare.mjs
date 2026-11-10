// This business beside the usual alternative, row by row. On a phone each row
// stands on its own with both answers named, so nothing is off the screen.
export const needs = ["title", "rows", "other"];
export function render(s, site, h) {
  const ours = s.ours ?? site.business.name;
  return `<section id="${h.esc(s.id)}" class="scroll-mt-20 py-16 sm:py-20 border-b border-surface-container-high bg-surface">
    <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
      ${h.heading(s, "mb-10 sm:mb-14", "text-primary-light")}
      <table class="w-full text-left block md:table bg-transparent md:bg-surface-container md:border border-surface-container-highest md:rounded-2xl md:overflow-hidden md:shadow-xl border-separate border-spacing-0">
        <thead class="sr-only md:not-sr-only md:table-header-group bg-surface-container-high font-headline text-base">
          <tr>
            <th scope="col" class="p-5 w-1/3 font-bold text-on-surface-variant border-b border-surface-container-highest">${h.esc(s.about ?? "")}</th>
            <th scope="col" class="p-5 w-1/3 font-bold text-secondary border-b border-surface-container-highest"><span class="inline-block w-2 h-2 rounded-full bg-secondary mr-2 align-middle"></span>${h.esc(ours)}</th>
            <th scope="col" class="p-5 w-1/3 font-bold text-on-surface-variant border-b border-surface-container-highest">${h.esc(s.other)}</th>
          </tr>
        </thead>
        <tbody class="block md:table-row-group text-sm space-y-3 md:space-y-0">
${s.rows.map((row, i) => `          <tr class="block md:table-row rounded-xl md:rounded-none border md:border-0 border-surface-container-high p-4 md:p-0 ${i % 2 ? "bg-surface-container md:bg-surface-container-low/40" : "bg-surface-container"} md:hover:bg-surface-container-high/30 transition-colors">
            <th scope="row" class="block md:table-cell md:p-5 pb-2 font-headline md:font-body font-bold md:font-semibold text-base md:text-sm text-display md:border-t border-surface-container-highest/40">${h.esc(row.what)}</th>
            <td class="block md:table-cell md:p-5 py-1 md:border-t border-surface-container-highest/40 ${row.plain ? "text-display" : "font-mono font-bold text-secondary"}"><span class="md:hidden block font-body font-normal text-xs text-on-surface-variant">${h.esc(ours)}</span>${h.esc(row.ours)}</td>
            <td class="block md:table-cell md:p-5 py-1 md:border-t border-surface-container-highest/40 text-on-surface-variant"><span class="md:hidden block text-xs">${h.esc(s.other)}</span>${h.esc(row.other)}</td>
          </tr>`).join("\n")}
        </tbody>
      </table>
    </div>
  </section>`;
}
