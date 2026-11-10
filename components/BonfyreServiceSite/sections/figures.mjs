// Numbers with their source. A table is shown only with where it came from and
// when it was read; with no rows on file the section is absent. The rows can be
// stated in the record or filled at build by material the site folder provides.
export const needs = ["title"];
export const present = s => (s.rows ?? []).length > 0 && Boolean(s.source?.name && s.source?.observed);
export function render(s, site, h) {
  // A two-column table whose second column is all counts is a tally; a look may set the counts large.
  const tally = s.columns.length === 2 && s.rows.every(row => /^\d[\d,.]*$/.test(String(row[1]).trim())) ? " data-tally" : "";
  return `<section id="${h.esc(s.id)}" class="scroll-mt-20 py-16 sm:py-20 border-b border-surface-container-high bg-surface">
    <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
      ${h.heading(s, "mb-10 sm:mb-14", "text-primary-light")}
      <table${tally} class="w-full text-left block md:table md:bg-surface-container md:border border-surface-container-highest md:rounded-2xl md:overflow-hidden md:shadow-xl border-separate border-spacing-0">
        <thead class="sr-only md:not-sr-only md:table-header-group bg-surface-container-high font-headline text-sm">
          <tr>${s.columns.map((c, i) => `<th scope="col" class="p-5 font-bold text-on-surface-variant border-b border-surface-container-highest${i ? " text-right" : ""}">${h.esc(c)}</th>`).join("")}</tr>
        </thead>
        <tbody class="block md:table-row-group text-sm space-y-3 md:space-y-0">
${s.rows.map(row => `          <tr class="block md:table-row rounded-xl md:rounded-none border md:border-0 border-surface-container-high bg-surface-container p-4 md:p-0">
            <th scope="row" class="block md:table-cell md:p-5 pb-2 font-headline font-bold text-base md:text-sm text-display md:border-t border-surface-container-highest/40">${h.esc(row[0])}</th>
            ${row.slice(1).map((cell, i) => `<td class="flex md:table-cell justify-between gap-4 md:p-5 py-1 md:text-right font-mono text-display md:border-t border-surface-container-highest/40"><span class="md:hidden font-body text-xs text-on-surface-variant">${h.esc(s.columns[i + 1])}</span><span>${h.esc(cell)}</span></td>`).join("\n            ")}
          </tr>`).join("\n")}
        </tbody>
      </table>
      <p class="mt-5 text-xs text-on-surface-variant leading-relaxed max-w-3xl">Source: ${s.source.url ? `<a href="${h.esc(s.source.url)}" rel="noopener" class="underline hover:text-display">${h.esc(s.source.name)}</a>` : h.esc(s.source.name)}, read ${h.esc(s.source.observed)}. ${h.esc(s.caveat ?? "")}</p>
    </div>
  </section>`;
}
