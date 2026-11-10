// What to gather, for however many the visitor is making. Every list is worked
// out before the page is built, by whatever owns the arithmetic, and the page
// only switches between them; nothing is calculated in the browser. With no
// lists on file the section is absent.
export const needs = ["title"];
export const present = s => (s.choices ?? []).some(c => (c.rows ?? []).length);
export function render(s, site, h) {
  const choices = s.choices.filter(c => (c.rows ?? []).length);
  return `<section id="${h.esc(s.id)}" class="scroll-mt-20 py-16 sm:py-20 border-b border-surface-container-high bg-surface">
    <div class="max-w-3xl mx-auto px-4 sm:px-6 lg:px-8" data-amounts>
      ${h.heading(s, "mb-8 sm:mb-10")}
      <fieldset class="mb-6">
        <legend class="block text-xs font-mono text-on-surface-variant mb-2 uppercase tracking-wider">${h.esc(s.choose ?? "How many")}</legend>
        <div class="flex flex-wrap gap-2">
          ${choices.map((c, i) => `<label class="cursor-pointer min-h-[44px] min-w-[44px] px-4 flex items-center justify-center rounded-lg border border-surface-container-high bg-surface-container-lowest text-sm font-semibold text-display has-[:checked]:border-secondary has-[:checked]:bg-secondary/10 has-[:focus-visible]:ring-1 has-[:focus-visible]:ring-secondary transition-colors">
            <input type="radio" name="amounts-${h.esc(s.id)}" value="${i}"${i ? "" : " checked"} class="sr-only" />${h.esc(c.label)}</label>`).join("\n          ")}
        </div>
      </fieldset>
      ${choices.map((c, i) => `<div data-amounts-list="${i}"${i ? ' class="hidden"' : ""}>
        ${c.note ? `<p class="text-sm text-on-surface-variant mb-3">${h.esc(c.note)}</p>` : ""}
        <table class="w-full text-left bg-surface-container border border-surface-container-highest rounded-2xl overflow-hidden border-separate border-spacing-0">
          <caption class="sr-only">${h.esc(s.title)}: ${h.esc(c.label)}</caption>
          <thead class="sr-only"><tr><th scope="col">${h.esc(s.what ?? "Ingredient")}</th><th scope="col">${h.esc(s.how_much ?? "Amount")}</th></tr></thead>
          <tbody class="text-sm sm:text-base">
            ${c.rows.map(([what, amount], n) => `<tr><th scope="row" class="p-4 font-semibold text-display${n ? " border-t border-surface-container-highest/40" : ""}">${h.esc(what)}</th><td class="p-4 text-right font-mono text-secondary${n ? " border-t border-surface-container-highest/40" : ""}">${h.esc(amount)}</td></tr>`).join("\n            ")}
          </tbody>
        </table>
      </div>`).join("\n      ")}
      ${s.note ? `<p class="mt-5 text-xs text-on-surface-variant leading-relaxed">${h.esc(s.note)}</p>` : ""}
    </div>
    <script>
    document.querySelectorAll("[data-amounts]").forEach(function (box) {
      box.addEventListener("change", function (event) { box.querySelectorAll("[data-amounts-list]").forEach(function (list) { list.classList.toggle("hidden", list.dataset.amountsList !== event.target.value); }); });
    });
    </script>
  </section>`;
}
