// What happens, in order, after somebody gets in touch.
export const needs = ["title", "items"];
export function render(s, site, h) {
  return `<section id="${h.esc(s.id)}" class="scroll-mt-20 py-16 sm:py-20 border-b border-surface-container-high bg-surface-container-lowest">
    <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
      ${h.heading(s, "mb-16")}
      <div class="grid grid-cols-1 md:grid-cols-3 gap-8">
${s.items.map((step, i) => `        <div class="bg-surface-container border border-surface-container-high p-8 rounded-2xl relative flex flex-col group hover:border-secondary/40 transition-colors">
          <span class="text-5xl font-mono font-bold text-surface-container-highest/60 mb-4 select-none">${String(i + 1).padStart(2, "0")}</span>
          <h3 class="font-headline font-bold text-xl text-display mb-2">${h.esc(step.title)}</h3>
          <p class="text-on-surface-variant text-sm leading-relaxed mb-6">${h.esc(step.text)}</p>
          ${step.tag ? `<div class="mt-auto flex items-center gap-2 text-xs font-mono ${h.tone(step.tone)}">
            ${h.icon(step.icon ?? "task_alt", "text-base")}
            <span>${h.esc(step.tag)}</span>
          </div>` : ""}
        </div>`).join("\n")}
      </div>
    </div>
  </section>`;
}
