// A way through to a room that lives elsewhere and does the work itself: a
// search box that asks that room, and plain links into it. The page holds no
// copy of what the room knows.
export const needs = ["title", "action"];
export function render(s, site, h) {
  if (!/^https:\/\/[a-z0-9.-]+\//.test(s.action)) throw new Error(`a door's action must be an https address (${s.action})`);
  return `<section id="${h.esc(s.id)}" class="scroll-mt-20 py-12 sm:py-16 border-b border-surface-container-high bg-surface-container-lowest">
    <div class="max-w-3xl mx-auto px-4 sm:px-6 lg:px-8">
      ${h.heading(s, "mb-8")}
      <form role="search" action="${h.esc(s.action)}" method="get" class="flex flex-col sm:flex-row gap-3">
        <label for="door-${h.esc(s.id)}" class="sr-only">${h.esc(s.placeholder ?? s.title)}</label>
        <input id="door-${h.esc(s.id)}" name="${h.esc(s.field ?? "q")}" type="search" required enterkeyhint="search" placeholder="${h.esc(s.placeholder ?? "")}" class="grow bg-surface-container border border-surface-container-high focus:border-secondary focus:ring-1 focus:ring-secondary text-display placeholder:text-outline text-base rounded-xl px-4 py-3.5 outline-none" />
        <button type="submit" class="min-h-[48px] px-6 bg-secondary hover:bg-secondary-dark text-surface font-headline font-bold text-sm uppercase tracking-wider rounded-xl transition-colors">${h.esc(s.button ?? "Search")}</button>
      </form>
      ${(s.links ?? []).length ? `<ul class="mt-6 divide-y divide-surface-container-high border-y border-surface-container-high">
        ${s.links.map(l => `<li><a href="${h.esc(l.href)}" class="flex items-center justify-between gap-3 min-h-[52px] py-2 text-display hover:text-secondary"><span><span class="block font-semibold">${h.esc(l.label)}</span>${l.text ? `<span class="block text-sm text-on-surface-variant">${h.esc(l.text)}</span>` : ""}</span>${h.icon("arrow_forward", "text-secondary")}</a></li>`).join("\n        ")}
      </ul>` : ""}
      ${s.note ? `<p class="mt-5 text-xs text-on-surface-variant leading-relaxed">${h.esc(s.note)}</p>` : ""}
    </div>
  </section>`;
}
