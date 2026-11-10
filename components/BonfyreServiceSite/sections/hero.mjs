// Opening: what is offered, where, and (on a page that asks for something) the
// request form, beside a picture when the record has one.
import { renderForm } from "../form.mjs";
export const needs = ["headline"];
export function render(s, site, h) {
  const picture = s.image ? `<div class="lg:col-span-5 flex flex-col items-center">
          <div class="relative w-full aspect-[4/3] rounded-2xl overflow-hidden shadow-2xl border border-surface-container-high group">
            <img src="${h.src(s.image.src)}" alt="${h.esc(s.image.alt ?? "")}" class="w-full h-full object-cover group-hover:scale-105 transition-transform duration-700" />
            <div class="absolute inset-0 bg-gradient-to-t from-surface-container-lowest/90 via-surface-container-lowest/20 to-transparent"></div>
            ${s.image.pill ? `<div class="absolute top-4 right-4 bg-surface/90 backdrop-blur-md border border-surface-container-highest px-3 py-1.5 rounded-full flex items-center gap-2">
              <span class="w-2 h-2 rounded-full bg-secondary"></span>
              <span class="text-xs font-mono uppercase text-display font-medium">${h.esc(s.image.pill)}</span>
            </div>` : ""}
            ${s.stat ? `<div class="absolute bottom-4 left-4 right-4 bg-surface/95 backdrop-blur-md border border-surface-container-high p-4 rounded-xl flex items-center justify-between">
              <div>
                <span class="text-xs font-mono text-secondary uppercase tracking-wider block font-semibold">${h.esc(s.stat.label)}</span>
                <span class="text-2xl font-mono font-bold text-display">${h.esc(s.stat.value)}</span>
                <span class="text-xs text-on-surface-variant block">${h.esc(s.stat.caption ?? "")}</span>
              </div>
              <div class="w-12 h-12 rounded-xl bg-secondary/10 border border-secondary/30 flex items-center justify-center text-secondary">
                ${h.icon(s.stat.icon ?? "verified_user", "text-2xl")}
              </div>
            </div>` : ""}
          </div>
          ${s.strip ? `<div class="w-full -mt-3 z-10 px-4">
            <div class="bg-surface-container-high border border-surface-container-highest px-3 py-1 rounded-xl shadow-lg flex flex-wrap items-center justify-between gap-x-3 text-xs font-mono">
              <span class="text-on-surface-variant flex items-center gap-1.5 py-2">
                ${h.icon(s.strip.icon ?? "schedule", "text-primary text-base")}
                ${h.esc(s.strip.label)} <strong class="text-display">${h.esc(s.strip.value)}</strong>
              </span>
              ${site.phone_tel ? `<a href="tel:${site.phone_tel}" class="text-secondary hover:underline flex items-center gap-1 min-h-[44px]">${site.phone_display} &rarr;</a>` : ""}
            </div>
          </div>` : ""}
        </div>` : "";
  // A reel: pictures of the work itself, each leading to it. The row is written
  // twice so a look may run it as an unbroken loop; the second copy is hidden
  // from assistive technology and from the keyboard.
  const frame = (item, again) => `<a class="hero-reel-item shrink-0 w-72 sm:w-96" href="${h.esc(h.siteHref(item.href))}"${again ? ' aria-hidden="true" tabindex="-1"' : ""}>
            <img class="w-full aspect-[16/10] object-cover rounded-xl border border-surface-container-high" src="${h.src(item.src)}" alt="${again ? "" : h.esc(item.alt ?? "")}" loading="lazy" decoding="async" />
            ${item.label ? `<span class="block mt-2 text-xs font-mono uppercase tracking-wider text-on-surface-variant">${h.esc(item.label)}</span>` : ""}
          </a>`;
  const reel = s.reel?.length ? `<div class="hero-reel mt-10 overflow-x-auto">
        <div class="hero-reel-track flex gap-4">
          ${s.reel.map(item => frame(item, false)).join("\n          ")}
          ${s.reel.map(item => frame(item, true)).join("\n          ")}
        </div>
      </div>` : "";
  return `<section class="relative overflow-hidden pt-8 sm:pt-12 pb-16 sm:pb-20 border-b border-surface-container-high bg-gradient-to-b from-surface to-surface-container-lowest">
    <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
      ${s.overline ? `<div class="inline-flex flex-wrap items-center gap-x-2 gap-y-0.5 bg-surface-container px-3.5 py-1.5 rounded-2xl sm:rounded-full border border-surface-container-highest mb-6 shadow-sm">
        <span class="w-2 h-2 rounded-full bg-secondary animate-pulse"></span>
        <span class="text-xs font-mono uppercase tracking-wider text-secondary font-semibold">${h.esc(site.business.name)}</span>
        <span class="text-xs text-outline">•</span>
        <span class="text-xs text-on-surface-variant font-medium">${h.esc(s.overline)}</span>
      </div>` : ""}
      <div class="grid grid-cols-1 lg:grid-cols-12 gap-12 items-center">
        <div class="${s.image ? "lg:col-span-7" : "lg:col-span-12 max-w-3xl"} flex flex-col">
          <h1 class="font-headline font-extrabold ${s.editorial ? "text-4xl sm:text-5xl lg:text-6xl" : "text-3xl sm:text-4xl lg:text-5xl"} text-display tracking-tight leading-[1.15]">${h.marked(s.headline)}</h1>
          ${s.lede ? `<p class="text-base sm:text-lg text-on-surface-variant mt-4 max-w-xl leading-relaxed">${h.esc(s.lede)}</p>` : ""}
          ${s.form ? `<div class="mt-8">${renderForm(s.form, site, h)}</div>` : s.button ? `<div class="mt-8 flex flex-col sm:flex-row sm:flex-wrap gap-4">
            <a href="${h.esc(h.localHref(s.href ?? site.ask))}" class="text-center bg-secondary hover:bg-secondary-dark text-surface font-headline font-bold text-sm uppercase tracking-wider px-8 py-4 rounded-lg shadow-xl shadow-secondary/20 transition-all">${h.esc(s.button)}</a>
            ${s.secondary ? `<a href="${h.esc(h.localHref(s.secondary.href))}" class="min-h-[48px] flex items-center justify-center rounded-lg border border-surface-container-highest px-6 py-3 font-semibold text-display">${h.esc(s.secondary.label)} &rarr;</a>` : ""}
            ${site.phone_tel ? `<a href="tel:${site.phone_tel}" class="bg-surface-container-high hover:bg-surface-bright border border-surface-container-highest text-display font-mono text-sm font-semibold px-8 py-4 rounded-lg transition-colors flex items-center justify-center gap-2">${h.icon("call", "text-secondary")}<span>Call ${site.phone_display}</span></a>` : ""}
          </div>` : ""}
        </div>
        ${picture}
      </div>
      ${reel}
    </div>
  </section>`;
}
