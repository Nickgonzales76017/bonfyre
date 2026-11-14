// Shared escaping and presentation helpers used by live site builds and
// profile preview/exports. Section owners remain the only renderers.
import {localHref,siteHref} from "./navigation.mjs";
export const esc = text => String(text ?? "").replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const tones = { accent: "text-secondary", second: "text-primary", soft: "text-primary-light", warn: "text-amber-400", quiet: "text-outline" };
export const sectionHelpers = {
  esc, localHref, siteHref,
  // A picture in the site's folder is addressed from the site's root, so it resolves on every page.
  src: path => esc(/^(https?:)?\/\//.test(path) || path.startsWith("/") ? path : "/" + path),
  icon: (name, cls = "") => `<span class="material-symbols-outlined ${cls}" aria-hidden="true">${esc(name)}</span>`,
  tone: name => tones[name ?? "accent"] ?? tones.accent,
  // *words* in a headline are the emphasised ones.
  marked: text => esc(text).replace(/\*([^*]+)\*/g, '<span class="text-secondary">$1</span>'),
  heading: (s, gap, tone = "text-secondary") => `<div class="text-center max-w-2xl mx-auto ${gap}">
        ${s.overline ? `<span class="text-xs font-mono uppercase tracking-widest ${tone} font-semibold">${esc(s.overline)}</span>` : ""}
        <h2 class="font-headline font-bold text-2xl sm:text-4xl text-display mt-2">${esc(s.title)}</h2>
        ${s.lede ? `<p class="text-on-surface-variant text-sm sm:text-base mt-3">${esc(s.lede)}</p>` : ""}
      </div>`,
};
