// Source-to-HTML composition; no filesystem publishing or deployed effects.
import { resolveTheme } from "./theme.mjs";
import { localHref, siteHref, checkLinks } from "./navigation.mjs";
import { formFields } from "./form.mjs";
import { catalogueReviewForm } from "./catalogue-review.mjs";
import { planReviewForm } from "./planner-model.mjs";
import * as shell from "./shell.mjs";
import { kinds } from "./section-registry.mjs";
import { pagesOf, validate } from "./site-contract.mjs";

const esc = text => String(text ?? "").replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const tones = { accent: "text-secondary", second: "text-primary", soft: "text-primary-light", warn: "text-amber-400", quiet: "text-outline" };
const h = {
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

export function renderSite(content, { linkPaths = null, sourceRoutes = [] } = {}) {
  const digits = validate(content);
  const contact = content.contact ?? {}, names = (contact.name ?? content.business.name).split(/\s+/), theme = resolveTheme(content.theme), all = pagesOf(content);
  const prepared = all.map(page => ({ ...page, shown: page.sections.map((s, i) => ({ id: `${s.kind}-${i + 1}`, ...s })).filter(s => kinds[s.kind].present?.(s) ?? true) }))
    // A further page with nothing on it but its opening is not published, and nothing links to it.
    .filter(page => !page.path || page.layout === "workspace" || page.shown.some((s, i) => s.form || (i > 0 && s.kind !== "call")));
  const formOf = page => page.shown.find(s => s.form)?.form;
  const pages = prepared.map(page => {
    const site = { ...content, contact, theme, page_path: '/' + (page.path ? page.path + '/' : ''), phone_tel: digits, phone_display: digits ? `(${digits.slice(0, 3)}) ${digits.slice(3, 6)}-${digits.slice(6)}` : "",
      initials: names.map(part => part[0]).join("").slice(0, 2).toUpperCase(), ask: formOf(page) ? "#ask" : "/#ask" };
    const links = content.navigation ?? [...(page.path ? [{ href: "/", label: all[0].menu }] : []),
      ...page.shown.filter(s => s.menu).map(s => ({ href: `#${s.id}`, label: s.menu })),
      ...prepared.filter(other => other.path && other.path !== page.path).map(other => ({ href: `/${other.path}/`, label: other.menu }))];
    // Each section says what kind it is, so a look can draw kinds differently.
    const body = page.shown.map(s => `  <!-- ${s.kind} -->\n  ${kinds[s.kind].render(s, site, h).replace("<section", `<section data-kind="${s.kind}"`)}`).join("\n\n");
    const workspace = page.layout === "workspace";
    const parts = workspace ? [shell.head({ ...site, ask: "#" + page.shown[0].id }, h, page).replace("Skip to the request form", "Skip to workspace"), "<main>", body, "</main></body></html>"] : [shell.head(site, h, page), shell.menu(site, h, links), body, shell.footer(site, h), shell.tail(site, h)];
    return { path: page.path, kinds: page.shown.map(s => s.kind), site, html: parts.join("\n") };
  });
  // A destination-owned package verifies the routes it actually packages.
  // The original site's homepage is not published by that package, and its
  // links to other public-site pages must not drag unrelated material in.
  if (linkPaths === null) checkLinks(pages);
  else {
    const allowed = new Set(linkPaths);
    checkLinks(pages.filter(page => allowed.has(page.path)), { sourceRoutes });
  }
  // What each form may be sent, for the server side: its labels, and the stated options of a choice.
  const forms = Object.fromEntries(prepared.filter(formOf).map(p => { const form = formOf(p), fields = formFields(form);
    return [form.id, { page: "/"+(p.path?p.path+"/":""), fields: Object.fromEntries(fields.map(f => [f.label, ["choice","select"].includes(f.type) ? f.options.map(o => typeof o === "string" ? o : o.label) : true])), required: fields.filter(f => !f.optional).map(f => f.label) }]; }));
  for (const page of prepared) for (const section of page.shown) if (section.kind === 'catalogue' && section.review) forms['catalogue-' + section.id] = catalogueReviewForm(section, '/' + (page.path ? page.path + '/' : ''));
  for (const page of prepared) for (const section of page.shown) if (section.kind === 'planner') forms['planner-'+section.id]=planReviewForm(section.catalogue,'/'+page.path+'/');
  return { pages, theme, forms };
}
export const renderPage = content => renderSite(content).pages[0];
