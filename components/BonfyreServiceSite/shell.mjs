// What every page has around its sections: the head, the menu, the footer,
// the owner's to-confirm list while it is a preview, and the form's behaviour.
import { formBehaviour } from "./form.mjs";
import { metadata } from "./seo.mjs";

export function head(site, h, page) {
  const b = site.business;
  return `<!DOCTYPE html>
<html lang="${h.esc(site.lang ?? "en")}"${site.theme.dark ? ' class="dark"' : ""} data-kit="service-site"${site.theme.look ? ` data-look="${h.esc(site.theme.look)}"` : ""}${site.theme.template ? ` data-template="${h.esc(site.theme.template)}"` : ""}>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <meta name="robots" content="${site.preview ? "noindex, nofollow" : "index, follow"}" />
  <meta name="theme-color" content="${site.theme.colors.surface}" />
  <title>${h.esc(page.path ? `${page.title} | ${b.name}` : b.page_title ?? b.name)}</title>
  ${page.description ?? b.description ? `<meta name="description" content="${h.esc(page.description ?? b.description)}" />` : ""}
  ${metadata(site,page)}
  <link rel="icon" href="${b.logo ? h.src(b.logo) : "data:,"}" />
  <link rel="preconnect" href="https://fonts.googleapis.com" />
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
  <link href="${h.esc(site.theme.font_href)}" rel="stylesheet" />
  <link href="https://fonts.googleapis.com/css2?family=Material+Symbols+Outlined:opsz,wght,FILL,GRAD@20..48,100..700,0..1,-50..200&display=block" rel="stylesheet" />
  <link href="/assets/site.css" rel="stylesheet" />
</head>
<body class="bg-surface-container-lowest text-on-surface font-body antialiased min-h-screen selection:bg-secondary selection:text-surface">
  <a href="${site.ask}" class="sr-only focus:not-sr-only focus:absolute focus:z-[60] focus:bg-secondary focus:text-surface focus:p-3">Skip to the request form</a>`;
}

// links: [{ href, label, here }]. Wide screens show them in a row; a phone gets
// the same links behind one button, and the phone number and the ask stay out.
export function menu(site, h, links) {
  const b = site.business, row = site.navigation ? (links.length>5?"2xl":"xl") : links.length > 5 ? "lg" : "md";
  return `<header class="sticky top-0 z-50 bg-surface/95 backdrop-blur-md border-b border-surface-container-high">
    <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 h-16 sm:h-20 flex items-center justify-between gap-3">
      <a href="/" class="flex items-center gap-3 min-h-[44px] min-w-0" aria-label="${h.esc(b.name)}, home">
        ${b.logo ? `<img src="${h.src(b.logo)}" alt="" class="${b.prominent_logo ? "w-36 sm:w-56 h-auto" : "h-9 w-auto"} object-contain shrink-0" />` : ""}
        <span class="${b.prominent_logo ? "sr-only" : b.logo ? "hidden sm:flex" : "flex"} flex-col min-w-0">
          <span class="font-headline font-bold text-sm tracking-wide text-display uppercase leading-tight line-clamp-2">${h.esc(b.short_name ?? b.name)}</span>
          ${b.tagline ? `<span class="hidden sm:block text-[11px] font-mono text-secondary tracking-widest uppercase leading-none mt-1">${h.esc(b.tagline)}</span>` : ""}
        </span>
      </a>
      <nav aria-label="Site" class="hidden ${row}:flex items-center gap-5 text-sm font-medium text-on-surface-variant">
        ${links.map(l => `<a href="${h.esc(l.href)}"${l.here ? ' aria-current="page"' : ""} class="hover:text-display transition-colors${l.here ? " text-display" : ""}">${h.esc(l.label)}</a>`).join("\n        ")}
      </nav>
      <div class="flex items-center gap-2 sm:gap-4 shrink-0">
        ${site.phone_tel ? `<a href="tel:${site.phone_tel}" aria-label="Call ${site.phone_display}" class="flex items-center justify-center gap-2 min-h-[44px] min-w-[44px] text-sm font-mono text-display hover:text-secondary transition-colors">
          ${h.icon("call", "text-secondary text-xl")}
          <span class="hidden xl:inline">${site.phone_display}</span>
        </a>` : ""}
        <a href="${site.ask}" class="min-h-[44px] hidden min-[360px]:flex items-center text-center bg-secondary hover:bg-secondary-dark text-surface font-headline font-bold text-xs uppercase tracking-wider px-3 sm:px-4 rounded shadow-lg shadow-secondary/20 transition-all">${h.esc(site.action)}</a>
        ${links.length ? `<details class="${row}:hidden relative" data-menu>
          <summary class="list-none [&::-webkit-details-marker]:hidden min-h-[44px] min-w-[44px] flex items-center justify-center rounded border border-surface-container-highest text-display cursor-pointer" aria-label="Menu">${h.icon("menu", "text-2xl")}</summary>
          <nav aria-label="Site, on a small screen" class="absolute right-0 top-full mt-2 w-64 max-w-[calc(100vw-2rem)] bg-surface-container border border-surface-container-highest rounded-xl shadow-2xl p-2 flex flex-col">
            ${links.map(l => `<a href="${h.esc(l.href)}"${l.here ? ' aria-current="page"' : ""} class="min-h-[44px] flex items-center px-3 rounded-lg text-sm font-medium text-on-surface hover:bg-surface-container-high">${h.esc(l.label)}</a>`).join("\n            ")}
          </nav>
        </details>` : ""}
      </div>
    </div>
  </header>
  <main>`;
}

export function footer(site, h) {
  const b = site.business, f = site.footer ?? {}, c = site.contact;
  const column = (title, body) => `<div class="flex flex-col gap-2">
          <span class="font-headline font-bold text-xs uppercase tracking-wider text-display mb-2">${h.esc(title)}</span>
          ${body}
        </div>`;
  const columns = [
    `<div class="flex flex-col gap-3">
          ${b.logo ? `<img src="${h.src(b.logo)}" alt="${h.esc(b.name)}" class="h-8 w-auto object-contain self-start" />` : `<span class="font-headline font-bold text-sm text-display">${h.esc(b.name)}</span>`}
          ${f.about ? `<p class="text-xs text-on-surface-variant leading-relaxed mt-2">${h.esc(f.about)}</p>` : ""}
        </div>`,
    f.areas ? column(f.areas.title, `<ul class="text-xs space-y-1.5 text-on-surface-variant">
            ${f.areas.items.map(i => `<li>• ${h.esc(i)}</li>`).join("\n            ")}
          </ul>`) : "",
c.location_line || site.phone_tel || c.email ?     column(f.contact_title ?? "Get in touch", `<div class="text-sm sm:text-xs">
            ${c.location_line ? `<div class="flex items-center gap-2 py-1.5">${h.icon("location_on", "text-secondary text-base shrink-0")}<span>${h.esc(c.location_line)}</span></div>` : ""}
            ${site.phone_tel ? `<a href="tel:${site.phone_tel}" class="flex items-center gap-2 min-h-[44px] sm:min-h-0 sm:py-1.5 text-display hover:underline font-mono">${h.icon("call", "text-primary text-base shrink-0")}${site.phone_display}</a>` : ""}
            ${c.email ? `<a href="mailto:${h.esc(c.email)}" class="flex items-center gap-2 min-h-[44px] sm:min-h-0 sm:py-1.5 hover:underline break-all">${h.icon("mail", "text-primary text-base shrink-0")}${h.esc(c.email)}</a>` : ""}
          </div>`) : "",
    f.notice ? column(f.notice.title, `<p class="text-xs text-on-surface-variant leading-relaxed">${h.esc(f.notice.text)}</p>`) : "",
  ].filter(Boolean);
  // A footer link is shown only when it leads somewhere.
  const links = (f.links ?? []).filter(l => l.href && l.href !== "#");
  return `</main>
  <footer class="bg-surface-container-lowest text-on-surface-variant py-12 sm:py-14">
    <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
      <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-${columns.length} gap-8 sm:gap-10 pb-10 sm:pb-12 border-b border-surface-container-high">
        ${columns.join("\n        ")}
      </div>
      <div class="pt-8 flex flex-col sm:flex-row items-center justify-between gap-4 text-xs font-mono text-on-surface-variant text-center sm:text-left">
        <span>© ${new Date().getFullYear()} ${h.esc(f.rights ?? b.name)}</span>
        ${links.length ? `<div class="flex flex-wrap items-center justify-center gap-x-4 gap-y-1">${links.map(l => `<a href="${h.esc(l.href)}" class="min-h-[44px] flex items-center hover:text-display transition-colors">${h.esc(l.label)}</a>`).join("")}</div>` : ""}
      </div>
    </div>
  </footer>`;
}

export function tail(site, h) {
  const note = site.preview ? `<aside class="bg-surface-container-low border-t border-outline text-on-surface text-sm py-5 px-4">
  <div class="max-w-4xl mx-auto"><strong class="text-display">Design preview for ${h.esc(site.business.name)}. Not yet public.</strong>
  <ul class="mt-2 pl-5 list-disc space-y-1">${(site.to_confirm ?? []).map(item => `<li>${h.esc(item)}</li>`).join("")}</ul></div></aside>` : "";
  return `${note}
<script>
(${formBehaviour.toString()})(${JSON.stringify(site.contact.email ?? null)}, ${site.leads === "server"});
// The small-screen menu closes when a link in it is followed or the page is tapped elsewhere.
document.querySelectorAll("[data-menu]").forEach(function (menu) {
  document.addEventListener("click", function (event) { if (menu.open && (!menu.contains(event.target) || event.target.closest("nav a"))) menu.open = false; });
  document.addEventListener("keydown", function (event) { if (event.key === "Escape") menu.open = false; });
});
</script>
</body>
</html>`;
}
