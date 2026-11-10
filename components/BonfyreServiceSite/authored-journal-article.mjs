/* Journal post to composed styled article. Inputs are parsed journal
 * sources (aurekai.journal-intake.v1); section bodies render through the
 * native bounded Markdown renderer, never a second JS dialect. The shell
 * (hero, breadcrumb, nav, meta, stylesheet refs) is emitted here so the
 * structure stays reviewable beside the reference it must match.
 */
import { spawnSync } from "node:child_process";
const fail = x => { throw Error("journal article composition: " + x); };
const esc = x => String(x ?? "").replace(/[&<>"']/g, c => ({
  "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const readMins = sections => {
  const words = sections.map(s => `${s.heading} ${s.body}`.split(/\s+/).length)
    .reduce((a, b) => a + b, 0);
  return Math.max(1, Math.round(words / 200));
};
const slugify = text => String(text ?? "").toLowerCase().normalize("NFKD")
  .replace(/[\u0300-\u036f]/g, "").replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "").slice(0, 80);
function renderSections(title, sections, renderMd) {
  const toc = [];
  const html = sections.map((section, i) => {
    if (!section.heading || typeof section.body !== "string") fail("section needs heading and body");
    const rendered = renderMd(section.body);
    if (i === 0 && section.heading === title) return rendered;
    const id = slugify(section.heading);
    if (!id) fail("section heading produces no anchor");
    toc.push({ id, text: section.heading });
    return `<h2 id="${id}">${esc(section.heading)}</h2>\n${rendered}`;
  }).join("\n");
  return { html, toc };
}
export function composeJournalArticle(journal, {
  baseUrl, siteName = "Aurekai", renderMd, navigation = [], meta = {}, related = [],
} = {}) {
  if (journal?.schema !== "aurekai.journal-intake.v1" ||
    !journal.title || !journal.slug || !Array.isArray(journal.sections) || !journal.sections.length)
    fail("parsed journal source required");
  if (typeof baseUrl !== "string" || !/^https:\/\/[a-z0-9.-]+$/.test(baseUrl)) fail("https base URL required");
  if (typeof renderMd !== "function") fail("native markdown render function required");
  if (!Array.isArray(navigation) || navigation.length > 25 ||
    navigation.some(l => typeof l?.label !== "string" || typeof l?.href !== "string")) fail("invalid navigation");
  const category = typeof meta.category === "string" && meta.category.length <= 60 ? meta.category : "";
  const route = `/blog/${journal.slug}/`;
  const canonical = baseUrl + route;
  const nav = navigation.map(l =>
    `<a href="${esc(l.href)}">${esc(l.label)}</a>`).join("");
  if (!Array.isArray(related) || related.length > 8 ||
    related.some(r => typeof r?.slug !== "string" || typeof r?.title !== "string")) fail("invalid related list");
  const { html: body, toc } = renderSections(journal.title, journal.sections, renderMd);
  const mins = readMins(journal.sections);
  const rail = `<aside class="journal-article-side"><div class="journal-side-sticky">` +
    `<span class="eyebrow">In this article</span>` +
    toc.map(t => `<a href="#${t.id}">${esc(t.text)}</a>`).join("") +
    `<hr><p>Explore the <a href="../../docs.html">Bonfÿre architecture</a> and ` +
    `<a href="../../explore.html">Aurekai products</a>.</p></div></aside>`;
  const keepReading = related.length ?
    `<section class="block alt"><div class="shell"><div class="head"><div>` +
    `<span class="eyebrow">Keep reading</span><h2>From the journal</h2></div>` +
    `<p>Other ideas we've been working through.</p></div>` +
    `<div class="journal-footer-links">` + related.map(r =>
      `<a class="journal-related" href="../${esc(r.slug)}/">${esc(r.title)} ↗</a>`).join("") +
    `<a href="../">All articles ↗</a></div></div></section>` : "";
  const footer = `<footer class="footer"><div class="shell"><span>${esc(siteName)} / Bonfÿre · Journal</span>` +
    `<span><a href="../">Journal</a> · <a href="../../market.html">Research</a> · ` +
    `<a href="../../docs.html">Technology</a></span></div></footer>`;
  const icon = /^[a-z0-9-]+$/.test(meta.icon || "") ? meta.icon : null;
  const html = `<!doctype html><html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="theme-color" content="#f5f3eb">
<title>${esc(journal.title)} — ${esc(siteName)} Journal</title><meta name="description" content="${esc(journal.description)}">
<meta property="og:title" content="${esc(journal.title)}"><meta property="og:description" content="${esc(journal.description)}"><meta property="og:type" content="article">
<link rel="canonical" href="${esc(canonical)}"><link rel="alternate" type="application/rss+xml" title="${esc(siteName)} Journal" href="${esc(baseUrl)}/blog/feed.xml">
<link rel="stylesheet" href="../../assets/aurekai-field-guide.css"><link rel="stylesheet" href="../../assets/aurekai-research.css"><link rel="stylesheet" href="../blog.css"></head>
<body><a class="skip" href="#main">Skip to article</a><header class="top"><div class="shell"><a class="brand" href="../../"><span class="mark">A</span> ${esc(siteName.toUpperCase())} <small>/ JOURNAL</small></a>
<nav aria-label="Site navigation">${nav}</nav></div></header>
<main id="main"><section class="journal-article-hero"><div class="shell journal-article-head">
<div class="journal-breadcrumb"><a href="../">Journal</a><span>/</span><span>${esc(category || "Journal")}</span></div>
${icon ? `<span class="glyph" aria-hidden="true"><svg viewBox="0 0 24 24"><use href="../../assets/product-icons.svg#${icon}"></use></svg></span>` : ""}<div class="eyebrow">${esc(category)} · ${mins} min read</div>
<h1>${esc(journal.title)}</h1><p class="lead">${esc(journal.description)}</p>
<div class="journal-meta"><span>${esc(journal.published || "")}</span></div>
</div></section>
<section class="journal-article-shell"><div class="shell journal-article-layout">
<article class="article-body">
${body}
</article>
${rail}
</div></section>
</main>${keepReading}${footer}</body></html>`;
  return { schema: "aurekai.journal-article.v1", slug: journal.slug, route, canonical, html };
}
export function nativeRenderMd(binary) {
  return markdown => {
    const run = spawnSync(binary, ["render-md"], { input: markdown, encoding: "utf8", timeout: 30000 });
    if (run.status !== 0) fail("native markdown render refused input");
    return run.stdout;
  };
}
export default { composeJournalArticle, nativeRenderMd };
