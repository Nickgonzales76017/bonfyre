#!/usr/bin/env bun
// Compatibility entry point: build/validate/render APIs remain stable while
// record grammar, material, layout, styles, and output have separate owners.
import { randomUUID } from "node:crypto";
import { cpSync, existsSync, mkdirSync, mkdtempSync, readFileSync, renameSync, rmSync, writeFileSync } from "node:fs";
import { basename, dirname, join, resolve, sep } from "node:path";
import { discovery } from "./seo.mjs";
import { workerSource } from "./lead-worker.mjs";
import { withMaterial } from "./site-material.mjs";
import { compileCss } from "./site-styles.mjs";
import { renderSite } from "./site-renderer.mjs";
export { pagesOf, validate } from "./site-contract.mjs";
export { kinds } from "./section-registry.mjs";
export { renderSite, renderPage } from "./site-renderer.mjs";

export async function build(siteDir, out = join(siteDir, "dist")) {
  const content = await withMaterial(JSON.parse(readFileSync(join(siteDir, "content.json"), "utf8")), siteDir);
  const files = content._files ?? {}; delete content._files;
  const { pages, theme, forms } = renderSite(content);
  // Every picture the record names has to be in the site's own folder.
  for (const page of pages) for (const src of page.html.matchAll(/<img src="\/([^"/][^"]*)"/g)) if (!existsSync(join(siteDir, src[1]))) throw new Error(`site record names ${src[1]}, which is not in ${siteDir}`);
  const sourcePath = resolve(siteDir), outPath = resolve(out);
  if (sourcePath === outPath || sourcePath.startsWith(outPath + sep)) throw new Error("Build output cannot replace the site source directory or one of its parents.");
  if (outPath.startsWith(sourcePath + sep) && outPath !== join(sourcePath, "dist")) throw new Error("Build output cannot overwrite authored site files; use dist or an output outside the site folder.");
  mkdirSync(dirname(outPath), { recursive: true });
  const stage = mkdtempSync(join(dirname(outPath), `.${basename(outPath)}.stage-`));
  try {
    mkdirSync(join(stage, "assets"), { recursive: true });
    if (existsSync(join(siteDir, "assets"))) cpSync(join(siteDir, "assets"), join(stage, "assets"), { recursive: true });
    for (const page of pages) { mkdirSync(join(stage, page.path), { recursive: true }); writeFileSync(join(stage, page.path, "index.html"), page.html); }
    // An address the site does not have answers "not here" in the site's own look, instead of the home page under a wrong name.
    const lost = renderSite({ ...content, pages: [...(content.pages ?? []), { path: "not-here", title: "Not here", menu: "Not here", sections: [
    { kind: "hero", headline: "That page is not here", lede: "The address may have changed. Everything this site has is one tap from the front page.", button: content.action }, { kind: "cases", id: "go", title: "Where to go", items: [{ icon: "home", title: content.business.name, text: "Start from the front page." }] }] }] }).pages.find(p => p.path === "not-here");
    writeFileSync(join(stage, "404.html"), lost.html.replace('<meta name="robots" content="index, follow" />', '<meta name="robots" content="noindex" />'));
    compileCss([...pages, lost], theme, join(stage, "assets/site.css"));
    for (const [path, bytes] of Object.entries(files)) { mkdirSync(join(stage, path, ".."), { recursive: true }); writeFileSync(join(stage, path), bytes); }
    const seo=discovery(content,pages);if(seo){writeFileSync(join(stage,"robots.txt"),seo.robots);writeFileSync(join(stage,"sitemap.xml"),seo.sitemap);}
    if (content.leads === "server") {
    writeFileSync(join(stage, "_worker.js"), workerSource(content.site_id, forms));
    // Pages otherwise invokes the Worker for every asset and spends the shared
    // Functions allowance on static visits. Only intake needs server execution.
    writeFileSync(join(stage, "_routes.json"), JSON.stringify({ version: 1, include: ["/api/lead", "/api/lead/*"], exclude: [] }) + "\n");
    }
    // A preview is kept out of search engines at the edge as well as in the page.
  writeFileSync(join(stage, "_headers"), `/*\n  X-Content-Type-Options: nosniff\n  Referrer-Policy: strict-origin-when-cross-origin\n${content.preview ? "  X-Robots-Tag: noindex, nofollow\n" : ""}/assets/*\n  Cache-Control: public, max-age=3600\n/data/*\n  Cache-Control: public, max-age=3600\n`);
    const previous = existsSync(outPath) ? join(dirname(outPath), `.${basename(outPath)}.previous-${randomUUID()}`) : null;
    if (previous) {
      renameSync(outPath, previous);
    }
    try { renameSync(stage, outPath); }
    catch (error) { if (previous) renameSync(previous, outPath); throw error; }
    if (previous) rmSync(previous, { recursive: true, force: true });
  } finally {
    rmSync(stage, { recursive: true, force: true });
  }
  return { out, site_id: content.site_id, data_files: Object.keys(files).length, data_bytes: Object.values(files).reduce((n, f) => n + f.length, 0), pages: Object.fromEntries(pages.map(p => ["/" + p.path, p.kinds])), forms: Object.keys(forms), leads: content.leads ?? "mail",
    template: theme.template, preview: Boolean(content.preview), phone: pages[0].site.phone_display || null, email: content.contact?.email ?? null };
}

if (import.meta.main) {
  const dir = process.argv[2];
  if (!dir) { console.error("usage: bun build.mjs <site-dir> [--out <dir>]"); process.exit(2); }
  const at = process.argv.indexOf("--out");
  console.log(JSON.stringify(await build(resolve(dir), at > 0 ? resolve(process.argv[at + 1]) : undefined)));
}
