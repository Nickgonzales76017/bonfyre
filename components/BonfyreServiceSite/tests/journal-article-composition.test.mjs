import { test, expect } from "bun:test";
import { readFileSync, existsSync } from "node:fs";
import { resolve } from "node:path";
import { parseJournalMarkdown } from "../authoring/journal-import.mjs";
import { composeJournalArticle, nativeRenderMd } from "../authored-journal-article.mjs";

const repo = resolve(import.meta.dir, "../../..");
const binary = resolve(repo, "cmd/BonfyreSurfaceScene/bonfyre-surface-scene");
if (!existsSync(binary)) throw new Error("build bonfyre-surface-scene first: make -C cmd/BonfyreSurfaceScene");

const SLUG = "a-job-posting-has-more-than-one-job";
const source = readFileSync(resolve(repo, `editorial/posts/${SLUG}.md`), "utf8");
const reference = readFileSync(resolve(repo, `site/blog/${SLUG}/index.html`), "utf8");
const frontmatter = Object.fromEntries([...source.match(/---\n([\s\S]*?)\n---/)?.[1]
  .split("\n").map(l => { const i = l.indexOf(":"); return i < 0 ? [] : [l.slice(0, i).trim(), l.slice(i + 1).trim()]; })
  .filter(p => p.length === 2) ?? []]);
const nav = [
  { label: "Home", href: "../../" }, { label: "Products", href: "../../explore.html" },
  { label: "Research", href: "../../market.html" }, { label: "Journal", href: "../" },
  { label: "Bonfyre", href: "../../docs.html" },
];

const journal = await parseJournalMarkdown(source, { filename: `${SLUG}.md` });
const related = [...reference.matchAll(/<a class="journal-related" href="\.\.\/([a-z0-9-]+)\/">(.*?) ↗<\/a>/g)]
  .map(m => ({ slug: m[1], title: m[2] }));
const page = composeJournalArticle(journal, {
  baseUrl: "https://nick.aurekai.work", renderMd: nativeRenderMd(binary),
  navigation: nav, meta: { category: frontmatter.category, icon: frontmatter.icon },
  related,
});

test("composed article carries the reference hero", () => {
  const h1 = ref => ref.match(/<h1>(.*?)<\/h1>/)?.[1];
  expect(h1(page.html)).toBe(h1(reference));
  expect(page.html).toContain("journal-breadcrumb");
  expect(page.html).toContain(frontmatter.category);
  expect(page.html).toContain(`product-icons.svg#${frontmatter.icon}`);
  expect(page.html).toContain(journal.description);
});

test("composed article carries every reference section in order", () => {
  const h2s = ref => [...ref.matchAll(/<h2(?: id="([^"]+)")?>(.*?)<\/h2>/g)].map(m => m[1] || m[2]);
  expect(h2s(page.html)).toEqual(h2s(reference));
  for (const section of journal.sections.slice(1)) {
    const firstLine = section.body.split("\n").map(l => l.trim()).find(l => l && !l.startsWith("#"));
    expect(page.html).toContain(firstLine.slice(0, 80).replace(/[*_`]/g, "").slice(0, 60));
  }
});

test("composed article carries rail, related links, and shell", () => {
  expect(page.html).toContain("journal-article-shell");
  expect(page.html).toContain("journal-side-sticky");
  expect(page.html).toContain("In this article");
  for (const r of related) expect(page.html).toContain(`../${r.slug}/`);
  expect(page.html).toContain("All articles ↗");
  expect(page.html).toContain('<footer class="footer">');
});

test("composed article carries the mature style refs and estate canonical", () => {
  for (const css of ["aurekai-field-guide.css", "aurekai-research.css", "blog.css"])
    expect(page.html).toContain(css);
  expect(page.html).toContain("theme-color");
  expect(page.canonical).toBe(`https://nick.aurekai.work/blog/${SLUG}/`);
  expect(page.html).toContain(page.canonical);
  expect(page.html).toContain("og:title");
});
