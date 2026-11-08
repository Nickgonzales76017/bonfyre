import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),"../..");
const blog=path.join(root,"site/blog");
const source=path.join(root,"editorial/posts");
const md=fs.readdirSync(source).filter(n=>n.endsWith(".md"));
test("all edited articles have canonical static HTML",() => {
 assert.equal(md.length,16);
 const seen=new Set();
 for(const f of md){
  const slug=f.replace(/\.md$/,""), html=fs.readFileSync(path.join(blog,slug,"index.html"),"utf8");
  assert.ok(!seen.has(slug));seen.add(slug);
  assert.ok(html.includes("<h1>")&&html.includes("</article>"),slug);
  assert.ok(html.includes("https://nickgonzales76017.github.io/bonfyre/blog/"+slug+"/"),slug);
  assert.ok(html.includes("Edited field notes"),slug);
  assert.ok(!html.includes("§")&&!html.includes("¤"),slug);
 }
});
test("journal index has categories, search, links and accessible controls",() => {
 const html=fs.readFileSync(path.join(blog,"index.html"),"utf8");
 assert.ok(html.includes('id="journal-search"'));
 assert.ok(html.includes('aria-pressed="true"'));
 assert.ok(html.includes('aria-live="polite"'));
 assert.ok(html.includes("blog/filter.js"));
 for(const p of md)assert.ok(html.includes("blog/"+p.replace(/\.md$/,"")+"/"));
});
test("RSS, sitemap and future import index cover the same sixteen articles",() => {
 const rss=fs.readFileSync(path.join(blog,"feed.xml"),"utf8");
 const sitemap=fs.readFileSync(path.join(blog,"sitemap.xml"),"utf8");
 const manifest=fs.readFileSync(path.join(blog,"post-index.tsv"),"utf8").trim().split("\n");
 assert.equal((rss.match(/<item>/g)||[]).length,16);
 assert.equal((sitemap.match(/<url>/g)||[]).length,17);
 assert.equal(manifest.length,17);
 for(const p of md)assert.ok(manifest.some(line=>line.includes("post:"+p.replace(/\.md$/,""))));
});
test("all declared icons exist in the published sprite",() => {
 const sprite=fs.readFileSync(path.join(root,"site/assets/product-icons.svg"),"utf8");
 for(const p of md){
   const content=fs.readFileSync(path.join(source,p),"utf8");
   const match=/^icon: ([a-z-]+)$/m.exec(content);
   assert.ok(match,p);assert.ok(sprite.includes('id="'+match[1]+'"'),p);
 }
});