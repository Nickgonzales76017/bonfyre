import { test, expect } from 'bun:test';
import { mkdtempSync, readFileSync, writeFileSync, existsSync, readdirSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { build } from '../build.mjs';
import { materialFilePath } from '../site-material.mjs';

// Pages matches these patterns against the pathname; queries are not routes.
const matches = (pattern, path) => pattern.endsWith('*') ? path.startsWith(pattern.slice(0, -1)) : path === pattern;
const invokes = (routes, address) => {
  const path = new URL(address, 'https://example.test').pathname;
  return routes.include.some(p => matches(p, path)) && !routes.exclude.some(p => matches(p, path));
};

test('published server sites invoke intake only for lead endpoints and remove routing when switched to mail', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'service-site-routing-'));
  try {
    const content = JSON.parse(readFileSync(new URL('./fixtures/sample-lawn/content.json', import.meta.url), 'utf8'));
    content.leads = 'server';
    writeFileSync(join(dir, 'content.json'), JSON.stringify(content));
    const { out } = await build(dir);
    const routes = JSON.parse(readFileSync(join(out, '_routes.json'), 'utf8'));
    expect(routes.version).toBe(1);
    for (const path of ['/api/lead', '/api/lead?probe=1', '/api/lead/catalogue-review', '/api/lead/unknown']) expect(invokes(routes, path)).toBe(true);
    for (const path of ['/', '/catalogue/?store=red3-display', '/assets/site.css', '/assets/catalogue-client.mjs', '/data/catalogue.json', '/robots.txt', '/sitemap.xml', '/missing/', '/api/leader', '/api/leadership']) expect(invokes(routes, path)).toBe(false);
    const worker = (await import(join(out, '_worker.js'))).default;
    for (const [path, status, why] of [['/api/lead', 405, 'post only'], ['/api/lead/catalogue-review', 404, 'no such review form']]) {
      const response = await worker.fetch(new Request('https://example.test' + path), {});
      expect(response.status).toBe(status);
      expect((await response.json()).why).toBe(why);
    }
    content.leads = 'mail';
    writeFileSync(join(dir, 'content.json'), JSON.stringify(content));
    await build(dir);
    expect(existsSync(join(out, '_routes.json'))).toBe(false);
    expect(existsSync(join(out, '_worker.js'))).toBe(false);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}, 30000);

test("build preserves the last complete site after a rejected revision and swaps a valid replacement", async () => {
  const dir = mkdtempSync(join(tmpdir(), "service-site-swap-"));
  try {
    const content = JSON.parse(readFileSync(new URL("./fixtures/sample-lawn/content.json", import.meta.url), "utf8"));
    writeFileSync(join(dir, "content.json"), JSON.stringify(content));
    const { out } = await build(dir);
    const original = readFileSync(join(out, "index.html"));
    writeFileSync(join(out, "previous-marker"), "last good release");
    const invalid = { ...content, navigation: [{ label: "Broken", href: "/nonexistent/" }] };
    writeFileSync(join(dir, "content.json"), JSON.stringify(invalid));
    await expect(build(dir)).rejects.toThrow();
    expect(readFileSync(join(out, "index.html"))).toEqual(original);
    expect(readFileSync(join(out, "previous-marker"), "utf8")).toBe("last good release");
    expect(readdirSync(dir).some(name => name.startsWith(".dist.stage-"))).toBe(false);
    writeFileSync(join(dir, "content.json"), JSON.stringify(content));
    await build(dir);
    expect(existsSync(join(out, "previous-marker"))).toBe(false);
    expect(readFileSync(join(out, "index.html"))).toEqual(original);
    expect(readdirSync(dir).some(name => name.startsWith(".dist."))).toBe(false);
    await expect(build(dir, dir)).rejects.toThrow("cannot replace the site source");
    await expect(build(dir, join(dir, "content.json"))).rejects.toThrow("cannot overwrite authored site files");
    expect(existsSync(join(dir, "content.json"))).toBe(true);
  } finally { rmSync(dir, { recursive: true, force: true }); }
}, 30000);

test("material files stay under their section and cannot replace site output", async () => {
  expect(materialFilePath("gallery", "images/one.webp")).toBe("data/gallery/images/one.webp");
  for (const path of ["../escape", "images/../../outside", "/absolute", "images//empty", "./same", "a\\b", ""]) {
    expect(() => materialFilePath("gallery", path)).toThrow("must stay inside");
  }
  const dir = mkdtempSync(join(tmpdir(), "service-site-material-"));
  try {
    const content = JSON.parse(readFileSync(new URL("./fixtures/sample-lawn/content.json", import.meta.url), "utf8"));
    writeFileSync(join(dir, "content.json"), JSON.stringify(content));
    const { out } = await build(dir);
    const original = readFileSync(join(out, "index.html"));
    content.sections[0].id = "gallery"; content.sections[0].material = "unsafe";
    writeFileSync(join(dir, "content.json"), JSON.stringify(content));
    writeFileSync(join(dir, "material.mjs"), 'export const unsafe = () => ({files: {"../../escape": "should never write"}});');
    await expect(build(dir)).rejects.toThrow("must stay inside");
    expect(readFileSync(join(out, "index.html"))).toEqual(original);
    expect(existsSync(join(dir, "escape"))).toBe(false);
  } finally { rmSync(dir, { recursive: true, force: true }); }
}, 30000);
