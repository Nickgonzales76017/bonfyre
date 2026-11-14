import {test, expect} from "bun:test";
import {readFileSync} from "node:fs";
import {join} from "node:path";
import {createHash} from "node:crypto";
import {newAuthoringTree} from "../runtime/authoring-tree.mjs";
import {toNativePageWrite} from "../../../components/BonfyreServiceSite/authoring/native-cms.mjs";
import {mergeJournalIntoTree, journalPageMetadata} from "../../../components/BonfyreServiceSite/authoring/journal-import.mjs";
import {detectIntakeKind, parseIntake} from "../../../components/BonfyreServiceSite/authoring/intake-registry.mjs";

const fix = join(import.meta.dir, "fixtures/intake");
const load = n => readFileSync(join(fix, n), "utf8");
const base = (site, route) => ({schema: "bonfyre.page-snapshot.v1", digest: "b".repeat(64), id: 0,
  snapshot: {namespace: site, route, page: null, fragments: []}});

test("fixtures still hash to their receipted SHASUMS", () => {
  for (const line of load("SHASUMS").trim().split("\n")) {
    const [sha, bytes, name] = line.split(/\s+/);
    const body = load(name);
    expect(createHash("sha256").update(body).digest("hex")).toBe(sha);
    expect(String(body.length)).toBe(bytes);
  }
});

test("detection names all five foreign kinds plus local markdown", () => {
  expect(detectIntakeKind(load("wp-post-normalized.txt"))).toBe("wordpress-normalized");
  expect(detectIntakeKind(load("feed-dallas-trimmed.xml"))).toBe("rss-feed");
  expect(detectIntakeKind(load("quakes-trimmed.geojson"))).toBe("geojson-feed");
  expect(detectIntakeKind(load("apppack-trimmed.json"))).toBe("frappe-apppack");
  expect(detectIntakeKind(load("ghost-post.json"))).toBe("ghost-post");
  expect(detectIntakeKind("---\nslug: s\ntitle: T\n---\nBody text here.\n")).toBe("local-markdown");
  expect(detectIntakeKind("<p>plain html</p>")).toBe("unknown");
});

for (const [kind, file, slug] of [
  ["wordpress-normalized", "wp-post-normalized.txt", null],
  ["rss-feed", "feed-dallas-trimmed.xml", "rss-dallas-city-news"],
  ["geojson-feed", "quakes-trimmed.geojson", "usgs-m45-digest"],
  ["frappe-apppack", "apppack-trimmed.json", null],
  ["ghost-post", "ghost-post.json", null],
]) {
  test(`${kind} normalizes to a composable journal draft`, async () => {
    const article = await parseIntake(kind, load(file), {filename: file, ...(slug ? {slug} : {})});
    expect(article.schema).toBe("aurekai.journal-intake.v1");
    expect(article.source_kind).toBe(kind);
    expect(article.source_digest).toMatch(/^[a-f0-9]{64}$/);
    expect(article.sections.length).toBeGreaterThan(0);
    const route = "/blog/" + article.slug + "/";
    const draft = mergeJournalIntoTree(newAuthoringTree("person-nick-work", route), article);
    const request = toNativePageWrite(base("person-nick-work", route), draft, {pageDraft: journalPageMetadata(article)});
    expect(request.operation).toBe("create");
    expect(request.page.status).toBe("draft");
    expect(request.fragments.length).toBe(article.sections.length);
  });
}

test("registry journals carry kind-correct titles, categories and digests", async () => {
  const wp = await parseIntake("wordpress-normalized", load("wp-post-normalized.txt"), {filename: "normalized.txt"});
  expect(wp.title).toContain("WordPress 7.1.3");
  expect(wp.slug).toBe("wp-wordpress-7-1-3-maintenance-and-security-release");
  expect(wp.category).toBe("WordPress");
  expect(wp.sections.length).toBeGreaterThanOrEqual(4);
  const rss = await parseIntake("rss-feed", load("feed-dallas-trimmed.xml"), {filename: "feed.xml", slug: "rss-x"});
  expect(rss.sections).toHaveLength(3);
  expect(rss.sections[0].body).toContain("https://");
  expect(rss.published).toMatch(/^\d{4}-\d{2}-\d{2}$/);
  const quakes = await parseIntake("geojson-feed", load("quakes-trimmed.geojson"), {filename: "q.geojson", slug: "usgs-x"});
  expect(quakes.sections).toHaveLength(5);
  expect(quakes.sections[0].heading).toMatch(/^M[0-9.]+ \u00b7 /);
  const mags = quakes.sections.map(s => parseFloat(s.heading.slice(1)));
  expect([...mags].sort((a, b) => b - a)).toEqual(mags);
  const pack = await parseIntake("frappe-apppack", load("apppack-trimmed.json"), {filename: "app.json"});
  expect(pack.title).toBe("insights surfaces");
  expect(pack.sections).toHaveLength(2);
  expect(pack.sections[0].body).toContain("Title (Data, required)");
  const ghost = await parseIntake("ghost-post", load("ghost-post.json"), {filename: "post.json"});
  expect(ghost.slug).toBe("welcome-to-the-newsroom");
  expect(ghost.category).toBe("News");
  expect(ghost.published).toBe("2026-10-01");
});

test("foreign intake refuses hostile and malformed sources", async () => {
  await expect(parseIntake("nope", "x")).rejects.toThrow(/unknown intake kind/);
  await expect(parseIntake("wordpress-html", "<p class=\"wp-block-paragraph\">x</p>")).rejects.toThrow(/normalized/);
  await expect(parseIntake("rss-feed", "<rss></rss>", {slug: "r"})).rejects.toThrow();
  await expect(parseIntake("rss-feed", "not xml at all, just fifty-plus characters of plain text here ok?", {slug: "r"})).rejects.toThrow(/recognized feed/);
  await expect(parseIntake("geojson-feed", "{\"type\":\"Point\"}", {slug: "g"})).rejects.toThrow();
  await expect(parseIntake("geojson-feed", "{bad json", {slug: "g"})).rejects.toThrow();
  await expect(parseIntake("frappe-apppack", "{\"kind\":\"Nope\"}", {slug: "f"})).rejects.toThrow();
  await expect(parseIntake("ghost-post", "{\"posts\":[]}", {slug: "g"})).rejects.toThrow();
  await expect(parseIntake("ghost-post", "{\"title\":\"t\"}", {slug: "g"})).rejects.toThrow();
});
