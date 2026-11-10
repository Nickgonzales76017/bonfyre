import { test, expect } from "bun:test";
import { Database } from "bun:sqlite";
import { existsSync, mkdtempSync, mkdirSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { clean, policy, publish, publishTerritories } from "../publish-edition.mjs";

test("retained feeds and published magazine profile compile into one provenance-carrying edition", () => {
  const base = mkdtempSync(join(tmpdir(), "bonfyre-edition-"));
  const estate = join(base, "estate"), out = join(base, "out");
  mkdirSync(estate);
  try {
    const archive = new Database(join(estate, "items.db"));
    archive.exec('CREATE TABLE item(title TEXT,body TEXT,href TEXT,image TEXT,publisher TEXT,"by" TEXT,at INTEGER,source TEXT,kind TEXT)');
    const insert = archive.query('INSERT INTO item VALUES(?,?,?,?,?,?,?,?,?)');
    const current = Math.floor(Date.now() / 1000);
    insert.run("A school story with \\\"quoted\\\" words", "A".repeat(270), "https://example.test/story", "https://example.test/cover.jpg", "School & Press", "Reporter", current, "feed:school", "story");
    insert.run("School photo", "", "https://example.test/photo", "https://example.test/photo.jpg", "School & Press", "", current - 90, "feed:school", "photo");
    insert.run("School audio", "", "https://example.test/audio", "https://example.test/audio.jpg", "School & Press", "", current - 100, "feed:school", "audio");
    insert.run("Future clock must not choose cover", "B".repeat(300), "https://example.test/future", "https://example.test/future.jpg", "Future", "", current + 86400 * 100, "feed:future", "story");
    archive.close();
    const cms = new Database(join(estate, "cms.db"));
    cms.exec("CREATE TABLE presentation_template(id INTEGER PRIMARY KEY,template TEXT,reference_name TEXT,reference_url TEXT,family TEXT,hero_mode TEXT,typography_mode TEXT,density TEXT,layout_tokens TEXT)");
    cms.query("INSERT INTO presentation_template(template,reference_name,reference_url,family,hero_mode,typography_mode,density,layout_tokens) VALUES(?,?,?,?,?,?,?,?)").run("magazine-one", "School Magazine", "", "magazine", "cover", "editorial", "medium", JSON.stringify({ ground: "#101010", accent: "red;}</style><script>" }));
    cms.close();

    const receipt = publish({ estate, out, territory: "life" });
    expect(receipt.template).toBe("School Magazine");
    expect(receipt.kept).toBe(4);
    expect(receipt.cover).toContain("school story");
    expect(receipt.strip).toBe(1);
    expect(receipt.listen).toBe(1);
    const page = readFileSync(join(out, "edition.html"), "utf8");
    expect(Buffer.byteLength(page)).toBe(receipt.bytes);
    expect(page).toContain("School &amp; Press");
    expect(page).toContain("School Magazine");
    expect(page).toContain("A school story with &quot;quoted&quot; words");
    expect(page).not.toContain("Future clock must not choose cover");
    expect(page).not.toContain("red;}</style><script>");
    expect(page).toContain("not a claim that its source still publishes it");
    expect(clean("\\\\\\\"word\\\\\\\"")).toBe('"word"');
    const stage = join(base, "staged");
    const installed = Bun.spawnSync(["make", "-C", join(import.meta.dir, ".."), "install-edition", `MEMBRANE_ROOT=${stage}`]);
    expect(installed.exitCode).toBe(0);
    expect(existsSync(join(stage, "scripts/edition.css"))).toBe(true);
    const staged = Bun.spawnSync(["bun", join(stage, "scripts/publish-edition.mjs"), "--estate", estate, "--out", join(base, "staged-out"), "--territory", "life"]);
    expect(staged.exitCode).toBe(0);
    expect(JSON.parse(staged.stdout.toString()).cover).toBe(receipt.cover);
    const all = publishTerritories({ estate, out: join(base, "public") });
    expect(all.source_read_count).toBe(1);
    expect(Object.keys(all.editions)).toEqual(policy.territories);
    const root = readFileSync(join(base, "public/edition.html"), "utf8");
    expect(root).toBe(readFileSync(join(base, `public/hosts/${policy.host_root}.${policy.root_territory}/edition.html`), "utf8"));
    for (const territory of policy.territories) expect(readFileSync(join(base, `public/hosts/${policy.host_root}.${territory}/edition.html`), "utf8")).toContain(`Aurekai · ${territory}`);
  } finally { rmSync(base, { recursive: true, force: true }); }
});
