import { test, expect } from "bun:test";
import { Database } from "bun:sqlite";
import { chmodSync, mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { normalise, redact, screen } from "../content-screen.mjs";

test("spelling normalization joins separated words without absorbing punctuation", () => {
  expect(normalise("SPAM!!!")).toBe("spam");
  expect(normalise("s p a m")).toBe("spam");
  expect(normalise("s-p-a-m")).toBe("spam");
  expect(normalise("sp@m")).toBe("spam");
  expect(normalise("a spam")).toBe("a spam");
});

test("all historical spelling and ordinary-prose boundaries survive the native cutover", () => {
  for (const written of ["spam", "S P A M", "s-p-a-m", "5p4m", "spám", "SPAM!!!", "sp@m", "s.p.a.m", "...spam...", "S.P.A.M!"])
    expect(` ${normalise(written)} `).toContain(" spam ");
  for (const written of ["class pamphlet", "a spa magazine", "spammer", "spa m"])
    expect(` ${normalise(written)} `).not.toContain(" spam ");
  expect(normalise("hi!")).toBe("hi");
  expect(redact("spam")).toBe("s***");
  expect(redact("x")).toBe("x*");
});

test("absent private terms never masquerade as a clean screen", async () => {
  const dir = mkdtempSync(join(tmpdir(), "bonfyre-missing-terms-"));
  try {
    const result = await screen({ termsPath: join(dir, "missing.txt") });
    expect(result.exitCode).toBe(2);
    expect(result.receipt.state).toBe("installed_without_terms");
    expect(result.receipt.screened).toBe(0);
    expect(result.receipt.flagged).toBe(0);
    expect(result.receipt.cannot_infer).toContain("looked at nothing");
    expect(result.receipt.cannot_infer).toContain("not a finding");
  } finally { rmSync(dir, { recursive: true, force: true }); }
});

test("ordinary surfaces exclude boxed material without erasing its receipt", () => {
  const kit = readFileSync(resolve(import.meta.dir, "../../site/matrix/surfaces/_kit.js"), "utf8");
  expect(kit).toContain("K.flaggedTwins");
  const reader = kit.slice(kit.indexOf("K.readSource = async")).split("\n  };")[0];
  expect(reader).toContain("boxed.has(row.twin)");
  expect(reader).toContain("includeFlagged");
  expect(kit).toContain("held back from ordinary projection");
  expect(kit).toContain("!== 'cleared'");
});

const live = process.env.BONFYRE_LIVE_SURFACE_TEST === "1" ? test : test.skip;
const surface = async name => {
  const response = await fetch(`https://aurekai.live/_surface/${name}`, { headers: { Accept: "application/json" }, signal: AbortSignal.timeout(30000) });
  if (!response.ok) throw new Error(`${name}: ${response.status}`);
  return response.json();
};

live("live flags say what they cannot infer and name their screen", async () => {
  const flags = (await surface("flagged-observations")).records;
  const screens = new Set((await surface("content-screens")).records.map(row => row.screen));
  for (const flag of flags) {
    for (const word of ["illegal", "false", "malicious", "deleted"]) expect(flag.cannot_infer.toLowerCase()).toContain(word);
    expect(flag.cannot_infer.toLowerCase()).toContain("not flagged is not safe");
    expect(screens.has(flag.screen)).toBe(true);
    expect(flag.signal).toBeTruthy();
    expect(flag.review_state).toBeTruthy();
  }
});

live("live declared screens do not masquerade as observations", async () => {
  const screens = (await surface("content-screens")).records;
  const declared = screens.filter(row => row.state === "declared");
  expect(declared.length).toBeGreaterThan(0);
  for (const row of screens) {
    expect(row.cannot_see).toBeTruthy();
    expect(row.cannot_infer).toBeTruthy();
  }
  for (const row of declared) {
    expect(row.sees.toLowerCase()).toContain("nothing yet");
    expect(row.reference_url).toBeTruthy();
    expect(row.reference_note).toBeTruthy();
    expect(/not a running one|has been classified|not screened/.test(row.cannot_infer.toLowerCase())).toBe(true);
  }
});

test("retained body at an edge creates one boxed CMS flag without disclosing terms", async () => {
  const dir = mkdtempSync(join(tmpdir(), "bonfyre-screen-"));
  try {
    const bodies = join(dir, "bodies"), receipts = join(dir, "receipts"), termsPath = join(dir, "terms.txt");
    mkdirSync(bodies); mkdirSync(receipts);
    writeFileSync(termsPath, "spam\n# private list remains external to the repo\n");
    writeFileSync(join(bodies, "twin-one.body"), "SPAM at the beginning. Then s-p-a-m at the end: spam");
    writeFileSync(join(receipts, "twin-one.json"), '{"observed_at":"old","content_sha256":"old"}\n{"observed_at":"now","content_sha256":"digest"}\n');
    const dbPath = join(dir, "cms.db");
    const db = new Database(dbPath);
    db.exec("CREATE TABLE content_flag(flag TEXT)");
    db.close();
    await expect(screen({ bodies, receipts, termsPath, dbPath, twin: "../outside" })).rejects.toThrow("single retained-body name");
    const logPath = join(dir, "created.json"), cms = join(dir, "cms.mjs");
    writeFileSync(cms, `#!/usr/bin/env bun\nimport { writeFileSync } from "node:fs";\nwriteFileSync(process.env.BONFYRE_SCREEN_TEST_LOG, process.argv.at(-1));\nconsole.log(JSON.stringify({created:true}));\n`);
    chmodSync(cms, 0o755);
    const previous = process.env.BONFYRE_SCREEN_TEST_LOG;
    process.env.BONFYRE_SCREEN_TEST_LOG = logPath;
    try {
      const probe = Bun.spawnSync([cms, "entry", "create", "content_flag", "--db", dbPath, "--ns", "root", "{}"], { env: { ...process.env } });
      expect([probe.exitCode, probe.stderr.toString()]).toEqual([0, ""]);
      const result = await screen({ bodies, receipts, termsPath, dbPath, cms });
      expect(result.exitCode).toBe(0);
      expect(result.receipt.screened).toBe(1);
      expect(result.receipt.flagged).toBe(1);
      const row = JSON.parse(readFileSync(logPath, "utf8"));
      expect(row.receipt_ref).toBe("digest");
      expect(row.observed_at).toBe("now");
      expect(row.sample_count).toBe("3");
      expect(row.review_state).toBe("boxed");
      expect(row.matched).toBe("s***");
      expect(JSON.stringify(row)).not.toContain("spam");
    } finally {
      if (previous === undefined) delete process.env.BONFYRE_SCREEN_TEST_LOG;
      else process.env.BONFYRE_SCREEN_TEST_LOG = previous;
    }
  } finally { rmSync(dir, { recursive: true, force: true }); }
});
