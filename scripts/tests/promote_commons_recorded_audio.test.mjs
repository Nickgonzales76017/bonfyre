import { afterAll, expect, test } from "bun:test";
import { chmodSync, existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { commonsTitle, licenseLaw, options, promote } from "../promote_commons_recorded_audio.mjs";

const root = mkdtempSync(join(tmpdir(), "commons-audio-"));
const items = join(root, "items.jsonl"), receipt = join(root, "plan.json");
const project = join(root, "project"), cmsBin = join(root, "cms"), cmsTrace = join(root, "cms.jsonl");
const badDigest = join(root, "bad-digest");
afterAll(() => rmSync(root, { recursive: true, force: true }));
const row = { source: "wikidata-recorded-music", kind: "audio", key: "recording-1", ident: "Q49617",
  title: "An authored recording", href: "https://www.wikidata.org/wiki/Q49617",
  origin_href: "https://commons.wikimedia.org/wiki/File:Authored_recording.ogg" };
writeFileSync(items, [row, { ...row, key: "recording-2", origin_href: "https://example.com/wiki/File:Wrong.ogg" },
  { ...row, key: "recording-3", ident: "", origin_href: "" }].map(value => JSON.stringify(value)).join("\n"));
const fetcher = async () => ({ ok: true, json: async () => ({ query: { pages: [{
  imageinfo: [{ url: "https://upload.wikimedia.org/test.ogg", size: 16, mime: "audio/ogg",
    extmetadata: { LicenseShortName: { value: "CC BY 4.0" }, Artist: { value: "<b>Creator &amp; archive</b>" } } }],
  categories: [{ title: "Category:Audio files of music" }] }] } }) });

test("Commons page and licence are independent gates", () => {
  expect(commonsTitle(row)).toBe("File:Authored_recording.ogg");
  expect(commonsTitle({ origin_href: "https://example.com/wiki/File:Authored_recording.ogg" })).toBeNull();
  expect(licenseLaw({ metadata: { LicenseShortName: "CC BY-NC 4.0" } })[0]).toBe(false);
  expect(licenseLaw({ metadata: { LicenseShortName: "CC BY 4.0" } })[0]).toBe(true);
});

test("full-frontier plan keeps one eligible item and explicit refusals without effects", async () => {
  const cfg = options(["--estate", root, "--out", receipt]);
  const report = await promote(cfg, fetcher);
  expect(report).toMatchObject({ candidates: 3, admitted: 0, eligible: 1, refused: 2, processed: 3 });
  expect(report.results.map(result => result.state)).toEqual(["ADMIT", "REFUSED", "REFUSED"]);
  expect(JSON.parse(readFileSync(receipt, "utf8"))).toEqual(report);
  expect(existsSync(join(root, "foreign"))).toBe(false);
});

test("apply writes separate CMS records only after a receipted body rehashes", async () => {
  writeFileSync(project, `#!/usr/bin/env bun
import { createHash } from "node:crypto";
import { existsSync, mkdirSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";
const args=process.argv.slice(2), receipt=args[args.indexOf("--receipt")+1], body=args[args.indexOf("--body")+1];
mkdirSync(dirname(receipt),{recursive:true}); mkdirSync(dirname(body),{recursive:true});
const bytes="retained audio"; writeFileSync(body,bytes);
const digest=createHash("sha256").update(bytes).digest("hex");
writeFileSync(receipt,JSON.stringify({policy_decision:"allow",content_sha256:existsSync(${JSON.stringify(badDigest)}) ? "0".repeat(64) : digest,body_path:body})+"\\n");
`);
  writeFileSync(cmsBin, `#!/usr/bin/env bun
import { appendFileSync } from "node:fs";
appendFileSync(${JSON.stringify(cmsTrace)},JSON.stringify(process.argv.slice(2))+"\\n");
`);
  chmodSync(project, 0o755); chmodSync(cmsBin, 0o755);
  const cfg = options(["--estate", root, "--items", items, "--out", receipt,
    "--project", project, "--cms-bin", cmsBin, "--apply"]);
  const report = await promote(cfg, fetcher);
  expect(report).toMatchObject({ admitted: 1, refused: 2, processed: 3 });
  const calls = readFileSync(cmsTrace, "utf8").trim().split("\n").map(line => JSON.parse(line));
  expect(calls.map(args => args[2])).toEqual(["--db", "music_work", "music_recording",
    "music_right", "music_credit", "media_program"]);
  const program = JSON.parse(calls.at(-1)[3]);
  expect(program.asset_ref).toMatch(/^\/assets\/twin\/[0-9a-f]{64}\.ogg$/);
  expect(program.publication_state).toBe("published");
});

test("a receipt/body mismatch refuses publication before any CMS entry", async () => {
  writeFileSync(badDigest, "1");
  writeFileSync(cmsTrace, "");
  const cfg = options(["--estate", root, "--items", items, "--out", receipt,
    "--project", project, "--cms-bin", cmsBin, "--apply"]);
  const report = await promote(cfg, fetcher);
  expect(report).toMatchObject({ admitted: 0, refused: 3 });
  expect(report.results[0]).toMatchObject({ state: "REFUSED", reason: "promotion_failed" });
  expect(report.results[0].detail).toContain("digest does not match");
  const calls = readFileSync(cmsTrace, "utf8").trim().split("\n").map(line => JSON.parse(line));
  expect(calls).toHaveLength(1);
});
