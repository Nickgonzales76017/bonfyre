import { test, expect } from "bun:test";
import { createHash } from "node:crypto";
import { mkdtempSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { bindObservation, appendOnce } from "../tools/domain_observation_bridge.mjs";

const html = `<html><head><title>OGC API Common</title>
<meta name="description" content="Shared API building blocks">
<meta property="og:site_name" content="Open Geospatial Consortium">
<meta property="og:url" content="https://example.test/ogc-common">
<meta property="og:updated_time" content="2026-04-30T02:27:55+00:00">
</head><body>standard</body></html>`;
const digest = bytes => createHash("sha256").update(bytes).digest("hex");

function fixture() {
  const root = mkdtempSync(join(tmpdir(), "bonfyre-domain-observation-"));
  const body = join(root, "body.html"), receipts = join(root, "evidence.jsonl"), ingest = join(root, "ingest");
  writeFileSync(body, html);
  const observation = { schema: "bonfyre-foreign-observation.v1", twin_id: "external:protocol-test",
    source_url: "https://example.test/ogc-common", observation_kind: "source_observed", http_status: 200,
    body_path: body, content_sha256: digest(html) };
  writeFileSync(receipts, `${JSON.stringify(observation)}\n`);
  mkdirSync(ingest);
  writeFileSync(join(ingest, "normalized.txt"), html);
  const manifest = { status: "ready", source_content_hash: digest(html),
    normalized_path: "normalized.txt", normalized_content_hash: digest(html) };
  writeFileSync(join(ingest, "intake-manifest.json"), JSON.stringify(manifest));
  return { root, body, receipts, ingest, manifest };
}

test("binds a checked semantic record and appends once", async () => {
  const { root, receipts, ingest } = fixture();
  const bound = await bindObservation("external:protocol-test", receipts, ingest);
  // Golden digest was compared against the retired DomainProfile Python binder.
  expect(bound.semantic_record_sha256).toBe("df545c0a9f785a334b967e125537c9dc3f81e378c67c997285730f1e60afc3ac");
  expect(bound.binding_validated).toBe(true);
  expect(bound.owner).toBe("DomainProfile");
  expect(bound.from_owner).toBe("ForeignTwin");
  expect(bound.state).toBe("profile_bound");
  expect(bound.semantic_fields).toEqual(["authority", "description", "name", "source_url", "updated_at"]);
  const output = join(root, "out.jsonl");
  expect(appendOnce(output, bound)).toBe(true);
  expect(appendOnce(output, bound)).toBe(false);
  expect(readFileSync(output, "utf8").trim().split("\n")).toHaveLength(1);
});

test("rejects body drift", async () => {
  const { body, receipts, ingest } = fixture();
  writeFileSync(body, `${html}tamper`);
  expect(bindObservation("external:protocol-test", receipts, ingest)).rejects.toThrow("ForeignTwin receipt");
});

test("rejects stale ingest source lineage", async () => {
  const { receipts, ingest, manifest } = fixture();
  manifest.source_content_hash = "0".repeat(64);
  writeFileSync(join(ingest, "intake-manifest.json"), JSON.stringify(manifest));
  expect(bindObservation("external:protocol-test", receipts, ingest)).rejects.toThrow("ingest source hash");
});

test("rejects normalized body drift", async () => {
  const { receipts, ingest } = fixture();
  writeFileSync(join(ingest, "normalized.txt"), `${html}tamper`);
  expect(bindObservation("external:protocol-test", receipts, ingest)).rejects.toThrow("normalized artifact");
});

test("rejects an observation for a different occurrence", async () => {
  const { receipts, ingest } = fixture();
  expect(bindObservation("external:other", receipts, ingest)).rejects.toThrow("no retained ForeignTwin observation");
});

test("uses observed URL and HTML title when Open Graph metadata is absent", async () => {
  const { receipts, ingest, manifest } = fixture();
  const fallback = `<html><head><title>School &amp; Community</title>
    <meta property="og:site_name" content="District">
    <meta name="description" content="Public program"></head></html>`;
  writeFileSync(join(ingest, "normalized.txt"), fallback);
  manifest.normalized_content_hash = digest(fallback);
  writeFileSync(join(ingest, "intake-manifest.json"), JSON.stringify(manifest));
  const receipt = await bindObservation("external:protocol-test", receipts, ingest);
  expect(receipt.semantic_fields).toEqual(["authority", "description", "name", "source_url"]);
  expect(receipt.source_url).toBe("https://example.test/ogc-common");
});

test("refuses an unidentifiable authority", async () => {
  const { receipts, ingest, manifest } = fixture();
  const insufficient = `<html><head><title>Untitled source</title>
    <meta name="description" content="A description"></head></html>`;
  writeFileSync(join(ingest, "normalized.txt"), insufficient);
  manifest.normalized_content_hash = digest(insufficient);
  writeFileSync(join(ingest, "intake-manifest.json"), JSON.stringify(manifest));
  expect(bindObservation("external:protocol-test", receipts, ingest)).rejects.toThrow("authority");
});
