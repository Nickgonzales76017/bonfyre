import { test, expect } from "bun:test";
import { mkdtempSync, readFileSync, rmSync, statSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const binary = process.argv[2] ?? join(import.meta.dir, "..", "bonfyre-project");
test("native ForeignTwin range sample is robots-governed, bounded and receipt-bound", async () => {
  const dir = mkdtempSync(join(tmpdir(), "bonfyre-range-sample-"));
  const payload = Buffer.alloc(3_000_000, 65);
  const server = Bun.serve({ port: 0, fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === "/robots.txt") return new Response("User-agent: *\nDisallow: /blocked\n", { status: 200 });
    if (url.pathname === "/html") return new Response("<!doctype html><html><body><main><h1>Observed</h1></main></body></html>", { headers: { "content-type": "text/html" } });
    if (url.pathname === "/redirect") return Response.redirect(new URL('/html', request.url), 302);
    if (url.pathname === "/audio" || url.pathname === "/blocked") {
      const range = request.headers.get("range") ?? "";
      const match = /^bytes=(\d+)-(\d+)$/.exec(range);
      if (match) {
        const start = Number(match[1]), end = Math.min(Number(match[2]), payload.length - 1);
        return new Response(payload.subarray(start, end + 1), { status: 206,
          headers: { "content-range": `bytes ${start}-${end}/${payload.length}` } });
      }
      return new Response(payload);
    }
    return new Response("missing", { status: 404 });
  } });
  try {
    const base = `http://127.0.0.1:${server.port}`;
    const receiptPath = join(dir, "sample.json"), bodyPath = join(dir, "sample.body");
    const run = async (twin, url, receipt, body) => {
      const proc = Bun.spawn([binary, "foreign", "observe", twin, url, "--receipt", receipt, "--body", body,
        "--observe-right", "public", "--max-bytes", "1200000", "--range-start", "200000"], { stdout: "pipe", stderr: "pipe" });
      const code = await proc.exited;
      return { code, stderr: await new Response(proc.stderr).text() };
    };
    const ok = await run("audio-sample", `${base}/audio`, receiptPath, bodyPath);
    expect(ok.code).toBe(0);
    const receipt = JSON.parse(readFileSync(receiptPath, "utf8").trim().split("\n").at(-1));
    expect(receipt).toMatchObject({ policy_decision: "allow", http_status: 206,
      observation_kind: "sample_observed", body_bytes: 1200000 });
    expect(statSync(bodyPath).size).toBe(1200000);
    expect(readFileSync(bodyPath).equals(payload.subarray(200000, 1400000))).toBe(true);
    const deniedPath = join(dir, "denied.json"), deniedBody = join(dir, "denied.body");
    const denied = await run("blocked-sample", `${base}/blocked`, deniedPath, deniedBody);
    expect(denied.code).toBe(0);
    expect(JSON.parse(readFileSync(deniedPath, "utf8").trim()).policy_decision).toBe("robots_disallow");
    const fullReceipt = join(dir, "full.json"), fullBody = join(dir, "full.body");
    const full = Bun.spawn([binary, "foreign", "observe", "html-full", `${base}/html`, "--receipt", fullReceipt, "--body", fullBody,
      "--observe-right", "public", "--full-max-bytes", "2000000"], { stdout: "pipe", stderr: "pipe" });
    expect(await full.exited).toBe(0);
    expect(JSON.parse(readFileSync(fullReceipt, "utf8").trim()).observation_kind).toBe("source_observed");
    expect(readFileSync(fullBody, "utf8")).toContain("<h1>Observed</h1>");
    const oversized = Bun.spawn([binary, "foreign", "observe", "oversized", `${base}/audio`, "--receipt", join(dir, "oversized.json"),
      "--body", join(dir, "oversized.body"), "--observe-right", "public", "--full-max-bytes", "200"], { stdout: "pipe", stderr: "pipe" });
    expect(await oversized.exited).not.toBe(0);
    const redirected = Bun.spawn([binary, "foreign", "observe", "redirected", `${base}/redirect`, "--receipt", join(dir, "redirect.json"),
      "--body", join(dir, "redirect.body"), "--observe-right", "public", "--full-max-bytes", "2000000"], { stdout: "pipe", stderr: "pipe" });
    expect(await redirected.exited).not.toBe(0);
    expect(JSON.parse(readFileSync(join(dir, "redirect.json"), "utf8").trim()).http_status).toBe(302);
  } finally { server.stop(true); rmSync(dir, { recursive: true, force: true }); }
});
