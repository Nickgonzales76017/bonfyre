import { test, expect } from "bun:test";
import { createHash } from "node:crypto";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { capturePage, findChrome, SCHEMA } from "../capture/page-capture.mjs";

// A page whose script changes it after it arrives: the response and the
// document are then different things, which is the point of keeping both.
const html = `<!doctype html><html><head><title>Offer</title></head><body>
<h1>Tinned sardines</h1><p id="price">Price: <span>$9.50</span></p>
<button id="buy" aria-label="Add sardines to basket">Add</button>
<script>const p=document.createElement("p");p.id="late";p.textContent="Only 3 left";document.body.append(p);</script>
</body></html>`;

test.skipIf(!findChrome())("one reading keeps the response, the document, the layout, the accessibility tree and the picture under one identity", async () => {
  const server = Bun.serve({ port: 0, hostname: "127.0.0.1", fetch: () => new Response(html, { headers: { "content-type": "text/html; charset=utf-8" } }) });
  const out = mkdtempSync(join(tmpdir(), "bonfyre-page-capture-"));
  try {
    const url = `http://127.0.0.1:${server.port}/offer`;
    const record = await capturePage(url, join(out, "a"), { allowLocal: true });
    expect(record.schema).toBe(SCHEMA);
    expect(record.http_status).toBe(200);
    expect(Object.keys(record.representations).sort()).toEqual(["accessibility.json", "dom.html", "layout.json", "response.body", "screenshot.png"]);
    for (const [name, row] of Object.entries(record.representations))
      expect(createHash("sha256").update(readFileSync(join(out, "a", name))).digest("hex")).toBe(row.sha256);
    // The response is what arrived; the document is what the script made of it.
    const response = readFileSync(join(out, "a", "response.body"), "utf8"), dom = readFileSync(join(out, "a", "dom.html"), "utf8");
    expect(response).toBe(html);
    expect(response).not.toContain('<p id="late">');
    expect(dom).toContain('<p id="late">Only 3 left</p>');
    expect(record.response_differs_from_dom).toBe(true);
    // Structure a reader can use without looking at the picture.
    const tree = JSON.parse(readFileSync(join(out, "a", "accessibility.json"), "utf8")).nodes;
    const button = tree.find(node => node.role?.value === "button");
    expect(button.name.value).toBe("Add sardines to basket");
    expect(tree.some(node => node.role?.value === "heading" && node.name?.value === "Tinned sardines")).toBe(true);
    const layout = JSON.parse(readFileSync(join(out, "a", "layout.json"), "utf8"));
    expect(layout.documents[0].layout.bounds.length).toBeGreaterThan(5);
    expect(layout.strings).toContain("Only 3 left");
    expect(readFileSync(join(out, "a", "screenshot.png")).subarray(1, 4).toString()).toBe("PNG");
    // The same page read again is the same capture; the identity is not a timestamp.
    const again = await capturePage(url, join(out, "b"), { allowLocal: true });
    for (const name of ["response.body", "dom.html"]) expect(again.representations[name].sha256).toBe(record.representations[name].sha256);
    expect(again.capture_id).toBe(record.capture_id);
    expect(again.captured_at).not.toBe("");
  } finally { server.stop(true); rmSync(out, { recursive: true, force: true }); }
}, 120_000);

test("an address that is not public https, or carries credentials, is not captured", async () => {
  for (const url of ["http://example.com/", "file:///etc/passwd", "https://user:pw@example.com/", "http://127.0.0.1:1/"])
    await expect(capturePage(url, "/tmp/never")).rejects.toThrow();
});
