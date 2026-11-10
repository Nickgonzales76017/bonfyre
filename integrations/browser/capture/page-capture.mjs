#!/usr/bin/env bun
// One reading of a page, kept as the different things a page is: the response
// as it arrived, the document after its scripts ran, where everything was laid
// out, what it exposes to assistive technology, and how it looked. They are
// bound to one capture identity, so a later reader can use the representation
// that answers its question without loading the page again -- and without
// asking a model to rediscover structure from a picture.
//
// Chrome runs headless in a throwaway profile with its sandbox on and is
// driven over the DevTools protocol on a local port it chooses itself.
//
//   usage: bun page-capture.mjs <url> <out-dir> [--wait-ms N]
import { createHash } from "node:crypto";
import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { findChrome, withChrome } from "./chrome-session.mjs";

export const SCHEMA = "bonfyre.page-capture.v1";
const sha = bytes => createHash("sha256").update(bytes).digest("hex");
export { findChrome };

export async function capturePage(address, outDir, { waitMs = 500, timeoutMs = 45_000, allowLocal = false } = {}) {
  const target = new URL(address);
  const local = ["127.0.0.1", "localhost", "[::1]"].includes(target.hostname);
  if (target.username || target.password) throw new Error("an address carrying credentials is not captured");
  if (!(target.protocol === "https:" || (allowLocal && local && target.protocol === "http:"))) throw new Error("only a public https address is captured");
  return withChrome(async ({ send, events, open }) => {
    const tab = await open();
    const { page, sessionId } = tab;
    await Promise.all([page("DOM.enable"), page("Accessibility.enable")]);
    const started = new Date().toISOString();
    const frameId = await tab.go(target.href);
    await Bun.sleep(waitMs);   // scripts that run after load get a bounded moment

    // The main document's own response, as the network delivered it.
    const arrived = events.filter(event => event.method === "Network.responseReceived" && event.params.type === "Document" && event.params.frameId === frameId).at(-1);
    let response = null, status = 0, contentType = "";
    if (arrived) {
      status = arrived.params.response.status; contentType = arrived.params.response.mimeType ?? "";
      const body = await page("Network.getResponseBody", { requestId: arrived.params.requestId }).catch(() => null);
      if (body) response = Buffer.from(body.body, body.base64Encoded ? "base64" : "utf8");
    }
    const dom = (await page("Runtime.evaluate", { expression: "document.documentElement.outerHTML", returnByValue: true })).result.value ?? "";
    const finalUrl = (await page("Runtime.evaluate", { expression: "location.href", returnByValue: true })).result.value ?? target.href;
    const layout = await page("DOMSnapshot.captureSnapshot", { computedStyles: ["display", "visibility"], includeDOMRects: true });
    const accessibility = await page("Accessibility.getFullAXTree");
    const picture = Buffer.from((await page("Page.captureScreenshot", { format: "png" })).data, "base64");
    const version = await send("Browser.getVersion");

    mkdirSync(outDir, { recursive: true });
    const kept = {};
    const keep = (name, bytes, what) => { writeFileSync(join(outDir, name), bytes); kept[name] = { sha256: sha(bytes), bytes: bytes.length, what }; };
    if (response) keep("response.body", response, "the main document as the network delivered it");
    keep("dom.html", Buffer.from(dom), "the document after its scripts ran");
    keep("layout.json", Buffer.from(JSON.stringify(layout)), "every node with its box, as positioned at 1280x900");
    keep("accessibility.json", Buffer.from(JSON.stringify(accessibility)), "roles, names and states exposed to assistive technology");
    keep("screenshot.png", picture, "the viewport as drawn");
    // The identity is what the page was, not when it was read or by which
    // browser session: the address it settled on, the response and the document.
    // Layout, the accessibility tree and the picture are views of that page
    // carrying session-local node numbers and pixels, so they are bound to the
    // identity and do not define it.
    const capture_id = "pc-" + sha(JSON.stringify([finalUrl, kept["response.body"]?.sha256 ?? "", kept["dom.html"].sha256])).slice(0, 32);
    const record = { schema: SCHEMA, capture_id, requested_url: target.href, final_url: finalUrl, http_status: status, content_type: contentType, captured_at: started,
      browser: version.product, viewport: [1280, 900], representations: kept,
      accessibility_nodes: accessibility.nodes?.length ?? 0, layout_documents: layout.documents?.length ?? 0,
      response_differs_from_dom: response ? sha(response) !== sha(Buffer.from(dom)) : null,
      cannot_infer: ["a captured page -> a true statement about what it describes", "a label on the page (paid, in stock, approved) -> that state verified with whoever owns it",
        "this capture -> what a signed-in reader sees; it was read as a stranger", "a matching capture_id -> the page unchanged for another reader, viewport or moment"] };
    writeFileSync(join(outDir, "capture.json"), JSON.stringify(record, null, 1));
    return record;
  }, { timeoutMs });
}

if (import.meta.main) {
  const [address, outDir] = process.argv.slice(2);
  if (!address || !outDir) { console.error("usage: bun page-capture.mjs <url> <out-dir> [--wait-ms N]"); process.exit(2); }
  const at = process.argv.indexOf("--wait-ms");
  try { console.log(JSON.stringify(await capturePage(address, outDir, { waitMs: at > 0 ? Number(process.argv[at + 1]) : 500, allowLocal: process.env.BONFYRE_CAPTURE_ALLOW_LOCAL === "1" }), null, 1)); }
  catch (why) { console.error(String(why.message ?? why)); process.exit(1); }
}
