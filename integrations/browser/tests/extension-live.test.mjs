// The extension itself, loaded unpacked into a real browser, talking to the
// real native host over Chrome's own native messaging, with the host calling
// the real owners. Nothing here is a stand-in: the panel's own script runs,
// the browser starts the host from a registration in a throwaway profile, and
// what the panel says is read off its page.
//
// Branded Chrome refuses an unpacked extension from the command line, so this
// needs Chrome for Testing or Chromium (puppeteer installs the former). It
// reads https://example.com/ and skips when no such browser is here.
import { test, expect } from "bun:test";
import { chmodSync, existsSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { homedir, tmpdir } from "node:os";
import { join } from "node:path";
import { createHash } from "node:crypto";
import { withChrome } from "../capture/chrome-session.mjs";

const repo = join(import.meta.dir, "../../..");
const extension = join(repo, "integrations/browser/extension");
const project = join(repo, "components/BonfyreProject/bonfyre-project"), queue = join(repo, "cmd/BonfyreQueue/bonfyre-queue");
function testingBrowser() {
  if (process.env.BONFYRE_TEST_BROWSER) return process.env.BONFYRE_TEST_BROWSER;
  const cache = join(homedir(), ".cache/puppeteer/chrome");
  for (const version of existsSync(cache) ? readdirSync(cache).sort().reverse() : []) {
    const path = join(cache, version, "chrome-mac-arm64/Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing");
    if (existsSync(path)) return path;
  }
  return ["/Applications/Chromium.app/Contents/MacOS/Chromium", Bun.which("chromium") ?? ""].find(path => path && existsSync(path)) ?? "";
}
const browser = testingBrowser();
// An unpacked extension's identity is derived from where it sits on disk.
const id = [...createHash("sha256").update(extension).digest("hex").slice(0, 32)].map(c => String.fromCharCode(97 + parseInt(c, 16))).join("");

test.skipIf(!browser || !existsSync(project) || !existsSync(queue))("the panel, in a real browser, asks the real host and says what the owners answered", async () => {
  const root = mkdtempSync(join(tmpdir(), "bonfyre-extension-live-"));
  const before = process.env.BONFYRE_CHROME;
  process.env.BONFYRE_CHROME = browser;
  try {
    mkdirSync(join(root, "receipts"), { recursive: true });
    const launcher = join(root, "host.sh");
    // The browser starts the host with an empty environment; everything it reads is stated.
    writeFileSync(launcher, `#!/bin/sh\nexport PATH="${process.env.PATH}" HOME="${homedir()}" BONFYRE_FOREIGN_ROOT="${root}" BONFYRE_FOREIGN_RECEIPTS="${root}/receipts" BONFYRE_BROWSER_QUEUE_DB="${root}/held.db" BONFYRE_PROJECT_BINARY="${project}" BONFYRE_QUEUE_BINARY="${queue}" BONFYRE_CHROME="${browser}"\nexec "${process.execPath}" "${join(repo, "integrations/browser/host/bonfyre-browser-host.mjs")}"\n`);
    chmodSync(launcher, 0o755);
    const prepare = profile => {
      mkdirSync(join(profile, "NativeMessagingHosts"), { recursive: true });
      writeFileSync(join(profile, "NativeMessagingHosts/com.bonfyre.browser_host.json"), JSON.stringify({ name: "com.bonfyre.browser_host", description: "test", path: launcher, type: "stdio", allowed_origins: [`chrome-extension://${id}/`] }));
    };
    await withChrome(async ({ send, open }) => {
      // The browser derived the same identity for the unpacked extension.
      let worker;
      for (let i = 0; i < 100 && !worker; i++) { worker = (await send("Target.getTargets")).targetInfos.find(info => info.type === "service_worker" && info.url === `chrome-extension://${id}/background.js`); if (!worker) await Bun.sleep(100); }
      expect(worker).toBeDefined();
      const panel = await open();
      await panel.go(`chrome-extension://${id}/panel.html`);
      const evaluate = async expression => {
        const answer = await panel.page("Runtime.evaluate", { expression, awaitPromise: true, returnByValue: true });
        if (answer.exceptionDetails) throw new Error(answer.exceptionDetails.exception?.description ?? answer.exceptionDetails.text);
        return answer.result.value;
      };
      const text = id => evaluate(`document.getElementById(${JSON.stringify(id)}).textContent`);
      const until = async (what, test) => { for (let i = 0; i < 400; i++) { const value = await what(); if (test(value)) return value; await Bun.sleep(100); } throw new Error("the panel never said it: " + await what()); };
      const click = id => evaluate(`document.getElementById(${JSON.stringify(id)}).click()`);

      // A page nothing was kept from: the panel says so and offers to keep it.
      await evaluate(`show("https://example.com/")`);
      expect(await until(() => text("kept"), value => value !== "")).toBe("Nothing has been kept from this page.");
      expect(await evaluate(`[keep.disabled, picture.disabled, hold.disabled]`)).toEqual([false, false, false]);

      // The reader's click keeps a copy through the real owner, and the history then shows it.
      await click("keep");
      expect(await until(() => text("said"), value => value !== "" && !value.startsWith("Reading"))).toContain("A copy was kept.");
      expect(await text("kept")).toBe("Kept once.");
      expect(await evaluate(`document.querySelectorAll("#history li").length`)).toBe(1);
      expect(await evaluate(`document.querySelector("#history li").textContent`)).toContain("a copy was kept");
      expect(readdirSync(join(root, "receipts")).length).toBe(1);
      expect(readdirSync(join(root, "bodies")).length).toBe(1);

      // The reader's click keeps how the page looks, through the capture organ in a second browser.
      await click("picture");
      expect(await until(() => text("said"), value => value.startsWith("Kept:") || value.startsWith("That could not") || value.startsWith("Aurekai is not"))).toContain("what it tells a screen reader");
      const readings = readdirSync(join(root, "captures", "example.com"));
      expect(readings).toHaveLength(1);
      const record = JSON.parse(readFileSync(join(root, "captures", "example.com", readings[0], "capture.json"), "utf8"));
      expect(Object.keys(record.representations).sort()).toEqual(["accessibility.json", "dom.html", "layout.json", "response.body", "screenshot.png"]);
      expect(readFileSync(join(root, "captures", "example.com", readings[0], "dom.html"), "utf8")).toContain("Example Domain");

      // A selection is held through the same port the panel uses. (The panel's own
      // button first asks the reader to grant the site, which nothing can click here.)
      const held = await evaluate(`ask("hold", { url: "https://example.com/", selection: "This domain is for use in documentation examples", note: "live test", confirmed: true })`);
      expect(held).toMatchObject({ ok: true, result: { native_owner: "BonfyreQueue", family: "held-selection" } });
      const node = JSON.parse(Bun.spawnSync([queue, "mission", held.result.mission, "--db", join(root, "held.db")]).stdout.toString());
      expect(JSON.parse(node.input_uri)).toMatchObject({ url: "https://example.com/", note: "live test" });
      expect(await evaluate(`ask("hold", { url: "https://example.com/", selection: "x" })`)).toMatchObject({ ok: false, refused: "not_confirmed" });

      // A page that is not public is refused in the reader's words, and nothing is offered.
      await evaluate(`show("http://127.0.0.1:1/private")`);
      expect(await until(() => text("kept"), value => value !== "")).toBe("This is not a public page, so nothing is kept from it.");
      expect(await evaluate(`[keep.disabled, picture.disabled, hold.disabled]`)).toEqual([true, true, true]);

      // The panel follows the tab the reader is on.
      const other = await open();
      await other.go("https://example.com/");
      await send("Target.activateTarget", { targetId: other.targetId });
      await evaluate(`current()`);
      expect(await until(() => text("where"), value => value.startsWith("https://"))).toBe("https://example.com/");
      expect(await until(() => text("kept"), value => value !== "")).toBe("Kept once.");
    }, { flags: [`--load-extension=${extension}`, `--disable-extensions-except=${extension}`], prepare, timeoutMs: 150_000 });
  } finally {
    if (before === undefined) delete process.env.BONFYRE_CHROME; else process.env.BONFYRE_CHROME = before;
    rmSync(root, { recursive: true, force: true });
  }
}, 180_000);
