// One headless Chrome, in a throwaway profile with its sandbox on, driven over
// the DevTools protocol on a port it chooses itself. Shared by every organ
// that borrows an engine from the installed browser.
import { existsSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

export const findChrome = () => process.env.BONFYRE_CHROME ?? ["/Applications/Google Chrome.app/Contents/MacOS/Google Chrome", "/Applications/Chromium.app/Contents/MacOS/Chromium",
  Bun.which("chromium") ?? "", Bun.which("google-chrome") ?? ""].find(path => path && existsSync(path));

// Only the child spawned by this helper is eligible for termination. Downloads
// or leave-page guards must not keep an isolated QA browser alive indefinitely.
export async function stopOwnedChrome(proc, {graceMs=1500, killMs=5000}={}) {
  const exited=proc.exited;
  const within=async ms=>{let timer;try{return await Promise.race([Promise.resolve(exited).then(()=>true),new Promise(resolve=>{timer=setTimeout(()=>resolve(false),ms);})]);}finally{clearTimeout(timer);}};
  if(proc.exitCode!==null||proc.signalCode){await exited;return;}
  proc.kill();
  if(await within(graceMs))return;
  proc.kill('SIGKILL');
  if(!await within(killMs))throw Error('The isolated browser did not confirm shutdown; its temporary profile was retained.');
}

export async function withChrome(work, { timeoutMs = 45_000, flags = [], window = "1280,900", prepare, profile: kept } = {}) {
  const chrome = findChrome();
  if (!chrome) throw new Error("no Chrome or Chromium here");
  // A caller that needs the browser to remember something across restarts
  // (stored files, a service worker) owns the profile and passes it in.
  const profile = kept ?? mkdtempSync(join(tmpdir(), "bonfyre-chrome-"));
  if (kept) rmSync(join(kept, "DevToolsActivePort"), { force: true });
  // Anything the browser must find in its profile at start (a native-messaging
  // host registration, say) is put there before it launches.
  if (prepare) await prepare(profile);
  const proc = Bun.spawn([chrome, "--headless=new", `--user-data-dir=${profile}`, "--remote-debugging-port=0", "--no-first-run", "--no-default-browser-check",
    "--disable-background-networking", `--window-size=${window}`, ...flags, "about:blank"], { stdout: "ignore", stderr: "ignore" });
  const deadline = Date.now() + timeoutMs;
  let socket;
  const waiting = new Map(), events = [];
  const rejectPending = () => {
    for (const { timer, failed } of waiting.values()) { clearTimeout(timer); failed(new Error("the browser connection closed")); }
    waiting.clear();
  };
  try {
    // Chrome writes the port it chose, and the browser endpoint, into its profile.
    const portFile = join(profile, "DevToolsActivePort");
    // The file appears before it is filled, so it is read until both lines are there.
    let port, path;
    for (;;) {
      if (existsSync(portFile)) { [port, path] = readFileSync(portFile, "utf8").trim().split("\n"); if (port && path) break; }
      if (Date.now() > deadline || proc.exitCode !== null) throw new Error("the browser did not start");
      await Bun.sleep(50);
    }
    socket = new WebSocket(`ws://127.0.0.1:${port}${path}`);
    await new Promise((ready, failed) => { socket.onopen = ready; socket.onerror = () => failed(new Error("the browser refused the connection")); });
    let next = 1;
    socket.onclose = rejectPending;
    socket.onmessage = message => {
      const data = JSON.parse(message.data);
      if (data.id && waiting.has(data.id)) { const { done, failed, timer } = waiting.get(data.id); waiting.delete(data.id); clearTimeout(timer); data.error ? failed(new Error(data.error.message)) : done(data.result); }
      else if (data.method) events.push(data);
    };
    const send = (method, params = {}, sessionId) => new Promise((done, failed) => {
      const id = next++;
      const timer = setTimeout(() => { if (waiting.delete(id)) failed(new Error(`${method} did not answer`)); }, Math.max(1000, deadline - Date.now()));
      waiting.set(id, { done, failed, timer });
      try { socket.send(JSON.stringify({ id, method, params, ...(sessionId ? { sessionId } : {}) })); }
      catch (error) { waiting.delete(id); clearTimeout(timer); failed(error); }
    });
    const until = async (test, what = "the page did not finish loading") => { for (;;) { const hit = events.find(test); if (hit) return hit; if (Date.now() > deadline) throw new Error(what); await Bun.sleep(25); } };
    // A fresh tab, attached, with the domains every organ here uses.
    const open = async () => {
      const { targetId } = await send("Target.createTarget", { url: "about:blank" });
      const { sessionId } = await send("Target.attachToTarget", { targetId, flatten: true });
      const page = (method, params) => send(method, params, sessionId);
      await Promise.all([page("Page.enable"), page("Network.enable"), page("Runtime.enable")]);
      await page("Page.setLifecycleEventsEnabled", { enabled: true });
      const go = async url => {
        // Events can arrive before the navigate response. Keep that interval,
        // but never let a previous document satisfy this navigation.
        const start = events.length;
        const { frameId, loaderId, errorText, isDownload } = await page("Page.navigate", { url });
        if (errorText) throw new Error(`navigation failed: ${errorText}`);
        if (isDownload) throw new Error("navigation started a download instead of loading a document");
        await until((event, index) => index >= start && event.sessionId === sessionId && event.params?.frameId === frameId && (loaderId
          ? event.method === "Page.lifecycleEvent" && event.params.loaderId === loaderId && event.params.name === "load"
          : event.method === "Page.navigatedWithinDocument" && event.params.url === new URL(url).href), "the requested document did not finish loading");
        return frameId;
      };
      return { page, go, sessionId, targetId };
    };
    return await work({ send, until, events, open, deadline });
  } finally {
    rejectPending();
    try { socket?.close(); } catch {}
    await stopOwnedChrome(proc);
    if (!kept) rmSync(profile, { recursive: true, force: true });
  }
}
