// One connection to the native host for as long as the panel is open: related
// questions about a page are one session, not a process each.
const where = document.getElementById("where"), kept = document.getElementById("kept");
const history = document.getElementById("history"), keep = document.getElementById("keep"), said = document.getElementById("said");
const picture = document.getElementById("picture"), hold = document.getElementById("hold"), note = document.getElementById("note");
let tabId = null;
let page = "";
const nativeSession = import('./native-session.mjs').then(module => module.createNativeSession(chrome.runtime));

async function ask(operation, more = {}) {
  try {
    return await (await nativeSession).ask(operation, more);
  } catch { return { ok: false, refused: 'disconnected', reason: 'Native session unavailable; reload before retrying a change.' }; }
}
// Other panel sections share this one session with the host.
window.aurekaiAsk = ask;
const when = stamp => stamp ? stamp.replace("T", " ").replace(/:\d\dZ$/, "") + " UTC" : "an unknown time";
const refusal = answer => answer.refused === "disconnected" ? "Aurekai is not reachable from this browser. " + (answer.reason || "")
  : answer.refused === "not_public_https" ? "This is not a public page, so nothing is kept from it."
  : answer.refused === "session_spent" ? "This panel has asked as much as one sitting allows. Close and reopen it."
  : "That could not be done: " + (answer.reason || answer.refused);

async function show(address) {
  page = address || "";
  where.textContent = page || "No page yet.";
  kept.textContent = ""; said.textContent = ""; history.replaceChildren();
  keep.disabled = picture.disabled = hold.disabled = true;
  if (!page) return;
  const answer = await ask("history", { url: page });
  if (address !== page) return;            // the reader moved on while this was asked
  if (!answer.ok) { kept.textContent = refusal(answer); return; }
  keep.disabled = picture.disabled = hold.disabled = false;
  const seen = answer.result?.result;
  const rows = Array.isArray(seen?.history) ? seen.history : [];
  if (!rows.length) { kept.textContent = "Nothing has been kept from this page."; return; }
  kept.textContent = rows.length === 1 ? "Kept once." : `Kept ${rows.length} times. Most recent last.`;
  for (const row of rows.slice(-8)) {
    const line = document.createElement("li");
    line.textContent = `${when(row.observed_at)}: ` + (row.retained ? "a copy was kept" : "it was read and no copy was kept")
      + (row.http_status && row.http_status !== 200 ? `; the site answered ${row.http_status}` : "");
    history.append(line);
  }
  if (seen.content_still_reachable === false) said.textContent = "The page did not answer the last time it was read.";
}

keep.addEventListener("click", async () => {
  const address = page;
  keep.disabled = true;
  said.textContent = "Reading the page…";
  // The click is the person asking; the host runs this only when told so.
  const answer = await ask("observe", { url: address, confirmed: true });
  if (address !== page) return;
  if (!answer.ok) { said.textContent = refusal(answer); keep.disabled = false; return; }
  const retention = answer.result?.retention ?? {};
  await show(address);
  said.textContent = retention.retained ? "A copy was kept. It records what the page said when it was read, not whether that is true."
    : "The page was read and no copy was kept" + (retention.reason ? `: ${retention.reason}.` : ".");
});

picture.addEventListener("click", async () => {
  const address = page;
  picture.disabled = true;
  said.textContent = "Opening the page in a separate browser to read it…";
  const answer = await ask("capture", { url: address, confirmed: true });
  if (address !== page) return;
  picture.disabled = false;
  said.textContent = !answer.ok ? refusal(answer)
    : "Kept: the page as it arrived, as it stood after loading, where everything sat, what it tells a screen reader, and a picture. "
      + "It was read as a stranger would see it, not as you are signed in.";
});

// Reading a selection means reading the page, so the reader is asked for that
// site, at the moment they ask for the selection, and for no other site.
hold.addEventListener("click", async () => {
  const address = page;
  let origin;
  try { origin = new URL(address).origin + "/*"; } catch { return; }
  const allowed = await chrome.permissions.request({ origins: [origin] }).catch(() => false);
  if (!allowed) { said.textContent = "Nothing was read from the page."; return; }
  let selection = "";
  try {
    const [{ result }] = await chrome.scripting.executeScript({ target: { tabId }, func: () => String(window.getSelection?.() ?? "") });
    selection = result ?? "";
  } catch (why) { said.textContent = "The selection could not be read from this page."; return; }
  if (!selection.trim()) { said.textContent = "Select some text on the page first."; return; }
  const answer = await ask("hold", { url: address, selection, note: note.value, confirmed: true });
  if (address !== page) return;
  said.textContent = answer.ok ? "Held. It is waiting to be taken up; nothing has been sent, booked or decided." : refusal(answer);
  if (answer.ok) note.value = "";
});

async function current() {
  const [tab] = await chrome.tabs.query({ active: true, lastFocusedWindow: true });
  tabId = tab?.id ?? null;
  show(tab?.url ?? "");
}
chrome.tabs.onActivated.addListener(current);
chrome.tabs.onUpdated.addListener((_, change, tab) => { if (change.url && tab.active) current(); });
current();
