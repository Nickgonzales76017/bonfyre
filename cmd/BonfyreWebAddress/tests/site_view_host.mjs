// A host for a site's MCP Apps view, built on the protocol's own host library
// (AppBridge from @modelcontextprotocol/ext-apps).  It puts the view in a
// sandboxed frame, completes the handshake the library requires, and delivers
// a tool result the way an assistant host does.  What the frame then shows is
// the view as a conforming host would draw it.  Bundle with:
//   bun build site_view_host.mjs --target browser --outfile host.js
import { AppBridge, PostMessageTransport } from "@modelcontextprotocol/ext-apps/app-bridge";

const { view, result, theme } = window.__SITE_VIEW__;
const log = document.getElementById("log");
const note = (text) => { log.textContent += text + "\n"; };
const frame = document.getElementById("view");
frame.srcdoc = view;
frame.addEventListener("load", async () => {
  try {
    const bridge = new AppBridge(null, { name: "bonfyre-view-host", version: "1" },
      { openLinks: {}, serverTools: {}, logging: {} },
      { hostContext: { theme, displayMode: "inline", platform: "web" } });
    bridge.onsizechange = ({ height }) => { if (height) frame.style.height = Math.min(height + 4, 1400) + "px"; note("size-changed " + height); };
    bridge.oninitialized = async () => {
      note("view initialized");
      await bridge.sendToolInput({ arguments: { ids: result.structuredContent.posts.map(p => p.id) } });
      await bridge.sendToolResult(result);
      note("tool result delivered: " + result.structuredContent.posts.length + " post(s)");
      document.body.setAttribute("data-done", "1");
    };
    await bridge.connect(new PostMessageTransport(frame.contentWindow, frame.contentWindow));
    note("bridge connected");
  } catch (error) { note("host error: " + (error && error.message || error)); document.body.setAttribute("data-done", "error"); }
}, { once: true });
