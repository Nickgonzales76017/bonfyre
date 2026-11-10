// Drives a site's MCP endpoint with the protocol's own reference client, so
// what is checked is what a real assistant's client would accept: the
// handshake, every tool's result against the schema that tool declares, and
// the resources.  Usage: node site_mcp_client.mjs URL  (the SDK must be
// resolvable from the working directory).
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";

const url = process.argv[2];
const client = new Client({ name: "bonfyre-site-conformance", version: "1" });
await client.connect(new StreamableHTTPClientTransport(new URL(url)));
const say = (k, v) => console.log(`${k}\t${v}`);
say("server", `${client.getServerVersion().name} / ${client.getServerVersion().title}`);
say("instructions", (client.getInstructions() || "").length > 40 ? "present" : "missing");
const { tools } = await client.listTools();
say("tools", tools.map(t => t.name).join(","));
say("writers", tools.filter(t => t.annotations?.readOnlyHint !== true).map(t => t.name).join(",") || "none");
say("destructive", tools.some(t => t.annotations?.destructiveHint === true));
say("view_tool", tools.filter(t => t._meta?.ui?.resourceUri).map(t => `${t.name}=${t._meta.ui.resourceUri}`).join(","));
const info = await client.callTool({ name: "site_info", arguments: {} });
say("site_info", `${info.structuredContent.name}|${info.structuredContent.posts}`);
const list = await client.callTool({ name: "list_posts", arguments: { limit: 2 } });
say("list_posts", `${list.structuredContent.posts.length} of ${list.structuredContent.total}`);
const first = list.structuredContent.posts[0];
const post = await client.callTool({ name: "get_post", arguments: { id: first.id } });
say("get_post", post.structuredContent.title === first.title && post.structuredContent.body.length > first.excerpt.length - 2);
const found = await client.callTool({ name: "search_posts", arguments: { query: process.argv[3] || first.title.split(" ").slice(0, 2).join(" "), top: 3 } });
say("search_posts", `${found.structuredContent.method}|${found.structuredContent.results.length}`);
const shown = await client.callTool({ name: "show_posts", arguments: { ids: [first.id] } });
say("show_posts", `${shown.structuredContent.posts.length}|text:${shown.content[0].text.includes(first.title)}`);
const missing = await client.callTool({ name: "get_post", arguments: { id: 99999999 } });
say("missing_post", missing.isError === true);
const unkeyed = await client.callTool({ name: "send_inquiry", arguments: { name: "T", contact: "t@example.test", message: "hello there" } });
say("write_without_key_refused", unkeyed.isError === true);
const pages = await client.callTool({ name: "list_pages", arguments: {} });
say("list_pages", pages.structuredContent.pages.length);
const offers = await client.callTool({ name: "list_offers", arguments: {} });
say("list_offers", offers.structuredContent.offers.length);
const { resources } = await client.listResources();
say("resources", resources.length);
const view = await client.readResource({ uri: "ui://site/posts" });
say("view", `${view.contents[0].mimeType}|${view.contents[0].text.includes("ui/initialize")}|script-safe:${!/innerHTML|document\.write|eval\(/.test(view.contents[0].text)}`);
const doc = await client.readResource({ uri: first.uri });
say("post_resource", `${doc.contents[0].mimeType}|${doc.contents[0].text.startsWith("# " + first.title)}`);
let refused = false;
try { await client.callTool({ name: "delete_everything", arguments: {} }); } catch { refused = true; }
say("unknown_tool_refused", refused);
await client.close();
