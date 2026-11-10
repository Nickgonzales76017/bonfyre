/* A site's own tools, offered to an assistant running in the visitor's browser.
 *
 * Where the browser has WebMCP (document.modelContext), each read tool the
 * site already answers at /mcp is registered with it.  Every tool here calls
 * that same endpoint, so a browser assistant and a remote one are told the
 * same thing.  Nothing here writes: leaving a message needs a key the owner
 * issued, and a page is not where a key belongs.  Without WebMCP this does
 * nothing at all.
 */
(function () {
  var context = document.modelContext || (typeof navigator !== "undefined" && navigator.modelContext);
  if (!context || typeof context.registerTool !== "function") return;
  var call = function (name, args, signal) {
    return fetch("/mcp", {
      method: "POST", signal: signal,
      headers: { "content-type": "application/json", accept: "application/json" },
      body: JSON.stringify({ jsonrpc: "2.0", id: Date.now(), method: "tools/call", params: { name: name, arguments: args || {} } })
    }).then(function (r) { return r.json(); }).then(function (j) {
      if (j.error) return "The site could not answer: " + j.error.message;
      var first = j.result && j.result.content && j.result.content[0];
      return first && first.text ? first.text : "The site returned nothing.";
    });
  };
  fetch("/mcp", { method: "POST", headers: { "content-type": "application/json", accept: "application/json" },
    body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "tools/list" }) })
    .then(function (r) { return r.json(); })
    .then(function (j) {
      (j.result && j.result.tools || []).forEach(function (tool) {
        if (!tool.annotations || tool.annotations.readOnlyHint !== true) return;
        if (tool._meta && tool._meta.ui) return;                /* a view is a host's to show, not a page's */
        context.registerTool({
          name: tool.name, description: tool.description, inputSchema: tool.inputSchema,
          annotations: { readOnlyHint: true, untrustedContentHint: false, consequentialHint: false },
          execute: function (args, options) { return call(tool.name, args, options && options.signal); }
        });
      });
    }).catch(function () {});
})();
