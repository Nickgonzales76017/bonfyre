# One application, several interfaces

Aurekai and Bonfÿre share application state across the website, native tools and compatible assistants. Each interface presents the work differently without reimplementing the underlying operation.

This guide uses the local documentation site to show how the same published page is opened in a browser, inspected from a terminal and read through MCP.

## Start with the website

The page belongs to the site's content store. A visitor does not need an API key, agent or developer account to read it.

```sh Browser
curl --resolve docs.localhost:8916:127.0.0.1 \
  -H 'Accept: text/html' \
  http://docs.localhost:8916/white-label-delivery
```

A published guide includes navigation, a page outline, code with copy controls, related pages and the correct site identity. The same route works in a regular browser.

## Inspect the site from the CLI

```sh Terminal
bonfyre site show aurekai-docs --json \
  --home ~/Developer/aurekai-profile-drafts/docs-native-20261011 --port 8916

bonfyre site mcp aurekai-docs --json \
  --home ~/Developer/aurekai-profile-drafts/docs-native-20261011 --port 8916

bonfyre site check aurekai-docs --json \
  --home ~/Developer/aurekai-profile-drafts/docs-native-20261011 --port 8916
```

These are operator views over the site's existing content and resolver, not separate publishing pipelines. The local `.localhost` address is a development address.

## Read it through MCP

The site's MCP endpoint provides structured tools including `site_info`, `list_pages`, `get_page`, `list_posts`, `search_posts`, `get_post` and `list_offers`. Compatible clients can inspect these tools without knowing how the website is rendered.

```sh List tools
curl -sS -X POST http://127.0.0.1:8916/mcp/aurekai-docs \
  -H 'content-type: application/json' \
  --data '{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}'
```

```sh Read a guide
curl -sS -X POST http://127.0.0.1:8916/mcp/aurekai-docs \
  -H 'content-type: application/json' \
  --data '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"get_page","arguments":{"route":"/white-label-delivery"}}}'
```

The first call discovers the interface. The second reads the same standing page that visitors see in the browser. Its route, title and body come from the site's source records.

## Let assistants use the right level of authority

Reading a page is different from submitting an enquiry or requesting a booking.

| Caller | Operation | What happens |
|---|---|---|
| Public visitor | Read a published page | The requested page is returned |
| Site reader | Search and inspect material | The tool responds within this site's content scope |
| Site writer | Submit an enquiry or booking request | A customer request is recorded for the site's owner |
| Agency operator | Manage customer sites and keys | Each customer remains an independently addressed subject |

Issue credentials through the site's own key interface:

```sh Terminal
bonfyre site key tallgrass --label workshop-reader
bonfyre site keys tallgrass
bonfyre site usage tallgrass
```

A credential for Tallgrass is not a credential for Juniper or Fernhollow. A read credential cannot perform a write operation. An independent customer's page should use its business name, icon and domain—not the Aurekai knight—unless it is explicitly an Aurekai-owned application.

## Browser-native tools and assistant UIs

The site publishes discovery metadata and can register read tools for compatible browser-native assistant integrations. Standard MCP clients receive tool descriptions and data; clients with richer interface support may present that same data as controls, tables or an operator view.

That is one content and action model, not separate implementations for ChatGPT, Claude, a website and the terminal. Rich controls must still respect the underlying site identity, permission and source record.

A workshop search result can become a passage viewer; a finance result can become a timeline; a migration result can become a source-to-destination comparison. Presentation changes are independent from whether the underlying action was authorized or executed.

## Build the right user experience

A website should excel at browsing, visual identity and customer-facing actions. A CLI should excel at composition, inspection and automation. An MCP interface should excel at structured discovery and scoped action. A partner workspace should make many authorized customer operations legible at once.

Share the native operation and its record owner. Do not force all four interfaces into the visual grammar of a chat transcript.

Continue with [white-label delivery](/white-label-delivery), [recording to workspace](/recording-to-workspace) or [request to books](/request-to-books) to follow a complete operation.
