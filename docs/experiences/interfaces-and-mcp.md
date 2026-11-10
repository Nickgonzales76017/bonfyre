# One application, several interfaces

Aurekai and Bonfÿre share the same records across the website, the command line and compatible assistants. Each interface presents the work differently without reimplementing the underlying operation.

This guide uses the local documentation site to show how the same published page is opened in a browser, inspected from a terminal and read through MCP.

## Start with the website

The [source and program workspace](/source-program-workspace) presents retained pages, source comparisons, dated cycle actions and a readable proposal. Users select and act on those records instead of editing the compiler's serialized input. A technical file export remains available when another system needs it.

The public workspace's drafts are browser-local. They are shared with its full studio on the same origin, but are not automatically the native CMS's records. Native handoff, organization access and execution each need their own authorized connection.

The installed source-reuse command is another supported entry point:

```sh Terminal
$HOME/.bonfyre/bin/bonfyre-runtime representation doctor
```

The Aurekai connector reaches it through the existing `BonfyreRuntime` owner. [Native source reuse](/native-source-reuse) explains explicit estate and table selection, read-only verification and the boundary between reusable material and completed work.

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

These are other views of the site's existing content, not separate publishing pipelines. The local `.localhost` address is a development address.

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

## Give assistants only the access they need

Reading a page is different from submitting an enquiry or requesting a booking.

| Caller | Operation | What happens |
|---|---|---|
| Public visitor | Read a published page | The requested page is returned |
| Site reader | Search and inspect material | The tool responds within this site's content scope |
| Site writer | Submit an enquiry or booking request | A customer request is recorded for the site's owner |
| Agency operator | Manage customer sites and keys | Each customer stays separate, under its own address |

Issue credentials through the site's own key interface:

```sh Terminal
bonfyre site key tallgrass --label workshop-reader
bonfyre site keys tallgrass
bonfyre site usage tallgrass
```

A credential for Tallgrass is not a credential for Juniper or Fernhollow. A read credential cannot perform a write operation. An independent customer's page should use its business name, icon and domain—not the Aurekai knight—unless it is explicitly an Aurekai-owned application.

## A second example: a studio's published programmes

The training app's coaches publish programmes. A studio can let an assistant read the published ones, and only those: the tool is read-only, returns the latest published version of each programme, and is off until the studio is named in the server's settings.

```sh Terminal
bun test integrations/mcp/tests/training_public_programs.test.mjs
```

That test walks the whole path on throwaway data: a member asks for a programme, a coach drafts and delivers it, the coach publishes it, the public catalogue shows it, and the assistant's tool reads the same routine. It also checks that a studio that has not opted in is refused and that a withdrawn programme disappears. Paid memberships are not part of it.

## Find the owners of a workflow

Before running work across several tools, inspect the owners and their declared connections:

```sh Terminal
bonfyre-experience-map resolve 'member training creator program request publication'
bonfyre-experience-map resolve training-creator-request 1
```

The first command finds candidate owners for the request, programme publication, creator access and member app. It includes their immediate connections, such as WorkGraph, with the source paths, evidence references and known gaps retained. The second starts from one exact architecture ID. These reads also work through the native MCP tool with `family: "BonfyreExperienceMap"` and the same command arguments.

The result is a map to inspect before execution. Search scores measure word matches, and connections retain their declared direction. The command does not check current owner binaries, authenticate a member, grant access to a paid programme or establish an ordered execution plan. Those checks belong to the owners named in the result.

## Browser tools and assistant interfaces

The site publishes discovery metadata and can register read tools for compatible in-browser assistants. Standard MCP clients receive tool descriptions and data; clients with richer interface support may present that same data as controls, tables or an operator view.

That is one content and action model, not separate implementations for ChatGPT, Claude, a website and the terminal. Rich controls must still respect the underlying site identity, permission and source record.

A workshop search result can become a passage viewer; a finance result can become a timeline; a migration result can become a source-to-destination comparison. Presentation changes are independent from whether the underlying action was authorized or executed.

## Build the right user experience

A website should excel at browsing, visual identity and customer-facing actions. A CLI should excel at composition, inspection and automation. An MCP interface should excel at structured discovery and scoped action. A partner workspace should make many authorized customer operations legible at once.

Share the operation and the record it writes. Do not force all four interfaces into the visual grammar of a chat transcript.

Continue with [white-label delivery](/white-label-delivery), [recording to workspace](/recording-to-workspace) or [request to books](/request-to-books) to follow a complete operation.

## Use product operations with an attachment

The administrative product interface now exposes `books_profiles`, `books_book`, `books_learn`, `monitor_mentions` and `manifest_lot`. These call the existing product modules. They accept the export, watch description or named listing text, so an attachment does not need an invented path on the Bonfyre machine.

For an existing Books profile, list profiles first and pass the selected name with `transactions_csv` to `books_book`. Ambiguous columns require the `columns` mapping. The result includes booked counts, held rows, available totals and any local-model refusal. The ordinary response shows up to 200 held rows and reports how many were omitted; `include_csv` requests the complete categorized carrier. A file without amounts has counts, not zero-dollar totals. Books does not post to a ledger. Learning is a separate write, and current named profiles belong to the machine rather than a client organization.

Use `manifest_lot` for listing text, keeping both the ready ITAD output and its held rows. Use `monitor_mentions` for a named business against sources this installation already retains. Model-assisted reads can still require resource admission even though they do not publish or change customer records.

Saved product work has a separate route. `workspace_list` discovers actual installed bindings in a personal or explicitly selected destination; record reads, previews, updates and `workspace_views` continue that same saved work. An empty personal list is not a reason to invent an installation or infer that another organization has none. Previewed writes must retain the exact destination, record revision and change identity. They do not publish merchant changes or grant new access.

The product payload limit and transport limit are separate. Current product text fields are bounded to 4,000,000 UTF-8 bytes; the gateway's larger MCP envelope does not remove that bound. Use the discovered schema and reported refusal rather than silently dropping attachment rows. Generic `native_tool_invoke` remains useful for a native operation that has no product interface; it does not turn this chat's attachment path into a path on the other machine.
