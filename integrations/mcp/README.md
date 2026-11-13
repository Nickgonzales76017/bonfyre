# Bonfÿre MCP — authenticated project operations, product workspaces, receipts and resources

The MCP implementation is **not** one generic AI tool bus. Bonfÿre exposes several owners through a Bun/HTTP project endpoint and separate product/workspace adapters, with scoped tool schemas, resources, prompts, current revision checks and auditable downstream effects. **MCP is a transport and interaction boundary, not the canonical owner of project files, customer records, invoices or source identity.**

[Project MCP source](project_node.mjs) · [Recursive overlay](recursive_overlay.mjs) · [Account workspace](workspace_mcp.mjs) · [Workbook-specific tools](workbook_mcp.mjs) · [Full operational workflows](../../docs/real-workflows-and-interoperability.md)

## The four materially different MCP interactions

| Interaction | Actual tool/owner | What is returned | Effect boundary |
|---|---|---|---|
| Read the current local project | `project_status` / `project_tree` / `project_read` / `project_search` | current branch, source hashes, exact file ranges, search results | read-only, not permission to change source |
| Make project changes | `project_patch_preview`, authorized native apply, `project_test_profile`, receipts | a reviewed patch against known preimage + test result | approval, dirty-state policy, source preimage and independent readback |
| Open an installed user's record | `workspace_list` → `workspace_records` → `workspace_views` | binding, exact record key, revision, SHA-256, editor link | same account owner and current grant; no shadow copy |
| Edit a saved workbook | `workbook_review` → `workbook_preview` → `workbook_update` | rows, source coordinates, calculation exceptions, preview digest, saved revision | workbook domain calculator, exact review, native account write; no ledger posting |

The canonical [Project catalogue](project_catalog.json) and [project node](project_node.mjs) also register Spines/V6/Fabric/Products owner routes, prompts, resources and product toolsets. Those routes keep their **different original owners**. A product-shortlist schema is not permission to invoke an effect. The legacy Python path and the Bun/native paths must be reconciled at their own release boundaries rather than reported together as one completed migration.

## 1. Read bundle: compact and non-effecting

`mcp_read_bundle` in [`project_node.mjs`](project_node.mjs) validates tool names against the current exposed catalogue and relevant product aperture. Each child must be annotated **readOnlyHint=true** and have validated arguments. The runner limits concurrency to **eight** and coalesces identical reads across a bundle (including calls already in flight). The output carries per-child ID, owner, success/refusal, sharing state and duration. It cannot silently call an effectful tool simply because a model put it in the bundle.

Product toolsets and resources expose compact schemas to the model. The measured context figure is **UTF-8 public tool-definition bytes**, *not* model tokens, speed, cost or reasoning quality. Service-profile resources include source identity, listener/deployment previews, requirement checks and exact toolset hashes.

## 2. Real project and source operations

A safe code-work path is:

~~~mermaid
flowchart LR
  Q["Goal with target source and acceptance"] --> OWNER["Atlas owner + native tool inventory"]
  OWNER --> READ["Project status/search/read exact SHA"]
  READ --> PRE["Patch preview / write constraints"]
  PRE --> APPLY["Authorized apply against preimage"]
  APPLY --> TEST["Allowlisted native tests"]
  TEST --> REC["Receipt, source diff and re-read"]
  REC --> LIVE["Separate release staging, activation and live readback"]
~~~

Project MCP does not complete this merely by generating a patch, claiming a task, getting HTTP 200, or watching an agent finish. A source file must actually change, tests must have the intended subject, and the intended public site needs release verification.

Source: [`project_node.mjs`](project_node.mjs) and [`project_runtime.mjs`](project_runtime.mjs). The actual tool catalogue declares the operation schemas, and individual native project tools enforce their write authorities.

## 3. Installed-workspace MCP: the same private records the person edits

[`workspace_mcp.mjs`](workspace_mcp.mjs) uses a fixed WebAddress owner, exact destination binding, account bearer session and native MessagePack record envelope.

**Read path:** `workspace_list` lists installed editors; `workspace_records` returns a saved draft or bounded record identities; `workspace_views` resolves current compatible editor links without copying the object into a new organization.

**Review/write path:** `workspace_preview` applies only bounded scalar-field proposals to a retained record at an exact revision/SHA-256, returning the before/after and preview digest; `workspace_update` must include that preview digest and a stable caller request ID, and rechecks account access and current revision. An ambiguous network response **is not retried as a POST**; the caller must read the current record. Original file references are not silently replaced by JSON strings or a second workspace database.

The [installed-workspace contract test](../../scripts/tests/installed-workspace-contract.test.mjs) covers eleven real workspace packages across ten product families, cross-editor continuation, conflicts, deletion/restore and an uncertain response. It also proves Catalogue and Room Planning may share **one destination-owned saved object and attachment**. That is more useful than saying “all apps connect to the same capability.”

## 4. Workbook MCP is an actual domain tool, not just a text edit

[`workbook_mcp.mjs`](workbook_mcp.mjs) adds:

- `workbook_review` — bounded row pages, source metadata, review decisions, supported formula recalculation, error reporting and optional original-cell inspection.
- `workbook_preview` — exact current revision and digest, column mapping, amount/account edits, Univer calculation, content-bound review decisions, changed cells and a preview digest. Does not save.
- `workbook_update` — writes that exact preview through the destination's native owner, retaining file references and caller request ID. Does not approve, invoice, pay or post a ledger line.

This differs from `workspace_update` by calling the workbook's **real financial domain compiler**. [Books XLSX](../../cmd/BonfyreDecide/src/books_xlsx.c) is an additional file interchange path: it preserves source workbook bytes/hash and worksheet/cell coordinates and produces an XLSX review, not an account grant or calculated remote spreadsheet without the specialized owner.

## 5. MCP resources and prompts are useful typed interfaces

The project server lists resources such as:

- `bonfyre://atlas/summary`, `bonfyre://native/inventory`, `bonfyre://models/status` and `bonfyre://operator/status`
- `bonfyre://mcp/toolsets`, `bonfyre://mcp/runtime`, `bonfyre://mcp/service-profiles` and product-specific toolset snapshots
- An embedded control-plane app resource for specified product-planning tools

Prompts include **evidence-bound intake**, **native delivery**, **addressed participation**, **institutional circulation** and **product pack workflow**. These are generated task guidance, not authority to mutate an institution, spend money or establish foreign truth.

## 6. A site can expose its own MCP — independent from the Project operator

[`site_product_test.sh`](../../cmd/BonfyreWebAddress/tests/site_product_test.sh) exercises a site's own `/mcp/{site}` path with an actual MCP initialize handshake under its host. This can make a customer-facing site machine-readable without exposing the entire Project MCP tool catalogue or local source-write authority. A site route, source catalogue, authenticated record editor and Project operator are four different trust and audience scopes.

## 7. Acceptance and known gaps

- **Verified in source tests:** project tools/resources/prompts, read-bundle classification and coalescing, recursive overlay source parity, workspace record revision/preview and workbook-domain gating. See [project tests](tests/project_node.test.mjs), [recursive overlay tests](tests/recursive_overlay.test.mjs) and [installed workspace tests](../../scripts/tests/installed-workspace-contract.test.mjs).
- **Not established by tests alone:** every deployed MCP listener is source-aligned, every installed account application is live, every browser control-plane app renders correctly, a paid-model mission is successful, or the legacy Python mission compiler is fully retired.
- **Never infer:** “MCP tool exists” → caller has authority; “read succeeded” → external source truth; “draft save succeeded” → public publication; “budget exists” → settlement; “plan completed” → live product effect.

### One rule for documentation and workflows

Name **the specific tool, product owner, input body, revision, output and verifier**. Do not relabel this as “the Matrix exposes a capability.” Matrix identity and addresses may be involved in an authorized site or account route, but they do not implement the workbook calculator, source patcher, WordPress signature check, document export or provider OAuth themselves.
