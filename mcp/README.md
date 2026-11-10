# Bonfÿre institutional MCP interface

**MCP is an interface to Bonfÿre's existing native owners—not a new authority system, a simulated toolset, or the definition of Aurekai.**

Aurekai is the digital investment/operating business and public product network. Bonfÿre is the execution and capability infrastructure. An AI agent can use MCP to inspect or request work, but a tool descriptor and a successful network connection do **not** authorize a project patch, institutional mutation, payment, publication or external effect.

[Operating model](../docs/aurekai-bonfyre-model.md) · [Architecture](../docs/architecture.md) · [Source root](../README.md)

## The important boundaries

| Layer | Owns | What it does not imply |
|---|---|---|
| **Matrix / identity** | Stable subject, address and institution references | Authority over a foreign account or person |
| **Capability closure** | Available, missing and workload-proven implementations | All named tools are callable or promoted |
| **WorkGraph / Project** | Work records, dependencies, leases, recovery and accepted execution | An agent message equals work completed |
| **Authority / EffectKernel** | Independent permission and exact effect decisions | Authorization may be manufactured by the transport |
| **ForeignTwin / providers** | Attributed read-only observation and declared external capability | Ownership or permission to mutate an external source |
| **Evidence / receipts** | Durable witness records, readback, replay and divergence | A local successful test proves a live public deployment |
| **Finance / commercial** | Distinct costs, quotes, offers, invoicing and settlement states | A stated price or paid provider credit equals cash |

## Current source organization

The publicly visible integration tree includes these families. **Source presence is not automatically current service reachability**.

| Source family | Use |
|---|---|
| [`recursive_overlay_tools.mjs`](../integrations/mcp/recursive_overlay_tools.mjs) and `bonfyre_oauth_gateway.mjs` | Gateway/tool surface, forwarding and OAuth edges |
| [`project_node.mjs`](../integrations/mcp/project_node.mjs), `project_operator.mjs`, `project_fabric.mjs` | Project-controlled work, patch and operator boundaries |
| `project_spines.mjs`, `project_v6.mjs`, `v6_hypervisor_bridge.mjs` | Execution spines, resource and hypervisor routing |
| `hypervisor_*.mjs` | Hypervisor resource, model, mission and execution integration |
| `workspace_mcp.mjs`, `workbook_mcp.mjs` | Account-owned application records and admissible edits |
| `operator_tools.mjs`, `operator_daemon.mjs` | Operator orchestration and recovery without implicit resume |
| [`tests/`](../integrations/mcp/tests/) | Tool and admission tests |

The actual tool inventory is evolving. Do not pin a contemporary claim to an older fixed count of gateway descriptors or handlers; inspect the current MCP exposure and the native owner when implementing a client.

## Read, prepare, execute: separate requests

1. **Discover:** inspect the architecture Atlas and native capability inventory; distinguish declared tools from activated and workload-proven ones.
2. **Read:** retrieve source/record state from its named canonical owner. Pass expected revision, object ID or digest when available.
3. **Prepare:** produce an exact bounded plan, candidate patch, receipt and independent review. Missing source or capability closure blocks progress.
4. **Preflight:** require current actor, institution, destination, cost and effect authorization from their independent owners.
5. **Apply:** only the exact allowed effect using its declared executor; an MCP caller cannot manufacture another user's approval.
6. **Verify:** inspect the native result receipt, source revision, tests and destination readback. Reconcile uncertainty before retrying.

A general API execution verb must refuse effects whose installed capability, independent review and exact WorkGraph executor cannot be bound. Reuse existing owners rather than implementing a parallel permissions layer in MCP.

## Failure handling

**Unavailable owner:** fail closed and return the real missing dependency, rather than fabricating a success receipt or silently falling back to a weaker implementation.

**Provider exhaustion:** detect credit, rate, quota and model availability as resource conditions; choose an admitted alternative only if capability, quality, privacy, price and authority allow it.

**Timeout or uncertain result:** read task and effect receipts before retrying. A historical `running` row without service ownership is not evidence that a process is alive.

**Local model:** a model file, FPQ pack, tokenizer and answering model service have different health contracts. A file existing on disk does not mean generation is ready.

**Commercial:** an executable offering, a quotation, an invoice and settled value must be read from their distinct owners; do not count them as the same thing.

## Legacy gateway history

Earlier gateway documentation described 53 rich descriptors, 38 native handlers, 15 forwarded project verbs, five resources and three prompts, plus an older 45-tool generation. Those counts describe **particular historic gateway cuts**, not an up-to-date count or implementation contract. The archived gateway/source and version-specific smoke tests remain useful when investigating those migrations.

The former one-tool `mesh-ledger.mjs` read-only aperture and older `rich-catalog.json` descriptor snapshot similarly do not establish that the current institutional MCP plane is restricted to that legacy subset.

## Verification and publishing

Use the current integration tests, native Atlas, owner readbacks and explicit deployment receipt before claiming a change is live. **A PR, locally passing smoke test, source parity result or callable transport is not proof of a production effect.**

Bonfÿre's public GitHub branch and local institutional runtime may have different revisions. Cite the exact source branch, release and receiving endpoint when describing a measured capability.
