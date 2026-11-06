# MCP gateway

The Bun gateway in `integrations/mcp/recursive_overlay.mjs` now owns the MCP
transport, the 53 rich tool descriptors, five rich resources, three prompts,
and events/metrics/delta HTTP paths. It executes 38 rich handlers through
native owners and forwards 15 exact project verbs to the project MCP node;
when that owner is unavailable the effect fails rather than falling back to
Python. Its CUE policy also declares recursive Matrix, foreign, CMS, surface,
and circuit additions. `rich-catalog.json` is a one-time native descriptor
snapshot of the legacy public catalog, not a runtime Python dependency.
Promotion runs native descriptor/resource/prompt/auxiliary-route smoke before
and after stopping the old loopback base. The promotion recipe has not been run
by this source change, so source parity is not live-deployment proof. The
deployed OAuth/2026 adapters still forward to the MCP owner. The separate
`mesh-ledger.mjs` is a non-deployed, read-only native
aperture for one tool and one resource, authored in `mesh-ledger.cue` and tested
with `bun test mcp/tests/mesh-ledger.test.mjs`. It opens the existing SQLite
ledger read-only and cannot patch a project or enact an effect.

## What the institution tools reach

The gateway formerly carried 45 tools and none of them reached the atlas, the finance
book, local generation, the estate as anything but four fixed projections, or
any of the seventeen carrier protocols. An agent could see the system's
activity without being able to use its substance -- and when a provider
returned `credit_balance_exhausted`, bounded work had nowhere to go.

| tool | reaches |
|---|---|
| `architecture_atlas` | the canonical registry, through its cheap read path |
| `model_local_inventory` | which local generation is actually runnable here |
| `model_local_generate` | native generation, refusing rather than falling back remotely |
| `estate_query` | read-only SQL over 6.5M receipted observations |
| `finance_book` | the service book, beside the executable catalogue |
| `finance_price_set` | pricing, which every commerce grammar then quotes |
| `protocol_fetch` | any of the eighteen carriers over the same state |
| `desk_open_decisions` | what the institution is holding open |

## Two things this surface must keep saying

**The book and the catalogue are different sets.** Every service with a price
is in the book; only what an organ can execute is in the x402 catalogue.
Pricing a book entry does not make it x402-sellable, and `finance_book` returns
both sets rather than implying one.

**A large model file is not a generation weight.** The first version counted any
`.gguf`/`.onnx` over 10 MB and reported `runnable: true` on the strength of a
piper speech voice, which cannot answer a prompt. `runnable` now admits an
answering resident model service; an FPQ pack and a binary are reported
separately, not mistaken for proof of a working chat endpoint.
