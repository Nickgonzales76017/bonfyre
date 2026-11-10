# Building blocks

Bonfÿre is being cut into nine areas a developer can use without taking the rest. The library areas are source-based and are not published on package registries. Separately, the representation compiler has an installed, digest-checked release path on the development Mac. That installation does not establish packaging or support for every area below.

## The nine areas

| Area | What it does | Where it lives |
|---|---|---|
| Data | Local SQL and vector search, with the source of every record kept | `cmd/BonfyreVec`, `cmd/BonfyreEmbed`, `cmd/BonfyreCMS` |
| Contracts | Typed schemas and the code generated from them | `schemas/control` |
| Models | Serving, quantization and embeddings | `cmd/BonfyreQuant`, `cmd/BonfyreModel` |
| Work | Requests, commitments and who may act on them | `cmd/BonfyreQueue`, `cmd/BonfyreAuth`, `cmd/BonfyreWorkflow` |
| Apps | Frappe and WordPress integration | `integrations/` |
| Media | Preparing and transcribing recordings | `cmd/BonfyreMediaPrep`, `cmd/BonfyreTranscribe` |
| Commerce | Moving records between systems and checking them on arrival | `cmd/BonfyreBridge` |
| Finance | Meters, invoices and recorded payments | `cmd/BonfyreFinance`, `cmd/BonfyrePay`, `cmd/BonfyreMeter` |
| Web | Sites, pages and the assistant endpoint for each | `cmd/BonfyreWebAddress`, `components/BonfyreServiceSite` |

## Where to start

| If you want to… | Read | Area |
|---|---|---|
| Search your own documents on your own machine | [Local-first applications](/local-first-applications) | Data |
| Turn a recording into searchable pages | [Recording to workspace](/recording-to-workspace) | Media, Data |
| Put up a site for a customer | [White-label delivery](/white-label-delivery) | Web |
| Follow a request through to an invoice | [Request to books](/request-to-books) | Work, Finance |
| Move records out of WordPress, Frappe or a database | [Integration and migration](/integration-and-migration) | Apps, Commerce |
| Measure and compress a model | [Models and compression](/models-and-compression) | Models |

## What is not ready

These are limits in the code as it stands, not plans.

- **No packages.** Nothing here is on npm, PyPI or a system package manager. You build from the repository.
- **One vector size per store.** A store is made at one width: `bonfyre-vec init store.db --dims 768` for a 768-dimension model, 384 if you say nothing. A body of another width is refused, not trimmed. Changing model means making a new store and embedding again.
- **Payments are records.** The finance area records invoices and payments someone has entered. It does not charge a card or talk to a bank.
- **Hooks in the Frappe apps are listed, not matched.** The integration reads the donor apps' document types; it does not claim their server-side behaviour.

## See it used

The [native source-reuse guide](/native-source-reuse) covers the installed `bonfyre-runtime representation` entry point, release integrity, read-only table verification and explicit materialization. Unchanged selected tables can retain existing outputs without another source snapshot. Changed sources still require verification and may need a new snapshot.

The [source and program workspace](/source-program-workspace) uses the same recurring-program compiler for its browser-local edition and native personal, organization and household destinations. Import review, source comparison and cycle actions retain their existing behavior. The chosen destination owns saved drafts; saving does not create a merchant order or complete native work.

The existing Cloudflare source-sync bridge is described separately in [Native source reuse](/native-source-reuse#existing-cloudflare-source-synchronization), including the native pull consumer, completion checks and unverified alternate-provider takeover.

The [work samples](https://aurekai-work-samples.pages.dev/) are built from these areas: the document search uses Data, the training app uses Work and Web, and the [three shop sites](/three-businesses) use Web.

## Static retrieval shared by Data, Models and Web

StaticSemanticSearch now supplies a native C reader in `lib/libbonfyre` and a browser reader in `site/matrix/static-embed.mjs`. A distilled `.bfse` artifact contains token vectors and their tokenizer material. A `.bfsi` index identifies the exact model used for its vectors. The browser can encode a question and combine semantic and keyword ranking without running the sentence model.

Published CMS sites use this through WebAddress's per-site search files, and the existing subject-host exporter carries those files with the site assets. This is an implemented consumer of the shared code. The separate ServiceSite build and Documents application have not yet adopted that path; their existing collection and publication owners still need to connect it.

The recorded 4.1 MB model is a file-size measurement, not a memory budget or proof of teacher-equivalent quality. Matching native and browser vectors does not establish that a retrieved passage answers a question. Keep source, passage order, model identity and permitted audience together. A private corpus must not be moved into public site search.

The managed Embed executable selected by this connector was older than the new checkout during the October 18 UTC recovery and refused `static`. That installed path needs reviewed promotion. Rebuilding source or deploying WebAddress does not by itself upgrade every command consumer.
