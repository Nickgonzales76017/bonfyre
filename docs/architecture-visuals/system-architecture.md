# Bonfÿre / Aurekai — complete system architecture

> **Coverage contract, not a demo inventory.** Bonfÿre is the native engine and source of executable contracts; Aurekai is the operating business, customer/product network and distribution estate. A room planner, narrated article, profile, map, application, or feed is an *instance* of the system, not its definition.
>
> **Evidence cut:** architecture registry checked against 151 declaration files, generated 2026-11-13 13:02 UTC (digest 4af1cc117e76): **557 architecture entries, 562 interactions, 11 expandable views**. Registry counts are *not* proof of deployed features: 345 tagged implemented, 154 measured, 34 architectural and 24 declared. Live readiness is a separate owner-and-release question.

[Repository README](../../README.md) · **[Workflows, exports and external platforms](../real-workflows-and-interoperability.md)** · **[Unified editors and native frontend](../authoring-frontend-architecture.md)** · [Technical architecture](../architecture.md) · [Source registry](../../architecture/README.md) · [Visual guide](README.md) · [Federated source libraries](../../integrations/federated/)

## 1. Parallel native systems with explicit bridges — not a Matrix intake funnel

**Start with the [concrete workflows, exports and MCP operations](../real-workflows-and-interoperability.md).** Matrix resolves addressed subjects and presence; it does **not** receive every record, application, workbook, scanned document, feed, provider call or code repository as a generic “capability.” Identity, format conversion, human interaction, temporal evidence, execution, site publishing, accounts and effects stay distinct.

~~~mermaid
flowchart TB
  WP["WordPress event / site export"] --> CMS["CMS exact remote ID + site drafts"]
  EXCEL["XLSX with sheet/cell provenance"] --> BOOK["Books / Workbook review owner"]
  SCAN["Paper / scanned PDF"] --> OCR["OCR line/geometry + human source review"]
  EDIT["Puck, GrapesJS, Editor.js"] --> SCENE["Native typed authoring document"]
  SCENE --> CMS
  CMS --> WEB["WebAddress native C + HTMX/browser components"]
  WEB --> EXPORT["HTML, WXR, Frappe exchange"]
  BOOK --> XMCP["Workbook MCP, exact account revision"]
  PROJECT["Git, GitHub, Forgejo, Josh, project source"] --> PMCP["Project MCP + native patch/tests/receipts"]
  HUMAN["Client / group / member / public audience"] --> ADDRESS["Matrix address + audience/disclosure"]
  ADDRESS --> WEB
  PROVIDER["YouTube / Patreon / Spotify / Toast candidate"] --> VERIFY["Exact provider credential and effect owner?"]
  VERIFY -. "Only when verified and scoped" .-> WEB
  DOC["Cited Markdown / brief"] --> EMIT["BonfyreEmit / PandocFormatter outputs"]
~~~

The arrows denote **bounded transformations**. A Wordpress webhook does not become a published Aurekai post merely because its signature verifies. An XLSX import does not imply recalculation of arbitrary Excel formulas or ledger posting. A signed-in assistant does not own a user's workspace record; it uses the current owner's scoped read/preview/update protocol. A named external platform in the enrollment ledger does not imply provider integration.

The reusable property of Bonfÿre is **verifiable continuity across otherwise different types of software**: an editor may preserve a native app binding; a WordPress exchange keeps article text through a local WXR round-trip; an XLSX review keeps cell coordinates; an MCP call shares the same revisioned account record. This is more precise than “everything composes through Matrix.”

### Concrete cross-product routes

| Starting material | Operable route | Output / refusal |
|---|---|---|
| WordPress posts | HMAC-verified bridge, idempotent native CMS event; separately WXR export | native entry or WXR 1.2; no presumed publish permission |
| Existing site | `bonfyre site export --out … [--format wordpress|frappe]` | offline HTML with CSS; WordPress/Frappe exchange |
| cPanel/InterWorx/Plesk exports | BonfyreBrief inventory, cited discrepancies, BonfyreBridge row checks | migration facts and tests, not live cutover |
| Excel XLSX | BonfyreBooks native reader; Account FinancialWorkbook; workbook-specific MCP | cell-preserving review, explicit revision and held calculation |
| Scanned PDF/image | BonfyreOCR on-device Vision/PDFKit | page, bounding boxes, confidence, text; uncertainty remains |
| Markdown / brief | BonfyreEmit and PandocFormatter | PDF, EPUB, HTML, TXT, RSS and DOCX formatter conversion |
| React/Puck/visual frontend | shared tree, CMS, native C/MD4C/Mustach output | authored native page; Svelte/React general export not yet verified |
| YouTube/Spotify/Patreon | public locator, provider-auth possibilities, owner-bound InteractionGrammar | candidate account or studied interface grammar; no automatic OAuth or API effect |
| Toast POS | commercial target reference only | **No verified Toast native connection** |

### Large-scale local computation belongs beside the native frontend

[Full source, measured results and build/release gaps](../native-data-compute-and-presentation.md)

~~~mermaid
flowchart TB
  YAFF["YAFF: exact architecture/interaction invariants"] -. "A separate architectural constraint" .-> WEB["Native WebAddress and typed application frontend"]
  CUE["CUE page regions/app reader shapes"] --> ZIG["Zig to C native lowerer"]
  ZIG --> WEB
  PCSS[".pcss PostCSS/Tailwind build + PresentationTemplate tokens"] --> WEB
  CONTENT["Native CMS page/article sections"] --> PUCK["Puck / GrapesJS / Editor.js shared tree"]
  PUCK --> WEB
  DATA["Source/material: SQLite, Parquet, numeric charts"] --> EMBED["BonfyreEmbed + BonfyreVec"]
  DATA --> VGPU["vGPU numerical kernels: kin and spatial neighborhood"]
  EMBED -. "Only the same model/dims/chart" .-> VGPU
  VGPU --> RESULTS["Persisted derived table with input/backend receipt"]
  DATA --> DUCK["DuckDB-Wasm browser SQL over Parquet"]
  RESULTS --> EXPERIENCE["Large interactive table, research and editorial"]
  DUCK --> EXPERIENCE
  WEB --> EXPERIENCE
  BIT["BitNet candidate rank"] -. "Only eligible/passported" .-> RESULTS
  FPQ["FPQ weight codec"] -. "Full generation parity still open" .-> EMBED
~~~

**Measured source paths:** the vGPU kin operation compared 46,376,100 pairs in one 6,810-item local workload and persisted 710 above a chosen similarity floor; the separate Web field read 109,761 Parquet source-channel rows via DuckDB-Wasm inside the tab and displayed them through WebGPU. This is local numerical analytics and data presentation, **not a 3D-asset pipeline or evidence that vGPU executes DuckDB SQL**.

The frontend path uses typed Puck/GrapesJS/Editor.js authoring, the C/MD4C/Mustach public renderer, generated CUE application lowerers and locally compiled `.pcss`/Tailwind styles. New PCSS source files remain **local-only relative to public GitHub `main`** until source synchronization; screenshot, token, mobile and public release parity are still open.

FPQ measures neural-weight representation (one Qwen tensor reconstruction is measured, whole-model generation is not). BitNet reorders an already allowed source frontier under passport and quality rules. **They do not decide provider truth or source rights.** The downstream product acceptance is a source-linked table/result → real authored section or installed account record → reviewed and verified public output.

## 2. Identity, groups, institutions, permissions and domains

~~~mermaid
flowchart LR
  P["People / actors"] --> I["Identity / actor graph"]
  O["Organizations, institutions, households"] --> I
  G["Groups, communities and memberships"] --> I
  I --> S["Matrix subject and standing addresses"]
  S --> H["Presence: host + path + territory + world"]
  H --> A{"Audience and per-field disclosure"}
  A --> U["Self / household / group / community"]
  A --> E["Public / market / institution"]
  H --> D["Multiple native sites and verified domains"]
  D --> APP["Installed apps and distinct owner permissions"]
~~~

**Native owners:** [BonfÿreAuth](../../cmd/BonfyreAuth/), [BonfÿreWebAddress](../../cmd/BonfyreWebAddress/), [community content types](../../cmd/BonfyreCMS/content-types/community_body.json), [community memberships](../../cmd/BonfyreCMS/content-types/community_membership.json), [institution planes](../../schemas/control/institution-planes.cue), [site presentation](../../cmd/BonfyreCMS/content-types/site_presentation.json).

Account, profile, organization, household, group, community, institution and market are not a single interchangeable tenant level. Membership, a publicly discoverable presence, an installed app, a domain binding, and a right to act are separate facts. A site can have several addresses without creating several canonical identities. Custom-domain issuance and generalized sub-organization delegation are still separate gated rollout work.

## 3. External estate, source acquisition and one-source/many-uses

~~~mermaid
flowchart LR
  C["Candidate catalogs / publisher roots / protocols"] --> Q["SourceFrontier: accounting and deduplication"]
  Q --> F["ForeignTwin: robots and bounded observations"]
  F --> T["Exact bytes, source identity, digest and provenance"]
  T --> P["Publication occurrences / retained material"]
  P --> K["Purpose, terms, lawful audience, rights"]
  K --> AP["Declared application intake"]
  AP --> CRM["CRM / opportunity"]
  AP --> NEWS["Reading room / community / collection"]
  AP --> ANALYSIS["Analysis / search / evidence"]
  AP --> AUDIO["Optional narrated or media edition"]
~~~

**Native owners:** [SourceFrontier](../../cmd/BonfyreSourceFrontier/), [BonfyreProject ForeignTwin](../../components/BonfyreProject/src/main.c), [CMS capability programs](../../cmd/BonfyreCMS/content-types/capability_program.json), [BonfyreExternalEstate](../../cmd/BonfyreExternalEstate/), [BonfyreWebAddress](../../cmd/BonfyreWebAddress/).

A source may support many applications. It does not become several source identities. Candidate counts, robots-eligible observations, admitted sources, material readbacks and application effects must never be aggregated into one misleading inventory number. The native source census covers publisher, protocol and endpoint distinctions; a page rendering a retained body does not establish that the publisher still publishes it.

## 4. Connection Studio and the actual provider / account estate

~~~mermaid
flowchart TB
  CS["Connection Studio / source catalog"] --> V["Declared providers and installation choices"]
  V --> B["Destination-bound account / organization install"]
  B --> CR["Sealed credentials and verified capability envelope"]
  CR --> R["Provider-state and resource admission"]
  R --> G{"Read vs write vs effect / spend authorization"}
  G -->|Read| F["Foreign observation / synced source"]
  G -->|Allowed effect| P["Provider-specific operation"]
  G -->|Not admitted| H["Hold / human approval / missing prerequisite"]
  P --> E["Receipt + account-scoped continuation"]
~~~

**Native owners:** [Connection catalog](../../cmd/BonfyreConnectionCatalog/), [ProviderEstate](../../cmd/BonfyreProviderEstate/), [BonfyreCredentials](../../cmd/BonfyreCredentials/), [Connection Studio](../../integrations/browser/connection-studio.mjs), [GitHub app connection](../../cmd/BonfyreWebAddress/src/connect_github_app.h).

Connections cover user-facing app installations, downstream customer accounts, service credentials, protocol carriers, source ingestion and business workflow destinations. These are *different contracts*. A provider appearing in the catalog does not mean an account is connected, a token works, a scope was granted, or a service can perform an effect. Provider onboarding is uneven: GitHub installation has a separately witnessed path; Neon/Sentry customer-account paths exist; most other provider actions do not have generalized invite, rotate and reconcile support.

## 5. Spatial, geographical and temporal capability — not a room-planning demo

~~~mermaid
flowchart TB
  S["Existing subject / source identity"] --> O["Spatial observation: geometry, method, source"]
  O --> T["Observed / effective / expiry / recorded time"]
  T --> N["BonfyreSpatial: indexed queries and reconstruction"]
  N --> F["Points, regions, relative positions, overlap / proximity"]
  N --> E["Evidence-linked geographic investigation"]
  N --> M["Maps, location-aware reading and civic views"]
  N --> A["Asset / operational / community contexts"]
  N --> R["Optional site and room layouts"]
  R --> P["Optional product / procurement continuation"]
~~~

**Native owners:** [spatial contract](../../schemas/control/spatial-observation.cue), [BonfyreSpatial](../../cmd/BonfyreSpatial/src/main.c), [spatial CMS record](../../cmd/BonfyreCMS/content-types/spatial_observation.json), [public estate field](../../site/matrix/field.html).

Spatial observations use **valid time and knowledge time**. Location is an observation about an existing subject, not a replacement identity. The native command can compile and answer its help interface, but the registry explicitly says the standalone BonfyreSpatial workload has *not yet* been exercised on a real workload. Some geographic calculations use a local tangent plane; do not advertise global-geodesic precision or polygon intersections absent a witness.

The estate-field visualization is another, separate spatial/data product: a measured public browser read of 109,761 channel rows over Parquet and WebGPU on 2026-09-25, with 2,364 then-admitted channels. Its 2026-10-03 offline/cache tests were local; the keeper was not published then. The **civic transparency**, place research, publisher distribution, asset location, and site/room planning examples must all derive from spatial/data contracts rather than room planning being the parent abstraction.

## 6. Media, narration, recording and live communication

~~~mermaid
flowchart TB
  X["Article, feed, post, recording, meeting or other source"] --> R["Rights + retention + audience"]
  R --> C["Content and media identities"]
  C --> E["Edit / transcribe / clean / segment"]
  E --> N["Narration, speech, audio arrangements"]
  E --> V["Video frame, scene, clip, subtitles"]
  E --> I["Images, media library and delivery"]
  N & V & I --> P["Publication / player / channel / collection"]
  P --> W["Accounts, communities, public sites and other apps"]
~~~

**Native command families:** BonfyreNarrate, BonfyreSpeechLoop, BonfyreTranscribe, BonfyreTranscriptClean, BonfyreTranscriptFamily, BonfyreTone, BonfyreMediaPrep, BonfyreFrameExtract, BonfyreSceneDetect, BonfyreVideoDemux, BonfyreClips, BonfyreRepurpose, BonfyreStitch, BonfyreMoQ, BonfyreNet. The [ListenProgram](../../cmd/BonfyreWebAddress/src/public_audio_deck.h) and [music-recording schema](../../cmd/BonfyreCMS/content-types/music_recording.json) are one surface, not the entirety of audiovisual capability.

Narration of a feed is an optional derived edition, not a universal source-processing requirement. A generated track is not a published post, licensed recording, proven voice quality, or active player. The public audio deck and legacy shared player still have separate controls; consolidation is an open gap.

## 7. Model representation, masks, BitNet, FPQ and runtime placement

~~~mermaid
flowchart TB
  W["Typed demand: task, semantic source, audience"] --> MASK["AddressPlane: exact declared masks / eligibility"]
  MASK --> E["Typed candidate frontier + exact owner verifier"]
  E --> PASS["ModelRepresentationPassport: receipt, parity and preservation"]
  PASS --> G{"ADMIT or DEFER?"}
  G -->|ADMIT| RESOURCE["ResourceAdmission: memory, compute, placement"]
  G -->|DEFER| SHADOW["Measurement / SHADOW, no production lowering"]
  RESOURCE --> SELECT["Compatible native model / backend / representation"]
  SELECT --> L["Qwen / MoE / local infer / bounded execution"]
  SELECT --> Q["FPQ representation / FPQx research"]
  SELECT --> B["BitNet candidate ranking"]
  L & Q & B --> MET["Latency, quality, parity, resource and outcome receipts"]
~~~

**Native owners:** [AddressPlane/WebAddress](../../cmd/BonfyreWebAddress/src/subject_address.c), [BonfyreBitNet](../../cmd/BonfyreBitNet/), [BonfyreFPQ](../../cmd/BonfyreFPQ/), [BonfyreModel](../../cmd/BonfyreModel/), [BonfyreQwenFPQ](../../cmd/BonfyreQwenFPQ/), [BonfyreDistribute](../../cmd/BonfyreDistribute/).

The mask is a cheap, deterministic **necessary-condition filter**, not a learned semantic verdict: (have & need) == need, plus prohibited-term checks. A BitNet ranker can reorder surviving *handles*, but may not bypass exact verification or reintroduce rejected candidates. Measured BitNet address ranking remains **SHADOW/DEFER**; its captured top-k quality did not justify admission. FPQ has measured real Qwen tensor distortion/size results and a passthrough native-F16 generation baseline, but no proved complete compressed-model layer/logit/generation parity. FPQ and BitNet should be shown as **representation and physical-computation work spanning the system**, not as features of a feed or a single page.

### Data/memory and physical execution adjoining the model work

~~~mermaid
flowchart LR
  IN["Source bytes, schema, records, vectors, media"] --> CAN["Content address, identity, provenance"]
  CAN --> STORE["SQLite, DuckDB/Parquet, Arrow, object stores"]
  STORE --> QUERY["Indexes, incremental queries, temporal KV, vector retrieval"]
  QUERY --> CONTEXT["ContextCut / retrieval / retained representation"]
  CONTEXT --> MODEL["Model and program execution"]
  MODEL --> RECEIPT["Stable proof / measurement / replay"]
  RECEIPT --> CAN
~~~

Other measured or examined substrate paths include HVM4, YaFF, egglog, Feldera, Gigatoken, Lance, Daft, Tract, Burn, CubeCL, Hydro, Restate, Verus and Automerge. **Do not equate a measured donor or extracted substrate with a completed native or production substitution.** Storage and compression are governed by reproducible bytes and semantic equivalence, not by a smaller file alone.

## 8. Git, GitHub, Forgejo, Josh and reusable source ownership

~~~mermaid
flowchart TB
  A["Account / institution-owned repository"] --> C["Credentials + provider admission"]
  C --> F["RepositoryFabric"]
  F --> PROV["Forgejo: provision, resolve, view, observe, change, hook"]
  F --> GIT["Git checkout / immutable commit identity"]
  GIT --> J["Josh filtered checkout: selected subtree / view"]
  J --> D["Native source project, apppack, extraction, or workspace"]
  D --> P["Project patch preview + independent verification"]
  P --> R["Committed source and retained receipts"]
  R --> REL["Release checks / explicitly authorized activation"]
  F --> GH["GitHub installation and 69 pinned companion repositories"]
~~~

**Native owners:** [RepositoryFabric](../../cmd/BonfyreRepositoryFabric/src/main.c), [smoke witness](../../cmd/BonfyreRepositoryFabric/tests/smoke.sh), [GitHub installation](../../cmd/BonfyreWebAddress/src/connect_github_app.h), [federated submodules](../../integrations/federated/).

The RepositoryFabric registry documents **live Forgejo 17.0.1 smoke tests** for provisioning, reading, changing, webhooks and Git checkout. Josh was built and shown to filter a repository at checkout: full root vs a re-rooted /notes subtree. **Only checkout --filter uses Josh**; read, resolve, observe, push/changes/workspace composition are *not* all transparently Josh-enabled. A working josh clone required newer Git than macOS /usr/bin/git. GitHub Apps, GitHub Pages, 69 pinned external libraries, local working branches, client repositories and Forgejo are separate ownership and delivery routes. A Git link preserves exact source lineage; it does not make a donor part of the active engine.

## 9. Work, autonomy, economics, protocol boundaries and evidence

~~~mermaid
flowchart TB
  GOAL["Client/operator goal"] --> WG["WorkGraph / dependencies / blocker and unlock graph"]
  WG --> CAP["Typed capability and installed-app compatibility"]
  CAP --> POL{"Authority, information policy, resource, cost, effect"}
  POL -->|Refused| HOLD["Review / held state / gap"]
  POL -->|Admitted| EX["Native command / service / model / agent / human"]
  EX --> REC["Receipts + exact input/output and witness links"]
  REC --> E["Proof frontier / temporal evidence / replay and divergence"]
  E --> NEXT["Status, reusable output, optional downstream continuation"]
  NEXT --> WG
~~~

**Native owners:** [WorkGraph](../../engine/core/src/workgraph.c), [authority](../../engine/core/src/authority.c), [resource admission](../../engine/core/src/control_admission.c), [commerce stages](../../engine/core/src/commerce.c). Distinguish the active C owners from archived earlier Python control-plane lineages. Model output, an agent transcript or a provider callback is not itself a human-authorized effect or a completed task. WorkGraph-to-run-attest wiring remains a known gap for several external agent runners.

~~~mermaid
flowchart LR
  D["Capability declaration"] --> N["Native MCP / A2A / API / CloudEvents / web carrier"]
  N --> G["Subject + account closure / tool admission"]
  G --> R["Runtime execution owner"]
  R --> C["Recorded result / evidence / status"]
  C --> UI["App / page / workflow / customer"]
  G --> F["Finance, payment, delivery checks"]
  F --> C
~~~

The commerce/federation surface includes x402, MPP, MCP, A2A, CloudEvents, Beckn, UCP and ACP as *protocol-compatible or declared boundaries*, not proof that every foreign provider is connected or that a quoted amount is verified settlement. The commercial chain must keep offers, quotes, orders, invoices, payments, fulfillment and receipts distinct. Business outcomes are owned by the finance/commerce records, not inferred from a plan.

## 10. Account applications, Frappe and CMS as a shared authoring substrate

~~~mermaid
flowchart TB
  A["Account / organization / community scope"] --> T["Typed objects, fields, permissions and workflow"]
  T --> CMS["BonfÿreCMS: publications, sites, sources, records"]
  T --> FR["Frappe compiler: schemas / DocTypes / app grammars"]
  FR --> CRM["CRM"]
  FR --> ERP["ERPNext"]
  FR --> HR["HRMS"]
  FR --> HEL["Helpdesk"]
  FR --> LMS["LMS"]
  FR --> WIKI["Wiki"]
  FR --> DR["Drive"]
  FR --> INS["Insights"]
  CMS & FR --> R["Native form / page / work / evidence composition"]
  R --> EX["Client and public sites with distinct audience cuts"]
~~~

**Native owners:** [BonfyreCMS](../../cmd/BonfyreCMS/), [BonfyreFrappeCompiler](../../cmd/BonfyreFrappeCompiler/), [SitePresentation](../../schemas/control/site-presentation.cue), [PageComposition](../../schemas/control/page-composition.cue), [federated Frappe libraries](../../integrations/federated/frappe/).

A compiled DocType/form is an extracted schema and grammar, **not execution of a Frappe controller or foreign-system mutation**. Business apps, profiles, articles, account dashboards, communities, boards, maps, search and public portals should be composed through the same contracts, with different owners and releases; disconnected demo islands are not an acceptance target.

### Native authoring and frontend are a cross-cutting compiler, not a CMS sidebar

The deeper **[unified authoring, frontend and innovation guide](../authoring-frontend-architecture.md)** traces how Puck, GrapesJS and Editor.js become three interaction modes over one typed document, how MD4C and Mustach lower safe rich content into C-rendered WebAddress pages, and how Source/Scene/SurfaceIR keeps data, appearance, actions, audience and revision separated.

~~~mermaid
flowchart LR
  O["Person, group, publisher, research or app owner"] --> S["Typed source, identity and action contracts"]
  S --> DOC["Shared nested authoring tree"]
  P["Puck + GrapesJS + Editor.js"] <--> DOC
  DOC --> CMS["CMS drafts, revisions, source continuity"]
  CMS --> C["MD4C + Mustach + C native components"]
  C --> IR["SurfaceIR / studied design / responsive roles"]
  IR --> B["WebAddress HTML, HTMX and scoped browser interactions"]
  B --> V["Viewer context + publication/authority readback"]
~~~

The **27-section authoring parity**, generated component vocabulary, **91 Stitch references mapped into 14 reusable patterns**, native Markdown/editorial rendering, live CMS revision boundary, and real compiled Frappe Lead example establish a more consequential architecture than visual design software on its own. These are verified development contracts and controlled test results, not a guarantee of complete 90-article public-site parity. Puck, GrapesJS and MD4C are not invented here; the distinctive design is their combined owner-preserving native compiler, shared across commercial and noncommercial experiences.

## 11. Four public entrances, site addressing and release verification

~~~mermaid
flowchart TB
  M["One Matrix subject and site program"] --> P["PageComposition + native components"]
  P --> WORK["aurekai.work"]
  P --> LIFE["aurekai.life"]
  P --> LIVE["aurekai.live"]
  P --> SHOP["aurekai.shop"]
  P --> SUB["Personal / client subdomain, custom address"]
  P --> PRE["Review-held draft / preview"]
  WORK & LIFE & LIVE & SHOP & SUB --> CHECK["Public route, content, asset, auth and browser readback"]
  CHECK --> ACT["Staged -> verified -> approved activation"]
  PRE -. "Not public" .-> CHECK
~~~

A working local editor, a source candidate, a successful release build, the GitHub Pages reference implementation, and a deployed personal domain are five different things. The 90-journal/page-parity and individual-host rollout targets must not be marked accepted without **actual live readback** of the intended articles, composition, navigation, audience and playback. The registries' implementation status is not a live-site receipt.

## 12. Coverage index — capabilities, not screenshot examples

| System family | Native or canonical source | What the simplified gallery previously lost |
|---|---|---|
| Identity, standing addresses and public presence | Matrix / WebAddress / Auth | Person, account, organization, household and institution all coexist |
| Group/community and participation | CMS community_body, membership, post, reaction, presence | Real social subjects, membership and group-scoped disclosure |
| Information policy / masks | InformationPolicy, AddressPlane | Exact field audience cuts; eligibility separate from truth |
| Native connections / account issuance | ConnectionCatalog, ProviderEstate, Credentials | Candidate discovery through provider tenants and destination-bound installs |
| Repository services | RepositoryFabric / GitHub / Git / Forgejo / Josh | Checkout, filtered source views, writes, hooks and provenance |
| Source collection and source frontier | SourceFrontier / ExternalEstate | Root/endpoints, candidate/admitted distinctions and coverage gaps |
| Retained observation | ForeignTwin / Project | Robots, exact bytes, digests, source identity, no presumed permission |
| Publication and editorial | CMS, WebAddress, SitePresentation | Rich profile, journal, independent sites, revisions, pages, rights |
| Activity, feeds and app continuations | CapabilityProgram / installed app intakes | One provenance, many *optional* workflows |
| Media / audio / video | Narrate, SpeechLoop, Clips, Transcribe, SceneDetect, MoQ | Recording and processing beyond article narration |
| Spatial/temporal mapping | SpatialObservation / BonfyreSpatial / estate field | Query and temporal geo evidence, not a furniture renderer |
| CRM / relationships / operations | Frappe compilers and app owners | Typed business state scoped per institution |
| Finance and commerce | Finance, Economy, Offer, Pay, CommerceChain | Quote vs financial fact vs settlement and delivery |
| Search / retrieval / vector | Vec, Index, Embed, Query, Model | Provenance-bearing retrieval with explicit representation gates |
| Mask-forwarding and ranking | AddressPlane / BitNet ranker | Deterministic exclusions, experimental rankings, exact verification |
| Model compression / inference | FPQ, FPQx, QwenFPQ, MoE, Quant | Tensor transform and runtime parity, not model-name marketing |
| Resource/physical execution | Distribute, SLI, ResourceAdmission, R7 | Explicit model, memory, kernel, device and capacity placement |
| Data and storage | SQLite, Arrow, DuckDB, Parquet, temporal KV, Zarr | Physical persistence and query contracts under all products |
| Work and agent execution | WorkGraph, Fabric, Orchestrate, Swarm, Run | Dependency, scope, execution receipt, promotion and replay |
| Evidence / provenance | Graph, Proof, Evidence, SERM | Verification is not a splash-screen badge |
| Protocol interoperability | MCP, A2A, CloudEvents, x402, Beckn, UCP, ACP | Compatible carriers are not implicit foreign write access |
| Public and account interfaces | Surface, WebAddress, CMS, page compiler | Account-native and public composition across four domains |
| CI, verification and releases | Project previews, tests, ratchet, stage/activate | Source change is not a live deployment |
| Legacy modernization and migration | Source integrations, Frappe/compiler, WebAddress | Customer sites, WordPress, database/hosting migrations as workflows |
| Training, learning and knowledge | LMS / Wiki / training programme owners | Real programs and saved records, not generic dashboards |
| Supplier, catalogue, procurement, room layouts | Commerce / Catalogue / spatial | Product/room planning is *one branch* of commerce and geometry |
| Civic transparency / public records | ForeignTwin / spatial / publication / civic | Grounded public evidence and safe aggregate maps; not camera targeting |

This index describes families with different maturity, not a uniform shipped feature set. Where source was archived, local-only, SHADOW, unadmitted, review-held, or missing live readback, say so on the consuming page.

## 13. Lossless registry coverage — all 25 families

The registry's 2026-11-13 source-checked export contains **557 architecture entries across 25 distinct family keys**, plus **562 interaction contracts**. This table deliberately retains the **native family keys** rather than silently merging categories because a project demo makes them seem similar.

| Native family | Entries | Native family | Entries | Native family | Entries |
|---|---:|---|---:|---|---:|
| apps | 24 | capability | 8 | command | 180 |
| commands | 16 | commons | 16 | context | 6 |
| economic | 19 | evidence | 15 | execution | 19 |
| external | 39 | fabric | 6 | forms | 2 |
| forwarding | 4 | identity | 11 | institutional | 26 |
| learning | 6 | model | 13 | opportunity | 5 |
| optimization | 22 | provider | 17 | representation | 2 |
| spatial | 2 | storage | 2 | substrate | 17 |
| surface | 80 | **Total** | **557** | | |

The **surface** family alone contains 80 entries; **external** holds 39; **institutional** 26; **optimization** 22; **economic** 19; **model** 13. A handful of marketing diagrams could never represent those families without loss. The 180 `command` and 16 `commands` entries are distinct registry keys, *not* a claim of 196 deployable binaries. For individual child architectures, source paths, forbidden inferences, interactions and witnesses, use the [lossless native registry](../../architecture/README.md).

## 14. Real native generated views, not manually curated demo diagrams

The native [architecture registry](../../architecture/README.md) already supports `view`, `expand`, `get`, `search`, `maturity`, `loss` and `validate`. Its **11 reversible generated summaries** were checked against the current source, not invented to fit a marketing story:

| Native generated view | Exact registered children |
|---|---|
| IdentityView | ActorGraph; HashIdentityGraph |
| InstitutionalView | AuthorityGraph; RelationshipGraph; ShadowInstitutionGraph |
| ExternalView | ForeignTwinGraph; ServiceLifecycleGraph; ResultReceiptBundleGraph |
| ProviderResourceView | ProviderStateFold; ResourceAdmissionGraph; ServiceLifecycleGraph; ForeignTwinGraph; ResourceActivationGraph |
| ModelView | FPQTransformationGraph; FPQSeedProgramGraph; FPQModeSelectionGraph; SLIExecutionGraph; KVTemporalGraph |
| SubstrateView | YaFF; Automerge; Feldera; egglog; HVM4; GigaToken; Lance; Daft; Tract; Burn; CubeCL; Hydro; Restate; Verus |
| FormsView | FormGraph; AtomicForm |
| CapabilityView | CapabilityLattice; DisCIPLCompositionGraph; CapabilityClosureGraph |
| ExecutionView | WorkGraph; OccurrenceSpine; TransportPhenotypeGraph; CapabilityClosureGraph |
| EvidenceView | ProofFrontierGraph; EvidenceBindingGraph; SERMReductionGraph |
| OpportunityView | BlockerGraph; UnlockGraph; ResourceActivationGraph |

~~~bash
# Generate a native view from real declarations, not screenshot fixtures
bun architecture/atlas.mjs view ModelView
bun architecture/atlas.mjs expand ExecutionView
bun architecture/atlas.mjs get repository-fabric
bun architecture/atlas.mjs search spatial
bun architecture/atlas.mjs loss
bun architecture/atlas.mjs validate
~~~

**The 11 registry views are not an exhaustive user-facing information architecture.** Social communities, spatial and geographic observations, Git provider management, business apps, editorial/media, institution tenancy, commerce and account/public composition have individual registered owners even when none of the existing generated views names them. Add a generated view without removing the underlying nodes. Do not reclassify a domain as “covered” because one client demo happens to mention it.

## 15. Read and change the right source

1. **Discovery:** query [the canonical registry](../../architecture/README.md) for the owner and declared interaction, or its 11 expandable views.
2. **Execution truth:** check the exact current source file, binary/test and witnessed receipt; never infer it from a label, theme, demo, marketing feature or old library.
3. **Experience truth:** check the account/application's saved owner record, allowed audience, editor and composition path.
4. **Live truth:** check a pinned release and public/private readbacks across the affected domains; a successful local test does not imply activation.
5. **Gaps:** use explicit held/unknown/DEFER states. Refuse unverified parity and speculative provider, spatial, model or financial claims.

The other diagrams in this directory are **drill-downs**, not a closed taxonomy. Extend this system-wide index when a newly witnessed native contract arrives; do not make the most recent client demo the organizing principle.
