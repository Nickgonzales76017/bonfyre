# Bonfÿre architecture, in pictures

Native vector diagrams of how Aurekai experiences compose over Bonfÿre. These are **views of system owners**, not independent architecture registries or proof that every pictured workflow is deployed. The canonical [Architecture Atlas](../../architecture/README.md) and its witnesses remain authoritative.

## 1. Organizations, sites and domains

![Aurekai organizations and accounts compose business sites, client portals and publications; WebAddress and Matrix resolve verified domains and paths.](site-addressing.svg)

**One identity can operate many distinct sites.** A business site, client portal and publication may share an account and installed applications while retaining separate content, policies, presentation and publication state. A domain is an *address for a site*, not a second organization or a substitute for a site ID.

**Grounded now:** [site_presentation](../../cmd/BonfyreCMS/content-types/site_presentation.json) records site ID, host, subject, territory and profile; [site_program](../../cmd/BonfyreCMS/content-types/site_program.json) declares application/capability composition. [subject_address.c](../../cmd/BonfyreWebAddress/src/subject_address.c) resolves a host to a published subject. WebAddress and Matrix maintain separate publication and address checks.

**Extension, not yet a verified general-purpose feature:** multiple host aliases and delegated organization/client sub-sites, custom-domain proof and TLS lifecycle, redirects, approved external origins, and nested client billing. These belong to explicit address bindings attached to the existing site/subject, not duplicate site presentations or separate Nginx blocks per client.

| Site/address mode | Outcome | Constraint |
|---|---|---|
| Aurekai subdomain | Serve a native site at a verified hostname | Matrix host + territory + audience must agree |
| Verified custom domain | Serve the same site at a customer hostname | Prove control; provision TLS; select canonical URL |
| Redirect | HTTP 301/302 to an explicit destination | Route policy, query preservation and loop prevention |
| Approved external origin | Hand off or proxy an external application | Allowlist, origin isolation and identity boundary |
| Private preview | Inspect draft content through an authenticated route | Never infer publication from a working URL |

Practical default: use **acme.aurekai.work** for an independent brand; use **/team**, **/journal**, **/clients** and **/apps** for sections inside that site. A child organization gets a separate subject and authority boundary when it owns its own members, records or obligations—not just because a navigation item is nested.

## 2. One source, many installed applications

![Retained source reaches matching installed application continuations: CRM, narration and collections.](source-to-apps.svg)

```mermaid
flowchart LR
    S["Source Library: retained item and provenance"] --> T["Typed feed-use projection"]
    T --> D{"Installed app declares matching intake?"}
    D -->|"No"| H["No continuation"]
    D -->|"Yes and authorized"| C["CRM: relationship or opportunity"]
    D -->|"Yes and authorized"| N["Creator studio: narration"]
    D -->|"Yes and authorized"| P["Collection / reading room"]
    C --> F["Finance only after its own approved event"]
```

One source identity can support different applications without repeated imports. Each receiving app owns its derived records and actions. A compatible intake is **not** evidence of a completed CRM write, published audio or a financial transaction. Source admission, installed capabilities and effect permission stay separate.

Related contracts: [capability programs](../../cmd/BonfyreCMS/content-types/capability_program.json), [site programs](../../cmd/BonfyreCMS/content-types/site_program.json), [business and applications model](../aurekai-bonfyre-model.md).

## 3. Editorial composition and publication

![Design, structure and article editing converge on CMS pages and fragments, with review before public profile, audio and distribution.](editorial-composition.svg)

```mermaid
flowchart TB
    G["Visual design"] --> A["Shared authored document"]
    P["Component structure"] --> A
    E["Article content"] --> A
    A --> CMS["BonfÿreCMS: site_page + site_fragment"]
    CMS --> Q{"Revision, rights and publication approved?"}
    Q -->|"Not yet"| D["Private draft / review queue"]
    Q -->|"Yes"| W["WebAddress: published site route"]
    W --> J["Journal and profile"]
    W --> N["Narration / audio edition"]
    W --> X["Channel and collection continuations"]
```

Design, structure and article content should compose **one document**, not three island editors or static imports. CMS retains the page/fragment coordinates, audience and status. An editor preview is not a public release. Narration is another edition of the article, not a competing canonical post.

Implementation references: [site fragments](../../cmd/BonfyreCMS/content-types/site_fragment.json), [profile publisher](../../cmd/BonfyreWebAddress/src/profile_home.h), [Pages runtime](../pages-runtime.md). Editor integration and personal-domain rollout have separate verification gates; this graphic does not assert that every site is live.

## 4. Work, execution and proof

![WorkGraph and CapabilityClosure enter Authority and EffectKernel; execution leaves receipts, witnesses and evidence.](work-to-evidence.svg)

```mermaid
flowchart TB
    W["WorkGraph: mission and dependencies"] --> C["CapabilityClosure: compatible realization"]
    C --> A{"Authority and EffectKernel: allowed effect?"}
    A -->|"Denied or held"| H["Hold, review or safe fallback"]
    A -->|"Admitted"| X["Native / local model / bounded agent / human"]
    X --> R["ReceiptEnvelope + WitnessDAG"]
    R --> E["EvidenceGraph + trace divergence"]
    E --> V["Replay / shadow / backtesting"]
    V --> G{"Promotion review"}
    G -->|"Proven"| I["Reusable Aurekai application / practice"]
    G -->|"Not proven"| H
```

An installed command is not a proven capability. A receipt is evidence of an occurrence, not universal proof of correctness. Agent providers are bounded carriers; they do not own semantic work or effects. The [Atlas](../../architecture/README.md) records maturity and witnesses separately from these collapsed views.

## 5. Business application family

```mermaid
flowchart TB
    O["Organization or account workspace"] --> I["Identity + authority + shared typed records"]
    I --> B["Frappe/Core: DocTypes and forms"]
    I --> C["CRM: relationships and opportunities"]
    I --> F["ERPNext: finance and procurement"]
    I --> H["HRMS: people and capacity"]
    I --> D["Helpdesk: service and incidents"]
    I --> L["LMS: learning"]
    I --> K["Wiki: knowledge"]
    I --> V["Drive: artifacts and evidence"]
    I --> Q["Insights: analytic projections"]
    C -. "Authorized commercial continuation" .-> F
```

Nine Frappe-derived application grammars can share typed business meaning without sharing unrestricted access. Their presence in the code is not a claim of complete runtime write-back. See [Frappe ×9](../architecture.md#frappe-9) and [federated sources](../../integrations/federated/).

## 6. Product, commerce and spatial work

```mermaid
flowchart LR
    S["Retained product and supplier identity"] --> C["Catalogue and product options"]
    C --> R["Room planning: 2D/3D choices"]
    C --> O["Offer or listing"]
    C --> P["Procurement draft"]
    R --> P
    O --> F{"Finance and fulfillment authorization"}
    P --> F
    F --> E["Recorded business event and evidence"]
```

The same product and options must survive movement between catalogue, room planning and procurement. Price observations, quotes, orders and settled payments are different facts. See [commercial operating model](../aurekai-bonfyre-model.md).

---

**Diagram rule:** name its semantic owner, source reference, publication/effect boundary and evidenced maturity. A visual is a navigation aid, not a substitute for native code or a deployed release receipt.
