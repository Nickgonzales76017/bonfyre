# Bonfÿre ↔ Aurekai ownership boundary

**Bonfÿre is a software engine. Aurekai is an operating business.** They have distinct institutional identities, contracts, data ownership, distribution and release authority. This document is a source and review policy for the Bonfÿre repository, not a claim that every historical coupling has already been removed.

[Bonfÿre developer documentation](../README.md) · [Native architecture registry](../architecture/README.md) · [Source integration and data contracts](architecture.md) · [Boundary check](../scripts/check-product-boundary.py)

## The hard line

| Axis | Bonfÿre | Aurekai |
|---|---|---|
| Identity | software project and developer product | operating company, customer-facing brands, public network |
| Source of truth | engine source, ABI, C/Zig/YAFF/CUE contracts, software tests and releases | actual business/account CMS, offer ledger, partnerships, contracts, publications, staff/client relationships and product states |
| Naming | Bonfÿre, bonfyre commands, core library and package identities | Aurekai, Aurekai WORK/LIVE/SHOP/LIFE hostnames, marketing and client properties |
| Money | software development/release cost or engine licensing when real | proposals, pricing, customer revenue/spend, fulfillment and paid relationships when real |
| Runtime authority | exact engine-level resource admission and typed effect interface; **no universal business grant** | each institution's own authenticated tenant, data scope, credentials, provider installation, write/publish/payment/release approvals |
| Publication | versioned documentation and binaries | specific site, article, client work, campaign, media and commerce output |
| Evidence | code/test benchmarks and compiled receipts | commercial contracts, public-facing live readbacks, customer delivery and financial verification |
| Reuse | APIs, compilers, libraries and portable generic formats | business playbooks, brand identity and operating practices, with explicit license/ownership terms |

Aurekai can install Bonfÿre; so can another operator. A Bonfÿre release is **not** an Aurekai release, a Bonfÿre user is **not** an Aurekai customer by default, and an Aurekai business account is **not** an engine test fixture unless the source clearly marks it as such.

## Architectural boundary and direction of dependency

~~~mermaid
flowchart TB
  subgraph ENGINE["Bonfÿre — versioned software boundary"]
    CORE["engine/core + lib/libbonfyre: neutral C ABI, no Aurekai host/customer constants"]
    ORGANS["cmd/Bonfyre*: native owners, compilers, transports and exact runtime APIs"]
    SCHEMAS["YAFF/CUE: generic source, rights, grant, site, output and capability types"]
    RELEASE["Engine source/build/test/versioned binary receipt"]
  end
  subgraph CLIENT["Aurekai — independently owned institution"]
    BUSINESS["Business operations, brands, client relations and economics"]
    TENANT["Installed tenant and current credentials/grants"]
    DOMAINS["aurekai.work / live / shop / life"]
    RECORDS["Aurekai CMS, sources, users, offerings and publication decisions"]
    DELIVERY["Aurekai release, client acceptance and financial outcomes"]
  end
  CORE --> ORGANS
  SCHEMAS --> ORGANS
  ORGANS --> RELEASE
  RELEASE --> INSTALL["Versioned installation and explicit adapter"]
  TENANT --> INSTALL
  INSTALL --> DOMAINS
  BUSINESS --> RECORDS --> DELIVERY
  DOMAINS --> DELIVERY
  INSTALL -. "Only admitted, scoped reads and effects" .-> RECORDS
~~~

The **direction is one-way** at code ownership: generic Bonfÿre core exports contracts; an Aurekai-specific deployment/configuration consumes them. The engine must not import an Aurekai product module, domain, campaign, price list or customer-specific workflow as a universal default. An optional integration can live in a brand-qualified site/pack/deploy module with independent tests, but it is **not** the ABI.

The authority direction is **not inheritance**. Neither product can read or write the other's internal institutional data merely from a namespace prefix, process owner, host or binary name. An installation authenticates to its own institution, selects an actual scope, and proves the exact action/release grant separately.

## Explicit repository ownership map

| Source | Home and rule |
|---|---|
| `engine/core/src`, `engine/core/include`, `lib/libbonfyre/src`, `lib/libbonfyre/include` | **Bonfÿre only.** No hard-coded Aurekai identifier, apex domain, campaign, site slogan or client data. New violations fail CI. |
| `cmd/Bonfyre*`, `schemas/control`, `architecture` | Bonfÿre native command owners and contract definitions. Distinguish portable interface/source test from *Aurekai deployment-specific data*. Existing embedded brand defaults are technical debt, not an endorsed dependency direction. |
| `deploy/`, `site/`, `integrations/browser/` | Mixed historical material. Separate reusable release and site/app shell from Aurekai-owned host, presentation and content configuration via exact deployment binding. No implicit universal tenant. |
| `packs/aurekai*`, `packs/*client*`, marketing docs/work samples | Aurekai-specific or explicitly named downstream business packs. **Move source authority to their real business/client repository or CMS**, retaining only tested adapters/fixtures/lineage in Bonfÿre. Do not delete current source packs during a documentation change. |
| `docs/architecture.md`, `docs/native-data-compute-and-presentation.md`, `architecture/README.md` | Technical Bonfÿre explanations; Aurekai may be referenced as a **deployment example** with exact status, never as the engine's institutional identity. |
| Aurekai commercial messaging, proposals, brands and sales | Aurekai-owned business documentation and websites. Bonfÿre README must never carry “Built on software. Driven by Aurekai” as its own slogan. |

## Release and account boundary

1. **Bonfÿre release:** engine source revision, binary/ABI/package signature, test results and extension contract. No customer data included in the release by default.
2. **Aurekai installation:** destination institution, licensed or otherwise authorized runtime version, exact domain/site scope and sealed connector references. This is *not* a blanket release of four public sites.
3. **Aurekai publication:** CMS revision, selected audience, source rights, a published identity and release readback for each addressed site. No assumed propagation just because an engine binary is installed.
4. **Aurekai commercial effect:** a separately authorized action, price or financial entry. A source lead, operational plan or proposal must not be counted as delivered revenue.
5. **Client subdeployment:** its own account grant, installation and data closure. A client may use Bonfÿre without adopting Aurekai as a principal; an Aurekai consultancy contract does not make Bonfÿre the owner of that client's records.

**History/compatibility:** the `aurekai/aurekai` repository currently publishes `@aurekai/runtime` and `akai` commands with historical `bonfyre` CLI aliases, while `aurekai/native-runtime` holds an older Akai C11 lineage. Those names need a **source-pinned compatibility inventory and migration plan**, not silent package deletion, ABI renaming or misrepresentation of existing users. Neither historical alias establishes present-day institutional identity.

## Physical source relocation — November 13, 2026

**The Aurekai destination is now writable and actual files have moved.** The authoritative business-side repository is [aurekai/aurekai](https://github.com/aurekai/aurekai), with its [ownership contract](https://github.com/aurekai/aurekai/blob/main/OWNERSHIP.md), [tenanted host binding](https://github.com/aurekai/aurekai/blob/main/deploy/tenant-hosts.tsv), [published-site source](https://github.com/aurekai/aurekai/tree/main/sites), [business source](https://github.com/aurekai/aurekai/tree/main/business) and [Git-blob source ledger](https://github.com/aurekai/aurekai/blob/main/docs/business-source-migration.tsv).

The first ten domain/business files are byte-identical copies of their previous Bonfÿre paths. They include territory presentation data, four house designs, route records, Stitch presentation language, observed-publication inputs, nginx's Aurekai deployment carrier, acquisition candidate evidence and work-example/program content. Aurekai's CI validates the ledger, origin host bindings, business identity and legacy package preservation. These are **copies awaiting consumer/release cutover**, not renamed or deleted source assets.

**The functional import split has begun:** the generic Bonfÿre CMS house importer accepts an explicit tenant-owned `--data` path and a read-only check; the typed authored rows importer accepts `--dataset-file territory_presentation=PATH`; the work-examples importer already accepts named source file environment overrides. Aurekai's [install-owned-cms-content.sh](https://github.com/aurekai/aurekai/blob/main/deploy/install-owned-cms-content.sh) binds five business-owned source families—house, front-door, route presentation, Stitch presentation template and work programs—through typed engine interfaces, refuses implicit database creation and requires an exact CMS destination/namespace for writes. [Aurekai CI](https://github.com/aurekai/aurekai/actions/workflows/business-boundary.yml) tests all five against mock CMS endpoints. These bindings are not a claim of production publication. Both repositories' test workflows cover these boundaries.

**Still not complete:** the active WebAddress binary and CMS may still read the retained Bonfÿre-side business copies; the Aurekai source cutover and four-domain public readbacks are not verified, package aliases have not been rewritten or deleted, and historical higher-level site handlers include remaining brand-specific code. The existing runtime release stays selected until site, account, stylesheet, CMS and external provider tests accept the new installation.

## Enforced now vs not yet separated

**Enforced on Bonfÿre `main`:** the [CI product-boundary check](../scripts/check-product-boundary.py) scans shipping core engine/library source for Aurekai brand identifiers and catches future additions. It also checks root developer identity, the separation document and required institutional invariants. Its self-test exercises both allowed source and refusal fixtures.

**Not yet enforced:** every `cmd/` source file, app pack, public HTML/CSS, historical partner repo, CMS business row, actual runtime access grant and live domain. The Bonfÿre engine still contains hardcoded Aurekai route/data names in higher-level site compilers and example surfaces. Those are explicit migration work, not evidence of separation already completed. This document and the CI check do not revoke credentials, change host records, republish a site or migrate npm/PyPI packages.

## No-regression acceptance

- Every new reusable core implementation stays Aurekai-free; its tests may use an Aurekai fixture but must also test a non-Aurekai institution.
- Every new Aurekai/other-client feature enters via an institution-owned pack or exact provider/site adapter, **not** a new brand literal in a generic engine command.
- A source-specific Aurekai page or work item does not become a default Bonfÿre product; it has an exact destination and publication authority.
- Core software releases and Aurekai commercial deployments have **separate receipts, changelogs and acceptance**.
- Platform-generated output says Bonfÿre only for engineering/product capabilities; Aurekai public experiences say Aurekai, with optional unambiguous technology attribution.
- Cross-institution test proves the engine's own grant cannot read a customer record or issue an Aurekai publication/effect without explicit delegation and bounded scope.

**Immediate next release work:** reconcile the active CMS and WebAddress consumers with the Aurekai-owned source ledger; eliminate remaining hard-coded host and content defaults from higher-level routes without losing legacy site and client access; run exact current/non-Aurekai site tests, controlled deployment and public four-apex readback. Keep the existing live release until the new source and domain readback is accepted.
