# Aurekai / Bonfÿre — Business Systems & Integration Delivery

**Nick Gonzales | Dallas, TX | Remote | English / Spanish**

I develop software that turns existing business systems, documents, APIs and human decisions into reliable operations. I work across PHP/WordPress, Node.js/TypeScript, Python, SQL, native C and AI tooling. The useful deliverable is not a chatbot or a new dashboard for its own sake: it is the customer's existing job completed, checked, and handed back to the system that owns it.

**The full engine:** [Bonfÿre](../README.md) is the native execution and institutional substrate within Aurekai. The [standalone agent-trace verifier](https://github.com/Nickgonzales76017/bonfyre-agent-trace-verifier) is one early, separately distributed component—not the scope of the full engineering work.

## Start with the work, not the stack

These are working public examples or published walkthroughs. The sample businesses, studio members and shipment records are illustrative; they don't represent customer contracts or access to external accounts.

| Business decision | What you can see or try | Current example |
|---|---|---|
| **Plan a new retail space** | Move real catalogue fixtures, compare source prices and assemble a reviewable brief | [Retail planner](https://aurekai-work-samples.pages.dev/red3-planner/) |
| **Prepare a product catalogue change** | Review editable fields, preserve originals, save and submit a proposal without changing the merchant's source | [Product workspace](https://aurekai-work-samples.pages.dev/catalogue/) |
| **Hand messy records to an operator** | Reconcile fictional shipment files, spot duplicates and missing proofs, prepare the receiving list | [Shipment document handoff](https://aurekai-work-samples.pages.dev/archive/) |
| **Operate separate customer identities** | Compare a ceramics studio, bicycle workshop and bakery with individual styles and offers from a shared site system | [Three businesses](https://docs.aurekai.work/three-businesses) |
| **Publish reviewed member programs** | See creator-side requests, versioned programs and member views across sample studios | [Studio application](https://aurekai-work-samples.pages.dev/training/) |
| **Find a source, not an invented answer** | Retrieve local document passages and source-linked notes | [Document search workspace](https://aurekai-work-samples.pages.dev/documents/) |

## Work that compounds across industries

Aurekai takes on an individual business problem, but the software below it is reusable. The same source/authority/work/receipt pattern is useful in:

- **ERP, agencies and international trade:** supplier or customer documents, typed imports, localized fields, approval queues, ERPNext/Frappe/WordPress synchronization and actual receiving-system readback.
- **Professional services and finance:** scoped proposals, CRM follow-up, statement reconciliation, exceptions, invoices and separate evidence of settlement.
- **Content, audio and creator businesses:** recording intake, editorial review, reusable media, audience-specific publishing and subscription operations with separate payment authority.
- **Community and institutional operations:** membership, proposals, governance, events, education, shared resources, and role-specific sites.
- **Local business and property workflows:** sourced observations, intake, inspections, appointments, procurement and handoffs to the responsible operator.
- **Developer and agent platforms:** native libraries, typed tool contracts, local model/data operations, CLI, web, MCP and agent-host interfaces over the same operation.

[Operating product families](https://docs.aurekai.work/operating-products) explores the customer side. The [Bonfÿre native source](../README.md) and the [broader developer guides](https://docs.aurekai.work/building-blocks) explain the implementation side. Not every industry path is a separately delivered app, and the current research catalogues candidate library cuts rather than released packages.

## Selected implementation evidence

| Customer problem | Relevant implementation and verified behavior | Evidence |
| --- | --- | --- |
| **WordPress content / CRM / API synchronization** | A JavaScript bridge verifies HMAC-SHA256 webhook signatures, binds site/post/version to a replay identity, and constructs namespaced native CMS entries. The focused assertion script covers valid and invalid signatures and missing identity. | [WordPress bridge](../integrations/wordpress/bridge.mjs) · [regression script](../integrations/wordpress/bridge.test.mjs) |
| **Frappe / ERPNext business logic and migrations** | Native C compiler for Frappe application schemas, DocType fields, conditional presentation and derived contracts. A later local nine-app compilation measured 1,152 DocTypes and 17,003 fields, including explicitly withheld and layout-only fields. That measured pass is later than this public repository snapshot. | [Frappe compiler source](../cmd/BonfyreFrappeCompiler/src/main.c) · [Bonfÿre architecture](../README.md) |
| **Financial data reconciliation / imports** | Native C parser takes normalized statement text through declarative bank/layout packs, checks running balances in integer cents, and refuses unreconciled output; packaged for independent installation through MacPorts. This is a parsing and verification tool, not an assertion of CPA/bookkeeping services. | [Statement Import](https://github.com/Nickgonzales76017/bonfyre-statement-import) · [merged MacPorts contribution](https://github.com/macports/macports-ports/pull/34009) |
| **Legacy business applications** | Development and maintenance through Upper90 Solutions of PHP/MySQL financial and insurance-loss workflows; Node/Express/PostgreSQL operational workflows for an equine practice. Also nonprofit platform and WordPress team development at Digital Cheetah. Customer source code and accounts remain with their owners. | Project background and architecture: [Bonfÿre](../README.md) |
| **AI tool and agent integration** | The current local Bonfÿre estate includes recursive MCP tool/resource/prompt forwarding, typed tool contracts, authority-bound execution, bounded context construction and native/workflow evaluation. A public TypeScript regression contribution tests that API, CLI and MCP presentations agree; another contribution bounds untrusted trace loading. Remote customer deployment is an engagement-specific step. | [agent-inspect merged PR](https://github.com/rajudandigam/agent-inspect/pull/229) · [AgentGuard merged PR](https://github.com/richinmrudul/agentguard/pull/214) |
| **Auditable data handoff** | Public upstream changes include content-addressed SARIF findings and offline signed result bundles with separate verification. | [Bernstein SARIF PR](https://github.com/sipyourdrink-ltd/bernstein/pull/3695) · [signed receipts PR](https://github.com/sipyourdrink-ltd/bernstein/pull/3903) |
| **Client-facing browser workflows** | A deployed Aurekai retail-planning demonstration has a save/reopen/enquiry path. A marked end-to-end intake test observed the submitted options and questions survive receiving storage and native CMS sync without duplicate creation. The demo is Aurekai's, not a paid customer deployment. | [Retail planner](https://aurekai-work-samples.pages.dev/red3-planner/) |

## The engineering pattern behind these projects

1. **Start from the existing application and the buyer's actual decision.** Identify its owners, records, workflows, role boundaries and acceptable outcome.
2. **Use an appropriate carrier, not an imposed stack.** PHP/WordPress, REST/webhooks, Frappe, SQL, TypeScript/Python, native C, hosted or local models, ordinary browser interfaces.
3. **Keep writes explicit and repeat-safe.** Match the input to its source and revision; validate permissions; separate preview, approval and effect; preserve retries and recoverable failure.
4. **Verify at the receiving system.** Return the persisted order, financial entry, customer request or published page—not just a green unit test from an intermediate service.
5. **Make the operation maintainable.** Preserve existing features, use narrow tests and handoff notes, and avoid rebuilding upstream products that already do the job.

## Good first paid engagements

- **ERP, CRM or MCP integration pilot:** one real read-and-review workflow against a documented API; tested auth, tool schema, preview/approval, error handling, retry and receipt.
- **WordPress / ecommerce integration:** a defined content, lead, catalogue, webhook or migration workflow with an end-to-end readback.
- **Financial / data workflow:** reconcile one source to an accepted total, map explicit exceptions, and deliver a reversible import/reporting path.
- **Legacy software maintenance:** trace one production defect through the exact PHP/Node/SQL codepath and deliver an isolated, regression-tested correction.
- **Audio or document processing:** locally process one real supplied media/document sample into reviewed, source-linked outputs and existing delivery/storage.

A proposal should set its fee and dates from the customer's actual workflow and acceptance criteria. The objective is a working paid result that can be repeated and maintained—not selling the buyer an entire private engine when a scoped integration will solve their problem.

## Stack and collaboration

**Primary:** PHP, WordPress, JavaScript/TypeScript, Node.js, Python, MySQL/MariaDB, PostgreSQL, SQL, REST APIs, webhooks, Git, C, Bun, htmx.

**Additional work:** Frappe/ERPNext schema and workflows, local model routing, MCP, browser-first software, audio/document pipelines, Arrow/Parquet and operational cost accounting.

**Contact:** [GitHub profile](https://github.com/Nickgonzales76017). Commercial discussions and contracts can take place through the customer's preferred work marketplace.
