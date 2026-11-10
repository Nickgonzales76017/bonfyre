# Provider estate: tonight's sign-up run

Source: the October 26 thread that began with Cloudflare's startup reply. Cloudflare said a bootstrapped (self-funded) Aurekai can be considered for Tier 3. Apply with the company-domain email and the Cloudflare account that actually runs Aurekai. Startup credits cover Aurekai's own product only. Customer resources stay in customer-owned accounts.

The goal tonight is not 102 sign-ups. It is an initial provider estate in which every account you open leaves behind a usable record: who owns it, the Aurekai organization inside it, its billing boundary, a machine identity, the container a customer would get, and the route to partner status.

## Before the first sign-up: the identity pack

Use one identity everywhere. Mismatched names, addresses or domains are what later fail business verification and partner review.

- Legal name; exact entity name and any trade name; EIN and its IRS letter (CP575 or 147C) if you have one. Twilio checks the business name against the EIN record.
- Business address, a phone that receives SMS, and a card whose billing details match.
- A person-named mailbox on the company domain for sign-ups and partner forms. Add `billing@`, `security@` and `ops@` later.
- Website, GitHub organization, logo, privacy policy, terms and a support page. For SMS work also: opt-in flow, sample messages and HELP/STOP handling. Twilio wants a working, public site tied to the business name.
- One description, used every time:

> Aurekai builds software for operating workflows and digital infrastructure across customer-owned services. The platform connects authorized customer accounts, executes scoped workflows across providers, and maintains account ownership, permissions, evidence, and operational state without taking ownership away from the customer.

Do not invent anything to get through an onboarding form.

## The tracker

Everything lives in `cmd/BonfyreProviderEstate/`:

- `providers.tsv`: the 102 targets, their wave, tags, what each manages, the customer container and partner route, and an `evidence` link to the page each came from. All 102 have a researched answer. Six say `none found`: Freshworks, Fastly, Buildkite, Namecheap, Porkbun and Paddle publish no way for a partner to create a customer's account, so for those Aurekai works inside an account the customer opens. Many mechanisms are gated: Datadog multi-org and Vultr sub-accounts are switched on by the provider, Telnyx managed accounts need approval, Zendesk's reseller API needs a reseller token, and Duo, JumpCloud and CrowdStrike child accounts are for MSP partners.
- `acquisition.tsv`: your row per provider. Fill it in as you go.

Never paste a key, token or recovery code into it. Put the secret in 1Password or Keychain and write where it lives (`op://Aurekai/Twilio/api-key`, `keychain:aurekai-github`). `check` refuses anything that reads like a credential.

```
cd cmd/BonfyreProviderEstate && make
./bonfyre-provider-estate next --wave A      # what to open next and what to have in hand
./bonfyre-provider-estate check              # run after each edit
./bonfyre-provider-estate summary            # counts by stage, wave and what is still needed
./bonfyre-provider-estate matrix             # the six tenancy questions, per provider
```

Tags: **A** self-service account, **O** create an organization, **M** machine/API identity, **P** partner program, **K** business verification expected, **D** company domain matters, **C** card needed early, **S** the multi-account feature is sales-gated. They are triage, not a promise about any provider's current form.

## Stages

Each stage is a claim about what is already recorded, and `check` holds you to it:

| Stage | Means |
|---|---|
| `account` | owner email recorded |
| `organization` | an Aurekai organization or team exists |
| `secured` | two-factor on, recovery material saved somewhere named |
| `machine` | one API token, OAuth app or service account, stored by reference |
| `recorded` | account or organization id written down |
| `partner_submitted` | partner application sent (status past bookmarked) |

For sales-gated providers, success tonight is an account if one is offered, the partner form sent, and the reference number written down. Approval is not tonight's job.

## Order

1. **Wave A, self-service:** GitHub, Vercel, DigitalOcean, GitLab, Supabase, MongoDB Atlas, Sentry, Grafana Cloud, New Relic, Datadog, Docker Hub, CircleCI, Buildkite, Cloudsmith, Airtable, Notion, Postmark, Resend, Mailgun, Twilio, Sinch, Vonage, Telnyx. For CI and package hosts: user, Aurekai organization, one empty test project, machine identity. Do not build workloads tonight.
2. **Wave B, cloud hierarchy:** Cloudflare, AWS, Azure, Google Cloud, Oracle, IBM, Akamai/Linode, Vultr, Hetzner, OVHcloud. Azure wants a phone and a non-prepaid card. A real Google organization needs the company domain verified.
3. **Wave C, communications:** Bandwidth, Plivo, Bird. Twilio, Vonage and Sinch already went in wave A.
4. **Wave D, after the identity pack is clean:** payments (Stripe Connect, Adyen for Platforms, PayPal, Square, Paddle, Chargebee, Recurly), Shopify as a **Partner**, identity (Workspace, Microsoft 365, Okta, Auth0, WorkOS, Clerk, JumpCloud, Duo), CRM and support, collaboration, data, security, deployment, and domains. For domain registrars, open the account and turn on two-factor, but do not move Aurekai's own domains tonight.

### Twilio, specifically

Trial: email, then personal phone, then project details; no card. The real target is a paid parent account, then Trust Hub business identity, then subaccounts per customer. US application SMS also needs A2P 10DLC registration, which requires a paid account and tax-ID-backed details. Send the technology-partner application too.

### Cloudflare, specifically

Two separate things. The **startup application** covers Aurekai's own account. The **PowerUP partner application** (resell, manage, distribute, consult) covers customer-facing work. Do not mix them.

## Three kinds of partner relationship

- **Technology / ISV:** Aurekai builds the provider into its product (Twilio, Vercel, Snowflake). Apply for this first where it exists.
- **Services / consulting:** Aurekai operates deployments for customers (Cloudflare manage/consult, Atlassian Solution Partner, Datadog services).
- **Reseller / MSP:** large-scale account issuance (Cloudflare resell/manage/distribute). Apply later.

HubSpot replaced its old provider program in 2026 with a paid Solutions Partner membership. Open the normal account and start the application, but don't pay just to check a box.

## The six questions

For each provider, answer these in `acquisition.tsv` once you have seen the console or docs. `unknown` is the honest default.

1. Can Aurekai create the customer's container? (`create_container`)
2. Can Aurekai invite or be delegated authority? (`delegate`)
3. Can Aurekai operate it programmatically? (`operate`)
4. Can Aurekai meter or audit it independently? (`meter`)
5. Can Aurekai suspend or revoke its access? (`revoke`)
6. Can ownership be transferred cleanly? (`transfer`)

These answers are the capability matrix. Subaccounts (Twilio, Vonage, Sinch), create-then-transfer plus collaborator access (Shopify), hierarchical administration (AWS, Cloudflare, Snowflake), federated directories (WorkOS) and regulated account holders (Stripe, Adyen) are the primitives Aurekai should learn once, rather than writing 102 integrations.

What the tracker does not establish: that a credential means Aurekai owns the account, that an account means a partner relationship, or that a provider's current terms match the tags. See `architecture/institutional/provider-estate.yaff`.
