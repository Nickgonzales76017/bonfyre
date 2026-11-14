# Aurekai provider and account onboarding — targets, tenancy, credentials, and real effects

**This directory's services are not 200 installed integrations.** Account onboarding tracks which external accounts an identity can obtain and whether an API or provider effect is eligible. The native ProviderEstate separately plans downstream customer tenancy; InteractionGrammar separately studies public UI/interaction conventions. The person, institution, imported source and action authority remain with their actual owners.

[200 target services](targets.tsv) · [provider auth methods](../provider-auth-capability.mjs) · [public-account locator rules](../account-choices.mjs) · [104 downstream provider prospects](../../../cmd/BonfyreProviderEstate/providers.tsv) · [provider estate tests](../../../cmd/BonfyreProviderEstate/tests/provider_estate_test.sh) · [real workflow map](../../../docs/real-workflows-and-interoperability.md)

## What is tracked and why it cannot be flattened

| Estate | Where it lives | What each row means | What it does **not** mean |
|---|---|---|---|
| **Account enrollment** | [`targets.tsv`](targets.tsv) — 200 named targets | per-service identity group, source tier, Gmail/Proton status, API gate; queue/checkpoints for acquisition and missing decisions | An API token has been granted, a user owns the account, or Aurekai can mutate it |
| **Downstream provider tenancy** | [`providers.tsv`](../../../cmd/BonfyreProviderEstate/providers.tsv) — 104 prospects | cloud/communications/email/commerce/identity/CRM/etc., tenancy primitive, partner/approval path, evidence source | Partner enrollment completed, tenant created, API implemented or customer delegated control |
| **Interaction grammar** | [`BonfyreInteractionGrammar C`](../../../cmd/BonfyreInteractionGrammar/src/main.c) — 20 patterns | studied interaction shape, exact native owner, intended verifier and lowerer | Donor ontology/data cloned, actor membership, media rights or remote API |
| **Public link** | [`account-choices.mjs`](../account-choices.mjs) | syntactically valid HTTPS locator that a signed-in person may save | OAuth sign-in, creator ownership proof, membership or imported content |
| **Credential record** | [BonfyreCredentials](../../../cmd/BonfyreCredentials/src/main.c) | provider-specific sealed value and declared use | Verified remote token/scope, provider read or transaction |
| **Real installed integration** | exact provider owner + destination authorization + code/test/live witness | a particular permitted read, event delivery, account provision or effect | Wider service authority beyond that exact operation |

## Six stages of a provider relationship

~~~mermaid
flowchart LR
  C["Catalogued target"] --> A["Selected account + enrollment mission"]
  A --> I["Identity-specific registration / consent"]
  I --> K["Credential or OAuth grant stored by native owner"]
  K --> V["Remote verification of scope, expiry and provider account"]
  V --> B["Destination-bound binding to source/app/service"]
  B --> E["Specific read, webhook, provision or effect + receipt"]
~~~

A service should remain visibly at the stage it has **actually** reached. The [account-enrollment owner](enrollment.mjs) tracks bounded queue missions, checkpoints, interruption/retry and manually held decisions. The browser extension panel/host is a *participation surface*, not a provider-auth bypass. Credentials are sealed separately; the tracker refuses raw secret-shaped values.

### Named examples

| Service | Existing native preparation | What remains distinct |
|---|---|---|
| **YouTube** | target 011; channel-link parser; user OAuth/PKCE and public Data API key paths declared | signed-in token and scopes, permitted channel/media reads, publish/upload and playback rights |
| **Spotify** | target 017; user PKCE, confidential server flow, separate app-only token candidate; native Spotify “listen” grammar | developer approval/current scope, current user token, catalog/playback authorization and actual media delivery |
| **Patreon** | target 028; public creator profile locator, confidential OAuth proposal, webhook concept | authenticated tier/member data, verified signed webhook, account entitlement and lawful payment state |
| **WordPress / Jetpack** | target 036; OAuth/app-password choices; independent [signed WordPress bridge](../../wordpress/README.md) | site-claim, real WP plugin execution, WordPress publishing permissions, external sync |
| **Cloudflare** | downstream provider with account/zone/Worker/R2 tenancy model and partner route | actual account creation/delegation, scoped token, confirmed effect/readback |
| **Twilio** | downstream provider with subaccount, number, messaging/voice tenancy model | partner enrollment, paid-account policy, account provisioning permission, messaging delivery |
| **GitHub** | provider prospect, GitHub app installation and real repository/fabric operations | which *specific* institution installed the app, exact repo scope, separate Forgejo/Josh and public Pages operations |
| **Shopify** | partner/merchant-store tenancy candidate; commerce interaction-grammar example | authorized customer store, catalog/order API scopes, actual order and settlement |
| **Toast POS** | commercial offer target only | no verified native Toast API/client, tenant admission, menu/order/transaction importer or effect |

Other entries include federated-social protocols, creator media hosts, podcast distributors, communities, publishers, email/telephony services, source hosts and the extensive provider ledger. **Do not convert an unverified row into a marketed integration by changing its visual label.** The [full list](targets.tsv) and [full downstream prospects](../../../cmd/BonfyreProviderEstate/providers.tsv) remain the source of breadth.

## OAuth and source access are different workflows

The [auth-method selector](../provider-auth-capability.mjs) distinguishes OAuth PKCE, confidential OAuth, client credentials, API keys, webhooks, public-only observation, and instance-based or manually reviewed paths. Some methods have `available:false` and a named missing owner. A plan that displays an OAuth method does not verify that the user authorized it or that Aurekai has provider-side rights.

The [public-account reference interface](../account-choices.mjs) recognizes correctly shaped YouTube, Patreon, GitHub, Bluesky, LinkedIn, SoundCloud, Twitch and other public profiles while explicitly warning that saving the link **does not sign into, verify ownership of or import data from that service**.

The independent [WordPress bridge](../../wordpress/README.md) does signed external events and native CMS upsert; the [native InteractionGrammar](../../../cmd/BonfyreInteractionGrammar/src/main.c) studies what a channel or group interface looks like. Neither is a substitute for an OAuth grant or the user's public account reference.

## Source, tests and exit conditions

[Enrollment tests](../tests/enrollment.test.mjs) and the browser panel tests exercise queue stages, recovery, stale revisions, credential sealing and resumption. [ProviderEstate tests](../../../cmd/BonfyreProviderEstate/tests/provider_estate_test.sh) verify tracking tables and redaction/refusal of raw secrets. The account claims, scopes and actual provider effects remain unverified unless the *individual provider owner* has a matching remote result and receipt.

**Required documentation per service:** the account required, provider tenancy primitive, auth method, real API route, available scopes, exact caller, destination ownership, observable external result, receipt, refusal state, and tested rollout. These are not all the same “capability.”
