# Aurekai live testing

Use this guide to exercise the public and signed-in Aurekai surfaces.  The
two accounts below are intentionally shared, free, self-only test members.
They have no operator, publisher, institution, payment, or administrative
grant.  Do not put personal information or private links in them.

## Live entry points

| Territory | Public start | Signed-in start |
| --- | --- | --- |
| LIFE | [aurekai.life](https://aurekai.life/) | [account](https://aurekai.life/account) |
| LIVE | [aurekai.live](https://aurekai.live/) | [account](https://aurekai.live/account) |
| SHOP | [aurekai.shop](https://aurekai.shop/) | [account](https://aurekai.shop/account) |
| WORK | [aurekai.work](https://aurekai.work/) | [account](https://aurekai.work/account) |

Sign in at [aurekai.life/login](https://aurekai.life/login).  The session is
scoped to the territory you are using, so use that territory's `/account`
link after signing in.

## Shared member accounts

| Account | Email | Password | Intended use |
| --- | --- | --- | --- |
| Member One | `aurekai-member-demo@aurekai.test` | `aurekai-demo-member-2026` | Primary signed-in walkthrough |
| Member Two | `aurekai-member-two@aurekai.test` | `aurekai-demo-member-two-2026` | A second browser/session for isolation checks |

These accounts were created and verified against the live Auth owner on
September 27, 2026.  Both can log in, load `/account`, and load the live
self-authoring profile fragment.

## Fast walkthrough

1. Open [LIFE](https://aurekai.life/) in one browser profile or private
   window.  This is the anonymous edition.
2. Sign in as **Member One** at
   [aurekai.life/login](https://aurekai.life/login), then open
   [your account](https://aurekai.life/account).
3. In **Your account**, verify the account identity and the three real
   member areas:

   - **Your Life presence** loads an HTMX-backed self-authoring surface.
   - **Connect your household to an institution** opens only an institution
     whose public material is already admitted to the estate. It does not ask
     the member to create a school page or send a private school credential.
   - **Linked accounts** lets the member add, inspect, and revoke a declared
     external account reference.
   - **Saved material** is the member-scoped collection used by Save actions
     on public material.

4. Under **Your Life presence**, open the embedded profile section.  It
   exposes two owner-scoped flows:

   - **Create my Life presence** creates a self-owned personal presence at a
     chosen lowercase slug.
   - **Observe privately** records a named observation of a public URL for
     that member rather than treating the URL as public publication.

5. Under **Linked accounts**, add a declared link with a provider label,
   account identifier, profile URL, and display name.  Refresh the page, then
   use **Revoke** to remove it.  This is a real persisted relationship flow;
   it is deliberately not a provider OAuth claim.
6. Open an item with a **Save** action while signed in, save it, and return to
   `/account` to confirm it appears under **Saved material**.
7. In the household section, enter `ischool.syr.edu` and open its public
   world. The institution page shows its exact admitted public material and
   distinguishes public planes from relationship-only planes. The **This
   household holds it** control records only the member's own scoped
   assertion for one plane; use **Release it** immediately after exercising
   it, because these are shared test members. A made-up host and a
   candidate-only source are refused before a household record is created.
8. In a separate private window, sign in as **Member Two**.  Its account,
   saved material, Life-presence controls, private observations, and declared
   account links are separate from Member One's.  This is the quickest way
   to test the signed-in boundary without touching a real user account.

Because these are shared test members, please inspect and revoke any link or
presence you create during a test.  For a durable personal presence, create
your own account at [signup](https://aurekai.life/signup).

## What is intentionally available today

| Flow | Expected result |
| --- | --- |
| Anonymous public territory | A public, cacheable territory edition without an account requirement |
| Signup and login | Auth creates a member and issues an HttpOnly territory session |
| Account | A member-owned account surface, not a generic profile mockup |
| Personal presence | A self-owned Life presence can be created from the account surface |
| Private observation | A public URL can be retained as that member's private observation |
| Public institution reader | An admitted institution exposes its exact public calendar/feed/document/data/page addresses before adoption |
| Household connection | A member can establish or release one private household assertion for one non-public institution plane; unadmitted and candidate-only hosts are refused |
| Declared external account | Add/read/revoke a member-owned account reference |
| Saved material | Save actions write to the signed-in member's own collection |
| Two-member boundary | The two demo accounts do not share member-owned state |

## What this guide does not claim

- A declared account link is not OAuth authorization and does not import or
  control the remote provider.
- The shared members do not have publisher, operator, institution,
  administrative, payment, or device-control authority.
- A personal presence is not automatically a public business, community, or
  organization publication.  Those require their own ownership and authority
  path.

## Local regression witness

From the repository root:

```sh
rtk make -C cmd/BonfyreWebAddress bonfyre-web-address
rtk sh cmd/BonfyreWebAddress/tests/auth_surface_test.sh cmd/BonfyreWebAddress/bonfyre-web-address
rtk sh cmd/BonfyreWebAddress/tests/external_account_surface_test.sh cmd/BonfyreWebAddress/bonfyre-web-address
rtk sh cmd/BonfyreWebAddress/tests/household_plane_test.sh cmd/BonfyreWebAddress/bonfyre-web-address
rtk bun architecture/atlas.mjs validate
```

The first auth witness exercises signup, login, session transport, account
resolution, and logout/revocation behavior.  The second exercises the human
account page, self-authoring fragment, dedicated session-host asset delivery,
and external-account add/read/revoke lifecycle. The household witness proves
that public institution material is rendered from admitted channels, a
candidate-only root cannot become a public institution or household
connection, and one household's non-public plane remains invisible to every
other reader.
