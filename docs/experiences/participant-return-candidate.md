# Participant return

Released on October 21, 2026. The filename is retained so references from the
original downloaded candidate continue to work.

## Open and continue your activity

Choose Profile > My activity, or open /profile?tab=activity on WORK, LIFE, LIVE
or SHOP. Following and Community requests belong to your private account;
changing profile sites does not publish them. Your public introductions and
posts remain separately scoped to each site.

An activity opens the same selected public object. Before sign-in, its public
material is readable and the sign-in link retains that destination. A deliberate
form records the permitted local action after sign-in. Signing in alone does
not repeat a submission. A local follow is not an external subscription or RSVP.

You can stop following and later follow again after reviewing the current state.
Repeated successful requests preserve the original record and timestamps. Old
creation requests cannot restore a deliberately withdrawn relationship. Existing
membership decisions, roles, expiry and authority references are not reset.

Search uses currently public source titles. When a source becomes private or is
removed, its private relationship remains inspectable without disclosing hidden
source text. Storage failures and stale forms do not return false success.

## Community ownership

Communities with their own owner or moderation policy use the native community
operation, not the legacy observation writer. Profile > Communities retains
its separate participant journey, including requests, acceptance, conversations
and replies. The release preserves the newer community implementation and its
current source; it does not replace it with another membership store.

## Applied release and checks

Source changes were admitted by the project patch owner. Real native testing
found and corrected a header-framing defect: the public server NUL-terminates
the final HTTP header. The corrected parser still refuses foreign origins and
duplicate authorization-relevant headers.

The frozen web and CMS release lives in:
~/.bonfyre/releases/participant-return-20261021-87MQ7N/

Native browser acceptance passed 46 tests and 489 assertions across participant,
community, profile, navigation and four-territory suites. The standalone C tests
passed 24 transition cases and five native-framed header checks. The Foundry
owner test passed. Source files and frozen binary hashes were retained.

Deployment changed only the web executable and its configured CMS executable.
The previous service configuration is retained for rollback. Digests of saved
items, workspace records, follows, memberships, site pages, profile posts and
pack installations/activations were unchanged across promotion. No account,
public post, participation record or sample community was created for live QA.

Real public HTTPS checks passed for participant pages on WORK, LIFE, LIVE and
SHOP, exact guest profile return addresses, and all four nick.aurekai profile
hosts. Desktop and phone views stayed within the viewport; private activity was
absent from public profiles. SHOP had transient TLS connection failures before
successful ordinary certificate-verified and browser checks; no TLS setting was
weakened. Existing browser tabs did not confirm a current signed-in account, so
customer-session verification remains distinct from isolated native sign-in tests.

The broader control-plane suite returned 870 passes, 259 skips and one existing
source-string assertion expecting K.session = async. The protected test update
and new canonical Atlas entry were not applied. Existing Atlas validation passed;
that does not register the new entry. Runtime deployment does not claim otherwise.

This release completes the participant-return increment. It does not establish
business fulfillment, funded-service outcomes, remote membership, notification
delivery or completion of the 388-production portfolio.
