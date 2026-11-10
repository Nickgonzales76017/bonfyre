# Profile sites

Choose **Profile** on desktop, or **Your profile** in the account menu on a phone.
This opens the private profile view on the current Aurekai territory.

## One account and separate public sites

The site selector opens each explicitly registered profile site associated with
the signed-in account. Work, Life, Live and Shop share the same member template,
but each has its own display name, introduction and public posts. Changing a site
selection does not copy or publish content.

Nick's registered sites are nick.aurekai.work, nick.aurekai.life,
nick.aurekai.live and nick.aurekai.shop. LIFE preserves its existing person-nick
site identity. The others have independent site identities. Setup did not create
posts or biographies and did not change private saved or workspace records.

## Saved material and publication

**Saved** is private across the account. Search saved titles, publishers or
collections, open a source, or choose **Share a link here** on the selected site.
The sharing form carries the exact original URL and asks for the author's own
words. It does not copy a private saved excerpt into a public post.

**Write a post** opens the same form without a saved source. The review names the
exact public site before **Publish on this site**. Matching repeated publication
requests reopen the same committed post. Conflicting or withdrawn identities
are refused. **Manage this post** can withdraw a post from this site while
preserving private saves and independently published posts elsewhere.

**Edit this profile** updates only the selected site's public display name and
introduction. A stale form does not overwrite newer values. **View public site**
opens the exact selected address and uses the existing session handoff when the
preview crosses territories.

## Ownership and release

Auth owns accounts and their explicit actor bindings. Matrix owns exact hosts
and active public presences. CMS owns site selection, site-namespaced
introductions and authored posts, and independently scoped private saves.
The shared member renderer is explicitly selected by member-profile-v1;
unrelated custom site presentations are unchanged.

The native release is retained under
~/.bonfyre/releases/profile-site-20261021/. Its frozen acceptance passed
75 tests with 563 assertions across nine profile, navigation, territory,
source, library, QR, audio and presentation suites. Real public readback and
current-session observation are recorded separately in that release directory.
Isolated browser editing tests are not live customer-account write witnesses.

The four selected profile hosts route to the existing native origin rather than
old static subject pages. Their exact DNS/route/rewrite preimages and changes are
retained with the release. This did not replace wildcard Worker routes or the
tunnel configuration.

This iteration supplies introductions, text/link posts, private saved references
and site-specific navigation. It does not claim avatar uploads, rich media
publishing, commerce or community moderation journeys, or completion of the
388-production portfolio. A protected Atlas update was blocked before execution;
that registry change remains pending rather than being represented as applied.

## Community continuation

The October 21 checkout adds a private Communities section and the corresponding
public discovery, local creation, request/approval, conversation and reply flow.
It preserves this document's account-private and per-site publication boundaries.
The tested implementation is described in
[Community participation](community-participation.md). The native web and CMS binaries were promoted with the participant-return
release on October 21. Canonical Atlas registration remains pending.


## My activity

Choose My activity for your private follows and legacy community requests.
Open an item to return to its exact source, stop following, or inspect its
current participation state. Local community memberships continue through
Communities. Neither section is published on any of your profile sites.

The applied release, native test results, live readback and remaining
verification boundaries are recorded in [Participant return](participant-return-candidate.md).


## Account choices and organization invitations

The Account page is now released with named choices instead of manual reference
fields. Choose a personal, organization or household space from the current
account list, inspect it, then open its work. Configuration actions appear only
when the current account has that capability. Searches refresh current access;
a revoked space does not remain available through a cached choice.

Public institutions are searchable by name. Selecting one opens that exact
institution; it does not join it or claim its identity. Duplicate display names
retain separate source addresses. A failed directory provides a retry, not an
input asking the person to supply an internal identifier.

For a public profile link, paste the profile address. Aurekai derives the known
service and account coordinate, presents a review, and saves only after explicit
confirmation. This declares a public link, not OAuth access or imported content.
Changing accounts clears old private choices and open link reviews.

An organization invitation can be opened directly or pasted as the complete
invitation link on /organizations/join. The browser handles its private code.
The recipient reviews the named organization and role before accepting; merely
opening the link does not grant access. Invalid or changed links do not submit.
Invitation secrets stay out of query strings and public display previews.

The frozen account-choices-20261021 release passed 69 registered native tests
with 677 assertions, plus the existing organization lifecycle script. Its public
HTTPS check verified the exact released modules on all four territories, hidden
private content for anonymous visitors, and desktop/phone layout. Authenticated
editing and acceptance used isolated native accounts, not the customer session.

The release changed the web executable, its frozen site files and browser
modules. Existing Auth/CMS executables, databases, account grants, saved work,
profile-site data and operator state were not modified by the promotion. The
previous service configuration and acceptance logs are retained under
~/.bonfyre/releases/account-choices-20261021/.

This is not a claim that every grant programme or skill-backed editor has adopted
the chooser. New content still needs appropriate authoring fields; existing
objects and destinations should be selected from current scoped choices rather
than entered from memory.
