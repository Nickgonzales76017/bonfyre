# Dating + social → native Aurekai/Bonfyre

2026-11-10. Four donor repos, one native social layer for dating,
hobbies, friends, nearby people and shared events. Deep integration
means: member profiles stay the identity, communities stay the venue,
inbox stays the conversation, presence stays the liveness — the new
code is only the dating/social DELTA (affinity, discovery, matching,
date-plans, safety edges), never a parallel stack.

## Donor verdicts (surveyed 2026-11-10, clones in /tmp/dating-donors/)

- helloharendra/Complete-Dating-App (Flutter, MIT): UI shell only.
  All services 0-byte except a mock match fabricator; filters, chat,
  matches, profile editing are dead onPressed handlers. Value taken:
  screen inventory (Discover/Matches/Explore/Profile), User DTO shape,
  filter dimensions (distance, age, interests). No code copied.
- duolicious/duolicious (FastAPI+Expo, AGPL-3.0, 1116 files): the only
  real system — quiz→trait→pgvector matching, WS chat with MAM,
  clubs, safety stack (reports, shadow-ban, classifiers), gold
  subscriptions, presence/visitors, location search. AGPL: network use
  of copied code would disclose the combined work — BEHAVIOR
  REIMPLEMENTED CLEAN-ROOM ONLY, no code copied, ever.
- elvan/social-dating-app-nextjs-react (Next.js, NO LICENSE): real
  auth/token flow, Like composite PK + mutual tabs, Pusher chat with
  read cursors, photo-approval queue. Unlicensed = all rights
  reserved: DESIGN REFERENCE ONLY, native originals written.
- start-again-06/AI-Integrated-Dating-App (Express+Expo, MIT): only
  safe code donor. Value: AI date-plan contract (location/intent/
  budget/interests → plans[]+backup+safety_note) and DatePlan
  persist/accept/history — ported WITH attribution AFTER fixing its
  defects (missing `accepted` column, broken index.js requires,
  signup path mismatch, budget math README-only).

## Organ map (delta only; existing organs unchanged)

- person_affinity (NEW CMS type): actor→target like/pass/superlike,
  withdrawable. Mutuals computed at read (A→B ∧ B→A), never stored.
- Discovery: people index + affinity exclusion + filters, served on
  the member directory; /discover surface stays donor-styled clone.
- Matching: interests overlap + recency + grid distance natively;
  trait-quiz + vector match% later (clean-room duo behavior).
- person_interest (NEW): actor interest labels w/ weights, feeds
  matching + event affinity + friend suggestions.
- Date-plans (NEW type + metered capability): MIT port of ai donor,
  fixed; LLM spend through the capability purchase rail.
- Safety: content_flag observation-state exists; ADD reporter edge
  (NEW report type) + photo-approval queue + rate limits; no
  auto-verdicts — flags gate projection, humans decide.
- Presence/visitors: community_presence exists; ADD profile-view edge
  (who viewed, match% snapshot at view time, expiring).
- Chat: mutual match opens inbox thread (existing inbox organ);
  read cursors + typing later.
- Clubs/events: communities exist; ADD interest-club flavor +
  event posts (community_post extension or subtype).
- Gold/subscription: capability purchase rail exists; ADD dating
  entitlement (unlimited likes, see-likers) as a capability program.

## Phase order (leverage-first)

1. Affinity + discovery v1: type, like/pass/withdraw, mutuals,
   directory w/ exclusion + filters. SHIPPED 2026-11-10 (uncommitted):
   person_affinity type, affinity_projection reads, /companions HTML+JSON,
   POST /companions/affinity, preview-verified end to end.
2. Interests + matching v1: interest labels, overlap score, recency,
   grid distance. SHIPPED 2026-11-10 (uncommitted): person_interest type
   (deterministic hash IDs, 24-cap), transparent score (2/shared cap 10 +
   recency), ranked ?sort=match, label search, profile edit UI + public
   chips with shared highlight. Grid distance deferred to P6 location.
3. Date-plans: MIT port + metered LLM + budget math that works.
   SHIPPED 2026-11-10 (uncommitted): date_plan type (accepted from day
   one), MIT-derived prompt + cost admission, Model Commons resolution
   (pin or capability edge), owned-runner execution with stderr reasons,
   mutual-gated companions, meter best-effort, /companions/plans UI.
   No live model fits this machine; golden tests + stub-runner E2E prove
   the path, unconfigured answers an honest 503.
4. Safety + presence: reports, approval queue, rate limits, visitors.
   SHIPPED 2026-11-10 (uncommitted): person_report type (5/day quota),
   block as affinity kind with both-directions enforcement, matches +
   attention views, profile report links. Visitors are derived from
   inbound edges (no per-view writes). Push notifications + photo-approval
   queue deferred to P6 hardening.
5. Clubs + events + gold: interest groups, event affinity, entitlement.
   SHIPPED 2026-11-10 (uncommitted): additive interest_label/event_at/venue
   fields, club directory + join, events + RSVP toggle with counts, club
   discovery filter, gold circle membership with expiry, free quotas
   (1 superlike/day, 3 plans/week) with gold bypass. No invented prices;
   purchase wiring waits on the facilitator decision.
6. Ranking + realtime hardening: trait quiz, vector match%,
   MAM/receipts/typing (clean-room duo behavior).
7. Native-language restyle: companions rows render as shared components.
   SHIPPED 2026-11-11 (uncommitted): ui:action template + pattern spec,
   affinity_projection builds a ui:card/ui:badge/ui:action authoring tree
   lowered by the native renderer; witness asserts ak-pattern ak-card;
   preview served 200 with five component rows live. No row markup left.

## Verification traps found 2026-11-11

- Fresh `sqlite3 .backup` copies of WAL-mode stores carry no wal file, and
  SQLITE_OPEN_READONLY opens of them fail at prepare (SQLITE_CANTOPEN)
  until a first read-write touch. The companions page truncated after its
  filters on pristine preview copies and passed once touched — identical
  binary and data. Fixed at the source: preview.sh and
  preview-snapshot.sh now normalize copies to PRAGMA journal_mode=DELETE
  right after backup. Pre-existing trap, not a projection regression
  (prepare fails before any emit runs).
- Sandboxed runs cannot bind ports or signal prior-session servers; live
  verification used one-shot escalated servers on alternate ports with
  /tmp store copies. The :28911 preview still serves the pre-restyle
  binary until its owner restarts it.
- curl POSTs to loopback preview servers fail spuriously in the sandbox
  (app 403s, once a 421 that exists nowhere in source): the managed
  proxy path mangles them. bun fetch, raw sockets and the fixture
  suites succeed against the same server. Verify all mutations with
  bun, never curl; curl GETs with content-verified bodies are fine.

## Laws

- No donor code enters the tree except MIT with attribution; duo/elvan
  contribute behavior descriptions only.
- No parallel identity/chat/presence/money stack — new tables join
  existing organs by subject address.
- Every slice: CMS type (if any) + native read + test + atlas entry.
- A like is private until mutual; a mutual opens conversation, never
  publishes; a report gates projection, never auto-verdicts.
