# Primitive: Identity Continuity

Filesystem stand-in for an LMS lesson (see `content/README.md` for why).
Every meaningful primitive should carry this shape: formal definition,
nontechnical intuition, a concrete real case, a failure case, an
implementation pointer, an exercise, related primitives, and external proof
— so the architecture becomes teachable material instead of staying buried
in chat history.

## Intuition

Renaming or reformatting something does not necessarily make it a new
thing. A finding, a file, a claim — if the content that actually matters
hasn't changed, its identity shouldn't change either, even if its
incidental encoding did.

## Formal definition

An identity for X is stable under identity continuity if: for any two
encodings E1, E2 of the same underlying content, `identity(E1) ==
identity(E2)`, computed by canonicalizing before hashing rather than
hashing the raw encoding. The identity must be *recomputed from canonical
content*, never read back from a stored field — a stored field can drift
from the content it was computed from; a recomputed identity cannot.

## Real case

Bernstein PR #3695 (SARIF finding identity). `verify_run_artifacts()`
recomputes the finding address from canonical content instead of trusting
a stored value. The maintainer (chernistry) verified this by testing
inputs that *should* be identical after normalization but weren't yet:
CRLF vs LF line endings in a code snippet, and Unicode NFD vs NFC
normalization of the same snippet text — both cases where the same
underlying finding was getting two different addresses because the raw
bytes differed even though the content didn't. Full verbatim review:
`content/research-episodes/bernstein-3616/receipts/maintainer-review-verbatim.md`.

## Failure case

The counterexample landed in the same PR thread: `main` already contained
a *second*, weaker finding-addresser (`_canonical_finding_bytes` in
`core/tasks/artifacts.py`, from a different PR, #3659) that computes
`str(raw.get(field, ""))` per field. A finding missing every field
canonicalizes to an all-empty preimage — and returns a *stable identity for
nothing*. That's identity continuity's evil twin: an implementation that's
technically deterministic but has stopped actually identifying the thing
it claims to identify. Reconciling the two addressers became its own
tracked issue (#3699), explicitly blocked on #3695 landing first.

## Bonfyre connection

`Entity`, `Canon`, `Hash`, `ArtifactGraph`. The same
question this primitive asks about a SARIF finding applies to a video
clip, an invoice, a research claim, or a repository artifact: what's the
canonical preimage, and does the identity function actually recompute from
it, or does it trust a cached value that can drift?

## Exercise

Design a stable identity for each of the following. For each, name the
canonical preimage (what actually has to match) and one plausible
encoding difference that should *not* change the identity:

- an invoice (should PDF re-rendering change its identity?)
- a video clip (should re-encoding at a different bitrate?)
- a code finding (this lesson's real case)
- a research claim (should rephrasing the same claim change it?)

## Advanced

Relationship to `ForeignTwin` and artifact lineage: what happens when the
*same* underlying entity legitimately exists in two systems with two
different canonicalization rules? (This is exactly the #3699 problem —
two addressers, each internally consistent, disagreeing with each other.)
Not resolved here; tracked as an open question on the
`bernstein-identity-continuity` content genome (`capital.db`).
