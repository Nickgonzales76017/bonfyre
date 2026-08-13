# A single scoped contribution can turn out to be blocked by a chain of smaller problems the maintainer didn't ask for -- and closing that chain, not the original patch, becomes the actual work.

**Status:** draft -- Wait until at least one of #214/#216/#217/#218 merges, or #213 moves off draft -- currently mid-review, story isn't resolved yet.

## Supporting claims

- adapter PR #213 was the original ask; maintainer held merge until 4 named blockers were resolved: bounded trace parsing (#203), export redaction (#200), portable evidence paths (#204), hidden CI evidence (#208)
- each blocker became its own PR (#214/#216/#217/#218), verified independently before being linked back as dependency-removal for #213
- a combined tree applying all five branches together was checked conflict-free and passing the full suite before claiming any dependency resolved

## Evidence packet

- PR #214: 1402 unit tests passing after the 2026-08-12 fix, on rebased main
- combined verification across #213/#214/#216/#217/#218 previously reported 1,305 tests passing (per capitalgym STATE.md, round history)

## People and projects

- richinmrudul (maintainer): reviews statically on fork PRs (credentials withheld), requested changes on #214 same night, resolved
- #213 verifier adapter, draft, blocked on upstream #197

## Bonfyre primitives

CapabilityClosureGraph, WorkGraph, ExternalBoundary, ProviderGraph, AuthorityGraph


## Nontechnical framing

"sometimes the favor you were asked for turns out to have four smaller favors hiding inside it"
