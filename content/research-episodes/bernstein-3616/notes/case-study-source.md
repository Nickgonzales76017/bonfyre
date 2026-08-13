# A security finding's identity should survive irrelevant textual change (line-ending, Unicode normal form, path separator) -- because the thing it identifies has not changed, only its incidental encoding has.

**Status:** held -- Hold until PR #3695 merges (per CURRENT_DIRECTIVE.md). Draft may be written now; do not publish before merge.

## Supporting claims

- verify_run_artifacts() recomputes the finding address from canonical content instead of reading it back from the stored payload -- an identity, not a stored field
- same finding on Windows and Linux SARIF output must resolve to the same address, or content-addressing across machines is defeated
- a second, weaker addresser already existed in main (#3659) that returns a stable address even for an empty/malformed result -- exactly what #3616 asked to avoid

## Evidence packet

- PR #3695 focused suite: 67 tests passed
- chernistry's own local Semgrep run against .semgrep.yml: 0 findings across 1861 files (CI's Semgrep failure was infra: git clone cert verification, unrelated)
- chernistry fixed 2 real stability gaps directly on the branch: CRLF vs LF snippet text, Unicode NFD vs NFC snippet text, both same-normal-form-different-encoding bug class
- commits 154c3b04d (fix) and 19a092a7f (10 tests / 30 cases covering the two properties)

## People and projects

- #3616: original maintainer-authored need for a canonical SARIF finding-payload identity
- #3695: our draft PR implementing it -- canonical serializer, verify_run_artifacts() recomputation, provenance CLI
- chernistry (reviewer/collaborator): verified stability under Windows separators, ./ and ../ in the URI, dict ordering, untracked SARIF fields
- #3699 filed by chernistry, explicitly blocked on #3695 landing first, to reconcile with the weaker addresser in main

## Quotable facts

> "That is the difference between an identity and a stored field, and it is the thing most implementations of this get wrong." -- chernistry, PR #3695 review
> "A SARIF file produced on Windows and one produced on Linux describing the identical finding were getting different identities, which defeats the point of content addressing across machines." -- chernistry

## Bonfyre primitives

Identity Continuity, ArtifactGraph, EvidenceGraph, Canon, Hash, Receipt


## Nontechnical framing

"If I insert three blank lines into a document, did the underlying problem become a different problem?"


## Attribution requirements

credit chernistry by GitHub handle for the CRLF/NFC fix and the #3699 diagnosis, not just as "a reviewer"
