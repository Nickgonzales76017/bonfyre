Source: https://github.com/sipyourdrink-ltd/bernstein/pull/3695#issuecomment-5265754134
Author: chernistry (COLLABORATOR)
Posted: 2026-08-12T10:54:47Z

---

Reviewed properly - this is good work, and the part I care most about is right: `verify_run_artifacts()` recomputes the address instead of reading it back from the payload. That is the difference between an identity and a stored field, and it is the thing most implementations of this get wrong.

I checked the address for stability rather than taking the description on trust. Windows separators, `./` and `//` and `../` in the URI, dict ordering, and untracked SARIF fields all behave - same input, same address, no leakage. Two cases did not, and since they were a few lines each I fixed them on your branch rather than sending them back:

| Case | Was | Now |
|---|---|---|
| Snippet with CRLF vs LF | different address | same |
| Unicode NFD vs NFC in the snippet | different address | same |

Both are the same class of bug - the same text arriving in a different normal form. A SARIF file produced on Windows and one produced on Linux describing the identical finding were getting different identities, which defeats the point of content addressing across machines.

Pushed as `154c3b04d`, plus `19a092a7f` with 10 tests / 30 cases covering those two properties so they cannot regress, and a docstring noting why this path repairs normal form where `core/tasks/artifacts.py` rejects it. `c56a48b57` merges current `main` - your base predated #3684, which is why `ruff check .` was red on five files you never touched.

Two things you should know, neither of which is on you:

**Semgrep is red for an infrastructure reason.** It failed at `git clone` with `server certificate verification failed`. I ran the exact CI invocation locally against `.semgrep.yml`: 0 findings across 1861 files. A rerun is queued; ignore that one.

**`main` already has a second finding addresser**, `_canonical_finding_bytes` in `core/tasks/artifacts.py`, landed by #3659 while you were working. Yours is the better implementation - the incumbent does `str(raw.get(field, ""))` on every field, so a result missing everything canonicalises to an all-empty preimage and returns a stable address for nothing, which is exactly what #3616 asked us not to do. Reconciling the two is our problem, not yours; I have filed it as #3699 and it explicitly waits for this PR to land first.

So: nothing blocking from me. Flip it out of draft whenever you are ready and I will merge once the checks settle - the draft flag is yours to control, which is why I have not touched it.

---

Follow-up comment, same thread, 2026-08-12T11:28:44Z:

Timing note: I am out of review capacity this week. Whenever you take this out of draft, expect my first pass Friday at the earliest, more likely Sunday.

No rush on your side — #3699 is explicitly blocked on this one and will wait.
