# A security finding should not become a different finding because someone inserted three blank lines

**Status:** published draft · **Episode:** bernstein-3616 · **Outcome:** merged upstream 2026-08-13
**Upstream:** [sipyourdrink-ltd/bernstein#3695](https://github.com/sipyourdrink-ltd/bernstein/pull/3695)

---

## The problem

A static analysis tool finds a bug and emits a SARIF record. Later, the same
tool runs again and finds the same bug. Is it the same finding?

If you answer that question with a stored identifier, you have not answered
it — you have deferred it. The identifier says what someone wrote down
earlier. It does not say whether the thing being identified is the same
thing.

The alternative is to compute identity from the content: canonicalise the
finding, hash it, and let the hash *be* the identity. This is content
addressing, and the failure mode is specific and nasty. Anything that
changes the bytes without changing the meaning changes the identity.

A code snippet checked out on Windows has `\r\n` line endings. The same
snippet on Linux has `\n`. Same finding. Different bytes. Different address.
Content addressing that cannot survive a `git clone` on a different
operating system is not content addressing — it is a checksum with
ambitions.

## What was built

`verify_run_artifacts()` recomputes the finding address from canonical
content rather than reading it back from the stored payload.

That sentence is the whole contribution, and the important word is
*recomputes*. A stored field can drift from the content it was computed
from. A recomputed identity cannot. If the content changed, the address
changes and you find out. If the content did not change, the address is
stable no matter how the bytes were transported.

The maintainer, [chernistry](https://github.com/chernistry), put it more
sharply in review:

> That is the difference between an identity and a stored field, and it is
> the thing most implementations of this get wrong.

## The part that was wrong

Here is why external review matters more than internal confidence.

The canonicalisation handled Windows path separators, `./` and `//` and
`../` in URIs, dict ordering, and untracked SARIF fields. It was tested. It
was, in the parts I had thought about, correct.

chernistry did not take the description on trust. He tested address
stability directly and found two cases that failed:

| Case | Was | Now |
|---|---|---|
| Snippet with CRLF vs LF | different address | same |
| Unicode NFD vs NFC in the snippet | different address | same |

Both are the same class of bug: the same text arriving in a different
normal form. And both were invisible from inside the implementation,
because if you have only ever thought about paths and key ordering, you do
not think to normalise the *text*.

He fixed them on the branch himself (`154c3b04d`), added 10 tests across 30
cases so they cannot regress (`19a092a7f`), and documented why this path
repairs normal form where `core/tasks/artifacts.py` deliberately rejects
it. Crediting that properly matters: the contribution that merged is better
than the contribution that was submitted, and the difference is his.

> A SARIF file produced on Windows and one produced on Linux describing the
> identical finding were getting different identities, which defeats the
> point of content addressing across machines.

## The incumbent

While this was in flight, a second finding addresser landed on `main` from
a different PR — `_canonical_finding_bytes` in `core/tasks/artifacts.py`.

It has a specific weakness. It calls `str(raw.get(field, ""))` on every
field, so a result that is missing everything canonicalises to an
all-empty preimage and returns a perfectly stable address — for nothing.
Every malformed finding collides on one identity. That is precisely the
behaviour the original issue asked to avoid: an address that is stable
because it is empty, not because the content is stable.

Reconciling the two is upstream's call and they have filed it as their own
issue. Worth stating plainly: having two addressers is not a scandal, it is
what happens when two people solve the same real problem in the same
window. The useful output is that the weakness in the incumbent is now
written down.

## The generalisable primitive

Strip the SARIF specifics and this is a rule that applies far beyond
security tooling:

> **Identity Continuity.** An identity for X is stable under identity
> continuity if, for any two encodings E₁ and E₂ of the same underlying
> content, `identity(E₁) == identity(E₂)` — computed by canonicalising
> before hashing, and *recomputed from canonical content* rather than read
> back from a stored field.

Two independent consequences, both load-bearing:

1. **Canonicalise before hashing.** Every encoding difference you fail to
   normalise becomes a false distinction. Line endings, Unicode normal
   form, key order, numeric representation (`1` vs `1.0`), path spelling.
2. **Recompute, never read back.** A stored identity is a claim about the
   past. A recomputed identity is a statement about the present.

The same rule showed up again within a day, in a completely different
context. Comparing two agent execution traces to detect a regression, the
first thing that will produce false positives is a dict serialised with
its keys in a different order — identical behaviour, different bytes,
reported as a change. It is the CRLF bug wearing different clothes.

## What this cost and what it produced

No cash. That is worth being precise about, because the temptation is to
describe a merged PR as a business outcome and it is not one.

What it produced is narrower and more durable: a maintainer of a security
tool independently verified a property, improved the implementation,
merged it over an incumbent, and filed dependent work. That is technical
trust, and it is the only thing here that could not have been manufactured
by trying harder.

---

*Attribution: the CRLF/LF and NFD/NFC normalisation fixes, the test corpus
covering them, and the diagnosis of the incumbent addresser are
[chernistry](https://github.com/chernistry)'s work, contributed directly to
the branch during review.*
