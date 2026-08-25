# GitHub Secure Open Source Fund — application dossier

**Program:** GitHub Secure Open Source Fund
**Form:** https://forms.office.com/r/HeMiufJcMD (Microsoft Forms, interactive sign-in required)
**Intake:** verified 2026-08-13 — *"Applications are open on a rolling basis and will be considered for all Program Sessions."*
**Award:** $10,000 per project — $6,000 during the 3-week program, $2,000 at 6 months, $2,000 at 12 months. Plus $10,000 Azure credits (eligible projects may reach up to $150,000 via Microsoft for Startups).
**Eligibility:** *"Anyone who is a current maintainer of an open source project."* Teams up to 3.
**Applying as:** maintainer of `Nickgonzales76017/bonfyre-agent-trace-verifier` — **not** via the Bernstein contribution, which chernistry maintains.

Everything below is drafted to paste. Nothing is submitted.

---

## Project

**Name:** Bonfyre Agent Trace Verifier
**Repo:** https://github.com/Nickgonzales76017/bonfyre-agent-trace-verifier
**Licence:** MIT
**Maintainer:** Nick Gonzales (@Nickgonzales76017)

**One line:** A deterministic, dependency-free verifier that checks whether a
governed agent execution actually stayed inside its authority, effect and
evidence policy — and emits a content-addressed receipt a reviewer can
re-verify offline.

## What problem it addresses

Agent systems are being given write access to real systems faster than
anyone is checking what they did with it. The dominant evaluation pattern
collapses safety into a single reward score, which hides the specific
failure that matters: an agent that completed the task *and* performed a
mutating effect it had no authority for, or produced a claim with no
evidence attached, or failed a write and never compensated it.

This verifier refuses that collapse. It keeps eight dimensions separate —
task success, semantic consistency, effect safety, authority compliance,
evidence sufficiency, recovery quality, trajectory efficiency, cost — and
fails each independently, so a policy violation cannot be averaged away by
a high task score.

It uses only the Python standard library, makes no network calls, and runs
as a GitHub Action, so it can gate CI without adding supply-chain surface
to the thing it is auditing.

## Why this is a security project, specifically

Three properties are security properties, not quality properties:

1. **Authority compliance.** Every action must map to an authority grant
   actually held by that actor. Effects marked review-gated require a
   named approving reviewer. This is the check that catches privilege
   escalation inside an agent run.
2. **Evidence sufficiency.** A claimed outcome must carry the evidence that
   produced it, attached to the event that produced it. Unevidenced success
   is the substrate of a convincing false report.
3. **Identity continuity of the receipt.** The receipt digest must be a
   genuine identity, not a checksum over transport accidents — otherwise
   audit trails silently fork across machines.

Point 3 was **wrong in this project until 2026-08-13** and the fix is the
most honest thing in this application. See below.

## Track record: security work landing in other people's security tools

The strongest evidence is not this repo's star count. It is that the same
maintainer's security work has been reviewed and merged by maintainers of
*other* security tools, this month:

| Where | What | State |
|---|---|---|
| `sipyourdrink-ltd/bernstein#3695` | Content-addressed SARIF finding identity — recompute the address from canonical content rather than reading back a stored field | **Merged.** Maintainer independently tested address stability, fixed two normalisation cases on the branch, added 10 tests / 30 cases, and merged it over an incumbent implementation |
| `richinmrudul/agentguard#214` | Bound untrusted execution-trace loading; stop echoing attacker-controlled field names and dict keys into error diagnostics | Open, review addressed, 1400 unit tests green locally |
| `richinmrudul/agentguard#217` | Redact recognised credentials from standards exports | Open |
| `richinmrudul/agentguard#216` | Retain hidden workflow evidence | Open |
| `richinmrudul/agentguard#218` | Persist portable evidence paths | Open |
| `confident-ai/deepeval#3019` | Deterministic baseline-vs-candidate trace alignment for agent regression detection; alignment layer delivered, co-developed with another contributor | Open, active |

The Bernstein reviewer's assessment of the core idea, on the public PR:

> That is the difference between an identity and a stored field, and it is
> the thing most implementations of this get wrong.

## The bug we found in ourselves, and what it demonstrates

During that upstream review the maintainer found two cases the submitted
implementation got wrong: a snippet with CRLF versus LF line endings, and
text in Unicode NFD versus NFC normal form, each produced a *different*
content address for an identical finding.

Applying that lesson back to this project found **the identical bug here**.
`canonical_bytes` sorted object keys but normalised no text, so the same
agent trace captured on Windows and on Linux produced different receipt
digests. A digest that changes when a trace crosses an operating system is
not an identity; it is a checksum over an accident of transport.

Fixed in v1.1.0: NFC folding and CRLF/CR → LF normalisation, applied
recursively and to dictionary **keys** as well as values (a key differing
only in normal form would otherwise survive `sort_keys` and change the
preimage). Integral floats fold so `1` and `1.0` agree after a JSON round
trip; booleans are explicitly excluded so `True` does not collapse into
`1`. Receipts now carry `canonicalization_version` so a consumer can tell
two digests are incomparable rather than silently comparing across a
change.

Eleven regression tests, including one asserting that normalisation does
**not** collapse genuinely different content, and one that re-encodes an
entire trace as CRLF and asserts the digest is unchanged. 15 tests pass.

This is what we would want the program to see: an external security review
taught us something, and it was applied back to our own code within a day
rather than filed away.

## What the funding would be used for

The award funds maintainer time on security outcomes. Concretely, in
priority order:

1. **Signed receipts.** The verifier currently emits a content-addressed
   receipt but does not sign it. Receipts should carry a detached signature
   bound to a key the reviewer can pin, so a receipt cannot be forged by
   whoever controls the trace source. This is the single largest gap
   between "useful in CI" and "usable as audit evidence".
2. **Event-source binding.** The verifier checks declared traces; it does
   not prove the source is truthful. Binding event identities to the
   originating system, and making evidence append-only, closes the obvious
   attack: an agent that writes its own flattering trace.
3. **Adversarial trace corpus.** A public fixture set of traces that are
   *designed* to pass a naive checker — unevidenced success, authority
   laundering through a second actor, compensations that do not actually
   compensate, encoding tricks against the digest. Useful to this project
   and reusable by anyone building a comparable checker.
4. **Threat model, written down.** Currently the limits are two sentences
   in the README. They should be a document that states what the verifier
   does and does not defend against, so nobody deploys it believing it
   proves more than it does.

## Honest limitations

Stated plainly, because a security programme should be told these rather
than discover them:

- **The project is early.** Small repo, low star count, not yet a
  dependency other projects rely on. The claim here is not "this is
  critical infrastructure"; it is "this is a focused security tool by a
  maintainer whose security work is being merged into other people's
  security tools."
- **It verifies declared traces.** It does not attest that the event
  source is honest. Items 1 and 2 above are exactly this gap.
- **Single maintainer.** One person, not a team of three.

## Links to include

- Project: https://github.com/Nickgonzales76017/bonfyre-agent-trace-verifier
- Identity-continuity fix: `v1.1.0`, commit `2baf69f`
- Merged upstream security contribution: https://github.com/sipyourdrink-ltd/bernstein/pull/3695
- Case study (the primitive, and the review that corrected it): `content/research-episodes/bernstein-3616/case-study.md`
- Alignment layer for agent regression detection: https://gist.github.com/Nickgonzales76017/fda6a0cdd509b3bb635adc34bd412369

---

## What is left for a human

The form is Microsoft Forms and renders its questions client-side, so the
field list cannot be read or filled programmatically — it needs an
interactive signed-in session. The application also leads to a virtual
interview.

**Submitting a funding application is an identity-asserting act in the
maintainer's name, so the submit itself is deliberately left to the
owner.** Everything that can be prepared in advance is above.
