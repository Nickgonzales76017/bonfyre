---
title: "A Signed Agent Receipt Still Needs a Second Observer"
status: draft
publication_targets:
  - Blogger
  - Aurekai site
source_state: "Hold until Bernstein project-side verification work is reviewed upstream"
evidence:
  - "https://github.com/sipyourdrink-ltd/bernstein/pull/3903"
  - "https://github.com/sipyourdrink-ltd/bernstein/issues/3880"
  - "https://github.com/Nickgonzales76017/bonfyre-agent-trace-verifier"
---

# A Signed Agent Receipt Still Needs a Second Observer

A coding agent can give you a signed receipt for what it says happened. That is
useful. It is not the same thing as the project independently observing the
same result.

That distinction sounds small until autonomous work crosses a repository
boundary.

A volunteer worker may run tests in its own environment, produce a patch, hash
the logs, bind the result to a project manifest, and sign the resulting bundle.
The bundle can be internally consistent and cryptographically valid. The
project still has a separate question:

> If we check the same submitted commit against *our* declared gates, do we see
> the same outcome?

Those are two evidence lineages, and collapsing them loses information.

## What the receipt proves

Bernstein's merged result-receipt bundle makes a worker submission portable. A
bundle carries the patch and its digest, gate results and log digests, the task
reference, project-manifest digest, sandbox/profile information, worker public
key, and receipt-chain position inside a DSSE/in-toto envelope.

Offline verification can detect a changed patch, changed gate log, wrong key,
broken chain, or a manifest mismatch without rerunning the agent.

That is already much stronger than a comment saying "tests pass."

But it is still a claim about a worker observation.

## The second observer

The project-side check being developed for Bernstein issue #3880 deliberately
forms a second observation instead of reusing the worker's result.

The safe GitHub shape is split in two:

1. A normal `pull_request` workflow checks out the fork with read-only
   permissions and no secrets, verifies the receipt against the project's
   committed manifest, and independently executes the manifest gate argv on
   the submitted commit.
2. A separate `workflow_run` receives only a bounded JSON verdict artifact and
   may publish a Check Run. It never checks out or executes the pull-request
   code.

The split is inconvenient on purpose. Fork-controlled code and a token capable
of writing trusted project state should not inhabit the same execution
context.

## Agreement is structured, not binary

The useful output is not just another green check.

The comparison should preserve at least:

- whether the signed bundle verifies;
- whether its manifest digest was checked against the project's manifest;
- whether the worker and project saw the same gate sequence;
- the worker's attested exit code;
- the project's independently observed exit code;
- both log digests;
- field-level reasons when the observations diverge.

Log digests do not have to be equal for the project run to agree. Honest test
runs can include timing, temporary paths, ordering noise, or other
non-semantic output. Treating byte-identical logs as the verdict would confuse
reproducibility of evidence with equivalence of outcome.

A changed command or exit code is different. Those are part of the declared
work and its observed result.

## Why this matters outside one repository

This pattern generalizes to agent systems, build farms, outsourced compute,
model evaluation, browser automation, and business workflows.

There are three different questions:

1. **Did an actor produce a well-formed, tamper-evident claim?**
2. **Was that actor authorized to perform the effect it claims?**
3. **Did an independent observer reproduce the outcome under the receiving
   institution's policy?**

One scalar reward or one "success" flag cannot answer all three.

In Bonfÿre terms, the signed worker bundle is a `ReceiptEnvelope`. The
project-side rerun is another `ExecutionOccurrence`. Their comparison becomes
evidence, not a reason to overwrite either lineage. If they disagree, that is
a `TraceDivergence` worth preserving. Aurekai can learn from the disagreement
only because the observations remain separate.

That separation is the larger lesson: agent infrastructure gets more useful
when evidence has multiple observers and explicit authority, rather than when
one runtime is trusted to grade its own homework.

---

*Draft status: the donor-side Bernstein receipt bundle is merged. The
project-side verification implementation is still under active contribution
work and this note should not be published as a completed feature until that
state changes.*
