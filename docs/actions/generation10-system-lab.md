# Generation-10 System Lab

This workflow treats the frozen Generation-10 / V8.1 system map as an executable CI contract rather than documentation-only architecture.

## Execution shape

`Generation-10 System Lab` compiles a source change into an **AffectedClosure** and runs four evidence planes:

1. **Semantic circuits** — requirement tests are selected by system owner rather than package. Circuits cross Constitution/BoundaryCompiler, WorkGraph/EffectKernel lifecycle, EvidenceGraph/replay, Frappe projections, provider/context/model, ObjectFabric/Format Commons, MoneyGraph, and Partner/Aurekai boundaries.
2. **Power swarm** — live `cmd/Bonfyre*` command trees are discovered dynamically, reconciled against the frozen 91-public-Power contract, sharded, and compiled with portable hosted-runner flags.
3. **Semantic fault lab** — scheduled, manual-chaos, and workflow-definition changes inject bounded violations of Generation-10 laws. The Action passes only when the control contract fails closed.
4. **ProofBundle fold** — every shard persists a `ReceiptEnvelope` before its verdict is enforced. The final job downloads the independent evidence fragments and seals a content-addressed `ProofBundle`.

A green run is bound to the exact Git SHA, exact Generation-10 contract hash, workflow run attempt, runner environment, and per-test output digests. An earlier green cannot silently survive a source or contract-factor change.

## Canonical contract

`tools/actions/generation10_contract.json` freezes the system-document invariants used by CI:

- 91 public Powers
- 9 Frappe lineages
- 83 Format profiles
- 112 Partner Commons profiles
- 82 deep-promoted specialties
- 410 ActiveClosure projections
- eight conservation laws
- `semantic lineage → execution lineage → kernel-witnessed effect → receipt lineage → evidence lineage → replay lineage`

The live repository can contain more command inventory entries than the canonical public-Power count. That difference is emitted as **topology drift evidence**, not silently normalized.

## Authority boundary

GitHub-hosted jobs run with `contents: read` and export:

- `BONFYRE_CI_NO_EXTERNAL_EFFECTS=1`
- `BONFYRE_AUTHORITY=observe`

No model download, paid-provider call, partner mutation, release, deployment, secret-bearing external effect, or merge belongs to this lab. Behavior requiring external authority should fail closed or be emitted as coverage debt.

## Modes

`affected`
: Map changed paths to the smallest cross-organ closure. Power compilation is limited to changed command trees or an inventory sample when no command changed.

`full`
: Run every semantic circuit and every native Power shard.

`chaos`
: Full mode plus all semantic fault cases.

Workflow-definition or `tools/actions/` changes automatically expand to every circuit and the fault lab so verifier changes verify the verifier itself. The nightly schedule uses `chaos`.

## Second observer

`Generation-10 Proof Observer` is a separate `workflow_run` workflow. It does **not** check out or execute producer code. It downloads the completed run artifacts and independently verifies:

- ProofBundle content address;
- exact producer source SHA;
- exactly one Generation-10 contract factor;
- every referenced receipt file digest;
- every `ReceiptEnvelope.content_sha256`;
- receipt source-factor equality;
- receipt contract-factor equality;
- receipt cardinality.

This separates producing evidence from believing evidence.

## Local compiler checks

```bash
python3 tools/actions/generation10_lab.py contract
python3 -m unittest -v tools/actions/test_generation10_lab.py
python3 tools/actions/generation10_lab.py plan --mode full --head "$(git rev-parse HEAD)"
```

Generated `.generation10/` content is ephemeral Action evidence and should not be committed.
