# A research system needs the ability to invalidate its own published claims when new evidence contradicts them -- not just to publish new results.

**Status:** draft -- No blocker -- this episode is already resolved (correction published, PR #1 merged on bonfyre-fpq-benchmark). Ready to move from draft to ready when article is actually written.

## Supporting claims

- the old FPQ-v8/HQQ 994-token comparison was retired from active evidence, not defended, after a full WikiText-2 pass and a failed v9 integrity check
- full-split evidence: 299,078-token shared stream, 299,077 scored next tokens; FP32 PPL 13.8911; HQQ 3-bit/group-64/axis-1 PPL 29.2934 (+110.88%)
- a native-v9 reconstruction attempt was terminated after its first reconstructed tensor reported cosine 1.000515 -- mathematically invalid, so the pack now publishes no full-split FPQ/v9 quality claim rather than a wrong one

## Evidence packet

- checksummed FP32 and HQQ JSON receipts; verifier asserts identical token digests and loss counts
- https://github.com/Nickgonzales76017/bonfyre-fpq-benchmark/pull/1 merged commit be01491afc53aaa2bf0b27b4a6f297b5205d8387

## People and projects

- bonfyre-fpq-benchmark (own repo)
- directory PRs #61 and #10 where the old number was originally published

## Bonfyre primitives

EvidenceGraph, TemporalEvidenceGraph, Proof, Canon, Time, Memory


## Nontechnical framing

claim lifecycle as a general research-integrity idea, not FPQ-specific
