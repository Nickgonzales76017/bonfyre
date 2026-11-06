# Aurekai research consequence compiler — initial institutional case

Development prototype, not published or validated against real institutional actions. Retired 2026-10-27 with no caller: restore `scripts/research/` with `zstd -dc --long=27 ~/.bonfyre/archive/retired/research-consequence-prototype-20261027.tar.zst | tar -xf -` from the repository root.

This reuses Bonfyre's generated InverseCompiler owner predicate grammar (`cmd/BonfyreSemanticOptimizer/generated/inverse-programs.json`) and native public-source receipts. It does not become a second authority, evidence, planning or proof owner. Its outputs are *proposals* for the existing WorkGraph, TargetObligation, ResourceReservation and ProofFrontier owners.

Input: the public SEAR 2026 data-center electricity reproduction check (153 items) and October 1 correction README, pinned to their actual SHA256 digests in `source-manifest.json`. REA analysis may add static implementation evidence later, but the first input is source-available code and author-reported corrections.

`impact-contract.mjs` accepts a typed graph of versioned sources, claims, computations and downstream reviews. It validates owner states against generated native grammar; refuses LIVE world, undischarged effects, cycles and changed source bytes; retains potential dependencies as *potential*; outputs a deterministic impact cut, review tasks and proposal-only native obligations. `sear-energy-case.mjs` builds the case from the exact source bytes and correction tables, recovers the 153 reported statuses, compares published and corrected rankings and supplies an explicitly hypothetical annual energy quantity as an illustration.

The local test suite covers parsers, full synthetic inventory, rank inversions, owner validation, non-admission of effects and hash drift. Tests are independent of live business state. They do **not** establish scientific reproduction of the authors' numerical models. Further work must run the authors' own 104-test suite in a pinned environment, compare against their CI, verify source and publication version alignment, then register actual EvidenceGraph and ProofFrontier receipts if warranted.

The SEAR report labels 9 rows `DIFFERS` and 13 `CODE NOT FOUND`. The latter means the original method was not recoverable, not that the corrected value has been proven by us.

No messages, PRs, applications, funding submissions, external device operations or supplier actions are part of this package.
