# BonfyreContextCut

`BonfyreContextCut` is the downstream context compiler for an AddressPlane →
BitNet → exact-owner material path. It does not retrieve, rank, verify, or
admit source material. It verifies already receipted body digests, tokenizes
each CID once using the declared GigaToken tokenizer passport, and emits a
recoverable purpose-bound context, packed `u32le` token body, and
`ContextCutManifest`.

Every compile requires `--purpose`. Its manifest carries a reconstructable
DecisionPlane frontier, not another resident copy of selected body text.
On-demand compilation splits large bodies into bounded, digest-bound chunks;
each case binds one chunk, its source body and owner
receipt, purpose, gate state, and four independent
relevance/usability/contradiction/instruction judgments. This is a local
projection only: compiling a cut never sends material to a hosted model.
Omitting a source body later requires *every* chunk to pass the conservative
omission test. The original context remains intact. The frontier adapter
rehashes the material manifest and every body before projection.

The token body cache is keyed by `CID × tokenizer passport`. A later context
cut can reuse exact token IDs without retokenizing an unchanged body, but it
must still recheck the current AddressPlane advertisement, information policy,
and receipt digest before it may reference that cached representation.

`context_cut.mjs` is the native compiler entrypoint. Its token batch bridge is
built from the pinned GigaToken 0.10 Rust source via `make` (set
`GIGATOKEN_SOURCE` to the source tree). It links only GigaToken's core modules,
not its Python bindings; neither compile nor execution invokes Python. Pass
`--gigatoken-bin ./bonfyre-gigatoken-native` with the full declared tokenizer
passport. `make test GIGATOKEN_TOKENIZER=/path/to/tokenizer.json` exercises
ADMIT, SHADOW, provider exact-owner evidence, cache reuse, and admission.

Tokenization is a distinct `gigatoken_context_materialization` compute
phenotype. The compiler first rehashes receipt-bound bodies, then requests a
transient native ResourceAdmission grant before tokenization. It releases that
grant before publishing the derived manifest. An admission grant is capacity
only: it neither selects a candidate nor admits a body as evidence.

The persisted 92-candidate BitNet SHADOW frontier replayed through
this native compiler with 29,848 token IDs, two selected bodies, and the exact
prior context and token digests (`76c96166…` and `39dd1eb6…`). This is parity
for that witnessed run, not universal tokenizer or deployment proof.

The manifest is an execution representation only. It is never evidence,
proof, authority, a commitment, or an effect.
