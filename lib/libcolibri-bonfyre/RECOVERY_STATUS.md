# Recovery status (2026-08-06)

The `.c`/`.h` source under `src/` was missing from every branch and every
loose object reachable from normal refs — only compiled `.o`/`.a` artifacts
and the public header were tracked. It was recovered from a dangling Codex
turn-diff checkpoint ref still present in this repo's object database
(`refs/codex/turn-diffs/checkpoints/.../2e77565a-4785-4aab-af40-12f4fbb8333c`),
dated **2026-07-21**.

## What this checkpoint is

- The public header (`include/colibri_bonfyre.h`) recovered from this
  checkpoint is byte-identical (modulo trailing whitespace) to the header
  already tracked in the main tree, so this is very likely the exact source
  the currently-shipped `.a`/`.o` files were built from, or extremely close
  to it.
- It compiles clean from scratch (`make clean && make`), and links into a
  working `bonfyre-moe` binary. `list-archs` and `status` both run correctly.
- One real bug was found and fixed during recovery: `cbf_forward`,
  `cbf_kv_cache_new`, `cbf_kv_cache_free`, and `cbf_engine_add_peer` were each
  defined twice — once as a real implementation in their dedicated file
  (`cbf_forward.c`, `cbf_quic_expert.c`) and once as a leftover stub in
  `cbf_engine.c` explicitly marked "Compatibility fallbacks ... until the
  direct path is wired." The stubs were dead duplicates from before the code
  was split into dedicated files; removed, keeping the real implementations.

## What this checkpoint is NOT

- **It is not the latest state.** A later, uncommitted session (2026-07-31,
  10 days after this checkpoint) reported "cbf_forward has multiple more
  distinct heap-corruption bugs" — work that happened *after* this snapshot
  and is not included here. Whatever fixes or further breakage came out of
  that session are not recovered; only this earlier state was findable.
- **Numerics are not verified correct.** This checkpoint's own
  `docs/COLIBRI_COMPILATION_STATUS.md` (recovered alongside the source)
  documents, as of this snapshot: expert weight unpacking uses placeholder
  zeros instead of real int4→float32 dequantization, KV cache persistence
  uses raw file I/O instead of real lambda-tensor compression, and the
  fragment-cache/QUIC-expert-streaming paths are stubs. None of that has
  been implemented or fixed as part of this recovery — only the duplicate-
  symbol linker bug was fixed.
- **Not tested against real model weights.** No GLM-5.2/DeepSeek/Mixtral
  weights are available in this environment; only the no-model-required code
  paths (`list-archs`, `status`, `--help`) have been exercised. A real
  ASan/inference run against actual weights has not happened.

## Bottom line

This restores the actual, buildable, editable source into version control
where previously there was none — a real precondition for any further native
MoE work — but it is a recovered *earlier* checkpoint with already-documented
incomplete numerics, not a finished or fully debugged inference engine.
Treat any inference output from this build as unverified until real weights,
a real ASan run, and a real accuracy comparison against a reference
implementation happen.
