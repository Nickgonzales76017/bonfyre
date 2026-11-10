# Review: relay source_files (fabric_store.mjs / fabric_work.mjs), 2026-10-17

Scope: validateSourceFiles, captureSources, seedSources, verifySourceSnapshot, createWorktree resume path, relay apply gate (fabric_work.mjs ~296-409), project_operator apply (git apply).

## Finding 1 (likely in practice in this repo): unselected dirty files can be patched against a stale view
- The worktree holds the selected files at their current bytes; every other path stays at the Git base. The final patch is applied to the dirty checkout with plain `git apply` (project_operator.mjs:365). That matches context lines only and ignores the patch's `index old..new` blob ids.
- Failure: the agent edits file X, which was not in source_files and has uncommitted changes in the checkout (2,385 changed paths here). If X's hunk context lines are untouched by those changes, the patch applies cleanly onto content the agent never saw. sourceCurrent covers only the selected files, so the relay still reports completed.
- Suggested fix, before apply (both apply-preflight points and the final one): for each path in the final diff's name-status that is not in source_files, require the checkout file to equal base_commit's blob (`git hash-object --no-filters -- X` vs `git rev-parse <base>:X`; absent vs absent). Otherwise refuse with reason `unselected_source_dirty` and name the paths, so the caller can add them to source_files.

## Finding 2 (minor): --check and apply use different whitespace rules
- Preflight uses `git apply --check --whitespace=error-all`; the apply uses `--whitespace=fix` (project_operator.mjs:282 vs 365). The applied bytes can differ from the reviewed patch (patch_sha256), so the receipt describes a patch that was not applied byte for byte. Use the same flag for both, or record the post-apply diff digest.

## Checked and sound
- Symlinks: confined() refuses symlinked components; bytes() opens with O_NOFOLLOW and requires a regular file; size checked before and after read with dev/ino/mtime/ctime equality.
- Paths: absolute, `..`, empty, control characters and `.git` (case-insensitive) are refused; duplicates are compared case-insensitively (right for APFS).
- Ignored files are refused before seeding; deletions use `update-index --force-remove` and refuse a non-regular destination.
- Blob integrity is recomputed (sha1/sha256 by id length) before update-index; the snapshot commit is a separate commit-tree over base; HEAD moves with an expected-old-value update-ref.
- Cancellation: per-file checkSignal/checkTask in seedSources. An interruption after write-tree but before the snapshot receipt leaves the worktree with a modified index, and resume refuses ("incomplete source baseline requires reconciliation"). That's safe; reconciliation is manual.
- Untracked files the agent creates are added with -N and checked, so they can't silently drop out of the patch.
