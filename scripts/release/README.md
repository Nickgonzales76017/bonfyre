# Bonfÿre release convergence

The public Git repository is a projection of the institution's software, not the institution's private operating store.

`converge.py` is the single local release-recovery entry point. It uses the privacy preflight JSON and four fixed candidate manifests already generated outside Git. It is **read-only by default**. In apply mode, it performs bounded, independently verified operations:

1. Confirm the audited HEAD/branch and the **exact two** legacy MCP index additions. Preserve their staged contents with SHA-256 readback before unstaging using Git's native index operation. Refuse extra staged changes.
2. Identify untracked `.archive`, `.bak`, `.old` and `.orig` files only where the same owner has an existing live replacement, and the archived bytes still match the privacy report. Refuse unique archives, changed content, symlinks, oversized files and build configurations still naming the old file.
3. Atomically move admitted archives to a private, same-filesystem quarantine **outside the Git checkout** with path/digest receipts. Retain active runtime records, client packs, historical evidence and the 341 independently unreviewed tracked deletions.
4. Add local-only exclusions for private runtime state and leftover archives to `.git/info/exclude`. This cannot erase tracked history and is not a substitute for the public `.gitignore`.
5. Recheck the source/manifest/report identities, actual file hashes and strong secret signatures. Copy accepted source to an owner-review snapshot outside Git. A candidate is not automatically a licensed public product.
6. Optionally construct an **independent parentless local Git branch** from only admitted source. Its temporary alternate index does not change the development branch or inherit its 150 additional commits. It **never pushes, force-pushes, deploys, or writes production state**.

The older Mac helper failed at `git restore --staged`. The reviewed recovery script `resolve_legacy_mcp_index.py` uses `git update-index --force-remove`, keeping a verified private backup. Convergence calls that owner rather than creating another Git recovery mechanism.

## Execution and testing

The audited checkout is anchored to HEAD `173d984a8e95` on `run8-r2c-convergence-20260823`. The saved private report is expected at `~/Downloads/bonfyre_privacy_preflight.json`, and the exact candidate manifests at `~/Downloads/bonfyre_release_gate/manifests/`.

The bounded operation, when the user-authorized Mac execution surface is available, is:

```bash
/usr/bin/env bash "$HOME/Developer/Bonfyre/scripts/release/finish_private_convergence.sh"
```

The first execution is designed to be noninteractive. Its quarantine and audit receipts are private under `~/Downloads/bonfyre_release_converged_20261012/`. Refusal leaves an explanation instead of calling `git reset --hard`, `git clean`, or silently cleaning a second agent's changes.

Run `python3 -m unittest discover -s scripts/release/tests -p 'test_converge.py' -v` for the disposable-Git regression. **The connected MCP project test profile does not currently expose arbitrary Python execution, so passing an isolated-equivalent test is not evidence this exact Mac test was run.**

## Not certified by convergence

The 341 tracked deletions, live client information, host secrets, all historical commits, 352 previously unscanned paths, third-party licenses, and financial/CRM records remain with their respective owners until specifically reviewed. Root/public gitignore protection and the Atlas pre-commit auto-staging behavior remain separate owner-authorized changes.

The isolated review repository under private Downloads has no remote and a push-refusal hook. It is **not published or approved**. Validate a complete install, explicit license, scoped client rights, a privacy gate on its exact tree and controlled authorization before any push. Existing public remote history is not erased by this review.
