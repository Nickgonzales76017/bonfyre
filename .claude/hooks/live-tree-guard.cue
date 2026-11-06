package hooks

// Authored destructive-command boundaries. The hook evaluates these in order.
liveTreeRules: [
  {
    pattern: #"\bumount\s+(-f|-l)|\bdiskutil\s+unmount\s+force|\bfusermount\s+-u"#
    reason: "Force-unmounting BonfyreFS wedges the mount and takes fabric.db's live readers with it. Stop the daemon through its unit instead."
  },
  {
    pattern: #"\brm\b[^|;]*/opt/bonfyre/releases/(command-plane-sealed|command-plane-current|matrix-edge|native-current|recursive-hypervisor-v6-1785391843|bonfyre-src-2026-07-21T21-50-30-375Z)"#
    reason: "That release tree is live: a *-current symlink resolves into it, or the address organ is running its binary from it. Archive it first and verify the archive before the original goes."
  },
  {
    pattern: #"\bgit\s+gc\b[^|;]*--prune"#
    reason: "This working tree carries deletions whose only other copy is an untracked *.archive file. Pruning unreachable objects is not reversible; confirm what is unreachable with git fsck --unreachable first."
  },
  {
    pattern: #"\bgit\s+(reset\s+--hard|checkout\s+--?\s*\.|clean\s+-[a-z]*f)"#
    reason: "This tree has substantial uncommitted state that is not recoverable from the index. Stash or commit before discarding."
  },
  {
    pattern: #"\brm\b[^|;]*/var/lib/bonfyre/(state|cms|matrix|auth)"#
    reason: "That is live institutional state being served right now, not a cache."
  },
  {
    pattern: #"\bsqlite3\s+\S*(fabric|matrix|auth|finance|operator)\.db[^|;]*\b(INSERT|UPDATE|DELETE|DROP|ALTER)\b"#
    reason: "Write to that store through the organ that owns it. A direct write bypasses the receipts and the invariants the organ maintains."
  },
]
