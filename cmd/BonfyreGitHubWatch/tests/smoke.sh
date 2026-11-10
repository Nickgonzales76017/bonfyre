#!/usr/bin/env bash
set -euo pipefail

binary="$1"
root="$(mktemp -d)"
trap 'rm -rf "$root"' EXIT
fixture="$root/notifications.tsv"
database="$root/occurrences.db"

printf '%s\n' \
  $'aurekai/bonfyre\tIssue\tFix proof frontier\tcomment\t2026-08-28T12:00:00Z\thttps://api.github.com/repos/aurekai/bonfyre/issues/42\tnick\thuman\thttps://github.com/aurekai/bonfyre/issues/42#issuecomment-1' \
  $'aurekai/bonfyre\tIssue\tDeploy\tcomment\t2026-08-28T12:01:00Z\thttps://api.github.com/repos/aurekai/bonfyre/issues/43\tvercel[bot]\tbot\thttps://github.com/aurekai/bonfyre/issues/43#issuecomment-1' \
  $'aurekai/bonfyre\tPR\tReview\tassign\t2026-08-28T12:02:00Z\thttps://api.github.com/repos/aurekai/bonfyre/pulls/44\tcollaborator\thuman\thttps://github.com/aurekai/bonfyre/pull/44#issuecomment-1' > "$fixture"

"$binary" ingest --db "$database" --fixture "$fixture" --self nick | grep -q '3 notifications -> 1 occurrences, 2 suppressed as bot/no-op, 0 already recorded'
"$binary" ingest --db "$database" --fixture "$fixture" --self nick | grep -q '3 notifications -> 0 occurrences, 2 suppressed as bot/no-op, 1 already recorded'
echo "github watch native smoke: PASS"
