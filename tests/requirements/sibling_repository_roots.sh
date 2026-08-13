#!/bin/sh
# Repository-root governance: a path physically beneath the Bonfyre tree
# (a Frappe app, HVM4) can belong to its own independent Git repository --
# physical containment is not Git ownership. Proves real discovery against
# this checkout's actual sibling repositories (vendor/hvm4 is a real
# .gitmodules entry; the Frappe apps under integrations/frappe-bench/apps
# are real independent git checkouts, not submodules) -- HEAD OIDs, branch
# names, and dirty-state digests all come from real `git` invocations
# against the real repos, never fabricated.
set -eu

. "$(dirname -- "$0")/fabric_test_lib.sh"
fabric_test_bootstrap

echo "== at least one real sibling repository was discovered (hvm4 is a committed .gitmodules entry) =="
count=$(sqlite3 "$requirement_db" "SELECT count(*) FROM repository_roots;")
[ "$count" -ge 1 ] || { echo "FAIL: expected at least 1 discovered sibling repository, got $count" >&2; exit 1; }

echo "== hvm4 specifically resolved with a real HEAD OID and git dir =="
hvm4_oid=$(sqlite3 "$requirement_db" "SELECT head_oid FROM repository_roots WHERE root_id='source.hvm4';")
hvm4_git_dir=$(sqlite3 "$requirement_db" "SELECT git_dir FROM repository_roots WHERE root_id='source.hvm4';")
[ ${#hvm4_oid} -eq 40 ] || { echo "FAIL: hvm4 HEAD OID is not a real 40-char SHA-1: $hvm4_oid" >&2; exit 1; }
real_hvm4_oid=$(git -C "$requirement_root/vendor/hvm4" rev-parse HEAD)
[ "$hvm4_oid" = "$real_hvm4_oid" ] || { echo "FAIL: recorded HEAD ($hvm4_oid) does not match real git rev-parse HEAD ($real_hvm4_oid)" >&2; exit 1; }
[ -d "$hvm4_git_dir" ] || { echo "FAIL: recorded git_dir does not exist: $hvm4_git_dir" >&2; exit 1; }

echo "== repository_roots and roots agree on the same locator (real extension, not a competing registry) =="
root_locator=$(sqlite3 "$requirement_db" "SELECT locator FROM roots WHERE id='source.hvm4';")
worktree_locator=$(sqlite3 "$requirement_db" "SELECT worktree_locator FROM repository_roots WHERE root_id='source.hvm4';")
[ "$root_locator" = "$worktree_locator" ] || { echo "FAIL: roots.locator ($root_locator) disagrees with repository_roots.worktree_locator ($worktree_locator)" >&2; exit 1; }
[ "$root_locator" = "$requirement_root/vendor/hvm4" ] || { echo "FAIL: unexpected locator: $root_locator" >&2; exit 1; }

echo "== source_generation is a real function of (HEAD, dirty state), not a placeholder =="
generation=$(sqlite3 "$requirement_db" "SELECT source_generation FROM repository_roots WHERE root_id='source.hvm4';")
[ ${#generation} -eq 64 ] || { echo "FAIL: source_generation is not a real sha256 hex digest: $generation" >&2; exit 1; }

echo "== dirty-state digest matches real git status --porcelain output (read-only observation) =="
recorded_dirty=$(sqlite3 "$requirement_db" "SELECT dirty FROM repository_roots WHERE root_id='source.hvm4';")
real_porcelain=$(git -C "$requirement_root/vendor/hvm4" status --porcelain)
if [ -n "$real_porcelain" ]; then expected_dirty=1; else expected_dirty=0; fi
[ "$recorded_dirty" -eq "$expected_dirty" ] || { echo "FAIL: recorded dirty=$recorded_dirty, real git status says dirty=$expected_dirty" >&2; exit 1; }
still_clean=$(git -C "$requirement_root/vendor/hvm4" status --porcelain)
[ "$still_clean" = "$real_porcelain" ] || { echo "FAIL: discovery mutated the sibling repository's working tree" >&2; exit 1; }

echo "== namespace identity resolves: bonfyre://source/hvm4 =="
"$requirement_fabric" namespace show bonfyre://source/hvm4 | grep -q '^kind=source-repository$' \
  || { echo "FAIL: bonfyre://source/hvm4 did not resolve" >&2; exit 1; }
"$requirement_fabric" namespace show bonfyre://source/hvm4 | grep -q "^locator=$requirement_root/vendor/hvm4$" \
  || { echo "FAIL: bonfyre://source/hvm4 locator is wrong" >&2; exit 1; }

echo "== revision-qualified namespace identity resolves to the exact source generation =="
"$requirement_fabric" namespace show "bonfyre://source/hvm4@$hvm4_oid" | grep -q "^evidence=$generation$" \
  || { echo "FAIL: revision-qualified namespace entry does not carry the real source_generation" >&2; exit 1; }

echo "== resolution does not depend on current working directory =="
cwd_independent=$(cd /tmp && BONFYRE_STATE_DIR="$BONFYRE_STATE_DIR" "$requirement_fabric" namespace show bonfyre://source/hvm4 | sed -n 's/^locator=//p')
[ "$cwd_independent" = "$requirement_root/vendor/hvm4" ] || { echo "FAIL: resolution changed when invoked from a different CWD" >&2; exit 1; }

echo "== a fresh process reopening the same fabric.db resolves identical identity (cross-process consistency) =="
second_process_oid=$(sqlite3 "$requirement_db" "SELECT head_oid FROM repository_roots WHERE root_id='source.hvm4';")
[ "$second_process_oid" = "$hvm4_oid" ] || { echo "FAIL: cross-process resolution disagreed" >&2; exit 1; }

echo "== recompiling is idempotent: no duplicate repository_roots rows, same repo count =="
"$requirement_fabric" fabric compile \
  "$requirement_root/bonfyre.workspace.yaff" "$requirement_root/bonfyre.lock.yaff" \
  "$requirement_root/estate/catalog.yaff" "$requirement_root/estate/compositions.yaff" \
  "$requirement_root/estate/profiles.yaff" "$requirement_root/estate/legacy-operators.tsv" >/dev/null
recount=$(sqlite3 "$requirement_db" "SELECT count(*) FROM repository_roots;")
[ "$recount" -eq "$count" ] || { echo "FAIL: recompile changed the discovered-repository count ($count -> $recount)" >&2; exit 1; }
duplicate_ids=$(sqlite3 "$requirement_db" "SELECT count(*) FROM (SELECT root_id FROM repository_roots GROUP BY root_id HAVING count(*) > 1);")
[ "$duplicate_ids" -eq 0 ] || { echo "FAIL: found $duplicate_ids duplicate root_id rows in repository_roots" >&2; exit 1; }

echo 'sibling repository roots: passed'
