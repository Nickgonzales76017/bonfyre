#!/usr/bin/env bash
set -euo pipefail

binary="$1"
root="$(mktemp -d)"
trap 'rm -rf "$root"' EXIT
database="$root/admission.db"
gib=$((1024 * 1024 * 1024))

"$binary" request --db "$database" --plane food --kind ingest --bytes $((8 * gib)) \
  --volume /data --free-bytes $((100 * gib)) --at 2026-08-28T12:00:00+00:00 \
  | grep -q '"verdict":"admit"'
"$binary" request --db "$database" --plane habitat --kind render --bytes $((8 * gib)) \
  --volume /data --free-bytes $((25 * gib)) --at 2026-08-28T13:00:00+00:00 \
  | grep -q '"verdict":"defer"'
"$binary" status --db "$database" --volume /data | grep -q '"committed_bytes":8589934592'
"$binary" release --db "$database" --grant 1 --at 2026-08-28T14:00:00+00:00 \
  | grep -q '"released_grant_id":1'
"$binary" status --db "$database" --volume /data | grep -q '"committed_bytes":0'

# Unset, absent and zero are three states.
#
# They were two: the policy struct read 0 as "unset", so --protected-floor-bytes 0
# was accepted, discarded, and replaced with the 10 GiB default. A 2 KiB grant was
# then refused against 1 MB of free space by a floor the caller had explicitly
# asked to remove, and nothing said the flag had been dropped.
floor_db="$root/floor.db"
if "$binary" request --db "$floor_db" --plane latent --kind embedding --bytes 2048      --free-bytes $((1024 * 1024)) --protected-floor-bytes 0 >/dev/null 2>&1; then
  echo "a bare 0 floor was accepted; it is ambiguous and must be refused" >&2
  exit 1
fi
# Captured rather than piped: grep -q closes the pipe on its first match and the
# binary dies of SIGPIPE, which pipefail then reports as the test failing.
refusal="$("$binary" request --db "$floor_db" --plane latent --kind embedding --bytes 2048 --free-bytes $((1024 * 1024)) --protected-floor-bytes 0 2>&1 || true)"
case "$refusal" in
  *"Pass --protected-floor-bytes none"*) ;;
  *) echo "0 was refused without naming the flag that works: $refusal" >&2; exit 1 ;;
esac
"$binary" request --db "$floor_db" --plane latent --kind embedding --bytes 2048   --free-bytes $((1024 * 1024)) --protected-floor-bytes none   | grep -q '"verdict":"admit"'
# The floor is a real safety property and survives the fix: omitting the flag
# still refuses the same grant against the same free space.
"$binary" request --db "$root/default.db" --plane latent --kind embedding --bytes 2048   --free-bytes $((1024 * 1024))   | grep -q '"verdict":"reject"'
# Memory is a volume. Too little free right now is "not now", never "never":
# free memory moves when any program exits. More than the machine has is "never".
memory="$("$binary" request --db "$root/memory.db" --plane model --kind model-resident --bytes $((6 * 1024 * 1024 * 1024)) --volume memory --free-bytes $((2 * 1024 * 1024 * 1024)))"
case "$memory" in *'"verdict":"defer"'*'other programs are using the memory now'*) ;; *) echo "memory shortfall was not a defer: $memory" >&2; exit 1 ;; esac
"$binary" request --db "$root/memory.db" --plane model --kind model-resident --bytes $((2 * 1024 * 1024 * 1024)) --volume memory --free-bytes $((8 * 1024 * 1024 * 1024)) | grep -q '"verdict":"admit"'
huge="$("$binary" request --db "$root/memory.db" --plane other --kind model-resident --bytes $((1024 * 1024 * 1024 * 1024)) --volume memory --free-bytes $((8 * 1024 * 1024 * 1024)))"
case "$huge" in *'"verdict":"reject"'*'single-grant ceiling'*) ;; *) echo "a model bigger than RAM was not rejected: $huge" >&2; exit 1 ;; esac
"$binary" memory | grep -q '"available_bytes":[0-9]'
echo "admission native smoke: PASS"
