#!/usr/bin/env bash
# A vector store either holds the body it was offered, or it says it refused it.
#
# The defect this guards: `sqlite3_step` on the vector insert had its result
# discarded, so a body of the wrong width was rejected by the vector table while
# the metadata row had already been written and the counter had already been
# incremented. A run reporting `inserted: 1` left a store holding
# `artifacts: 1, vectors: 0` -- an artifact with no body, and an exit code of 0.
#
# A second defect sat underneath it: the parser stopped at the store's width, so
# a longer vector was silently truncated and stored as if it were whole. That is
# worse than a refusal, because the store then holds a different vector from the
# one offered and every neighbour computed against it is quietly wrong.
set -u
BIN="${VEC_BIN:-./bonfyre-vec}"
pass=0; fail=0
ok()  { printf '  ok   %s\n' "$1"; pass=$((pass+1)); }
bad() { printf '  FAIL %s\n' "$1"; fail=$((fail+1)); }
check() { if [ "$2" = "$3" ]; then ok "$1"; else bad "$1 (want '$3', got '$2')"; fi; }

[ -x "$BIN" ] || { echo "no bonfyre-vec at $BIN" >&2; exit 1; }
tmp=$(mktemp -d); trap 'rm -rf "$tmp"' EXIT
W=$("$BIN" 2>&1 | grep -oE 'VEC_DIMS|384' | head -1); W=${W:-384}

mk() { python3 -c "
import json,sys
print(json.dumps({'id':sys.argv[1],'source':'width-test','embedding':[0.05]*int(sys.argv[2])}))" "$1" "$2" > "$tmp/$1.json"; }
mk narrow 32
mk wide   512
mk exact  "$W"

run() {  # name -> sets $rc $arts $vecs $said
  rm -f "$tmp/s.db"; "$BIN" init "$tmp/s.db" >/dev/null 2>&1
  said=$("$BIN" insert "$tmp/s.db" "$tmp/$1.json" 2>&1); rc=$?
  arts=$(sqlite3 "$tmp/s.db" "SELECT COUNT(*) FROM artifacts;" 2>/dev/null); arts=${arts:-0}
  vecs=$(sqlite3 "$tmp/s.db" "SELECT COUNT(*) FROM vec_artifacts_rowids;" 2>/dev/null); vecs=${vecs:-0}
}

echo "== a body of the store's width is stored =="
run exact
check "an exact-width body exits 0" "$rc" "0"
check "and one artifact is present" "$arts" "1"
check "and one vector is present" "$vecs" "1"

echo "== a body of the wrong width is refused, not half-stored =="
for case in narrow wide; do
  run $case
  # The exit code is the part a caller acts on. Reporting success after storing
  # nothing is how an empty store comes to be believed populated.
  check "a $case body does not exit 0" "$([ "$rc" -ne 0 ] && echo differs || echo zero)" "differs"
  printf '%s' "$said" | grep -qi 'refus' && ok "and the refusal is stated for $case" \
                                         || bad "a $case body was refused silently"
  # The corrupting half: metadata written for a body the vector table rejected.
  check "no orphan artifact remains after a $case refusal" "$arts" "0"
  check "and no vector was written for $case" "$vecs" "0"
done

echo "== the store never disagrees with itself =="
# artifacts and vectors are two halves of one record. A store where they differ
# has already lost the property every later neighbour computation assumes.
for case in exact narrow wide; do
  run $case
  check "$case leaves artifacts and vectors equal" "$arts" "$vecs"
done

echo "== an over-long body is refused rather than truncated =="
run wide
check "a 512-wide body stores nothing" "$vecs" "0"
printf '%s' "$said" | grep -qE '512' \
  && ok "and the refusal names the real width it was offered" \
  || bad "the refusal does not say how wide the body actually was"

echo
echo "== a store can be made at the width of the model in use =="
rm -f "$tmp/w.db"; "$BIN" init "$tmp/w.db" --dims 768 >/dev/null 2>&1; check "init --dims 768 succeeds" "$?" "0"
mk seven68 768
"$BIN" insert "$tmp/w.db" "$tmp/seven68.json" >/dev/null 2>&1; check "a 768-wide body is stored in a 768-wide store" "$?" "0"
check "its vector is there" "$(sqlite3 "$tmp/w.db" "SELECT COUNT(*) FROM vec_artifacts_rowids;" 2>/dev/null)" "1"
"$BIN" insert "$tmp/w.db" "$tmp/exact.json" >/dev/null 2>&1; rc=$?
[ "$rc" != 0 ] && ok "a 384-wide body is refused by a 768-wide store" || bad "a 384-wide body was accepted by a 768-wide store"
check "and left nothing behind" "$(sqlite3 "$tmp/w.db" "SELECT COUNT(*) FROM artifacts;" 2>/dev/null)" "1"
"$BIN" search "$tmp/w.db" "$tmp/seven68.json" --top 1 2>/dev/null | grep -q seven68 && ok "a 768-wide query finds it" || bad "a 768-wide query did not find the stored body"
"$BIN" init "$tmp/w.db" --dims 384 >/dev/null 2>&1; rc=$?
[ "$rc" != 0 ] && ok "an existing store is not silently re-made at another width" || bad "init changed, or claimed to change, an existing store's width"
"$BIN" init "$tmp/z.db" --dims 0 >/dev/null 2>&1; rc=$?
[ "$rc" != 0 ] && ok "a width of zero is refused" || bad "a width of zero was accepted"

echo "vector width admission: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
