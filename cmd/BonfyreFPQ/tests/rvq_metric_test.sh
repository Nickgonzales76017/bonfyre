#!/usr/bin/env bash
# The RVQ correction metric must be capable of being wrong.
#
# It reported "RVQ pre RMSE 0.378446 / post RMSE 23.997465 (-6241.1% reduction)"
# beside a genuine cosine of 0.999852, and nothing stopped. Two faults produced
# it: the diagnostic ran twice into one accumulator that was divided once, and
# the second pass shadowed the encoder's lattice scale (8 * coord_bits) with
# wnorm/padded, three orders of magnitude smaller, so scaled_orig came out near
# zero and post_err was essentially minus the lattice point.
#
# The test below is the identity that cannot be satisfied by a broken metric.
# With the RVQ disabled every tile is the zeroed tile 0, so the corrected point
# is the lattice point and the post error IS the residual. pre and post must be
# equal -- not close, equal -- and any scale error between them shows up as a
# ratio immediately.
set -u
: "${FPQ:?set FPQ to the bonfyre-fpq binary}"
: "${FPQ_MODEL:?set FPQ_MODEL to a GGML model file}"
pass=0; fail=0
ok()  { printf '  ok   %s\n' "$1"; pass=$((pass+1)); }
bad() { printf '  FAIL %s\n' "$1"; fail=$((fail+1)); }

read_rmse() { # <stderr text> <label>
  printf '%s' "$1" | sed -n "s/.*$2 *RMSE: *\([0-9.]*\).*/\1/p" | head -1
}

echo "== with the correction disabled, the correction cannot change the error =="
OFF=$(BONFYRE_FPQ_DISABLE_RVQ=1 "$FPQ" roundtrip-v8 "$FPQ_MODEL" --limit 1 --bits 3 2>&1)
pre=$(read_rmse "$OFF" "RVQ pre")
post=$(read_rmse "$OFF" "RVQ post")
if [ -n "$pre" ] && [ "$pre" = "$post" ]; then
  ok "pre and post agree exactly at $pre when every tile is the zero tile"
else
  bad "pre=$pre post=$post; the two are not measured in the same space"
fi

echo "== with it enabled, the correction reduces the error and says so honestly =="
ON=$("$FPQ" roundtrip-v8 "$FPQ_MODEL" --limit 1 --bits 3 2>&1)
pre_on=$(read_rmse "$ON" "RVQ pre")
post_on=$(read_rmse "$ON" "RVQ post")
python3 - "$pre_on" "$post_on" <<'PY' && ok "post $post_on is below pre $pre_on" || bad "post $post_on is not below pre $pre_on"
import sys
pre, post = float(sys.argv[1]), float(sys.argv[2])
raise SystemExit(0 if 0.0 < post < pre else 1)
PY
# The residual energy is summed once. Two passes into one accumulator inflated
# the reported pre RMSE by sqrt(2) and nothing downstream could have noticed.
if [ "$pre" = "$pre_on" ]; then
  ok "the pre RMSE is the same whether or not the correction runs"
else
  bad "pre changed with the correction disabled ($pre vs $pre_on); it is measured before it"
fi

case "$ON" in
  *"% reduction"*) ok "an improvement is reported as a reduction" ;;
  *) bad "a genuine improvement was not labelled a reduction" ;;
esac
# The word must track the sign. A glob for "-...% reduction" would match the
# hyphen in "Lattice-Flow" earlier in the same line, so read the printed
# percentage itself.
pct=$(printf '%s' "$ON" | sed -n 's/.*RVQ post RMSE: *[0-9.]* *(\(-*[0-9.]*\)%.*/\1/p' | head -1)
case "$pct" in
  -*) bad "a negative percentage $pct is printed, and reduction is the wrong word for it" ;;
  "") bad "no percentage was printed at all" ;;
  *)  ok "the printed percentage $pct% is positive, so reduction is the right word" ;;
esac

echo
echo "fpq rvq metric: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
