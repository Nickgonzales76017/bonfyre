#!/bin/sh
# HVM4 is here, built, and reduces.
#
# The atlas carried this architecture at `implemented` with a source_path and a
# witness that were both `.py.archive` -- two files that cannot run -- and a
# known_gap reading "installed binary is HVM2 (2.0.22); HVM4 upgrade +
# hvm-lang front-end still pending". All three were stale. `vendor/hvm4/src/hvm4`
# is a built arm64 binary that answers `HVM 4.0`, and it reduces the bench
# programs vendored beside it.
#
# That is the shape of a lost maturity: not a capability that decayed, but one
# that arrived and was never told to the registry, while the registry went on
# citing evidence that had been retired. A capability the estate cannot see is
# one it will rebuild by hand.
set -eu

repo=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd -P)
hvm="$repo/vendor/hvm4/src/hvm4"
fail() { echo "FAIL: $1"; exit 1; }

[ -x "$hvm" ] || fail "no built HVM4 at $hvm"

version=$("$hvm" 2>&1 | head -1)
case "$version" in
    "HVM 4"*) ;;
    *) fail "expected HVM 4, got: $version" ;;
esac

# A bounded reduction with an answer that is a fact about the program rather
# than about this machine: fib(34) over Nat, reduced by interaction net.
out=$("$hvm" "$repo/vendor/hvm4/devs/bench/fib_nat.hvm" 2>&1 | tr -d '[:space:]')
[ "$out" = "5702887" ] || fail "fib_nat reduced to '$out', expected 5702887"

# Reduction is deterministic: the same program gives the same normal form.
again=$("$hvm" "$repo/vendor/hvm4/devs/bench/fib_nat.hvm" 2>&1 | tr -d '[:space:]')
[ "$out" = "$again" ] || fail "two reductions of one program disagreed: $out vs $again"

# A second program, so the check is of the runtime and not of one memoised path.
mul=$("$hvm" "$repo/vendor/hvm4/devs/bench/gen_mul4k.hvm" 2>&1 | head -c 200)
[ -n "$mul" ] || fail "gen_mul4k produced nothing"

echo "PASS hvm4: $version reduces fib_nat to 5702887, deterministically, and is not HVM2"
