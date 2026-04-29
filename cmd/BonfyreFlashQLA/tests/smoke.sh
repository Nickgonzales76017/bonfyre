#!/usr/bin/env bash
# bonfyre-flashqla smoke tests
set -euo pipefail

BIN="./bonfyre-flashqla"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

fail() { echo "FAIL: $*" >&2; exit 1; }

# ── 1. binary exists and prints version ─────────────────────────────
[[ -x "$BIN" ]] || fail "bonfyre-flashqla not built (run: make)"
"$BIN" --version | grep -q "bonfyre-flashqla" || fail "--version"
echo "  ok: --version"

# ── 2. gen: produce a synthetic input tensor ────────────────────────
"$BIN" gen --B 1 --T 32 --H 4 --K 16 --V 16 --out "$TMP/test.bfgla" --seed 1
[[ -f "$TMP/test.bfgla" ]] || fail "gen: output file missing"
# magic bytes 'BGLA' = 0x414C4742 (LE)
MAGIC=$(xxd -l 4 -p "$TMP/test.bfgla")
[[ "$MAGIC" == "42474c41" ]] || fail "gen: bad magic ($MAGIC)"
echo "  ok: gen (magic ok)"

# ── 3. run tile (default backend) ───────────────────────────────────
"$BIN" run --in "$TMP/test.bfgla" --out "$TMP/tile.bfgdn" --chunk-size 8
[[ -f "$TMP/tile.bfgdn" ]] || fail "run tile: output missing"
OUT_MAGIC=$(xxd -l 4 -p "$TMP/tile.bfgdn")
[[ "$OUT_MAGIC" == "4e44474f" ]] || fail "run tile: bad output magic ($OUT_MAGIC)"
echo "  ok: run tile (output magic ok)"

# ── 4. output size: [B T H V] + [B H K V] float32s ─────────────────
# B=1 T=32 H=4 K=16 V=16
# header:  7 × u32 = 28 bytes
# o:       1*32*4*16 * 4 = 8192 bytes
# h_final: 1*4*16*16 * 4 = 4096 bytes
# total:   28 + 8192 + 4096 = 12316 bytes
EXPECTED=12316
ACTUAL=$(wc -c < "$TMP/tile.bfgdn")
ACTUAL="${ACTUAL// /}"
[[ "$ACTUAL" -eq "$EXPECTED" ]] || fail "run tile: output size $ACTUAL != $EXPECTED"
echo "  ok: output size ($ACTUAL bytes)"

# ── 5. run ref backend ──────────────────────────────────────────────
"$BIN" run --in "$TMP/test.bfgla" --out "$TMP/ref.bfgdn" --backend ref --chunk-size 8
[[ -f "$TMP/ref.bfgdn" ]] || fail "run ref: output missing"
echo "  ok: run ref"

# ── 6. verify: tile and ref must match to < 1e-4 ────────────────────
VERIFY_OUT=$("$BIN" verify --ref "$TMP/ref.bfgdn" --test "$TMP/tile.bfgdn")
echo "$VERIFY_OUT" | grep -q "ok: max_abs_err" || {
    echo "$VERIFY_OUT"
    fail "verify: tile output diverges from ref"
}
echo "  ok: verify (tile matches ref within 1e-4)"

# ── 7. bench: must complete without error ───────────────────────────
"$BIN" bench --B 1 --T 16 --H 2 --K 8 --V 8 --iters 3 --chunk-size 4 \
    | grep -q "tokens/s" || fail "bench: missing tokens/s line"
echo "  ok: bench tile"

"$BIN" bench --B 1 --T 16 --H 2 --K 8 --V 8 --iters 3 --backend ref \
    | grep -q "tokens/s" || fail "bench ref: missing tokens/s line"
echo "  ok: bench ref"

# ── 8. bench --compare: tile must beat ref ──────────────────────────
# Uses larger dims so the speedup is measurable (K=V=64, T=256, H=8)
COMPARE_OUT=$("$BIN" bench --B 1 --T 256 --H 8 --K 64 --V 64 --iters 20 --compare)
echo "$COMPARE_OUT" | grep -q "speedup:" || fail "bench --compare: no speedup line"
# Extract speedup value and check it is >= 1.1
SPEEDUP=$(echo "$COMPARE_OUT" | grep "speedup:" | grep -oE '[0-9]+\.[0-9]+' | head -1)
# Use awk for float comparison (bash can't compare floats)
PASS=$(awk "BEGIN { print ($SPEEDUP >= 1.1) ? \"yes\" : \"no\" }")
if [[ "$PASS" != "yes" ]]; then
    echo "$COMPARE_OUT"
    fail "bench --compare: speedup $SPEEDUP < 1.1x (tile not faster than ref)"
fi
# Warn if below 1.5x target
TARGET_PASS=$(awk "BEGIN { print ($SPEEDUP >= 1.5) ? \"yes\" : \"no\" }")
if [[ "$TARGET_PASS" != "yes" ]]; then
    echo "  WARN: speedup $SPEEDUP < 1.5x target (expected 1.5-2.5x on modern hardware)"
else
    echo "  ok: bench --compare speedup ${SPEEDUP}x >= 1.5x"
fi

# ── 9. doctor: must print dispatch path ─────────────────────────────
"$BIN" doctor | grep -q "dispatch path" || fail "doctor: missing dispatch path"
echo "  ok: doctor"

echo "flashqla smoke ok"
