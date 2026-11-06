#!/usr/bin/env bash
set -euo pipefail

# E2E smoke/integration test for bonfyre-* live apps
# - For each discovered bonfyre-* binary, attempt a `status` command (if supported)
# - For `bonfyre-narrate`, run a self-verify against sample demo WAVs
# - Write per-app logs to `site/demos/e2e-results/` and print a summary

OUT_DIR="site/demos/e2e-results"
mkdir -p "$OUT_DIR"

BIN_DIRS=(./cmd)
declare -a BINS

# discover executables named bonfyre-*
while IFS= read -r -d '' bin; do
  BINS+=("$bin")
done < <(find "${BIN_DIRS[@]}" -maxdepth 2 -type f -name 'bonfyre-*' -perm -111 -print0 2>/dev/null)

if [ ${#BINS[@]} -eq 0 ]; then
  echo "No bonfyre-* binaries found in ${BIN_DIRS[*]}. Try running 'make'." >&2
  exit 2
fi

echo "Found ${#BINS[@]} binaries. Running smoke checks..."

PASS=0
FAIL=0
results=()

for bin in "${BINS[@]}"; do
  name=$(basename "$bin")
  echo "--- Testing $name ---"
  app_out="$OUT_DIR/$name"
  mkdir -p "$app_out"

  # prefer `status` command if binary supports it
  set +e
  "$bin" status > "$app_out/status.stdout" 2> "$app_out/status.stderr"
  rc=$?
  set -e

  if [ $rc -eq 0 ]; then
    echo "$name: status OK" | tee "$app_out/summary.txt"
    results+=("$name:PASS")
    PASS=$((PASS+1))
  else
    # fallback: --help
    set +e
    "$bin" --help > "$app_out/help.stdout" 2> "$app_out/help.stderr"
    rc2=$?
    set -e
    if [ $rc2 -eq 0 ]; then
      echo "$name: --help OK (status unsupported)" | tee -a "$app_out/summary.txt"
      results+=("$name:PASS(help)")
      PASS=$((PASS+1))
    else
      echo "$name: FAIL (status/help failed)" | tee -a "$app_out/summary.txt" "$app_out/summary.err"
      results+=("$name:FAIL")
      FAIL=$((FAIL+1))
    fi
  fi

  # special-case: run a more thorough narrate self-verify if binary is bonfyre-narrate
  if [ "$name" = "bonfyre-narrate" ]; then
    if [ -f site/demos/test_a.wav ]; then
      echo "Running narrate self-verify on site/demos/test_a.wav" | tee -a "$app_out/summary.txt"
      set +e
      "$bin" verify site/demos/test_a.wav site/demos/test_a.wav > "$app_out/verify.stdout" 2> "$app_out/verify.stderr"
      vrc=$?
      set -e
      if [ $vrc -eq 0 ]; then
        # extract composite score if present
        comp=$(jq -r '.composite // .composite_score // empty' < "$app_out/verify.stdout" 2>/dev/null || true)
        echo "narrate verify exit=$vrc composite=$comp" | tee -a "$app_out/summary.txt"
      else
        echo "narrate verify FAILED (exit=$vrc)" | tee -a "$app_out/summary.txt" "$app_out/summary.err"
      fi
    else
      echo "No sample WAV at site/demos/test_a.wav; skipping narrate self-verify" | tee -a "$app_out/summary.txt"
    fi
  fi

done

echo
echo "E2E Summary: PASS=$PASS FAIL=$FAIL (details in $OUT_DIR)"
for r in "${results[@]}"; do echo " - $r"; done

if [ $FAIL -gt 0 ]; then
  echo "One or more checks failed." >&2
  exit 1
fi

echo "All smoke checks passed." 
