#!/bin/sh
# Recorded JS reports, with the discovery-only exercised wording corrected.
set -eu
here=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
organ=$(dirname "$here")
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
mkdir -p "$tmp/architecture" "$tmp/cmd/Fixture/tests/deep" "$tmp/tests/deep" "$tmp/docs"
cp "$here/../../../architecture/atlas-fields.cue" "$tmp/architecture/"
export BONFYRE_ATLAS_DIR="$tmp/architecture"
printf 'source\n' > "$tmp/docs/readme.txt"
printf 'retired\n' > "$tmp/docs/proof.sh.archive"
for suite in cmd/Fixture/tests/run.sh cmd/Fixture/tests/grade_test.sh cmd/Fixture/tests/grade_e2e.sh cmd/Fixture/tests/deep/hidden.sh tests/deep/nested.sh; do
  printf '#!/bin/sh\nexit 99\n' > "$tmp/$suite"
done
ln -s run.sh "$tmp/cmd/Fixture/tests/link.sh"
ln -s Fixture "$tmp/cmd/Linked"
cat > "$tmp/architecture/fixture.yaff" <<'EOF'
atlas witness-grade-fixture
architecture a-prose
  maturity measured
  witness A prose description
architecture b-missing
  maturity proven
  witness tests/missing.sh
architecture c-archive
  maturity measured
  witness docs/proof.sh.archive
architecture d-file
  maturity measured
  witness docs/readme.txt exists
architecture e-command
  maturity measured
  witness cmd/Fixture/tests/run.sh
architecture f-root-nested
  maturity proven
  witness tests/deep/nested.sh
architecture g-command-nested
  maturity measured
  witness cmd/Fixture/tests/deep/hidden.sh
architecture h-symlink-file
  maturity measured
  witness cmd/Fixture/tests/link.sh
architecture i-test
  maturity measured
  witness cmd/Fixture/tests/grade_test.sh
architecture j-e2e
  maturity measured
  witness cmd/Fixture/tests/grade_e2e.sh
architecture k-first-live
  maturity measured
  witness tests/absent.sh
  witness docs/readme.txt
  witness cmd/Fixture/tests/run.sh
architecture l-symlink-dir
  maturity measured
  witness cmd/Linked/tests/run.sh
architecture ignored
  maturity implemented
  witness cmd/Fixture/tests/run.sh
EOF
cat > "$tmp/mutation.json" <<'EOF'
{"guarded":[{"table":"goals","field":"state","noticed_by":["run","nested","grade","run"]}]}
EOF
printf '%s\n' '{"guarded":[{"noticed_by":["grade_test"]}]}' > "$tmp/exact.json"
printf '%s\n' '{"guarded":[{"noticed_by":["run"]},null]}' > "$tmp/invalid-shape.json"
printf '%s\n' '{broken' > "$tmp/malformed.json"
report() {
  case_name=$1; shift
  "$organ/bonfyre-atlas" witness-grade "$@" > "$tmp/actual"
  sed 's/it is in the regression, but has never been shown to fail/a shell suite was discovered; execution is not evidenced/' "$here/witness-grade/$case_name.stdout" > "$tmp/expected"
  diff -u "$tmp/expected" "$tmp/actual"
  awk '/^  (graded|exercised|runnable|named) / {sum += $2; tiers++} /^[0-9]+ architectures/ {total=$1} END {exit !(tiers==4 && sum==total)}' "$tmp/actual"
}
report baseline
report mutation --mutation "$tmp/mutation.json"
report exact --mutation "$tmp/exact.json"
report missing --mutation "$tmp/missing.json"
report malformed --mutation "$tmp/malformed.json"
report invalid-shape --mutation "$tmp/invalid-shape.json"
# A stale index is never consulted: classification comes from parsed YAFF.
printf '%s\n' '{"architectures":{}}' > "$tmp/architecture/atlas.index.json"
report stale --mutation "$tmp/mutation.json"
for option in --unknown --mutation; do
  if "$organ/bonfyre-atlas" witness-grade "$option" > "$tmp/actual" 2> "$tmp/error"; then exit 1; else status=$?; fi
  [ "$status" = 2 ]
  grep -F 'usage: bonfyre-atlas witness-grade' "$tmp/error" >/dev/null
done
echo 'bonfyre-atlas witness-grade: 9 cases PASS'
