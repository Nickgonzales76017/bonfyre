#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
mode=${1:-generate}
shared=${BONFYRE_ACCEPTANCE_RUN:-${BONFYRE_STATE_DIR:?BONFYRE_STATE_DIR is required}}/shared/command-completion
state="$shared/state"
matrix="$shared/command-evidence.tsv"
db="$state/fabric.db"

# Command completion crosses two build authorities. The Fabric may need bounded
# hosted-runner warning penetration, but native Powers must retain each
# Makefile's private CFLAGS/OPTFLAGS/include contract. Never leak a test-level
# CFLAGS override into the 93-command workload build.
fabric_cflags=${BONFYRE_FABRIC_TEST_CFLAGS:-${CFLAGS:-}}
command_cc=${CC:-cc}
case " $command_cc " in
  *" -D_GNU_SOURCE "*) ;;
  *) command_cc="$command_cc -D_GNU_SOURCE" ;;
esac
case " $command_cc " in
  *" -D_DEFAULT_SOURCE "*) ;;
  *) command_cc="$command_cc -D_DEFAULT_SOURCE" ;;
esac

generate_matrix() {
  rm -rf "$shared"
  mkdir -p "$shared"
  if [ -n "$fabric_cflags" ]; then
    CFLAGS="$fabric_cflags" BONFYRE_STATE_DIR="$shared" sh "$root/tests/fabric_smoke.sh" >/dev/null
  else
    BONFYRE_STATE_DIR="$shared" sh "$root/tests/fabric_smoke.sh" >/dev/null
  fi
  test -x "$root/scripts/bonfyre-command-workloads" || {
    echo 'missing production command workload runner' >&2
    return 1
  }
  env -u CFLAGS -u OPTFLAGS CC="$command_cc" BONFYRE_STATE_DIR="$state" \
    "$root/scripts/bonfyre-command-workloads"
  BONFYRE_STATE_DIR="$state" BONFYRE_COMMAND_EVIDENCE="$matrix" \
    "$root/scripts/bonfyre-command-evidence" >/dev/null
}

test -f "$matrix" || [ "$mode" = generate ] || {
  echo 'same-run command evidence matrix is absent' >&2
  exit 1
}

[ "$mode" = generate ] && generate_matrix
test -f "$matrix"
test -f "$db"

rows=$(awk 'NR > 1 { count++ } END { print count + 0 }' "$matrix")
unique=$(awk -F '\t' 'NR > 1 { seen[$1] = 1 } END { print length(seen) }' "$matrix")
typed=$(sqlite3 "$db" "SELECT count(*) FROM operator_contract_bindings WHERE operator_id LIKE 'command.%' AND generation=(SELECT value FROM fabric_meta WHERE key='catalog_generation');")
workloads=$(awk -F '\t' 'NR > 1 && $12 == "quality_proven" { count++ } END { print count + 0 }' "$matrix")
quality=$(awk -F '\t' 'NR > 1 && $11 == "passed" && $12 == "quality_proven" { count++ } END { print count + 0 }' "$matrix")
unproven=$(awk -F '\t' 'NR > 1 && $12 != "quality_proven" { count++ } END { print count + 0 }' "$matrix")
missing_events=$(awk -F '\t' 'NR > 1 && $8 == "" { count++ } END { print count + 0 }' "$matrix")
missing_receipts=$(awk -F '\t' 'NR > 1 && $9 == "" { count++ } END { print count + 0 }' "$matrix")
stale=$(awk -F '\t' 'NR == 2 { generation=$4 } NR > 1 && $4 != generation { count++ } END { print count + 0 }' "$matrix")
# Commands whose bound contract is explicitly invocation_kind='blocked' (e.g.
# wire-level service dependencies with no local workload) can never be
# workload- or quality-proven by design; the dispatcher itself refuses to run
# them (see fabric.operators.unproven_safety_blocked). Only unproven counts
# beyond this known, contract-enforced set indicate a real regression.
blocked=$(sqlite3 "$db" "SELECT count(*) FROM operator_contract_bindings b JOIN operator_contracts c ON c.id=b.contract_id AND c.generation=b.generation JOIN fabric_meta m ON m.key='catalog_generation' WHERE b.operator_id LIKE 'command.%' AND b.generation=m.value AND c.invocation_kind='blocked';")

[ "$rows" -eq 93 ]
[ "$unique" -eq 93 ]
[ "$typed" -eq 93 ]
case "$mode" in
  generate)
    if [ "$workloads" -ne $((93 - blocked)) ] || [ "$unproven" -ne "$blocked" ]; then
      echo "command workload proof mismatch: workloads=$workloads unproven=$unproven expected_blocked=$blocked" >&2
      exit 1
    fi
    ;;
  quality)
    if [ "$quality" -ne $((93 - blocked)) ] || [ "$unproven" -ne "$blocked" ]; then
      echo "command quality proof mismatch: quality=$quality unproven=$unproven expected_blocked=$blocked" >&2
      exit 1
    fi
    ;;
  events)
    if [ "$missing_events" -ne "$blocked" ]; then
      echo "unexpected missing events: missing_events=$missing_events expected_blocked=$blocked" >&2
      exit 1
    fi
    ;;
  receipts)
    if [ "$missing_receipts" -ne "$blocked" ]; then
      echo "unexpected missing receipts: missing_receipts=$missing_receipts expected_blocked=$blocked" >&2
      exit 1
    fi
    ;;
  generations) [ "$stale" -eq 0 ] ;;
  *) echo "unknown command completion check: $mode" >&2; exit 2 ;;
esac

printf 'command_rows=%s\nunique_commands=%s\ntyped_command_contracts=%s\n' \
  "$rows" "$unique" "$typed"
printf 'command_workloads_proven=%s\ncommand_quality_results=%s\nunproven=%s\nblocked_by_contract=%s\n' \
  "$workloads" "$quality" "$unproven" "$blocked"
printf 'missing_events=%s\nmissing_receipts=%s\nstale_generations=%s\n' \
  "$missing_events" "$missing_receipts" "$stale"
