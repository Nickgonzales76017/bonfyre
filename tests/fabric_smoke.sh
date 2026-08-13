#!/bin/sh
set -eu

root_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
if [ -n "${BONFYRE_STATE_DIR:-}" ]; then
  state_dir=$BONFYRE_STATE_DIR
  mkdir -p "$state_dir"
else
  state_dir=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-fabric-test.XXXXXX")
  trap 'rm -rf "$state_dir"' EXIT
fi

make -C "$root_dir/engine/core"
make -C "$root_dir/programs/bonfyre"
make -C "$root_dir/programs/bonfyred"
make -C "$root_dir/cmd/BonfyreHash"
for command_dir in BonfyreDetectObjects BonfyreFragment BonfyreRecipe BonfyreRun BonfyreWorkflow BonfyreFamily; do
  make -C "$root_dir/cmd/$command_dir"
done

export BONFYRE_STATE_DIR="$state_dir/state"
fabric="$root_dir/programs/bonfyre/bonfyre"

"$fabric" fabric compile "$root_dir/bonfyre.workspace.yaff" "$root_dir/bonfyre.lock.yaff" \
  "$root_dir/estate/catalog.yaff" "$root_dir/estate/compositions.yaff" \
  "$root_dir/estate/profiles.yaff" "$root_dir/estate/legacy-operators.tsv" | grep -q '^operators_compiled=96$'
(
  cd "$state_dir"
  "$fabric" fabric compile "$root_dir/bonfyre.workspace.yaff" | grep -q '^catalog_generation='
)
# 9 declared workspace roots + 2 provider roots (feldera, restate) + 10
# discovered sibling-repository roots (9 Frappe apps + hvm4) on this checkout.
"$fabric" root list | awk 'NR > 1 { roots++ } END { exit(roots == 21 ? 0 : 1) }'
"$fabric" catalog list | awk 'NR > 1 { operators++ } END { exit(operators == 98 ? 0 : 1) }'
sqlite3 "$BONFYRE_STATE_DIR/fabric.db" "SELECT count(*) = 93 FROM catalog_bindings WHERE operator_id LIKE 'command.%' AND binding_state = 'bound';" | grep -qx '1'

artifact_uri=$("$fabric" artifact ingest "$root_dir/estate/catalog.yaff" text/yaff | sed -n 's/^uri=//p')
"$fabric" namespace show "$artifact_uri" | grep -q '^kind=artifact$'
"$fabric" mission create acceptance >/dev/null
"$fabric" workflow start acceptance artifact-hash "$artifact_uri" | grep -q 'nodes=1'
"$fabric" work run acceptance | grep -q 'node=hash status=complete output=bonfyre://artifact/'
"$fabric" mission show acceptance | grep -q '^status=complete$'
"$fabric" fabric status | grep -q '^events=2$'
"$fabric" fabric status | grep -q '^usage_ledger=1$'
"$fabric" fabric status | grep -q '^economic_ledger=1$'
"$fabric" fabric status | grep -q '^value_ledger=1$'
sqlite3 "$BONFYRE_STATE_DIR/fabric.db" 'SELECT bytes_in > 0 AND bytes_out > 0 FROM usage_ledger LIMIT 1;' | grep -qx '1'
sqlite3 "$BONFYRE_STATE_DIR/fabric.db" 'SELECT accepted IS NULL FROM value_ledger LIMIT 1;' | grep -qx '1'

"$fabric" mission create unproven >/dev/null
"$fabric" work add unproven intake core.intake "$artifact_uri" >/dev/null
if "$fabric" work run unproven >/dev/null 2>&1; then
  echo "unproven operator unexpectedly executed" >&2
  exit 1
fi
"$fabric" mission show unproven | grep -q '^status=partial$'

effect_id=$("$fabric" effect request acceptance publish-local "$artifact_uri" | sed -n 's#^effect=bonfyre://effect/##p')
"$fabric" effect approve "$effect_id" | grep -q 'state=approved$'
publication_uri=$("$fabric" effect commit "$effect_id" | sed -n 's/.*publication=\([^ ]*\).*/\1/p')
publication_path=$("$fabric" namespace show "$publication_uri" | sed -n 's/^locator=//p')
cmp "$root_dir/estate/catalog.yaff" "$publication_path"
"$fabric" effect rollback "$effect_id" | grep -q 'state=compensated$'
test ! -e "$publication_path"

external_id=$("$fabric" effect request acceptance publish "$artifact_uri" | sed -n 's#^effect=bonfyre://effect/##p')
"$fabric" effect approve "$external_id" >/dev/null
if "$fabric" effect commit "$external_id" >/dev/null 2>&1; then
  echo "unbound external effect unexpectedly committed" >&2
  exit 1
fi

"$root_dir/programs/bonfyred/bonfyred" --health | grep -q '^bonfyred=ready$'
daemon_port=$((20000 + ($$ % 10000)))
"$root_dir/programs/bonfyred/bonfyred" serve --port "$daemon_port" >"$state_dir/bonfyred.log" 2>&1 &
daemon_pid=$!
for _ in 1 2 3 4 5; do
  if curl --silent --fail "http://127.0.0.1:$daemon_port/catalog" | grep -q '"operators":98'; then
    break
  fi
  sleep 1
done
curl --silent --fail "http://127.0.0.1:$daemon_port/mission/acceptance" | grep -q '"status":"complete"'
kill "$daemon_pid"
wait "$daemon_pid"
echo "fabric smoke: passed"
