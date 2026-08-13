fabric_test_bootstrap() {
  requirement_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
  requirement_runtime="${BONFYRE_STATE_DIR:?BONFYRE_STATE_DIR is required}/runtime"
  mkdir -p "$requirement_runtime"

  make -C "$requirement_root/engine/core" >/dev/null
  make -C "$requirement_root/programs/bonfyre" >/dev/null
  export BONFYRE_STATE_DIR="$requirement_runtime/state"
  requirement_fabric="$requirement_root/programs/bonfyre/bonfyre"
  requirement_db="$BONFYRE_STATE_DIR/fabric.db"

  "$requirement_fabric" fabric compile \
    "$requirement_root/bonfyre.workspace.yaff" \
    "$requirement_root/bonfyre.lock.yaff" \
    "$requirement_root/estate/catalog.yaff" \
    "$requirement_root/estate/compositions.yaff" \
    "$requirement_root/estate/profiles.yaff" \
    "$requirement_root/estate/legacy-operators.tsv" >/dev/null
}

fabric_test_ingest() {
  "$requirement_fabric" artifact ingest "$1" "$2" | sed -n 's/^uri=//p'
}

fabric_test_run_node() {
  mission_id=$1
  node_id=$2
  operator_id=$3
  input_uri=$4

  "$requirement_fabric" mission create "$mission_id" >/dev/null
  "$requirement_fabric" work add "$mission_id" "$node_id" "$operator_id" "$input_uri" >/dev/null
  "$requirement_fabric" work run "$mission_id"
}

fabric_test_start_daemon() {
  make -C "$requirement_root/programs/bonfyred" >/dev/null
  requirement_port=$(python3 - <<'PY'
import socket
sock = socket.socket()
sock.bind(("127.0.0.1", 0))
print(sock.getsockname()[1])
sock.close()
PY
)
  "$requirement_root/programs/bonfyred/bonfyred" serve --port "$requirement_port" \
    >"$requirement_runtime/bonfyred.stdout" 2>"$requirement_runtime/bonfyred.stderr" &
  requirement_daemon_pid=$!
  attempts=0
  while ! curl --silent --fail "http://127.0.0.1:$requirement_port/health" \
      >"$requirement_runtime/health.json" 2>/dev/null; do
    attempts=$((attempts + 1))
    if [ "$attempts" -ge 100 ] || ! kill -0 "$requirement_daemon_pid" 2>/dev/null; then
      cat "$requirement_runtime/bonfyred.stderr" >&2
      return 1
    fi
    sleep 0.05
  done
}

fabric_test_stop_daemon() {
  if [ -n "${requirement_daemon_pid:-}" ] && kill -0 "$requirement_daemon_pid" 2>/dev/null; then
    kill "$requirement_daemon_pid"
    wait "$requirement_daemon_pid"
  fi
  requirement_daemon_pid=
}
