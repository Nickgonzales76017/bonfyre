#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tmpdir="$(mktemp -d)"
server_pid=""
cleanup() {
  if [[ -n "$server_pid" ]]; then
    kill "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
  rm -rf "$tmpdir"
}
trap cleanup EXIT

make -C "$root" >/dev/null
printf 'current public source fixture\n' >"$tmpdir/source.html"
printf 'robots policy denied fixture\n' >"$tmpdir/denied.html"
printf 'User-agent: *\nDisallow: /denied*\n' >"$tmpdir/robots.txt"
python3 -m http.server 18991 --bind 127.0.0.1 --directory "$tmpdir" >/dev/null 2>&1 &
server_pid=$!
sleep 1

receipt="$tmpdir/receipts.jsonl"
"$root/bonfyre-project" foreign observe housing-source-fixture \
  http://127.0.0.1:18991/source.html --receipt "$receipt"

python3 - "$receipt" <<'PY'
import json
import sys

row = json.loads(open(sys.argv[1], encoding="utf-8").readline())
assert row["schema"] == "bonfyre-foreign-observation.v1"
assert row["twin_id"] == "housing-source-fixture"
assert row["http_status"] == 200
assert len(row["content_sha256"]) == 64
assert row["availability_claim"] is False
assert row["observation_kind"] == "source_observed"
assert row["observe_right"] == "public"
assert row["policy_decision"] == "allow"
assert row["fetch_attempted"] is True
assert row["robots_http_status"] == 200
PY

retained="$tmpdir/retained-body.html"
"$root/bonfyre-project" foreign observe housing-source-fixture \
  http://127.0.0.1:18991/source.html --receipt "$receipt" --body "$retained"

python3 - "$receipt" "$retained" <<'RETAINED'
import hashlib
import json
import sys

rows = [json.loads(line) for line in open(sys.argv[1], encoding="utf-8") if line.strip()]
assert len(rows) == 2, rows
retained = rows[-1]
assert retained["body_path"] == sys.argv[2]
body = open(sys.argv[2], "rb").read()
assert body == b"current public source fixture\n", body
assert retained["content_sha256"] == hashlib.sha256(body).hexdigest()
assert retained["body_bytes"] == len(body)
assert retained["availability_claim"] is False
assert "body_path" not in rows[0]
RETAINED

policy_body="$tmpdir/policy-body.html"
"$root/bonfyre-project" foreign observe robots-policy-fixture \
  http://127.0.0.1:18991/denied.html --receipt "$receipt" --body "$policy_body"

python3 - "$receipt" "$policy_body" <<'POLICY'
import json
import os
import sys

rows = [json.loads(line) for line in open(sys.argv[1], encoding="utf-8") if line.strip()]
row = rows[-1]
assert row["observation_kind"] == "policy_excluded"
assert row["policy_decision"] == "robots_disallow"
assert row["fetch_attempted"] is False
assert row["robots_http_status"] == 200
assert row["http_status"] == 0
assert row["content_sha256"] is None
assert "body_path" not in row
assert not os.path.exists(sys.argv[2])
POLICY

printf 'User-agent: *\nDisallow: /\n\nUser-agent: BonfyreProject\nAllow: /\n' >"$tmpdir/robots.txt"
"$root/bonfyre-project" foreign observe specific-agent-policy-fixture \
  http://127.0.0.1:18991/denied.html --receipt "$receipt"

python3 - "$receipt" <<'SPECIFIC'
import json
import sys

rows = [json.loads(line) for line in open(sys.argv[1], encoding="utf-8") if line.strip()]
row = rows[-1]
assert row["observation_kind"] == "source_observed"
assert row["policy_decision"] == "allow"
assert row["fetch_attempted"] is True
SPECIFIC

if "$root/bonfyre-project" foreign observe boundary-fixture \
  http://127.0.0.1:18991/missing.html --receipt "$receipt"; then
  echo "foreign observe accepted a target 404 as a source observation" >&2
  exit 1
fi

python3 - "$receipt" <<'BOUNDARY'
import json
import sys

rows = [json.loads(line) for line in open(sys.argv[1], encoding="utf-8") if line.strip()]
row = rows[-1]
assert row["observation_kind"] == "boundary_response"
assert row["policy_decision"] == "allow"
assert row["fetch_attempted"] is True
assert row["http_status"] == 404
BOUNDARY

"$root/bonfyre-project" foreign observe denied-right-fixture \
  http://127.0.0.1:18991/source.html --receipt "$receipt" --observe-right denied

python3 - "$receipt" <<'RIGHT'
import json
import sys

rows = [json.loads(line) for line in open(sys.argv[1], encoding="utf-8") if line.strip()]
row = rows[-1]
assert row["observation_kind"] == "policy_excluded"
assert row["policy_decision"] == "observe_right_denied"
assert row["observe_right"] == "denied"
assert row["fetch_attempted"] is False
assert row["robots_transport_exit"] == 0
assert row["robots_http_status"] == 0
RIGHT

indeterminate_body="$tmpdir/indeterminate-body.html"
if "$root/bonfyre-project" foreign observe unavailable-policy-fixture \
  http://127.0.0.1:18991/source.html --receipt "$receipt" --body "$indeterminate_body" \
  --robots http://127.0.0.1:1/robots.txt; then
  echo "foreign observe fetched after an unavailable robots preflight" >&2
  exit 1
fi

python3 - "$receipt" "$indeterminate_body" <<'INDETERMINATE'
import json
import os
import sys

rows = [json.loads(line) for line in open(sys.argv[1], encoding="utf-8") if line.strip()]
row = rows[-1]
assert row["observation_kind"] == "policy_indeterminate"
assert row["policy_decision"] == "robots_preflight_unavailable"
assert row["fetch_attempted"] is False
assert row["content_sha256"] is None
assert not os.path.exists(sys.argv[2])
INDETERMINATE

if "$root/bonfyre-project" foreign observe x file:///etc/hosts >/dev/null 2>&1; then
  echo "foreign observe accepted a non-http boundary" >&2
  exit 1
fi

if "$root/bonfyre-project" foreign observe x http://127.0.0.1:18991/source.html --body >/dev/null 2>&1; then
  echo "foreign observe accepted --body without a path" >&2
  exit 1
fi
