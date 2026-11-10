#!/usr/bin/env bash
set -euo pipefail

binary="$1"
root="$(mktemp -d)"
trap 'rm -rf "$root"' EXIT
database="$root/attention.db"

"$binary" schedule --db "$database" --watch proton --subject provider:proton \
  --temperature warm --reheat-on provider-capacity --at 2026-08-28T12:00:00+00:00 >/dev/null
for hour in 13 14 15; do
  "$binary" check --db "$database" --watch proton --changed false \
    --at "2026-08-28T${hour}:00:00+00:00" >/dev/null
done
"$binary" due --db "$database" --at 2026-08-28T16:00:00+00:00 | grep -q '^\[\]$'
if "$binary" reheat --db "$database" --watch proton --signal wrong --at 2026-08-28T16:00:00+00:00; then
  exit 1
fi
"$binary" reheat --db "$database" --watch proton --signal provider-capacity --at 2026-08-28T16:00:00+00:00 \
  | grep -q '"temperature":"hot"'
"$binary" due --db "$database" --at 2026-08-28T16:00:00+00:00 | grep -q '"watch_id":"proton"'
echo "attention native smoke: PASS"
