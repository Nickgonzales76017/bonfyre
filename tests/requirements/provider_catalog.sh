#!/bin/sh
# Catalog identity + RuntimeImage contract for external providers (Feldera,
# Restate). Neither provider process runs locally in this test -- what's
# proven is that each has a real, compiled catalog identity (kind=provider,
# source_ref=runtime image reference), a real root registration, and a real
# bonfyre://provider/<id> namespace entry, so the fabric has a durable,
# addressable notion of "this provider exists and here is its runtime image"
# independent of whether the provider is currently running.
set -eu

. "$(dirname -- "$0")/fabric_test_lib.sh"
fabric_test_bootstrap

echo "== providers declaration compiled into the catalog =="
count=$(sqlite3 "$requirement_db" "SELECT count(*) FROM catalog WHERE kind='provider';")
[ "$count" -eq 2 ] || { echo "FAIL: expected 2 provider catalog rows, got $count" >&2; exit 1; }

echo "== feldera: catalog identity carries a real RuntimeImage reference =="
feldera_ref=$(sqlite3 "$requirement_db" "SELECT source_ref FROM catalog WHERE id='provider.feldera';")
case "$feldera_ref" in
  ghcr.io/feldera/*) : ;;
  *) echo "FAIL: unexpected feldera source_ref: $feldera_ref" >&2; exit 1 ;;
esac

echo "== restate: catalog identity carries a real RuntimeImage reference =="
restate_ref=$(sqlite3 "$requirement_db" "SELECT source_ref FROM catalog WHERE id='provider.restate';")
case "$restate_ref" in
  docker.restate.dev/*) : ;;
  *) echo "FAIL: unexpected restate source_ref: $restate_ref" >&2; exit 1 ;;
esac

echo "== both providers have a matching root registration under authority runtime-image =="
for id in provider.feldera provider.restate; do
  root_locator=$(sqlite3 "$requirement_db" "SELECT locator FROM roots WHERE id='$id' AND authority_class='runtime-image';")
  [ -n "$root_locator" ] || { echo "FAIL: no runtime-image root for $id" >&2; exit 1; }
  catalog_ref=$(sqlite3 "$requirement_db" "SELECT source_ref FROM catalog WHERE id='$id';")
  [ "$root_locator" = "$catalog_ref" ] || { echo "FAIL: root locator and catalog source_ref disagree for $id: $root_locator vs $catalog_ref" >&2; exit 1; }
done

echo "== both providers are addressable through the real bonfyre:// namespace =="
for slug in feldera restate; do
  "$requirement_fabric" namespace show "bonfyre://provider/$slug" | grep -q '^kind=provider$' \
    || { echo "FAIL: bonfyre://provider/$slug is not registered in the namespace" >&2; exit 1; }
done

echo "== command.* operators are untouched by provider compilation (still 93) =="
command_count=$(sqlite3 "$requirement_db" "SELECT count(*) FROM catalog WHERE id LIKE 'command.%';")
[ "$command_count" -eq 93 ] || { echo "FAIL: expected 93 command operators, got $command_count" >&2; exit 1; }

echo "== recompiling the workspace is idempotent (no duplicate/drifted provider rows) =="
"$requirement_fabric" fabric compile \
  "$requirement_root/bonfyre.workspace.yaff" "$requirement_root/bonfyre.lock.yaff" \
  "$requirement_root/estate/catalog.yaff" "$requirement_root/estate/compositions.yaff" \
  "$requirement_root/estate/profiles.yaff" "$requirement_root/estate/legacy-operators.tsv" >/dev/null
recount=$(sqlite3 "$requirement_db" "SELECT count(*) FROM catalog WHERE kind='provider';")
[ "$recount" -eq 2 ] || { echo "FAIL: recompile produced $recount provider rows, expected 2" >&2; exit 1; }

echo 'provider catalog: passed'
