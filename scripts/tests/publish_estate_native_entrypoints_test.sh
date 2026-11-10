#!/bin/sh
# Guard the direct standing publish entrypoints after their native cutovers.
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
sh -n "$root/scripts/publish_estate.sh"
if rg -n 'python[0-9]*|[[:alnum:]_-]+\.py(["[:space:]]|$)' "$root/scripts/publish_estate.sh"; then
  echo 'standing publish regained a direct Python entrypoint' >&2
  exit 1
fi
for tool in \
  cmd/BonfyreSourceFrontier/tools/measure_carried_material.mjs \
  scripts/load_search_index.mjs \
  scripts/address_every_publisher.mjs \
  scripts/compose_member_worlds.mjs \
  scripts/project_event_sources.mjs \
  scripts/project_listings.mjs \
  scripts/publish_market.mjs \
  scripts/publish-edition.mjs \
  scripts/publish_economics.mjs \
  scripts/bootstrap_funding_frontier.mjs; do
  [ -f "$root/$tool" ] || { echo "missing native entrypoint: $tool" >&2; exit 1; }
done
echo 'publish_estate_native_entrypoints_test: PASS'
