#!/bin/sh
# Makes each listed opportunity the estate is pursuing an exchange in its
# commerce chain, at the two stages the opportunity record itself establishes:
# the product (what is on offer) and the listing (where it was seen, and
# when). Nothing later is recorded: an opportunity is not an inventory count,
# an offer, a quote or a commitment, and the chain refuses to skip to one.
#
# Only opportunities whose source is a classified listing are adopted; an
# application or a message sent is not a product on a listing.
#
#   commerce_adopt_opportunities.sh --finance-db F --commerce-db C
set -eu
finance='' commerce=''
while [ "$#" -gt 1 ]; do
  case "$1" in --finance-db) finance=$2 ;; --commerce-db) commerce=$2 ;; *) echo "unknown option $1" >&2; exit 2 ;; esac
  shift 2
done
[ -n "$finance" ] && [ -n "$commerce" ] || { echo "usage: $0 --finance-db F --commerce-db C" >&2; exit 2; }
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
organ=${BONFYRE_COMMERCE_BIN:-$root/cmd/BonfyreCommerce/bonfyre-commerce}
product_owner='ObjectContract / catalogue'
listing_owner='merchant or marketplace ForeignTwin'

adopted=0 already=0
sqlite3 -separator '	' "file:$finance?mode=ro" \
  "SELECT id, replace(replace(title,char(9),' '),char(10),' '), source_ref, observed_at FROM opportunities
   WHERE source_kind='classified' AND source_ref LIKE 'http%' ORDER BY id" > "${TMPDIR:-/tmp}/adopt.$$"
trap 'rm -f "${TMPDIR:-/tmp}/adopt.$$"' EXIT INT TERM
while IFS='	' read -r id title ref at; do
  exchange="opportunity:$id"
  if [ "$(sqlite3 "file:$commerce?mode=ro" "SELECT COUNT(*) FROM commerce_progress WHERE stage='listing' AND exchange='$(printf "%s" "$exchange" | sed "s/'/''/g")'" 2>/dev/null)" = 1 ]; then
    already=$((already + 1)); continue
  fi
  "$organ" record --db "$commerce" --exchange "$exchange" --stage product --owner "$product_owner" \
    --detail "$title" --at "$at" > /dev/null
  "$organ" record --db "$commerce" --exchange "$exchange" --stage listing --owner "$listing_owner" \
    --detail "$ref -- observed as finance opportunity $id" --at "$at" > /dev/null
  adopted=$((adopted + 1))
done < "${TMPDIR:-/tmp}/adopt.$$"
printf '{"kind":"commerce-adoption","adopted":%s,"already_in_chain":%s}\n' "$adopted" "$already"
