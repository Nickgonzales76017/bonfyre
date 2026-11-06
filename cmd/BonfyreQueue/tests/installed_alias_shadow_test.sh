#!/bin/sh
# Non-destructive candidate installation proof. Never changes managed files.
set -eu
root=$(CDPATH="" cd -- "$(dirname -- "$0")/../../.." && pwd)
managed=${BONFYRE_BIN:-"$HOME/.bonfyre/bin"}
[ -d "$managed" ] || { echo "alias shadow: SKIP (no managed bin)"; exit 0; }
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-managed-alias.XXXXXX")
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
for src in "$managed"/bonfyre*; do
  [ -f "$src" ] && [ ! -L "$src" ] || continue
  name=${src##*/}
  [ "$name" = bonfyre-site ] && continue
  if ! ln "$src" "$tmp/$name"; then
    echo "alias shadow: REFUSED (cannot stage original inodes)" >&2
    exit 1
  fi
done
[ -f "$tmp/bonfyre-web-address" ] || { echo "alias shadow: no existing managed WebAddress" >&2; exit 1; }
ln -s bonfyre-web-address "$tmp/bonfyre-site"
cmp -s "$tmp/bonfyre-site" "$managed/bonfyre-site" || { echo "alias shadow: site body differs" >&2; exit 1; }
BONFYRE_BIN="$tmp" sh "$root/scripts/native-ratchet.sh" check
echo "alias shadow: managed-binary identity-only convergence PASS (not installed)"
checkout="$root/cmd/BonfyreWebAddress/bonfyre-web-address"
if [ -f "$checkout" ] && [ ! -L "$checkout" ]; then
  rm "$tmp/bonfyre-web-address"
  cp "$checkout" "$tmp/bonfyre-web-address"
  measured=$(BONFYRE_BIN="$tmp" sh "$root/scripts/native-ratchet.sh" measure)
  echo "alias shadow: checkout candidate measured $measured"
  if BONFYRE_BIN="$tmp" sh "$root/scripts/native-ratchet.sh" check > "$tmp/candidate.log" 2>&1; then
    echo "alias shadow: checkout WebAddress candidate passes ratchet (not installed)"
  else
    echo "alias shadow: checkout WebAddress candidate REFUSED by existing ratchet (not installed)"
    grep -E "largest_bin_bytes|installed_bonfyre_bytes" "$tmp/candidate.log" || true
  fi
  if command -v strip >/dev/null 2>&1; then
    original_bytes=$(wc -c < "$tmp/bonfyre-web-address" | tr -d ' ')
    if strip -x -S "$tmp/bonfyre-web-address" > "$tmp/strip.log" 2>&1; then
      stripped_bytes=$(wc -c < "$tmp/bonfyre-web-address" | tr -d ' ')
      echo "alias shadow: stripped checkout bytes $original_bytes -> $stripped_bytes"
      if BONFYRE_BIN="$tmp" sh "$root/scripts/native-ratchet.sh" check > "$tmp/stripped.log" 2>&1; then
        echo "alias shadow: stripped checkout candidate satisfies ratchet (not installed)"
      else
        echo "alias shadow: stripped checkout candidate REFUSED by existing ratchet"
        grep -E "largest_bin_bytes|installed_bonfyre_bytes" "$tmp/stripped.log" || true
      fi
    else
      echo "alias shadow: strip candidate unavailable (no installed changes)"
    fi
  fi
fi
