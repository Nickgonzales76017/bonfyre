#!/bin/sh
# Project the account UI sources into site/assets with a sha256 manifest.
#   scripts/build-account-ui.sh [--check]   (run from the repo root)
set -eu
check=0; [ "${1:-}" = "--check" ] && check=1
manifest=$(mktemp); trap 'rm -f "$manifest"' EXIT
printf 'source\tprojection\tsha256\tbytes\n' > "$manifest"
for pair in account-session.mjs:account-session.mjs account-navigation.mjs:account-navigation.mjs account-navigation.css:account-navigation.css application-previews.js:showcase.js application-previews.css:application-previews.css; do
  source=${pair%%:*}; target=${pair#*:}
  src=components/BonfyreServiceSite/$source; dst=site/assets/$target
  if [ $check = 1 ]; then cmp -s "$src" "$dst" || { echo "Published projection differs from its source: $target" >&2; exit 1; }
  else mkdir -p "$(dirname "$dst")"; cp "$src" "$dst"; fi
  printf '%s\t%s\t%s\t%s\n' "$source" "$target" "$(shasum -a 256 "$src" | cut -d' ' -f1)" "$(wc -c < "$src" | tr -d ' ')" >> "$manifest"
done
if [ $check = 1 ]; then cmp -s "$manifest" site/assets/account-ui-SOURCE.tsv || { echo "Account UI manifest differs." >&2; exit 1; }
else cp "$manifest" site/assets/account-ui-SOURCE.tsv; fi
echo "ACCOUNT_UI_PROJECTION_PASS 5 files; $([ $check = 1 ] && echo checked || echo written)"
