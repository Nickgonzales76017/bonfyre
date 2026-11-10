#!/usr/bin/env bash
# The estate's layer over the surfaces laid out by borrowed sheets is keyed on
# names those surfaces carry.  Nothing in a browser reports a selector that no
# longer matches, so this checks the names from both sides: every hook the
# layer styles still exists in the surface it is meant for, and the layer is in
# the sheet every one of those surfaces loads.
set -euo pipefail
root=$(cd "$(dirname "$0")/../../.." && pwd)
sheet="$root/site/matrix/aurekai-bar.css"
surfaces="$root/site/matrix/surfaces"
fail() { echo "borrowed sheet dress: $*" >&2; exit 1; }
grep -q 'surfaces in borrowed sheets' "$sheet" || fail "the layer is missing from aurekai-bar.css"
check() { # surface file, donor sheet name, hooks...
    local file="$surfaces/$1" donor="$2"; shift 2
    [ -f "$file" ] || fail "no surface $file"
    grep -q "donor-$donor" "$sheet" || grep -q 'href\*="/assets/donor-"' "$sheet" || fail "the layer does not address $donor"
    for hook in "$@"; do
        grep -q "$hook" "$sheet" || fail "the layer no longer styles $hook"
        case "$hook" in
            \#*) grep -q "id=\"${hook#\#}\"" "$file" || fail "$1 no longer carries $hook" ;;
            .*)  grep -q "${hook#.}" "$file" || fail "$1 no longer carries $hook" ;;
        esac
    done
}
check board.html pinry '#pn-terr' '#pn-links' '#pn-grid' '.pn-nav'
check stage.html mobilizon '#mz-links' '#mz-terr' '#mz-banner' '#mz-event' '#mz-title'
check community.html lemmy '#bf-nav' '#bf-h1' '#bf-lede' '#bf-sorts'
check listings.html nextcommerce '#nx-nav' '#nx-productwrap' '#nx-grid' '#nx-count' '#nx-question'
# The words a visitor should not meet again.
for word in 'published public condition' 'Search capabilities' 'What this room published'; do
    if grep -rq "$word" "$surfaces/stage.html" "$surfaces/listings.html"; then fail "\"$word\" is back in a surface"; fi
done
echo "borrowed sheet dress: PASS"
