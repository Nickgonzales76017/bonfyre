#!/bin/sh
# The interface ratchet. Counts of the patterns Nick rejected -- stacked
# collapses, page-level inline styles, machine words shown to people, and
# owners wired by hand into main.c instead of declared in
# schemas/control/web-routes.cue -- in the code that draws pages. Each count may only go down; `tighten` records a win.
#   scripts/ui-ratchet.sh check | measure | tighten
set -eu
root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
budget="$root/architecture/ui-ratchet.tsv"
src="$root/cmd/BonfyreWebAddress/src $root/integrations/browser $root/components/BonfyreServiceSite $root/site/matrix/surfaces"
count(){ grep -rhoE --include='*.h' --include='*.c' --include='*.mjs' --include='*.html' --exclude-dir=node_modules --exclude-dir=tests --exclude-dir=build -e "$1" $src 2>/dev/null | wc -l | tr -d ' '; }
measure(){
  printf 'collapses\t%s\n' "$(count '<details')"
  printf 'inline_styles\t%s\n' "$(count '<style[ >]')"
  printf 'machine_words\t%s\n' "$(count '>[^<>{}$]*\b(Retain|Retained|retain |Materiali[sz]e|Admit |Admitted|Projection|Carrier recipe|Receipt-bound|Attest )[^<>{}$]*<')"
  printf 'own_documents\t%s\n' "$(count '<!doctype html')"
  printf 'dispatch_branches\t%s\n' "$(grep -o 'handled=[a-z_]*(' "$root/cmd/BonfyreWebAddress/src/main.c" | wc -l | tr -d ' ')"
  printf 'route_path_compares\t%s\n' "$(cat "$root"/cmd/BonfyreWebAddress/src/*.h "$root"/cmd/BonfyreWebAddress/src/*.c | grep -oE 'str(n)?cmp\((target|path),"/[^"]*"' | wc -l | tr -d ' ')"
  printf 'cli_verb_ladders\t%s\n' "$(grep -rhoE 'strcmp\((argv\[[0-9]\]|cmd|verb|command|sub|op),"[a-z][a-z0-9-]*"\)' "$root"/cmd/*/src "$root"/components/*/src 2>/dev/null | wc -l | tr -d ' ')"
  printf 'hand_printed_controls\t%s\n' "$(cat "$root"/cmd/BonfyreWebAddress/src/*.h | grep -oE '<(button|input|select|textarea|form)[ >]' | wc -l | tr -d ' ')"
  printf 'hardcoded_hosts\t%s\n' "$(cat "$root"/cmd/BonfyreWebAddress/src/*.h "$root"/cmd/BonfyreWebAddress/src/*.c | grep -oE 'https://[a-z]*\.?aurekai\.(work|life|live|shop)' | wc -l | tr -d ' ')"
}
case "${1:-check}" in
  measure) measure ;;
  tighten) measure > "$budget"; echo "ui ratchet: tightened"; cat "$budget" ;;
  check)
    [ -f "$budget" ] || { echo "ui ratchet: no budget; run tighten" >&2; exit 1; }
    bad=0
    measure | while IFS="$(printf '\t')" read -r name now; do
      max=$(awk -F'\t' -v n="$name" '$1==n{print $2}' "$budget")
      if [ "$now" -gt "${max:-0}" ]; then echo "ui ratchet: $name grew to $now (budget $max). Use the shell and the .ak-pattern components; plain words; no collapses." >&2; echo fail; fi
    done | grep -q fail && bad=1
    [ "$bad" -eq 0 ] && echo "ui ratchet: ok $(measure | tr '\t\n' '= ')" || exit 1 ;;
esac
