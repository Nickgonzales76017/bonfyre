#!/bin/sh
# The interface ratchet. Counts of the patterns Nick rejected -- stacked
# collapses, page-level inline styles, machine words shown to people -- in the
# code that draws pages. Each count may only go down; `tighten` records a win.
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
