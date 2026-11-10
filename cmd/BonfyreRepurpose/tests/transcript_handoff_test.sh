#!/bin/sh
set -eu
root=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
repurpose=$root/BonfyreRepurpose/bonfyre-repurpose
brief=$root/BonfyreBrief/bonfyre-brief
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-transcript-handoff.XXXXXX")
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
mkdir "$tmp/wrapped"
printf '# Interview\n\n## Summary\n- The shipment arrived\n  with two damaged cartons.\n- The supplier will replace them.\n\n## Action Items\n- No action items detected\n\n## Deep Summary\n- No deep insights extracted\n' > "$tmp/wrapped/brief.md"
"$repurpose" all "$tmp/wrapped" "$tmp/wrapped-out" >/dev/null
for name in tweet-thread linkedin carousel youtube-desc newsletter; do
  file=$tmp/wrapped-out/$name.md
  grep -Fq 'The shipment arrived with two damaged cartons.' "$file"
  if grep -Eq 'No action items detected|No deep insights extracted' "$file"; then exit 1; fi
done
if grep -Eq '[0-9]+:[0-9]{2}|## Chapters' "$tmp/wrapped-out/youtube-desc.md"; then exit 1; fi
printf '# Interview\n\n## Summary\n- Two cartons arrived damaged.\n\n## Action Items\n- Ask the supplier\n  for replacement cartons.\n' > "$tmp/actions.md"
"$repurpose" newsletter "$tmp/actions.md" "$tmp/actions-out" >/dev/null
grep -Fq 'Ask the supplier for replacement cartons.' "$tmp/actions-out/newsletter.md"
printf 'The shipment\n arrived with\t two damaged cartons. The supplier confirmed replacement delivery tomorrow.' > "$tmp/transcript.txt"
"$brief" "$tmp/transcript.txt" "$tmp/brief" --title 'Delivery review' >/dev/null
grep -Fq 'The shipment arrived with two damaged cartons.' "$tmp/brief/brief.md"
"$repurpose" all "$tmp/brief" "$tmp/chain" >/dev/null
if grep -Fq '→ Arrived with' "$tmp/chain/linkedin.md"; then exit 1; fi
for kind in line joined; do
  { printf '# Interview\n\n## Summary\n'; if [ "$kind" = line ]; then awk 'BEGIN { for(i=0;i<5000;i++) printf "x"; print "" }'; else awk 'BEGIN { printf "- "; for(i=0;i<3000;i++) printf "x"; printf "\n  "; for(i=0;i<3000;i++) printf "y"; print "" }'; fi; } > "$tmp/$kind.md"
  if "$repurpose" all "$tmp/$kind.md" "$tmp/$kind-out" >"$tmp/out" 2>"$tmp/err"; then exit 1; fi
  grep -Fq 'exceeds supported length' "$tmp/err"
done
echo 'transcript handoff: 4 cases PASS'
