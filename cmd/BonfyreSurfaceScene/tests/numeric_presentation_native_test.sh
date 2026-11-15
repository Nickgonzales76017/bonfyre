#!/bin/sh
# Native numeric presentation: addresses, factoring, HVM4 selection, egglog
# witness, CSS lowering, the shared-body cache and the CMS cut, all through
# `bonfyre-surface-scene numeric`. CMS snapshots are written by the real
# browser writer (native-cms.mjs) so the cache reads what the editor stores.
set -eu
here=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
repo=$(CDPATH='' cd -- "$here/../.." && pwd)
bin=${1:-$here/bonfyre-surface-scene}
tmp=$(mktemp -d /tmp/bf-numeric-XXXXXX); trap 'rm -rf "$tmp"' EXIT
n=0
ok(){ n=$((n+1)); }
die(){ echo "numeric presentation native: FAIL $*" >&2; exit 1; }
num(){ "$bin" numeric "$@"; }
refuses(){ pattern=$1; shift; if out=$("$@" 2>&1); then die "expected refusal /$pattern/, got $out"; fi
  printf '%s' "$out" | grep -q -- "$pattern" || die "expected /$pattern/, got: $out"; ok; }
is(){ [ "$1" = "$2" ] || die "$3: '$1' != '$2'"; ok; }

# 1. canonical addresses; unsafe styles refused
a=$(echo '{"phone":{"gap":0,"layout":"stack"},"desktop":{"columns":3,"gap":17,"layout":"grid"}}' | num address)
b=$(echo '{"desktop":{"layout":"grid","gap":17,"columns":3},"phone":{"layout":"stack","gap":0}}' | num address)
is "$a" "$b" "address is order independent"
browser=$(cd "$repo" && bun -e 'import {numericPresentationAddress as a} from "./components/BonfyreServiceSite/authoring/numeric-presentation.mjs";console.log(JSON.stringify(a({desktop:{layout:"grid",gap:17,columns:3},phone:{layout:"stack",gap:0}})))')
is "$browser" "$a" "browser and native addresses agree"
refuses range sh -c "echo '{\"phone\":{\"gap\":65}}' | '$bin' numeric address"
refuses range sh -c "echo '{\"phone\":{\"gap\":\"10px\"}}' | '$bin' numeric address"
refuses 'unknown property' sh -c "echo '{\"phone\":{\"css\":\"injection\"}}' | '$bin' numeric address"

# 2. factor a shared basis, keep sparse residuals, dedupe repeat variants
cat > "$tmp/samples.json" <<'EOF'
[{"id":"work","responsive":{"desktop":{"layout":"grid","columns":3,"gap":17},"phone":{"layout":"stack","gap":8}}},
 {"id":"life","responsive":{"desktop":{"layout":"grid","columns":2,"gap":17},"phone":{"layout":"stack","gap":0}}},
 {"id":"shop","responsive":{"desktop":{"layout":"grid","columns":3,"gap":17},"phone":{"layout":"stack","gap":8}}},
 {"id":"bare","responsive":{"phone":{"layout":"stack","gap":8}}}]
EOF
num factor < "$tmp/samples.json" > "$tmp/family.json"
is "$(jq -r .basis.desktop.layout "$tmp/family.json")" grid "basis layout"
is "$(jq -r .basis.phone.gap "$tmp/family.json")" 8 "basis phone gap"
is "$(jq '.programs|length' "$tmp/family.json")" 3 "programs deduped"
is "$(jq -r '.bindings[0].address' "$tmp/family.json")" "$(jq -r '.bindings[2].address' "$tmp/family.json")" "repeat variants share an address"
is "$(jq '[.programs[]|select(.delta.desktop.layout==null and (.delta.desktop|has("layout")))]|length' "$tmp/family.json")" 1 "a removed property is a null residual"
for i in 0 1 2 3; do
  address=$(jq -r ".bindings[$i].address" "$tmp/family.json")
  want=$(jq -c ".[$i].responsive" "$tmp/samples.json" | num normalize)
  is "$(num expand --address "$address" < "$tmp/family.json")" "$want" "expand binding $i"
done
jq '.basis_address="'"$(printf '0%.0s' $(seq 64))"'"' "$tmp/family.json" > "$tmp/tampered.json"
refuses 'invalid basis' sh -c "'$bin' numeric expand --address $(jq -r '.bindings[0].address' "$tmp/family.json") < '$tmp/tampered.json'"

# 3. HVM4 lowering selects exactly what the C reference selects
echo '[{"id":"compact","responsive":{"desktop":{"gap":8}}},{"id":"editorial","responsive":{"desktop":{"gap":17}}},
 {"id":"spacious","responsive":{"desktop":{"gap":24}}},{"id":"wide","responsive":{"desktop":{"gap":40}}}]' | num factor > "$tmp/gaps.json"
jq '{family:.,predicate:{viewport:"desktop",field:"gap",max:20}}' "$tmp/gaps.json" | num select > "$tmp/lowered.json"
is "$(jq -c .expected "$tmp/lowered.json")" "[1,2,0,0]" "lowered selection"
jq -r .program "$tmp/lowered.json" | grep -q '@main = @keep' || die "HVM program shape"; ok
refuses 'invalid numeric' sh -c "jq '{family:.,predicate:{viewport:\"desktop\",field:\"gap\",max:65}}' '$tmp/gaps.json' | '$bin' numeric select"
hvm="$repo/vendor/hvm4/src/hvm4"
egglog="$HOME/.bonfyre/substrates/v6.1/egglog/target/release/egglog"
if [ -x "$hvm" ]; then
  jq -r .program "$tmp/lowered.json" > "$tmp/select.hvm"
  is "$("$hvm" "$tmp/select.hvm" -C16 | tr -s ' \n' '\n' | grep -E '^[0-9]+$' | paste -sd, -)" "1,2,0,0" "HVM4 agrees"
fi

# 4. the compiler: HVM4 + egglog agreement, CSS cut, fail-closed Lightning
if [ -x "$hvm" ] && [ -x "$egglog" ]; then
  echo '[{"id":"one","responsive":{"desktop":{"gap":8},"phone":{"gap":0}}},{"id":"two","responsive":{"desktop":{"gap":17},"phone":{"gap":0}}},
   {"id":"three","responsive":{"desktop":{"gap":26},"phone":{"gap":6}}},{"id":"four","responsive":{"desktop":{"gap":40}}}]' | num factor > "$tmp/four.json"
  jq '{family:.,predicate:{viewport:"desktop",field:"gap",max:20}}' "$tmp/four.json" > "$tmp/compile.json"
  num compile < "$tmp/compile.json" > "$tmp/compiled.json"
  is "$(jq -r .hvm.engine "$tmp/compiled.json")" hvm4 "hvm engine"
  is "$(jq -c .hvm.observed "$tmp/compiled.json")" "[1,2,0,0]" "hvm observed"
  is "$(jq -r .equivalence.engine "$tmp/compiled.json")" egglog "egglog witness"
  is "$(jq -r .engine "$tmp/compiled.json")" unoptimized-reference "raw engine"
  css=$(jq -r .css "$tmp/compiled.json")
  printf '%s' "$css" | grep -q 'data-desktop-gap="8"' && printf '%s' "$css" | grep -q 'data-desktop-gap="17"' || die "selected gaps in CSS"; ok
  if printf '%s' "$css" | grep -q 'data-desktop-gap="26"'; then die "unselected gap in CSS"; fi; ok
  printf '#!/bin/sh\nprintf ".wrong{gap:2px}"\n' > "$tmp/tamper"; chmod +x "$tmp/tamper"
  num compile --css lightning --lightning-fixture "$tmp/tamper" --engine-label fixture:tampered < "$tmp/compile.json" > "$tmp/tampered-out.json"
  is "$(jq -r .engine "$tmp/tampered-out.json")" fixture:tampered "fixture label carried"
  [ "$(jq -r .output_sha256 "$tmp/tampered-out.json")" != "$(jq -r .raw_sha256 "$tmp/tampered-out.json")" ] || die "tamper must change the output hash"; ok
  is "$(jq '.source_addresses|length' "$tmp/tampered-out.json")" 2 "CSS backend cannot change selection"
fi
refuses fixture sh -c "printf '.a{}' | '$bin' numeric lightning --lightning-fixture /bin/cat --engine-label lightningcss:1.33.0"
is "$(printf '.a{gap:17px}' | num lightning --lightning-fixture /bin/cat --engine-label fixture:unchanged | jq -r .css)" ".a{gap:17px}" "fixture transform"

# 5. the shared-body cache over real CMS snapshots
snap(){ (cd "$repo" && bun -e '
import {newAuthoringTree,applyAuthoringEdit} from "./cmd/BonfyreSurfaceScene/runtime/authoring-tree.mjs";
import {toNativePageWrite} from "./components/BonfyreServiceSite/authoring/native-cms.mjs";
const [namespace,gap,revision,coordinate]=process.argv.slice(1);
let doc=newAuthoringTree(namespace);
doc=applyAuthoringEdit(doc,{type:"insert",block:{id:coordinate,kind:"ui:grid",props:{heading:"Common",payload:{columns:3}},
 responsive:{desktop:{layout:"grid",columns:3,gap:Number(gap)},phone:{gap:0}},children:[]}});
const base={schema:"bonfyre.page-snapshot.v1",id:0,digest:"a".repeat(64),snapshot:{namespace,route:"/",page:null,fragments:[]}};
const write=toNativePageWrite(base,doc,{pageDraft:{title:"Grid",heading:"Grid"}});
console.log(JSON.stringify({schema:base.schema,id:5,digest:revision,snapshot:{namespace,route:"/",page:{...write.page,id:5},fragments:write.fragments}}));
' "$@"); }
h(){ printf "$1%.0s" $(seq 64); }
db="$tmp/derived.sqlite"
snap person-work 17 "$(h b)" same-coordinate > "$tmp/work.json"
snap person-life 17 "$(h c)" same-coordinate > "$tmp/life.json"
is "$(num intern --db "$db" < "$tmp/work.json" | jq .inserted)" 1 "first body inserted"
is "$(num intern --db "$db" < "$tmp/life.json" | jq .reused)" 1 "second scope reuses the body"
is "$(num counts --db "$db")" '{"bodies":1,"references":2}' "one body, two references"
wa=$(num resolve --db "$db" --namespace person-work --route / --coordinate same-coordinate)
la=$(num resolve --db "$db" --namespace person-life --route / --coordinate same-coordinate)
is "$(printf '%s' "$wa" | jq -r .digest)" "$(printf '%s' "$la" | jq -r .digest)" "shared digest"
is "$(printf '%s' "$wa" | jq .value.desktop.gap)" 17 "resolved value"
[ "$(printf '%s' "$wa" | jq -r .source_revision)" != "$(printf '%s' "$la" | jq -r .source_revision)" ] || die "revisions stay per scope"; ok
is "$(num resolve --db "$db" --namespace person-life --route / --coordinate foreign-coordinate)" null "foreign coordinate"
snap person-work 26 "$(h d)" same-coordinate | num intern --db "$db" > "$tmp/r.json"
is "$(jq .inserted "$tmp/r.json")" 1 "changed style inserts a body"
is "$(num counts --db "$db")" '{"bodies":2,"references":2}' "reference moved"
is "$(num resolve --db "$db" --namespace person-work --route / --coordinate same-coordinate | jq .value.desktop.gap)" 26 "work now 26"
is "$(num resolve --db "$db" --namespace person-life --route / --coordinate same-coordinate | jq .value.desktop.gap)" 17 "life still 17"
jq '.snapshot.fragments[0].semantic_tags.authoring_v1.numeric_presentation_digest="'"$(h f)"'"' "$tmp/work.json" > "$tmp/bad.json"
refuses 'does not match' sh -c "'$bin' numeric intern --db '$tmp/fresh.sqlite' < '$tmp/bad.json'"
is "$(num counts --db "$tmp/fresh.sqlite")" '{"bodies":0,"references":0}' "refusal does not mutate"
refuses 'absolute explicit store path' sh -c "'$bin' numeric counts --db relative.sqlite"

# 6. the CMS cut
if [ -x "$hvm" ] && [ -x "$egglog" ]; then
  snap person-one 17 "$(h b)" grid > "$tmp/s1.json"; snap person-two 17 "$(h c)" grid > "$tmp/s2.json"; snap person-three 32 "$(h d)" grid > "$tmp/s3.json"
  jq -s '{snapshots:.,predicate:{viewport:"desktop",field:"gap",max:20}}' "$tmp/s1.json" "$tmp/s2.json" "$tmp/s3.json" > "$tmp/cut.json"
  num cut --db "$tmp/cut.sqlite" < "$tmp/cut.json" > "$tmp/cut-out.json"
  is "$(jq -c .receipt.cache "$tmp/cut-out.json")" '{"bodies":2,"references":3}' "cut cache"
  is "$(jq '.receipt.selected_consumers|length' "$tmp/cut-out.json")" 2 "selected consumers"
  is "$(jq '.receipt.layout_sources|length' "$tmp/cut-out.json")" 1 "one layout source"
  is "$(jq -r .receipt.engine "$tmp/cut-out.json")" unoptimized-reference "cut engine"
  is "$(jq -c .receipt.source_revisions "$tmp/cut-out.json")" "[\"$(h b)\",\"$(h c)\",\"$(h d)\"]" "source revisions"
  jq -r .css "$tmp/cut-out.json" | grep -q 'data-desktop-gap="17"' || die "cut CSS"; ok
  if jq -r .css "$tmp/cut-out.json" | grep -q 'data-desktop-gap="32"'; then die "cut CSS leaked 32"; fi; ok
  jq -s '{snapshots:[.[0]],predicate:{viewport:"desktop",field:"gap",max:20}}' "$tmp/s1.json" > "$tmp/one.json"
  refuses 'only Lightning' sh -c "'$bin' numeric cut --db '$tmp/c2.sqlite' --artifact-dir '$tmp' --css raw < '$tmp/one.json'"
  refuses 'Lightning CSS not installed' sh -c "BONFYRE_LIGHTNINGCSS=/nonexistent '$bin' numeric cut --db '$tmp/c2.sqlite' --css lightning < '$tmp/one.json'"
  refuses 'only pinned Lightning CSS' sh -c "'$bin' numeric cut --db '$tmp/c2.sqlite' --css lightning --artifact-dir '$tmp' --lightning-fixture /bin/cat --engine-label fixture:exact < '$tmp/one.json'"
fi
echo "numeric presentation native: PASS ($n checks)"
