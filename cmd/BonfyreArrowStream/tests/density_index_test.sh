#!/bin/sh
# Offline native cross-owner witness: RepositoryFabric YAFF + CMS BFP1 -> Parquet.
set -eu
[ "$#" = 1 ] || { echo 'usage: density_index_test.sh ARROW_BINARY' >&2; exit 2; }
arrow=$1
case "$arrow" in /*) ;; *) arrow="$(pwd)/$arrow";; esac
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
cms="$root/cmd/BonfyreCMS/bonfyre-cms"
[ -x "$cms" ] || { echo 'needs built native BonfyreCMS' >&2; exit 1; }
tmp=$(mktemp -d /tmp/bonfyre-density-test.XXXXXXXX)
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
umask 077
fail(){ echo "density-index: FAIL $*" >&2; exit 1; }
schemas="$tmp/schemas";mkdir -p "$schemas"
for kind in pack_artifact pack_version pack_head pack_dependency pack_overlay pack_demand pack_installation pack_activation; do
 cp "$root/cmd/BonfyreCMS/content-types/$kind.json" "$schemas/"
done
db="$tmp/cms.db"
"$cms" schema migrate --db "$db" --schemas "$schemas" >/dev/null
add(){ "$cms" entry create "$1" "$2" --db "$db" --schemas "$schemas" --ns root >/dev/null; }
field(){ printf '%s\n' "$1" | awk -F '\t' -v key="$2" '$1==key { print $2; exit }'; }
commit=c13cbf207df7fc64eef481ac5ca98b059bcddb9b
tree=6e0458e81fad21af002aece9c88cb04c3bf43b65
digest=$(printf 'bonfyre.repository-cut.v1\nforgejo\naurekai\nrepository-fabric-proof\nHEAD\n\n%s\n%s\n' "$commit" "$tree" | shasum -a 256 | awk '{print $1}')
cat >"$tmp/cut.yaff" <<END
schema: bonfyre.repository-cut.v1
source_provider: forgejo
source_org: aurekai
source_repository: repository-fabric-proof
requested_ref: HEAD
josh_filter: none
virtual_commit: $commit
virtual_tree: $tree
cut_digest: $digest
canonical_mapping: direct-git
pack_root: uncompiled
install_authority: none
publication_authority: none
END
root_digest=$(printf 'studio:1' | shasum -a 256 | awk '{print $1}')
add pack_artifact '{"pack":"studio","canonical_name":"Fixture Studio","publisher":"fixture","layer":"official","kind":"base","purpose":"Native density witness","governed_by":"fixture","cannot_infer":"No rights or installation","status":"published"}'
add pack_version "{\"pack\":\"studio\",\"version\":\"1\",\"root\":\"$root_digest\",\"changed\":\"Initial\",\"status\":\"published\"}"
add pack_head '{"pack":"studio","channel":"stable","version":"1","advanced_by":"fixture","status":"published"}'
add pack_demand "{\"pack\":\"studio\",\"demand_kind\":\"source\",\"needs\":\"Reviewed source cut\",\"preferred_sources\":\"repository-cut:$digest\",\"status\":\"published\"}"
compiled=$("$cms" pack-image --db "$db" --schemas "$schemas" --pack studio --out-dir "$tmp")
image=$(field "$compiled" image_path)
[ -f "$image" ] || fail 'CMS did not produce BFP1'
out="$tmp/combined.parquet"
result=$("$arrow" index --cut "$tmp/cut.yaff" --pack-image "$image" --out "$out") || fail 'index construction'
[ "$(field "$result" rows)" = 3 ] || fail 'expected cut, pack, edge'
[ "$(field "$result" matched_edges)" = 1 ] || fail 'cut-to-pack association not matched'
[ "$(field "$result" authority)" = derived-only ] || fail 'projection claimed authority'
[ "$(shasum -a 256 "$out" | awk '{print $1}')" = "$(field "$result" sha256)" ] || fail 'Parquet digest'
relation=$(duckdb -noheader -csv -c "SELECT COUNT(*) FROM read_parquet('$out') e JOIN read_parquet('$out') c ON e.source_cut_digest=c.identity_digest WHERE e.kind='pack_source_cut' AND c.kind='repository_cut' AND e.pack='studio' AND e.link_state='receipt-matched'")
[ "$relation" = 1 ] || fail 'missing Parquet join'
mkdir -p "$tmp/cuts" "$tmp/packs"
cp "$tmp/cut.yaff" "$tmp/cuts/one.yaff"; cp "$tmp/cut.yaff" "$tmp/cuts/two.yaff"
cp "$image" "$tmp/packs/one.bfp"; cp "$image" "$tmp/packs/two.bfp"
batch=$("$arrow" index --cuts "$tmp/cuts" --packs "$tmp/packs" --out "$tmp/batch.parquet") || fail "directory indexing"
[ "$(field "$batch" rows)" = 3 ] || fail "identity-deduped directory count"
[ "$(field "$batch" source_cuts)" = 1 ] || fail "duplicate source receipts inflated cut count"
[ "$(field "$batch" pack_images)" = 1 ] || fail "duplicate BFP images inflated pack count"
[ "$(field "$batch" matched_edges)" = 1 ] || fail "duplicate edges inflated match count"
codec=$(duckdb -noheader -csv -c "SELECT COUNT(*) FROM parquet_metadata('$tmp/batch.parquet') WHERE compression='ZSTD'")
[ "$codec" -gt 0 ] || fail "Parquet output was not ZSTD compressed"
[ "$(sqlite3 "$db" 'SELECT COUNT(*) FROM pack_installation')" = 0 ] || fail 'unexpected installation'
[ "$(sqlite3 "$db" 'SELECT COUNT(*) FROM pack_activation')" = 0 ] || fail 'unexpected activation'
if "$arrow" index --cut "$tmp/cut.yaff" --pack-image "$image" --out "$out" >/dev/null 2>&1;then fail 'index overwritten';fi
sed 's/source_org: aurekai/source_org: replaced/' "$tmp/cut.yaff" >"$tmp/tampered.yaff"
if "$arrow" index --cut "$tmp/tampered.yaff" --out "$tmp/refused.parquet" >/dev/null 2>&1;then fail 'tampered cut accepted';fi
[ ! -e "$tmp/refused.parquet" ] || fail 'refused cut wrote output'
cp "$image" "$tmp/bad.bfp";printf x >>"$tmp/bad.bfp"
if "$arrow" index --pack-image "$tmp/bad.bfp" --out "$tmp/refused2.parquet" >/dev/null 2>&1;then fail 'damaged BFP accepted';fi
ln -s "$image" "$tmp/link.bfp"
if "$arrow" index --pack-image "$tmp/link.bfp" --out "$tmp/refused3.parquet" >/dev/null 2>&1;then fail 'symlink BFP accepted';fi
echo 'DENSITY_INDEX_PASS: 3 Parquet rows, exact cut/pack edge join, ZSTD, digest, immutable target, tamper/symlink refusal, no authority writes'
