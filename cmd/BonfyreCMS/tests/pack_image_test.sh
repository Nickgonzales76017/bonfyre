#!/bin/sh
# Native PackGraph BFP1 and repository-cut contract; no added JS runner.
set -eu
CMS=$1
case "$CMS" in /*) ;; *) CMS="$(pwd)/$CMS";; esac
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
tmp=$(mktemp -d "/tmp/bonfyre-pack-image.XXXXXXXX")
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
fail(){ echo "pack-image: FAIL $*" >&2; exit 1; }
schemas="$tmp/schemas"; mkdir -p "$schemas"
for type in pack_artifact pack_version pack_head pack_dependency pack_overlay pack_demand pack_installation pack_activation; do
 cp "$root/cmd/BonfyreCMS/content-types/$type.json" "$schemas/"
done
db="$tmp/cms.db"
"$CMS" schema migrate --db "$db" --schemas "$schemas" >/dev/null
add(){ "$CMS" entry create "$1" "$2" --db "$db" --schemas "$schemas" --ns root >/dev/null; }
root_hash(){ printf '%s' "$1:1" | shasum -a 256 | awk '{print $1}'; }
for name in foundation studio; do
 add pack_artifact "{\"pack\":\"$name\",\"canonical_name\":\"$name\",\"publisher\":\"fixture\",\"layer\":\"official\",\"kind\":\"base\",\"purpose\":\"Native image fixture\",\"governed_by\":\"fixture\",\"cannot_infer\":\"No publication or installation\",\"status\":\"published\"}"
 add pack_version "{\"pack\":\"$name\",\"version\":\"1\",\"root\":\"$(root_hash "$name")\",\"changed\":\"Initial\",\"status\":\"published\"}"
 add pack_head "{\"pack\":\"$name\",\"channel\":\"stable\",\"version\":\"1\",\"advanced_by\":\"fixture\",\"status\":\"published\"}"
done
add pack_dependency '{"pack":"studio","depends_on":"foundation","why":"Fixture dependency","status":"published"}'
image(){ "$CMS" pack-image --db "$db" --schemas "$schemas" --pack studio "$@"; }
field(){ printf '%s\n' "$1" | awk -F '\t' -v key="$2" '$1==key { print $2; exit }'; }
cut=25b231f1cb6e7a04f08d379f9a4f00e363f4e95ba70c67d00c5d09d2cb7e4126
if image --cut-digest "$cut" >/dev/null;then fail "undeclared cut accepted";fi
add pack_demand "{\"pack\":\"studio\",\"demand_kind\":\"source\",\"needs\":\"Exact repository cut\",\"preferred_sources\":\"repository-cut:$cut\",\"status\":\"published\"}"
bound=$(image --cut-digest "$cut") || fail "declared cut refused"
[ "$(field "$bound" repository_cut)" = "$cut" ] || fail "cut binding missing"
if image --cut-digest aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa >/dev/null;then fail "arbitrary cut accepted";fi
first=$(image --out-dir "$tmp") || fail "image compilation"
path1=$(field "$first" image_path);digest=$(field "$first" image_digest)
[ -f "$path1" ] && [ "$(od -An -tx1 -N4 "$path1" | tr -d ' \n')" = 42465031 ] || fail "BFP1 header missing"
[ "$(shasum -a 256 "$path1" | awk '{print $1}')" = "$digest" ] || fail "image digest differs"
second=$(image --out-dir "$tmp") || fail "immutable reuse refused"
[ "$(field "$second" state)" = existing ] && [ "$(field "$second" image_digest)" = "$digest" ] || fail "nondeterministic image"
[ "$(field "$(image --verify-image "$path1")" state)" = verified ] || fail "image readback"
cp "$path1" "$tmp/damaged.bfp"; printf x >>"$tmp/damaged.bfp"
if image --verify-image "$tmp/damaged.bfp" >/dev/null;then fail "corrupt image accepted";fi
ln -s "$path1" "$tmp/spoof.bfp"
if image --verify-image "$tmp/spoof.bfp" >/dev/null;then fail "symlink image accepted";fi
add pack_demand '{"pack":"studio","demand_kind":"source","needs":"Next source","status":"published"}'
if image --verify-image "$path1" >/dev/null;then fail "stale closure accepted";fi
next=$(image --out-dir "$tmp") || fail "new closure compilation"
[ "$(field "$next" image_digest)" != "$digest" ] || fail "changed demand preserved digest"
add pack_demand '{"pack":"studio","demand_kind":"source","needs":"Third source","status":"published"}'
third=$(field "$(image)" image_digest)
ln -s "$(field "$next" image_path)" "$tmp/$third.bfp"
if image --out-dir "$tmp" >/dev/null;then fail "symlink writer accepted";fi
[ "$(sqlite3 "$db" 'SELECT COUNT(*) FROM pack_installation')" = 0 ] || fail "unexpected installation"
[ "$(sqlite3 "$db" 'SELECT COUNT(*) FROM pack_activation')" = 0 ] || fail "unexpected activation"
echo "PACK_IMAGE_PASS: native repository demand, deterministic BFP1, readback, stale/corrupt/symlink refusal, no authority writes"
