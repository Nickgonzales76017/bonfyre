#!/bin/sh
# Material objects are emitted as route.html so an export is inspectable on a
# filesystem.  A direct R2 custom domain does not perform the estate Worker's
# extension rewrite, so publish an exact alias for every HTML route as well.
# This is intentionally full-estate: limiting it to front pages would merely
# move the 404 frontier from the Worker quota to the material lane.
set -eu

usage() { echo "usage: $0 --source DIST [--prepare-only --out DIR | --verify]" >&2; exit 2; }
source_dir=''
prepare_only=0
verify_only=0
out_dir=''
while [ "$#" -gt 0 ]; do
  case "$1" in
    --source) source_dir=${2:-}; shift 2 ;;
    --prepare-only) prepare_only=1; shift ;;
    --verify) verify_only=1; shift ;;
    --out) out_dir=${2:-}; shift 2 ;;
    *) usage ;;
  esac
done
[ -n "$source_dir" ] && [ -d "$source_dir" ] || usage
[ "$prepare_only" = 0 ] || [ "$verify_only" = 0 ] || usage

if [ "$prepare_only" = 1 ]; then
  [ -n "$out_dir" ] || usage
  mkdir -p "$out_dir"
  aliases=$out_dir
  cleanup() { :; }
else
  aliases=$(mktemp -d)
  cleanup() { rm -rf "$aliases"; }
fi
trap cleanup EXIT INT TERM

# Estate export names are compiler-generated URL paths and cannot contain a
# newline; portable POSIX read keeps this usable on the Mac publisher too.
# A route can be a page and also the parent of other pages (people.html beside
# people/...). The object store holds both keys; a directory on disk cannot, and
# the whole alias pass used to stop there. Those few are named first and sent
# one at a time after the sync.
both=$(mktemp)
find "$source_dir" -type f -name '*.html' -print | sed "s|^$source_dir/||; s|\.html\$||" |
  awk '{ seen[$0] = 1 } END { for (a in seen) { n = split(a, part, "/"); p = ""; for (i = 1; i < n; i++) { p = (i == 1 ? part[1] : p "/" part[i]); if (p in seen) dir[p] = 1 } } for (p in dir) print p }' > "$both"

find "$source_dir" -type f -name '*.html' -print |
while IFS= read -r page; do
  relative=${page#"$source_dir"/}
  alias=${relative%.html}
  if grep -qxF "$alias" "$both"; then continue; fi
  # R2 cannot represent the empty object key that a browser asks for at '/'.
  # A non-Worker redirect policy owns '/' -> '/index.html'; every other route
  # gets an exact object identity here.
  [ "$alias" = index ] && alias=index
  target=$aliases/$alias
  mkdir -p "$(dirname "$target")"
  cp "$page" "$target"
done

if [ "$prepare_only" = 1 ]; then
  find "$aliases" -type f | wc -l | tr -d ' '
  exit 0
fi

: "${CLOUDFLARE_ACCESS_KEY_ID:?set R2 access key id}"
: "${CLOUDFLARE_SECRET_ACCESS_KEY:?set R2 secret access key}"
: "${CLOUDFLARE_R2_ENDPOINT:?set R2 endpoint}"
if [ "$verify_only" = 1 ]; then
  # This checks every generated extensionless key, rather than trusting one
  # successful front-page probe. The normal sync comparison catches a missing
  # object or a changed body without writing the bucket.
  pending=$(AWS_ACCESS_KEY_ID="$CLOUDFLARE_ACCESS_KEY_ID" \
    AWS_SECRET_ACCESS_KEY="$CLOUDFLARE_SECRET_ACCESS_KEY" \
    AWS_DEFAULT_REGION=auto \
    aws s3 sync "$aliases/" "s3://${BONFYRE_R2_BUCKET:-aurekai-estate}/" \
      --endpoint-url "$CLOUDFLARE_R2_ENDPOINT" --dryrun --only-show-errors --no-progress)
  if [ -n "$pending" ]; then
    printf '%s\n' "$pending" >&2
    echo "r2 route aliases: incomplete" >&2
    exit 1
  fi
  find "$aliases" -type f | wc -l | tr -d ' '
  exit 0
fi

AWS_ACCESS_KEY_ID="$CLOUDFLARE_ACCESS_KEY_ID" \
AWS_SECRET_ACCESS_KEY="$CLOUDFLARE_SECRET_ACCESS_KEY" \
AWS_DEFAULT_REGION=auto \
aws s3 sync "$aliases/" "s3://${BONFYRE_R2_BUCKET:-aurekai-estate}/" \
  --endpoint-url "$CLOUDFLARE_R2_ENDPOINT" --only-show-errors --no-progress
while IFS= read -r alias; do
  [ -n "$alias" ] || continue
  AWS_ACCESS_KEY_ID="$CLOUDFLARE_ACCESS_KEY_ID" \
  AWS_SECRET_ACCESS_KEY="$CLOUDFLARE_SECRET_ACCESS_KEY" \
  AWS_DEFAULT_REGION=auto \
  aws s3 cp "$source_dir/$alias.html" "s3://${BONFYRE_R2_BUCKET:-aurekai-estate}/$alias" \
    --content-type "text/html; charset=utf-8" --endpoint-url "$CLOUDFLARE_R2_ENDPOINT" --only-show-errors --no-progress
done < "$both"
rm -f "$both"
find "$aliases" -type f | wc -l | tr -d ' '

