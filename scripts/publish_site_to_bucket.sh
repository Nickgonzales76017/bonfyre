#!/bin/sh
# One independent site as its own storage bucket with the site's address
# attached, so the address is served straight from storage: no Worker in the
# path, no daily request cap, and Cloudflare keeps the DNS record itself.
#
#   publish_site_to_bucket.sh NAME HOST EXPORT_ROOT [--attach]
#
# EXPORT_ROOT is what export_subject_hosts.sh wrote (hosts/HOST/..., assets/).
# A page is stored under the path a reader asks for (/clinic, not
# /clinic.html); "/" is sent to index.html by one rewrite rule in the zone.
set -eu
name=$1; host=$2; root=$3; attach=${4:-}
refs=${BONFYRE_CLOUD_REFS:-${HOME:?}/.bonfyre/secrets/cloud.refs.env}
set -a; . "$refs" >/dev/null 2>&1; set +a
bucket="aurekai-site-$name"
api="https://api.cloudflare.com/client/v4/accounts/$CLOUDFLARE_ACCOUNT_ID"
auth="Authorization: Bearer $CLOUDFLARE_API_TOKEN"
[ -f "$root/hosts/$host/index.html" ] || { echo "no export for $host under $root" >&2; exit 2; }
grep -q "$(printf '%s' "$host" | cut -d. -f1)" "$root/hosts/$host/index.html" || { echo "the export for $host does not look like its own front page" >&2; exit 3; }
curl -fsS "$api/r2/buckets/$bucket" -H "$auth" >/dev/null 2>&1 ||
  curl -fsS -X POST "$api/r2/buckets" -H "$auth" -H 'content-type: application/json' -d "{\"name\":\"$bucket\"}" >/dev/null
s3() { AWS_ACCESS_KEY_ID="$CLOUDFLARE_ACCESS_KEY_ID" AWS_SECRET_ACCESS_KEY="$CLOUDFLARE_SECRET_ACCESS_KEY" AWS_DEFAULT_REGION=auto aws s3 "$@" --endpoint-url "$CLOUDFLARE_R2_ENDPOINT" --only-show-errors; }
( cd "$root/hosts/$host" && find . -name '*.html' | sed 's|^\./||' ) | while IFS= read -r file; do
  s3 cp "$root/hosts/$host/$file" "s3://$bucket/$file" --content-type 'text/html; charset=utf-8' --cache-control 'public, max-age=300'
  [ "$file" = index.html ] || s3 cp "$root/hosts/$host/$file" "s3://$bucket/${file%.html}" --content-type 'text/html; charset=utf-8' --cache-control 'public, max-age=300'
done
for asset in "$root"/assets/*; do
  case "$asset" in *.css) type='text/css; charset=utf-8' ;; *.js) type='application/javascript; charset=utf-8' ;; *) continue ;; esac
  s3 cp "$asset" "s3://$bucket/assets/$(basename "$asset")" --content-type "$type" --cache-control 'public, max-age=300'
done
echo "stored $host in $bucket"
if [ "$attach" = "--attach" ]; then
  zone=$(printf '%s' "$host" | awk -F. '{print $(NF-1)"."$NF}')
  zid=$(curl -fsS "https://api.cloudflare.com/client/v4/zones?name=$zone" -H "$auth" | sed -n 's/.*"result":\[{"id":"\([0-9a-f]*\)".*/\1/p')
  curl -sS -X POST "$api/r2/buckets/$bucket/domains/custom" -H "$auth" -H 'content-type: application/json' \
       -d "{\"domain\":\"$host\",\"zoneId\":\"$zid\",\"enabled\":true}" | sed 's/.*"success":\([a-z]*\).*"errors":\(\[[^]]*\]\).*/attach success=\1 errors=\2/' | cut -c1-200
fi
