#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
conf="$root/deploy/nginx/bonfyre-addressplane.conf"

grep -q 'aurekai.life \*.aurekai.life' "$conf"
grep -q 'aurekai.live \*.aurekai.live' "$conf"
grep -q 'aurekai.shop \*.aurekai.shop' "$conf"
grep -q 'aurekai.work \*.aurekai.work' "$conf"
grep -q 'theleaflounge.store \*.theleaflounge.store' "$conf"
grep -q 'proxy_set_header Host \$bonfyre_canonical_host' "$conf"
grep -q 'proxy_set_header Upgrade \$http_upgrade' "$conf"
grep -q 'proxy_set_header Connection \$bonfyre_connection_upgrade' "$conf"
grep -q 'proxy_buffering off' "$conf"
grep -q 'proxy_read_timeout 1h' "$conf"
grep -q 'return 308 https://\$host\$request_uri' "$conf"
# Delegated estates authorize against their named-institution closure, not a
# generic session that could belong to a different client estate.
grep -q 'auth_request /_bonfyre_institution' "$root/deploy/nginx/aurekai-surfaces.conf"
grep -q 'map \$host \$bonfyre_institution' "$root/deploy/nginx/aurekai-surfaces.conf"
grep -q 'upper90.aurekai.work upper90' "$root/deploy/nginx/aurekai-surfaces.conf"
grep -q 'jrss.aurekai.work jrss' "$root/deploy/nginx/aurekai-surfaces.conf"
grep -q 'ai-loss.aurekai.work ai-loss' "$root/deploy/nginx/aurekai-surfaces.conf"
grep -q 'leads.aurekai.work leads' "$root/deploy/nginx/aurekai-surfaces.conf"
grep -q 'auth_request /_bonfyre_institution' "$root/deploy/nginx/aurekai-surfaces.conf"
grep -q 'location = /_bonfyre_institution' "$root/deploy/nginx/aurekai-surfaces.conf"
grep -q 'X-Bonfyre-Institution \$bonfyre_institution' "$root/deploy/nginx/aurekai-surfaces.conf"
grep -q 'access_log off' "$root/deploy/nginx/aurekai-surfaces.conf"
grep -q 'noindex, nofollow, noarchive, nosnippet' "$root/deploy/nginx/aurekai-surfaces.conf"
# Plain HTTP never redirects a client hostname into a discoverable HTTPS URL,
# and the private virtual host permanently declines public machine carriers.
grep -q 'server_name upper90.aurekai.work jrss.aurekai.work ai-loss.aurekai.work leads.aurekai.work;' "$root/deploy/nginx/aurekai-surfaces.conf"
grep -q 'Client hosts do not redirect from plaintext HTTP' "$root/deploy/nginx/aurekai-surfaces.conf"
grep -q 'X-Frame-Options "DENY"' "$root/deploy/nginx/aurekai-surfaces.conf"
grep -q 'Cross-Origin-Resource-Policy "same-origin"' "$root/deploy/nginx/aurekai-surfaces.conf"
grep -q 'ro-crate' "$root/deploy/nginx/aurekai-surfaces.conf"
# A delegated client host terminates at its named native dispatcher. It has no
# legacy 404 fallthrough into a public or unaffiliated application surface.
grep -q 'proxy_pass http://127.0.0.1:8787;' "$root/deploy/nginx/aurekai-surfaces.conf"
! grep -q '@bonfyre_legacy_surface' "$root/deploy/nginx/aurekai-surfaces.conf"
if grep -Eq '(^|[[:space:]])(rewrite|try_files)[[:space:]]' "$conf"; then
    echo 'Nginx edge contains application routing' >&2
    exit 1
fi
echo 'nginx address edge: PASS'
