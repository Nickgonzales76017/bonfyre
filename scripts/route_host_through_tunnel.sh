#!/bin/sh
# Serve one estate host from this machine through the tunnel instead of the
# edge Worker, the way the territory apexes are served.  The Worker has a daily
# request cap on the free plan; a host routed here does not pass through it.
#
# Needs: a tunnel login certificate for the host's zone (cloudflared tunnel
# login, choosing that zone), and a wrangler login (bunx wrangler login).
#
#   route_host_through_tunnel.sh HOST [CERT]
set -eu
host=$1
cert=${2:-$HOME/.cloudflared/cert.pem}
tunnel=${BONFYRE_TUNNEL:-bonfyre-oracle-continuation}
zone=$(printf '%s' "$host" | awk -F. '{print $(NF-1)"."$NF}')
[ -f "$cert" ] || { echo "no tunnel certificate at $cert; run: cloudflared tunnel login (choose $zone)" >&2; exit 2; }
# Refreshes the saved login if it has lapsed.
env -u CLOUDFLARE_API_TOKEN -u CLOUDFLARE_ACCOUNT_ID bunx wrangler@4.147.0 whoami >/dev/null 2>&1 || { echo "wrangler is not logged in; run: bunx wrangler login" >&2; exit 2; }
token=$(sed -n 's/^oauth_token *= *"\(.*\)"/\1/p' "$HOME/Library/Preferences/.wrangler/config/default.toml")
api=https://api.cloudflare.com/client/v4
zid=$(curl -fsS "$api/zones?name=$zone" -H "Authorization: Bearer $token" | sed -n 's/.*"result":\[{"id":"\([0-9a-f]*\)".*/\1/p')
[ -n "$zid" ] || { echo "cannot read zone $zone" >&2; exit 3; }
out=$(cloudflared tunnel --origincert "$cert" route dns "$tunnel" "$host" 2>&1 | tail -1)
case "$out" in *"$host which will route"*|*"already configured"*) : ;; *) echo "the certificate is not for $zone: $out" >&2; exit 3 ;; esac
curl -fsS "$api/zones/$zid/workers/routes" -H "Authorization: Bearer $token" | grep -q "\"$host/\*\"" ||
  curl -fsS -X POST "$api/zones/$zid/workers/routes" -H "Authorization: Bearer $token" -H 'content-type: application/json' \
       -d "{\"pattern\":\"$host/*\"}" >/dev/null
sleep 15
printf '%s %s\n' "$host" "$(curl -s -o /dev/null -m 40 -w '%{http_code}' "https://$host/")"
