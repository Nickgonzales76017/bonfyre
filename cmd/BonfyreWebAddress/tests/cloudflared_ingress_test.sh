#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
config="$root/deploy/cloudflared/bonfyre-addressplane-ingress.yml"
if ! command -v cloudflared >/dev/null 2>&1; then
    echo 'cloudflared address ingress: SKIP (cloudflared unavailable)'
    exit 0
fi
cloudflared tunnel --config "$config" ingress validate
cloudflared tunnel --config "$config" ingress rule https://mcp.aurekai.work/mcp | grep -q 'service: http://127.0.0.1:8791'
cloudflared tunnel --config "$config" ingress rule https://auth.aurekai.work/.well-known/oauth-authorization-server | grep -q 'service: http://127.0.0.1:8791'
cloudflared tunnel --config "$config" ingress rule https://leaflounge.aurekai.shop/plants/luna | grep -q 'service: http://127.0.0.1:28901'
cloudflared tunnel --config "$config" ingress rule https://theleaflounge.store/plants/luna | grep -q 'service: http://127.0.0.1:28901'
cloudflared tunnel --config "$config" ingress rule https://foreign.example/ | grep -q 'http_status:404'
echo 'cloudflared address ingress: PASS'
