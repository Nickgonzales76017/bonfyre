#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
tmp=$(mktemp -d)
cleanup() { rm -rf "$tmp"; }
trap cleanup EXIT INT TERM

(cd "$root" && cue export ./schemas/control -e placementPlane --out json) > "$tmp/placement.json"
bun "$root/tools/compile_placement_plane.mjs" --input "$tmp/placement.json" --out "$tmp/out"

bun -e 'import {readFileSync} from "node:fs"; import {join} from "node:path"; const dir=process.argv[1], get=p=>JSON.parse(readFileSync(join(dir,p),"utf8")), routes=get("cloudflare/worker-route-plan.json"), receipt=get("placement-receipt.json"), matrix=get("failure-matrix.json"), r2=get("cloudflare/r2-material-domain-plan.json"), sql=readFileSync(join(dir,"supabase/placement-plane.sql"),"utf8"); const patterns=new Set(routes.routes.map(x=>x.patterns||x.pattern)); for(const p of ["aurekai.work/oauth*","aurekai.work/mcp*","aurekai.work/.well-known/oauth-*","mcp.aurekai.work/*","auth.aurekai.work/*"]) if(!patterns.has(p)) throw Error(p); const primary=Object.fromEntries(receipt.services.map(x=>[x.id,x.primary])); if(primary["mcp-resource"]!=="oracle"||primary["oauth-authority"]!=="oracle"||primary["continuity-projection"]!=="supabase"||matrix.oracle_unavailable.public_delivery!=="serves last proven material"||r2.domain!=="material.aurekai.work"||r2.worker_exclusion!=="material.aurekai.work/*"||!sql.includes("placement_consume_oauth_transaction")||sql.includes("signing_key")||sql.includes("private_key")) throw Error("placement contract")' "$tmp/out"

ingress="$tmp/out/cloudflared/placement-plane-ingress.yml"
mcp_line=$(grep -n 'hostname: mcp.aurekai.work' "$ingress" | cut -d: -f1)
wildcard_line=$(grep -n 'hostname: "\*.aurekai.work"' "$ingress" | cut -d: -f1)
[ "$mcp_line" -lt "$wildcard_line" ]
grep -q 'BONFYRE_MCP_PUBLIC_ORIGIN=https://mcp.aurekai.work' "$tmp/out/systemd/bonfyre-oauth-mcp-placement.conf"
grep -q 'BONFYRE_AUTH_PUBLIC_ORIGIN=https://auth.aurekai.work' "$tmp/out/systemd/bonfyre-oauth-mcp-placement.conf"
echo 'placement plane: PASS'
