#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
auth="$root/cmd/BonfyreAuth/bonfyre-auth"
lex="$root/cmd/BonfyreLex/bonfyre-lex"
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-auth-actor.XXXXXX")
trap 'rm -rf "$tmp"' EXIT INT TERM
db="$tmp/auth.db"

"$auth" --db "$db" signup --email media-viewer@example.test --name 'Media Viewer' --password fixture-secret --tier free >/dev/null
login=$("$auth" --db "$db" login --email media-viewer@example.test --password fixture-secret)
token=$(printf '%s' "$login" | "$lex" field --kind json_string --key session.token)
[ -n "$token" ] || { echo 'missing test session token' >&2; exit 1; }

# Authentication by itself does not select, infer, or create an Actor.
if "$auth" --db "$db" actor-closure --token "$token" >/dev/null 2>&1; then
    echo 'unbound session produced an ActorClosure' >&2
    exit 1
fi

"$auth" --db "$db" actor-bind --user-id 1 --actor actor:media-viewer >/dev/null
"$auth" --db "$db" actor-closure --token "$token" > "$tmp/closure.json"
grep -q '"authenticated":true' "$tmp/closure.json"
grep -q '"actor_subject":"actor:media-viewer"' "$tmp/closure.json"
grep -q 'not_actor_authority_or_rights' "$tmp/closure.json"
printf '%s\n' "$token" | "$auth" --db "$db" actor-closure-stdin > "$tmp/stdin-closure.json"
cmp "$tmp/closure.json" "$tmp/stdin-closure.json"

# A current session loses its closure immediately when the binding is revoked.
"$auth" --db "$db" actor-revoke --user-id 1 >/dev/null
if "$auth" --db "$db" actor-closure --token "$token" >/dev/null 2>&1; then
    echo 'revoked ActorClosure remained usable' >&2
    exit 1
fi

echo 'actor closures: PASS'
