#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
auth="$root/cmd/BonfyreAuth/bonfyre-auth"
lex="$root/cmd/BonfyreLex/bonfyre-lex"
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-auth-institution.XXXXXX")
trap 'rm -rf "$tmp"' EXIT INT TERM
db="$tmp/auth.db"

"$auth" --db "$db" signup --email client-operator@example.test --name 'Client Operator' --password fixture-secret --tier free >/dev/null
login=$("$auth" --db "$db" login --email client-operator@example.test --password fixture-secret)
token=$(printf '%s' "$login" | "$lex" field --kind json_string --key session.token)
[ -n "$token" ] || { echo 'missing test session token' >&2; exit 1; }

# A real session is not a client-estate grant.
if "$auth" --db "$db" institution-authorize --token "$token" --institution jrss --role read >/dev/null 2>&1; then
    echo 'ungranted session accessed JRSS' >&2
    exit 1
fi

"$auth" --db "$db" institution-grant --user-id 1 --institution jrss --role read >/dev/null
"$auth" --db "$db" institution-authorize --token "$token" --institution jrss --role read | grep -q '"authorized":true'
if "$auth" --db "$db" institution-authorize --token "$token" --institution jrss --role write >/dev/null 2>&1; then
    echo 'read-only JRSS grant authorized write' >&2
    exit 1
fi
if "$auth" --db "$db" institution-authorize --token "$token" --institution ai-loss --role read >/dev/null 2>&1; then
    echo 'JRSS grant crossed into AI Loss' >&2
    exit 1
fi

# Parent grants descend only through the declared institutional tree.
"$auth" --db "$db" institution-grant --user-id 1 --institution upper90 --role admin >/dev/null
for child in jrss ai-loss leads; do
    "$auth" --db "$db" institution-authorize --token "$token" --institution "$child" --role write | grep -q '"authorized":true'
done
if "$auth" --db "$db" institution-authorize --token "$token" --institution bonfyre --role read >/dev/null 2>&1; then
    echo 'Upper90 grant climbed into Bonfyre' >&2
    exit 1
fi
"$auth" --db "$db" institution-revoke --user-id 1 --institution upper90 >/dev/null
if "$auth" --db "$db" institution-authorize --token "$token" --institution leads --role read >/dev/null 2>&1; then
    echo 'revoked parent grant remained usable by child' >&2
    exit 1
fi
"$auth" --db "$db" institution-grant --user-id 1 --institution bonfyre --role admin >/dev/null
for estate in bonfyre upper90 jrss ai-loss leads; do
    "$auth" --db "$db" institution-authorize --token "$token" --institution "$estate" --role admin | grep -q '"authorized":true'
done
"$auth" --db "$db" institution-revoke --user-id 1 --institution bonfyre >/dev/null

"$auth" --db "$db" institution-grant --user-id 1 --institution jrss --role write >/dev/null
"$auth" --db "$db" institution-authorize --token "$token" --institution jrss --role write | grep -q '"authorized":true'

# The same still-valid session must lose access at revocation time.
"$auth" --db "$db" institution-revoke --user-id 1 --institution jrss >/dev/null
if "$auth" --db "$db" institution-authorize --token "$token" --institution jrss --role read >/dev/null 2>&1; then
    echo 'revoked grant remained usable' >&2
    exit 1
fi

echo 'institution grants: PASS'
