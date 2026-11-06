#!/bin/sh
# A public profile relationship is useful without pretending that it has
# authenticated an OAuth account. This witness holds the ownership, session,
# revocation, and non-escalation boundaries at the Auth owner.
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
auth="$root/bonfyre-auth"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT INT TERM
db="$tmp/auth.db"

"$auth" --db "$db" signup --email one@example.test --name One --password correct-horse-battery-staple >/dev/null
"$auth" --db "$db" signup --email two@example.test --name Two --password correct-horse-battery-staple >/dev/null
one=$("$auth" --db "$db" login --email one@example.test --password correct-horse-battery-staple | sed -n 's/.*"token":"\([^"]*\)".*/\1/p')
two=$("$auth" --db "$db" login --email two@example.test --password correct-horse-battery-staple | sed -n 's/.*"token":"\([^"]*\)".*/\1/p')
[ -n "$one" ] && [ -n "$two" ]

"$auth" --db "$db" external-link-claim --token "$one" --provider github \
  --remote-account octo-one --profile-url https://github.com/octo-one --display-name 'Octo One' |
  grep -q '"proof_state":"declared"'
"$auth" --db "$db" external-link-list --token "$one" |
  grep -q 'https://github.com/octo-one'
if "$auth" --db "$db" external-link-list --token "$two" | grep -q octo-one; then
  echo "external account link crossed account boundary" >&2
  exit 1
fi

# A locator is not a token-shaped value or arbitrary remote request target.
if "$auth" --db "$db" external-link-claim --token "$one" --provider github \
  --remote-account bad --profile-url 'https://token@example.test/secret' >/dev/null 2>&1; then
  echo "credential-shaped profile URL was accepted" >&2
  exit 1
fi
if "$auth" --db "$db" external-link-claim --token "$one" --provider github \
  --remote-account bad --profile-url http://github.com/bad >/dev/null 2>&1; then
  echo "non-HTTPS profile URL was accepted" >&2
  exit 1
fi

"$auth" --db "$db" external-link-revoke --token "$one" --link-id 1 | grep -q '"revoked":true'
"$auth" --db "$db" external-link-list --token "$one" | grep -q '"proof_state":"revoked"'
if "$auth" --db "$db" external-link-revoke --token "$two" --link-id 1 >/dev/null 2>&1; then
  echo "another account revoked a link it does not own" >&2
  exit 1
fi

echo "external account link: ok"
