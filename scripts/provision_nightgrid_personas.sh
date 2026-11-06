#!/usr/bin/env bash
# Provision the Night Grid demo personas.
#
# These are synthetic accounts for a DemoInstitution in world DEMO. They exist
# because the estate had exactly two authority states to test against -- a
# superuser holding every grant, and 38 probe accounts holding none -- and every
# interesting authority bug lives between those.
#
# Deliberate choices:
#
#   Addresses use .invalid, a reserved TLD that can never resolve. No message
#   these accounts generate can reach a real mailbox, and no persona can be
#   mistaken for a real person's account.
#
#   Passwords are generated here and passed on stdin through http-signup rather
#   than as argv, so they never appear in the process list, in shell history, or
#   in a systemd journal line.
#
#   Roles come from the persona lattice declared in namespaces.yaff, not from
#   this script. What each persona may do is a contract question.
#
# Credentials are written to one file, chmod 600, outside the repository.
#
# Cannot infer: a provisioned persona is not an authorized one. Signup creates
# an account; the grant decides what it reaches, and every surface still checks.
set -euo pipefail
ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
PERSONAS="$ROOT/scripts/nightgrid-persona-contract.mjs"

KEY="${BONFYRE_SSH_KEY:?set BONFYRE_SSH_KEY}"
HOST="${BONFYRE_SSH_HOST:?set BONFYRE_SSH_HOST}"
OUT="${1:-$HOME/Documents/Bonfyre/NIGHTGRID-PERSONAS.md}"
CONTRACT="${BONFYRE_INSTITUTION_CONTRACT:-$ROOT/integrations/mcp/institution_contract.json}"
AUTH_DB=/var/lib/bonfyre/auth.db

remote() { ssh -i "$KEY" -o BatchMode=yes "$HOST" "$@"; }
[ -f "$CONTRACT" ] || { echo "no institution contract at $CONTRACT" >&2; exit 1; }

mkdir -p "$(dirname "$OUT")"
umask 077
tmp=$(mktemp); trap 'rm -f "$tmp"' EXIT

{
  echo "# Night Grid demo personas"
  echo
  echo "Generated $(date -u +%Y-%m-%dT%H:%M:%SZ). Synthetic accounts in a DemoInstitution"
  echo "(world DEMO). Addresses use the reserved .invalid TLD and can never receive mail."
  echo "This file is chmod 600 and lives outside the repository."
  echo
  echo "| persona | email | password | institution role | may touch |"
  echo "|---------|-------|----------|------------------|-----------|"
} > "$tmp"

bun "$PERSONAS" rows "$CONTRACT" | while IFS=$'\t' read -r persona rank caps; do
  email="${persona}@nightgrid.demo.invalid"
  name="Night Grid ${persona} (DEMO)"
  # Not `tr </dev/urandom | head -c`: under pipefail, head closing the pipe
  # sends tr SIGPIPE, the pipeline reports failure, and set -e kills the run
  # immediately after the first password is generated.
  password=$(openssl rand -base64 18 | tr -d '/+=' )

  # stdin, never argv
  created=$(printf '{"email":"%s","name":"%s","password":"%s"}' "$email" "$name" "$password" \
    | remote "sudo /usr/local/bin/bonfyre-auth --db $AUTH_DB http-signup" 2>/dev/null || true)
  uid=$(printf '%s' "$created" | bun "$PERSONAS" signup-id 2>/dev/null || true)

  if [ -z "${uid:-}" ]; then
    # A persona that already exists cannot have its password recovered -- the
    # store keeps a salted hash. Writing the file without its row would leave an
    # account nobody can sign in as, which is exactly what an interrupted first
    # run produced. Fail loudly instead of silently emitting a partial file.
    printf '  %-10s ALREADY EXISTS -- its password is unrecoverable\n' "$persona" >&2
    printf '  Remove it and re-run, or this file will be incomplete:\n' >&2
    printf "    sudo sqlite3 %s \"DELETE FROM institution_grants WHERE user_id=(SELECT id FROM users WHERE email='%s'); DELETE FROM users WHERE email='%s';\"\n" "$AUTH_DB" "$email" "$email" >&2
    echo "INCOMPLETE" > "$tmp.incomplete"
    continue
  fi

  # </dev/null matters: ssh reads stdin, and this loop is fed by a pipe. Without
  # it the grant call swallows the remaining personas and the loop ends after one.
  remote "sudo /usr/local/bin/bonfyre-auth --db $AUTH_DB institution-grant --user-id $uid --institution nightgrid --role $rank" </dev/null >/dev/null 2>&1 \
    && printf '  %-10s id=%-4s role=%-6s %s\n' "$persona" "$uid" "$rank" "$caps" \
    || printf '  %-10s id=%-4s GRANT FAILED\n' "$persona" "$uid"

  printf '| %s | `%s` | `%s` | %s | %s |\n' "$persona" "$email" "$password" "$rank" "$caps" >> "$tmp"
done

{
  echo
  echo "## What each persona must NOT gain"
  echo
  echo "The lattice is declared in \`architecture/institutional/namespaces.yaff\`."
  echo "A venue operator must not reach district administration; a sponsor must not"
  echo "gain generic institution admin; an editor must not reach finance. Those are"
  echo "the assertions \`nightgrid_lattice_test.sh\` makes."
  echo
  echo "## Revoking all of them"
  echo
  echo '```sh'
  echo "for id in \$(sudo sqlite3 $AUTH_DB \"SELECT id FROM users WHERE email LIKE '%@nightgrid.demo.invalid';\"); do"
  echo "  sudo /usr/local/bin/bonfyre-auth --db $AUTH_DB institution-revoke --user-id \$id --institution nightgrid"
  echo "done"
  echo '```'
} >> "$tmp"

if [ -f "$tmp.incomplete" ]; then
  rm -f "$tmp.incomplete"
  echo >&2
  echo "refusing to write $OUT: at least one persona already existed, so the file" >&2
  echo "would list fewer accounts than the estate actually has." >&2
  exit 1
fi

mv "$tmp" "$OUT"; chmod 600 "$OUT"; trap - EXIT
echo
echo "credentials written to $OUT (chmod 600)"
