#!/usr/bin/env bash
# Seal the currently deployed organ set into a release the attestation can read.
#
# Sealing and attesting are different jobs. release_attest.mjs answers "are
# sealed, deployed and running the same bytes"; it cannot answer that for the
# organs in /usr/local/bin because no release ever sealed them. This produces
# that missing side, in the sha256sum manifest format the existing sealed
# release already uses, so there is one manifest format rather than two.
#
# Two refusals are deliberate:
#
#   It will not seal while a unit is running bytes other than the deployed
#   ones. Sealing then would record a set nobody is executing and give the
#   drift a certificate.
#
#   It will not seal backup copies. /usr/local/bin carries .bak-*, .previous
#   and timestamped duplicates; sealing those would make the ambiguity about
#   which binary is real permanent instead of visible.
#
# The seal is additive: a new directory, hashes of what is deployed, and no
# change to any current pointer. Nothing about the running estate moves.
#
# Cannot infer: a sealed artifact set is not a tested, authorized or good one.
# It records which bytes were deployed at a moment, and nothing else.
set -euo pipefail

KEY="${BONFYRE_SSH_KEY:?set BONFYRE_SSH_KEY}"
HOST="${BONFYRE_SSH_HOST:?set BONFYRE_SSH_HOST}"
NAME="${1:-organs-sealed-$(date -u +%Y%m%dT%H%M%SZ)}"

remote() { ssh -i "$KEY" -o BatchMode=yes "$HOST" "$@"; }
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REMOTE_BUN=$(remote "command -v bun")
[ -n "$REMOTE_BUN" ] || { echo "  Bun is not installed on the estate" >&2; exit 1; }
scp -q -i "$KEY" "$SCRIPT_DIR/release_attest.mjs" "$SCRIPT_DIR/release_metadata.mjs" "$HOST:/tmp/"
remote "sudo install -m 0755 /tmp/release_attest.mjs /opt/bonfyre/scripts/release_attest.mjs && sudo install -m 0755 /tmp/release_metadata.mjs /opt/bonfyre/scripts/release_metadata.mjs"

echo "== refuse to seal over drift"
if ! remote "sudo $REMOTE_BUN /opt/bonfyre/scripts/release_attest.mjs" >/dev/null 2>&1; then
  echo "  attestation failed: a unit is not executing its deployed bytes." >&2
  echo "  Restart it and re-run; sealing now would certify bytes nobody runs." >&2
  exit 1
fi
echo "  every attested unit is executing what is on disk"

echo "== seal"
remote "set -euo pipefail
  R=/opt/bonfyre/releases/$NAME
  sudo mkdir -p \"\$R\"

  # Real organs only. A backup copy is excluded by name, not by guessing which
  # of two similar binaries is canonical.
  find /usr/local/bin -maxdepth 1 -type f -name 'bonfyre*' \
    ! -name '*.bak-*' ! -name '*.previous' ! -name '*.2026*' \
    | sort > /tmp/seal-organs.txt

  # A unit that executes an interpreted entrypoint deploys that file just as
  # much as an organ, and leaving it unsealed leaves that unit permanently
  # unanswerable. Take the last ExecStart of every bonfyre unit -- last,
  # because a drop-in clears and resets it, which is how the recursive gateway
  # replaced the mesh entrypoint.
  : > /tmp/seal-entrypoints.txt
  for unit in \$(systemctl list-units --type=service --no-pager --plain --no-legend 'bonfyre*' | awk '{print \$1}'); do
    sudo $REMOTE_BUN /opt/bonfyre/scripts/release_attest.mjs --entrypoint "\$unit" >> /tmp/seal-entrypoints.txt
  done
  sort -u /tmp/seal-entrypoints.txt | while read -r f; do [ -f "\$f" ] && echo "\$f"; done > /tmp/seal-entry-real.txt

  cat /tmp/seal-organs.txt /tmp/seal-entry-real.txt | sort -u > /tmp/seal-files.txt

  sudo cp /tmp/seal-files.txt \"\$R/organ-files.txt\"
  sudo sh -c \"xargs sha256sum < /tmp/seal-files.txt > '\$R/sha256-manifest.txt'\"

  count=\$(wc -l < /tmp/seal-files.txt)
  entries=\$(wc -l < /tmp/seal-entry-real.txt)
  excluded=\$(find /usr/local/bin -maxdepth 1 -type f \\( -name '*.bak-*' -o -name '*.previous' -o -name '*.2026*' \\) | wc -l)

  sudo $REMOTE_BUN /opt/bonfyre/scripts/release_metadata.mjs "$NAME" "\$count" "\$entries" "\$excluded" | sudo tee "\$R/release.json" >/dev/null
  echo \"  sealed \$count artifacts, \$entries of them unit entrypoints (\$excluded backup copies excluded)\"
  echo \"  \$R\"
"

echo "== verify the seal answers the question it was made for"
remote "sudo $REMOTE_BUN /opt/bonfyre/scripts/release_attest.mjs --manifest /opt/bonfyre/releases/$NAME/sha256-manifest.txt"
