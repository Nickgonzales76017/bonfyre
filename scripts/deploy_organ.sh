#!/usr/bin/env bash
# Deploy one native organ to the live estate, failing closed at every step.
#
# This exists because deploying by hand produced exactly the failure it now
# prevents. The hand-rolled line was:
#
#     sudo make 2>&1 | grep -E " error" || true
#     sudo install ... && systemctl restart ...
#
# The pipe sends make's exit status to grep, `|| true` discards what is left,
# and the install then ships whatever stale binary was still on disk from the
# previous successful build. The service restarts, reports active, and serves
# code that was never built from the source just copied. Nothing in that
# sequence says anything is wrong.
#
# Order matters here and is not stylistic:
#   build, and stop if it failed
#   install only bytes this run produced
#   restart, because installing without restarting leaves the old inode running
#   attest, because "restarted" is not "executing the new bytes"
#   probe a real route, because "executing" is not "serving"
#
# Usage: deploy_organ.sh <organ-dir> <binary> <unit> [probe-url] [expect-status]
set -euo pipefail

ORGAN="${1:?organ directory under cmd/, e.g. BonfyreWebAddress}"
BINARY="${2:?installed binary name, e.g. bonfyre-web-address}"
UNIT="${3:?systemd unit}"
PROBE="${4:-}"
EXPECT="${5:-200}"

KEY="${BONFYRE_SSH_KEY:?set BONFYRE_SSH_KEY}"
HOST="${BONFYRE_SSH_HOST:?set BONFYRE_SSH_HOST}"
ORGAN_DIR="cmd/$ORGAN"
SRC="$ORGAN_DIR/src/main.c"
REMOTE_SRC="/opt/bonfyre/native-src/cmd/$ORGAN"

step() { printf '\n== %s\n' "$1"; }
remote() { ssh -i "$KEY" -o BatchMode=yes "$HOST" "$@"; }
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)

[ -f "$SRC" ] || { echo "no source at $SRC" >&2; exit 1; }

step "build locally first, so a broken change never reaches the estate"
( cd "cmd/$ORGAN" && make ) >/dev/null

# Ship the organ, not one file of it.
#
# This staged src/main.c and nothing else. BonfyreCMS compiles seventeen sources
# and BonfyreWebAddress two, so a deploy replaced one translation unit and then
# built it against whatever generation of the others was on the estate. Neither
# the local binary nor a coherent one: a mixture, produced by a script whose
# whole subject is not shipping mixtures.
#
# The Makefile is part of the source. The estate's WebAddress Makefile predated
# the projection source and did not compile it, so the same main.c built two
# different programs in the two places and the deploy could not tell.
step "stage the whole organ: every source, generated header, test witness and Makefile"
# COPYFILE_DISABLE: macOS tar otherwise writes an AppleDouble ._<name> beside
# every file, which land on the estate as extra sources named .c and .h. The
# attestation below caught them on the first run, which is the point of it, but
# a deploy tool should not be the thing manufacturing stale sources.
# Sources only, never built artifacts. The first attempt shipped the directory
# wholesale, which carried the locally-compiled tests/site-fragment-projection-test
# with it; the estate's make then found that binary newer than its sources, ran it,
# and a Darwin executable failed on Linux with a shell syntax error. Shipping a
# built binary and letting make believe it is current is the same stale-bytes
# failure this script exists to prevent, one directory over.
# A build step is not always a compiler. This organ's Makefile now runs a
# Python surface compiler and a Python experience test before it links
# anything, and the manifest shipped only C, headers and shell -- so the
# estate's build failed on a file the deploy could not have sent. The manifest
# follows what the build actually opens, not what the language used to be.
# Generated native headers are build inputs too. A new executor against the
# origin's previous generated program is a mixed generation, not a deployment.
( cd "$ORGAN_DIR" && find src tests generated -type f \
      \( -name '*.c' -o -name '*.h' -o -name '*.sh' -o -name '*.py' \) 2>/dev/null \
    | grep -v '/\._' | sort
  [ -f Makefile ] && echo Makefile ) > /tmp/$ORGAN.manifest
COPYFILE_DISABLE=1 tar -C "$ORGAN_DIR" -cf - -T /tmp/$ORGAN.manifest \
  | ssh -i "$KEY" -o BatchMode=yes "$HOST" "cat > /tmp/$ORGAN.tar"
remote "set -e
  sudo mkdir -p $REMOTE_SRC
  sudo tar -C $REMOTE_SRC -xf /tmp/$ORGAN.tar"

# Shared native headers are build inputs too. Stage and attest the complete
# bounded libbonfyre include set, rather than assuming an origin checkout has
# the same version as the organ staged above.
step "stage and attest shared native headers"
COPYFILE_DISABLE=1 tar -cf - -T <(find lib/libbonfyre/include -type f -name '*.h' | sort) \
  | ssh -i "$KEY" -o BatchMode=yes "$HOST" \
      "sudo mkdir -p /opt/bonfyre/native-src/lib/libbonfyre/include && sudo tar -C /opt/bonfyre/native-src -xf -"
LOCAL_SHARED_DIGEST=$(find lib/libbonfyre/include -type f -name '*.h' | sort | xargs shasum -a 256 | shasum -a 256 | cut -d' ' -f1)
REMOTE_SHARED_DIGEST=$(remote "cd /opt/bonfyre/native-src && sudo find lib/libbonfyre/include -type f -name '*.h' | sort | xargs sha256sum | sha256sum | cut -d' ' -f1")
if [ "$LOCAL_SHARED_DIGEST" != "$REMOTE_SHARED_DIGEST" ]; then
  echo "  shared native headers disagree after staging" >&2
  exit 1
fi

# A witness needs its fixture, and this organ's build runs one that reads a
# content type another organ owns. Staging only the organ left the estate's
# build failing on a file it could not have had, which reads like a broken
# deploy rather than an unstaged dependency. Sources only, and only the ones a
# build step actually opens.
step "stage the content types this organ's witnesses read"
# A build step names its inputs as repo-relative paths, so follow them rather
# than guessing at a language or a directory. This organ's Makefile runs a
# Python surface compiler that reads the site's own home generator and rewrites
# the compiled page; neither lives under cmd/, and the estate's build failed on
# files the deploy had no rule to send.
FIXTURES=$( { grep -rho --binary-files=without-match --exclude-dir=__pycache__ \
                'cmd/Bonfyre[A-Za-z]*/content-types/[A-Za-z_]*\.json' \
                "$ORGAN_DIR/tests" 2>/dev/null
              grep -rhoE --binary-files=without-match \
                --exclude-dir=__pycache__ 'site/[A-Za-z0-9_./-]+\.(py|html)' \
                "$ORGAN_DIR/src" "$ORGAN_DIR/tests" 2>/dev/null
            } | sort -u || true)
if [ -n "$FIXTURES" ]; then
  echo "$FIXTURES" | while read -r f; do
    [ -f "$f" ] || { echo "  witness fixture missing locally: $f" >&2; exit 1; }
    echo "  $f"
  done
  COPYFILE_DISABLE=1 tar -cf - $FIXTURES \
    | ssh -i "$KEY" -o BatchMode=yes "$HOST" "cat > /tmp/$ORGAN.fixtures.tar"
  remote "set -e
    sudo mkdir -p /opt/bonfyre/native-src
    sudo tar -C /opt/bonfyre/native-src -xf /tmp/$ORGAN.fixtures.tar"
else
  echo "  none"
fi

# Digest before build. `cp` reports success for a file that lands somewhere the
# build never reads, and a stale extra source on the estate compiles happily
# beside the new ones. Comparing the sources the estate will actually build
# against the ones just sent is the only step that can catch either.
step "attest the estate's source tree is the one just sent"
LOCAL_DIGEST=$(cd "$ORGAN_DIR" && find src tests generated -type f \( -name '*.c' -o -name '*.h' -o -name '*.py' \) 2>/dev/null \
  | grep -v '/\._' | sort | xargs shasum -a 256 2>/dev/null | shasum -a 256 | cut -d' ' -f1)
REMOTE_DIGEST=$(remote "cd $REMOTE_SRC && sudo find src tests generated -type f \( -name '*.c' -o -name '*.h' -o -name '*.py' \) 2>/dev/null \
  | sort | xargs sha256sum 2>/dev/null | sha256sum | cut -d' ' -f1")
if [ "$LOCAL_DIGEST" != "$REMOTE_DIGEST" ]; then
  echo "  source trees disagree after staging:" >&2
  echo "    local  ${LOCAL_DIGEST:0:16}" >&2
  echo "    estate ${REMOTE_DIGEST:0:16}" >&2
  remote "cd $REMOTE_SRC && sudo find src tests generated -type f \( -name '*.c' -o -name '*.h' -o -name '*.py' \) 2>/dev/null | sort" \
    > /tmp/$ORGAN.remote.list 2>/dev/null || true
  ( cd "$ORGAN_DIR" && find src tests generated -type f \( -name '*.c' -o -name '*.h' -o -name '*.py' \) 2>/dev/null | sort ) \
    > /tmp/$ORGAN.local.list
  echo "  files present on one side only:" >&2
  diff /tmp/$ORGAN.local.list /tmp/$ORGAN.remote.list >&2 || true
  echo "  (an extra file on the estate is a stale source the build may still link)" >&2
  exit 1
fi
echo "  ${LOCAL_DIGEST:0:16} on both sides"

step "build on the estate; a failed build stops the deploy"
remote "set -e
  cd $REMOTE_SRC
  # No pipe: make's own exit status decides, and set -e acts on it.
  sudo make"

step "install and restart together; installed-but-not-restarted is the silent case"
case "$ORGAN" in
  BonfyreCMS) SUPPORT_SCRIPTS=(apply_fact_deltas.mjs) ;;
  BonfyreQuery) SUPPORT_SCRIPTS=(columnar_body.mjs lower_parquet_to_zarr.mjs) ;;
  BonfyreWebAddress) SUPPORT_SCRIPTS=(columnar_body.mjs bind_parquet_bodies.mjs promote_observation_sources.mjs cool_foreign_bodies.mjs inspect_foreign_field.mjs) ;;
  *) SUPPORT_SCRIPTS=() ;;
esac
for support in "${SUPPORT_SCRIPTS[@]}"; do
  # Ship each owner's companion program with the native binary and attest its
  # exact bytes. A stale remote script is as real a mixed generation as C.
  scp -q -i "$KEY" "$SCRIPT_DIR/$support" "$HOST:/tmp/$support"
  remote "sudo mkdir -p /opt/bonfyre/scripts && sudo install -m 0755 /tmp/$support /opt/bonfyre/scripts/$support"
  LOCAL_SUPPORT_DIGEST=$(shasum -a 256 "$SCRIPT_DIR/$support" | cut -d' ' -f1)
  REMOTE_SUPPORT_DIGEST=$(remote "sha256sum /opt/bonfyre/scripts/$support | cut -d' ' -f1")
  [ "$LOCAL_SUPPORT_DIGEST" = "$REMOTE_SUPPORT_DIGEST" ] || { echo "  $support digest disagrees after install" >&2; exit 1; }
done
remote "set -e
  cd $REMOTE_SRC
  sudo install -m 0755 -o root -g root $BINARY /usr/local/bin/$BINARY
  sudo systemctl restart $UNIT"
sleep 3

step "attest that the running inode is the deployed one"
REMOTE_BUN=$(remote "command -v bun")
[ -n "$REMOTE_BUN" ] || { echo "  Bun is not installed on the estate" >&2; exit 1; }
scp -q -i "$KEY" "$SCRIPT_DIR/release_attest.mjs" "$HOST:/tmp/release_attest.mjs"
remote "sudo install -m 0755 /tmp/release_attest.mjs /opt/bonfyre/scripts/release_attest.mjs"
remote "sudo $REMOTE_BUN /opt/bonfyre/scripts/release_attest.mjs --units $UNIT"

step "probe a real route; executing is not serving"
if [ -n "$PROBE" ]; then
  # Ask the way a person's browser asks. curl's default Accept is `*/*`, which
  # this estate honours literally: the organ answers as a machine, and a route
  # whose machine view requires a session then reports 401 from a deploy that
  # is completely healthy. Twice that read as a broken page.
  code=$(curl -s --max-time 25 -o /dev/null -w '%{http_code}' \
           -H 'Accept: text/html,application/xhtml+xml' "$PROBE")
  if [ "$code" != "$EXPECT" ]; then
    echo "  probe $PROBE returned $code, expected $EXPECT" >&2
    exit 1
  fi
  echo "  $PROBE -> $code"
else
  echo "  (no probe url given; deployment is not proven to serve)"
fi

printf '\n%s deployed and attested\n' "$BINARY"
