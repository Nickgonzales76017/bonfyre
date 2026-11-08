#!/bin/sh
# The guard, in both directions.
#
# It used strstr against the whole comma-joined entitlement list, which is a
# substring test and not a membership test, and it failed both ways at once.
#
# Open: a free key entitled to "Ingest,Brief,Proof" allowed the operation "e".
# It allowed "In", "rie", "f,Proof", and the entire list passed as one operation
# name. Any fragment of any entitlement was an entitlement.
#
# Closed: entitlements are capitalised and the capability registry this estate
# indexes is lowercase, so all 103 indexed capabilities were denied on free and
# pro keys.
#
# Neither was visible from the tier anyone would reach for. Enterprise carries
# allowed_ops "*", which short-circuits before the comparison, so the one tier
# most likely to be tested was the one tier that could not expose either bug.
# That is why this asserts against free and pro and treats enterprise as the
# case that proves least.
set -eu
bin=${1:?binary required}
bin=$(CDPATH= cd -- "$(dirname -- "$bin")" && pwd -P)/$(basename -- "$bin")
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

# Keys are signed by the institution identity and verified against a trusted
# public key, so this test makes its own identity and trusts only that.
sign=${BONFYRE_SIGN_BIN:-$(CDPATH= cd -- "$(dirname -- "$0")/../../BonfyreSign" && pwd -P)/bonfyre-sign}
[ -x "$sign" ] || { echo "bonfyre-sign not built at $sign" >&2; exit 1; }
mkdir -m 700 "$tmp/k"
"$sign" keygen --key-dir "$tmp/k" >/dev/null
export BONFYRE_SIGN_BIN="$sign" BONFYRE_GATE_KEY_DIR="$tmp/k" BONFYRE_GATE_TRUST="$tmp/k/institution.ed25519.public"

"$bin" issue --tier free --org Test --out "$tmp/free.json" >/dev/null 2>&1
"$bin" issue --tier pro  --org Test --out "$tmp/pro.json"  >/dev/null 2>&1
"$bin" issue --tier enterprise --org Test --out "$tmp/ent.json" >/dev/null 2>&1

# The entitlement list must actually be read. It lives inside the entitlements
# object, and reading it as a top-level key found it only while the shared JSON
# reader searched flat; when that reader learned about depth it went empty, and
# an empty list denies everything while looking like a policy decision.
"$bin" check "$tmp/free.json" 2>&1 | grep -q 'Ops: *Ingest,Brief,Proof' || {
  echo 'entitlements did not load; guard would deny everything for the wrong reason' >&2; exit 1; }

# Fails open: every one of these is a fragment, not an entitlement.
for bad in e In rie f,Proof Ingest,Brief,Proof ngest Brie; do
  if "$bin" guard "$tmp/free.json" --op "$bad" >/dev/null 2>&1; then
    echo "guard allowed '$bad', which is a fragment and not an entitlement" >&2; exit 1
  fi
done

# Fails closed: an entitlement is matched whole and without regard to case.
for good in Ingest ingest INGEST Brief brief Proof proof; do
  "$bin" guard "$tmp/free.json" --op "$good" >/dev/null 2>&1 || {
    echo "guard denied '$good', which the free tier is entitled to" >&2; exit 1; }
done

# A tier grants what it grants and no more.
"$bin" guard "$tmp/free.json" --op offer >/dev/null 2>&1 && {
  echo 'free key allowed offer' >&2; exit 1; }
"$bin" guard "$tmp/pro.json" --op offer >/dev/null 2>&1 || {
  echo 'pro key denied offer' >&2; exit 1; }
"$bin" guard "$tmp/pro.json" --op transcribe >/dev/null 2>&1 && {
  echo 'pro key allowed transcribe' >&2; exit 1; }

# Wildcard still means everything, and is the weakest evidence here.
"$bin" guard "$tmp/ent.json" --op anything-at-all >/dev/null 2>&1 || {
  echo 'enterprise wildcard denied' >&2; exit 1; }

# A revoked key grants nothing regardless of entitlement.
sed 's/"status": "active"/"status": "revoked"/' "$tmp/free.json" > "$tmp/revoked.json"
"$bin" guard "$tmp/revoked.json" --op ingest >/dev/null 2>&1 && {
  echo 'revoked key still granted' >&2; exit 1; }

echo 'gate guard: PASS (fragments refused, real ops allowed either case, revocation holds)'
