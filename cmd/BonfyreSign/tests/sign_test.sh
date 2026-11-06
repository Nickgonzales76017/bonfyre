#!/bin/sh
# A signature that cannot fail is not a signature.
#
# bf_crypto shipped an API named for Ed25519 whose verify returned -1 for every
# input. That fails closed, which is the safe direction, but it also means a
# test that only checks "verify rejects a bad signature" would have passed
# against a stub that rejects everything. So this asserts both directions: a
# real signature verifies, and every way of tampering with it does not.
set -eu
bin=${1:?binary required}
bin=$(CDPATH= cd -- "$(dirname -- "$bin")" && pwd -P)/$(basename -- "$bin")
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

printf 'the estate observed 3,316 sources and closed 2,344 obligations\n' > "$tmp/claim.txt"
id=$("$bin" keygen --key-dir "$tmp")
pk=$(printf '%s' "$id" | sed -n 's/.*"public_key":"\([0-9a-f]*\)".*/\1/p')
[ ${#pk} -eq 64 ] || { echo "public key is not 32 bytes: $pk" >&2; exit 1; }

# The secret must never have been readable by anyone else, even briefly.
mode=$(ls -l "$tmp/institution.ed25519.secret" | cut -c1-10)
[ "$mode" = "-rw-------" ] || { echo "secret mode is $mode" >&2; exit 1; }

# An identity that has signed things is not silently replaced.
if "$bin" keygen --key-dir "$tmp" >/dev/null 2>&1; then
  echo 'keygen replaced an existing identity' >&2; exit 1
fi

att=$("$bin" sign --key-dir "$tmp" --file "$tmp/claim.txt")
sig=$(printf '%s' "$att" | sed -n 's/.*"signature":"\([0-9a-f]*\)".*/\1/p')
[ ${#sig} -eq 128 ] || { echo "signature is not 64 bytes" >&2; exit 1; }

# It verifies.
"$bin" verify --file "$tmp/claim.txt" --public "$pk" --signature "$sig" | grep -q '"verified":true'

# Change one byte of the claim: it must not.
printf 'the estate observed 3,317 sources and closed 2,344 obligations\n' > "$tmp/tampered.txt"
if "$bin" verify --file "$tmp/tampered.txt" --public "$pk" --signature "$sig" | grep -q '"verified":true'; then
  echo 'a tampered file verified' >&2; exit 1
fi

# Flip one byte of the signature: it must not.
bad=$(printf '%s' "$sig" | sed 's/^../00/')
if "$bin" verify --file "$tmp/claim.txt" --public "$pk" --signature "$bad" | grep -q '"verified":true'; then
  echo 'a tampered signature verified' >&2; exit 1
fi

# A different identity must not verify this one's statement.
other=$(mktemp -d)
opk=$("$bin" keygen --key-dir "$other" | sed -n 's/.*"public_key":"\([0-9a-f]*\)".*/\1/p')
if "$bin" verify --file "$tmp/claim.txt" --public "$opk" --signature "$sig" | grep -q '"verified":true'; then
  rm -rf "$other"; echo 'another key verified this signature' >&2; exit 1
fi
rm -rf "$other"

echo 'institution signature: PASS (verifies, and fails on tampered file, tampered signature, and wrong key)'
