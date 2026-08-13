#!/bin/sh
set -eu

ROOT="${1:-/tmp/bonfyre_wire_test}"
BIN="${BIN:-cmd/BonfyreWire/bonfyre-wire}"

rm -rf "$ROOT"
mkdir -p "$ROOT"

CAPTURE_JSON="$($BIN ingest-pcap cmd/BonfyreWire/fixtures/media_fixture.jsonl --authorized --root "$ROOT")"
CAPTURE_ID="$(printf '%s' "$CAPTURE_JSON" | sed -n 's/.*"capture_id":"\([^"]*\)".*/\1/p')"
[ -n "$CAPTURE_ID" ]

# dumb-device path: no --authorized required, locked to metadata-only
DUMB_CAPTURE_JSON="$($BIN ingest-pcap cmd/BonfyreWire/fixtures/media_fixture.jsonl --dumb-device --root "$ROOT")"
DUMB_CAPTURE_ID="$(printf '%s' "$DUMB_CAPTURE_JSON" | sed -n 's/.*"capture_id":"\([^"]*\)".*/\1/p')"
[ -n "$DUMB_CAPTURE_ID" ]
$BIN flows "$DUMB_CAPTURE_ID" --root "$ROOT" | grep -q '"app_proto"'

# verify dumb-device cannot be combined with --payload
set +e
$BIN ingest-pcap cmd/BonfyreWire/fixtures/media_fixture.jsonl --dumb-device --payload --root "$ROOT" >/dev/null 2>/dev/null
[ $? -ne 0 ]
set -e

$BIN flows "$CAPTURE_ID" --root "$ROOT" | grep -q '"app_proto":"RTP"'
$BIN media-detect "$CAPTURE_ID" --root "$ROOT" | grep -q 'probable_asr_candidate'
$BIN meter "$CAPTURE_ID" --root "$ROOT" | grep -q '"byte_count"'
$BIN scale "$CAPTURE_ID" --root "$ROOT" | grep -q '"asr_workers_needed"'
$BIN route "$CAPTURE_ID" --root "$ROOT" | grep -Eq '"smallest_sufficient_local":"(speech-loop|transcribe)"'
$BIN report "$CAPTURE_ID" --root "$ROOT" | grep -q '"report_dir"'

set +e
$BIN ingest-pcap cmd/BonfyreWire/fixtures/encrypted_fixture.jsonl --authorized --payload --unencrypted-only --root "$ROOT" >/tmp/bonfyre-wire-refusal.json 2>/tmp/bonfyre-wire-refusal.err
RC=$?
set -e
[ "$RC" -eq 0 ]
$BIN media-detect "$(printf '%s' "$($BIN ingest-pcap cmd/BonfyreWire/fixtures/encrypted_fixture.jsonl --authorized --root "$ROOT")" | sed -n 's/.*"capture_id":"\([^"]*\)".*/\1/p')" --root "$ROOT" | grep -q '"encrypted":true'

echo "wire smoke ok"
