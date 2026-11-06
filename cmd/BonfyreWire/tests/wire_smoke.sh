#!/bin/sh
# What this proves: the organ reads packets out of a real capture and derives
# flows, media candidates, metering, scaling and routing from them; and that
# something which is not a capture is refused.
#   PCAP=/path/to/real-sip-rtp.pcap sh cmd/BonfyreWire/tests/wire_smoke.sh [root]
set -eu

ROOT="${1:-${TMPDIR:-/tmp}/bonfyre_wire_test}"
BIN="${BIN:-cmd/BonfyreWire/bonfyre-wire}"
PCAP="${PCAP:?set PCAP to a real packet capture carrying SIP and RTP}"

rm -rf "$ROOT"
mkdir -p "$ROOT"
field() { sed -n "s/.*\"$1\":\"\{0,1\}\([^\",}]*\).*/\1/p"; }

CAPTURE_JSON="$($BIN ingest-pcap "$PCAP" --authorized --root "$ROOT")"
CAPTURE_ID="$(printf '%s' "$CAPTURE_JSON" | field capture_id)"
[ -n "$CAPTURE_ID" ]
[ "$(printf '%s' "$CAPTURE_JSON" | field packet_count)" -gt 0 ]

# dumb-device path: no --authorized required, locked to metadata-only
DUMB_CAPTURE_ID="$($BIN ingest-pcap "$PCAP" --dumb-device --root "$ROOT" | field capture_id)"
[ -n "$DUMB_CAPTURE_ID" ]
$BIN flows "$DUMB_CAPTURE_ID" --root "$ROOT" | grep -q '"app_proto"'

# dumb-device cannot be combined with --payload
if $BIN ingest-pcap "$PCAP" --dumb-device --payload --root "$ROOT" >/dev/null 2>&1; then exit 1; fi

$BIN flows "$CAPTURE_ID" --root "$ROOT" | grep -q '"app_proto":"RTP"'
$BIN flows "$CAPTURE_ID" --root "$ROOT" | grep -q '"app_proto":"SIP"'
$BIN media-detect "$CAPTURE_ID" --root "$ROOT" | grep -q '"media_flow_detected"'
$BIN meter "$CAPTURE_ID" --root "$ROOT" | grep -q '"record_hash"'
$BIN scale "$CAPTURE_ID" --root "$ROOT" | grep -q '"asr_workers_needed"'
$BIN route "$CAPTURE_ID" --root "$ROOT" | grep -q '"smallest_sufficient_local"'
$BIN report "$CAPTURE_ID" --root "$ROOT" | grep -q '"report_dir"'

# the same packets arriving as a stream are the same capture size
STREAM_JSON="$($BIN listen --interface stream --authorized --root "$ROOT" < "$PCAP")"
[ "$(printf '%s' "$STREAM_JSON" | field packet_count)" = "$(printf '%s' "$CAPTURE_JSON" | field packet_count)" ]

# a description of flows is not a capture of them
printf '%s\n' '{"src_ip":"10.0.0.10","dst_ip":"10.0.0.20","hint":"rtp"}' > "$ROOT/described.jsonl"
if $BIN ingest-pcap "$ROOT/described.jsonl" --authorized --root "$ROOT" >/dev/null 2>&1; then exit 1; fi
if $BIN listen --interface stream --authorized --root "$ROOT" < "$ROOT/described.jsonl" >/dev/null 2>&1; then exit 1; fi

echo "wire smoke ok"
