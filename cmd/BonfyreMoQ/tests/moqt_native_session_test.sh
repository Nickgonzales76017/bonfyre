#!/bin/sh
# A real local QUIC witness, not a TCP relay or a wire-only fixture.  It
# starts the native `moqt-21` listener, performs TLS/QUIC handshake, then
# sends SETUP on a raw uni stream and SUBSCRIBE on a raw bidi stream.
set -eu

bin=${1:-./bonfyre-moq}
tmp=$(mktemp -d)
pid=''
cleanup() { [ -z "$pid" ] || kill "$pid" 2>/dev/null || true; [ -z "$pid" ] || wait "$pid" 2>/dev/null || true; rm -rf "$tmp"; }
trap cleanup EXIT INT TERM

openssl req -x509 -newkey rsa:2048 -keyout "$tmp/key.pem" -out "$tmp/cert.pem" \
  -days 1 -nodes -subj /CN=127.0.0.1 >/dev/null 2>&1
port=19444
"$bin" moqt-native-server --bind 127.0.0.1 --port "$port" --cert "$tmp/cert.pem" --key "$tmp/key.pem" >"$tmp/server.out" 2>"$tmp/server.err" &
pid=$!
for _ in $(seq 1 60); do grep -q 'listening_paired_control_stream_server' "$tmp/server.out" 2>/dev/null && break; sleep 0.05; done
grep -q '"alpn":"moqt-21"' "$tmp/server.out"

! "$bin" moqt-native-subscribe --host 127.0.0.1 --port "$port" --authority "127.0.0.1:$port" --path /live \
  --namespace bonfyre,world --track semantic-delta >"$tmp/client.out" 2>"$tmp/client.err"
grep -q '"transport":"native_quic"' "$tmp/client.out"
grep -q '"control_stream":"paired_setup_completed"' "$tmp/client.out"
grep -q '"request_stream":"subscribe_submitted"' "$tmp/client.out"
grep -q 'stream=' "$tmp/server.err"
grep -q 'stream=' "$tmp/client.err"
grep -q 'received paired server SETUP' "$tmp/client.err"
grep -q 'rejected subscribe .*UNAUTHORIZED' "$tmp/server.err"
grep -q 'received REQUEST_ERROR' "$tmp/client.err"
grep -q 'refused pending TransportLease authority' "$tmp/client.err"

echo 'BonfyreMoQ native QUIC session: PASS'
