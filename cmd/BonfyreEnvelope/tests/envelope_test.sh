#!/bin/sh
# A JSON envelope is lowered once into a typed message: a field the message
# does not have, or a value of the wrong type, is refused; the binary form is
# smaller and decodes to the same message; and a message's identity is the
# same whichever form or field order it arrived in.
set -eu
bin=${1:-./bonfyre-envelope}
tmp=$(mktemp -d); trap 'rm -rf "$tmp"' EXIT INT TERM
fail() { echo "FAIL: $1" >&2; exit 1; }
good='{"id":7,"operation":"hold","url":"https://school.example.org/notices/42","confirmed":true,"selection":"Bake sale Friday 3pm","note":"for the room parents"}'
printf '%s' "$good" | "$bin" check OperationEnvelope || fail "a well-formed envelope was refused"
printf '%s' '{"id":7,"operation":"hold","confirmed":true,"run":"rm -rf"}' | "$bin" check OperationEnvelope 2>/dev/null && fail "a field the message does not have was accepted"
printf '%s' '{"id":"seven","operation":"hold"}' | "$bin" check OperationEnvelope 2>/dev/null && fail "a number given as words was accepted"
printf '%s' '{"id":7,"operation":"hold","confirmed":"yes"}' | "$bin" check OperationEnvelope 2>/dev/null && fail "a truth value given as text was accepted"
# The library's own JSON reader takes the text "yes" for true; this must not.
for lenient in '"yes"' '"true"' '"1"' '1' 'null'; do
  printf '{"id":7,"operation":"hold","confirmed":%s}' "$lenient" | "$bin" check OperationEnvelope 2>/dev/null && fail "confirmed given as $lenient was accepted"
done
printf '%s' '{"id":"7","operation":"hold"}' | "$bin" check OperationEnvelope 2>/dev/null && fail "a number given as text was accepted"
printf '%s' "$good" | "$bin" check HeldSelection 2>/dev/null && fail "an envelope passed as a different message"

printf '%s' "$good" | "$bin" encode OperationEnvelope > "$tmp/op.bin"
[ "$(wc -c < "$tmp/op.bin")" -lt "${#good}" ] || fail "the binary form is not smaller than the JSON"
back=$("$bin" decode OperationEnvelope < "$tmp/op.bin")
for part in '"id":"7"' '"operation":"hold"' '"confirmed":true' '"note":"for the room parents"'; do echo "$back" | grep -q "$part" || fail "decoded message lost $part: $back"; done

# Identity: the same message in another field order, with defaults written out, and as binary.
a=$(printf '%s' "$good" | "$bin" digest OperationEnvelope)
b=$(printf '%s' '{"note":"for the room parents","selection":"Bake sale Friday 3pm","confirmed":true,"url":"https://school.example.org/notices/42","operation":"hold","id":7}' | "$bin" digest OperationEnvelope)
c=$("$bin" digest OperationEnvelope < "$tmp/op.bin")
[ "$a" = "$b" ] && [ "$a" = "$c" ] || fail "one message has more than one identity ($a $b $c)"
d=$(printf '%s' '{"id":7,"operation":"hold","url":"https://school.example.org/notices/42","confirmed":false,"selection":"Bake sale Friday 3pm","note":"for the room parents"}' | "$bin" digest OperationEnvelope)
[ "$a" != "$d" ] || fail "a message that was not confirmed has the identity of one that was"
printf 'garbage' | "$bin" decode EffectAttempt >/dev/null 2>&1 && fail "bytes that are not a message were decoded"
echo "PASS envelope: ${#good} bytes of JSON, $(wc -c < "$tmp/op.bin" | tr -d ' ') bytes typed"
