#!/bin/sh
# Test transport for `bonfyre-decide plane assess`: answers any request with the
# recorded provider envelope in BONFYRE_FAKE_TRANSPORT and HTTP 200, and
# records the request body and the curl config it was handed.
for a in "$@"; do
  case $a in @*) cp "${a#@}" "$BONFYRE_FAKE_CAPTURE.body" ;; esac
done
prev=''
for a in "$@"; do [ "$prev" = --config ] && cp "$a" "$BONFYRE_FAKE_CAPTURE.cfg"; prev=$a; done
cat "$BONFYRE_FAKE_TRANSPORT"
printf '\n200'
