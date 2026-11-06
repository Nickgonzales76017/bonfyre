#!/bin/sh
set -eu

bin=${1:-./bonfyre-moq}
out=$("$bin" moqt-subscribe-wire --namespace bonfyre,world,deep-ellum --track semantic-delta --request-id 7)

printf '%s\n' "$out" | grep -q '"protocol":"draft-ietf-moq-transport-21"'
printf '%s\n' "$out" | grep -q '"message":"SUBSCRIBE"'
printf '%s\n' "$out" | grep -q '"wire_hex":"03002b070307626f6e6679726505776f726c640a646565702d656c6c756d0e73656d616e7469632d64656c746100"'
printf '%s\n' "$out" | grep -q '"truth":"not_authorized_not_subscribed"'
! "$bin" moqt-subscribe-wire --namespace '' --track semantic-delta >/dev/null 2>&1
! "$bin" moqt-subscribe-wire --namespace bonfyre --track '' >/dev/null 2>&1

setup=$("$bin" moqt-setup-wire --authority example.test --path /live?world=deep-ellum)
printf '%s\n' "$setup" | grep -q '"message":"SETUP"'
printf '%s\n' "$setup" | grep -q '"alpn":"moqt-21"'
printf '%s\n' "$setup" | grep -q '"truth":"not_authorized_not_connected"'
! "$bin" moqt-setup-wire --authority example.test --path relative >/dev/null 2>&1

echo 'BonfyreMoQ MOQT wire contract: PASS'
