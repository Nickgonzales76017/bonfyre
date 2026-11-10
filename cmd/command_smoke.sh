#!/bin/sh
# Native command-surface witness. It intentionally has no interpreter fallback.
set -eu

if [ "$#" -eq 0 ]; then
  printf '%s\n' "usage: command_smoke.sh <native-command> [...]" >&2
  exit 2
fi

for command_path in "$@"; do
  if [ ! -x "$command_path" ]; then
    printf '%s\n' "missing executable: $command_path" >&2
    exit 1
  fi
  "$command_path" --help >/dev/null 2>&1
done
