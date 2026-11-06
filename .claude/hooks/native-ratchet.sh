#!/bin/sh
# PostToolUse(Edit|Write|MultiEdit): when a script file (.mjs .js .cjs .ts .py)
# is written, hold the change to the native ratchet. A worse figure blocks with
# the reason, so the agent ports to C over YAFF/CUE or retires something else
# instead of growing the JS estate. Budgets: architecture/native-ratchet.cue.
event=$(cat)
path=$(printf '%s' "$event" | sed -n 's/.*"file_path" *: *"\([^"]*\)".*/\1/p' | head -1)
case "$path" in
  *.mjs|*.js|*.cjs|*.ts|*.tsx|*.py) ;;
  *) exit 0 ;;
esac
case "$path" in */node_modules/*|*/vendor/*) exit 0 ;; esac
root=${CLAUDE_PROJECT_DIR:-$(CDPATH='' cd -- "$(dirname -- "$0")/../.." && pwd)}
if out=$(BONFYRE_BIN=/nonexistent sh "$root/scripts/native-ratchet.sh" check 2>&1); then exit 0; fi
reason=$(printf 'The native ratchet refuses this: the JS/script estate grew past its budget.\n%s\n\nPort the behaviour to C over YAFF/CUE (docs/engineering/mjs-retirement-20261022/PLAN.md), or retire an equal amount with scripts/retire.sh. Budgets only go down; raising one is a reviewed edit to architecture/native-ratchet.cue.' "$out" |
  sed 's/\\/\\\\/g; s/"/\\"/g' | awk '{printf "%s\\n", $0}')
printf '{"decision":"block","reason":"%s"}\n' "$reason"
