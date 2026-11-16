#!/bin/sh
# Guard the ui_open_v call protocol: ui_open_v takes literal string pairs
# terminated by ui_open_a's NULL. A UiAttr array argument is a type error the
# compiler accepts silently (varargs) and spins or crashes the server at
# render time -- 24 such sites wedged every page front on 2026-11-15.
# The struct-array shape belongs to ui_elem only.
set -eu
here=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
src=$here/../src
fail=0
hits=$(grep -rn 'ui_open_v(.*(.*UiAttr' "$src" --include='*.h' --include='*.c' || true)
if [ -n "$hits" ]; then
  printf '%s\n' "$hits" >&2
  echo "ui_open_v guard: struct-array argument is ui_elem's protocol, not ui_open_v's" >&2
  fail=1
fi
# Direct ui_open_v calls must come through the ui_open_a macro (NULL
# terminator); a bare call with no terminator over-reads the stack.
direct=$(grep -rn '[^_a-zA-Z]ui_open_v\s*(' "$src" --include='*.h' --include='*.c' | grep -v 'static UI_UNUSED void ui_open_v' | grep -v '#define ui_open_a' || true)
if [ -n "$direct" ]; then
  printf '%s\n' "$direct" >&2
  echo "ui_open_v guard: call through ui_open_a so the argument list is NULL-terminated" >&2
  fail=1
fi
[ "$fail" = 0 ] && echo "ui_open_v guard: ok"
exit "$fail"
