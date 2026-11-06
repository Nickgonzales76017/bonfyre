#!/bin/sh
# Watch is a generated native SiteProgram lowerer.  It must never become a
# browser-side JSON reader that selects a renderer, builds markup, or persists
# playback identity in localStorage.
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
header="$root/cmd/BonfyreWebAddress/generated/native_site_application_lowerers.h"

grep -q 'bf_site_application_Watch_ops' "$header"
# The generator emits the application's own slug between the kind and the
# content type it lowers, so the two names are no longer adjacent. What this
# asserts is the binding, not the byte layout: Watch lowers MediaProgram, on
# one record.
grep -q '"Watch","watch","MediaProgram"' "$header"
grep -q 'BF_SITE_MATERIAL_READER_MEDIA_PROGRAM' "$header"
! test -e "$root/cmd/BonfyreWebAddress/src/watch_fragment_ui.c"
! test -e "$root/cmd/BonfyreWebAddress/src/community_fragment_ui.c"
echo 'watch media-program fixture: PASS'
