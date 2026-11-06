#!/bin/sh
# Match retained declarations to demonstrated supply; eligibility is not authority.
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd -P)
cd "$root"
exec bun "$root/components/BonfyreProject/tools/project-demand.mjs" "$@"
