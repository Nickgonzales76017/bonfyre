#!/usr/bin/env bash
# Compatibility entry point retained for the original fully qualified command.
# The v2 driver is tested against prior private scan directories and held cohorts.
set -Eeuo pipefail
exec /usr/bin/env bash "$(dirname "$0")/finish_private_convergence_v2.sh" "$@"
