#!/bin/sh
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
exec sh "$root/tests/requirements/workgraph_restart.sh"
