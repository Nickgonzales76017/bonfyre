#!/bin/sh
# Assess retained grants.gov opportunities by residual class. This preserves
# the legacy evidence-presence heuristic, not grant-specific evidence checking.
# Partner, capital and temporal residuals remain separate. Attestation file
# presence is not signature verification; the report states these limits.
# Cannot infer: assessed -> applied for, eligible, or awarded.
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd -P)
cd "$root"
grants=${1:-$HOME/.bonfyre/grants/live}
estate=${ESTATE_DIR:-$HOME/.bonfyre/estate}
out=${2:-$estate/grant-closure.json}
exec bun "$root/components/BonfyreProject/tools/economic-reports.mjs" grant-closure "$grants" "$estate" "$out"
