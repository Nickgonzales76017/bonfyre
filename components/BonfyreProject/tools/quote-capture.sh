#!/bin/sh
# Record retained HTTP 402 prices without paying or fetching anything.
# Body and payment-required header envelopes are supported; both raw witnesses
# are hashed. Quoted is only rung one: no commitment, payment or settlement.
# Cannot infer: a quote -> an intention to pay, a budget, or a fair price.
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd -P)
cd "$root"
quotes=${1:-$HOME/.bonfyre/estate/quotes}
out=${2:-$HOME/.bonfyre/estate/quote-evidence.jsonl}
exec bun "$root/components/BonfyreProject/tools/economic-reports.mjs" quote-capture "$quotes" "$out"
