#!/bin/sh
set -eu

source_dir=$(CDPATH= cd -- "$(dirname -- "$0")/../src" && pwd)
source_file="$source_dir/fpq_run.c"

grep -q 'return !v || strcmp(v, "0") != 0;' "$source_file"
grep -q 'BONFYRE_QWEN_ATTENTION_BIAS=0' "$source_file"
grep -q 'qwen_bias_applied' "$source_file"

echo "Qwen attention-bias contract passed: default-on, explicit zero opt-out"
