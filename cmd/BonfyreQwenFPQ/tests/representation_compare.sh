#!/bin/sh
# One model, two representations, one consumer. The safetensors source is
# converted to a native v9 pack, and that pack and the FP16 passthrough pack
# each generate from the same prompt through bonfyre-qwen-fpq, greedy. Reports
# file size, peak memory, load, prefill, decode rate and how many generated
# tokens agree. It measures; it asserts only that both runs complete.
#
# Slow by design: the v9 run took about 23 minutes on the machine it was
# written on. Not part of any default test target.
set -eu
fpq=${BONFYRE_FPQ:-$HOME/.bonfyre/bin/bonfyre-fpq}
qwen=${BONFYRE_QWEN_FPQ:-$HOME/.bonfyre/bin/bonfyre-qwen-fpq}
models=${BONFYRE_MODELS:-$HOME/BonfyreModels}
source=$models/small/qwen2.5-coder-0.5b-instruct/model.safetensors
tokenizer=$models/small/qwen2.5-coder-0.5b-instruct/tokenizer.json
passthrough=$models/fpq/qwen2.5-coder-0.5b-instruct-native-fp16.fpq
tokens=${TOKENS:-48}
for need in "$fpq" "$qwen" "$source" "$tokenizer" "$passthrough" "$passthrough.config.json"; do
  [ -e "$need" ] || { echo "SKIP: $need not present"; exit 0; }
done
work=${WORK:-$(mktemp -d)}
cd "$work"
printf 'Write a Python function clamp(x, low, high) that returns x limited to the range.\n' > prompt.txt
[ -s v9.fpq ] || "$fpq" convert-fpq "$source" v9.fpq > convert.log 2>&1
# The runtime reads the model's shape from a config beside the pack; without
# it the layer count is guessed and the run dies asking for a layer past the last.
sed 's/"compression": *"[^"]*"/"compression": "fpq-v9-convert-fpq"/' "$passthrough.config.json" > v9.fpq.config.json
run() {
  ( /usr/bin/time -l "$qwen" bench-generate --pack "$2" --tokenizer "$tokenizer" --prompt prompt.txt --out "$1.txt" \
      --max-new-tokens "$tokens" --greedy --json "$1.json" --token-trace "$1.trace.jsonl" ) > "$1.log" 2>&1 \
    || { echo "FAIL: $1 did not complete"; tail -3 "$1.log"; exit 1; }
}
run passthrough "$passthrough"
run v9 "$work/v9.fpq"
report() {
  printf '%s | %s bytes | %s peak resident bytes | ' "$1" "$(wc -c < "$2" | tr -d ' ')" "$(awk '/maximum resident set size/{print $1}' "$1.log")"
  jq -r '"load \(.model_load_seconds)s | prefill \(.prefill_seconds)s | decode \(.tok_per_sec_decode_only) tok/s | \(.generated_tokens) tokens"' "$1.json"
}
report passthrough "$passthrough"
report v9 "$work/v9.fpq"
jq -r 'select(.type=="token") | .token_id' passthrough.trace.jsonl > a.ids
jq -r 'select(.type=="token") | .token_id' v9.trace.jsonl > b.ids
paste a.ids b.ids | awk '{n++; if ($1==$2) same++; else if (!first) first=n} END{printf "tokens agreeing: %d of %d; first divergence at token %s\n", same, n, first ? first : "none"}'
echo "cannot infer: one prompt and $tokens greedy tokens -> generation quality; agreement with the passthrough -> agreement with the original framework"
echo "work kept at $work"
