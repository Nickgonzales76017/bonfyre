set -eu
make -C cmd/BonfyreModel
make -C cmd/BonfyreQuant
make -C cmd/BonfyreSLI bonfyre-sli
make -C cmd/BonfyreCompress
export PATH="$PWD/cmd/BonfyreModel:$PWD/cmd/BonfyreQuant:$PWD/cmd/BonfyreSLI:$PWD/cmd/BonfyreCompress:$PATH"

bonfyre_compression=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-compression-guide.XXXXXX")
printf '%s\n' "$bonfyre_compression"
export BONFYRE_MODEL_DB="$bonfyre_compression/models.db"
export BONFYRE_CATALOG_DB="$bonfyre_compression/catalog.db"
export BONFYRE_MODEL_CACHE="$bonfyre_compression/cache"
cat > "$bonfyre_compression/model.json" <<'MODEL'
{
  "id": "minilm-l3-guide",
  "name": "paraphrase-MiniLM-L3-v2",
  "format": "safetensors",
  "sha256": "cf1e4e2d420c664973037c3c73125d7a8fc69952495093ef8f50596f8943a433",
  "size_mb": 69.569488,
  "sources": [{
    "url": "https://huggingface.co/sentence-transformers/paraphrase-MiniLM-L3-v2/resolve/4ca70771034acceecb2e72475f72050fcdde4ddc/model.safetensors",
    "scheme": "https"
  }]
}
MODEL
bonfyre-model add "$bonfyre_compression/model.json"
bonfyre-model pull minilm-l3-guide
bonfyre-model verify minilm-l3-guide
bonfyre_weights="$BONFYRE_MODEL_CACHE/minilm-l3-guide.safetensors"

bonfyre-quant roundtrip "$bonfyre_weights" --bits 3 \
  > "$bonfyre_compression/roundtrip.txt" || test "$?" -eq 2
cat "$bonfyre_compression/roundtrip.txt"

bonfyre_tensor=encoder.layer.0.attention.self.query.weight
bonfyre-quant encode "$bonfyre_weights" --tensor "$bonfyre_tensor" \
  --bits 3 --out "$bonfyre_compression/query.bqfp"
bonfyre-sli inspect --model "$bonfyre_compression/query.bqfp"
bonfyre-sli decode --model "$bonfyre_compression/query.bqfp" \
  --out "$bonfyre_compression/query.f32"
bonfyre-quant compare "$bonfyre_weights" --tensor "$bonfyre_tensor" \
  --weights "$bonfyre_compression/query.f32"
wc -c "$bonfyre_compression/query.bqfp" "$bonfyre_compression/query.f32"

bonfyre-compress savings docs > "$bonfyre_compression/savings.json"
cat "$bonfyre_compression/savings.json"
bonfyre-compress family docs --out "$bonfyre_compression/docs.tar.zst"
mkdir "$bonfyre_compression/restored"
zstd -q -d -c "$bonfyre_compression/docs.tar.zst" \
  | tar -xf - -C "$bonfyre_compression/restored"
diff -r docs "$bonfyre_compression/restored/docs"

head -c 100 "$bonfyre_compression/query.bqfp" > "$bonfyre_compression/cut.bqfp"
if bonfyre-sli decode --model "$bonfyre_compression/cut.bqfp" \
  --out "$bonfyre_compression/cut.f32"; then
  printf '%s\n' 'Incomplete tensor unexpectedly decoded' >&2
  exit 1
fi
test ! -e "$bonfyre_compression/cut.f32"

printf "%s\n" "$bonfyre_compression" > .bonfyre-runtime/compression-guide-20261011/guide-run-path.txt
