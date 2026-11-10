# Models and compression

Download a trained sentence model, measure its saved weights and recover a compressed copy of your documentation.

The model is [sentence-transformers/paraphrase-MiniLM-L3-v2](https://huggingface.co/sentence-transformers/paraphrase-MiniLM-L3-v2), pinned to a specific revision and SHA-256. It maps text into 384-dimensional embeddings. The walkthrough uses its 69.6 MB Safetensors file.

## Build the tools

Start in a configured Bonfÿre source checkout with a C compiler, Python 3, curl, tar and Zstandard installed. Keep the commands in one shell.

```sh Terminal
make -C cmd/BonfyreModel
make -C cmd/BonfyreQuant
make -C cmd/BonfyreSLI bonfyre-sli
make -C cmd/BonfyreCompress
export PATH="$PWD/cmd/BonfyreModel:$PWD/cmd/BonfyreQuant:$PWD/cmd/BonfyreSLI:$PWD/cmd/BonfyreCompress:$PATH"
```

## Download and verify MiniLM

Use a separate registry and cache for this run. The manifest gives Model a pinned download address and the publisher's file digest. Model checks the digest before accepting the download.

```sh Terminal
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
```

Running `pull` again reuses the downloaded file. `verify` reads the file and checks its SHA-256 again.

## Measure every floating-point tensor

Quant writes each eligible tensor through the BQFP encoder, reads it back through the shared decoder and compares the recovered weights with the original. It measures the actual file size, including headers and codebooks.

```sh Terminal
bonfyre-quant roundtrip "$bonfyre_weights" --bits 3 \
  > "$bonfyre_compression/roundtrip.txt" || test "$?" -eq 2
cat "$bonfyre_compression/roundtrip.txt"
```

Exit code 2 means at least one tensor fell below the command's 0.999 cosine threshold. Other failures stop the walkthrough.

| MiniLM result | Measured value |
|---|---:|
| Floating-point tensors | 55 |
| Weights encoded and recovered | 17,389,824 |
| Original float32 weight bytes | 69,559,296 |
| Combined BQFP tensor bytes | 19,421,016 |
| Reduction | 72.1%, or 3.582× smaller |
| Physical bits per weight | 8.934 |
| Element-weighted cosine | 0.998609 |
| Lowest tensor cosine | 0.998582 |
| Tensors below 0.999 cosine | 25 |

One integer tensor is skipped. `--bits 3` selects the codec setting; this BQFP version stores lattice coefficients in signed bytes. The physical size above includes those bytes, tile indices, scales and codebooks. The benchmark processes one tensor at a time and removes its temporary file after measuring it.

## Keep a compressed attention matrix

Save the first layer's query matrix, recover it through SLI and compare it with the original model:

```sh Terminal
bonfyre_tensor=encoder.layer.0.attention.self.query.weight
bonfyre-quant encode "$bonfyre_weights" --tensor "$bonfyre_tensor" \
  --bits 3 --out "$bonfyre_compression/query.bqfp"
bonfyre-sli inspect --model "$bonfyre_compression/query.bqfp"
bonfyre-sli decode --model "$bonfyre_compression/query.bqfp" \
  --out "$bonfyre_compression/query.f32"
bonfyre-quant compare "$bonfyre_weights" --tensor "$bonfyre_tensor" \
  --weights "$bonfyre_compression/query.f32"
wc -c "$bonfyre_compression/query.bqfp" "$bonfyre_compression/query.f32"
```

The matrix contains 147,456 weights. Its BQFP file is 177,960 bytes; its recovered float32 file is 589,824 bytes. The measured cosine is 0.998747 and RMSE is 0.004176.

These measurements help choose storage and reconstruction tradeoffs. To select a representation for a search or generation application, run that application's evaluation with the recovered model as well. This walkthrough measures weight reconstruction; the [recording workspace](/recording-to-workspace) exercises local models in a working search application.

## Restore the documentation archive

Application documents can be compressed and recovered exactly. Pack this checkout's documentation, unpack it and compare the complete directory:

```sh Terminal
bonfyre-compress savings docs > "$bonfyre_compression/savings.json"
cat "$bonfyre_compression/savings.json"
bonfyre-compress family docs --out "$bonfyre_compression/docs.tar.zst"
mkdir "$bonfyre_compression/restored"
zstd -q -d -c "$bonfyre_compression/docs.tar.zst" \
  | tar -xf - -C "$bonfyre_compression/restored"
diff -r docs "$bonfyre_compression/restored/docs"
```

A successful `diff` prints nothing. The retained run packed 660,020 bytes of documentation into 190,981 bytes, saving 71.1%, and recovered every file exactly. Your total follows the documentation in your checkout.

## Reject an incomplete weight file

A transfer cut short must fail before producing decoded weights:

```sh Terminal
head -c 100 "$bonfyre_compression/query.bqfp" > "$bonfyre_compression/cut.bqfp"
if bonfyre-sli decode --model "$bonfyre_compression/cut.bqfp" \
  --out "$bonfyre_compression/cut.f32"; then
  printf '%s\n' 'Incomplete tensor unexpectedly decoded' >&2
  exit 1
fi
test ! -e "$bonfyre_compression/cut.f32"
```

Your run directory contains the pinned manifest, original model, measurements, saved attention matrix and restored documentation. Keep the files relevant to the workload you plan to ship.
