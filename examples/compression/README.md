# Lambda Tensors Compression Demo

Compress 1,000 JSON records and see the difference. Random access included.

## Quick start

```bash
cd ../../
make lib    # build liblambda-tensors
cd examples/compression/
./run.sh
```

## Expected output

```
=== Lambda Tensors Compression Demo ===

Generating 1,000 sample JSON records...
Raw JSON size: 68420 bytes

Compressing with Lambda Tensors...
Compressed 68420 bytes → 9237 bytes (13.5% of original)

=== Results ===
Raw JSON:        68420 bytes
Lambda Tensors:  9237 bytes (13.5% of original)

Key advantage: random access to ANY field in ANY record
without decompressing the whole dataset.
```

## What's different from gzip?

| Feature | gzip | Lambda Tensors |
|---|---|---|
| Compression ratio | ~5.5% | ~13.5% |
| Random access | No (must decompress all) | **Yes** (O(1) per field) |
| Knows about JSON structure | No | **Yes** |
| Read field 3000 without decompressing | No | **Yes** |

Lambda Tensors is not trying to beat gzip on raw ratio — it's a different tool. It understands that JSON records share structure, and exploits that for both compression and random access.

## Files

| File | Purpose |
|---|---|
| `run.sh` | Builds and runs the full demo |
| `compress-demo.c` | C source — generates data, compresses, reads |
