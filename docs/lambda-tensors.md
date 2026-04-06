# Lambda Tensors

Lambda Tensors is a structural compression algorithm for JSON data. It's included in Bonfyre as `liblambda-tensors` (41 KB static library).

## The idea

Classical compression (gzip, zstd) encodes byte patterns. Lambda Tensors encodes the **generative structure** behind data.

When you have 10,000 JSON records that share the same schema and similar values, most of the information is redundant. Lambda Tensors factors that redundancy into:

1. **A generator** — a lambda expression that produces records
2. **Bindings** — the per-record values that differ
3. **A reconstruction rule** — apply bindings to generator → exact original

This is not lossy. Every byte reconstructs exactly.

## Why not just use gzip?

| | gzip | Lambda Tensors (Huffman) |
|---|---|---|
| Size (N=10,000) | 5.5% of raw | 13.5% of raw |
| Random access | ✗ (must decompress all) | **✓ (O(1) per field)** |
| Query compressed data | ✗ | ✓ (partial application) |
| Schema-aware | ✗ | ✓ |

gzip is smaller on raw size. Lambda Tensors is larger but gives you **random access** — you can read field 7 of record 3,000 without decompressing anything else. For APIs, databases, and structured archives, that's a different tool entirely.

## Encoding tiers

Lambda Tensors has five encoding tiers, each building on the last:

| Tier | Method | Size (N=10K) | Description |
|---|---|---|---|
| V1 | Varint + zigzag + type bytes | 88% | Basic binary packing |
| V2 | Small-int, float32 downshift | 64.9% | Type-aware encoding |
| V2 + Interned | Cross-member string dedup | 29% | Family string table |
| V2 + Huffman | Canonical Huffman per position | 13.5% | Family-aware probability model |
| V2 + Arithmetic | Range coding (experimental) | ~11% | Near-entropy bound |

## The lambda calculus connection

The generator isn't a passive template — it's a lambda expression:

```
λ(b₀, b₁, ..., bₙ). { field₀: b₀, field₁: b₁, ..., fieldₙ: bₙ }
```

- **Compression** = λ-abstraction (extract varying parts as parameters)
- **Decompression** = β-reduction (apply bindings to reconstruct)
- **Query** = partial application (apply some bindings, leave others)
- **Transform** = symbolic reduction (rewrite without decompressing)

## Using the library

### C

```c
#include <lambda_tensors.h>

// Compress a family of JSON records
LT_Family *fam = lt_family_create();
lt_family_add(fam, json_str_1, len_1);
lt_family_add(fam, json_str_2, len_2);
// ...
lt_family_finalize(fam);

// Read a single field without full decompression
const char *val = lt_family_read_field(fam, record_idx, field_idx);

lt_family_free(fam);
```

### Building

```bash
cd lib/liblambda-tensors
make            # builds liblambda-tensors.a + liblambda-tensors.so
make test       # runs test suite
make install    # copies to PREFIX/lib and PREFIX/include
```

### Linking

```bash
cc -o myapp myapp.c -Ipath/to/include -Lpath/to/lib -llambda-tensors -lm
```

## Best domains

Lambda Tensors works best on:
- API response caching (many similar JSON objects)
- Configuration archives (versioned configs)
- Transcript/caption/alignment bundles
- Media metadata families
- Any domain with many structurally similar records

It works poorly on:
- Random binary data
- Single isolated files
- Already-compressed data
