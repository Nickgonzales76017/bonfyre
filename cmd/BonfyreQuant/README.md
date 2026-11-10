# BonfyreQuant

Recursive lattice-flow weight quantization: measures what a quantizer did
to a real model, tensor by tensor. Read-only; it reports, it never rewrites
the model.

## Build, test, install

```sh
make -C cmd/BonfyreQuant
make -C cmd/BonfyreQuant test BONFYRE_QUANT_MODEL=/path/to/model.safetensors BONFYRE_QUANT_TENSOR=model.layers.0.input_layernorm.weight
make -C cmd/BonfyreQuant install MANAGED_PREFIX=~/.bonfyre
```

Tests measure the named model and refuse a text file, a missing tensor, or
no model at all. Without a model the target says so and passes nothing.

## Use

```sh
bonfyre-quant roundtrip MODEL.safetensors [--bits 2|3|4] [--limit N] [--tensor NAME]
bonfyre-quant encode MODEL.safetensors --out FILE.bqfp [--tensor NAME] [--bits N]
bonfyre-quant compare MODEL.safetensors --weights FILE.f32 [--tensor NAME]
bonfyre-quant benchmark MODEL.safetensors [--bits N] [--limit N]
```

BQFP is E8 lattice + μ-law warp + 16D RVQ over 3-bit blocks by default.
Every reported cosine, error, and bits-per-weight is of a tensor read from
the model named on the command line; storage counts physical headers,
codebooks, and blocks.

## Contract and limits

- A high roundtrip cosine is not unchanged generation quality.
- The bits setting is the codec, not physical storage bits per weight.
- Reads safetensors F32, F16, and BF16 only. Roundtrip processes and
  discards individual tensors; it does not produce a loadable compressed
  model.
- The command existing never authorizes an effect; scope still applies.

No version is pinned and none is published as a package yet. The live
contract is `cmd-quant` in the Architecture Atlas.
