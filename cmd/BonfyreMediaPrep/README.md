# BonfyreMediaPrep

Audio normalization for the transcription pipeline: inspect a recording,
resample and rechannel it, trim silence, and loudness-normalize it into
the shape recognizers expect.

## Build, test, install

```sh
make -C cmd/BonfyreMediaPrep
make -C cmd/BonfyreMediaPrep test
make -C cmd/BonfyreMediaPrep install MANAGED_PREFIX=~/.bonfyre
```

Tests replay behavior vectors on small real fixtures in fresh temp dirs.

## Use

```sh
bonfyre-media-prep inspect <input>
bonfyre-media-prep normalize <input> <output> [--sample-rate N] [--channels N] [--trim-silence] [--loudnorm]
```

`bonfyre-transcribe` resolves an adjacent media-prep helper before the
checkout layout; explicit helper configuration retains precedence.

## Contract and limits

- Normalization changes representation, never the recorded words.
- The command existing never authorizes a read or a mutation; scope
  still applies.

No version is pinned and none is published as a package yet. The live
contract is `cmd-media-prep` in the Architecture Atlas.
