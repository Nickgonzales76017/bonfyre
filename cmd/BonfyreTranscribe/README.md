# BonfyreTranscribe

Speech to timed text: managed Whisper recognition over prepped audio,
with word-bounded caption control. Metal GPU on by default, CPU fallback
one flag away.

## Build, test, install

```sh
make -C cmd/BonfyreTranscribe
make -C cmd/BonfyreTranscribe test
make -C cmd/BonfyreTranscribe install MANAGED_PREFIX=~/.bonfyre
```

## Use

```sh
bonfyre-transcribe <input-audio> <output-dir> [--model NAME] [--language CODE|auto]
bonfyre-transcribe <input-audio> <output-dir> [--gpu] [--no-gpu] [--max-len 1..500]
```

`--no-gpu` decodes on CPU where the Metal backend is absent or denied
(sandboxes, CI, VMs). The control-plane suite honors it through
`BONFYRE_TRANSCRIBE_NO_GPU=1`.

## Contract and limits

- Running this command does not run HCP diagnosis or re-decode repair;
  those live in a different program the caller names as the hcp
  recognizer.
- Shorter segmentation does not establish accurate words or boundaries.
- The command existing never authorizes a read or a mutation; scope
  still applies.

No version is pinned and none is published as a package yet. The live
contract is `cmd-transcribe` in the Architecture Atlas.
