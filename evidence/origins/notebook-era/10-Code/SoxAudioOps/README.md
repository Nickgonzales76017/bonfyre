# SoxAudioOps

Operator wrapper around `sox` for precision audio manipulation.

## Subcommands
- `silence-detect` — detect silence regions in audio
- `fingerprint` — extract audio statistics / fingerprint
- `normalize-peak` — peak-normalize to -3dB
- `trim-silence` — trim leading/trailing silence

## Usage
```bash
bash bin/sox_ops.sh silence-detect input.wav --dry-run
bash bin/sox_ops.sh trim-silence input.wav --out trimmed.wav
```

## Install
```bash
brew install sox
```
