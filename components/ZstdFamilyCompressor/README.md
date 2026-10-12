# ZstdFamilyCompressor

Zstandard compression with dictionary training for family-aware artifact packing.

## Subcommands
- `train-dict` — train a shared dictionary from artifact samples
- `compress` — compress a single file (optionally with trained dict)
- `decompress` — decompress
- `pack-family` — tar + zstd an entire family directory

## Usage
```bash
bash bin/zstd_compress.sh train-dict ./family-samples/ --dict-out family.dict
bash bin/zstd_compress.sh compress brief.md --dict family.dict --level 19
bash bin/zstd_compress.sh pack-family ./family/ --dict family.dict --out family.tar.zst
```

## Install
```bash
brew install zstd
```
