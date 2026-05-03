# TokenizerLayer

Universal text segmentation operator using sentencepiece or tiktoken.

## Usage
```bash
python3 bin/tokenizer_layer.py segment input.txt --model sp --out tokens.json
python3 bin/tokenizer_layer.py count input.txt --model tiktoken --dry-run
```

## Install (optional, falls back to whitespace tokenizer)
```bash
pip install sentencepiece
# or
pip install tiktoken
```
