# BonfyreMFADict

Small native MFA dictionary builder for Bonfyre.

## Purpose
- read transcript text or simple JSON transcript payloads
- tokenize words
- emit a naive MFA lexicon
- write `status.json`

## Build
```bash
make
```

## Usage
```bash
./bonfyre-mfa-dict --transcript sample.txt --out outputs/sample/sample.dict
```

## Notes
This stays bootstrap-grade on purpose. It owns deterministic lexicon generation,
not phoneme-quality modeling.
