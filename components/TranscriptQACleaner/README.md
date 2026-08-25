# Transcript QA Cleaner

Post-ASR transcript cleanup CLI with a stable artifact contract.

## What It Does

- removes filler and loop artifacts
- strips common whisper hallucinations
- normalizes whitespace and punctuation
- emits:
  - `cleaned.txt`
  - `status.json`

## Usage

```bash
python3 bin/transcript_qa_cleaner.py --transcript raw.txt --out cleaned.txt
```

## Notes

- designed to plug into `LocalAITranscriptionService`
- current logic is heuristic, fast, and local-first
- older script path; the preferred native front door is now:
  - `10-Code/BonfyreTranscriptClean`
  - or `10-Code/BonfyreTranscriptFamily` when cleanup is part of the full transcript-family flow
