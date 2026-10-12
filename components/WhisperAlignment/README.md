WhisperX + MFA alignment helper

Prototype wrapper to run a fast whisper.cpp transcription, refine timestamps with WhisperX, and optionally run Montreal Forced Aligner (MFA) for phoneme-level alignment.

This repo contains an idempotent shell wrapper that:
- accepts an audio file, optional transcript
- runs whisper (via existing wrapper or prints command)
- runs WhisperX for word-level timestamps (if installed)
- runs MFA for phoneme alignment (if installed)

All steps are dry-run friendly and will print the exact commands if required binaries are missing.

Usage examples are in the `bin/` script.
