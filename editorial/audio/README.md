# Aurekai Journal voice direction

**Selected house narrator: Sarah** — Kokoro-82M v1.0, voice ID `af_sarah`.

Sarah was chosen after a three-voice, nine-sample audition using actual Journal prose. New source-bound recorded editions should use Sarah unless the editorial voice decision is explicitly changed. The old `af_heart` recordings are superseded; never relabel an existing audio file as Sarah without regenerating its actual sound.

The selected voice is AI-generated and must be disclosed. Each edition has its own audio artifact, source-prose SHA-256, measurable waveform and playback timing. If an essay changes, regenerate its recording rather than publishing stale speech. The previous browser voice picker and speech-synthesis fallback are retired. Articles without an actual Sarah recording do not advertise an audio edition; they remain fully readable.

[Sarah narration pilot workflow](https://github.com/Nickgonzales76017/bonfyre/actions/workflows/journal-neural-pilot.yml) replaces the first four Heart pilot recordings with actual Sarah audio. The original nine audition samples remain available in the workflow artifacts for comparison. The remaining essays require source-bound recordings; recording one selected voice does not imply those files already exist. Avoid committing a mass archive of large recordings without a media-delivery and retention decision.

Kokoro-82M is Apache-2.0; `kokoro-onnx` is MIT. No paid voice API or cloned human identity is necessary.

## Release standard

Narration is a publication asset, not a reader-controlled model demo. The public player has one house voice, real MP3 playback, accurate time seeking, measured sound amplitudes where the browser can decode the recording, and a native-audio fallback. Never show imaginary waveforms, a list of browser voices, or a listening button for prose without a recording. A recording must be bound to the exact edited Markdown body digest, have intelligible sentence and paragraph boundaries, explicit AI disclosure, and a review status. Existing pilot assets remain provisional, not human-approved or certified studio masters. Do not claim the whole 66-article collection is narrated until source-matched media exists.

## Long-form narration standard

Sarah remains the chosen house voice. Voice identity and recording quality are separate decisions. The original four pilot tracks were synthesized at a nominal speed of 0.96 with repeated 210-character speech resets. The first actual files had about 167–169 **voiced** words per minute, even though long inserted silences made the overall rate 138–140 words per minute. This is the rushed-then-stalled cadence we are replacing.

Kokoro-ONNX 0.6.1 already supports continuous overlapping-window synthesis and built-in `sentence_pause` / `clause_pause` parameters. Use those for speech timing rather than restarting the voice engine every sentence or artificially padding an existing recording. Official implementation: https://github.com/thewh1teagle/kokoro-onnx/blob/main/src/kokoro_onnx/__init__.py.

The calibrated producer in `.github/workflows/journal-neural-pilot.yml` auditions actual Aurekai paragraphs at a set of slower *synthesis* speeds, measures rate after discounting detected speech pauses, and selects a consistent pace near 142 voiced words per minute. These are automated technical checks, **not a substitute for human editorial listening approval**. The initial 0.80 / 0.85 / 0.90 auditions produced 161.1 / 176.2 / 185.4 voiced WPM respectively and were correctly rejected, so the next audition tests slower model settings. Never lower the acceptance threshold to force a failing track through; measure and audition again.

The production listener must judge sentence-level cadence, meaningful emphasis, natural consonant and vowel articulation, technical names, numbers, punctuation, repeated or skipped phrases, excessive dead air, and audible cut points. A source-bound audio fingerprint proves which manuscript was narrated; it does not prove that the resulting speech is expressive. The ACX audiobook planning baseline of approximately 155 overall words per minute is a reference, not a mandatory listening rate: https://help.acx.com/s/article/producing-and-recording-your-audiobook.

If Sarah remains synthetic or unnatural at a comfortable paced rate, compare a more expressive long-form engine on the same source material instead of repeatedly reducing Kokoro's speed. A 2026 CMU SEI overview distinguishes utility-focused Piper, expressive Chatterbox and long-form VibeVoice: https://sei.cmu.edu/blog/four-open-source-text-to-speech-systems-and-when-to-use-them/. Do not claim a voice is human-recorded or studio mastered when it is not.
