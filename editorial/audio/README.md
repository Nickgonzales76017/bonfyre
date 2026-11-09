# Aurekai Journal voice direction

**Selected house narrator: Sarah** — Kokoro-82M v1.0, voice ID `af_sarah`.

Sarah was chosen after a three-voice, nine-sample audition using actual Journal prose. New source-bound recorded editions should use Sarah unless the editorial voice decision is explicitly changed. The old `af_heart` recordings are superseded; never relabel an existing audio file as Sarah without regenerating its actual sound.

The selected voice is AI-generated and must be disclosed. Each edition has its own audio artifact, source-prose SHA-256, measurable waveform and playback timing. If an essay changes, regenerate its recording rather than publishing stale speech. The previous browser voice picker and speech-synthesis fallback are retired. Articles without an actual Sarah recording do not advertise an audio edition; they remain fully readable.

[Sarah narration pilot workflow](https://github.com/Nickgonzales76017/bonfyre/actions/workflows/journal-neural-pilot.yml) replaces the first four Heart pilot recordings with actual Sarah audio. The original nine audition samples remain available in the workflow artifacts for comparison. The remaining essays require source-bound recordings; recording one selected voice does not imply those files already exist. Avoid committing a mass archive of large recordings without a media-delivery and retention decision.

Kokoro-82M is Apache-2.0; `kokoro-onnx` is MIT. No paid voice API or cloned human identity is necessary.

## Release standard

Narration is a publication asset, not a reader-controlled model demo. The public player has one house voice, real MP3 playback, accurate time seeking, measured sound amplitudes where the browser can decode the recording, and a native-audio fallback. Never show imaginary waveforms, a list of browser voices, or a listening button for prose without a recording. A recording must be bound to the exact edited Markdown body digest, have intelligible sentence and paragraph boundaries, explicit AI disclosure, and a review status. Existing pilot assets remain provisional, not human-approved or certified studio masters. Do not claim the whole 66-article collection is narrated until source-matched media exists.
