# Native clip execution and private review

BonfyreClips has three real media paths and a separate time-range discovery
path. All read source material already available locally. **An export is not
permission to publish**, and no clip command posts externally or contacts a
rights holder.

## Cut a real segment — human-readable output

```sh
make -C cmd/BonfyreClips install-export
bonfyre-clips cut \
  --source /path/to/video.mp4 --start 1:04.200 --end 1:27.500 \
  --out /path/to/new-private-cut \
  --title "Council discussion" --credit "Supplied recording" \
  --captions /path/to/source-timed-transcript.srt \
  --source-ref "https://example.org/meeting"
```

Open `new-private-cut/index.html` to review the **real decoded MP4**,
caption-aligned timeline controls, source hash and output hash. The output
contains `clip.mp4`, `preview.png`, `captions.srt`, `captions.vtt`,
`timeline.edl`, `index.html` and an internal machine `receipt.json`.
Neither a user-supplied JSON plan nor synthetic transcript is required. Without
source-timed SRT, export still works, showing that captions need human work.

Cuts are at most 180 seconds. The original source must carry video and audio.
SRT cue times belong to the **original source clock**, are checked for ordered,
nonoverlapping bounded intervals and converted to cut-relative times. The
caller may choose a local font, brand, crop, and optional source-reference URL.
The URL is an unverified provenance coordinate, not permission or rights proof.
The output is private and has `noindex` on the review page.

## Harvest actual ranked clips from source-timed speech

```sh
bonfyre-clips harvest \
  --source /path/to/meeting.mp4 \
  --captions /path/to/meeting.srt \
  --out /path/to/new-private-collection \
  --title "Meeting highlights" --credit "Supplied recording" \
  --top 3 --min-duration 8 --max-duration 90
```

The C BonfyreClips native scorer reads the source-timed SRT transcript through
an ephemeral compatibility input and selects disjoint ranges. For each candidate,
the same decoded media exporter creates an actual MP4 and captioned human review
page. The collection `index.html` shows playable clips, time ranges and
navigation into the exact clip timeline. Native ranking is a **heuristic**, not
approval of speech or permission to reuse it; malformed or unscorable sources
fail without inventing clips. A three-minute upper bound still applies.
Existing `discover`, `timestamps` and `all` CLI contracts are preserved.

## Existing machine-plan export

`bonfyre-clips export PLAN.json NEW_OUTPUT_DIRECTORY` remains supported for
registered internal callers. Its plan contains source, font, numeric
source-relative start/end, title, brand, credit and bounded caption cues. The
machine JSON document is a compatibility/receipt format, never the required
customer-facing output. The *user* receives playable media, navigable HTML and
caption files instead.

## Execution and safety

The native CLI delegates actual rendering to packaged Bun code; FFmpeg,
ffprobe, ImageMagick and a local font are required. Installed scripts are
versioned together by `make install-export`. Overrides include
`BONFYRE_CLIP_EXPORT`, `BONFYRE_CLIP_CUT`, `BONFYRE_CLIP_HARVEST`,
`BONFYRE_CLIPS_BIN`, `BONFYRE_BUN_BINARY`, `BONFYRE_FFMPEG`,
`BONFYRE_FFPROBE`, `BONFYRE_MAGICK`, and `BONFYRE_TEST_FONT`.

No output overwrites an existing destination. The exporter renders to a
temporary sibling, checks input identity, FFprobe duration/audio/video geometry,
full decoded playback, and SHA-256 source/output identity, then renames only
after verifying its bytes. A failed or stale result is removed; a batch is
committed only when **every** clip decodes and rereads correctly. The receipt
retains the limitation: a successfully decoded file does not establish rights,
caption correctness, visual approval, publication authority, acceptance or
cash collection. Publication and MediaLibrary binding belong to separately
authorized owners, not to this private preparation tool.

Run `make -C cmd/BonfyreClips test`: the suite includes a real Chrome playback
witness with caption seeking, mobile viewport, no script exceptions, no network
writes, independent FFprobe/hash verification, source rejection, retained output
on replay, and failed-job rollback.

This is one executable media-production building block, **not** an entire
Podcast Plant, Universal ForeignTwin Import Studio, Dallas Civic Studio or
Creator application. External URL ingestion, recording rights, native
MediaLibrary publication, editorial approval and provider-resolved clip import
must connect through those owners.
