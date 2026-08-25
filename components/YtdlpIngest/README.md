# YtdlpIngest

Media extraction and metadata mining via `yt-dlp`. Feeds audio into the transcription pipeline.

## Subcommands
- `extract-audio` — download audio as WAV
- `extract-meta` — dump metadata JSON (title, duration, tags, etc.)
- `download` — full download
- `list-formats` — list available formats

## Usage
```bash
bash bin/ytdlp_ingest.sh extract-audio "https://..." --out ./intake/
bash bin/ytdlp_ingest.sh extract-meta "https://..." --dry-run
```

## Install
```bash
brew install yt-dlp
```
