# Bonfyre Watch

`bonfyre watch` is the filesystem reality bridge.

It turns a real folder into a Bonfyre trigger surface:

```bash
bonfyre watch ~/Downloads --pipeline transcript-family
```

That means:

```text
drop file into watched folder
→ watcher notices new file
→ Bonfyre triggers the chosen pipeline
→ output lands in the configured output directory
→ event is recorded in watch.db
```

## Commands

```bash
bonfyre watch <dir> --pipeline transcript-family
bonfyre watch <dir> --pipeline pipeline
bonfyre watch <dir> --pipeline transcribe
```

Useful flags:

- `--out <dir>` set output root
- `--interval <seconds>` polling interval, default `2`
- `--once` scan once and exit
- `--dry-run` record planned actions without executing pipeline
- `--root <dir>` watcher state root, default `layeros/state`

## Current execution mapping

- `transcript-family`
  - runs `bonfyre-transcript-family <input> <output-dir>`
- `pipeline`
  - runs `bonfyre-pipeline run <input> --out <output-dir>`
- `transcribe`
  - runs `bonfyre-transcribe <input> <output-dir>`

## State

Watcher state is stored in:

```text
<root>/watch/watch.db
<root>/watch-output/
```

Tables:

- `watch_sessions`
- `watch_events`

## Example

One-shot planning pass:

```bash
bonfyre watch ~/Downloads --pipeline transcript-family --once --dry-run
```

Live loop:

```bash
bonfyre watch ~/Downloads --pipeline transcript-family --out layeros/state/watch-output
```
