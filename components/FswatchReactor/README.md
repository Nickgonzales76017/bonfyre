# FswatchReactor

File-watcher reactive pipeline trigger via `fswatch`. Enables zero-server event-driven pipelines.

## Subcommands
- `watch` — continuously watch a path and run command on changes
- `once` — wait for a single change, run command, exit
- `list-events` — show fswatch event flags for next change

## Usage
```bash
# Watch intake/ and auto-transcribe new WAV files:
bash bin/fswatch_reactor.sh watch ./intake/ --pattern '*.wav' --cmd 'bash ../LocalAITranscriptionService/bin/transcribe.sh'

# One-shot: wait for next change
bash bin/fswatch_reactor.sh once ./output/ --cmd 'echo New file:' --dry-run
```

## Install
```bash
brew install fswatch
```
