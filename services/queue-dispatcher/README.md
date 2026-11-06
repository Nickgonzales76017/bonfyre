# Queue dispatcher

This service drains the LocalAITranscriptionService queue while mutable queue,
lock, and cache state remains under `.bonfyre-runtime/`. The installed
LaunchAgent writes its logs under the local Bonfÿre runtime prefix so macOS does
not need to grant a background process Documents access merely to open logs.

The notebook-era NightlyBrainstorm writer is intentionally absent. Its source,
profiles, and completed queue are frozen under
`evidence/origins/notebook-era/runtime/` for replay only.

Run a non-mutating check with:

```bash
perl services/queue-dispatcher/drain_transcription_queue.pl --status
perl services/queue-dispatcher/drain_transcription_queue.pl --dry-run
```

The LaunchAgent executes an installed copy under the local Bonfÿre runtime
prefix and receives the repository root through `BONFYRE_REPO_ROOT`. This keeps
canonical source out of mutable operator state and avoids granting a background
service broad access to the Documents tree merely to load its own program.
