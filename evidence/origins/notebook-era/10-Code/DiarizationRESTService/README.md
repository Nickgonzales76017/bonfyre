# Diarization REST Service

Thin local REST wrapper over the existing speaker diarization pipeline.

## Endpoints

- `GET /health`
- `POST /jobs`
- `GET /jobs/<job_id>`

## Request

```json
{
  "audioPath": "/abs/path/to/audio.wav",
  "outputDir": "/abs/path/to/out",
  "dryRun": true
}
```

## Notes

- wraps `10-Code/SpeakerDiarization/bin/diarize_pyannote_resemblyzer.sh`
- stores lightweight job state in `state/jobs.json`
- no external web framework required

## Run

```bash
python3 server.py --host 127.0.0.1 --port 8777
```
