# RNNT gRPC Server

Streaming ASR service scaffold with a stable gRPC contract.

## Included

- `proto/rnnt_streaming.proto`
- `server.py` lightweight runnable scaffold
- `tests/smoke.sh`

## Notes

- current implementation is a contract-first mock server
- if `grpcio` is unavailable, `--dry-run` still validates the server config
- designed to feed interim captions to live UI and final text to batch reprocessing

## Run

```bash
python3 server.py --dry-run
python3 server.py --host 127.0.0.1 --port 50051
```
