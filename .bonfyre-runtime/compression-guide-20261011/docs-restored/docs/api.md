# API Reference — bonfyre-api

`bonfyre-api` is the HTTP gateway that exposes all Bonfyre binaries over REST.

## Starting the server

```bash
bonfyre-api --port 9090 --static frontend/ serve
```

| Flag | Default | Description |
|---|---|---|
| `--port PORT` | 8080 | HTTP listen port |
| `--static DIR` | (none) | Serve static files from directory |
| `--db FILE` | `~/.local/share/bonfyre/api.db` | SQLite database path |

## Endpoints

### Health

```
GET /api/health
```

```json
{"status": "ok", "version": "1.0.0", "service": "bonfyre-api"}
```

### System status

```
GET /api/status
```

```json
{
  "total_jobs": 42,
  "completed_jobs": 38,
  "total_uploads": 15,
  "available_binaries": 38,
  "binaries": ["bonfyre-cms", "bonfyre-ingest", ...]
}
```

### Upload a file

```
POST /api/upload
Content-Type: multipart/form-data

file=@interview.mp3
```

```json
{"filename": "interview.mp3", "path": "/path/to/uploads/interview.mp3", "size": 1234567}
```

### Submit a job

```
POST /api/jobs
Content-Type: application/json

{"binary": "bonfyre-ingest", "args": ["--file", "interview.mp3"]}
```

```json
{"id": 1, "binary": "bonfyre-ingest", "status": "running"}
```

### List jobs

```
GET /api/jobs
```

```json
[
  {"id": 1, "binary": "bonfyre-ingest", "status": "completed", "created_at": 1712345678},
  {"id": 2, "binary": "bonfyre-transcribe", "status": "running", "created_at": 1712345700}
]
```

### Get job detail

```
GET /api/jobs/:id
```

```json
{"id": 1, "binary": "bonfyre-ingest", "status": "completed", "output": "...", "created_at": 1712345678}
```

### Proxy to any binary

```
* /api/binaries/:name/*
```

Forwards the request to the named `bonfyre-*` binary via fork/exec. The binary receives the request body as stdin and its stdout becomes the response.

Examples:
```
GET  /api/binaries/bonfyre-meter/status
POST /api/binaries/bonfyre-auth/login  {"email": "...", "password": "..."}
GET  /api/binaries/bonfyre-finance/report
```

### Static files

```
GET /*
```

Serves files from the `--static` directory. Falls back to `index.html` for SPA routing.

## Authentication

Pass a Bearer token in the `Authorization` header:

```
Authorization: Bearer bfy_abc123...
```

Tokens are created via `bonfyre-auth signup` / `bonfyre-auth login` and validated via `bonfyre-auth verify`.

## CORS

All responses include CORS headers allowing any origin. Configure in production via a reverse proxy.
