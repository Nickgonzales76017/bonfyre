#!/usr/bin/env python3
import argparse
import json
import subprocess
import threading
import uuid
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Dict, Any


ROOT = Path(__file__).resolve().parent
STATE_DIR = ROOT / "state"
STATE_DIR.mkdir(parents=True, exist_ok=True)
JOBS_PATH = STATE_DIR / "jobs.json"
DIAR_SCRIPT = ROOT.parent / "SpeakerDiarization" / "bin" / "diarize_pyannote_resemblyzer.sh"


def load_jobs() -> Dict[str, Any]:
    if not JOBS_PATH.exists():
        return {}
    try:
        return json.loads(JOBS_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_jobs(payload: Dict[str, Any]) -> None:
    JOBS_PATH.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def update_job(job_id: str, patch: Dict[str, Any]) -> Dict[str, Any]:
    jobs = load_jobs()
    current = jobs.get(job_id, {})
    current.update(patch)
    jobs[job_id] = current
    save_jobs(jobs)
    return current


def run_job(job_id: str, audio_path: Path, out_dir: Path, dry_run: bool) -> None:
    cmd = [str(DIAR_SCRIPT), "--audio", str(audio_path), "--out", str(out_dir)]
    if dry_run:
        cmd.append("--dry-run")
    try:
        result = subprocess.run(cmd, check=False, capture_output=True, text=True)
        status_payload = {"exitCode": result.returncode, "stdout": result.stdout[-4000:], "stderr": result.stderr[-4000:]}
        sidecar_status = None
        status_file = out_dir / "status.json"
        if status_file.exists():
            try:
                sidecar_status = json.loads(status_file.read_text(encoding="utf-8"))
            except Exception:
                sidecar_status = None
        update_job(
            job_id,
            {
                "status": "completed" if result.returncode == 0 else "failed",
                "command": cmd,
                "result": status_payload,
                "sidecarStatus": sidecar_status,
            },
        )
    except Exception as error:
        update_job(job_id, {"status": "failed", "error": str(error), "command": cmd})


class Handler(BaseHTTPRequestHandler):
    def _send(self, status: int, payload: Dict[str, Any]) -> None:
        body = json.dumps(payload, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        if self.path == "/health":
            self._send(
                HTTPStatus.OK,
                {
                    "service": "DiarizationRESTService",
                    "ok": True,
                    "diarizationScript": str(DIAR_SCRIPT),
                    "scriptPresent": DIAR_SCRIPT.exists(),
                },
            )
            return

        if self.path.startswith("/jobs/"):
            job_id = self.path.split("/jobs/", 1)[1]
            jobs = load_jobs()
            payload = jobs.get(job_id)
            if not payload:
                self._send(HTTPStatus.NOT_FOUND, {"error": "job_not_found", "jobId": job_id})
                return
            self._send(HTTPStatus.OK, payload)
            return

        self._send(HTTPStatus.NOT_FOUND, {"error": "not_found"})

    def do_POST(self) -> None:
        if self.path != "/jobs":
            self._send(HTTPStatus.NOT_FOUND, {"error": "not_found"})
            return

        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length else b"{}"
        try:
            payload = json.loads(raw.decode("utf-8"))
        except Exception:
            self._send(HTTPStatus.BAD_REQUEST, {"error": "invalid_json"})
            return

        audio_path = Path(str(payload.get("audioPath") or ""))
        output_dir = Path(str(payload.get("outputDir") or (ROOT / "jobs" / str(uuid.uuid4()))))
        dry_run = bool(payload.get("dryRun"))

        if not audio_path.exists():
            self._send(HTTPStatus.BAD_REQUEST, {"error": "missing_audio", "audioPath": str(audio_path)})
            return

        job_id = uuid.uuid4().hex[:12]
        output_dir.mkdir(parents=True, exist_ok=True)
        job = {
            "jobId": job_id,
            "status": "queued",
            "audioPath": str(audio_path),
            "outputDir": str(output_dir),
            "dryRun": dry_run,
        }
        update_job(job_id, job)
        thread = threading.Thread(target=run_job, args=(job_id, audio_path, output_dir, dry_run), daemon=True)
        thread.start()
        self._send(HTTPStatus.ACCEPTED, job)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8777)
    args = parser.parse_args()

    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"DiarizationRESTService listening on http://{args.host}:{args.port}")
    server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
