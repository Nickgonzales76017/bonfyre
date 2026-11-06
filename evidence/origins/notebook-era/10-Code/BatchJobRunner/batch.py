"""Batch Job Runner — process multiple audio files through the transcription pipeline."""

import argparse
import json
import os
import sys
from datetime import datetime

SUPPORTED = {".mp3", ".m4a", ".ogg", ".wav", ".webm", ".flac"}


def discover_jobs(input_dir):
    """Find all audio files or intake folders in a directory."""
    jobs = []
    for name in sorted(os.listdir(input_dir)):
        path = os.path.join(input_dir, name)
        if os.path.isdir(path):
            # Intake folder — look for manifest or audio inside
            jobs.append({"type": "folder", "path": path, "name": name})
        elif os.path.splitext(name)[1].lower() in SUPPORTED:
            jobs.append({"type": "file", "path": path, "name": name})
    return jobs


def process_job(job, output_dir):
    """Process a single job. Placeholder for pipeline integration."""
    slug = os.path.splitext(job["name"])[0].lower().replace(" ", "-")
    job_output = os.path.join(output_dir, slug)
    os.makedirs(job_output, exist_ok=True)

    # TODO: Wire into LocalAITranscriptionService pipeline
    # For now, create a placeholder result
    result = {
        "job": job["name"],
        "type": job["type"],
        "status": "placeholder",
        "output_dir": job_output,
        "processed_at": datetime.now().isoformat(),
    }

    result_path = os.path.join(job_output, "result.json")
    with open(result_path, "w") as f:
        json.dump(result, f, indent=2)

    return {"name": job["name"], "status": "ok", "output": job_output}


def run_batch(input_dir, output_dir, fail_fast=False):
    """Run all jobs in a directory."""
    jobs = discover_jobs(input_dir)
    if not jobs:
        print("No audio files or intake folders found.")
        return [], []

    os.makedirs(output_dir, exist_ok=True)
    completed = []
    failures = []

    for i, job in enumerate(jobs, 1):
        print(f"[{i}/{len(jobs)}] {job['name']}...", end=" ")
        try:
            result = process_job(job, output_dir)
            completed.append(result)
            print("✓")
        except Exception as e:
            failure = {
                "name": job["name"],
                "path": job["path"],
                "error": str(e),
                "failed_at": datetime.now().isoformat(),
            }
            failures.append(failure)
            print(f"✗ {e}")
            if fail_fast:
                print("Stopping (--fail-fast).")
                break

    return completed, failures


def main():
    parser = argparse.ArgumentParser(description="Batch Job Runner")
    parser.add_argument("input", help="Directory of audio files or intake folders")
    parser.add_argument("--output", "-o", default="outputs", help="Output directory")
    parser.add_argument("--retry", help="Retry from a failure manifest JSON")
    parser.add_argument("--fail-fast", action="store_true", help="Stop on first failure")
    args = parser.parse_args()

    if args.retry:
        with open(args.retry) as f:
            retry_jobs = json.load(f)
        print(f"Retrying {len(retry_jobs)} failed jobs...")
        # TODO: implement retry from manifest
        return

    completed, failures = run_batch(args.input, args.output, args.fail_fast)

    print(f"\nDone: {len(completed)} completed, {len(failures)} failed.")

    if failures:
        retry_path = os.path.join(args.output, "retry.json")
        with open(retry_path, "w") as f:
            json.dump(failures, f, indent=2)
        print(f"Retry manifest: {retry_path}")


if __name__ == "__main__":
    main()
