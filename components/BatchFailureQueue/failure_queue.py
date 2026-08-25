"""Batch Failure Queue — capture, retry, and report batch execution failures."""

import json
import os
from datetime import datetime


class FailureQueue:
    """Collects failures during a batch run and produces retry manifests."""

    def __init__(self):
        self.failures = []
        self.completed = []

    def record_success(self, job_name, output_path):
        self.completed.append({
            "name": job_name,
            "output": output_path,
            "completed_at": datetime.now().isoformat(),
        })

    def record_failure(self, job_name, job_path, error):
        self.failures.append({
            "name": job_name,
            "path": job_path,
            "error": str(error),
            "failed_at": datetime.now().isoformat(),
        })

    @property
    def has_failures(self):
        return len(self.failures) > 0

    def summary(self):
        total = len(self.completed) + len(self.failures)
        return {
            "total": total,
            "completed": len(self.completed),
            "failed": len(self.failures),
        }

    def write_retry_manifest(self, path):
        """Write a retry.json for failed jobs."""
        with open(path, "w") as f:
            json.dump(self.failures, f, indent=2)
        return path

    def load_retry_manifest(self, path):
        """Load a retry manifest and return job list."""
        with open(path) as f:
            return json.load(f)

    def print_summary(self):
        s = self.summary()
        print(f"\nBatch complete: {s['completed']}/{s['total']} succeeded, {s['failed']} failed.")
        if self.failures:
            print("Failed jobs:")
            for f in self.failures:
                print(f"  ✗ {f['name']}: {f['error']}")


if __name__ == "__main__":
    # Demo usage
    q = FailureQueue()
    q.record_success("test-file.wav", "/outputs/test-file/")
    q.record_failure("broken.mp3", "/inputs/broken.mp3", "ffmpeg decode error")
    q.print_summary()
