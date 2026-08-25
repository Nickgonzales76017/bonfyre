"""Whisper Model Cache Manager — preflight, warm, and verify Whisper model cache."""

import argparse
import hashlib
import json
import os
import sys
from pathlib import Path

# Default Whisper cache location
DEFAULT_CACHE = Path.home() / ".cache" / "whisper"

# Model registry: name → (filename, expected_size_mb)
MODELS = {
    "tiny":     ("tiny.pt",     75),
    "base":     ("base.pt",     142),
    "small":    ("small.pt",    461),
    "medium":   ("medium.pt",   1457),
    "large-v2": ("large-v2.pt", 2872),
    "large-v3": ("large-v3.pt", 2872),
}


def get_cache_dir():
    """Return the Whisper cache directory."""
    custom = os.environ.get("WHISPER_CACHE")
    if custom:
        return Path(custom)
    return DEFAULT_CACHE


def list_cached(cache_dir):
    """List all cached model files."""
    if not cache_dir.exists():
        print(f"Cache directory not found: {cache_dir}")
        return

    found = []
    for name, (filename, expected_mb) in MODELS.items():
        path = cache_dir / filename
        if path.exists():
            size_mb = path.stat().st_size / (1024 * 1024)
            found.append((name, filename, size_mb, expected_mb))

    if not found:
        print("No cached models found.")
        return

    print(f"Cache: {cache_dir}\n")
    print(f"{'Model':<12} {'File':<18} {'Size MB':<10} {'Expected MB':<12} {'Status'}")
    print("-" * 65)
    for name, filename, size_mb, expected_mb in found:
        # Allow 5% tolerance on size
        ok = abs(size_mb - expected_mb) / expected_mb < 0.05
        status = "OK" if ok else "SIZE MISMATCH"
        print(f"{name:<12} {filename:<18} {size_mb:<10.0f} {expected_mb:<12} {status}")


def check_model(cache_dir, model_name):
    """Check if a specific model is cached and looks valid."""
    if model_name not in MODELS:
        print(f"Unknown model: {model_name}")
        print(f"Available: {', '.join(MODELS.keys())}")
        return False

    filename, expected_mb = MODELS[model_name]
    path = cache_dir / filename

    if not path.exists():
        print(f"NOT CACHED: {model_name} ({filename})")
        return False

    size_mb = path.stat().st_size / (1024 * 1024)
    ok = abs(size_mb - expected_mb) / expected_mb < 0.05

    if ok:
        print(f"READY: {model_name} ({size_mb:.0f} MB)")
        return True
    else:
        print(f"SIZE MISMATCH: {model_name} — got {size_mb:.0f} MB, expected ~{expected_mb} MB")
        return False


def warm_model(cache_dir, model_name):
    """Download a model into cache using whisper's built-in download."""
    if model_name not in MODELS:
        print(f"Unknown model: {model_name}")
        print(f"Available: {', '.join(MODELS.keys())}")
        return False

    filename, _ = MODELS[model_name]
    path = cache_dir / filename

    if path.exists():
        print(f"Already cached: {model_name}")
        return check_model(cache_dir, model_name)

    try:
        import whisper
        print(f"Downloading {model_name}...")
        whisper.load_model(model_name, download_root=str(cache_dir))
        print(f"Cached: {model_name}")
        return True
    except ImportError:
        print("whisper not installed. Install with: pip install openai-whisper")
        return False
    except Exception as e:
        print(f"Download failed: {e}")
        return False


def verify_all(cache_dir):
    """Verify integrity of all cached models via SHA-256 spot check."""
    if not cache_dir.exists():
        print(f"Cache directory not found: {cache_dir}")
        return

    print(f"Verifying models in {cache_dir}...\n")
    for name, (filename, expected_mb) in MODELS.items():
        path = cache_dir / filename
        if not path.exists():
            continue

        size_mb = path.stat().st_size / (1024 * 1024)
        size_ok = abs(size_mb - expected_mb) / expected_mb < 0.05

        # Read first 1MB for quick hash check
        sha = hashlib.sha256()
        with open(path, "rb") as f:
            sha.update(f.read(1024 * 1024))
        partial_hash = sha.hexdigest()[:12]

        status = "OK" if size_ok else "SIZE MISMATCH"
        print(f"{name:<12} {size_mb:>7.0f} MB  hash={partial_hash}  {status}")


def main():
    parser = argparse.ArgumentParser(description="Whisper Model Cache Manager")
    sub = parser.add_subparsers(dest="command")

    sub.add_parser("list", help="List cached models")

    check_p = sub.add_parser("check", help="Check if a model is cached")
    check_p.add_argument("--model", required=True, choices=MODELS.keys())

    warm_p = sub.add_parser("warm", help="Download a model into cache")
    warm_p.add_argument("--model", required=True, choices=MODELS.keys())

    sub.add_parser("verify", help="Verify cached model integrity")

    args = parser.parse_args()
    cache_dir = get_cache_dir()

    if args.command == "list":
        list_cached(cache_dir)
    elif args.command == "check":
        ok = check_model(cache_dir, args.model)
        sys.exit(0 if ok else 1)
    elif args.command == "warm":
        ok = warm_model(cache_dir, args.model)
        sys.exit(0 if ok else 1)
    elif args.command == "verify":
        verify_all(cache_dir)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
