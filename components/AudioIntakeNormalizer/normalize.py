"""Audio Intake Normalizer — convert any audio to Whisper-ready 16kHz mono WAV."""

import argparse
import json
import os
import subprocess
import sys


SAMPLE_RATE = 16000
CHANNELS = 1
CODEC = "pcm_s16le"
SUPPORTED = {".mp3", ".m4a", ".ogg", ".wav", ".webm", ".flac", ".aac", ".wma", ".opus"}


def probe(path):
    """Return ffprobe JSON for the input file."""
    cmd = [
        "ffprobe", "-v", "quiet", "-print_format", "json",
        "-show_format", "-show_streams", path,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"ffprobe failed: {result.stderr.strip()}")
    return json.loads(result.stdout)


def normalize(input_path, output_path=None):
    """Normalize audio file to 16kHz mono WAV."""
    if not os.path.isfile(input_path):
        raise FileNotFoundError(input_path)

    ext = os.path.splitext(input_path)[1].lower()
    if ext not in SUPPORTED:
        raise ValueError(f"Unsupported format: {ext}")

    if output_path is None:
        base = os.path.splitext(input_path)[0]
        output_path = f"{base}.normalized.wav"

    cmd = [
        "ffmpeg", "-y", "-i", input_path,
        "-ar", str(SAMPLE_RATE),
        "-ac", str(CHANNELS),
        "-c:a", CODEC,
        output_path,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"ffmpeg failed: {result.stderr.strip()}")

    return output_path


def normalize_batch(input_dir, output_dir=None):
    """Normalize all audio files in a directory."""
    if output_dir is None:
        output_dir = os.path.join(input_dir, "normalized")
    os.makedirs(output_dir, exist_ok=True)

    results = []
    for name in sorted(os.listdir(input_dir)):
        ext = os.path.splitext(name)[1].lower()
        if ext not in SUPPORTED:
            continue
        src = os.path.join(input_dir, name)
        dst = os.path.join(output_dir, os.path.splitext(name)[0] + ".wav")
        try:
            normalize(src, dst)
            results.append({"file": name, "status": "ok", "output": dst})
            print(f"  ✓ {name}")
        except Exception as e:
            results.append({"file": name, "status": "error", "error": str(e)})
            print(f"  ✗ {name}: {e}")

    return results


def main():
    parser = argparse.ArgumentParser(description="Normalize audio for Whisper transcription")
    parser.add_argument("input", help="Audio file or directory")
    parser.add_argument("--output", "-o", help="Output path (file or directory)")
    parser.add_argument("--batch", action="store_true", help="Process all files in directory")
    args = parser.parse_args()

    if args.batch or os.path.isdir(args.input):
        results = normalize_batch(args.input, args.output)
        ok = sum(1 for r in results if r["status"] == "ok")
        print(f"\n{ok}/{len(results)} files normalized.")
    else:
        out = normalize(args.input, args.output)
        print(f"Normalized: {out}")


if __name__ == "__main__":
    main()
