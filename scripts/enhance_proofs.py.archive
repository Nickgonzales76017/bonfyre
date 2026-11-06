#!/usr/bin/env python3
"""enhance_proofs.py — Add SHA-256 hashes and enhanced metadata to all proof.json files.

Usage: python3 enhance_proofs.py <pages-dir>
  e.g. python3 enhance_proofs.py ~/Projects/pages-freelancer-evidence

Walks site/demos/<slug>/proofs/*/proof.json, adds:
  - artifacts{} with sha256 + bytes for each file in the proof dir
  - schema_version
  - hash_algorithm
  - deterministic flag
  - pipeline stages
"""
import hashlib, json, os, sys, glob

def sha256_file(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            h.update(chunk)
    return h.hexdigest()

def enhance_proof(proof_dir):
    proof_path = os.path.join(proof_dir, 'proof.json')
    if not os.path.isfile(proof_path):
        return False

    with open(proof_path, 'r') as f:
        proof = json.load(f)

    # Build artifacts manifest with hashes
    artifacts = {}
    for fname in sorted(os.listdir(proof_dir)):
        fpath = os.path.join(proof_dir, fname)
        if not os.path.isfile(fpath):
            continue
        if fname == 'proof.json':
            artifacts[fname] = {"sha256": "SELF", "bytes": os.path.getsize(fpath)}
            continue
        artifacts[fname] = {
            "sha256": sha256_file(fpath),
            "bytes": os.path.getsize(fpath)
        }

    proof['schema_version'] = '2.0.0'
    proof['hash_algorithm'] = 'sha256'
    proof['deterministic'] = True
    proof['artifacts'] = artifacts

    # Ensure pipeline stages list exists
    if 'pipeline' not in proof:
        stages = []
        if any(f.startswith('transcribe') for f in os.listdir(proof_dir)):
            stages = ['media-prep', 'transcribe', 'clean', 'brief', 'proof']
        elif os.path.exists(os.path.join(proof_dir, 'transcript.json')):
            stages = ['media-prep', 'transcribe', 'clean', 'brief', 'proof']
        proof['pipeline'] = stages

    # Extract confidence from transcribe metadata if available
    meta_path = os.path.join(proof_dir, 'transcribe-meta.json')
    if os.path.isfile(meta_path):
        with open(meta_path, 'r') as f:
            meta = json.load(f)
        if 'avg_confidence' not in proof.get('transcribe', {}):
            if 'transcribe' not in proof:
                proof['transcribe'] = {}
            for key in ['avg_confidence', 'avg_logprob', 'segments_total', 'rtf']:
                if key in meta:
                    proof['transcribe'][key] = meta[key]

    # Write back
    with open(proof_path, 'w') as f:
        json.dump(proof, f, indent=2)
    return True

def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <pages-dir>")
        sys.exit(1)

    pages_dir = os.path.expanduser(sys.argv[1])
    demos_dir = os.path.join(pages_dir, 'site', 'demos')
    if not os.path.isdir(demos_dir):
        print(f"No demos dir: {demos_dir}")
        sys.exit(1)

    count = 0
    for slug_dir in sorted(glob.glob(os.path.join(demos_dir, '*'))):
        proofs_dir = os.path.join(slug_dir, 'proofs')
        if not os.path.isdir(proofs_dir):
            continue
        for proof_dir in sorted(glob.glob(os.path.join(proofs_dir, '*'))):
            if not os.path.isdir(proof_dir):
                continue
            if enhance_proof(proof_dir):
                count += 1
                print(f"  Enhanced: {os.path.basename(proof_dir)}")

    print(f"\nEnhanced {count} proof directories in {pages_dir}")

if __name__ == '__main__':
    main()
