#!/usr/bin/env python3
"""Add a 'Related hubs' section linking to the Transcription Pipeline hub in transcription component notes.

Usage:
    python3 add_hub_backlinks.py --root /path/to/vault

This will insert the section after frontmatter if not present.
"""

import argparse
from pathlib import Path

HUB_LINK = '[[04-Systems/02-Pipelines/Transcription Pipeline]]'
SKIP_DIRS = {'.obsidian', '.bonfyre-runtime', '.git'}
KEYWORDS = ['transcript','transcription','speaker','audio','paragraphizer','cleanup','whisper','bootstrap','intake','summary','deliverable','diarize','diarization']


def has_related_hub(body: str) -> bool:
    return 'Transcription Pipeline' in body or HUB_LINK in body


def process(path: Path) -> bool:
    text = path.read_text(encoding='utf-8')
    if not text.startswith('---'):
        return False
    parts = text.split('\n')
    second_idx = None
    for i in range(1, len(parts)):
        if parts[i].strip() == '---':
            second_idx = i
            break
    if second_idx is None:
        return False
    fm = '\n'.join(parts[0:second_idx+1])
    body = '\n'.join(parts[second_idx+1:])
    if has_related_hub(body):
        return False
    # insert Related hubs header
    insert = '\n## Related hubs\n- ' + HUB_LINK + '\n\n'
    new_text = fm + '\n' + insert + body
    path.write_text(new_text, encoding='utf-8')
    return True


def main():
    import sys
    parser = argparse.ArgumentParser()
    parser.add_argument('--root', required=True)
    args = parser.parse_args()
    root = Path(args.root).expanduser()
    updated = 0
    scanned = 0
    for p in sorted(root.rglob('*.md')):
        if any(part in SKIP_DIRS for part in p.parts):
            continue
        rel = str(p.relative_to(root))
        scanned += 1
        # quick filter by filename or path keywords
        low = rel.lower()
        if any(k in low for k in KEYWORDS):
            try:
                if process(p):
                    print('UPDATED:', rel)
                    updated += 1
            except Exception as e:
                print('ERROR:', rel, e)
    print('\nScanned:', scanned, 'Updated:', updated)

if __name__ == '__main__':
    main()
