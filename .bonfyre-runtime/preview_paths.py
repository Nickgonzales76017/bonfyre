#!/usr/bin/env python3
import argparse
import json
from pathlib import Path
import re
from datetime import datetime

FW_RE = re.compile(r"\A---\s*\n(.*?)\n---\s*\n", re.S)

def read_frontmatter(path):
    text = path.read_text(encoding='utf-8')
    m = FW_RE.match(text)
    if not m:
        return {}
    fm_text = m.group(1)
    fm = {}
    for line in fm_text.splitlines():
        if ':' in line:
            k,v = line.split(':',1)
            fm[k.strip()] = v.strip().strip('"')
    return fm


def find_first_note(root, paths):
    root = Path(root)
    for p in paths:
        dirp = root / p
        if not dirp.exists():
            continue
        for md in sorted(dirp.rglob('*.md')):
            return md
    return None


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--root', required=True)
    p.add_argument('--pass', dest='pass_name', required=True)
    p.add_argument('--limit', type=int, default=1)
    args = p.parse_args()
    root = Path(args.root)
    prof_path = root / '10-Code' / 'NightlyBrainstorm' / 'profiles' / f"{args.pass_name}.json"
    prof = json.loads(prof_path.read_text(encoding='utf-8'))
    print(f"Profile: {prof.get('name')}\n")
    paths = prof.get('selector', {}).get('paths', [])
    note = find_first_note(root, paths)
    date = datetime.now().strftime('%Y-%m-%d')
    if note:
        fm = read_frontmatter(note)
        title = fm.get('title') or note.stem
        print(f"Sample note: {note.relative_to(root)}")
    else:
        title = 'Sample Title'
        print("No sample note found in selector paths; using placeholder title")

    out = prof.get('output', {})
    mode = out.get('mode')
    if mode == 'create':
        tpl = out.get('path_template','(none)')
        path = tpl.replace('{{title}}', title).replace('{{date}}', date)
        print('Text create path:', root / path)
        fm = out.get('frontmatter', {})
        if fm.get('audio'):
            print('Frontmatter audio link:', fm['audio'].replace('{{date}}', date))
    elif mode == 'append':
        dest = out.get('destination')
        if dest == 'source':
            print('Text will be appended to source note')
        else:
            print('Text append destination:', root / dest)
    else:
        print('No text output')

    audio = out.get('audio')
    if audio and audio.get('enabled'):
        cat = audio.get('category', 'Daily')
        fn = audio.get('filename_template') or f"{title}-{date}.wav"
        fn = fn.replace('{{title}}', title).replace('{{date}}', date)
        print('Audio path:', root / '07-Audio' / cat / fn)

if __name__ == '__main__':
    main()
