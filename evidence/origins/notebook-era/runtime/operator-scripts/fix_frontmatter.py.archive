#!/usr/bin/env python3
"""Normalize frontmatter tags and aliases across Obsidian vault markdown files.

Rules applied:
- Ensure `tags` is a YAML list containing the note `type` (e.g., project, idea, system, pipeline, concept, offer) as the first tag.
- If `status` exists in frontmatter, add it to tags (after type).
- Ensure `aliases` is a list and includes the note `title` (or filename) as an alias.
- Preserve other frontmatter keys and the note body.

Usage:
    python fix_frontmatter.py --root /path/to/vault

Default: applies changes in-place.
Add `--dry-run` to only report intended changes.
"""

import argparse
import re
from pathlib import Path
from typing import List, Dict, Tuple

SKIP_DIRS = {'.obsidian', '.bonfyre-runtime', '.git'}
TYPE_TAGS = {'idea','project','system','pipeline','concept','offer'}


def parse_frontmatter(block: str) -> Tuple[Dict[str,object], List[str]]:
    """Very small YAML-like frontmatter parser for our controlled frontmatter.
    Returns (mapping, key_order) where mapping values are str or list[str]."""
    lines = block.splitlines()
    fm = {}
    order = []
    i = 0
    n = len(lines)
    while i < n:
        line = lines[i]
        if not line.strip():
            i += 1
            continue
        m = re.match(r"^([A-Za-z0-9_\-]+):\s*(.*)$", line)
        if m:
            key = m.group(1)
            val = m.group(2).strip()
            order.append(key)
            if val.startswith('[') and val.endswith(']'):
                # inline array
                items = [x.strip().strip('\"').strip("'") for x in val.strip('[]').split(',') if x.strip()]
                fm[key] = items
                i += 1
                continue
            if val == '':
                # possibly indented list
                items = []
                j = i+1
                while j < n and re.match(r"^\s*[-]\s+(.+)$", lines[j]):
                    m2 = re.match(r"^\s*[-]\s+(.+)$", lines[j])
                    items.append(m2.group(1).strip().strip('\"').strip("'"))
                    j += 1
                if items:
                    fm[key] = items
                    i = j
                    continue
                else:
                    fm[key] = ''
                    i += 1
                    continue
            else:
                # scalar
                fm[key] = val.strip().strip('\"').strip("'")
                i += 1
                continue
        else:
            i += 1
    return fm, order


def serialize_frontmatter(fm: Dict[str,object], order: List[str]) -> str:
    out = ['---']
    for k in order:
        if k not in fm:
            continue
        v = fm[k]
        if isinstance(v, list):
            out.append(f"{k}:")
            for item in v:
                out.append(f"  - {item}")
        else:
            out.append(f"{k}: {v}")
    # include any remaining keys not in order
    for k in sorted(k for k in fm.keys() if k not in order):
        v = fm[k]
        if isinstance(v, list):
            out.append(f"{k}:")
            for item in v:
                out.append(f"  - {item}")
        else:
            out.append(f"{k}: {v}")
    out.append('---')
    return '\n'.join(out) + '\n\n'


def normalize(fm: Dict[str,object], filename: str) -> Tuple[bool, Dict[str,object]]:
    changed = False
    fm2 = dict(fm)

    note_type = (fm.get('type') or '').lower()
    title = fm.get('title') or Path(filename).stem
    status = (fm.get('status') or '').lower()

    # Normalize tags
    existing_tags = fm.get('tags') or []
    if isinstance(existing_tags, str):
        # single scalar
        existing_tags = [existing_tags]
    existing_tags = [t for t in existing_tags if t is not None]
    existing_tags = [str(t).strip() for t in existing_tags if str(t).strip()]
    lower_tags = [t.lower() for t in existing_tags]

    new_tags = []
    if note_type in TYPE_TAGS:
        if note_type not in lower_tags:
            new_tags.append(note_type)
        else:
            # put the existing variant (preserve case) first
            idx = lower_tags.index(note_type)
            new_tags.append(existing_tags[idx])
    # add status if present
    if status and status not in new_tags and status not in [t.lower() for t in new_tags]:
        # if status already in existing tags, preserve original casing
        if status in lower_tags:
            new_tags.append(existing_tags[lower_tags.index(status)])
        else:
            new_tags.append(status)

    # append other existing tags (preserve order) excluding duplicates
    for t in existing_tags:
        tl = t.lower()
        if tl in [x.lower() for x in new_tags]:
            continue
        new_tags.append(t)

    if new_tags != existing_tags:
        fm2['tags'] = new_tags
        changed = True

    # Normalize aliases
    existing_aliases = fm.get('aliases') or []
    if isinstance(existing_aliases, str):
        existing_aliases = [existing_aliases]
    existing_aliases = [a for a in existing_aliases if a is not None]
    existing_aliases = [str(a).strip() for a in existing_aliases if str(a).strip()]

    if title not in existing_aliases:
        aliases = [title] + existing_aliases
        fm2['aliases'] = aliases
        changed = True
    else:
        # ensure aliases exists as list
        if 'aliases' not in fm or not isinstance(fm.get('aliases'), list):
            fm2['aliases'] = existing_aliases
            changed = True

    return changed, fm2


def process_file(path: Path, dry_run: bool=False) -> Tuple[bool,str]:
    text = path.read_text(encoding='utf-8')
    if not text.startswith('---'):
        return False, 'no-frontmatter'
    # find second ---
    parts = text.split('\n')
    # find the line index of the second '---'
    second_idx = None
    for i in range(1, len(parts)):
        if parts[i].strip() == '---':
            second_idx = i
            break
    if second_idx is None:
        return False, 'malformed-frontmatter'
    fm_block = '\n'.join(parts[1:second_idx])
    body = '\n'.join(parts[second_idx+1:])

    fm, order = parse_frontmatter(fm_block)
    changed, fm2 = normalize(fm, path.name)
    if not changed:
        return False, 'no-change'

    # preserve original order but ensure tags and aliases in order list
    if 'tags' in fm2 and 'tags' not in order:
        order.append('tags')
    if 'aliases' in fm2 and 'aliases' not in order:
        order.append('aliases')

    new_fm_text = serialize_frontmatter(fm2, order)
    new_text = new_fm_text + body
    if not dry_run:
        path.write_text(new_text, encoding='utf-8')
    return True, 'updated'


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--root', required=True, help='Vault root path')
    parser.add_argument('--dry-run', action='store_true', help='Report changes but do not write')

    args = parser.parse_args()
    root = Path(args.root).expanduser()
    if not root.exists():
        print('Vault root not found:', root)
        return 1

    md_files = [p for p in root.rglob('*.md') if not any(part in SKIP_DIRS for part in p.parts)]
    updated = 0
    skipped = 0
    errors = 0
    for p in md_files:
        try:
            ok, reason = process_file(p, dry_run=args.dry_run)
            if ok:
                updated += 1
                print(f'UPDATED: {p.relative_to(root)}')
            else:
                skipped += 1
        except Exception as e:
            errors += 1
            print(f'ERROR: {p.relative_to(root)} -> {e}')

    print('\nSummary:')
    print(f'  files scanned: {len(md_files)}')
    print(f'  updated:       {updated}')
    print(f'  skipped:       {skipped}')
    print(f'  errors:        {errors}')
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
