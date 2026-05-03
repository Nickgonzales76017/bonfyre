#!/usr/bin/env python3
"""Concatenate all markdown files under a root into a single combined markdown file.

Usage: export_vault_concat.py --root /path/to/vault --out /path/to/combined_vault.md

It writes a header before each file with the relative path so the combined file
can be navigated and used for summarization or bulk upload.
"""
from pathlib import Path
import argparse
import sys
import io


def build_parser():
    p = argparse.ArgumentParser()
    p.add_argument("--root", type=Path, default=Path('.'), help="Vault root to scan")
    p.add_argument("--out", type=Path, default=Path('combined_vault.md'), help="Output combined file")
    p.add_argument("--ext", type=str, default='.md', help="File extension to include")
    p.add_argument("--follow-symlinks", action='store_true')
    return p


def should_skip(path: Path, out: Path):
    # skip output file itself, .git, node_modules, and hidden .obsidian cache
    parts = [p.name for p in path.resolve().parents] + [path.name]
    if out.resolve() == path.resolve():
        return True
    if any(p in ('.git', 'node_modules', '.obsidian', '__pycache__') for p in parts):
        return True
    return False


def main():
    args = build_parser().parse_args()
    root = args.root.resolve()
    out = args.out.resolve()

    md_files = []
    for p in sorted(root.rglob(f'*{args.ext}')):
        try:
            if should_skip(p, out):
                continue
        except Exception:
            continue
        if p.is_file():
            md_files.append(p)

    if not md_files:
        print("No markdown files found.")
        return 1

    out.parent.mkdir(parents=True, exist_ok=True)
    with io.open(out, 'w', encoding='utf-8') as fh:
        fh.write(f"# Combined Vault Export\n\n")
        fh.write(f"Root: {root}\n\n")
        for p in md_files:
            rel = p.relative_to(root)
            fh.write('\n---\n\n')
            fh.write(f"<!-- FILE: {rel} -->\n\n")
            try:
                text = p.read_text(encoding='utf-8')
            except Exception:
                try:
                    text = p.read_text(encoding='latin-1')
                except Exception:
                    text = "<!-- unreadable file encoding -->"
            fh.write(text)
            fh.write('\n')

    print(f"Wrote combined file: {out} ({len(md_files)} files)")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
