#!/usr/bin/env python3
"""Regenerate project, systems, and pipelines index notes from vault frontmatter.

Writes:
- 02-Projects/_Index.md
- 04-Systems/01-Core Systems/_Index.md
- 04-Systems/02-Pipelines/_Index.md
- 04-Systems/02-Pipelines/Transcription Pipeline.md (hub)

Usage:
    python3 generate_indexes.py --root /path/to/vault
"""

import argparse
from pathlib import Path
import re
import yaml

SKIP_DIRS = {'.obsidian', '.bonfyre-runtime', '.git'}


def read_frontmatter(path: Path):
    text = path.read_text(encoding='utf-8')
    if not text.startswith('---'):
        return {}
    # find second ---
    parts = text.split('\n')
    second_idx = None
    for i in range(1, len(parts)):
        if parts[i].strip() == '---':
            second_idx = i
            break
    if second_idx is None:
        return {}
    fm_block = '\n'.join(parts[1:second_idx])
    try:
        return yaml.safe_load(fm_block) or {}
    except Exception:
        # naive parse: key: value lines
        fm = {}
        for line in fm_block.splitlines():
            if ':' in line:
                k,v = line.split(':',1)
                fm[k.strip()] = v.strip()
        return fm


def scan_notes(root: Path):
    projects = []
    systems = []
    pipelines = []
    ideas = []
    for p in sorted(root.rglob('*.md')):
        if any(part in SKIP_DIRS for part in p.parts):
            continue
        fm = read_frontmatter(p)
        t = (fm.get('type') or '').lower()
        title = fm.get('title') or p.stem
        rel = p.relative_to(root)
        entry = (title, str(rel))
        if t == 'project' or p.parts[-2] == '02-Projects':
            projects.append(entry)
        elif t == 'system' or '04-Systems' in str(rel):
            systems.append(entry)
        elif t == 'pipeline' or '02-Pipelines' in str(rel):
            pipelines.append(entry)
        elif t == 'idea' or '01-Ideas' in str(rel):
            ideas.append(entry)
    return projects, systems, pipelines, ideas


def write_projects_index(root: Path, projects):
    out = ['# Projects Index','\nThis index is generated from project frontmatter.']
    out.append('\n## Projects\n')
    for title, rel in projects:
        out.append(f'- [[{rel}]]')
    path = root / '02-Projects' / '_Index.md'
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('\n'.join(out) + '\n')
    print('Wrote', path)


def write_systems_index(root: Path, systems):
    out = ['# Core Systems Index','\nGenerated systems index.']
    out.append('\n## Systems\n')
    for title, rel in systems:
        out.append(f'- [[{rel}]]')
    path = root / '04-Systems' / '01-Core Systems' / '_Index.md'
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('\n'.join(out) + '\n')
    print('Wrote', path)


def write_pipelines_index(root: Path, pipelines):
    out = ['# Pipelines Index','\nGenerated pipelines index.']
    out.append('\n## Pipelines\n')
    for title, rel in pipelines:
        out.append(f'- [[{rel}]]')
    path = root / '04-Systems' / '02-Pipelines' / '_Index.md'
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('\n'.join(out) + '\n')
    print('Wrote', path)


def write_transcription_hub(root: Path, ideas):
    # build list of transcription-related ideas by keyword
    keywords = ['transcript','transcription','speaker','audio','paragraphizer','cleanup','whisper','bootstrap','intake','summary','deliverable']
    comps = [entry for entry in ideas if any(k in entry[0].lower() for k in keywords)]
    path = root / '04-Systems' / '02-Pipelines' / 'Transcription Pipeline.md'
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = []
    lines.append('---')
    lines.append('type: pipeline')
    lines.append('title: Transcription Pipeline')
    lines.append('created: 2026-04-04')
    lines.append('updated: 2026-04-04')
    lines.append('status: active')
    lines.append('stage: design')
    lines.append('system_role: Connector')
    lines.append('review_cadence: weekly')
    lines.append('tags:')
    lines.append('  - pipeline')
    lines.append('  - active')
    lines.append('aliases:')
    lines.append('  - Transcription Pipeline')
    lines.append('---\n')
    lines.append('# Transcription Pipeline — Cluster Hub\n')
    lines.append('This hub links the components that together form the transcription pipeline.')
    lines.append('\n## Components')
    for title, rel in sorted(comps):
        lines.append(f'- [[01-Ideas/{title}]]')
    path.write_text('\n'.join(lines) + '\n')
    print('Wrote', path)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--root', required=True)
    args = parser.parse_args()
    root = Path(args.root).expanduser()
    projects, systems, pipelines, ideas = scan_notes(root)
    write_projects_index(root, projects)
    write_systems_index(root, systems)
    write_pipelines_index(root, pipelines)
    write_transcription_hub(root, ideas)

if __name__ == '__main__':
    main()
