#!/usr/bin/env python3
from pathlib import Path
import re

VAULT = Path('/Users/nickgonzales/Documents/Bonfyre')
PROJECTS_DIR = VAULT / '02-Projects'
CODE_DIR = VAULT / '10-Code'

TITLE_RE = re.compile(r'Project -\s*(.*)\.md$')

def slugify(name: str) -> str:
    parts = re.split(r'[^A-Za-z0-9]+', name)
    parts = [p for p in parts if p]
    if not parts:
        return name.strip().replace(' ', '')
    return ''.join(p[0].upper() + p[1:] if len(p)>1 else p.upper() for p in parts)

projects = []
for p in sorted(PROJECTS_DIR.glob('Project - *.md')):
    m = TITLE_RE.search(p.name)
    if not m:
        continue
    title = m.group(1).strip()
    slug = slugify(title)
    target = CODE_DIR / slug
    exists = target.exists()
    projects.append((title, slug, str(target), exists))

missing = [t for t in projects if not t[3]]
print(f'Total projects found: {len(projects)}')
print(f'Projects missing starter code: {len(missing)}')
for title,slug,path,exists in missing:
    print(f'- {title} -> {path}')
