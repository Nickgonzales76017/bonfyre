#!/usr/bin/env python3
from pathlib import Path
import re
import sys

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

created = []
scanned = 0
for p in sorted(PROJECTS_DIR.glob('Project - *.md')):
    scanned += 1
    m = TITLE_RE.search(p.name)
    if not m:
        continue
    title = m.group(1).strip()
    slug = slugify(title)
    target = CODE_DIR / slug
    if target.exists():
        continue
    # create starter dir
    target.mkdir(parents=True, exist_ok=True)
    readme = target / 'README.md'
    main_py = target / 'main.py'
    gitkeep = target / '.gitkeep'
    readme.write_text(f"# {title}\n\nStarter code for project. Source project note: ../02-Projects/{p.name}\n\nRun: python3 main.py\n", encoding='utf-8')
    main_py.write_text(f"#!/usr/bin/env python3\n\"\"\"Starter runner for {title}\nRefer to project note: 02-Projects/{p.name}\n\"\"\"\n\nif __name__ == '__main__':\n    print('Placeholder for {title}')\n", encoding='utf-8')
    gitkeep.write_text('\n')
    created.append((title, str(target)))

print(f'Scanned {scanned} project notes')
print(f'Created {len(created)} starter code directories:')
for t,dirpath in created:
    print(f' - {t}: {dirpath}')

if not created:
    print('Nothing to create — all projects have starter code dirs.')
