#!/usr/bin/env python3
"""
Patch all Bonfyre binaries: replace bf_ensure_dir(p) / bf_ensure_dir(path)
with bf_ensure_parent_dir in the local ensure_dir wrapper.
Fixes the bug where SQLite DB paths were created as directories.
"""
import glob

PATTERNS = [
    ("static void ensure_dir(const char *path) { bf_ensure_dir(path); }",
     "static void ensure_dir(const char *path) { bf_ensure_parent_dir(path); }"),
    ("static void ensure_dir(const char *p) { bf_ensure_dir(p); }",
     "static void ensure_dir(const char *p) { bf_ensure_parent_dir(p); }"),
]

files = glob.glob("/Users/nickgonzales/Documents/Bonfyre/cmd/*/src/main.c")
patched = 0
for f in sorted(files):
    data = open(f).read()
    new = data
    for old, rep in PATTERNS:
        new = new.replace(old, rep)
    if new != data:
        open(f, 'w').write(new)
        print(f"patched: {f}")
        patched += 1

print(f"\ndone: {patched} patched")
