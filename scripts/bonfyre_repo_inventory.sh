#!/bin/zsh
set -euo pipefail

ROOT="/Users/nickgonzales/Documents/Bonfyre"
OSS="/Users/nickgonzales/Projects/bonfyre-oss"
PROJECTS="/Users/nickgonzales/Projects"

echo "Bonfyre workspace inventory"
echo

echo "Vault top-level code dirs:"
find "$ROOT/10-Code" -mindepth 1 -maxdepth 1 -type d | wc -l | awk '{print "  " $1}'

echo "bonfyre-oss cmd entrypoints:"
find "$OSS/cmd" -mindepth 1 -maxdepth 1 -type d | wc -l | awk '{print "  " $1}'

echo "Pages and bonfyre repos in Projects:"
find "$PROJECTS" -mindepth 1 -maxdepth 1 -type d \( -name 'pages-*' -o -name 'bonfyre-*' \) | wc -l | awk '{print "  " $1}'

echo
echo "bonfyre-oss command list:"
find "$OSS/cmd" -mindepth 1 -maxdepth 1 -type d | sed 's#^.*/##' | sort

echo
echo "Vault 10-Code list:"
find "$ROOT/10-Code" -mindepth 1 -maxdepth 1 -type d | sed 's#^.*/##' | sort
