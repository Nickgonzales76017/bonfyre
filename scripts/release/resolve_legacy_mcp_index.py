#!/usr/bin/env python3
"""Index-only release recovery for the exact legacy MCP AD additions.

Read-only by default. With --apply, verify private staged-blob backups (resuming
an existing backup made by the older helper) and remove only the two index
additions. Do not remove files, reset work, change history, or push.
"""
from __future__ import annotations
import argparse
import hashlib
import os
from pathlib import Path
import subprocess
import sys

TARGETS = ('mcp/README.md', 'mcp/bonfyre_mcp_gateway.py')

def sha(data):
    return hashlib.sha256(data).hexdigest()

def git(repo, *args, check=True):
    p = subprocess.run(['git', '-C', str(repo), *args],
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if check and p.returncode:
        why = p.stderr.decode('utf8', 'replace').strip()
        raise RuntimeError('git {} failed (exit {}): {}'.format(args[0], p.returncode, why[:1200]))
    return p

def staged(repo):
    parts = git(repo, 'diff', '--cached', '--name-status', '-z').stdout.split(b'\0')
    return [(parts[i].decode('ascii', 'replace'),
             parts[i+1].decode('utf8', 'surrogateescape'))
            for i in range(0, len(parts)-1, 2) if parts[i]]

def preflight(repo, expected_head, expected_branch):
    if not (repo/'.git').exists():
        raise RuntimeError('Not a standard non-bare checkout')
    head = git(repo, 'rev-parse', '--short=12', 'HEAD').stdout.decode().strip()
    branch = git(repo, 'branch', '--show-current').stdout.decode().strip()
    if expected_head and head != expected_head:
        raise RuntimeError('HEAD has changed; refusing index modification')
    if expected_branch and branch != expected_branch:
        raise RuntimeError('Branch has changed; refusing index modification')
    actual = staged(repo)
    wanted = {('A', p) for p in TARGETS}
    if set(actual) != wanted or len(actual) != len(TARGETS):
        raise RuntimeError('Index is not exactly the two legacy additions')
    for name in TARGETS:
        if git(repo, 'cat-file', '-e', 'HEAD:'+name, check=False).returncode == 0:
            raise RuntimeError('Path exists at HEAD: '+name)
    if not (repo/TARGETS[0]).is_file() or (repo/TARGETS[1]).exists():
        raise RuntimeError('Expected README present and Python gateway absent from worktree')
    blobs = {name:git(repo, 'show', ':'+name).stdout for name in TARGETS}
    if any(not v for v in blobs.values()):
        raise RuntimeError('Missing or empty original staged content')
    return head, branch, blobs, sha((repo/TARGETS[0]).read_bytes())

def private_folder(repo, backup):
    folder = backup.expanduser().absolute()
    if folder.is_symlink():
        raise RuntimeError('Private backup must not be a symlink')
    if os.path.commonpath((str(folder.resolve(strict=False)), str(repo))) == str(repo):
        raise RuntimeError('Backup must be outside Git checkout')
    return folder

def verified_backup(repo, target, blobs):
    folder = private_folder(repo, target)
    manifest = ''.join('{}  {}\n'.format(sha(data), name)
                       for name, data in blobs.items()).encode()
    if folder.exists():
        if not folder.is_dir():
            raise RuntimeError('Existing backup is not a directory')
        for name, data in blobs.items():
            p = folder/name
            if p.is_symlink() or not p.is_file() or p.read_bytes() != data:
                raise RuntimeError('Existing backup differs from staged blob: '+name)
        p = folder/'BACKUP_SHA256.txt'
        if p.is_symlink() or not p.is_file() or p.read_bytes() != manifest:
            raise RuntimeError('Existing backup checksum manifest does not match')
        return folder, 'resumed existing verified backup'
    mode = os.umask(0o077)
    try:
        folder.mkdir(parents=True, mode=0o700)
        for name, data in blobs.items():
            p = folder/name
            p.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            with p.open('xb') as stream:
                stream.write(data)
                stream.flush()
                os.fsync(stream.fileno())
            p.chmod(0o600)
            if p.read_bytes() != data:
                raise RuntimeError('Backup readback mismatch: '+name)
        p = folder/'BACKUP_SHA256.txt'
        with p.open('xb') as stream:
            stream.write(manifest)
            stream.flush()
            os.fsync(stream.fileno())
        p.chmod(0o600)
    finally:
        os.umask(mode)
    return folder, 'created and verified backup'

def execute(opts):
    repo = opts.repo.expanduser().resolve(strict=True)
    head, branch, blobs, readme_sha = preflight(
        repo, opts.expected_head, opts.expected_branch)
    if not opts.apply:
        print('READY (read-only): {} @ {}; two exact staged additions'.format(branch, head))
        if opts.backup_dir:
            p = private_folder(repo, opts.backup_dir)
            print('Private backup:', p, '(exists)' if p.exists() else '(new)')
        return
    if not opts.backup_dir:
        raise RuntimeError('Apply requires --backup-dir')
    folder, state = verified_backup(repo, opts.backup_dir, blobs)
    # Unlike the prior helper, do not call git restore. update-index owns Git's
    # index.lock and removes the two staged additions even when absent at HEAD.
    result = git(repo, 'update-index', '--force-remove', '--', *TARGETS, check=False)
    if result.returncode:
        err = result.stderr.decode('utf8','replace').strip()
        raise RuntimeError('Index update refused; backups retained: '+err[:1200])
    if (staged(repo) or (repo/TARGETS[1]).exists() or
        sha((repo/TARGETS[0]).read_bytes()) != readme_sha or
        git(repo, 'rev-parse', '--short=12', 'HEAD').stdout.decode().strip() != head):
        raise RuntimeError('Postcondition mismatch; original staged blobs remain in private backup')
    print('SUCCESS: both legacy MCP additions removed from Git index')
    print('BACKUP:', folder, '('+state+')')
    print('UNTOUCHED: current files, HEAD, other work, history and remotes')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', type=Path, default=Path.home()/'Developer/Bonfyre')
    parser.add_argument('--expected-head')
    parser.add_argument('--expected-branch', default='run8-r2c-convergence-20260823')
    parser.add_argument('--backup-dir', type=Path)
    parser.add_argument('--apply', action='store_true')
    opts = parser.parse_args()
    try:
        execute(opts)
    except (ValueError, OSError, RuntimeError) as exc:
        print('REFUSED:', str(exc), file=sys.stderr)
        sys.exit(2)
