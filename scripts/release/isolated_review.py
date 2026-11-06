#!/usr/bin/env python3
"""Construct an independently hosted, unpublishable-by-default local source review.

Consumes only the digest-verified snapshot made by converge.py. Never stages
files in the development checkout, inherits development Git history, configures
a remote, or publishes anything.
"""
from __future__ import annotations
import argparse
import json
import os
from pathlib import Path, PurePosixPath
import stat
import sys
from converge import (sha, git, result_text, inside, checked_file, COHORTS,
                      PRIVATE_PARTS, PRIVATE_EXT, STRICT_SECRET, STRICT_BEARER,
                      PRIVATE_KEY)

def load_private_snapshot(repo, private, report):
    receipt = json.loads((private / "convergence-summary.json").read_text())
    listing = json.loads((private / "public-review-manifest.json").read_text())
    if (receipt.get("head"), receipt.get("branch")) != (report["head"], report["branch"]):
        raise ValueError("Private cleanup receipt differs from audited repository")
    if (not receipt.get("legacy_staged_removed") == 2 or
            listing.get("approval") != "PENDING"):
        raise ValueError("Private cleanup has not finished")
    candidates = {(x.get("path"), x.get("origin")): x for x in report["entries"]}
    files, found, count = [], set(), 0
    for item in listing.get("files", []):
        name, cohort = item.get("path", ""), item.get("cohort", "")
        parts = PurePosixPath(name).parts
        if (not name or name.startswith("/") or "\\" in name or
            ".." in parts or "." in parts or name.startswith(".") or
            any(part in PRIVATE_PARTS for part in parts) or
            PurePosixPath(name).suffix.lower() in PRIVATE_EXT or
            cohort not in COHORTS or name in found):
            raise ValueError("Unsafe, repeated or undeclared public source")
        found.add(name)
        observed = [x for (p, origin), x in candidates.items() if p == name and
                    x.get("class") == "CANDIDATE" and x.get("inspected") and
                    not x.get("findings") and x.get("sha256") == item.get("sha256")]
        if len(observed) != 1:
            raise ValueError("Public source lacks unique clean scanner evidence")
        sample = private / "public-review" / cohort / name
        if not inside(sample.resolve(strict=False), private) or sample.is_symlink() or not sample.is_file():
            raise ValueError("Private snapshot path is missing or redirected")
        data = sample.read_bytes()
        if (len(data) > 2097152 or b"\x00" in data or sha(data) != item.get("sha256") or
            STRICT_SECRET.search(data) or STRICT_BEARER.search(data) or PRIVATE_KEY.search(data)):
            raise ValueError("Private source snapshot changed or resembles a secret")
        count += len(data)
        if count > 8388608 or len(files) >= 128:
            raise ValueError("Snapshot over fixed release-size limit")
        files.append((name, data))
    if not files:
        raise ValueError("No preapproved candidate files to review")
    return files, receipt

def isolated(repo, private, report, branch):
    files, receipt = load_private_snapshot(repo, private, report)
    dest = private / "isolated-public-git"
    if dest.exists() or dest.is_symlink():
        raise ValueError("Independent review Git repository already exists")
    if not (branch.startswith("bonfyre-public/") and
            branch.removeprefix("bonfyre-public/").replace("-","").isalnum()):
        raise ValueError("Unexpected review branch name")
    if git(repo, "diff", "--cached", "--name-only").stdout.strip():
        raise ValueError("Development Git index is no longer clean")
    if result_text(repo, "rev-parse", "--short=12", "HEAD") != receipt["head"]:
        raise ValueError("Developer HEAD moved since cleanup")
    old_umask = os.umask(0o077)
    try:
        dest.mkdir(mode=0o700)
        git(dest, "init", "-q")
        ref = "refs/heads/" + branch
        git(dest, "symbolic-ref", "HEAD", ref)
        for name, data in files:
            target = dest / name
            target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            with target.open("xb") as out: out.write(data)
            _, live = checked_file(repo, name)
            if sha(live) != sha(data):
                raise ValueError("Source changed during isolated review construction")
            perms = (repo / name).stat().st_mode
            target.chmod(0o755 if perms & stat.S_IXUSR else 0o644)
        git(dest, "add", "--", *(p for p, _ in files))
        tree = result_text(dest, "write-tree")
        env = os.environ.copy()
        env.update({"GIT_AUTHOR_NAME": "Bonfyre Source Review",
                    "GIT_AUTHOR_EMAIL": "source-review@bonfyre.invalid",
                    "GIT_COMMITTER_NAME": "Bonfyre Source Review",
                    "GIT_COMMITTER_EMAIL": "source-review@bonfyre.invalid"})
        commit = result_text(dest, "commit-tree", tree, "-m",
                             "Bonfyre private source snapshot; NOT APPROVED FOR PUBLICATION", env=env)
        git(dest, "update-ref", ref, commit, "0" * len(commit))
        actual = set(result_text(dest, "ls-tree", "-r", "--name-only", commit).splitlines())
        if (actual != {n for n, _ in files} or result_text(dest, "remote") or
            result_text(dest, "rev-list", "--parents", "-n", "1", commit) != commit):
            raise ValueError("Review commit contains unexpected files, history or remotes")
        hooks = dest / ".git/hooks/pre-push"
        hooks.write_text("#!/bin/sh\n"
                         "echo 'Private release review has no public approval. Push refused.' >&2\n"
                         "exit 1\n")
        hooks.chmod(0o700)
        result = {"repository": str(dest), "branch": branch, "commit": commit,
                  "parent_commits": 0, "files": len(files), "remote": None,
                  "push_hook": "refuse", "review_status": "PENDING_OWNER_REVIEW",
                  "published": False}
        (private / "isolated-public-review.json").write_text(json.dumps(result, indent=2)+"\n")
        print("ISOLATED REVIEW READY:", json.dumps(result, sort_keys=True))
    finally:
        os.umask(old_umask)

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--private-out", type=Path, required=True)
    parser.add_argument("--review-branch", default="bonfyre-public/source-review-20261012")
    opts = parser.parse_args()
    repo = opts.repo.expanduser().resolve(strict=True)
    private = opts.private_out.expanduser().resolve(strict=True)
    if inside(private, repo) or not private.is_dir():
        raise ValueError("Review destination must be a private directory outside Git")
    report = json.loads(opts.report.expanduser().read_bytes())
    if report.get("schema") != "bonfyre.release.privacy_preflight.v1":
        raise ValueError("Incorrect privacy report")
    isolated(repo, private, report, opts.review_branch)

if __name__ == "__main__":
    try: main()
    except (OSError, ValueError, RuntimeError) as exc:
        print("REFUSED:", str(exc), file=sys.stderr)
        sys.exit(2)
