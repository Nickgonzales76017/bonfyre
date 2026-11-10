#!/usr/bin/env python3
"""Bounded, privacy-first Bonfyre release convergence.

Read-only unless --apply. With --apply: restore the exact legacy MCP Git index,
move hash-verified obsolete untracked archives to private quarantine, locally
exclude runtime state, and prepare a clean, parentless *local* Git release ref
from strictly checked source cohorts. Never delete client state or push.
"""
from __future__ import annotations
import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
import subprocess
import sys
from resolve_legacy_mcp_index import preflight, verified_backup, staged, TARGETS

DEFAULT_HEAD = "173d984a8e95"
DEFAULT_BRANCH = "run8-r2c-convergence-20260823"
ARCHIVE_EXT = (".archive", ".bak", ".old", ".orig")
SUFFIXES = (".mjs", ".js", ".py", ".c", ".sh", ".zig")
COHORTS = ("native_bridge", "native_capability", "wordpress_adapter", "public_documentation")
PRIVATE_PARTS = {".git", ".bonfyre", ".bonfyre-runtime", ".claude", ".agents",
                 "packs", "evidence", "tmp", "node_modules", "venv", "target"}
PRIVATE_EXT = {".pem", ".key", ".p12", ".pfx", ".sqlite", ".sqlite3", ".db",
               ".log", ".zip", ".tar", ".parquet", ".arrow", ".onnx", ".gguf"}
STRICT_SECRET = re.compile(
    rb"\b(?:API[_-]?KEY|SECRET|TOKEN|PASSWORD|ACCESS[_-]?TOKEN|CLIENT[_-]?SECRET|PRIVATE[_-]?KEY)\b\s*[:=]\s*[\"'][^\"'\r\n]{8,}",
    re.I)
STRICT_BEARER = re.compile(rb"\bBearer\s+[A-Za-z0-9_.~-]{12,}", re.I)
PRIVATE_KEY = re.compile(rb"-----BEGIN (?:RSA |OPENSSH |EC )?PRIVATE KEY-----")

def sha(data):
    return hashlib.sha256(data).hexdigest()

def git(repo, *args, env=None, check=True):
    p = subprocess.run(["git", "-C", str(repo), *args],
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
    if check and p.returncode:
        raise RuntimeError("Git operation refused: " + args[0] + " exit " + str(p.returncode))
    return p

def result_text(repo, *args, env=None):
    return git(repo, *args, env=env).stdout.decode("utf8", "replace").strip()

def inside(path, parent):
    return os.path.commonpath((str(path), str(parent))) == str(parent)

def checked_file(repo, name, max_size=2097152):
    if not isinstance(name, str) or not name or name.startswith("/") or "\\" in name or "\0" in name:
        raise ValueError("Unsafe source path")
    parts = PurePosixPath(name).parts
    if ".." in parts or "." in parts:
        raise ValueError("Traversal in source path")
    loc = repo
    for part in parts:
        loc = loc / part
        if loc.is_symlink():
            raise ValueError("Symlink in source path")
    if not inside(loc.resolve(strict=False), repo) or not loc.is_file():
        raise ValueError("Source missing or outside repository")
    st = loc.stat()
    if not stat.S_ISREG(st.st_mode) or st.st_size > max_size:
        raise ValueError("Source is nonregular or oversized")
    data = loc.read_bytes()
    if len(data) != st.st_size:
        raise ValueError("Source changed during read")
    return loc, data

def verified_archives(repo, report):
    tracked = set(git(repo, "ls-files", "-z").stdout.decode("utf8", "surrogateescape").split("\0"))
    catalog = {r.get("path") for r in report["entries"]}
    found = []
    skipped = {}
    for row in report["entries"]:
        path = row.get("path", "")
        if not (row.get("origin") == "untracked" and
                isinstance(path, str) and path.endswith(ARCHIVE_EXT) and
                row.get("inspected") and row.get("sha256") and path not in tracked):
            continue
        parts = PurePosixPath(path).parts
        if (".." in parts or "\\" in path or not path.startswith(
                ("cmd/", "components/", "scripts/", "integrations/"))):
            skipped["private_or_invalid_owner"] = skipped.get("private_or_invalid_owner", 0) + 1
            continue
        unarchived = path.rsplit(".", 1)[0]
        stem = unarchived.rsplit(".", 1)[0]
        replacements = [unarchived] + [stem + ext for ext in SUFFIXES]
        if not any((repo / candidate).is_file() for candidate in replacements):
            skipped["unique_archive"] = skipped.get("unique_archive", 0) + 1
            continue
        try:
            src, content = checked_file(repo, path)
            if sha(content) != row["sha256"] or len(content) != row.get("bytes"):
                raise ValueError("Archive changed")
            # Local build files must not reference the archive by filename.
            for cfg in (src.parent / "Makefile", src.parent / "package.json",
                        src.parent / "build.zig", src.parent.parent / "Makefile"):
                if cfg.is_file() and not cfg.is_symlink() and cfg.stat().st_size < 250000:
                    body = cfg.read_bytes()
                    if src.name.encode() in body or path.encode() in body:
                        raise ValueError("Build still names archived file")
            found.append((path, row["sha256"], len(content)))
        except (OSError, ValueError):
            skipped["unverified_or_still_used"] = skipped.get("unverified_or_still_used", 0) + 1
    return found, skipped

def candidate_cohorts(repo, report, raw, directory):
    if not directory.is_dir():
        return [], ["no_manifest_directory"]
    lookup = {(x.get("path"), x.get("origin")): x for x in report["entries"]}
    admitted, refused = [], []
    canonical = sha(json.dumps(report, sort_keys=True, separators=(",", ":")).encode())
    for cohort in COHORTS:
        try:
            manifest = json.loads((directory / (cohort + ".json")).read_bytes())
            if (manifest.get("schema") != "bonfyre.public_release.candidate.v1" or
                (manifest.get("head"), manifest.get("branch")) !=
                (report["head"], report["branch"]) or
                manifest.get("source_report_sha256") != sha(raw) and
                manifest.get("source_report_canonical_sha256") != canonical):
                raise ValueError("Wrong source snapshot")
            entries = manifest.get("files")
            if not isinstance(entries, list) or not entries or len(entries) > 128:
                raise ValueError("Invalid cohort size")
            files, seen, total = [], set(), 0
            for row in entries:
                name = row.get("path", "")
                if name in seen or any(p in PRIVATE_PARTS for p in PurePosixPath(name).parts):
                    raise ValueError("Private, repeated or nonproduct candidate")
                seen.add(name)
                if name.startswith(".") or PurePosixPath(name).suffix.lower() in PRIVATE_EXT:
                    raise ValueError("Private material")
                source = lookup.get((name, row.get("observed_origin")))
                if (not source or source.get("class") != "CANDIDATE" or
                    source.get("findings") or not source.get("inspected")):
                    raise ValueError("No clean scanner observation")
                _, content = checked_file(repo, name)
                if ((sha(content), len(content)) != (row.get("sha256"), row.get("bytes")) or
                    (source.get("sha256"), source.get("bytes")) !=
                    (row.get("sha256"), row.get("bytes"))):
                    raise ValueError("Changed source candidate")
                if (b"\0" in content or STRICT_SECRET.search(content) or
                    STRICT_BEARER.search(content) or PRIVATE_KEY.search(content)):
                    raise ValueError("Credential-shaped or binary content")
                total += len(content)
                if total > 8388608:
                    raise ValueError("Oversized cohort")
                files.append((name, content))
            admitted.append((cohort, files))
        except (ValueError, OSError, TypeError, KeyError):
            refused.append(cohort)
    return admitted, refused

def prepare_clean_ref(repo, private, cohorts, branch):
    if not re.fullmatch(r"bonfyre-public/[a-z0-9-]{3,70}", branch):
        raise ValueError("Invalid clean ref")
    files = [(name, data) for _, group in cohorts for name, data in group]
    if not files or len(set(n for n, _ in files)) != len(files):
        raise ValueError("No safe files or overlapping cohorts")
    ref = "refs/heads/" + branch
    if git(repo, "show-ref", "--verify", "--quiet", ref, check=False).returncode == 0:
        raise ValueError("Clean release ref exists")
    env = os.environ.copy()
    env["GIT_INDEX_FILE"] = str(private / "independent-public.index")
    git(repo, "read-tree", "--empty", env=env)
    for name, data in files:
        _, now = checked_file(repo, name)
        if sha(now) != sha(data):
            raise ValueError("Public candidate changed during snapshot")
        git(repo, "add", "--", name, env=env)
    tree = result_text(repo, "write-tree", env=env)
    commit = result_text(repo, "commit-tree", tree, "-m",
                         "Bonfyre: isolated public-source review snapshot (not deployed)",
                         env=env)
    if not re.fullmatch(r"[0-9a-f]{40,64}", commit):
        raise ValueError("Git did not return a clean commit identifier")
    git(repo, "update-ref", ref, commit, "0" * len(commit))
    (private / "clean-review-ref.json").write_text(
        json.dumps({"ref": ref, "tree": tree, "commit": commit,
                    "parent_commits": 0, "files": len(files),
                    "published": False}, indent=2) + "\n")
    return ref, commit, len(files)

def apply(opts, repo, head, branch, retirement, cohorts):
    destination = opts.private_out.expanduser().absolute()
    if destination.is_symlink() or inside(destination.resolve(strict=False), repo):
        raise ValueError("Private output must be outside the public checkout")
    if destination.exists():
        raise ValueError("Private output already exists; refusing overwrite")
    if (repo / ".git/index.lock").exists():
        raise ValueError("Git index is locked; refusing before private output creation")
    if retirement and destination.parent.stat().st_dev != repo.stat().st_dev:
        raise ValueError("Private archive quarantine must share the repository filesystem")
    if opts.prepare_local_release:
        raise ValueError("In-checkout release refs are retired; use isolated_review.py")
    # Preflight all index and commit requirements before writing any private data.
    preflight(repo, head, branch)
    if opts.prepare_local_release:
        if not re.fullmatch(r"bonfyre-public/[a-z0-9-]{3,70}", opts.release_branch):
            raise ValueError("Invalid clean release ref")
        files = [name for _, group in cohorts for name, _ in group]
        if not files or len(set(files)) != len(files):
            raise ValueError("Clean release has no files or overlapping cohorts")
        if git(repo, "show-ref", "--verify", "--quiet",
               "refs/heads/" + opts.release_branch, check=False).returncode == 0:
            raise ValueError("Review branch exists; refusing overwrite")
        git(repo, "var", "GIT_AUTHOR_IDENT")
        git(repo, "var", "GIT_COMMITTER_IDENT")
    old_umask = os.umask(0o077)
    try:
        destination.mkdir(parents=True, mode=0o700)
        _, _, blobs, readme_sha = preflight(repo, head, branch)
        verified_backup(repo, destination / "legacy-mcp-staged-backup", blobs)
        result = git(repo, "update-index", "--force-remove", "--", *TARGETS, check=False)
        if result.returncode or staged(repo) or (repo / TARGETS[1]).exists() or sha(
                (repo / TARGETS[0]).read_bytes()) != readme_sha:
            raise RuntimeError("Legacy index cleanup refused; backup retained")
        # Export a source-only private snapshot. Never infer owner approval.
        exported = []
        for cohort, files in cohorts:
            for name, data in files:
                copy = destination / "public-review" / cohort / name
                copy.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
                with copy.open("xb") as f: f.write(data)
                if copy.read_bytes() != data:
                    raise RuntimeError("Review copy verification failed")
                exported.append({"cohort": cohort, "path": name, "sha256": sha(data)})
        (destination / "public-review-manifest.json").write_text(
            json.dumps({"approval": "PENDING", "files": exported}, indent=2) + "\n")
        moved = []
        if retirement:
            if destination.stat().st_dev != repo.stat().st_dev:
                raise ValueError("Private retirement must stay on the repository filesystem")
            for name, expected, size in retirement:
                source, data = checked_file(repo, name)
                if len(data) != size or sha(data) != expected:
                    continue
                target = destination / "retired-archives" / name
                target.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
                if target.exists() or target.is_symlink():
                    raise ValueError("Retirement target unexpectedly exists")
                os.replace(source, target)
                if sha(target.read_bytes()) != expected:
                    raise RuntimeError("Retired file failed readback")
                moved.append({"path": name, "sha256": expected, "bytes": size})
        (destination / "retirement-receipt.json").write_text(
            json.dumps({"reversible": True, "files": moved}, indent=2) + "\n")
        # Local-only exclusions do not hide already tracked files or modify Git history.
        exclude = repo / ".git/info/exclude"
        if not exclude.is_file() or exclude.is_symlink():
            raise ValueError("Local Git exclude unavailable")
        existing = exclude.read_text()
        ignore = ("/.bonfyre-runtime/", "/.bonfyre/", "/.run6/",
                  ".env.*", "*.sqlite3", "*.archive", "*.bak", "*.old", "*.orig")
        missing = [item for item in ignore if item not in existing.splitlines()]
        if missing:
            exclude.write_text(existing + "\n# Local private/archival Bonfyre state\n" +
                               "\n".join(missing) + "\n")
        local_ref = prepare_clean_ref(
            repo, destination, cohorts, opts.release_branch) if opts.prepare_local_release else None
        if result_text(repo, "rev-parse", "--short=12", "HEAD") != head:
            raise RuntimeError("HEAD moved during convergence")
        summary = {"head": head, "branch": branch, "legacy_staged_removed": 2,
                   "quarantined_archives": len(moved),
                   "private_review_files": len(exported),
                   "local_ignore_rules_added": len(missing),
                   "local_clean_release_ref": local_ref,
                   "remote_push": False, "production_mutation": False}
        (destination / "convergence-summary.json").write_text(
            json.dumps(summary, indent=2) + "\n")
        print("CONVERGED:", json.dumps(summary, sort_keys=True))
        print("PRIVATE RECEIPTS:", destination)
    finally:
        os.umask(old_umask)

def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--repo", type=Path, default=Path.home() / "Developer/Bonfyre")
    ap.add_argument("--report", type=Path, default=Path.home() / "Downloads/bonfyre_privacy_preflight.json")
    ap.add_argument("--manifests", type=Path, default=Path.home() / "Downloads/bonfyre_release_gate/manifests")
    ap.add_argument("--private-out", type=Path, default=Path.home() / "Downloads/bonfyre_release_converged_20261012")
    ap.add_argument("--expected-head", default=DEFAULT_HEAD)
    ap.add_argument("--expected-branch", default=DEFAULT_BRANCH)
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--prepare-local-release", action="store_true")
    ap.add_argument("--release-branch", default="bonfyre-public/source-review-20261012")
    opts = ap.parse_args()
    repo = opts.repo.expanduser().resolve(strict=True)
    head = result_text(repo, "rev-parse", "--short=12", "HEAD")
    branch = result_text(repo, "branch", "--show-current")
    if (head, branch) != (opts.expected_head, opts.expected_branch):
        raise ValueError("Source checkout moved since audited report")
    raw = opts.report.expanduser().read_bytes()
    report = json.loads(raw)
    if (report.get("schema") != "bonfyre.release.privacy_preflight.v1" or
        (report.get("head"), report.get("branch")) != (head, branch) or
        len(report.get("entries", [])) < 1000):
        raise ValueError("Incomplete or mismatched privacy report")
    retirement, reasons = verified_archives(repo, report)
    cohorts, refused = candidate_cohorts(repo, report, raw, opts.manifests.expanduser())
    print("RELEASE PLAN:", len(retirement), "verified archived replacements,",
          sum(len(rows) for _, rows in cohorts), "private-review source files,",
          len(refused), "cohorts refused")
    print("SKIPPED:", json.dumps(reasons, sort_keys=True))
    print("COHORTS REFUSED:", ",".join(refused) if refused else "none")
    if opts.apply:
        apply(opts, repo, head, branch, retirement, cohorts)
    else:
        print("READ ONLY: no Git index, files, refs or remote changed")

if __name__ == "__main__":
    try: main()
    except (OSError, ValueError, RuntimeError) as e:
        print("REFUSED:", str(e), file=sys.stderr)
        sys.exit(2)
