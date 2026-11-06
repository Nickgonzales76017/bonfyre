#!/usr/bin/env python3
"""Measure what the transitional compatibility root still owns.

Generation-10 keeps one transitional root for pre-cutover material. Its
directory name has moved (``10-Code`` -> ``components``) and may move again, so
every rule here resolves the root at runtime instead of hard-coding it.

The cutover question is not "has the directory been renamed" but "does any
child still hold capability that no current owner provides". A rename answers
the first question and none of the second, so this tool measures the second
directly and refuses to call a child superseded without per-symbol and per-file
evidence.

Fates
  retain_until_native_parity  live reference the mandatory suite and CI cite
  absorb_required             symbol or source residue; deleting loses capability
  absorb_docs_and_fixtures    only documentation or sample-output residue
  superseded_by_cmd           nothing left but scratch and build products
  absorb_to_current_runtime   no cmd/ owner, but live source still calls it
  absorb_to_evidence          laboratory-era material, no owner and no caller
  repair_required             gitlink with no .gitmodules registration

Residue is graded because the severities are not comparable. A missing symbol
can end a capability; a missing ``README.md`` cannot.

``--verify`` re-derives the ledger and fails on drift, so the fence test cannot
pass against a stale projection.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# Candidate names for the transitional root, newest first.
TRANSITIONAL_ROOT_CANDIDATES = ("components", "10-Code")

LEDGER_PATH = Path("generated/projections/estate/transitional-root-absorption.json")

CONTROL_PLANE = "BonfyreControlPlane"

# Trees that already own current executable semantics. A legacy symbol that
# reappears in any of them has been absorbed, not lost.
CURRENT_OWNER_TREES = ("cmd", "lib", "engine", "programs", "services")

SOURCE_SUFFIXES = (".c", ".h")

# Residue grading. Anything that is not scratch, documentation, or a recorded
# sample run is treated as source, because an unclassified file is the case
# where guessing is most expensive.
SCRATCH_SUFFIXES = (".bak", ".new", ".orig", ".old")
SCRATCH_NAMES = (".gitignore",)
SAMPLE_OUTPUT_DIRS = ("outputs", "tmp-out-check")
DOCUMENTATION_NAMES = ("README.md",)

# Trees excluded from the live-reference scan. Historical evidence naming a
# child is what makes it history; a stale generated projection is regenerated
# rather than obeyed. Neither is a reason to keep a child in a source root.
NON_REFERENCING_PREFIXES = (
    "evidence/origins/",
    "evidence/recovery/",
    "generated/",
)

MAX_SCANNED_BYTES = 1_000_000

# A top-level C definition: return type, name, parameter list, then an opening
# brace.  The body may begin on the same line: several current command owners
# use compact wrappers such as ``static void iso_now(...) { ... }``.  Treating
# those wrappers as declarations made already-absorbed capability look like
# residue merely because of formatting.
#
# Deliberately conservative -- a missed definition shows up as unresolved
# residue, which fails closed.  A semicolon before the brace still excludes
# prototypes and calls.
DEFINITION = re.compile(
    r"^(?:static\s+)?[A-Za-z_][A-Za-z0-9_ \t\*]*?\b([A-Za-z_][A-Za-z0-9_]*)\s*\([^;]*?\)\s*\{",
    re.M,
)

MACHO_ELF_MAGIC = (
    b"\xcf\xfa\xed\xfe",
    b"\xce\xfa\xed\xfe",
    b"\xca\xfe\xba\xbe",
    b"\x7fELF",
)

GITLINK_MODE = "160000"


def transitional_root(root: Path = ROOT) -> str | None:
    for name in TRANSITIONAL_ROOT_CANDIDATES:
        if (root / name).is_dir():
            return name
    return None


def tracked_files(prefix: str = "", root: Path = ROOT) -> list[str]:
    command = ["git", "ls-files"]
    if prefix:
        command += ["--", prefix]
    out = subprocess.run(
        command,
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    return [line for line in out.split("\n") if line]


def tracked_gitlinks(root: Path = ROOT) -> set[str]:
    out = subprocess.run(
        ["git", "ls-files", "-s"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    links = set()
    for line in out.split("\n"):
        if not line.startswith(GITLINK_MODE):
            continue
        links.add(line.split("\t", 1)[1])
    return links


def registered_submodules(root: Path = ROOT) -> set[str]:
    gitmodules = root / ".gitmodules"
    if not gitmodules.is_file():
        return set()
    return set(re.findall(r"^\s*path\s*=\s*(.+)$", gitmodules.read_text(), re.M))


def is_build_product(path: Path) -> bool:
    if not path.is_file() or path.is_symlink():
        return False
    with path.open("rb") as handle:
        magic = handle.read(4)
    return magic in MACHO_ELF_MAGIC


def definitions(path: Path) -> set[str]:
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return set()
    return set(DEFINITION.findall(text))


def owner_symbol_corpus(root: Path = ROOT) -> set[str]:
    """Every symbol defined by a current owner tree.

    ``legacy_from_*`` directories are preserved history rather than a current
    owner, so a symbol that survives only there is still residue.
    """
    corpus: set[str] = set()
    for tree in CURRENT_OWNER_TREES:
        base = root / tree
        if not base.is_dir():
            continue
        for dirpath, dirnames, filenames in os.walk(base):
            dirnames[:] = [d for d in dirnames if not d.startswith("legacy_from_")]
            for name in filenames:
                if name.endswith(SOURCE_SUFFIXES):
                    corpus |= definitions(Path(dirpath) / name)
    return corpus


def symbol_is_absorbed(symbol: str, corpus: set[str]) -> bool:
    """True when a current owner still provides the symbol.

    Absorption into ``lib`` renames a bare helper to its ``bf_`` namespaced
    form (``sha256_init`` -> ``bf_sha256_init``), so the namespaced name counts
    as the same capability.
    """
    return symbol in corpus or f"bf_{symbol}" in corpus


def live_reference_index(name: str, root: Path = ROOT) -> dict[str, str]:
    """Read every tracked file that could still call into the transitional root.

    Read once and handed to ``live_reference_map``, because this read dominates
    the cost of the whole measurement.
    """
    corpus: dict[str, str] = {}
    for relative in tracked_files("", root):
        if relative.startswith(f"{name}/"):
            continue
        if relative.startswith(NON_REFERENCING_PREFIXES):
            continue
        path = root / relative
        if not path.is_file() or path.is_symlink():
            continue
        if path.stat().st_size > MAX_SCANNED_BYTES:
            continue
        try:
            corpus[relative] = path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
    return corpus


def live_reference_map(
    children: list[str], corpus: dict[str, str]
) -> dict[str, list[str]]:
    """Map each child to the live files naming it, in one pass over the corpus.

    One alternation over each file beats one scan per child; the per-child form
    made the fence too slow to run with the rest of the suite.
    """
    if not children:
        return {}
    alternation = "|".join(re.escape(name) for name in sorted(children, key=len, reverse=True))
    pattern = re.compile(rf"(?<![A-Za-z0-9_])({alternation})(?![A-Za-z0-9_])")
    found: dict[str, set[str]] = {name: set() for name in children}
    for relative, text in corpus.items():
        for match in pattern.findall(text):
            found[match].add(relative)
    return {name: sorted(paths) for name, paths in found.items()}


def classify(root: Path = ROOT) -> dict:
    name = transitional_root(root)
    if name is None:
        return {"transitional_root": None, "children": [], "build_products": []}

    files = tracked_files(name, root)
    gitlinks = tracked_gitlinks(root)
    registered = registered_submodules(root)
    corpus = owner_symbol_corpus(root)
    reference_corpus = live_reference_index(name, root)

    # A tracked path with no worktree file is a deletion awaiting commit. It
    # owns nothing, so classifying it as residue would resurrect material the
    # DeletionProof already retired.
    deleted_pending_commit = sorted(
        path
        for path in files
        if not (root / path).exists() and f"{name}/{path.split('/')[1]}" not in gitlinks
    )
    present = [path for path in files if path not in set(deleted_pending_commit)]

    # Only directories are children. The root's own tracked files describe the
    # root, and matching their bare names ("Makefile", "README.md") against the
    # repository would report every unrelated file as a caller.
    by_child: dict[str, list[str]] = {}
    root_files: list[str] = []
    for path in present:
        parts = path.split("/")
        if len(parts) < 3 and not (root / name / parts[1]).is_dir():
            root_files.append(path)
            continue
        by_child.setdefault(parts[1], []).append(path)

    references = live_reference_map(sorted(by_child), reference_corpus)

    build_products: list[str] = []
    children: list[dict] = []

    for child, paths in sorted(by_child.items()):
        child_path = f"{name}/{child}"
        entry: dict = {"name": child, "tracked_files": len(paths)}

        child_products = sorted(p for p in paths if is_build_product(root / p))
        build_products.extend(child_products)
        if child_products:
            entry["build_products"] = child_products

        if child_path in gitlinks:
            entry["fate"] = "repair_required"
            entry["reason"] = (
                "tracked as a gitlink but absent from .gitmodules, so no clone "
                "can populate it"
            )
            entry["registered_submodule"] = child_path in registered
            children.append(entry)
            continue

        if child == CONTROL_PLANE:
            entry["fate"] = "retain_until_native_parity"
            entry["reason"] = (
                "cited by the mandatory control-plane suite, the native-kernel "
                "workflow and live atlas witnesses; native parity is unproven"
            )
            entry["witnesses"] = sorted(
                w
                for w in (
                    ".github/workflows/native-kernel.yml",
                    f"{child_path}/tests",
                )
                if (root / w).exists()
            )
            children.append(entry)
            continue

        cmd_dir = root / "cmd" / child
        if cmd_dir.is_dir():
            entry.update(_classify_native_command(root, name, child, paths, corpus))
            children.append(entry)
            continue

        callers = references.get(child, [])
        entry["live_references"] = callers
        if callers:
            entry["fate"] = "absorb_to_current_runtime"
            entry["reason"] = (
                "no cmd/ owner exists, but live source still names it, so it "
                "needs a runtime owner before it can move to evidence"
            )
        else:
            entry["fate"] = "absorb_to_evidence"
            entry["reason"] = (
                "laboratory-era material with no current executable owner and "
                "no live caller"
            )
        note = root / f"evidence/origins/notebook-era/02-Projects/Project - {child}.md"
        if note.is_file():
            entry["notebook_witness"] = str(note.relative_to(root))
        children.append(entry)

    return {
        "transitional_root": name,
        "children": children,
        "build_products": sorted(build_products),
        "deleted_pending_commit": deleted_pending_commit,
        "root_files": sorted(root_files),
    }


def _classify_native_command(
    root: Path, name: str, child: str, paths: list[str], corpus: set[str]
) -> dict:
    """Compare one legacy command tree against its current ``cmd/`` owner."""
    residue_symbols: set[str] = set()
    graded: dict[str, list[str]] = {}

    for path in paths:
        relative = path.split("/", 2)[2] if path.count("/") >= 2 else ""
        if not relative:
            continue
        source = root / path
        if is_build_product(source):
            continue
        target = root / "cmd" / child / relative
        if not target.exists():
            graded.setdefault(_residue_grade(relative), []).append(path)
        if source.suffix in SOURCE_SUFFIXES:
            legacy = definitions(source)
            current = definitions(target) if target.is_file() else set()
            for symbol in legacy - current:
                if not symbol_is_absorbed(symbol, corpus):
                    residue_symbols.add(symbol)

    entry: dict = {"current_owner": f"cmd/{child}"}
    for grade, items in sorted(graded.items()):
        entry[f"residue_{grade}"] = sorted(items)
    if residue_symbols:
        entry["residue_symbols"] = sorted(residue_symbols)

    if residue_symbols or "source" in graded:
        entry["fate"] = "absorb_required"
        entry["reason"] = (
            "the current owner does not provide every symbol and source file "
            "this legacy tree still holds"
        )
    elif "documentation" in graded or "sample_output" in graded:
        entry["fate"] = "absorb_docs_and_fixtures"
        entry["reason"] = (
            "executable semantics are covered by the current owner; only "
            "documentation or recorded sample runs remain"
        )
    else:
        entry["fate"] = "superseded_by_cmd"
        entry["reason"] = (
            "every legacy symbol and durable file is provided by the current owner"
        )
    return entry


def _residue_grade(relative: str) -> str:
    name = os.path.basename(relative)
    if name.endswith(SCRATCH_SUFFIXES) or name in SCRATCH_NAMES:
        return "scratch"
    if name in DOCUMENTATION_NAMES:
        return "documentation"
    if any(part in SAMPLE_OUTPUT_DIRS for part in relative.split("/")[:-1]):
        return "sample_output"
    return "source"


def ledger(root: Path = ROOT) -> dict:
    result = classify(root)
    counts: dict[str, int] = {}
    for child in result["children"]:
        counts[child["fate"]] = counts.get(child["fate"], 0) + 1
    result["fate_counts"] = dict(sorted(counts.items()))
    result["schema"] = "bonfyre.transitional-root-absorption.v1"
    return result


def render(data: dict) -> str:
    return json.dumps(data, indent=2, sort_keys=True) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default=str(LEDGER_PATH))
    parser.add_argument(
        "--verify",
        action="store_true",
        help="fail when the committed ledger differs from a fresh measurement",
    )
    parser.add_argument(
        "--delete-build-products",
        action="store_true",
        help="delete committed build products and write a DeletionProof",
    )
    parser.add_argument("--proof-dir", default="evidence/verification/transitional-root")
    args = parser.parse_args(argv)

    data = ledger()
    rendered = render(data)
    out = ROOT / args.out

    if args.verify:
        if not out.is_file():
            print(f"missing ledger: {args.out}", file=sys.stderr)
            return 1
        if out.read_text() != rendered:
            print(f"ledger drifted from measurement: {args.out}", file=sys.stderr)
            return 1
        print(f"ledger current: {args.out}")
        return 0

    if args.delete_build_products:
        return _delete_build_products(data, Path(args.proof_dir))

    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(rendered)
    print(f"wrote {args.out}: {data['fate_counts']}")
    return 0


def _delete_build_products(data: dict, proof_dir: Path) -> int:
    products = data["build_products"]
    if not products:
        print("no committed build products under the transitional root")
        return 0

    records = []
    total = 0
    for relative in products:
        path = ROOT / relative
        payload = path.read_bytes()
        total += len(payload)
        records.append(
            {
                "path": relative,
                "bytes": len(payload),
                "sha256": hashlib.sha256(payload).hexdigest(),
            }
        )

    digest = hashlib.sha256(
        "".join(f"{r['sha256']} {r['path']}\n" for r in records).encode()
    ).hexdigest()

    proof = {
        "schema": "bonfyre.deletion-proof.v1",
        "kind": "transitional-root-build-products",
        "transitional_root": data["transitional_root"],
        "reason": (
            "committed executable build products of superseded command trees; "
            "regenerable from the current cmd/ owner and recoverable from git "
            "history"
        ),
        "file_count": len(records),
        "total_bytes": total,
        "root_digest_sha256": digest,
        "files": records,
    }

    target = ROOT / proof_dir
    target.mkdir(parents=True, exist_ok=True)
    proof_path = target / "build-products-deletion-proof.json"
    proof_path.write_text(json.dumps(proof, indent=2, sort_keys=True) + "\n")

    for relative in products:
        (ROOT / relative).unlink()

    print(
        f"deleted {len(records)} build products ({total} bytes); "
        f"proof {proof_path.relative_to(ROOT)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
