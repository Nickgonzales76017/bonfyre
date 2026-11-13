#!/usr/bin/env python3
"""Bonfÿre core must not depend on the Aurekai operating business.

This checks *shipping generic source*, not test fixtures or brand-qualified
deployments. Product owners still need separate runtime admission and release
tests; string scanning is not an authority proof.
"""
from __future__ import annotations

import re
import sys
import tempfile
from pathlib import Path

CORE = (
    Path("engine/core/src"),
    Path("engine/core/include"),
    Path("lib/libbonfyre/src"),
    Path("lib/libbonfyre/include"),
)
SHIPPING_SUFFIXES = {".c", ".h", ".zig", ".m", ".mm", ".cc", ".cpp"}
BRAND = re.compile(r"aurekai|aurekai\.(?:work|life|live|shop)", re.IGNORECASE)
FORBIDDEN_README = (
    "built on software. driven by aurekai.",
    "bonfÿre is the software behind aurekai",
    "bonfyre is the software behind aurekai",
    "bonfÿre / aurekai — complete system architecture",
)
ROOT_REQUIRED = (
    "Aurekai is an independently owned operating institution",
    "Bonfyre core cannot acquire an Aurekai tenant, domain, business record, provider credential, or commercial authority",
    "Aurekai using a Bonfyre binary",
    "Bonfyre providing a runtime to Aurekai",
)


def problems(root: Path) -> list[str]:
    failures: list[str] = []

    for folder in CORE:
        absolute = root / folder
        if not absolute.is_dir():
            failures.append(f"missing generic core path {folder.as_posix()}")
            continue
        for source in sorted(absolute.rglob("*")):
            if source.is_symlink() or not source.is_file() or source.suffix not in SHIPPING_SUFFIXES:
                continue
            data = source.read_text(encoding="utf-8", errors="replace")
            for line_number, line in enumerate(data.splitlines(), 1):
                if BRAND.search(line):
                    relative = source.relative_to(root)
                    failures.append(f"{relative}:{line_number}: Aurekai-specific brand/host in generic shipping core; use a tenant adapter")

    readme = root / "README.md"
    if not readme.is_file():
        failures.append("missing Bonfÿre developer README")
    else:
        intro = readme.read_text(encoding="utf-8", errors="replace")
        if not re.search(r"(?im)^# Bonfÿre\s*$", intro):
            failures.append("README.md: missing standalone Bonfÿre project heading")
        for phrase in FORBIDDEN_README:
            if phrase in intro.lower():
                failures.append(f"README.md: Aurekai business branding used as Bonfÿre identity: {phrase}")

    charter = root / "docs/OWNERSHIP-BOUNDARY.md"
    if not charter.is_file():
        failures.append("missing docs/OWNERSHIP-BOUNDARY.md")
    else:
        body = charter.read_text(encoding="utf-8")
        for section in ("## The hard line", "## Architectural boundary", "## Release and account boundary", "## Enforced now vs not yet separated"):
            if section not in body:
                failures.append(f"ownership charter missing section: {section}")

    roots = root / "architecture/institutional/root.yaff"
    if not roots.is_file():
        failures.append("missing institutional root architecture")
    else:
        body = roots.read_text(encoding="utf-8")
        for phrase in ROOT_REQUIRED:
            if phrase not in body:
                failures.append(f"institutional root lacks boundary invariant: {phrase}")

    return failures


def self_test() -> None:
    # These fixtures exercise the check before CI scans an entire checkout.
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        for folder in CORE:
            (root / folder).mkdir(parents=True, exist_ok=True)
        (root / "README.md").write_text("# Bonfÿre\n\nNeutral native engine.\n", encoding="utf-8")
        (root / "docs").mkdir()
        (root / "docs/OWNERSHIP-BOUNDARY.md").write_text(
            "\n".join((
                "## The hard line", "## Architectural boundary",
                "## Release and account boundary", "## Enforced now vs not yet separated",
            )), encoding="utf-8")
        (root / "architecture/institutional").mkdir(parents=True)
        (root / "architecture/institutional/root.yaff").write_text(
            "\n".join(ROOT_REQUIRED), encoding="utf-8")
        (root / "engine/core/src/neutral.c").write_text(
            'const char *default_tenant = NULL; /* selected by the caller */\n', encoding="utf-8")
        assert problems(root) == [], f"positive fixture failed: {problems(root)}"
        brand_file = root / "engine/core/src/brand.c"
        brand_file.write_text('const char *host = "aurekai.work";\n', encoding="utf-8")
        assert any("brand.c:1" in item for item in problems(root)), "brand source was admitted"
        brand_file.unlink()
        (root / "README.md").write_text(
            "# Bonfÿre\n\nBuilt on software. Driven by Aurekai.\n", encoding="utf-8")
        assert any("branding" in item for item in problems(root)), "business slogan was admitted"
        (root / "README.md").write_text("# Bonfÿre\n", encoding="utf-8")
        with (root / "architecture/institutional/root.yaff").open("w", encoding="utf-8") as sink:
            sink.write("")
        assert any("lacks boundary invariant" in item for item in problems(root)), "erased architecture boundary was admitted"
    print("ownership boundary fixtures: pass")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--self-test":
        self_test()
    else:
        root = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else Path(__file__).resolve().parent.parent
        errors = problems(root)
        if errors:
            for error in errors:
                print(f"ERROR {error}", file=sys.stderr)
            raise SystemExit(1)
        print("Bonfÿre engine ↔ Aurekai business boundary: pass")
