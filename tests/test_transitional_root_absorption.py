"""Fences for the transitional compatibility root cutover.

``tests/test_repository_shape.py`` proves the root has one owner and one name.
That is placement. These fences prove the harder property: the cutover may not
delete or rename away capability that no current owner provides.
"""

import hashlib
import json
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
TOOL = ROOT / "tools/transitional_root_absorption.py"
LEDGER = ROOT / "generated/projections/estate/transitional-root-absorption.json"
AUTH_PAY_PROOF = (
    ROOT
    / "evidence/verification/transitional-root/auth-pay-source-deletion-proof.json"
)
ORPHAN_GITLINK_PROOF = (
    ROOT
    / "evidence/verification/transitional-root/orphan-gitlink-disposition-proof.json"
)

# Trees that execute. Generated projections are excluded on purpose: a stale
# projection is regenerated, not fenced.
LIVE_SOURCE_TREES = ("cmd", "lib", "engine", "bin", "scripts", "tools", ".github")

sys.path.insert(0, str(ROOT / "tools"))

import transitional_root_absorption as absorption  # noqa: E402


@pytest.fixture(scope="module")
def ledger():
    assert LEDGER.is_file(), "run tools/transitional_root_absorption.py"
    return json.loads(LEDGER.read_text())


def test_transitional_root_is_resolved_by_measurement_not_by_name():
    """Whichever name the root currently carries, exactly one resolves."""
    resolved = absorption.transitional_root(ROOT)
    present = [
        name
        for name in absorption.TRANSITIONAL_ROOT_CANDIDATES
        if (ROOT / name).is_dir()
    ]
    assert present == [resolved], f"ambiguous transitional root: {present}"


def test_definition_measurement_is_format_independent(tmp_path):
    """Compact wrappers are definitions, while prototypes and calls are not."""
    source = tmp_path / "compact.c"
    source.write_text(
        "static void compact(char *out) { helper(out); }\n"
        "static void declaration(char *out);\n"
        "void caller(void) { compact(0); }\n"
        "if (condition) { compact(0); }\n"
    )
    assert absorption.definitions(source) == {"compact", "caller"}


def test_ledger_matches_a_fresh_measurement(ledger):
    result = subprocess.run(
        [sys.executable, str(TOOL), "--verify"],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def test_every_child_has_exactly_one_fate(ledger):
    fates = {
        "retain_until_native_parity",
        "absorb_required",
        "absorb_docs_and_fixtures",
        "superseded_by_cmd",
        "absorb_to_current_runtime",
        "absorb_to_evidence",
        "repair_required",
    }
    unclassified = [c["name"] for c in ledger["children"] if c.get("fate") not in fates]
    assert not unclassified, f"child has no cutover fate: {unclassified}"
    assert sum(ledger["fate_counts"].values()) == len(ledger["children"])


def test_no_committed_build_product_survives_under_the_transitional_root(ledger):
    assert ledger["build_products"] == [], (
        "executable build products are regenerable from cmd/ and must not be "
        "committed as source"
    )


def test_superseded_auth_and_pay_copies_have_a_recoverable_deletion_proof():
    proof = json.loads(AUTH_PAY_PROOF.read_text())
    assert proof["schema"] == "bonfyre.deletion-proof.v1"
    assert proof["recovery"] == "git-history"
    assert proof["file_count"] == len(proof["files"])
    assert proof["total_bytes"] == sum(item["bytes"] for item in proof["files"])
    digest_input = "".join(
        f"{item['sha256']} {item['path']}\n" for item in proof["files"]
    ).encode()
    assert hashlib.sha256(digest_input).hexdigest() == proof["root_digest_sha256"]
    for item in proof["files"]:
        assert not (ROOT / item["path"]).exists(), item["path"]
    for owner in proof["current_owners"]:
        owner_path = ROOT / owner["path"]
        # The owner digests are observations at deletion time, not a freeze on
        # future current-source improvements.
        assert (owner_path / "Makefile").is_file()
        assert (owner_path / "src/main.c").is_file()


def test_root_survives_while_any_child_still_owns_capability(ledger):
    """The capability-loss fence.

    A rename answers placement. Until every ``absorb_required`` residue has a
    current owner, deleting the root would end capability the repository still
    advertises, so the root must still exist.
    """
    at_risk = [c["name"] for c in ledger["children"] if c["fate"] == "absorb_required"]
    if at_risk:
        assert absorption.transitional_root(ROOT) is not None, (
            f"transitional root removed while these children still hold "
            f"unabsorbed capability: {at_risk}"
        )


def test_hcp_audio_pipeline_is_named_as_unabsorbed(ledger):
    """BonfyreTranscribe is the sharpest case and gets its own fence.

    ``docs/benchmarks.md`` advertises the HCP pipeline, but its only
    implementation is the legacy tree. Deleting that tree would leave a
    documented capability with no source.
    """
    entry = next(c for c in ledger["children"] if c["name"] == "BonfyreTranscribe")
    if entry["fate"] != "absorb_required":
        # Absorbed: the current owner must now provide it.
        assert "hcp_process" in (ROOT / "cmd/BonfyreTranscribe/src/main.c").read_text()
        return
    assert "fft_radix2" in entry["residue_symbols"]
    assert any(f.endswith("hcp_subword_freq.h") for f in entry["residue_source"])
    assert "hcp_process" in (ROOT / "docs/benchmarks.md").read_text()


def test_control_plane_is_retained_until_native_parity(ledger):
    entry = next(c for c in ledger["children"] if c["name"] == "BonfyreControlPlane")
    assert entry["fate"] == "retain_until_native_parity"
    assert entry["witnesses"], "retention must cite a live witness"
    for witness in entry["witnesses"]:
        assert (ROOT / witness).exists(), witness


def test_gitlink_children_are_reported_rather_than_silently_empty(ledger):
    """Three children are gitlinks with no ``.gitmodules`` entry.

    They present as empty directories, so a size- or content-based sweep reads
    them as worthless. The ledger has to say they are unpopulated references.
    """
    registered = absorption.registered_submodules(ROOT)
    for entry in ledger["children"]:
        if entry["fate"] != "repair_required":
            continue
        path = f"{ledger['transitional_root']}/{entry['name']}"
        assert path not in registered
        assert entry["registered_submodule"] is False


def test_orphan_gitlinks_remain_removed_under_an_identity_bound_proof():
    """The three empty-looking references were removed, not forgotten.

    The proof preserves the exact Git object identities and the historical
    superproject trees that introduced them.  It deliberately does not claim
    the unavailable subproject contents were empty or swap in a similarly
    named remote repository at a different commit.
    """
    proof = json.loads(ORPHAN_GITLINK_PROOF.read_text())
    assert proof["schema"] == "bonfyre.gitlink-disposition-proof.v1"
    assert hashlib.sha256(proof["root_digest_input"].encode()).hexdigest() == (
        proof["root_digest_sha256"]
    )

    expected = {
        "10-Code/BonfyreCMS": "00f2b86b77f90b0359c1fce3633d3c44e5aaf71c",
        "10-Code/bf-adjusters-pipeline": "b9dd2c57d5afc860f4e7c155c2fa30530c43dace",
        "10-Code/liblambda-tensors": "0a3bdce2986116cb45d5dacad6c65dcc862d9365",
    }
    observed = {
        item["gitlink"]["path_at_introduction"]: item["gitlink"]["object_id"]
        for item in proof["dispositions"]
    }
    assert observed == expected

    tracked = absorption.tracked_gitlinks(ROOT)
    for item in proof["dispositions"]:
        gitlink = item["gitlink"]
        assert item["disposition"] == "remove"
        assert gitlink["mode"] == "160000"
        assert gitlink["path_at_introduction"] not in tracked
        assert gitlink["path_after_root_relocation"] not in tracked
        assert gitlink["registered_in_gitmodules"] is False
        assert gitlink["object_present_in_superproject"] is False

        for commit in proof["first_seen_commits"]:
            tree = subprocess.run(
                ["git", "ls-tree", commit, "--", gitlink["path_at_introduction"]],
                cwd=ROOT,
                check=True,
                capture_output=True,
                text=True,
            ).stdout
            assert tree.startswith(
                f"160000 commit {gitlink['object_id']}\t"
            ), (commit, gitlink["path_at_introduction"])

        owner = item["current_owner"]
        if owner is not None:
            assert (ROOT / owner["path"]).is_dir()


def test_reproduction_instructions_resolve_after_the_rename():
    """A renamed root must not leave a recorded proof unreproducible.

    ``BonfyreFPQ`` never moved with the transitional root -- it lives under
    ``cmd/`` -- so a sweep that rewrote the old root name into these files
    would point them at a directory that has never existed.
    """
    root = absorption.transitional_root(ROOT)
    documents = (
        "cmd/BonfyreFPQ/BENCHMARKS.md",
        "cmd/BonfyreFPQ/results/2026-04-10-proof-pack/README.md",
    )
    unresolvable = []
    for relative in documents:
        for line in (ROOT / relative).read_text().splitlines():
            stripped = line.strip()
            if not stripped.startswith("cd "):
                continue
            target = stripped[3:].strip()
            if target.startswith(str(ROOT)):
                target = target[len(str(ROOT)) + 1 :]
            elif root and not target.startswith(f"{root}/"):
                continue
            if not (ROOT / target).exists():
                unresolvable.append(f"{relative} -> {target}")
    assert not unresolvable, f"recorded proof cannot be reproduced: {unresolvable}"


def test_live_source_never_points_at_a_missing_transitional_path():
    root = absorption.transitional_root(ROOT)
    if root is None:
        return
    tracked = subprocess.run(
        ["git", "ls-files", "-z", "--", *LIVE_SOURCE_TREES],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.split("\0")

    dangling = []
    for relative in tracked:
        if not relative or "legacy_from_" in relative:
            continue
        path = ROOT / relative
        if not path.is_file() or path.stat().st_size > 1_000_000:
            continue
        text = path.read_text(encoding="utf-8", errors="ignore")
        if f"{root}/" not in text:
            continue
        for token in absorption.re.findall(rf"{root}/[A-Za-z0-9_.\-]+", text):
            if not (ROOT / token).exists():
                dangling.append(f"{relative} -> {token}")
    assert not dangling, f"live source references a missing path: {dangling}"
