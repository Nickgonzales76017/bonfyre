"""CI fences for the compatibility-root and ControlPlane cutover."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github/workflows/native-kernel.yml"


def test_native_ci_runs_the_full_reference_suite():
    workflow = WORKFLOW.read_text()
    assert 'working-directory: components/BonfyreControlPlane' in workflow
    assert 'run: python3 -m pytest tests/ -q' in workflow
    assert 'run: python3 -m unittest discover -s tests' not in workflow


def test_native_ci_fences_absorption_ledger_drift():
    workflow = WORKFLOW.read_text()
    assert '"tools/transitional_root_absorption.py"' in workflow
    assert '"generated/projections/estate/transitional-root-absorption.json"' in workflow
    assert 'run: python3 tools/transitional_root_absorption.py --verify' in workflow
