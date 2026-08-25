import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Dict, List


ROOT = Path(__file__).resolve().parents[2]
CODE_ROOT = ROOT / "10-Code"
REPORT_ROOT = Path(__file__).resolve().parent / "reports"
MONETIZATION_ROOT = ROOT / "05-Monetization"
PIPELINE_NOTE_ROOT = ROOT / "04-Systems" / "02-Pipelines"


def run_cmd(args: List[str], *, cwd: Path, extra_env: Dict[str, str] = None) -> None:
    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)
    subprocess.run(args, cwd=str(cwd), check=True, env=env)


def read_json(path: Path) -> Dict[str, object]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def write_report(name: str, payload: Dict[str, object], markdown: str) -> None:
    REPORT_ROOT.mkdir(parents=True, exist_ok=True)
    (REPORT_ROOT / f"{name}.json").write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    (REPORT_ROOT / f"{name}.md").write_text(markdown, encoding="utf-8")


def write_vault_snapshot(name: str, payload: Dict[str, object], markdown: str, *, root: Path) -> None:
    root.mkdir(parents=True, exist_ok=True)
    (root / f"_{name}.json").write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    (root / f"_{name}.md").write_text(markdown, encoding="utf-8")


def render_transcription_revenue_markdown(payload: Dict[str, object]) -> str:
    proof = payload["proof"]  # type: ignore[index]
    offers = payload["offers"]  # type: ignore[index]
    distribution = payload["distribution"]  # type: ignore[index]
    analytics = payload["analytics"]  # type: ignore[index]
    top_projects = analytics.get("top_projects", []) if isinstance(analytics, dict) else []
    top_lines = "\n".join(
        f"- {item['name']} ({item['score']})"
        for item in top_projects[:5]
        if isinstance(item, dict) and "name" in item and "score" in item
    ) or "- none"

    recent_sends = distribution.get("recent_sends", []) if isinstance(distribution, dict) else []
    send_lines = "\n".join(
        f"- {item['channel']} -> {item['target']} ({item['reply_type']})"
        for item in recent_sends[:5]
        if isinstance(item, dict)
    ) or "- none"

    offer_lines = "\n".join(
        f"- {item['offer_name']} -> {item['buyer_segment']} ({item['recommendation']})"
        for item in offers.get("offers", [])
        if isinstance(item, dict)
    ) or "- none"

    return (
        "# Transcription Revenue Pipeline\n\n"
        f"- generated_at: {payload['generated_at']}\n"
        f"- reviewed proofs: {proof.get('count', 0)}\n"
        f"- generated offers: {offers.get('generated_offer_count', 0)}\n"
        f"- total sends: {distribution.get('total_sends', 0)}\n"
        f"- pending replies: {distribution.get('pending_count', 0)}\n"
        f"- response rate: {distribution.get('response_rate', 0.0)}\n\n"
        "## Offers\n"
        f"{offer_lines}\n\n"
        "## Recent Distribution\n"
        f"{send_lines}\n\n"
        "## Momentum\n"
        f"{top_lines}\n"
    )


def render_service_delivery_markdown(payload: Dict[str, object]) -> str:
    arbitrage = payload["arbitrage"]  # type: ignore[index]
    delivery = payload["delivery"]  # type: ignore[index]
    marketplace = payload["marketplace"]  # type: ignore[index]
    analytics = payload["analytics"]  # type: ignore[index]
    top_projects = analytics.get("top_projects", []) if isinstance(analytics, dict) else []
    top_lines = "\n".join(
        f"- {item['name']} ({item['score']})"
        for item in top_projects[:5]
        if isinstance(item, dict) and "name" in item and "score" in item
    ) or "- none"

    return (
        "# Service Delivery Pipeline\n\n"
        f"- generated_at: {payload['generated_at']}\n"
        f"- services tracked: {arbitrage.get('service_count', 0)}\n"
        f"- arbitrage jobs: {arbitrage.get('job_count', 0)}\n"
        f"- handoffs: {arbitrage.get('handoff_count', 0)}\n"
        f"- pipeline jobs: {delivery.get('total_jobs', 0)}\n"
        f"- estimated margin: {delivery.get('estimated_margin', 0)}\n"
        f"- bundle count: {marketplace.get('bundle_count', 0)}\n\n"
        "## Momentum\n"
        f"{top_lines}\n"
    )


def render_browser_fulfillment_markdown(payload: Dict[str, object]) -> str:
    browser = payload["browser"]  # type: ignore[index]
    local = payload["local"]  # type: ignore[index]
    sync = payload["sync"]  # type: ignore[index]
    proofs = payload["proof"]  # type: ignore[index]

    browser_lines = "\n".join(
        f"- {key.replace('_', ' ')}: {'yes' if value else 'no'}"
        for key, value in browser.items()
        if isinstance(value, bool)
    ) or "- none"

    return (
        "# Browser Fulfillment Pipeline\n\n"
        f"- generated_at: {payload['generated_at']}\n"
        f"- staged intake packages: {local.get('intake_package_count', 0)}\n"
        f"- browser status exports: {sync.get('status_sync_count', 0)}\n"
        f"- promoted proofs: {proofs.get('proof_count', 0)}\n\n"
        "## Browser Shell Readiness\n"
        f"{browser_lines}\n\n"
        "## Recent Intake Packages\n"
        + ("\n".join(f"- {name}" for name in local.get("recent_packages", [])) or "- none")
        + "\n\n## Recent Status Sync Artifacts\n"
        + ("\n".join(f"- {name}" for name in sync.get("recent_status_syncs", [])) or "- none")
        + "\n"
    )


def refresh_transcription_revenue() -> Dict[str, object]:
    run_cmd(
        [sys.executable, "-m", "personal_market_layer.cli", "--sync-all"],
        cwd=CODE_ROOT / "PersonalMarketLayer",
        extra_env={"PYTHONPATH": "src"},
    )
    run_cmd([sys.executable, "distribute.py", "status"], cwd=CODE_ROOT / "QuietDistributionEngine")
    run_cmd(
        [sys.executable, "engine.py", "status", "--vault", str(ROOT)],
        cwd=CODE_ROOT / "PersonalDataEngine",
    )

    proof_index = read_json(CODE_ROOT / "LocalAITranscriptionService" / "samples" / "proof-deliverables" / "index.json")
    offer_snapshot = read_json(MONETIZATION_ROOT / "_offer-pipeline-snapshot.json")
    distribution_snapshot = read_json(MONETIZATION_ROOT / "_distribution-pipeline-snapshot.json")
    analytics_snapshot = read_json(CODE_ROOT / "PersonalDataEngine" / "reports" / "status.json")

    payload = {
        "pipeline": "transcription-revenue",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "proof": {
            "count": len(proof_index.get("proofs", [])) if isinstance(proof_index.get("proofs"), list) else 0,
            "index_path": str(CODE_ROOT / "LocalAITranscriptionService" / "samples" / "proof-deliverables" / "index.json"),
        },
        "offers": offer_snapshot,
        "distribution": distribution_snapshot,
        "analytics": analytics_snapshot,
    }
    markdown = render_transcription_revenue_markdown(payload)
    write_report("transcription-revenue", payload, markdown)
    write_vault_snapshot("Revenue Product Pipeline Snapshot", payload, markdown, root=MONETIZATION_ROOT)
    return payload


def refresh_service_delivery() -> Dict[str, object]:
    run_cmd([sys.executable, "arbitrage.py", "status"], cwd=CODE_ROOT / "ServiceArbitrageHub")
    run_cmd([sys.executable, "pipeline.py", "status"], cwd=CODE_ROOT / "AIOverseasLaborPipeline")
    run_cmd([sys.executable, "marketplace.py", "status"], cwd=CODE_ROOT / "RepackagedServiceMarketplace")
    run_cmd(
        [sys.executable, "engine.py", "status", "--vault", str(ROOT)],
        cwd=CODE_ROOT / "PersonalDataEngine",
    )

    arbitrage_snapshot = read_json(CODE_ROOT / "ServiceArbitrageHub" / "reports" / "status.json")
    delivery_snapshot = read_json(CODE_ROOT / "AIOverseasLaborPipeline" / "reports" / "status.json")
    marketplace_snapshot = read_json(CODE_ROOT / "RepackagedServiceMarketplace" / "reports" / "status.json")
    analytics_snapshot = read_json(CODE_ROOT / "PersonalDataEngine" / "reports" / "status.json")

    payload = {
        "pipeline": "service-delivery",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "arbitrage": arbitrage_snapshot,
        "delivery": delivery_snapshot,
        "marketplace": marketplace_snapshot,
        "analytics": analytics_snapshot,
    }
    markdown = render_service_delivery_markdown(payload)
    write_report("service-delivery", payload, markdown)
    write_vault_snapshot("Service Delivery Pipeline Snapshot", payload, markdown, root=PIPELINE_NOTE_ROOT)
    return payload


def refresh_browser_fulfillment() -> Dict[str, object]:
    intake_root = CODE_ROOT / "LocalAITranscriptionService" / "samples" / "intake-packages"
    output_root = CODE_ROOT / "LocalAITranscriptionService" / "outputs"
    proof_index = read_json(CODE_ROOT / "LocalAITranscriptionService" / "samples" / "proof-deliverables" / "index.json")

    intake_packages = sorted(
        path.name
        for path in intake_root.glob("*.intake-package.json")
        if path.is_file()
    )
    status_syncs = sorted(
        str(path.relative_to(output_root))
        for path in output_root.glob("**/browser-status.json")
        if path.is_file()
    )

    browser_checks = {
        "pwa_manifest": (CODE_ROOT / "WebWorkerSaaS" / "manifest.webmanifest").exists(),
        "service_worker": (CODE_ROOT / "WebWorkerSaaS" / "sw.js").exists(),
        "status_import": "importStatusFiles" in (CODE_ROOT / "WebWorkerSaaS" / "app.js").read_text(encoding="utf-8"),
        "package_export": "buildPackage" in (CODE_ROOT / "WebWorkerSaaS" / "app.js").read_text(encoding="utf-8"),
    }

    payload = {
        "pipeline": "browser-fulfillment",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "browser": browser_checks,
        "local": {
            "intake_package_count": len(intake_packages),
            "recent_packages": intake_packages[-5:],
        },
        "sync": {
            "status_sync_count": len(status_syncs),
            "recent_status_syncs": status_syncs[-5:],
        },
        "proof": {
            "proof_count": len(proof_index.get("proofs", [])) if isinstance(proof_index.get("proofs"), list) else 0,
        },
    }
    markdown = render_browser_fulfillment_markdown(payload)
    write_report("browser-fulfillment", payload, markdown)
    write_vault_snapshot("Browser Fulfillment Pipeline Snapshot", payload, markdown, root=PIPELINE_NOTE_ROOT)
    return payload


PIPELINES: Dict[str, Callable[[], Dict[str, object]]] = {
    "transcription-revenue": refresh_transcription_revenue,
    "service-delivery": refresh_service_delivery,
    "browser-fulfillment": refresh_browser_fulfillment,
}


def list_pipelines() -> int:
    for name in list(PIPELINES) + ["all-active"]:
        print(name)
    return 0


def run_pipeline(name: str) -> int:
    if name == "all-active":
        combined = []
        for pipeline_name in PIPELINES:
            payload = PIPELINES[pipeline_name]()
            combined.append({"pipeline": pipeline_name, "generated_at": payload["generated_at"]})
            print(json.dumps({"pipeline": pipeline_name, "generated_at": payload["generated_at"]}, indent=2))
        all_payload = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "pipelines": combined,
        }
        all_markdown = "# Active Product Pipelines\n\n" + "\n".join(
            f"- {item['pipeline']}: {item['generated_at']}" for item in combined
        ) + "\n"
        write_report("all-active", all_payload, all_markdown)
        write_vault_snapshot("Active Product Pipelines", all_payload, all_markdown, root=PIPELINE_NOTE_ROOT)
        return 0

    if name not in PIPELINES:
        raise SystemExit(f"Unknown pipeline: {name}")
    payload = PIPELINES[name]()
    print(json.dumps(payload, indent=2))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run Bonfyre product pipelines.")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("list", help="List available product pipelines.")
    run_parser = subparsers.add_parser("run", help="Run a named product pipeline.")
    run_parser.add_argument("pipeline", help="Pipeline name.")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if args.command == "list":
        return list_pipelines()
    if args.command == "run":
        return run_pipeline(args.pipeline)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
