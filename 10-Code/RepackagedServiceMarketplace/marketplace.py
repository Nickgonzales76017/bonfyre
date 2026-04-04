"""Repackaged Service Marketplace — bundle designer and pricing engine."""

import argparse
import json
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[1] / "_shared"))

from bonfyre_toolkit import data_path, write_json_report


DATA_PATH = data_path(__file__, "bundles.json")

# Service components with base costs and à la carte prices
COMPONENTS = {
    "transcription": {"cost": 1.50, "price": 12.00, "desc": "Clean formatted transcript"},
    "summary": {"cost": 0.50, "price": 5.00, "desc": "Executive summary with key points"},
    "action-items": {"cost": 0.30, "price": 3.00, "desc": "Extracted action items"},
    "speaker-labels": {"cost": 0.80, "price": 4.00, "desc": "Speaker identification and labeling"},
    "timestamps": {"cost": 0.20, "price": 2.00, "desc": "Timestamp markers throughout"},
    "human-qa": {"cost": 2.00, "price": 5.00, "desc": "Human quality review pass"},
    "rush-delivery": {"cost": 0.00, "price": 8.00, "desc": "Same-hour delivery"},
    "multi-format": {"cost": 0.30, "price": 3.00, "desc": "Delivery in TXT + DOCX + PDF"},
}


def load_bundles():
    if DATA_PATH.exists():
        return json.loads(DATA_PATH.read_text())
    return []


def save_bundles(bundles):
    DATA_PATH.write_text(json.dumps(bundles, indent=2))


def cmd_components(args):
    print(f"{'Component':<18} {'Cost':<8} {'Price':<8} {'Description'}")
    print("-" * 65)
    for name, c in COMPONENTS.items():
        print(f"{name:<18} ${c['cost']:<7.2f} ${c['price']:<7.2f} {c['desc']}")


def cmd_bundle(args):
    parts = [c.strip() for c in args.components.split(",")]
    invalid = [c for c in parts if c not in COMPONENTS]
    if invalid:
        print(f"Unknown components: {', '.join(invalid)}")
        print(f"Available: {', '.join(COMPONENTS.keys())}")
        return

    total_cost = sum(COMPONENTS[c]["cost"] for c in parts)
    a_la_carte = sum(COMPONENTS[c]["price"] for c in parts)
    sell_price = args.sell_price
    margin = sell_price - total_cost
    margin_pct = (margin / sell_price * 100) if sell_price > 0 else 0
    discount = ((a_la_carte - sell_price) / a_la_carte * 100) if a_la_carte > 0 else 0

    bundle = {
        "name": args.name,
        "components": parts,
        "sell_price": sell_price,
        "total_cost": round(total_cost, 2),
        "a_la_carte_total": round(a_la_carte, 2),
        "margin": round(margin, 2),
        "margin_pct": round(margin_pct, 1),
        "buyer_discount_pct": round(discount, 1),
        "target": args.target or "general",
    }

    bundles = load_bundles()
    # Replace if same name exists
    bundles = [b for b in bundles if b["name"] != args.name]
    bundles.append(bundle)
    save_bundles(bundles)

    print(f"Bundle created: {args.name}\n")
    print(f"  Components: {', '.join(parts)}")
    print(f"  Cost:       ${total_cost:.2f}")
    print(f"  À la carte: ${a_la_carte:.2f}")
    print(f"  Sell price: ${sell_price:.2f}")
    print(f"  Margin:     ${margin:.2f} ({margin_pct:.0f}%)")
    print(f"  Buyer saves: {discount:.0f}% vs individual pricing")
    print(f"  Target:     {bundle['target']}")


def cmd_list(args):
    bundles = load_bundles()
    if not bundles:
        print("No bundles created yet. Use 'bundle' command to create one.")
        return

    print(f"{'Bundle':<30} {'Price':<8} {'Cost':<8} {'Margin':<8} {'%':<6} {'Target'}")
    print("-" * 75)
    for b in bundles:
        print(f"{b['name']:<30} ${b['sell_price']:<7.2f} ${b['total_cost']:<7.2f} ${b['margin']:<7.2f} {b['margin_pct']:<5.0f}% {b['target']}")


def cmd_compare(args):
    bundles = load_bundles()
    bundle = next((b for b in bundles if b["name"] == args.bundle), None)
    if not bundle:
        print(f"Bundle not found: {args.bundle}")
        return

    print(f"## {bundle['name']} — Bundle vs À La Carte\n")
    print(f"{'Component':<18} {'Individual':<12} {'In Bundle'}")
    print("-" * 42)

    for comp in bundle["components"]:
        individual = COMPONENTS[comp]["price"]
        bundled = COMPONENTS[comp]["cost"]
        print(f"{comp:<18} ${individual:<11.2f} ${bundled:.2f}")

    print("-" * 42)
    print(f"{'Total':<18} ${bundle['a_la_carte_total']:<11.2f} ${bundle['sell_price']:.2f}")
    print(f"\nBuyer saves: ${bundle['a_la_carte_total'] - bundle['sell_price']:.2f} ({bundle['buyer_discount_pct']:.0f}%)")
    print(f"Your margin: ${bundle['margin']:.2f} ({bundle['margin_pct']:.0f}%)")


def cmd_export(args):
    bundles = load_bundles()
    bundle = next((b for b in bundles if b["name"] == args.bundle), None)
    if not bundle:
        print(f"Bundle not found: {args.bundle}")
        return

    print(f"# {bundle['name']}\n")
    print(f"**${bundle['sell_price']:.0f}** — one price, everything included.\n")
    print("## What You Get\n")
    for comp in bundle["components"]:
        desc = COMPONENTS[comp]["desc"]
        print(f"- {desc}")
    print(f"\n## Why This Bundle")
    savings = bundle["a_la_carte_total"] - bundle["sell_price"]
    print(f"Buying these separately would cost ${bundle['a_la_carte_total']:.0f}. ")
    print(f"This package saves you ${savings:.0f} and delivers everything in one clean handoff.")
    print(f"\n*Target: {bundle['target']}*")


def cmd_status(args):
    bundles = load_bundles()
    top_bundle = None
    if bundles:
        top_bundle = max(bundles, key=lambda item: item.get("margin", 0))
    payload = {
        "project": "RepackagedServiceMarketplace",
        "bundle_count": len(bundles),
        "top_bundle": top_bundle["name"] if top_bundle else None,
        "top_margin": top_bundle["margin"] if top_bundle else None,
        "targets": sorted({bundle.get("target", "general") for bundle in bundles}),
    }
    report = write_json_report(__file__, "status.json", payload)
    print(f"Project: {payload['project']}")
    print(f"Bundles: {payload['bundle_count']}")
    if top_bundle:
        print(f"Top bundle: {top_bundle['name']} (${top_bundle['margin']:.2f} margin)")
    print(f"Report: {report}")


def main():
    parser = argparse.ArgumentParser(description="Repackaged Service Marketplace")
    sub = parser.add_subparsers(dest="command")

    sub.add_parser("components", help="List available service components")

    bundle_p = sub.add_parser("bundle", help="Create a new bundle")
    bundle_p.add_argument("--name", required=True, help="Bundle name")
    bundle_p.add_argument("--components", required=True, help="Comma-separated component names")
    bundle_p.add_argument("--sell-price", required=True, type=float, help="Bundle sell price")
    bundle_p.add_argument("--target", default="general", help="Target buyer segment")

    sub.add_parser("list", help="List all bundles with margins")

    compare_p = sub.add_parser("compare", help="Bundle vs à la carte comparison")
    compare_p.add_argument("--bundle", required=True, help="Bundle name")

    export_p = sub.add_parser("export", help="Export bundle as offer copy")
    export_p.add_argument("--bundle", required=True, help="Bundle name")
    sub.add_parser("status", help="Project status snapshot")

    args = parser.parse_args()
    cmds = {"components": cmd_components, "bundle": cmd_bundle, "list": cmd_list, "compare": cmd_compare, "export": cmd_export, "status": cmd_status}
    if args.command in cmds:
        cmds[args.command](args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
