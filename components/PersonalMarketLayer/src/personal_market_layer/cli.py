import argparse
import json
from pathlib import Path

from .offer import generate_offer_package, sync_offer_pipeline


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate buyer-facing offer packages from reviewed proof assets.")
    parser.add_argument("--proof-dir", type=Path, help="Path to a promoted proof directory.")
    parser.add_argument(
        "--proof-index",
        type=Path,
        default=Path("../LocalAITranscriptionService/samples/proof-deliverables/index.json"),
        help="Path to a proof index. Used when --proof-dir is not provided.",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path("outputs"),
        help="Root folder for generated offer packages.",
    )
    parser.add_argument(
        "--vault-monetization-root",
        type=Path,
        default=Path("../../05-Monetization"),
        help="Vault monetization folder for generated offer notes.",
    )
    parser.add_argument(
        "--sync-all",
        action="store_true",
        help="Generate offer packages for all reviewed proofs and refresh the vault snapshot.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.sync_all:
        result = sync_offer_pipeline(
            proof_index=args.proof_index,
            output_root=args.output_root,
            vault_monetization_root=args.vault_monetization_root,
        )
    else:
        result = generate_offer_package(
            proof_index=args.proof_index,
            proof_dir=args.proof_dir,
            output_root=args.output_root,
            vault_monetization_root=args.vault_monetization_root,
        )
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
