#!/usr/bin/env python3
import argparse
from pathlib import Path


ROOT = Path(__file__).resolve().parent
PROTO = ROOT / "proto" / "rnnt_streaming.proto"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=50051)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if args.dry_run:
        print(f"Would start RNNTGrpcServer on {args.host}:{args.port}")
        print(f"Proto: {PROTO}")
        print("Current mode: contract-first scaffold")
        return 0

    try:
        import grpc  # type: ignore  # noqa: F401
    except Exception:
        print("grpcio is not installed. Run with --dry-run or install grpcio + generated stubs.")
        return 2

    print(f"RNNTGrpcServer scaffold ready on {args.host}:{args.port}")
    print("Generated stubs and RNNT backend are not wired yet.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
