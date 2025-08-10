from __future__ import annotations

import argparse
import shutil
from pathlib import Path

from . import bronze, silver, gold
from .io_paths import BRONZE_DIR, SILVER_DIR, GOLD_DIR, ensure_dirs


def cmd_bronze(_: argparse.Namespace) -> None:
    bronze.run()


def cmd_silver(_: argparse.Namespace) -> None:
    silver.run()


def cmd_gold(_: argparse.Namespace) -> None:
    gold.run()


def cmd_run_all(_: argparse.Namespace) -> None:
    bronze.run()
    silver.run()
    gold.run()


def cmd_clean(_: argparse.Namespace) -> None:
    for path in [BRONZE_DIR.parent, SILVER_DIR.parent, GOLD_DIR]:
        if path.exists():
            shutil.rmtree(path)
            print(f"[Clean] Removido {path}")
    ensure_dirs()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pipeline",
        description="Pipeline Medallion (Bronze/Silver/Gold)",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("bronze", help="Executa a camada Bronze").set_defaults(
        func=cmd_bronze
    )
    sub.add_parser("silver", help="Executa a camada Silver").set_defaults(
        func=cmd_silver
    )
    sub.add_parser("gold", help="Executa a camada Gold").set_defaults(func=cmd_gold)
    sub.add_parser("run-all", help="Executa Bronze -> Silver -> Gold").set_defaults(
        func=cmd_run_all
    )
    sub.add_parser("clean", help="Limpa as camadas processadas").set_defaults(
        func=cmd_clean
    )
    return parser


def main() -> None:
    ensure_dirs()
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()


