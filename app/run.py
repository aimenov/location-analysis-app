from __future__ import annotations

import argparse

from .pipeline import parse_asof, run_pipeline
from .logging_setup import LoggingConfig, configure_logging


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True, help="Path to config JSON")
    ap.add_argument("--asof", required=True, help='As-of timestamp (e.g. "2026-04-16T12:00:00Z")')
    ap.add_argument("--log-level", default="INFO", help='Log level (DEBUG, INFO, WARNING, ERROR). Default: "INFO".')
    ap.add_argument("--log-file", default="", help="Optional log file path. Default: disabled for module run.")
    args = ap.parse_args()

    log_file = str(args.log_file).strip() or None
    configure_logging(cfg=LoggingConfig(level=str(args.log_level), log_file=log_file, console=True))

    asof = parse_asof(args.asof)
    _, locations = run_pipeline(config_path=args.config, asof=asof)

    print(f"Wrote {len(locations)} employee location rows.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

