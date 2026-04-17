from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

from app.pipeline import parse_asof, run_pipeline
from app.logging_setup import LoggingConfig, configure_logging


def _base_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


def _resource_dir() -> Path:
    if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
        return Path(sys._MEIPASS)  # type: ignore[attr-defined]
    return _base_dir()


def main() -> int:
    base_dir = _base_dir()
    resource_dir = _resource_dir()

    default_input = base_dir / "in"
    default_output = base_dir / "out"
    default_config = resource_dir / "config" / "mvp_config.json"
    default_log = default_output / "employee_location_app.log"

    ap = argparse.ArgumentParser(
        description="Read report files from a folder and generate employee location output."
    )
    ap.add_argument("--input-dir", default=str(default_input), help="Folder with Excel/PDF reports")
    ap.add_argument("--output-dir", default=str(default_output), help="Folder for generated output files")
    ap.add_argument("--config", default=str(default_config), help="Config JSON path")
    ap.add_argument(
        "--log-file",
        default=str(default_log),
        help="Log file path (default: out/employee_location_app.log). Use empty string to disable file logging.",
    )
    ap.add_argument(
        "--log-level",
        default="INFO",
        help='Log level (DEBUG, INFO, WARNING, ERROR). Default: "INFO".',
    )
    ap.add_argument(
        "--quiet",
        action="store_true",
        help="Disable console logging (file logging still enabled unless --log-file is empty).",
    )
    ap.add_argument(
        "--asof",
        default=datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        help='As-of timestamp. Default: current UTC time, e.g. "2026-04-16T12:00:00Z"',
    )
    args = ap.parse_args()

    log_file = str(args.log_file).strip()
    if log_file == "":
        log_file = None
    configure_logging(
        cfg=LoggingConfig(
            level=str(args.log_level),
            log_file=log_file,
            console=not bool(args.quiet),
        )
    )

    asof = parse_asof(args.asof)
    try:
        _, locations = run_pipeline(
            config_path=args.config,
            asof=asof,
            input_dir=args.input_dir,
            output_dir=args.output_dir,
        )
    except RuntimeError as exc:
        # Common in publish-safe repos where `in/` is empty by default.
        print(str(exc))
        return 2

    print(f"Wrote {len(locations)} employee location rows to {args.output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

