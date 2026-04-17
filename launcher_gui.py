from __future__ import annotations

import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path

from app.pipeline import run_pipeline
from app.logging_setup import LoggingConfig, configure_logging


def _base_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


def _resource_dir() -> Path:
    if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
        return Path(sys._MEIPASS)  # type: ignore[attr-defined]
    return _base_dir()


def _show_message(title: str, message: str) -> None:
    # Tk is in the stdlib and works well for a tiny dialog.
    import tkinter as tk
    from tkinter import messagebox

    root = tk.Tk()
    root.withdraw()
    root.attributes("-topmost", True)
    messagebox.showinfo(title, message)
    root.destroy()


def _show_error(title: str, message: str) -> None:
    import tkinter as tk
    from tkinter import messagebox

    root = tk.Tk()
    root.withdraw()
    root.attributes("-topmost", True)
    messagebox.showerror(title, message)
    root.destroy()


def main() -> int:
    base_dir = _base_dir()
    resource_dir = _resource_dir()

    input_dir = base_dir / "in"
    output_dir = base_dir / "out"
    config_path = resource_dir / "config" / "mvp_config.json"
    log_file = output_dir / "employee_location_app.log"

    asof = datetime.now(timezone.utc).replace(microsecond=0)

    if not input_dir.exists():
        _show_error(
            "Employee Location App",
            f"Input folder not found:\n{input_dir}\n\nCreate an 'in' folder next to the .exe and put reports there.",
        )
        return 2

    if not any(input_dir.iterdir()):
        _show_error(
            "Employee Location App",
            f"No report files found in:\n{input_dir}\n\nPut Excel/PDF reports in the 'in' folder and run again.",
        )
        return 2

    try:
        configure_logging(cfg=LoggingConfig(level="INFO", log_file=str(log_file), console=False))
        _, locations = run_pipeline(
            config_path=str(config_path),
            asof=asof,
            input_dir=str(input_dir),
            output_dir=str(output_dir),
        )
    except Exception as exc:
        details = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        _show_error(
            "Employee Location App - Error",
            "Failed to generate output.\n\n"
            f"Error:\n{exc}\n\n"
            f"Details:\n{details}",
        )
        return 1

    out_xlsx = output_dir / "employee_locations.xlsx"
    _show_message(
        "Employee Location App",
        f"Done.\n\nRows: {len(locations)}\nOutput:\n{out_xlsx}",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

