from __future__ import annotations

import logging
from dataclasses import dataclass
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional


@dataclass(frozen=True)
class LoggingConfig:
    level: str = "INFO"
    log_file: Optional[str] = None
    console: bool = True
    max_bytes: int = 2_000_000
    backup_count: int = 3


def configure_logging(*, cfg: LoggingConfig) -> str | None:
    """
    Configure application logging once for CLI/GUI entrypoints.

    Returns the resolved log file path if file logging is enabled.
    """
    root = logging.getLogger()
    if root.handlers:
        # Avoid double-logging if the host process configured logging already.
        return cfg.log_file

    level_name = (cfg.level or "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    root.setLevel(level)

    formatter = logging.Formatter(
        fmt="%(asctime)sZ %(levelname)s %(name)s - %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    resolved_log_file: str | None = None
    if cfg.log_file:
        log_path = Path(cfg.log_file).expanduser().resolve()
        log_path.parent.mkdir(parents=True, exist_ok=True)
        handler = RotatingFileHandler(
            filename=str(log_path),
            maxBytes=int(cfg.max_bytes),
            backupCount=int(cfg.backup_count),
            encoding="utf-8",
        )
        handler.setFormatter(formatter)
        handler.setLevel(level)
        root.addHandler(handler)
        resolved_log_file = str(log_path)

    if cfg.console:
        sh = logging.StreamHandler()
        sh.setFormatter(formatter)
        sh.setLevel(level)
        root.addHandler(sh)

    logging.getLogger("pdfplumber").setLevel(logging.WARNING)
    logging.getLogger("openpyxl").setLevel(logging.WARNING)

    return resolved_log_file

