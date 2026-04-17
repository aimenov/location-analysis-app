from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Optional

import pandas as pd
import pdfplumber


ReportType = Literal[
    "remote_working_request",
    "hr_attendance",
    "absence_details",
    "export_travel",
    "transport_arrival",
    "transport_departure",
]


@dataclass(frozen=True)
class DiscoveredReport:
    report_type: ReportType
    path: Path

    # Optional human-friendly label (used as `source` for evidence).
    source_label: Optional[str] = None


def _read_excel_preview(path: Path, sheet_name=0, rows: int = 12) -> pd.DataFrame:
    return pd.read_excel(path, sheet_name=sheet_name, header=None, nrows=rows, engine="openpyxl")


def _excel_preview_text(df: pd.DataFrame) -> str:
    vals = df.fillna("").astype(str).values.flatten().tolist()
    return " ".join(vals).lower()


def _detect_excel_report_type(path: Path) -> Optional[ReportType]:
    try:
        xls = pd.ExcelFile(path, engine="openpyxl")
    except Exception:
        return None

    sheet_names = [str(s).lower() for s in xls.sheet_names]
    sheets_text = " ".join(sheet_names)

    if "detail log by cardholders" in sheets_text or "detail by cardholders" in sheets_text:
        try:
            preview = _read_excel_preview(path, sheet_name="Detail Log By Cardholders", rows=15)
        except Exception:
            preview = _read_excel_preview(path, sheet_name=0, rows=15)
        text = _excel_preview_text(preview)
        if "reader" in text and "badge #" in text and "first name - last name" in text:
            return "hr_attendance"

    if "excel output" in sheets_text:
        try:
            preview = _read_excel_preview(path, sheet_name="Excel Output", rows=12)
        except Exception:
            preview = _read_excel_preview(path, sheet_name=0, rows=12)
        text = _excel_preview_text(preview)
        if "time type" in text and "last name (in english)" in text and "first name (in english)" in text:
            return "absence_details"

    try:
        preview0 = _read_excel_preview(path, sheet_name=0, rows=10)
        text0 = _excel_preview_text(preview0)
    except Exception:
        text0 = ""

    if "employee full name" in text0 and "remote location" in text0 and "request approval status" in text0:
        return "remote_working_request"

    if "personnel number" in text0 and "trip activity type" in text0 and "destination/multiple destinations" in text0:
        return "export_travel"

    return None


def _extract_pdf_preview_text(path: Path, max_pages: int = 2) -> str:
    try:
        with pdfplumber.open(str(path)) as pdf:
            parts: list[str] = []
            for page in pdf.pages[:max_pages]:
                parts.append(page.extract_text() or "")
        return "\n".join(parts).lower()
    except Exception:
        return ""


def _detect_pdf_report_type(path: Path) -> Optional[ReportType]:
    text = _extract_pdf_preview_text(path)
    if not text:
        return None

    if (
        "arrivals/departures:arrival" in text
        or ("arrival summary" in text and "eta:" in text and "ata:" in text)
    ):
        return "transport_arrival"
    if (
        "arrivals/departures:departure" in text
        or ("departure summary" in text and "etd:" in text and "atd:" in text)
    ):
        return "transport_departure"
    return None


def discover_reports(in_dir: str = "in") -> list[DiscoveredReport]:
    root = Path(in_dir)
    if not root.exists():
        raise FileNotFoundError(f"Input directory not found: {root}")

    reports: list[DiscoveredReport] = []

    for p in sorted(root.iterdir()):
        if not p.is_file():
            continue
        suffix = p.suffix.lower()
        report_type: Optional[ReportType] = None
        if suffix in {".xlsx", ".xls"}:
            report_type = _detect_excel_report_type(p)
        elif suffix == ".pdf":
            report_type = _detect_pdf_report_type(p)

        if report_type is not None:
            reports.append(DiscoveredReport(report_type=report_type, path=p))

    # De-duplicate while keeping order
    seen: set[tuple[str, str]] = set()
    unique: list[DiscoveredReport] = []
    for r in reports:
        key = (r.report_type, str(r.path))
        if key not in seen:
            seen.add(key)
            unique.append(r)

    return unique

