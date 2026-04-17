from __future__ import annotations

import logging
import re
from datetime import timedelta
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd
import pdfplumber

from .models import NormalizedEvent
from .utils import parse_dt
from .validation import require_columns

logger = logging.getLogger(__name__)


_HR_READER_CODE = re.compile(r"reader\s*:\s*(?P<code>[^-]+?)(?:\s*-\s*|$)", re.IGNORECASE)
_PDF_LOCATION_RE = re.compile(
    r"Location:\s*(?P<loc>[A-Za-z0-9_]+)\s+(?:ETA|ETD):\s*(?P<dt>\d{2}\.\d{2}\.\d{4}\s+\d{2}:\d{2})",
    re.IGNORECASE,
)


def _first_token_before_paren(s: str) -> str:
    if "(" in s:
        return s.split("(")[0].strip()
    return s.strip()


def _extract_office_code(reader_val: object) -> Optional[str]:
    if reader_val is None or (isinstance(reader_val, float) and pd.isna(reader_val)):
        return None
    s = str(reader_val).strip()
    if not s:
        return None
    m = _HR_READER_CODE.search(s)
    code = m.group("code").strip() if m else s

    cu = code.upper()
    # Your offices: AS-Astana, AK-Aktau, AT-Atyrau, BT-Bautino, EW/Samal-Zapadny Eskene
    if "SAMAL" in cu or "ZAPAD" in cu:
        return "EW / Samal"
    if re.search(r"\bAS\b", cu):
        return "AS"
    if re.search(r"\bAK\b", cu):
        return "AK"
    if re.search(r"\bAT\b", cu):
        return "AT"
    if re.search(r"\bBT\b", cu):
        return "BT"
    if re.search(r"\bEW\b", cu):
        return "EW / Samal"

    # Unknown reader => not a meaningful office location for MVP.
    return None


def _parse_hr_datetime(date_val: object, time_val: object) -> pd.Timestamp:
    """
    HR Attendance uses dd/mm/yyyy for Date and HH:MM:SS for Time.
    We'll parse with dayfirst=True.
    """
    d = str(date_val).strip()
    t = str(time_val).strip()
    if not d or d.lower() == "nan" or not t or t.lower() == "nan":
        raise ValueError(f"Missing HR date/time: {date_val=}, {time_val=}")
    dt_str = f"{d} {t}"
    return pd.Timestamp(parse_dt(dt_str, dayfirst=True))


def _make_event(
    *,
    employee_id: Optional[str],
    email: Optional[str],
    name: Optional[str],
    event_type: str,
    start_ts,
    end_ts,
    location_raw: Optional[str],
    source: str,
    source_priority: int,
    extra: Optional[dict] = None,
) -> NormalizedEvent:
    return NormalizedEvent(
        employee_id=employee_id,
        email=email,
        name=name,
        event_type=event_type,
        start_ts=start_ts,
        end_ts=end_ts,
        location_raw=location_raw,
        source=source,
        source_priority=int(source_priority),
        extra=extra or {},
    )


def parse_remote_working_request(path: Path, *, asof_tz: str, source_label: str, source_priority: int) -> list[NormalizedEvent]:
    df = pd.read_excel(path, sheet_name=0, engine="openpyxl")
    # The relevant sheet name in your file is: "Remote Working Request Detaile"
    # but using sheet_name=0 keeps us resilient if there are extra sheets order-changes.
    # If the first sheet is wrong, we'll try the explicit one.
    if "Remote Location" not in df.columns:
        df = pd.read_excel(path, sheet_name="Remote Working Request Detaile", engine="openpyxl")

    require_columns(
        df=df,
        table=f"remote_working_request:{path.name}",
        required=["Request Approval Status", "NK", "Employee Full Name", "Start Date", "End Date", "Remote Location"],
    )

    events: list[NormalizedEvent] = []
    for _, r in df.iterrows():
        status = r.get("Request Approval Status")
        if isinstance(status, str) and "approve" not in status.lower():
            continue
        employee_id = str(r.get("NK")) if pd.notna(r.get("NK")) else None
        name = str(r.get("Employee Full Name")) if pd.notna(r.get("Employee Full Name")) else None
        start_ts = parse_dt(r.get("Start Date"), tz=asof_tz)
        end_ts = parse_dt(r.get("End Date"), tz=asof_tz) + timedelta(days=1)
        location_raw = str(r.get("Remote Location")) if pd.notna(r.get("Remote Location")) else None

        events.append(
            _make_event(
                employee_id=employee_id,
                email=None,
                name=name,
                event_type="working_format",
                start_ts=start_ts,
                end_ts=end_ts,
                location_raw=_first_token_before_paren(location_raw) if location_raw else None,
                source=source_label,
                source_priority=source_priority,
            )
        )
    return events


def parse_hr_attendance(path: Path, *, asof_tz: str, source_label: str, source_priority: int) -> list[NormalizedEvent]:
    # Your HR Attendance file has pre-headers; the correct header row is 3
    # for sheet: "Detail Log By Cardholders".
    sheet = "Detail Log By Cardholders"
    # Read once with header=None to find the header row robustly.
    raw = pd.read_excel(path, sheet_name=sheet, header=None, engine="openpyxl")
    header_row = None
    for i in range(min(50, len(raw.index))):
        row_vals = raw.iloc[i].astype(str).fillna("").tolist()
        hay = " ".join(row_vals).lower()
        if "nk" in hay and "reader" in hay and "date" in hay and "time" in hay:
            header_row = i
            break
    if header_row is None:
        header_row = 3

    df = pd.read_excel(path, sheet_name=sheet, header=header_row, engine="openpyxl")
    # Expected columns (after your inspection):
    # 'NK', 'First Name - Last name', 'Badge #', 'Date', 'Time', 'Reader'
    require_columns(
        df=df,
        table=f"hr_attendance:{path.name}",
        required=["NK", "First Name - Last name", "Date", "Time", "Reader"],
    )
    events: list[NormalizedEvent] = []
    rejected = 0
    for _, r in df.iterrows():
        nk = r.get("NK")
        employee_id = str(nk) if pd.notna(nk) else None
        if employee_id is None:
            badge = r.get("Badge #")
            employee_id = str(badge) if pd.notna(badge) else None
        name = str(r.get("First Name - Last name")) if pd.notna(r.get("First Name - Last name")) else None
        try:
            dt = parse_dt(f"{r.get('Date')} {r.get('Time')}", tz=asof_tz, dayfirst=True)
        except Exception:
            rejected += 1
            continue
        office_code = _extract_office_code(r.get("Reader"))
        events.append(
            _make_event(
                employee_id=employee_id,
                email=None,
                name=name,
                event_type="office_checkin",
                start_ts=dt,
                end_ts=None,
                location_raw=office_code,
                source=source_label,
                source_priority=source_priority,
            )
        )
    if rejected:
        logger.warning(
            "Rejected %d row(s) in HR attendance due to unparseable date/time. file=%s source=%s",
            rejected,
            str(path),
            source_label,
        )
    return events


def parse_absence_details(path: Path, *, asof_tz: str, source_label: str, source_priority: int) -> list[NormalizedEvent]:
    sheet = "Excel Output"
    raw = pd.read_excel(path, sheet_name=sheet, header=None, engine="openpyxl")
    header_row = None
    for i in range(min(50, len(raw.index))):
        row_vals = raw.iloc[i].astype(str).fillna("").tolist()
        hay = " ".join(row_vals).lower()
        if "last name (in english)" in hay and "first name (in english)" in hay and "time type" in hay:
            header_row = i
            break
    if header_row is None:
        header_row = 2

    df = pd.read_excel(path, sheet_name=sheet, header=header_row, engine="openpyxl")
    require_columns(
        df=df,
        table=f"absence_details:{path.name}",
        required=["userId", "(userId) First Name (in English)", "(userId) Last Name (in English)", "Time Type", "startDate", "endDate"],
    )

    events: list[NormalizedEvent] = []
    for _, r in df.iterrows():
        employee_id = str(r.get("userId")) if pd.notna(r.get("userId")) else None
        first = r.get("(userId) First Name (in English)")
        last = r.get("(userId) Last Name (in English)")
        name = None
        if pd.notna(first) and pd.notna(last):
            name = f"{str(first).strip()} {str(last).strip()}"

        time_type = r.get("Time Type")
        tt = str(time_type).strip() if pd.notna(time_type) else ""
        event_type = "vacation" if tt.upper().startswith("ANN") else "day_off"

        start_ts = parse_dt(r.get("startDate"), tz=asof_tz, dayfirst=False)
        end_ts = parse_dt(r.get("endDate"), tz=asof_tz, dayfirst=False) + timedelta(days=1)
        events.append(
            _make_event(
                employee_id=employee_id,
                email=None,
                name=name,
                event_type=event_type,
                start_ts=start_ts,
                end_ts=end_ts,
                location_raw=None,
                source=source_label,
                source_priority=source_priority,
            )
        )
    return events


def parse_export_travel(path: Path, *, asof_tz: str, source_label: str, source_priority: int) -> list[NormalizedEvent]:
    df = pd.read_excel(path, sheet_name=0, engine="openpyxl")
    # Columns in your file are already proper:
    # 'Personnel Number', 'Employee Name', 'Beginning Date of Trip Segment', 'End Date of Trip Segment',
    # 'Destination/Multiple Destinations', 'Approval Travel Status'
    require_columns(
        df=df,
        table=f"export_travel:{path.name}",
        required=[
            "Personnel Number",
            "Employee Name",
            "Beginning Date of Trip Segment",
            "End Date of Trip Segment",
            "Destination/Multiple Destinations",
            "Approval Travel Status",
        ],
    )
    events: list[NormalizedEvent] = []
    for _, r in df.iterrows():
        status = r.get("Approval Travel Status")
        if isinstance(status, str) and "approve" not in status.lower():
            continue
        employee_id = str(r.get("Personnel Number")) if pd.notna(r.get("Personnel Number")) else None
        name = str(r.get("Employee Name")) if pd.notna(r.get("Employee Name")) else None
        start_ts = parse_dt(r.get("Beginning Date of Trip Segment"), tz=asof_tz)
        end_ts = parse_dt(r.get("End Date of Trip Segment"), tz=asof_tz) + timedelta(days=1)
        dest = r.get("Destination/Multiple Destinations")
        destination_raw = str(dest).strip() if pd.notna(dest) else None
        destination_raw = _first_token_before_paren(destination_raw) if destination_raw else None

        events.append(
            _make_event(
                employee_id=employee_id,
                email=None,
                name=name,
                event_type="travel",
                start_ts=start_ts,
                end_ts=end_ts,
                location_raw=destination_raw,
                source=source_label,
                source_priority=source_priority,
            )
        )
    return events


def _extract_pdf_text(path: Path) -> str:
    with pdfplumber.open(str(path)) as pdf:
        parts: list[str] = []
        for page in pdf.pages:
            parts.append(page.extract_text() or "")
    return "\n".join(parts)


def _parse_name_from_pdf_line(line: str) -> Optional[str]:
    """
    Example line:
      NURAGANOV, Nurbek NCOC 87015442958 CHAGALA RT
    We'll capture last + first from the comma-separated prefix.
    """
    s = line.strip()
    if not s or "," not in s:
        return None
    # last name is usually uppercase before comma
    # Capture:
    #   LAST, First ...
    # Example: "NURAGANOV, Nurbek NCOC ..."
    # We keep this permissive to avoid regex edge-cases in PDF-extracted text.
    m = re.match(r"^(?P<last>[A-Z][^,]*),\s*(?P<first>[A-Za-z'-]+)", s)
    if not m:
        return None
    last = m.group("last").strip()
    first = m.group("first").strip()

    # Clean up leftovers like extra punctuation
    last = re.sub(r"[^A-Za-z\-\'\s]", "", last).strip()
    # Title-case for consistency with Excel first/last order.
    return f"{first.title()} {last.title()}"


def parse_transport_pdf(path: Path, *, side: str, source_label: str, source_priority: int, asof_tz: str = "UTC") -> list[NormalizedEvent]:
    """
    transport pdf = flight routing summary with passengers in blocks.
    We infer `travel` events:
      start_ts = ETA/ETD datetime
      end_ts   = start_ts + 1 day
      location_raw = airport/city token from `Location:` line
    """
    text = _extract_pdf_text(path)
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

    events: list[NormalizedEvent] = []
    current_dt = None
    current_loc = None
    in_passenger_section = False

    for ln in lines:
        # Example:
        # Location: IST ETA: 16.04.2026 07:20 Number of Arrivals: 1
        if ln.lower().startswith("location:"):
            m = _PDF_LOCATION_RE.search(ln)
            if m:
                current_loc = m.group("loc")
                dt_str = m.group("dt")
                # dd.mm.yyyy HH:MM
                current_dt = parse_dt(dt_str, tz=asof_tz, dayfirst=True)
                in_passenger_section = False
            continue

        if ln.upper().startswith("ATA:") or ln.upper().startswith("ATD:"):
            in_passenger_section = True
            continue

        if ln.lower().startswith("transportation no:"):
            in_passenger_section = False
            continue

        if in_passenger_section and current_dt is not None and current_loc is not None:
            name = _parse_name_from_pdf_line(ln)
            if name:
                events.append(
                    _make_event(
                        employee_id=None,
                        email=None,
                        name=name,
                        event_type="travel",
                        start_ts=current_dt,
                        end_ts=current_dt + timedelta(days=1),
                        location_raw=_first_token_before_paren(current_loc),
                        source=source_label,
                        source_priority=source_priority,
                    )
                )
    return events

