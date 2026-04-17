from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, Optional

from dateutil import parser as date_parser


_WS_RE = re.compile(r"\\s+")


def normalize_whitespace(s: str) -> str:
    return _WS_RE.sub(" ", s.strip())


def normalize_name(name: Optional[str]) -> Optional[str]:
    if not name:
        return None
    s = normalize_whitespace(name).lower()
    # Keep letters/numbers (unicode), spaces, and basic separators.
    # Using `\w` keeps Cyrillic too (letters/digits/underscore).
    s = re.sub(r"[^\w\s\-\\.'`]", "", s, flags=re.UNICODE)
    s = normalize_whitespace(s)
    return s or None


def normalize_name_tokenset(name: Optional[str]) -> Optional[str]:
    """
    Normalize a name into an order-insensitive token key.

    This intentionally trades some precision for better real-world entity matching
    in reports where "First Last" and "Last First" appear inconsistently.
    """
    n = normalize_name(name)
    if not n:
        return None
    tokens = [t for t in n.split(" ") if t]
    if not tokens:
        return None
    tokens.sort()
    return " ".join(tokens)


def normalize_email(email: Optional[str]) -> Optional[str]:
    if not email:
        return None
    s = email.strip().lower()
    return s or None


def normalize_employee_id(employee_id: Optional[str]) -> Optional[str]:
    if not employee_id:
        return None
    s = str(employee_id).strip()
    return s or None


def parse_dt(value: Any, tz: str = "UTC", dayfirst: bool = False) -> datetime:
    """
    Parse many date/datetime inputs into a timezone-aware datetime.
    Assumption for MVP: naive datetimes are interpreted as UTC.
    """
    if isinstance(value, datetime):
        dt = value
    else:
        dt = date_parser.parse(str(value), dayfirst=dayfirst)

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def coalesce(*vals):
    for v in vals:
        if v is not None and v != "":
            return v
    return None

