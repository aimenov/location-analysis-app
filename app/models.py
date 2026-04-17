from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass(frozen=True)
class NormalizedEvent:
    employee_id: Optional[str]
    email: Optional[str]
    name: Optional[str]
    event_type: str
    start_ts: datetime
    end_ts: Optional[datetime]
    location_raw: Optional[str]
    source: str
    source_priority: int
    extra: dict


@dataclass(frozen=True)
class ResolvedEmployee:
    employee_key: str  # stable key produced by entity resolution
    employee_id: Optional[str]
    email: Optional[str]
    name: Optional[str]

