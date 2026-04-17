from __future__ import annotations

from typing import Iterable

import pandas as pd

from .models import NormalizedEvent


def events_to_frame(events: Iterable[NormalizedEvent]) -> pd.DataFrame:
    rows = []
    for e in events:
        rows.append(
            {
                "employee_id": e.employee_id,
                "email": e.email,
                "name": e.name,
                "event_type": e.event_type,
                "start_ts": e.start_ts,
                "end_ts": e.end_ts,
                "location_raw": e.location_raw,
                "source": e.source,
                "source_priority": e.source_priority,
                "extra": e.extra,
            }
        )
    return pd.DataFrame(rows)

