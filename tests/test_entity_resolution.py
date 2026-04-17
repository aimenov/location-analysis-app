from __future__ import annotations

import pandas as pd

from app.entity_resolution import EntityResolutionConfig, resolve_employees


def test_resolution_prefers_email_match_over_name() -> None:
    df = pd.DataFrame(
        [
            {"employee_id": None, "email": "a@example.com", "name": "Alice A", "event_type": "travel", "start_ts": "2026-01-01", "end_ts": None, "location_raw": None, "source": "s1", "source_priority": 10, "extra": {}},
            {"employee_id": None, "email": "a@example.com", "name": "Alice Different", "event_type": "vacation", "start_ts": "2026-01-02", "end_ts": None, "location_raw": None, "source": "s2", "source_priority": 20, "extra": {}},
        ]
    )
    out = resolve_employees(df, EntityResolutionConfig(fuzzy_enabled=False))
    assert out["employee_key"].nunique() == 1
    assert out.iloc[0]["resolved_email"] == "a@example.com"


def test_resolution_fuzzy_merges_close_names_when_enabled() -> None:
    df = pd.DataFrame(
        [
            {"employee_id": None, "email": None, "name": "John Smith", "event_type": "travel", "start_ts": "2026-01-01", "end_ts": None, "location_raw": None, "source": "s1", "source_priority": 10, "extra": {}},
            {"employee_id": None, "email": None, "name": "Smith John", "event_type": "vacation", "start_ts": "2026-01-02", "end_ts": None, "location_raw": None, "source": "s2", "source_priority": 20, "extra": {}},
        ]
    )
    out = resolve_employees(df, EntityResolutionConfig(fuzzy_enabled=True, fuzzy_threshold=80))
    assert out["employee_key"].nunique() == 1

