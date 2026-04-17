from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from app.location_dictionary import LocationDictionary
from app.rules_engine import RulesConfig, infer_employee_locations_with_trace


def _dt(s: str) -> datetime:
    # ISO string with Z -> UTC
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s).astimezone(timezone.utc)


def _events(rows: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    # Mimic entity_resolution output columns required by rules engine output
    if "resolved_employee_id" not in df.columns:
        df["resolved_employee_id"] = df.get("employee_id")
    if "resolved_email" not in df.columns:
        df["resolved_email"] = df.get("email")
    if "resolved_name" not in df.columns:
        df["resolved_name"] = df.get("name")
    return df


def test_vacation_beats_office_checkin_when_both_active() -> None:
    asof = _dt("2026-04-16T12:00:00Z")
    rules = RulesConfig(event_type_priority=["vacation", "day_off", "office_checkin", "travel", "working_format"])
    loc = LocationDictionary(raw_to_canonical={"as": "Astana"})

    events = _events(
        [
            {
                "employee_key": "e1",
                "employee_id": "1",
                "email": None,
                "name": "Alice",
                "event_type": "office_checkin",
                "start_ts": _dt("2026-04-16T09:00:00Z"),
                "end_ts": None,
                "location_raw": "AS",
                "source": "hr",
                "source_priority": 90,
            },
            {
                "employee_key": "e1",
                "employee_id": "1",
                "email": None,
                "name": "Alice",
                "event_type": "vacation",
                "start_ts": _dt("2026-04-15T00:00:00Z"),
                "end_ts": _dt("2026-04-18T00:00:00Z"),
                "location_raw": None,
                "source": "absence",
                "source_priority": 95,
            },
        ]
    )

    locations, trace = infer_employee_locations_with_trace(events=events, asof=asof, rules=rules, location_dict=loc)
    row = locations.iloc[0]
    assert row["location"] == "OFF"
    assert row["chosen_event_type"] == "vacation"
    assert trace["is_winner"].any()


def test_office_checkin_decays_after_valid_hours() -> None:
    asof = _dt("2026-04-16T20:00:00Z")
    rules = RulesConfig(
        event_type_priority=["vacation", "day_off", "office_checkin", "travel", "working_format"],
        office_checkin_valid_hours=4,
    )
    loc = LocationDictionary(raw_to_canonical={"as": "Astana"})

    events = _events(
        [
            {
                "employee_key": "e1",
                "employee_id": "1",
                "email": None,
                "name": "Alice",
                "event_type": "office_checkin",
                "start_ts": _dt("2026-04-16T12:00:00Z"),
                "end_ts": None,
                "location_raw": "AS",
                "source": "hr",
                "source_priority": 90,
            },
            {
                "employee_key": "e1",
                "employee_id": "1",
                "email": None,
                "name": "Alice",
                "event_type": "working_format",
                "start_ts": _dt("2026-04-16T00:00:00Z"),
                "end_ts": _dt("2026-04-17T00:00:00Z"),
                "location_raw": "remote",
                "source": "remote_req",
                "source_priority": 50,
            },
        ]
    )

    locations, trace = infer_employee_locations_with_trace(events=events, asof=asof, rules=rules, location_dict=loc)
    row = locations.iloc[0]
    assert row["chosen_event_type"] == "working_format"
    assert row["location"] in {"REMOTE", "remote", "UNKNOWN"}  # canonicalization depends on dict
    # Ensure checkin is marked stale in trace
    stale = trace[(trace["event_type"] == "office_checkin") & (trace["active_reason"] == "stale_checkin")]
    assert len(stale.index) == 1

