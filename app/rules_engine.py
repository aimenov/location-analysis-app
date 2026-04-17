from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd

from .location_dictionary import LocationDictionary


@dataclass(frozen=True)
class RulesConfig:
    event_type_priority: list[str]
    office_checkin_valid_hours: int = 16
    working_format_default_location: str = "REMOTE"


def _is_active(asof: datetime, start_ts: datetime, end_ts: Optional[datetime]) -> bool:
    if start_ts > asof:
        return False
    if end_ts is None:
        return True
    return asof < end_ts


def _active_reason(*, asof: datetime, valid_checkin_since: datetime, event_type: str, start_ts: datetime, end_ts: Optional[datetime]) -> tuple[bool, str]:
    if event_type == "office_checkin":
        if start_ts > asof:
            return False, "future_checkin"
        if start_ts < valid_checkin_since:
            return False, "stale_checkin"
        return True, "active_checkin"

    if start_ts > asof:
        return False, "future_event"
    if end_ts is None:
        return True, "open_ended"
    if asof >= end_ts:
        return False, "ended"
    return True, "active_window"


def infer_employee_locations_with_trace(
    *,
    events: pd.DataFrame,
    asof: datetime,
    rules: RulesConfig,
    location_dict: LocationDictionary,
    trace_top_n: int = 5,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Like `infer_employee_locations`, but also returns a decision trace table.

    The trace is designed to answer "why did X win?" and "why did Y lose?" for each employee.
    """
    valid_hours = max(1, int(rules.office_checkin_valid_hours))
    checkin_min = asof - timedelta(hours=valid_hours)
    type_rank = {t: i for i, t in enumerate(rules.event_type_priority)}

    base = events.copy()
    base["location_canonical"] = base["location_raw"].map(location_dict.canonicalize)
    base["event_type_rank"] = base["event_type"].map(lambda t: type_rank.get(t, 999))

    active_flags = base.apply(
        lambda r: _active_reason(
            asof=asof,
            valid_checkin_since=checkin_min,
            event_type=r["event_type"],
            start_ts=r["start_ts"],
            end_ts=r["end_ts"],
        ),
        axis=1,
        result_type="expand",
    )
    base["is_active"] = active_flags[0]
    base["active_reason"] = active_flags[1]

    rank_cols = ["employee_key", "event_type_rank", "source_priority", "start_ts"]
    base_sorted = base.sort_values(by=rank_cols, ascending=[True, True, False, False]).copy()

    active_sorted = base_sorted[base_sorted["is_active"]].copy()
    chosen_active = active_sorted.groupby("employee_key", as_index=False).head(1)
    chosen_any = base_sorted.groupby("employee_key", as_index=False).head(1)

    chosen = chosen_any.merge(
        chosen_active[
            ["employee_key", "event_type", "start_ts", "end_ts", "location_canonical", "source", "source_priority"]
        ],
        on="employee_key",
        how="left",
        suffixes=("_fallback", ""),
    )

    def pick(col: str):
        return chosen[col].where(chosen[col].notna(), chosen[f"{col}_fallback"])

    chosen["chosen_event_type"] = pick("event_type")
    chosen["chosen_start_ts"] = pick("start_ts")
    chosen["chosen_end_ts"] = pick("end_ts")
    chosen["chosen_location_canonical"] = pick("location_canonical")
    chosen["chosen_source"] = pick("source")
    chosen["chosen_source_priority"] = pick("source_priority")

    def final_location(r) -> str:
        et = r["chosen_event_type"]
        loc = r["chosen_location_canonical"]
        if pd.isna(loc):
            loc = None
        if et in ("vacation", "day_off"):
            return "OFF"
        if et == "travel":
            return str(loc or "TRAVEL")
        if et == "office_checkin":
            return str(loc or "OFFICE")
        if et == "working_format":
            return str(loc or rules.working_format_default_location)
        return str(loc or "UNKNOWN")

    chosen["location"] = chosen.apply(final_location, axis=1)

    locations = chosen[
        [
            "employee_key",
            "resolved_employee_id",
            "resolved_email",
            "resolved_name",
            "location",
            "chosen_event_type",
            "chosen_start_ts",
            "chosen_end_ts",
            "chosen_source",
            "chosen_source_priority",
        ]
    ].sort_values(by=["resolved_name", "resolved_email", "employee_key"])

    # Trace: top-N candidates with active info and winner marker.
    base_sorted["candidate_rank"] = base_sorted.groupby("employee_key").cumcount() + 1
    trace = base_sorted[base_sorted["candidate_rank"] <= int(trace_top_n)].copy()
    trace = trace.merge(
        locations[["employee_key", "chosen_event_type", "chosen_start_ts", "chosen_source"]],
        on="employee_key",
        how="left",
    )
    trace["is_winner"] = (
        (trace["event_type"] == trace["chosen_event_type"])
        & (trace["start_ts"] == trace["chosen_start_ts"])
        & (trace["source"] == trace["chosen_source"])
    )
    trace = trace[
        [
            "employee_key",
            "resolved_employee_id",
            "resolved_email",
            "resolved_name",
            "candidate_rank",
            "is_winner",
            "is_active",
            "active_reason",
            "event_type",
            "event_type_rank",
            "source",
            "source_priority",
            "start_ts",
            "end_ts",
            "location_raw",
            "location_canonical",
        ]
    ].sort_values(by=["resolved_name", "employee_key", "candidate_rank"])

    return locations, trace


def infer_employee_locations(
    *,
    events: pd.DataFrame,
    asof: datetime,
    rules: RulesConfig,
    location_dict: LocationDictionary,
) -> pd.DataFrame:
    """
    Returns a per-employee table with inferred location + evidence.

    Decision strategy:
      1) Filter to events active at `asof` (with special handling for office_checkin)
      2) Rank by:
         - event_type priority (config)
         - source_priority (higher wins)
         - most recent start_ts
      3) Compute a canonical location from location_raw and event_type.
    """
    locations, _trace = infer_employee_locations_with_trace(
        events=events,
        asof=asof,
        rules=rules,
        location_dict=location_dict,
        trace_top_n=1,
    )
    return locations

