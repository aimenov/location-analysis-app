from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Optional

import pandas as pd
from rapidfuzz import fuzz

from .utils import normalize_email, normalize_employee_id, normalize_name, normalize_name_tokenset


@dataclass(frozen=True)
class EntityResolutionConfig:
    fuzzy_enabled: bool = True
    fuzzy_threshold: int = 92


def _stable_key(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:16]


def resolve_employees(events: pd.DataFrame, cfg: EntityResolutionConfig) -> pd.DataFrame:
    """
    Adds:
      - employee_key: stable resolved identity
      - resolved_email/resolved_employee_id/resolved_name: best-known attributes

    MVP strategy:
      1) Strict match on normalized email if present
      2) Strict match on employee_id if present
      3) Strict match on normalized name
      4) Optional fuzzy match on name (token_set_ratio) among unmatched
    """
    df = events.copy()
    # Use apply() to preserve Python None values (map() may upcast to NaN).
    df["email_n"] = df["email"].apply(normalize_email)
    df["employee_id_n"] = df["employee_id"].apply(normalize_employee_id)
    df["name_n"] = df["name"].apply(normalize_name)
    df["name_tokenset"] = df["name"].apply(normalize_name_tokenset)

    # Pandas may upcast None to NaN; normalize to real None so truthiness checks behave.
    for col in ("email_n", "employee_id_n", "name_n", "name_tokenset"):
        df[col] = df[col].where(df[col].notna(), None)

    employee_key: list[Optional[str]] = [None] * len(df)

    email_to_key: dict[str, str] = {}
    id_to_key: dict[str, str] = {}
    name_to_key: dict[str, str] = {}

    def get_or_make_key(seed: str) -> str:
        return _stable_key(seed)

    def is_missing(v: object) -> bool:
        return v is None or (isinstance(v, float) and pd.isna(v))

    # Pass 1: email
    for i, r in df.iterrows():
        em = r["email_n"]
        if is_missing(em):
            continue
        em = str(em)
        if em:
            if em not in email_to_key:
                email_to_key[em] = get_or_make_key(f"email:{em}")
            employee_key[i] = email_to_key[em]

    # Pass 2: employee_id (only if still unresolved)
    for i, r in df.iterrows():
        if employee_key[i] is not None:
            continue
        eid = r["employee_id_n"]
        if is_missing(eid):
            continue
        eid = str(eid)
        if eid:
            if eid not in id_to_key:
                id_to_key[eid] = get_or_make_key(f"id:{eid}")
            employee_key[i] = id_to_key[eid]

    # Bridge: if an event already has a resolved key (from email/id),
    # register its name token-set to that same key so name-only sources
    # (like PDFs) can attach to the canonical identity.
    for i, r in df.iterrows():
        k = employee_key[i]
        if k is None:
            continue
        nm = r["name_tokenset"]
        if is_missing(nm):
            continue
        nm = str(nm)
        if nm and nm not in name_to_key:
            name_to_key[nm] = k

    # Pass 3: exact name (order-insensitive token set)
    for i, r in df.iterrows():
        if employee_key[i] is not None:
            continue
        nm = r["name_tokenset"]
        if is_missing(nm):
            continue
        nm = str(nm)
        if nm:
            if nm not in name_to_key:
                name_to_key[nm] = get_or_make_key(f"name:{nm}")
            employee_key[i] = name_to_key[nm]

    # Pass 4: fuzzy name (only for those with a name but no key)
    if cfg.fuzzy_enabled:
        unresolved = [i for i, k in enumerate(employee_key) if k is None and df.iloc[i]["name_tokenset"]]
        if unresolved:
            # Build candidate list from already-known names
            known = list(name_to_key.items())
            for i in unresolved:
                nm = df.iloc[i]["name_tokenset"]
                best = None
                best_score = -1
                for known_name, key in known:
                    score = fuzz.token_set_ratio(nm, known_name)
                    if score > best_score:
                        best_score = score
                        best = key
                if best is not None and best_score >= cfg.fuzzy_threshold:
                    employee_key[i] = best
                else:
                    employee_key[i] = get_or_make_key(f"name:{nm}")

    # Fallback: make key from row index (should be rare)
    for i, k in enumerate(employee_key):
        if k is None:
            employee_key[i] = get_or_make_key(f"row:{i}")

    df["employee_key"] = employee_key

    # Best-known attributes per key (prefer non-null, then most frequent)
    def best_nonnull(series: pd.Series) -> Optional[str]:
        s = series.dropna()
        s = s[s.astype(str) != ""]
        if s.empty:
            return None
        return str(s.value_counts().index[0])

    resolved = (
        df.groupby("employee_key", as_index=False)
        .agg(
            resolved_employee_id=("employee_id_n", best_nonnull),
            resolved_email=("email_n", best_nonnull),
            resolved_name=("name", best_nonnull),
        )
    )
    df = df.merge(resolved, on="employee_key", how="left")
    return df.drop(columns=["email_n", "employee_id_n", "name_n", "name_tokenset"])

