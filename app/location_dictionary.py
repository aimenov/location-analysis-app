from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import pandas as pd

from .utils import normalize_whitespace


@dataclass(frozen=True)
class LocationDictionary:
    raw_to_canonical: dict[str, str]

    @classmethod
    def from_csv(cls, path: str) -> "LocationDictionary":
        df = pd.read_csv(path)
        mapping: dict[str, str] = {}
        for _, r in df.iterrows():
            raw = str(r["raw"]) if pd.notna(r["raw"]) else ""
            canonical = str(r["canonical"]) if pd.notna(r["canonical"]) else ""
            raw_n = normalize_whitespace(raw).lower()
            if raw_n and canonical:
                mapping[raw_n] = canonical
        return cls(raw_to_canonical=mapping)

    def canonicalize(self, raw: Optional[str]) -> Optional[str]:
        if raw is None or (isinstance(raw, float) and pd.isna(raw)):
            return None
        s = str(raw)
        key = normalize_whitespace(s).lower()
        if key in self.raw_to_canonical:
            return self.raw_to_canonical[key]

        # Handle cases like "ASTANA (KZ)" -> "astana"
        if "(" in s:
            key2 = normalize_whitespace(s.split("(")[0]).lower()
            if key2 in self.raw_to_canonical:
                return self.raw_to_canonical[key2]

        # Handle cases like "EW / Samal - Zapadny Eskene" -> "ew / samal"
        if " / " in s:
            key3 = normalize_whitespace(s.split(" / ")[0]).lower()
            if key3 in self.raw_to_canonical:
                return self.raw_to_canonical[key3]

        return self.raw_to_canonical.get(key, s)

