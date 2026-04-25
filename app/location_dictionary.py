from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Optional

import pandas as pd

from .utils import normalize_whitespace


@dataclass(frozen=True)
class LocationDictionary:
    raw_to_canonical: dict[str, str]

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


_IATA_CODE_RE = re.compile(r"^[A-Z]{3}$")


def enrich_location_dictionary_in_place(location_dict: LocationDictionary, *, raw_values: pd.Series) -> int:
    """
    Add previously unseen short codes (e.g. airport/site codes) into the in-memory dictionary.

    Strategy:
    - If a value looks like an IATA code (3 letters), add a self-mapping (e.g. \"ABC\" -> \"ABC\").
    - This prevents \"unknown\" placeholders and makes missing mappings visible to users.
    """
    added = 0
    mapping = location_dict.raw_to_canonical
    for v in raw_values.dropna().unique().tolist():
        s = str(v).strip()
        if not s:
            continue
        up = s.upper()
        key = normalize_whitespace(up).lower()
        if key in mapping:
            continue
        if _IATA_CODE_RE.match(up):
            mapping[key] = up
            added += 1
    return added


# Raw lookup token (lower-cased when indexed) -> canonical label shown in outputs.
_IMPLICIT_ROWS: tuple[tuple[str, str], ...] = (
    ("spb", "Saint Petersburg"),
    ("saint petersburg", "Saint Petersburg"),
    ("moscow", "Moscow"),
    ("msk", "Moscow"),
    ("nyc", "New York"),
    ("new york", "New York"),
    ("remote", "REMOTE"),
    ("home", "REMOTE"),
    ("office", "OFFICE"),
    ("hq", "OFFICE"),
    ("as", "Astana"),
    ("astana", "Astana"),
    ("ak", "Aktau"),
    ("aktau", "Aktau"),
    ("at", "Atyrau"),
    ("atyrau", "Atyrau"),
    ("bt", "Bautino"),
    ("bautino", "Bautino"),
    ("ew", "Zapadny Eskene"),
    ("ew / samal", "Zapadny Eskene"),
    ("samal", "Zapadny Eskene"),
    ("nqz", "Astana"),
    ("ala", "Almaty"),
    ("almaty", "Almaty"),
    # NOTE: In these transport exports, SCO is used for Aktau (not Shymkent).
    # Shymkent's common airport code is CIT.
    ("sco", "Aktau"),
    ("cit", "Shymkent"),
    ("shymkent", "Shymkent"),
    ("ist", "Istanbul"),
    ("chagala", "Chagala"),
    ("akta u", "Aktau"),
    # HR reader/site codes & airport codes seen in transport PDFs
    ("mcp", "MCP"),
    ("guw", "Atyrau"),
)


def implicit_location_dictionary() -> LocationDictionary:
    """
    Build the location dictionary in memory immediately before each computation run.

    There is no external CSV; edit ``_IMPLICIT_ROWS`` above to extend mappings.
    """
    mapping: dict[str, str] = {}
    for raw, canonical in _IMPLICIT_ROWS:
        raw_n = normalize_whitespace(raw).lower()
        if raw_n and canonical:
            mapping[raw_n] = canonical
    return LocationDictionary(raw_to_canonical=mapping)
